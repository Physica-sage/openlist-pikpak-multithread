package op

import (
	"context"
	"fmt"
	"os"
	stdpath "path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

const (
	objsUpdateHookBatchConcurrencyEnv = "OPENLIST_BATCH_HOOK_CONCURRENCY"
	objsUpdateHookNotFoundTimeoutEnv  = "OPENLIST_BATCH_HOOK_NOT_FOUND_TIMEOUT"
	defaultObjsUpdateHookConcurrency  = 2
	maxObjsUpdateHookConcurrency      = 4
	maxObjsUpdateHookListAttempts     = 4
	defaultObjsUpdateHookNotFoundWait = 10 * time.Minute
	maxObjsUpdateHookNotFoundWait     = time.Hour
)

var objsUpdateHookListRetryDelays = [...]time.Duration{
	100 * time.Millisecond,
	300 * time.Millisecond,
	time.Second,
}

type objsUpdateHookBatchContextKey struct{}

type objsUpdateHookTarget struct {
	storage   driver.Driver
	dirPath   string
	recursive bool
}

// ObjsUpdateHookBatch collects successful direct-operation hook targets and
// dispatches non-overlapping targets with bounded concurrency after a batch
// request has finished.
type ObjsUpdateHookBatch struct {
	mu         sync.Mutex
	targets    []objsUpdateHookTarget
	seen       map[string]struct{}
	dispatched bool
}

// WithObjsUpdateHookBatch returns a context that makes Move and Copy collect
// their exact hook targets instead of launching one hook goroutine per item.
func WithObjsUpdateHookBatch(ctx context.Context) (context.Context, *ObjsUpdateHookBatch) {
	batch := &ObjsUpdateHookBatch{seen: make(map[string]struct{})}
	return context.WithValue(ctx, objsUpdateHookBatchContextKey{}, batch), batch
}

func enqueueObjsUpdateHook(ctx context.Context, storage driver.Driver, dirPath string, recursive bool) bool {
	batch, ok := ctx.Value(objsUpdateHookBatchContextKey{}).(*ObjsUpdateHookBatch)
	if !ok || batch == nil {
		return false
	}
	return batch.add(storage, dirPath, recursive)
}

func (b *ObjsUpdateHookBatch) add(storage driver.Driver, dirPath string, recursive bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.dispatched {
		return false
	}
	cleanedPath := stdpath.Clean(dirPath)
	key := fmt.Sprintf("%p\x00%s\x00%t", storage.GetStorage(), cleanedPath, recursive)
	if _, ok := b.seen[key]; ok {
		return true
	}
	b.seen[key] = struct{}{}
	b.targets = append(b.targets, objsUpdateHookTarget{
		storage:   storage,
		dirPath:   cleanedPath,
		recursive: recursive,
	})
	return true
}

func objsUpdateHookBatchConcurrency() int {
	value := strings.TrimSpace(os.Getenv(objsUpdateHookBatchConcurrencyEnv))
	concurrency, err := strconv.Atoi(value)
	if err != nil || concurrency < 1 || concurrency > maxObjsUpdateHookConcurrency {
		return defaultObjsUpdateHookConcurrency
	}
	return concurrency
}

func objsUpdateHookBatchLimiter() (*rate.Limiter, bool) {
	item, err := GetSettingItemByKey(conf.HandleHookRateLimit)
	if err != nil {
		return nil, false
	}
	if item == nil {
		return nil, true
	}
	limit, err := strconv.ParseFloat(strings.TrimSpace(item.Value), 64)
	if err != nil || limit <= 0 {
		return nil, true
	}
	return rate.NewLimiter(rate.Limit(limit), 1), true
}

func objsUpdateHookPathContains(parent string, child string) bool {
	parent = stdpath.Clean(parent)
	child = stdpath.Clean(child)
	if parent == child {
		return true
	}
	if parent == "/" {
		return strings.HasPrefix(child, "/")
	}
	return strings.HasPrefix(child, parent+"/")
}

func objsUpdateHookTargetsOverlap(targets []objsUpdateHookTarget) bool {
	for i := range targets {
		for j := i + 1; j < len(targets); j++ {
			if targets[i].storage.GetStorage() != targets[j].storage.GetStorage() {
				continue
			}
			if objsUpdateHookPathContains(targets[i].dirPath, targets[j].dirPath) ||
				objsUpdateHookPathContains(targets[j].dirPath, targets[i].dirPath) {
				return true
			}
		}
	}
	return false
}

type objsUpdateHookWork struct {
	target       objsUpdateHookTarget
	rateLimited  bool
	topLevel     bool
	listAttempts int
	firstAttempt time.Time
}

func objsUpdateHookNotFoundTimeout() time.Duration {
	value := strings.TrimSpace(os.Getenv(objsUpdateHookNotFoundTimeoutEnv))
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 || timeout > maxObjsUpdateHookNotFoundWait {
		return defaultObjsUpdateHookNotFoundWait
	}
	return timeout
}

func objsUpdateHookNotFoundRetryDelay(attempt int) time.Duration {
	if attempt <= len(objsUpdateHookListRetryDelays) {
		return objsUpdateHookListRetryDelays[attempt-1]
	}
	delay := objsUpdateHookListRetryDelays[len(objsUpdateHookListRetryDelays)-1]
	for range attempt - len(objsUpdateHookListRetryDelays) {
		delay *= 2
		if delay >= 30*time.Second {
			return 30 * time.Second
		}
	}
	return delay
}

func objsUpdateHookListRetryDelay(work objsUpdateHookWork, err error) (time.Duration, bool) {
	if work.topLevel && errs.IsObjectNotFound(err) {
		delay := objsUpdateHookNotFoundRetryDelay(work.listAttempts)
		return delay, time.Since(work.firstAttempt)+delay <= objsUpdateHookNotFoundTimeout()
	}
	if work.listAttempts < maxObjsUpdateHookListAttempts {
		return objsUpdateHookListRetryDelays[work.listAttempts-1], true
	}
	return 0, false
}

type objsUpdateHookWorkQueue struct {
	mu             sync.Mutex
	ready          *sync.Cond
	topLevel       []objsUpdateHookWork
	children       []objsUpdateHookWork
	pending        int
	topLevelActive int
}

func newObjsUpdateHookWorkQueue(targets []objsUpdateHookTarget) *objsUpdateHookWorkQueue {
	queue := &objsUpdateHookWorkQueue{
		topLevel: make([]objsUpdateHookWork, 0, len(targets)),
		pending:  len(targets),
	}
	queue.ready = sync.NewCond(&queue.mu)
	for _, target := range targets {
		queue.topLevel = append(queue.topLevel, objsUpdateHookWork{target: target, topLevel: true})
	}
	return queue
}

func (q *objsUpdateHookWorkQueue) take() (objsUpdateHookWork, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.pending > 0 {
		if len(q.topLevel) > 0 {
			work := q.topLevel[0]
			q.topLevel = q.topLevel[1:]
			q.topLevelActive++
			return work, true
		}
		if q.topLevelActive == 0 && len(q.children) > 0 {
			work := q.children[0]
			q.children = q.children[1:]
			return work, true
		}
		q.ready.Wait()
	}
	return objsUpdateHookWork{}, false
}

func (q *objsUpdateHookWorkQueue) retry(work objsUpdateHookWork, delay time.Duration) {
	if work.topLevel {
		q.mu.Lock()
		q.topLevelActive--
		q.ready.Broadcast()
		q.mu.Unlock()
	}
	time.AfterFunc(delay, func() {
		q.mu.Lock()
		if work.topLevel {
			q.topLevel = append(q.topLevel, work)
		} else {
			q.children = append(q.children, work)
		}
		q.ready.Signal()
		q.mu.Unlock()
	})
}

func (q *objsUpdateHookWorkQueue) finish(work objsUpdateHookWork, children []objsUpdateHookWork) {
	q.mu.Lock()
	q.pending += len(children) - 1
	if work.topLevel {
		q.topLevelActive--
	}
	q.children = append(q.children, children...)
	q.ready.Broadcast()
	q.mu.Unlock()
}

type objsUpdateHookEvent struct {
	parent string
	files  []model.Obj
}

type objsUpdateHookEventQueue struct {
	mu     sync.Mutex
	ready  *sync.Cond
	items  []objsUpdateHookEvent
	closed bool
}

func newObjsUpdateHookEventQueue() *objsUpdateHookEventQueue {
	queue := &objsUpdateHookEventQueue{}
	queue.ready = sync.NewCond(&queue.mu)
	return queue
}

func (q *objsUpdateHookEventQueue) push(event objsUpdateHookEvent) {
	q.mu.Lock()
	if !q.closed {
		q.items = append(q.items, event)
		q.ready.Signal()
	}
	q.mu.Unlock()
}

func (q *objsUpdateHookEventQueue) take() (objsUpdateHookEvent, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.ready.Wait()
	}
	if len(q.items) == 0 {
		return objsUpdateHookEvent{}, false
	}
	event := q.items[0]
	q.items = q.items[1:]
	return event, true
}

func (q *objsUpdateHookEventQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.ready.Broadcast()
	q.mu.Unlock()
}

type objsUpdateHookLane struct {
	name    string
	queue   *objsUpdateHookEventQueue
	workers sync.WaitGroup
	debug   *objsUpdateHookBatchDebug
}

func newObjsUpdateHookLane(ctx context.Context, hook ObjsUpdateHook, name string, concurrency int, debug *objsUpdateHookBatchDebug) *objsUpdateHookLane {
	lane := &objsUpdateHookLane{name: name, queue: newObjsUpdateHookEventQueue(), debug: debug}
	lane.workers.Add(concurrency)
	for workerID := range concurrency {
		go func(workerID int) {
			defer lane.workers.Done()
			for {
				event, ok := lane.queue.take()
				if !ok {
					return
				}
				started := lane.debug.hookStart(lane.name, workerID, event.parent)
				hookCtx := withObjsUpdateHookDebugTrace(ctx, ObjsUpdateHookDebugTrace{
					BatchID:  lane.debug.batchID,
					HookName: lane.name,
					WorkerID: workerID,
				})
				hook(hookCtx, event.parent, event.files)
				lane.debug.hookDone(lane.name, workerID, event.parent, started)
			}
		}(workerID)
	}
	return lane
}

func closeObjsUpdateHookLanes(lanes []*objsUpdateHookLane) {
	for _, lane := range lanes {
		lane.queue.close()
	}
	for _, lane := range lanes {
		lane.workers.Wait()
	}
}

func handleObjsUpdateHookWork(ctx context.Context, work objsUpdateHookWork, limiter *rate.Limiter) (objsUpdateHookEvent, []objsUpdateHookWork, error) {
	if work.rateLimited && limiter != nil {
		if err := limiter.Wait(ctx); err != nil {
			return objsUpdateHookEvent{}, nil, err
		}
	}
	target := work.target
	files, err := List(ctx, target.storage, target.dirPath, model.ListArgs{SkipHook: true})
	if err != nil {
		return objsUpdateHookEvent{}, nil, err
	}
	event := objsUpdateHookEvent{
		parent: utils.GetFullPath(target.storage.GetStorage().MountPath, target.dirPath),
		files:  files,
	}
	if !target.recursive {
		return event, nil, nil
	}
	children := make([]objsUpdateHookWork, 0)
	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		children = append(children, objsUpdateHookWork{
			target: objsUpdateHookTarget{
				storage:   target.storage,
				dirPath:   stdpath.Join(target.dirPath, file.GetName()),
				recursive: true,
			},
			rateLimited: true,
		})
	}
	return event, children, nil
}

func dispatchObjsUpdateHookTargets(ctx context.Context, targets []objsUpdateHookTarget, concurrency int, limiter *rate.Limiter) {
	hooks := append([]ObjsUpdateHook(nil), objsUpdateHooks...)
	limiterDescription := "disabled"
	if limiter != nil {
		limiterDescription = "enabled"
	}
	debug := newObjsUpdateHookBatchDebug(targets, concurrency, hooks, limiterDescription)
	defer debug.complete()
	lanes := make([]*objsUpdateHookLane, 0, len(hooks))
	for index, hook := range hooks {
		name := fmt.Sprintf("%d:%s", index, objsUpdateHookName(hook))
		lanes = append(lanes, newObjsUpdateHookLane(ctx, hook, name, concurrency, debug))
	}

	queue := newObjsUpdateHookWorkQueue(targets)
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for workerID := range concurrency {
		go func(workerID int) {
			defer workers.Done()
			for {
				work, ok := queue.take()
				if !ok {
					return
				}
				if work.firstAttempt.IsZero() {
					work.firstAttempt = time.Now()
				}
				debug.scanStart(workerID, work)
				scanStarted := time.Now()
				event, children, err := handleObjsUpdateHookWork(ctx, work, limiter)
				if err != nil {
					work.listAttempts++
					if delay, retry := objsUpdateHookListRetryDelay(work, err); retry {
						debug.scanError(workerID, work, err, true, delay)
						log.Warnf(
							"batch hook list failed for %s (attempt %d), retrying in %s: %v",
							work.target.dirPath,
							work.listAttempts,
							delay,
							err,
						)
						queue.retry(work, delay)
						continue
					}
					debug.scanError(workerID, work, err, false, 0)
					log.Errorf(
						"batch hook list permanently failed for %s after %d attempts: %v",
						work.target.dirPath,
						work.listAttempts,
						err,
					)
					queue.finish(work, nil)
					continue
				}
				debug.scanDone(workerID, work, len(event.files), len(children), time.Since(scanStarted))
				for _, lane := range lanes {
					debug.hookQueued(lane.name, event.parent)
					lane.queue.push(event)
				}
				queue.finish(work, children)
			}
		}(workerID)
	}
	workers.Wait()
	closeObjsUpdateHookLanes(lanes)
}

func dispatchObjsUpdateHookTargetsSequentially(ctx context.Context, targets []objsUpdateHookTarget) {
	for _, target := range targets {
		objsUpdateHook(ctx, target.storage, target.dirPath, target.recursive)
	}
}

// Dispatch launches a bounded set of fair, directory-level hook workers.
// Calling Dispatch more than once is safe; only the first call consumes the
// collected targets.
func (b *ObjsUpdateHookBatch) Dispatch(ctx context.Context) {
	if b == nil {
		return
	}
	ctx = context.WithoutCancel(ctx)
	b.mu.Lock()
	if b.dispatched {
		b.mu.Unlock()
		return
	}
	b.dispatched = true
	targets := append([]objsUpdateHookTarget(nil), b.targets...)
	b.mu.Unlock()
	if len(targets) == 0 || !needHandleObjsUpdateHook() {
		return
	}
	go func() {
		concurrency := min(objsUpdateHookBatchConcurrency(), len(targets))
		if concurrency == 1 || objsUpdateHookTargetsOverlap(targets) {
			dispatchObjsUpdateHookTargetsSequentially(ctx, targets)
			return
		}
		limiter, ok := objsUpdateHookBatchLimiter()
		if !ok {
			dispatchObjsUpdateHookTargetsSequentially(ctx, targets)
			return
		}
		dispatchObjsUpdateHookTargets(ctx, targets, concurrency, limiter)
	}()
}
