package op

import (
	"context"
	"fmt"
	"os"
	stdpath "path"
	"strconv"
	"strings"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"golang.org/x/time/rate"
)

const (
	objsUpdateHookBatchConcurrencyEnv = "OPENLIST_BATCH_HOOK_CONCURRENCY"
	defaultObjsUpdateHookConcurrency  = 2
	maxObjsUpdateHookConcurrency      = 4
)

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
	target      objsUpdateHookTarget
	rateLimited bool
}

type objsUpdateHookWorkQueue struct {
	mu      sync.Mutex
	ready   *sync.Cond
	items   []objsUpdateHookWork
	pending int
}

func newObjsUpdateHookWorkQueue(targets []objsUpdateHookTarget) *objsUpdateHookWorkQueue {
	queue := &objsUpdateHookWorkQueue{
		items:   make([]objsUpdateHookWork, 0, len(targets)),
		pending: len(targets),
	}
	queue.ready = sync.NewCond(&queue.mu)
	for _, target := range targets {
		queue.items = append(queue.items, objsUpdateHookWork{target: target})
	}
	return queue
}

func (q *objsUpdateHookWorkQueue) take() (objsUpdateHookWork, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && q.pending > 0 {
		q.ready.Wait()
	}
	if q.pending == 0 {
		return objsUpdateHookWork{}, false
	}
	work := q.items[0]
	q.items = q.items[1:]
	return work, true
}

func (q *objsUpdateHookWorkQueue) finish(children []objsUpdateHookWork) {
	q.mu.Lock()
	q.pending += len(children) - 1
	q.items = append(q.items, children...)
	q.ready.Broadcast()
	q.mu.Unlock()
}

func handleObjsUpdateHookWork(ctx context.Context, work objsUpdateHookWork, limiter *rate.Limiter) []objsUpdateHookWork {
	if work.rateLimited && limiter != nil {
		if err := limiter.Wait(ctx); err != nil {
			return nil
		}
	}
	target := work.target
	files, err := List(ctx, target.storage, target.dirPath, model.ListArgs{SkipHook: true})
	if err != nil {
		return nil
	}
	HandleObjsUpdateHook(ctx, utils.GetFullPath(target.storage.GetStorage().MountPath, target.dirPath), files)
	if !target.recursive {
		return nil
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
	return children
}

func dispatchObjsUpdateHookTargets(ctx context.Context, targets []objsUpdateHookTarget, concurrency int, limiter *rate.Limiter) {
	queue := newObjsUpdateHookWorkQueue(targets)
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for range concurrency {
		go func() {
			defer workers.Done()
			for {
				work, ok := queue.take()
				if !ok {
					return
				}
				queue.finish(handleObjsUpdateHookWork(ctx, work, limiter))
			}
		}()
	}
	workers.Wait()
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
