package op

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

const objsUpdateHookBatchDebugEnv = "OPENLIST_BATCH_HOOK_DEBUG"

var (
	objsUpdateHookBatchDebugSequence      atomic.Uint64
	objsUpdateHookBatchDebugStallInterval = 30 * time.Second
)

type objsUpdateHookDebugContextKey struct{}

// ObjsUpdateHookDebugTrace correlates driver-specific diagnostics with a batch
// hook worker. It is present only for hooks dispatched by this batch scheduler.
type ObjsUpdateHookDebugTrace struct {
	BatchID  uint64
	HookName string
	WorkerID int
}

// GetObjsUpdateHookDebugTrace returns the current batch-hook correlation data.
func GetObjsUpdateHookDebugTrace(ctx context.Context) (ObjsUpdateHookDebugTrace, bool) {
	trace, ok := ctx.Value(objsUpdateHookDebugContextKey{}).(ObjsUpdateHookDebugTrace)
	return trace, ok
}

func withObjsUpdateHookDebugTrace(ctx context.Context, trace ObjsUpdateHookDebugTrace) context.Context {
	return context.WithValue(ctx, objsUpdateHookDebugContextKey{}, trace)
}

func objsUpdateHookBatchDebugEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(objsUpdateHookBatchDebugEnv))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func objsUpdateHookName(hook ObjsUpdateHook) string {
	function := runtime.FuncForPC(reflect.ValueOf(hook).Pointer())
	if function == nil {
		return "unknown"
	}
	return function.Name()
}

type objsUpdateHookDebugTask struct {
	path    string
	started time.Time
}

type objsUpdateHookDebugLane struct {
	queued    int
	active    map[int]objsUpdateHookDebugTask
	completed int
}

type objsUpdateHookBatchDebug struct {
	enabled bool
	batchID uint64
	started time.Time

	mu              sync.Mutex
	scanners        map[int]objsUpdateHookDebugTask
	scanned         int
	scanRetries     int
	scanFailures    int
	lanes           map[string]*objsUpdateHookDebugLane
	watchdogStop    chan struct{}
	watchdogDone    chan struct{}
	watchdogStopOne sync.Once
}

func newObjsUpdateHookBatchDebug(targets []objsUpdateHookTarget, concurrency int, hooks []ObjsUpdateHook, limiterDescription string) *objsUpdateHookBatchDebug {
	d := &objsUpdateHookBatchDebug{enabled: objsUpdateHookBatchDebugEnabled()}
	if !d.enabled {
		return d
	}
	d.batchID = objsUpdateHookBatchDebugSequence.Add(1)
	d.started = time.Now()
	d.scanners = make(map[int]objsUpdateHookDebugTask, concurrency)
	d.lanes = make(map[string]*objsUpdateHookDebugLane, len(hooks))
	d.watchdogStop = make(chan struct{})
	d.watchdogDone = make(chan struct{})
	hookNames := make([]string, 0, len(hooks))
	for index, hook := range hooks {
		name := fmt.Sprintf("%d:%s", index, objsUpdateHookName(hook))
		hookNames = append(hookNames, name)
		d.lanes[name] = &objsUpdateHookDebugLane{active: make(map[int]objsUpdateHookDebugTask, concurrency)}
	}
	fields := log.Fields{
		"batch":       d.batchID,
		"concurrency": concurrency,
		"event":       "batch_start",
		"hooks":       strings.Join(hookNames, ","),
		"targets":     len(targets),
	}
	fields["rate_limiter"] = limiterDescription
	d.info(fields)
	for index, target := range targets {
		d.info(log.Fields{
			"event":     "target_collected",
			"index":     index,
			"path":      target.dirPath,
			"recursive": target.recursive,
			"storage":   target.storage.GetStorage().MountPath,
		})
	}
	go d.watchdog()
	return d
}

func (d *objsUpdateHookBatchDebug) info(fields log.Fields) {
	if d == nil || !d.enabled {
		return
	}
	if _, ok := fields["batch"]; !ok {
		fields["batch"] = d.batchID
	}
	log.WithFields(fields).Info("[batch-hook]")
}

func (d *objsUpdateHookBatchDebug) scanStart(worker int, work objsUpdateHookWork) {
	if d == nil || !d.enabled {
		return
	}
	d.mu.Lock()
	d.scanners[worker] = objsUpdateHookDebugTask{path: work.target.dirPath, started: time.Now()}
	d.mu.Unlock()
	d.info(log.Fields{
		"attempt":   work.listAttempts + 1,
		"event":     "scan_start",
		"path":      work.target.dirPath,
		"top_level": work.topLevel,
		"worker":    worker,
	})
}

func (d *objsUpdateHookBatchDebug) scanDone(worker int, work objsUpdateHookWork, fileCount int, childCount int, duration time.Duration) {
	if d == nil || !d.enabled {
		return
	}
	d.mu.Lock()
	delete(d.scanners, worker)
	d.scanned++
	d.mu.Unlock()
	d.info(log.Fields{
		"children":    childCount,
		"duration_ms": duration.Milliseconds(),
		"event":       "scan_done",
		"objects":     fileCount,
		"path":        work.target.dirPath,
		"worker":      worker,
	})
}

func (d *objsUpdateHookBatchDebug) scanError(worker int, work objsUpdateHookWork, err error, retrying bool, delay time.Duration) {
	if d == nil || !d.enabled {
		return
	}
	d.mu.Lock()
	delete(d.scanners, worker)
	if retrying {
		d.scanRetries++
	} else {
		d.scanFailures++
	}
	d.mu.Unlock()
	fields := log.Fields{
		"attempt": work.listAttempts,
		"error":   err,
		"event":   "scan_failed",
		"path":    work.target.dirPath,
		"retry":   retrying,
		"worker":  worker,
	}
	if retrying {
		fields["retry_delay_ms"] = delay.Milliseconds()
	}
	d.info(fields)
}

func (d *objsUpdateHookBatchDebug) hookQueued(lane string, parent string) {
	if d == nil || !d.enabled {
		return
	}
	d.mu.Lock()
	stats := d.lanes[lane]
	stats.queued++
	queued := stats.queued
	active := len(stats.active)
	d.mu.Unlock()
	d.info(log.Fields{
		"active": active,
		"event":  "hook_queued",
		"hook":   lane,
		"parent": parent,
		"queued": queued,
	})
}

func (d *objsUpdateHookBatchDebug) hookStart(lane string, worker int, parent string) time.Time {
	started := time.Now()
	if d == nil || !d.enabled {
		return started
	}
	d.mu.Lock()
	stats := d.lanes[lane]
	if stats.queued > 0 {
		stats.queued--
	}
	stats.active[worker] = objsUpdateHookDebugTask{path: parent, started: started}
	queued := stats.queued
	active := len(stats.active)
	d.mu.Unlock()
	d.info(log.Fields{
		"active": active,
		"event":  "hook_start",
		"hook":   lane,
		"parent": parent,
		"queued": queued,
		"worker": worker,
	})
	return started
}

func (d *objsUpdateHookBatchDebug) hookDone(lane string, worker int, parent string, started time.Time) {
	if d == nil || !d.enabled {
		return
	}
	d.mu.Lock()
	stats := d.lanes[lane]
	delete(stats.active, worker)
	stats.completed++
	queued := stats.queued
	active := len(stats.active)
	completed := stats.completed
	d.mu.Unlock()
	d.info(log.Fields{
		"active":      active,
		"completed":   completed,
		"duration_ms": time.Since(started).Milliseconds(),
		"event":       "hook_done",
		"hook":        lane,
		"parent":      parent,
		"queued":      queued,
		"worker":      worker,
	})
}

func (d *objsUpdateHookBatchDebug) watchdog() {
	defer close(d.watchdogDone)
	interval := objsUpdateHookBatchDebugStallInterval
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.logWatchdog()
		case <-d.watchdogStop:
			return
		}
	}
}

func formatObjsUpdateHookDebugTasks(tasks map[int]objsUpdateHookDebugTask, now time.Time) string {
	workers := make([]int, 0, len(tasks))
	for worker := range tasks {
		workers = append(workers, worker)
	}
	sort.Ints(workers)
	parts := make([]string, 0, len(workers))
	for _, worker := range workers {
		task := tasks[worker]
		parts = append(parts, fmt.Sprintf("worker=%d path=%q age=%s", worker, task.path, now.Sub(task.started).Round(time.Millisecond)))
	}
	return strings.Join(parts, "; ")
}

func (d *objsUpdateHookBatchDebug) logWatchdog() {
	if d == nil || !d.enabled {
		return
	}
	now := time.Now()
	d.mu.Lock()
	scanners := make(map[int]objsUpdateHookDebugTask, len(d.scanners))
	for worker, task := range d.scanners {
		scanners[worker] = task
	}
	type laneSnapshot struct {
		name      string
		queued    int
		active    map[int]objsUpdateHookDebugTask
		completed int
	}
	lanes := make([]laneSnapshot, 0, len(d.lanes))
	for name, stats := range d.lanes {
		active := make(map[int]objsUpdateHookDebugTask, len(stats.active))
		for worker, task := range stats.active {
			active[worker] = task
		}
		lanes = append(lanes, laneSnapshot{name: name, queued: stats.queued, active: active, completed: stats.completed})
	}
	scanned := d.scanned
	retries := d.scanRetries
	failures := d.scanFailures
	d.mu.Unlock()
	sort.Slice(lanes, func(i, j int) bool { return lanes[i].name < lanes[j].name })
	d.info(log.Fields{
		"active_tasks": formatObjsUpdateHookDebugTasks(scanners, now),
		"active":       len(scanners),
		"elapsed_ms":   now.Sub(d.started).Milliseconds(),
		"event":        "scanner_watchdog",
		"failed":       failures,
		"retries":      retries,
		"scanned":      scanned,
	})
	for _, lane := range lanes {
		d.info(log.Fields{
			"active_tasks": formatObjsUpdateHookDebugTasks(lane.active, now),
			"active":       len(lane.active),
			"completed":    lane.completed,
			"elapsed_ms":   now.Sub(d.started).Milliseconds(),
			"event":        "hook_watchdog",
			"hook":         lane.name,
			"queued":       lane.queued,
		})
	}
}

func (d *objsUpdateHookBatchDebug) complete() {
	if d == nil || !d.enabled {
		return
	}
	d.watchdogStopOne.Do(func() { close(d.watchdogStop) })
	<-d.watchdogDone
	d.mu.Lock()
	scanned := d.scanned
	retries := d.scanRetries
	failures := d.scanFailures
	d.mu.Unlock()
	d.info(log.Fields{
		"duration_ms": time.Since(d.started).Milliseconds(),
		"event":       "batch_complete",
		"failed":      failures,
		"retries":     retries,
		"scanned":     scanned,
	})
}
