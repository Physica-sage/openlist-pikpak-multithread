package strm

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/op"
	log "github.com/sirupsen/logrus"
)

const strmBatchHookDebugEnv = "OPENLIST_BATCH_HOOK_DEBUG"

func strmBatchHookDebugEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(strmBatchHookDebugEnv))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func strmHookDebugEntry(ctx context.Context) *log.Entry {
	fields := log.Fields{}
	if trace, ok := op.GetObjsUpdateHookDebugTrace(ctx); ok {
		fields["batch"] = trace.BatchID
		fields["hook"] = trace.HookName
		fields["worker"] = trace.WorkerID
	}
	return log.WithFields(fields)
}

func beginStrmHookDebug(ctx context.Context, path string, objectCount int) func() {
	if !strmBatchHookDebugEnabled() {
		return func() {}
	}
	started := time.Now()
	entry := strmHookDebugEntry(ctx).WithFields(log.Fields{
		"event":   "hook_enter",
		"objects": objectCount,
		"path":    path,
	})
	entry.Info("[strm-hook]")
	return func() {
		strmHookDebugEntry(ctx).WithFields(log.Fields{
			"duration_ms": time.Since(started).Milliseconds(),
			"event":       "hook_exit",
			"objects":     objectCount,
			"path":        path,
		}).Info("[strm-hook]")
	}
}

func beginStrmLocalDebug(ctx context.Context, localPath string, objectCount int) func() {
	if !strmBatchHookDebugEnabled() {
		return func() {}
	}
	started := time.Now()
	strmHookDebugEntry(ctx).WithFields(log.Fields{
		"event":      "local_sync_start",
		"local_path": localPath,
		"objects":    objectCount,
	}).Info("[strm-hook]")
	return func() {
		strmHookDebugEntry(ctx).WithFields(log.Fields{
			"duration_ms": time.Since(started).Milliseconds(),
			"event":       "local_sync_done",
			"local_path":  localPath,
			"objects":     objectCount,
		}).Info("[strm-hook]")
	}
}

func beginStrmCleanupDebug(ctx context.Context, localPath string) func() {
	if !strmBatchHookDebugEnabled() {
		return func() {}
	}
	started := time.Now()
	strmHookDebugEntry(ctx).WithFields(log.Fields{
		"event":      "local_cleanup_start",
		"local_path": localPath,
	}).Info("[strm-hook]")
	return func() {
		strmHookDebugEntry(ctx).WithFields(log.Fields{
			"duration_ms": time.Since(started).Milliseconds(),
			"event":       "local_cleanup_done",
			"local_path":  localPath,
		}).Info("[strm-hook]")
	}
}

type strmObjectDebugTrace struct {
	enabled      bool
	ctx          context.Context
	localPath    string
	started      time.Time
	currentStage string
	stageStarted time.Time
	doneLogged   bool
}

func newStrmObjectDebugTrace(ctx context.Context, localPath string) *strmObjectDebugTrace {
	trace := &strmObjectDebugTrace{enabled: strmBatchHookDebugEnabled()}
	if !trace.enabled {
		return trace
	}
	trace.ctx = ctx
	trace.localPath = localPath
	trace.started = time.Now()
	trace.stageStarted = trace.started
	trace.currentStage = "start"
	strmHookDebugEntry(ctx).WithFields(log.Fields{
		"event":      "object_start",
		"local_path": localPath,
	}).Info("[strm-hook]")
	return trace
}

func (t *strmObjectDebugTrace) stage(stage string) {
	if t == nil || !t.enabled {
		return
	}
	now := time.Now()
	strmHookDebugEntry(t.ctx).WithFields(log.Fields{
		"event":             "object_stage",
		"local_path":        t.localPath,
		"previous_stage":    t.currentStage,
		"previous_stage_ms": now.Sub(t.stageStarted).Milliseconds(),
		"stage":             stage,
	}).Info("[strm-hook]")
	t.currentStage = stage
	t.stageStarted = now
}

func (t *strmObjectDebugTrace) done() {
	if t == nil || !t.enabled || t.doneLogged {
		return
	}
	t.doneLogged = true
	now := time.Now()
	strmHookDebugEntry(t.ctx).WithFields(log.Fields{
		"duration_ms":   now.Sub(t.started).Milliseconds(),
		"event":         "object_done",
		"last_stage":    t.currentStage,
		"last_stage_ms": now.Sub(t.stageStarted).Milliseconds(),
		"local_path":    t.localPath,
	}).Info("[strm-hook]")
}
