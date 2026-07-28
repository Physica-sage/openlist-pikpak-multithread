package strm

import (
	"bytes"
	"context"
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"
)

type lockedStrmHookLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedStrmHookLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedStrmHookLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestStrmBatchHookDebugLogsLocalObjectStages(t *testing.T) {
	t.Setenv("OPENLIST_BATCH_HOOK_DEBUG", "true")
	logger := log.StandardLogger()
	oldOutput := logger.Out
	oldFormatter := logger.Formatter
	oldLevel := logger.Level
	var output lockedStrmHookLogBuffer
	log.SetOutput(&output)
	log.SetFormatter(&log.TextFormatter{DisableTimestamp: true, DisableColors: true})
	log.SetLevel(log.InfoLevel)
	t.Cleanup(func() {
		log.SetOutput(oldOutput)
		log.SetFormatter(oldFormatter)
		log.SetLevel(oldLevel)
	})

	ctx := context.Background()
	finishHook := beginStrmHookDebug(ctx, "/mount/show", 1)
	finishLocal := beginStrmLocalDebug(ctx, "/local/show", 1)
	finishCleanup := beginStrmCleanupDebug(ctx, "/local/show")
	trace := newStrmObjectDebugTrace(ctx, "/local/show/subtitle.srt")
	trace.stage("link")
	trace.stage("range_read_compare")
	trace.done()
	finishCleanup()
	finishLocal()
	finishHook()

	got := output.String()
	for _, want := range []string{
		"[strm-hook]",
		"event=hook_enter",
		"event=local_sync_start",
		"event=object_start",
		"stage=link",
		"stage=range_read_compare",
		"event=object_done",
		"event=local_cleanup_start",
		"event=local_cleanup_done",
		"event=local_sync_done",
		"event=hook_exit",
	} {
		if !bytes.Contains([]byte(got), []byte(want)) {
			t.Fatalf("STRM debug logs do not contain %q:\n%s", want, got)
		}
	}
}
