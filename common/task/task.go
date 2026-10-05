package task

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var errTaskPanicked = errors.New("task panicked")

type Task struct {
	Interval time.Duration
	Execute  func() error
	access   sync.Mutex
	running  bool
	stop     chan struct{}
}

// runExecute wraps Execute so a panic inside a periodic task is logged and
// returned as errTaskPanicked instead of crashing the whole process.
func (t *Task) runExecute() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("task panic recovered: %v\n%s", r, debug.Stack())
			err = fmt.Errorf("%w: %v", errTaskPanicked, r)
		}
	}()
	return t.Execute()
}

// SetInterval updates the tick interval; the loop re-reads it every
// iteration, so the change takes effect on the next tick. Re-Creating the
// task via Close+Start from inside Execute instead would leave the old
// goroutine running alongside the new one (double execution).
func (t *Task) SetInterval(d time.Duration) {
	t.access.Lock()
	t.Interval = d
	t.access.Unlock()
}

func (t *Task) Start(first bool) error {
	t.access.Lock()
	if t.running {
		t.access.Unlock()
		return nil
	}
	t.running = true
	t.stop = make(chan struct{})
	t.access.Unlock()

	go func() {
		if first {
			// A panicked first run must not kill the goroutine either: the
			// next tick retries, e.g. after the panel fixes its config.
			if err := t.runExecute(); err != nil && !errors.Is(err, errTaskPanicked) {
				t.access.Lock()
				t.running = false
				close(t.stop)
				t.access.Unlock()
				return
			}
		}

		for {
			t.access.Lock()
			interval := t.Interval
			t.access.Unlock()
			select {
			case <-time.After(interval):
			case <-t.stop:
				return
			}

			if err := t.runExecute(); err != nil {
				if errors.Is(err, errTaskPanicked) {
					// Keep the loop alive so a transient panic (bad panel
					// config, missing limiter during reload) degrades into a
					// logged error instead of a crash loop.
					continue
				}
				t.access.Lock()
				t.running = false
				close(t.stop)
				t.access.Unlock()
				return
			}
		}
	}()

	return nil
}

func (t *Task) Close() {
	t.access.Lock()
	if t.running {
		t.running = false
		close(t.stop)
	}
	t.access.Unlock()
}
