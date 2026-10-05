package hy2

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/common/counter"
)

func newTestHysteria2(minTraffic int64) (*Hysteria2, *HookServer) {
	hook := &HookServer{Tag: "t", ReportMinTrafficBytes: minTraffic}
	h := &Hysteria2{
		Hy2nodes: map[string]Hysteria2node{
			"t": {TrafficLogger: hook},
		},
		Auth: &V2bX{usersMap: map[string]int{"u1": 1}},
	}
	return h, hook
}

func seed(hook *HookServer, uuid string, up, down int64) {
	var tc *counter.TrafficCounter
	if v, ok := hook.Counter.Load("t"); ok {
		tc = v.(*counter.TrafficCounter)
	} else {
		tc = counter.NewTrafficCounter()
		hook.Counter.Store("t", tc)
	}
	tc.Tx(uuid, int(up))
	tc.Rx(uuid, int(down))
}

func rawCount(t *testing.T, hook *HookServer, uuid string) (int64, int64) {
	t.Helper()
	v, ok := hook.Counter.Load("t")
	if !ok {
		return 0, 0
	}
	cts, ok := v.(*counter.TrafficCounter).Counters.Load(uuid)
	if !ok {
		return 0, 0
	}
	ts := cts.(*counter.TrafficStorage)
	return ts.UpCounter.Load(), ts.DownCounter.Load()
}

// GetUserTrafficSlice must drain the counters atomically and drop counters of
// users that are no longer registered.
func TestGetUserTrafficSliceDrainsAndDropsUnknown(t *testing.T) {
	h, hook := newTestHysteria2(0)
	seed(hook, "u1", 100, 40)
	seed(hook, "ghost", 500, 0)

	traffic, err := h.GetUserTrafficSlice("t", true)
	if err != nil {
		t.Fatalf("GetUserTrafficSlice: %s", err)
	}
	var found bool
	for _, tr := range traffic {
		if tr.UID == 1 {
			found = true
			if tr.Upload != 100 || tr.Download != 40 {
				t.Fatalf("traffic for u1 = %d/%d, want 100/40", tr.Upload, tr.Download)
			}
		} else {
			t.Fatalf("unexpected traffic entry for uid %d", tr.UID)
		}
	}
	if !found {
		t.Fatal("traffic for u1 missing from drained slice")
	}
	if up, down := rawCount(t, hook, "u1"); up != 0 || down != 0 {
		t.Fatalf("counters not drained: up=%d down=%d", up, down)
	}
	if v, ok := hook.Counter.Load("t"); ok {
		tc := v.(*counter.TrafficCounter)
		if _, exists := tc.Counters.Load("ghost"); exists {
			t.Fatal("counter of removed user was not dropped")
		}
		if _, exists := tc.Counters.Load("u1"); !exists {
			t.Fatal("counter of active user must be kept after drain")
		}
	}
}

// Traffic below the reporting threshold must stay in the counters and keep
// accumulating instead of being reported or erased.
func TestGetUserTrafficSliceKeepsTrafficBelowThreshold(t *testing.T) {
	h, hook := newTestHysteria2(50)
	seed(hook, "u1", 30, 10)

	traffic, err := h.GetUserTrafficSlice("t", true)
	if err != nil {
		t.Fatalf("GetUserTrafficSlice: %s", err)
	}
	if len(traffic) != 0 {
		t.Fatalf("traffic below threshold was reported: %+v", traffic)
	}
	if up, down := rawCount(t, hook, "u1"); up != 30 || down != 10 {
		t.Fatalf("below-threshold traffic not preserved: up=%d down=%d, want 30/10", up, down)
	}
}

// RestoreUserTraffic must put drained-but-unacknowledged bytes back so a
// failed report does not lose traffic.
func TestRestoreUserTrafficAddsBack(t *testing.T) {
	h, hook := newTestHysteria2(0)

	if err := h.RestoreUserTraffic("t", []panel.UserTraffic{{UID: 1, Upload: 100, Download: 40}}); err != nil {
		t.Fatalf("RestoreUserTraffic: %s", err)
	}
	if up, down := rawCount(t, hook, "u1"); up != 100 || down != 40 {
		t.Fatalf("restored counters = %d/%d, want 100/40", up, down)
	}

	// traffic that arrived between drain and restore must not be overwritten:
	// restore adds back the drained bytes on top of them
	seed(hook, "u1", 10, 5)
	if err := h.RestoreUserTraffic("t", []panel.UserTraffic{{UID: 1, Upload: 100, Download: 40}}); err != nil {
		t.Fatalf("RestoreUserTraffic: %s", err)
	}
	if up, down := rawCount(t, hook, "u1"); up != 210 || down != 85 {
		t.Fatalf("restored counters = %d/%d, want 210/85 (additive, new traffic preserved)", up, down)
	}

	// unknown users must not resurrect stale counters
	if err := h.RestoreUserTraffic("t", []panel.UserTraffic{{UID: 99, Upload: 1, Download: 1}}); err != nil {
		t.Fatalf("RestoreUserTraffic: %s", err)
	}
	if v, ok := hook.Counter.Load("t"); ok {
		if _, exists := v.(*counter.TrafficCounter).Counters.Load("ghost"); exists {
			t.Fatal("counter created for unknown user")
		}
	}
}

// concurrent drain and accumulate must not lose or duplicate bytes: the sum
// of everything drained plus what remains must equal what was written
func TestConcurrentDrainAndAdd(t *testing.T) {
	h, hook := newTestHysteria2(0)
	seed(hook, "u1", 0, 0)

	var drained atomic.Int64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			v, _ := hook.Counter.Load("t")
			v.(*counter.TrafficCounter).Tx("u1", 1)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			traffic, _ := h.GetUserTrafficSlice("t", true)
			for _, tr := range traffic {
				drained.Add(tr.Upload + tr.Download)
			}
		}
	}()
	wg.Wait()

	// collect whatever is still in the counters after the concurrent phase
	total := drained.Load()
	for i := 0; i < 5; i++ {
		traffic, _ := h.GetUserTrafficSlice("t", true)
		for _, tr := range traffic {
			total += tr.Upload + tr.Download
		}
	}
	up, down := rawCount(t, hook, "u1")
	if total+up+down != 200 {
		t.Fatalf("traffic not conserved: drained=%d remaining=%d/%d, want sum 200", total, up, down)
	}
}
