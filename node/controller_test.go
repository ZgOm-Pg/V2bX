package node

import (
	"encoding/json"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/conf"
	vCore "github.com/InazumaV/V2bX/core"
	"github.com/InazumaV/V2bX/limiter"
)

func TestMain(m *testing.M) {
	limiter.Init()
	os.Exit(m.Run())
}

// chainChild is the child core behind the real Selector used by
// TestRollbackThroughSelectorCleansPartialNode.
var chainChild *fakeCore

func init() {
	vCore.RegisterCore("faketest", func(c *conf.CoreConfig) (vCore.Core, error) {
		if chainChild == nil {
			chainChild = newFakeCore()
		}
		return chainChild, nil
	})
}

// ---- fake core ----

type fakeCore struct {
	mu               sync.Mutex
	nodes            map[string]*panel.NodeInfo
	users            map[string]map[string]bool
	failAddUsersPort int
	failAddNodePort  int
	traffic          map[string]int64
	restored         int
	addUsersCalls    int
}

func newFakeCore() *fakeCore {
	return &fakeCore{
		nodes:   map[string]*panel.NodeInfo{},
		users:   map[string]map[string]bool{},
		traffic: map[string]int64{},
	}
}

func (f *fakeCore) Start() error    { return nil }
func (f *fakeCore) Close() error    { return nil }
func (f *fakeCore) Type() string    { return "fake" }
func (f *fakeCore) Protocols() []string { return []string{"shadowsocks"} }

func (f *fakeCore) AddNode(tag string, info *panel.NodeInfo, config *conf.Options) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAddNodePort != 0 && info != nil && info.Common != nil &&
		info.Common.ServerPort == f.failAddNodePort {
		// simulate a partially created node before failing (xray adds the
		// inbound and then fails on the outbound)
		f.nodes[tag] = info
		return fmt.Errorf("simulated add node failure for port %d", f.failAddNodePort)
	}
	if _, exists := f.nodes[tag]; exists {
		return fmt.Errorf("duplicate inbound: %s", tag)
	}
	f.nodes[tag] = info
	return nil
}

func (f *fakeCore) DelNode(tag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.nodes[tag]; !exists {
		return fmt.Errorf("node not found: %s", tag)
	}
	delete(f.nodes, tag)
	return nil
}

func (f *fakeCore) AddUsers(p *vCore.AddUsersParams) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addUsersCalls++
	if f.failAddUsersPort != 0 && p.NodeInfo != nil && p.NodeInfo.Common != nil &&
		p.NodeInfo.Common.ServerPort == f.failAddUsersPort {
		return 0, fmt.Errorf("simulated add users failure for port %d", f.failAddUsersPort)
	}
	if f.users[p.Tag] == nil {
		f.users[p.Tag] = map[string]bool{}
	}
	for _, u := range p.Users {
		f.users[p.Tag][u.Uuid] = true
	}
	return len(p.Users), nil
}

func (f *fakeCore) DelUsers(users []panel.UserInfo, tag string, info *panel.NodeInfo) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range users {
		delete(f.users[tag], u.Uuid)
	}
	return nil
}

func (f *fakeCore) GetUserTrafficSlice(tag string, reset bool) ([]panel.UserTraffic, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	up := f.traffic[tag]
	if reset {
		f.traffic[tag] = 0
	}
	if up == 0 {
		return nil, nil
	}
	return []panel.UserTraffic{{UID: 1, Upload: up, Download: 0}}, nil
}

func (f *fakeCore) RestoreUserTraffic(tag string, traffic []panel.UserTraffic) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range traffic {
		f.traffic[tag] += t.Upload + t.Download
		f.restored++
	}
	return nil
}

func (f *fakeCore) userCount(tag string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.users[tag])
}

func (f *fakeCore) hasUser(tag, uuid string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.users[tag][uuid]
}

func (f *fakeCore) addUsersCallsSnapshot() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.addUsersCalls
}

func (f *fakeCore) nodePort(tag string) (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.nodes[tag]
	if !ok || n.Common == nil {
		return 0, false
	}
	return n.Common.ServerPort, true
}

func (f *fakeCore) pendingTraffic(tag string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.traffic[tag]
}

// ---- fake panel (httptest) ----

const oneUserJSON = `{"users":[{"id":1,"uuid":"u1","speed_limit":0,"device_limit":0}]}`

type fakePanel struct {
	mu         sync.Mutex
	nodePort   int
	usersJSON  string
	userFail   bool
	pushStatus int
	// pushAcceptDrop simulates a panel that has already accounted the
	// reported bytes but whose response never reaches the client: the
	// request is read, the payload accumulated, and the connection is
	// closed without any HTTP response.
	pushAcceptDrop    bool
	pushRequests      int32
	pushBytesAccepted int64
	srv               *httptest.Server
}

func newFakePanel(t *testing.T) *fakePanel {
	fp := &fakePanel{
		nodePort:   12344,
		usersJSON:  oneUserJSON,
		pushStatus: http.StatusOK,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/server/UniProxy/config", func(w http.ResponseWriter, r *http.Request) {
		fp.mu.Lock()
		port := fp.nodePort
		fp.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"server_port":%d,"cipher":"aes-128-gcm","server_key":"testkey","base_config":{"push_interval":3600,"pull_interval":3600},"routes":[]}`, port)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/user", func(w http.ResponseWriter, r *http.Request) {
		fp.mu.Lock()
		body, fail := fp.usersJSON, fp.userFail
		fp.mu.Unlock()
		if fail {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"%x"`, sha256.Sum256([]byte(body))))
		if r.Header.Get("If-None-Match") == w.Header().Get("ETag") {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/push", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fp.pushRequests, 1)
		fp.mu.Lock()
		st, drop := fp.pushStatus, fp.pushAcceptDrop
		fp.mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		var reported map[string][]int64
		_ = json.Unmarshal(body, &reported)
		var sum int64
		for _, v := range reported {
			if len(v) == 2 {
				sum += v[0] + v[1]
			}
		}
		atomic.AddInt64(&fp.pushBytesAccepted, sum)
		if drop {
			// accepted-and-accounted, but the response is lost
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, _ := hj.Hijack()
				conn.Close()
				return
			}
		}
		w.WriteHeader(st)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/alivelist", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"alive":{}}`)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/alive", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	fp.srv = httptest.NewServer(mux)
	t.Cleanup(fp.srv.Close)
	return fp
}

func (fp *fakePanel) setUsers(body string) {
	fp.mu.Lock()
	fp.usersJSON = body
	fp.mu.Unlock()
}

func (fp *fakePanel) setNodePort(port int) {
	fp.mu.Lock()
	fp.nodePort = port
	fp.mu.Unlock()
}

// ---- controller assembly ----

func newTestController(t *testing.T, fp *fakePanel, server vCore.Core, opts func(*conf.Options)) *Controller {
	t.Helper()
	api, err := panel.New(&conf.ApiConfig{
		APIHost:  fp.srv.URL,
		NodeID:   1,
		Key:      "testkey",
		NodeType: "shadowsocks",
	})
	if err != nil {
		t.Fatalf("panel.New: %s", err)
	}
	o := &conf.Options{ReportMinTraffic: 0}
	if opts != nil {
		opts(o)
	}
	c := NewController(server, api, o)
	if err := c.Start(); err != nil {
		t.Fatalf("controller start: %s", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func waitFor(t *testing.T, timeout time.Duration, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func accumulatedTraffic(c *Controller, uuid string) int64 {
	c.trafficMu.Lock()
	defer c.trafficMu.Unlock()
	return c.traffic[uuid]
}

// ---- problem 1: dynamic speed limit task ----

func TestDynamicSpeedLimitTaskStartedAndTriggers(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	c := newTestController(t, fp, core, func(o *conf.Options) {
		o.LimitConfig.EnableDynamicSpeedLimit = true
		o.LimitConfig.DynamicSpeedLimitConfig = &conf.DynamicSpeedLimitConfig{
			Periodic: 1, Traffic: 50, SpeedLimit: 2048, ExpireTime: 60,
		}
	})
	if c.dynamicSpeedLimitPeriodic == nil {
		t.Fatal("dynamic speed limit task was not created")
	}
	core.mu.Lock()
	core.traffic[c.tag] = 100
	core.mu.Unlock()
	if err := c.reportUserTrafficTask(); err != nil {
		t.Fatalf("reportUserTrafficTask: %s", err)
	}
	waitFor(t, 4*time.Second, "dynamic speed limit to be applied", func() bool {
		l, err := limiter.GetLimiter(c.tag)
		if err != nil {
			return false
		}
		v, ok := l.UserLimitInfo.Load(c.tag + "|u1")
		if !ok {
			return false
		}
		dyn, _ := v.(*limiter.UserLimitInfo).DynamicState()
		return dyn == 2048
	})
}

func TestDynamicSpeedLimitTaskNotStartedWhenDisabled(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	c := newTestController(t, fp, core, nil)
	if c.dynamicSpeedLimitPeriodic != nil {
		t.Fatal("dynamic speed limit task must not be created when disabled")
	}
}

func TestCloseStopsDynamicSpeedLimitTask(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	c := newTestController(t, fp, core, func(o *conf.Options) {
		o.LimitConfig.EnableDynamicSpeedLimit = true
		o.LimitConfig.DynamicSpeedLimitConfig = &conf.DynamicSpeedLimitConfig{
			Periodic: 1, Traffic: 50, SpeedLimit: 2048, ExpireTime: 60,
		}
	})
	tag := c.tag
	if err := c.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}
	if _, err := limiter.GetLimiter(tag); err == nil {
		t.Fatal("limiter not removed on close")
	}
	if _, ok := core.nodePort(tag); ok {
		t.Fatal("node not removed from core on close")
	}
}

// ---- problem 2: failed report must not double count dynamic traffic ----

func TestFailedReportDoesNotDoubleCountDynamicTraffic(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	c := newTestController(t, fp, core, func(o *conf.Options) {
		o.LimitConfig.EnableDynamicSpeedLimit = true
		o.LimitConfig.DynamicSpeedLimitConfig = &conf.DynamicSpeedLimitConfig{
			Periodic: 3600, Traffic: 1 << 40, SpeedLimit: 2048, ExpireTime: 60,
		}
	})
	fp.mu.Lock()
	fp.pushStatus = http.StatusInternalServerError
	fp.mu.Unlock()
	core.mu.Lock()
	core.traffic[c.tag] = 100
	core.mu.Unlock()

	// two consecutive failed report cycles
	if err := c.reportUserTrafficTask(); err != nil {
		t.Fatalf("reportUserTrafficTask: %s", err)
	}
	if err := c.reportUserTrafficTask(); err != nil {
		t.Fatalf("reportUserTrafficTask: %s", err)
	}
	if got := accumulatedTraffic(c, "u1"); got != 0 {
		t.Fatalf("dynamic traffic accumulated %d bytes after two failed reports, want 0 (core still holds %d)", got, core.pendingTraffic(c.tag))
	}
	if got := core.pendingTraffic(c.tag); got != 100 {
		t.Fatalf("core traffic after failed reports = %d, want 100 (restore must keep the bytes)", got)
	}

	// a successful report counts the same real bytes exactly once
	fp.mu.Lock()
	fp.pushStatus = http.StatusOK
	fp.mu.Unlock()
	if err := c.reportUserTrafficTask(); err != nil {
		t.Fatalf("reportUserTrafficTask: %s", err)
	}
	if got := accumulatedTraffic(c, "u1"); got != 100 {
		t.Fatalf("dynamic traffic after successful report = %d, want 100", got)
	}
	if got := core.pendingTraffic(c.tag); got != 0 {
		t.Fatalf("core traffic after successful report = %d, want 0", got)
	}
}

// ---- problem 3: rollback must clean up the new node ----

func TestRollbackAfterAddUsersFailureSameTag(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	c := newTestController(t, fp, core, nil)
	core.mu.Lock()
	core.failAddUsersPort = 12345
	core.mu.Unlock()

	fp.setNodePort(12345)
	c.nodeInfoMonitor()

	port, ok := core.nodePort(c.tag)
	if !ok {
		t.Fatalf("no node in core after failed reload, tag %s", c.tag)
	}
	if port != 12344 {
		t.Fatalf("core serves port %d after rollback, want old port 12344", port)
	}
	if got := core.userCount(c.tag); got != 1 {
		t.Fatalf("users in core after rollback = %d, want 1", got)
	}
	if c.info == nil || c.info.Common == nil || c.info.Common.ServerPort != 12344 {
		t.Fatalf("controller info does not match rolled back node: %+v", c.info)
	}
}

// End-to-end rollback through a real Selector: when the child core's AddNode
// fails after partially creating the node, the Selector must clean the child
// up (the tag mapping is never stored) and the controller rollback must still
// restore the old node.
func TestRollbackThroughSelectorCleansPartialNode(t *testing.T) {
	fp := newFakePanel(t)
	chainChild = newFakeCore()
	sel, err := vCore.NewSelector([]conf.CoreConfig{{Type: "faketest", Name: "faketest"}})
	if err != nil {
		t.Fatalf("NewSelector: %s", err)
	}
	c := newTestController(t, fp, sel, func(o *conf.Options) {
		o.CoreName = "faketest"
		o.Core = "faketest"
	})
	chainChild.mu.Lock()
	chainChild.failAddNodePort = 12345
	chainChild.mu.Unlock()

	oldTag := c.tag
	fp.setNodePort(12345)
	c.nodeInfoMonitor()

	if chainChild.nodes == nil {
		t.Fatal("child core missing")
	}
	// the partially created new node must have been cleaned from the child
	chainChild.mu.Lock()
	_, leaked := chainChild.nodes[oldTag+"_"]
	leakedByPort := false
	for tag, n := range chainChild.nodes {
		if n != nil && n.Common != nil && n.Common.ServerPort == 12345 {
			leakedByPort = true
			_ = tag
		}
	}
	chainChild.mu.Unlock()
	if leaked || leakedByPort {
		t.Fatal("partially created new node left in child core after failed reload")
	}
	port, ok := chainChild.nodePort(oldTag)
	if !ok || port != 12344 {
		t.Fatalf("old node not restored through Selector (exists=%v port=%d)", ok, port)
	}
	if c.info == nil || c.info.Common == nil || c.info.Common.ServerPort != 12344 {
		t.Fatalf("controller info does not match rolled back node: %+v", c.info)
	}
}

func TestRollbackRemovesNewNodeDifferentTag(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	c := newTestController(t, fp, core, nil)

	oldTag := "old-node"
	newTag := "new-node"
	oldInfo := &panel.NodeInfo{Common: &panel.CommonNode{ServerPort: 12344}}
	newInfo := &panel.NodeInfo{Common: &panel.CommonNode{ServerPort: 12345}}
	oldUsers := []panel.UserInfo{{Id: 1, Uuid: "u1"}}

	if err := core.AddNode(oldTag, oldInfo, c.Options); err != nil {
		t.Fatal(err)
	}
	core.mu.Lock()
	core.users[oldTag] = map[string]bool{"u1": true}
	core.mu.Unlock()

	// Mirror the reload sequence: the old node has already been removed by
	// the time the new node is created and AddUsers fails on it.
	if err := core.DelNode(oldTag); err != nil {
		t.Fatal(err)
	}
	c.tag = newTag
	c.info = newInfo
	if err := core.AddNode(newTag, newInfo, c.Options); err != nil {
		t.Fatal(err)
	}

	if err := c.rollbackNodeReload(oldTag, oldInfo, oldUsers, true); err != nil {
		t.Fatalf("rollback reported failure: %s", err)
	}

	if _, ok := core.nodePort(newTag); ok {
		t.Fatal("new node left in core after rollback")
	}
	port, ok := core.nodePort(oldTag)
	if !ok || port != 12344 {
		t.Fatalf("old node not restored correctly (exists=%v port=%d)", ok, port)
	}
	if got := core.userCount(oldTag); got != 1 {
		t.Fatalf("old users not restored, count = %d", got)
	}
	if _, err := limiter.GetLimiter(newTag); err == nil {
		t.Fatal("limiter for new tag left behind after rollback")
	}
	if _, err := limiter.GetLimiter(oldTag); err != nil {
		t.Fatalf("limiter for old tag not restored: %s", err)
	}

	// a rollback that cannot restore the old node must report failure: the
	// old node from the previous rollback is still in the core, so re-adding
	// it collides (duplicate inbound) and the error must surface
	c.tag = newTag
	if err := core.AddNode(newTag, newInfo, c.Options); err != nil {
		t.Fatal(err)
	}
	if err := c.rollbackNodeReload(oldTag, oldInfo, oldUsers, true); err == nil {
		t.Fatal("rollback reported success although the old node could not be restored")
	}
}

// ---- problem 4: empty user list vs 304 vs request failure ----

func TestEmptyUserListClearsUsers(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	c := newTestController(t, fp, core, nil)

	fp.setUsers(`{"users":[]}`)
	c.nodeInfoMonitor()

	if got := core.userCount(c.tag); got != 0 {
		t.Fatalf("users in core after empty user list = %d, want 0", got)
	}
	if len(c.userList) != 0 {
		t.Fatalf("controller user list = %d, want 0", len(c.userList))
	}
}

func Test304KeepsUsers(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	c := newTestController(t, fp, core, nil)

	// same body as at Start() -> 304
	c.nodeInfoMonitor()

	if got := core.userCount(c.tag); got != 1 {
		t.Fatalf("users in core after 304 = %d, want 1", got)
	}
	if len(c.userList) != 1 {
		t.Fatalf("controller user list after 304 = %d, want 1", len(c.userList))
	}
}

func TestFailedUserRequestKeepsUsers(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	c := newTestController(t, fp, core, nil)

	fp.mu.Lock()
	fp.userFail = true
	fp.mu.Unlock()
	c.nodeInfoMonitor()

	if got := core.userCount(c.tag); got != 1 {
		t.Fatalf("users in core after failed request = %d, want 1", got)
	}
	if len(c.userList) != 1 {
		t.Fatalf("controller user list after failed request = %d, want 1", len(c.userList))
	}
}

func TestZeroUserNodeCanStart(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	fp.setUsers(`{"users":[]}`)
	// Start() is invoked by newTestController; it must tolerate zero users
	c := newTestController(t, fp, core, nil)
	if got := core.userCount(c.tag); got != 0 {
		t.Fatalf("users in core = %d, want 0", got)
	}
}

// ---- problem 1: report idempotency (documented reproduction, NOT fixed) ----

// Reproduces the double-accounting window: the panel accepts and accumulates
// the reported delta but the response never reaches the node. Both layers
// then duplicate the bytes — resty's automatic retry (same report re-sent)
// and the next reporting cycle (RestoreUserTraffic keeps the bytes so they
// are re-reported). This test pins the reproduction; a complete fix needs
// panel-side deduplication and cannot be implemented node-side alone.
func TestFailedReportDuplicatesPanelAccounting(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	c := newTestController(t, fp, core, func(o *conf.Options) {
		o.LimitConfig.EnableDynamicSpeedLimit = true
		o.LimitConfig.DynamicSpeedLimitConfig = &conf.DynamicSpeedLimitConfig{
			Periodic: 3600, Traffic: 1 << 40, SpeedLimit: 2048, ExpireTime: 60,
		}
	})
	fp.mu.Lock()
	fp.pushAcceptDrop = true
	fp.mu.Unlock()
	core.mu.Lock()
	core.traffic[c.tag] = 100 // exactly 100 real bytes pending
	core.mu.Unlock()

	// cycle 1: report dropped after acceptance — resty retries re-send it
	if err := c.reportUserTrafficTask(); err != nil {
		t.Fatalf("reportUserTrafficTask: %s", err)
	}
	reqs := atomic.LoadInt32(&fp.pushRequests)
	accepted := atomic.LoadInt64(&fp.pushBytesAccepted)
	if reqs < 2 {
		t.Fatalf("panel received %d requests, want >=2 (resty auto-retry layer)", reqs)
	}
	if accepted < 200 {
		t.Fatalf("panel accepted %d bytes for 100 real bytes, want >=200 (duplicate accounting)", accepted)
	}
	if got := core.pendingTraffic(c.tag); got != 100 {
		t.Fatalf("core pending traffic = %d, want 100 (restore must not lose bytes)", got)
	}

	// cycle 2: the same 100 bytes are reported again (next-cycle layer)
	if err := c.reportUserTrafficTask(); err != nil {
		t.Fatalf("reportUserTrafficTask: %s", err)
	}
	accepted2 := atomic.LoadInt64(&fp.pushBytesAccepted)
	if accepted2 < accepted+100 {
		t.Fatalf("panel total %d did not grow by >=100 on the next cycle", accepted2)
	}
	t.Logf("REPRO: 100 real bytes; panel accepted %d bytes over %d requests in 2 cycles", accepted2, atomic.LoadInt32(&fp.pushRequests))
}

// ---- problem 2: failed user apply must survive 304 ----

func TestAddFailureRetriedAfter304(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	c := newTestController(t, fp, core, nil)

	fp.setUsers(`{"users":[{"id":1,"uuid":"u1"},{"id":2,"uuid":"u2"}]}`)
	core.mu.Lock()
	core.failAddUsersPort = c.info.Common.ServerPort
	core.mu.Unlock()
	c.nodeInfoMonitor()
	if got := core.userCount(c.tag); got != 1 {
		t.Fatalf("users applied during failing add = %d, want 1", got)
	}

	// panel keeps returning 304 while the add keeps failing
	c.nodeInfoMonitor()
	c.nodeInfoMonitor()
	if got := core.userCount(c.tag); got != 1 {
		t.Fatalf("users applied across failing 304 cycles = %d, want 1", got)
	}

	// the blocker disappears: the pending users must be applied without any
	// new panel change
	core.mu.Lock()
	core.failAddUsersPort = 0
	core.mu.Unlock()
	c.nodeInfoMonitor()
	if got := core.userCount(c.tag); got != 2 {
		t.Fatalf("pending users not applied after recovery: %d, want 2", got)
	}
	if core.hasUser(c.tag, "u2") == false {
		t.Fatal("u2 missing from core after recovery")
	}
	l, err := limiter.GetLimiter(c.tag)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := l.UserLimitInfo.Load(c.tag + "|u2"); !ok {
		t.Fatal("limiter entry for u2 missing after recovery")
	}

	// once applied, no redundant AddUsers on further 304 cycles
	before := core.addUsersCallsSnapshot()
	c.nodeInfoMonitor()
	if after := core.addUsersCallsSnapshot(); after != before {
		t.Fatalf("redundant AddUsers after successful apply: %d -> %d", before, after)
	}
	if got := core.userCount(c.tag); got != 2 {
		t.Fatalf("user count drifted after 304: %d, want 2", got)
	}
}

func TestNewer200ListWinsDuringRetry(t *testing.T) {
	fp := newFakePanel(t)
	core := newFakeCore()
	c := newTestController(t, fp, core, nil)

	// target u1+u2 pending; the add fails
	fp.setUsers(`{"users":[{"id":1,"uuid":"u1"},{"id":2,"uuid":"u2"}]}`)
	core.mu.Lock()
	core.failAddUsersPort = c.info.Common.ServerPort
	core.mu.Unlock()
	c.nodeInfoMonitor()

	// a newer list revokes u2 before it was ever applied and adds u3
	fp.setUsers(`{"users":[{"id":1,"uuid":"u1"},{"id":3,"uuid":"u3"}]}`)
	core.mu.Lock()
	core.failAddUsersPort = 0
	core.mu.Unlock()
	c.nodeInfoMonitor()

	if core.hasUser(c.tag, "u2") {
		t.Fatal("u2 was applied although the newer list revoked it")
	}
	if core.hasUser(c.tag, "u3") == false {
		t.Fatal("u3 from the newest list was not applied")
	}
	if got := core.userCount(c.tag); got != 2 {
		t.Fatalf("core users = %d, want 2", got)
	}
}

func TestPartialDeleteSuccessAddFailureRecovers(t *testing.T) {
	two := `{"users":[{"id":1,"uuid":"u1"},{"id":2,"uuid":"u2"}]}`
	fp := newFakePanel(t)
	fp.setUsers(two)
	core := newFakeCore()
	c := newTestController(t, fp, core, nil)

	// target: revoke u1, add u3; the add fails while the delete succeeded
	fp.setUsers(`{"users":[{"id":2,"uuid":"u2"},{"id":3,"uuid":"u3"}]}`)
	core.mu.Lock()
	core.failAddUsersPort = c.info.Common.ServerPort
	core.mu.Unlock()
	c.nodeInfoMonitor()
	if core.hasUser(c.tag, "u1") == true {
		t.Fatal("u1 should have been deleted")
	}
	if got := core.userCount(c.tag); got != 1 {
		t.Fatalf("core users after partial failure = %d, want 1 (u2)", got)
	}

	// failure clears: the pending add must be retried on the next 304 cycle
	core.mu.Lock()
	core.failAddUsersPort = 0
	core.mu.Unlock()
	c.nodeInfoMonitor()
	if core.hasUser(c.tag, "u3") == false {
		t.Fatal("pending u3 was not applied on the next cycle")
	}
	if core.hasUser(c.tag, "u1") == true {
		t.Fatal("u1 reappeared")
	}
	if got := core.userCount(c.tag); got != 2 {
		t.Fatalf("core users after recovery = %d, want 2", got)
	}
}
