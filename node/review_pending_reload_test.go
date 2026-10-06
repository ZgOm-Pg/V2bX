package node

import "testing"

const reviewTwoUsers = `{"users":[{"id":1,"uuid":"u1"},{"id":2,"uuid":"u2"}]}`

func TestReviewReloadNewUsersReachRealLimiter(t *testing.T) {
    fp := newFakePanel(t)
    child := newFakeCore()
    c := newTestController(t, fp, child, nil)
    fp.setUsers(reviewTwoUsers)
    fp.setNodePort(12345)
    if err := c.nodeInfoMonitor(); err != nil { t.Fatal(err) }
    if !child.hasUser(c.tag, "u2") { t.Fatal("fixture: u2 missing from child core after reload") }
    if _, reject := c.limiter.CheckLimit(c.tag+"|u2", "192.0.2.2", true, true); reject {
        t.Fatal("u2 was installed in the core but the real limiter rejects it after reload")
    }
}

func TestReviewPendingUsersSurviveNodeReloadWith304(t *testing.T) {
    fp := newFakePanel(t)
    child := newFakeCore()
    c := newTestController(t, fp, child, nil)
    fp.setUsers(reviewTwoUsers)
    child.mu.Lock()
    child.failAddUsersPort = c.info.Common.ServerPort
    child.mu.Unlock()
    if err := c.nodeInfoMonitor(); err != nil { t.Fatal(err) }
    if c.userListTarget == nil { t.Fatal("fixture: failed add did not retain target") }
    child.mu.Lock()
    child.failAddUsersPort = 0
    child.mu.Unlock()
    fp.setNodePort(12345)
    // User endpoint returns 304; config changes and succeeds in reloading.
    if err := c.nodeInfoMonitor(); err != nil { t.Fatal(err) }
    for i := 0; i < 3; i++ {
        if err := c.nodeInfoMonitor(); err != nil { t.Fatal(err) }
    }
    if !child.hasUser(c.tag, "u2") {
        t.Fatalf("pending u2 permanently lost across successful node reload with 304: target=%v applied=%v", c.userListTarget, c.userList)
    }
}
