package panel

import (
	"net/http"

	"github.com/InazumaV/V2bX/conf"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func newOnlineTestClient(t *testing.T, handler http.Handler) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := New(&conf.ApiConfig{
		APIHost:  srv.URL,
		NodeID:   1,
		Key:      "testkey",
		NodeType: "shadowsocks",
	})
	if err != nil {
		t.Fatalf("New: %s", err)
	}
	return c, srv
}

func onlinePayload() *map[int][]string {
	return &map[int][]string{1: {"127.0.0.1"}}
}

func TestReportNodeOnlineUsersSuccessReturnsNil(t *testing.T) {
	c, _ := newOnlineTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	if err := c.ReportNodeOnlineUsers(onlinePayload()); err != nil {
		t.Fatalf("successful report returned error: %s", err)
	}
}

func TestReportNodeOnlineUsersServerErrorReturnsError(t *testing.T) {
	c, _ := newOnlineTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	if err := c.ReportNodeOnlineUsers(onlinePayload()); err == nil {
		t.Fatal("server error was swallowed and reported as success")
	}
}

func TestReportNodeOnlineUsersConnectionFailureReturnsError(t *testing.T) {
	// reserve an address, then close the listener: connections are refused
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := srv.Listener.Addr().String()
	srv.Close()
	c, err := New(&conf.ApiConfig{APIHost: "http://" + addr, NodeID: 1, Key: "k", NodeType: "shadowsocks"})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ReportNodeOnlineUsers(onlinePayload()); err == nil {
		t.Fatal("connection failure was swallowed and reported as success")
	}
}

// after a failure the next reporting cycle must still work
func TestReportNodeOnlineUsersRecoversAfterFailure(t *testing.T) {
	var fail atomic.Bool
	c, _ := newOnlineTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	fail.Store(true)
	if err := c.ReportNodeOnlineUsers(onlinePayload()); err == nil {
		t.Fatal("first (failing) report returned nil")
	}
	fail.Store(false)
	if err := c.ReportNodeOnlineUsers(onlinePayload()); err != nil {
		t.Fatalf("next report after failure returned error: %s", err)
	}
}
