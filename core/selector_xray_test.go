package core_test

import (
	"net"
	"testing"
	"time"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/conf"
	vCore "github.com/InazumaV/V2bX/core"
	// register the real xray core
	_ "github.com/InazumaV/V2bX/core/xray"
)

func freePortX(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForListenerX(t *testing.T, port int, wantUp bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", itoa(port)), 300*time.Millisecond)
		if err == nil {
			c.Close()
			if wantUp {
				return
			}
		} else if !wantUp {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s: listener state wrong (wantUp=%v)", what, wantUp)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// A duplicate Selector.AddNode with the same tag must be rejected without
// touching the healthy node that is already listening.
func TestSelectorDuplicateAddNodeKeepsOriginalNode(t *testing.T) {
	sel, err := vCore.NewSelector([]conf.CoreConfig{{Type: "xray", XrayConfig: &conf.XrayConfig{
		LogConfig:        &conf.XrayLogConfig{Level: "none"},
		ConnectionConfig: &conf.XrayConnectionConfig{},
	}}})
	if err != nil {
		t.Fatalf("NewSelector: %s", err)
	}

	port := freePortX(t)
	opts := &conf.Options{
		Core:        "xray",
		ListenIP:    "127.0.0.1",
		XrayOptions: &conf.XrayOptions{},
	}
	info := &panel.NodeInfo{
		Type:     "shadowsocks",
		Security: panel.None,
		Common:   &panel.CommonNode{ServerPort: port},
		Shadowsocks: &panel.ShadowsocksNode{
			CommonNode: panel.CommonNode{ServerPort: port},
			Cipher:     "aes-128-gcm",
		},
	}
	tag := "ss-dup-test"

	if err := sel.Start(); err != nil {
		t.Fatalf("selector start: %s", err)
	}
	t.Cleanup(func() { _ = sel.Close() })

	if err := sel.AddNode(tag, info, opts); err != nil {
		t.Fatalf("initial AddNode: %s", err)
	}
	waitForListenerX(t, port, true, "original node listener")

	if err := sel.AddNode(tag, info, opts); err == nil {
		t.Fatal("duplicate AddNode must be rejected")
	}

	// the original node must still be listening and its mapping intact
	// (a successful DelNode proves the mapping survived; it errors with
	// "the node is not have" otherwise)
	waitForListenerX(t, port, true, "original listener after duplicate add")
	if err := sel.DelNode(tag); err != nil {
		t.Fatalf("DelNode after duplicate add: %s (mapping lost?)", err)
	}
	waitForListenerX(t, port, false, "listener after DelNode")
}
