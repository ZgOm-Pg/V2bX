package xray

import (
	"net"
	"testing"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/conf"
)

func newTestXray(t *testing.T) *Xray {
	t.Helper()
	c, err := New(&conf.CoreConfig{XrayConfig: &conf.XrayConfig{
		LogConfig:        &conf.XrayLogConfig{Level: "none"},
		ConnectionConfig: &conf.XrayConnectionConfig{},
	}})
	if err != nil {
		t.Fatalf("new xray core: %s", err)
	}
	x := c.(*Xray)
	// Start() wires the inbound/outbound managers and the dispatcher
	if err := x.Start(); err != nil {
		t.Fatalf("start xray core: %s", err)
	}
	t.Cleanup(func() { _ = x.Close() })
	return x
}

func ssNodeInfo(port int) *panel.NodeInfo {
	return &panel.NodeInfo{
		Type:     "shadowsocks",
		Security: panel.None,
		Common:   &panel.CommonNode{ServerPort: port},
		Shadowsocks: &panel.ShadowsocksNode{
			CommonNode: panel.CommonNode{ServerPort: port},
			Cipher:     "aes-128-gcm",
		},
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// DelNode must attempt to remove both the inbound and the outbound even when
// one of them is missing (partial AddNode), so a failed reload never leaves
// half a node behind.
func TestDelNodeRemovesLeftoversAfterPartialAdd(t *testing.T) {
	x := newTestXray(t)
	opts := &conf.Options{XrayOptions: &conf.XrayOptions{}, ListenIP: "127.0.0.1"}
	tag := "ss-partial"
	port := freePort(t)
	info := ssNodeInfo(port)

	// sanity: a full add/delete round trip works
	if err := x.AddNode(tag, info, opts); err != nil {
		t.Fatalf("AddNode: %s", err)
	}
	if err := x.DelNode(tag); err != nil {
		t.Fatalf("DelNode: %s", err)
	}

	// inbound exists, outbound missing
	inb, err := buildInbound(opts, info, tag)
	if err != nil {
		t.Fatal(err)
	}
	if err := x.addInbound(inb); err != nil {
		t.Fatal(err)
	}
	_ = x.DelNode(tag)
	if err := x.AddNode(tag, info, opts); err != nil {
		t.Fatalf("AddNode after inbound-only cleanup failed: %s", err)
	}
	if err := x.DelNode(tag); err != nil {
		t.Fatal(err)
	}

	// outbound exists, inbound missing: the old early-return leaked it
	ob, err := buildOutbound(opts, tag)
	if err != nil {
		t.Fatal(err)
	}
	if err := x.addOutbound(ob); err != nil {
		t.Fatal(err)
	}
	_ = x.DelNode(tag)
	if err := x.AddNode(tag, info, opts); err != nil {
		t.Fatalf("AddNode after outbound-only cleanup failed: %s (outbound leftover?)", err)
	}
	if err := x.DelNode(tag); err != nil {
		t.Fatal(err)
	}
}
