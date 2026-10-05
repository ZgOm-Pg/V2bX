package core

import (
	"errors"
	"testing"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/conf"
)

type stubCore struct {
	addNodeErr error
	nodes      map[string]bool
	delNodes   []string
}

func newStubCore() *stubCore { return &stubCore{nodes: map[string]bool{}} }

func (s *stubCore) Start() error { return nil }
func (s *stubCore) Close() error { return nil }
func (s *stubCore) Type() string { return "stub" }
func (s *stubCore) Protocols() []string {
	return []string{"vmess", "vless", "trojan", "shadowsocks"}
}

func (s *stubCore) AddNode(tag string, info *panel.NodeInfo, option *conf.Options) error {
	// resources exist in the child core even when the call fails afterwards
	// (xray: inbound created, outbound build/add failed)
	s.nodes[tag] = true
	if s.addNodeErr != nil {
		return s.addNodeErr
	}
	return nil
}

func (s *stubCore) DelNode(tag string) error {
	s.delNodes = append(s.delNodes, tag)
	delete(s.nodes, tag)
	return nil
}

func (s *stubCore) AddUsers(p *AddUsersParams) (int, error) { return len(p.Users), nil }

func (s *stubCore) DelUsers(users []panel.UserInfo, tag string, info *panel.NodeInfo) error {
	return nil
}

func (s *stubCore) GetUserTrafficSlice(tag string, reset bool) ([]panel.UserTraffic, error) {
	return nil, nil
}

func (s *stubCore) RestoreUserTraffic(tag string, traffic []panel.UserTraffic) error {
	return nil
}

// A failed child AddNode may leave partially created resources behind. The
// Selector only stores the tag mapping on success, so a later DelNode would
// report "the node is not have" and never clean the child core up; the
// Selector itself must trigger the child cleanup.
func TestSelectorCleansUpAfterFailedAddNode(t *testing.T) {
	child := newStubCore()
	child.addNodeErr = errors.New("simulated outbound failure")
	sel := &Selector{cores: map[string]Core{"stub": child}}
	opts := &conf.Options{CoreName: "stub", Core: "stub"}

	if err := sel.AddNode("tag1", &panel.NodeInfo{}, opts); err == nil {
		t.Fatal("expected AddNode to fail")
	}
	if child.nodes["tag1"] {
		t.Fatal("partially created node left in child core")
	}
	found := false
	for _, d := range child.delNodes {
		if d == "tag1" {
			found = true
		}
	}
	if !found {
		t.Fatal("Selector did not ask the child core to clean up the failed node")
	}
	if _, ok := sel.nodes.Load("tag1"); ok {
		t.Fatal("mapping stored for a node whose AddNode failed")
	}
	if err := sel.DelNode("tag1"); err == nil {
		t.Fatal("DelNode for a never-registered tag must report absence")
	}
}

func TestSelectorAddDelNodeSuccessPath(t *testing.T) {
	child := newStubCore()
	sel := &Selector{cores: map[string]Core{"stub": child}}
	opts := &conf.Options{CoreName: "stub", Core: "stub"}

	if err := sel.AddNode("tag2", &panel.NodeInfo{}, opts); err != nil {
		t.Fatalf("AddNode: %s", err)
	}
	if _, ok := sel.nodes.Load("tag2"); !ok {
		t.Fatal("mapping not stored on success")
	}
	if err := sel.DelNode("tag2"); err != nil {
		t.Fatalf("DelNode: %s", err)
	}
	if child.nodes["tag2"] {
		t.Fatal("child node not removed")
	}
	if _, ok := sel.nodes.Load("tag2"); ok {
		t.Fatal("mapping not removed on delete")
	}
}
