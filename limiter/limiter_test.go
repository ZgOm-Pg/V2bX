package limiter

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/common/format"
	"github.com/InazumaV/V2bX/conf"
)

func TestMain(m *testing.M) {
	Init()
	os.Exit(m.Run())
}

func newTestLimiter(nodeSpeed int) (*Limiter, string, string) {
	tag := fmt.Sprintf("test-%d", time.Now().UnixNano())
	l := AddLimiter(tag, &conf.LimitConfig{SpeedLimit: nodeSpeed},
		[]panel.UserInfo{{Id: 1, Uuid: "u1"}}, nil)
	return l, tag, format.UserTag(tag, "u1")
}

// A dynamic speed limit that has expired must only revoke the dynamic
// restriction. A user without an own speed limit used to be deleted from
// UserLimitInfo entirely, locking the still-valid user out of the node.
func TestExpiredDynamicLimitKeepsUser(t *testing.T) {
	l, tag, taguuid := newTestLimiter(0)
	if err := l.UpdateDynamicSpeedLimit(tag, "u1", 1024, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("UpdateDynamicSpeedLimit: %s", err)
	}

	if _, reject := l.CheckLimit(taguuid, "1.2.3.4", true, true); reject {
		t.Fatal("first connection after expiry was rejected")
	}
	// the second connection used to hit the deleted entry and got rejected
	if _, reject := l.CheckLimit(taguuid, "1.2.3.4", true, true); reject {
		t.Fatal("user was locked out after dynamic limit expiry")
	}
	v, ok := l.UserLimitInfo.Load(taguuid)
	if !ok {
		t.Fatal("valid user entry was deleted on expiry")
	}
	dyn, exp := v.(*UserLimitInfo).DynamicState()
	if dyn != 0 || exp != 0 {
		t.Fatalf("dynamic limit not revoked: dyn=%d exp=%d", dyn, exp)
	}
}

// The actual token bucket must follow limit changes, not only the config
// field. Existing connections keep their old bucket until their next
// CheckLimit call; every new/re-checking connection must immediately get
// the current rate.
func TestBucketRateFollowsLimitChanges(t *testing.T) {
	l, tag, taguuid := newTestLimiter(10) // node limit: 10 Mbps

	b, reject := l.CheckLimit(taguuid, "1.2.3.4", true, true)
	if reject || b == nil {
		t.Fatal("first CheckLimit rejected or returned no bucket")
	}
	if got := int64(b.Rate()); got != 1250000 {
		t.Fatalf("initial bucket rate = %d B/s, want 1250000", got)
	}

	if err := l.UpdateDynamicSpeedLimit(tag, "u1", 1, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("UpdateDynamicSpeedLimit: %s", err)
	}
	b2, _ := l.CheckLimit(taguuid, "1.2.3.4", true, true)
	if got := int64(b2.Rate()); got != 125000 {
		t.Fatalf("bucket rate after 1 Mbps dynamic limit = %d B/s, want 125000", got)
	}

	if err := l.UpdateDynamicSpeedLimit(tag, "u1", 1, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("UpdateDynamicSpeedLimit: %s", err)
	}
	b3, _ := l.CheckLimit(taguuid, "1.2.3.4", true, true)
	if got := int64(b3.Rate()); got != 1250000 {
		t.Fatalf("bucket rate after expiry = %d B/s, want 1250000", got)
	}
}

// Production methods UpdateDynamicSpeedLimit and CheckLimit run on different
// goroutines and touch the same UserLimitInfo fields; this pair must be race
// free (verify with go test -race).
func TestUserLimitInfoConcurrentAccess(t *testing.T) {
	l, tag, taguuid := newTestLimiter(10)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = l.CheckLimit(taguuid, "1.2.3.4", true, true)
				}
			}
		}()
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = l.UpdateDynamicSpeedLimit(tag, "u1", 1024, time.Now().Add(time.Hour))
					_ = l.UpdateDynamicSpeedLimit(tag, "u1", 2048, time.Now().Add(-time.Hour))
				}
			}
		}()
	}
	// a reader goroutine like the reporting path also observes the state
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if v, ok := l.UserLimitInfo.Load(taguuid); ok {
					_, _ = v.(*UserLimitInfo).DynamicState()
				}
			}
		}
	}()
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}
