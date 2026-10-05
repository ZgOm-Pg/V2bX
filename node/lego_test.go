package node

import (
	"os"
	"testing"

	"github.com/InazumaV/V2bX/conf"
)

// TestLego_CreateCertByDns and TestLego_RenewCert talk to the real Let's
// Encrypt ACME API and need valid Cloudflare credentials. They are opt-in
// integration tests: set V2BX_LEGO_E2E=1 to run them. Plain unit tests must
// never touch the network — the old init() registered a real ACME account at
// package load time, which -skip could not prevent.
func newE2ELego(t *testing.T) *Lego {
	t.Helper()
	if os.Getenv("V2BX_LEGO_E2E") == "" {
		t.Skip("set V2BX_LEGO_E2E=1 to run the ACME integration tests")
	}
	l, err := NewLego(&conf.CertConfig{
		CertMode:   "dns",
		Email:      "test@test.com",
		CertDomain: "test.test.com",
		Provider:   "cloudflare",
		DNSEnv: map[string]string{
			"CF_DNS_API_TOKEN": "123",
		},
		CertFile: "./cert/1.pem",
		KeyFile:  "./cert/1.key",
	})
	if err != nil {
		t.Fatalf("NewLego: %s", err)
	}
	return l
}

func TestLego_CreateCertByDns(t *testing.T) {
	l := newE2ELego(t)
	if err := l.CreateCert(); err != nil {
		t.Error(err)
	}
}

func TestLego_RenewCert(t *testing.T) {
	l := newE2ELego(t)
	t.Log(l.RenewCert())
}
