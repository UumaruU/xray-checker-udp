package webctl

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"

	"xray-status/internal/config"
	"xray-status/internal/store"
)

func testController(t *testing.T, port string) (*Controller, *store.Store) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "webctl.db")
	st, err := store.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cfg := config.Config{Port: port, TLSMode: "off", DBPath: dbPath}
	h := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	return New(cfg, st, h), st
}

func TestFailedStartDoesNotPersistDisabled(t *testing.T) {
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)

	c, st := testController(t, port)
	if err := st.SetSetting(settingKey, "1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err == nil {
		t.Fatal("Start succeeded even though the port is occupied")
	}
	if got := st.GetSetting(settingKey, ""); got != "1" {
		t.Fatalf("failed Start changed web_enabled to %q, want 1", got)
	}
	if c.Running() {
		t.Fatal("controller reports running after failed Start")
	}
}

func TestLegacyDisabledStateRecoversOnce(t *testing.T) {
	c, st := testController(t, "0")
	if err := st.SetSetting(settingKey, "0"); err != nil {
		t.Fatal(err)
	}
	if !c.Enabled() {
		t.Fatal("legacy web_enabled=0 must be retried after upgrading")
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Shutdown(context.Background()) })
	if got := st.GetSetting(explicitSettingKey, ""); got != "1" {
		t.Fatalf("migration marker = %q, want 1", got)
	}
}

func TestExplicitStopRemainsDisabled(t *testing.T) {
	c, st := testController(t, "0")
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(); err != nil {
		t.Fatal(err)
	}

	restarted := New(c.cfg, st, c.handler)
	if restarted.Enabled() {
		t.Fatal("explicit Stop must remain disabled after restart")
	}
}
