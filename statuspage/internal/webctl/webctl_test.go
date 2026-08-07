package webctl

import (
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"

	"xray-status/internal/config"
	"xray-status/internal/store"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open("sqlite", filepath.Join(t.TempDir(), "status.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestEnabledRecoversLegacyDisabledState(t *testing.T) {
	st := testStore(t)
	if err := st.SetSetting(settingKey, "0"); err != nil {
		t.Fatalf("set legacy state: %v", err)
	}

	c := New(config.Config{}, st, http.NotFoundHandler())
	if !c.Enabled() {
		t.Fatal("legacy disabled state must be ignored until v2 state is written")
	}
}

func TestExplicitStopPersistsDisabledState(t *testing.T) {
	st := testStore(t)
	c := New(config.Config{}, st, http.NotFoundHandler())

	if err := c.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	afterRestart := New(config.Config{}, st, http.NotFoundHandler())
	if afterRestart.Enabled() {
		t.Fatal("explicit Stop must remain disabled after restart")
	}
}

func TestFailedStartDoesNotDisableWeb(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer ln.Close()

	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	st := testStore(t)
	if err := st.SetSetting(settingKey, "1"); err != nil {
		t.Fatalf("set enabled state: %v", err)
	}
	if err := st.SetSetting(settingStateV2Key, "1"); err != nil {
		t.Fatalf("set v2 marker: %v", err)
	}

	c := New(config.Config{Port: port, TLSMode: "off"}, st, http.NotFoundHandler())
	if err := c.Start(); err == nil {
		t.Fatal("Start must fail while the configured port is occupied")
	}
	if !c.Enabled() {
		t.Fatal("failed Start must not persist disabled state")
	}
}
