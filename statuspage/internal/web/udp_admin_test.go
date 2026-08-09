package web

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"xray-status/internal/config"
	"xray-status/internal/store"
)

func TestUDPAdminGraphNeverTurnsMissingIntoLoss(t *testing.T) {
	st, err := store.Open("sqlite", filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rr := httptest.NewRecorder()
	New(config.Config{}, st).Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/udp-admin", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	body := rr.Body.String()
	for _, invariant := range []string{
		"v===65533)continue", "status:'loss'", "requestAnimationFrame", "slot*b.intervalUs/1000",
		"addReceiver", "pairingModal", "method:'DELETE'", "addEventListener('wheel'", "zoomReset",
	} {
		if !strings.Contains(body, invariant) {
			t.Fatalf("admin graph is missing invariant %q", invariant)
		}
	}
}
