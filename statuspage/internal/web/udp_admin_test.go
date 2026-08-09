package web

import (
	"encoding/json"
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
		"internalPort", "internalSubscriptionURL", "INTERNAL_PORT</code> файла <code>.env",
	} {
		if !strings.Contains(body, invariant) {
			t.Fatalf("admin graph is missing invariant %q", invariant)
		}
	}
}

func TestUDPAdminSettingsReportsEffectiveInternalPort(t *testing.T) {
	st, err := store.Open("sqlite", filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Config{PanelAdminToken: "admin-token", InternalPort: "18081"}
	req := httptest.NewRequest(http.MethodGet, "/api/admin/udp/settings", nil)
	req.Header.Set("Authorization", "Bearer admin-token")
	rr := httptest.NewRecorder()
	New(cfg, st).Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}

	var body struct {
		Connectivity struct {
			InternalPort    string `json:"internalPort"`
			SubscriptionURL string `json:"subscriptionURL"`
			UDPPanelURL     string `json:"udpPanelURL"`
		} `json:"connectivity"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Connectivity.InternalPort != "18081" || body.Connectivity.SubscriptionURL != "http://127.0.0.1:18081/sub" || body.Connectivity.UDPPanelURL != "http://127.0.0.1:18081" {
		t.Fatalf("unexpected connectivity: %+v", body.Connectivity)
	}
}
