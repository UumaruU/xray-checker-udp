package web

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"xray-status/internal/store"
)

func bearer(r *http.Request) string {
	value := r.Header.Get("Authorization")
	if !strings.HasPrefix(value, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
}

func constantTokenMatch(want, got string) bool {
	if want == "" || got == "" || len(want) != len(got) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(want), []byte(got)) == 1
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type receiverRegisterRequest struct {
	ReceiverID     string `json:"receiverId"`
	BootstrapToken string `json:"bootstrapToken"`
	Version        string `json:"version"`
}

func (s *Server) receiverRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req receiverRegisterRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	now := time.Now()
	receiver, alreadyRegistered, err := s.st.PrepareUDPReceiverRegistration(req.ReceiverID, req.BootstrapToken, req.Version, now)
	if err != nil {
		http.Error(w, "registration rejected", http.StatusUnauthorized)
		return
	}
	if !alreadyRegistered {
		if err := probePendingReceiver(r.Context(), receiver, req.BootstrapToken); err != nil {
			http.Error(w, "receiver reached panel; waiting for reverse UDP probe: "+err.Error(), http.StatusTooEarly)
			return
		}
	}
	token, cfg, err := s.st.FinishUDPReceiverRegistration(req.ReceiverID, req.BootstrapToken, req.Version, now)
	if err != nil {
		http.Error(w, "registration could not be finalized", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"permanentToken": token, "config": cfg})
}

type receiverHeartbeatRequest struct {
	ReceiverID     string          `json:"receiverId"`
	Version        string          `json:"version"`
	ConfigRevision int64           `json:"configRevision"`
	Received       uint64          `json:"received"`
	Responded      uint64          `json:"responded"`
	Invalid        uint64          `json:"invalid"`
	Replay         uint64          `json:"replay"`
	RateLimited    uint64          `json:"rateLimited"`
	Peers          json.RawMessage `json:"peers"`
}

func (s *Server) receiverHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/heartbeat") {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 6 || parts[4] == "" {
		http.NotFound(w, r)
		return
	}
	id := parts[4]
	if !s.st.AuthenticateUDPReceiver(id, bearer(r)) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req receiverHeartbeatRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil || req.ReceiverID != id {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	health := map[string]any{
		"received": req.Received, "responded": req.Responded, "invalid": req.Invalid,
		"replay": req.Replay, "rateLimited": req.RateLimited, "peers": req.Peers,
	}
	cfg, changed, err := s.st.HeartbeatUDPReceiver(id, req.Version, req.ConfigRevision, health, time.Now())
	if err != nil {
		http.Error(w, "heartbeat failed", http.StatusInternalServerError)
		return
	}
	resp := map[string]any{"desiredRevision": cfg.Revision}
	if changed {
		resp["config"] = cfg
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) requireUDPAdmin(w http.ResponseWriter, r *http.Request) bool {
	if !constantTokenMatch(s.cfg.PanelAdminToken, bearer(r)) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func panelURLForRequest(configured string, r *http.Request) string {
	if configured != "" {
		return configured
	}
	scheme := "https"
	if r.TLS == nil && (strings.HasPrefix(r.Host, "127.0.0.1") || strings.HasPrefix(r.Host, "localhost")) {
		scheme = "http"
	}
	return scheme + "://" + r.Host
}

func receiverCompose(image, panelURL, receiverID, bootstrap string, port int) string {
	return fmt.Sprintf(`services:
  receiver:
    image: %s
    restart: unless-stopped
    environment:
      PANEL_URL: %s
      RECEIVER_ID: %s
      BOOTSTRAP_TOKEN: %s
      RECEIVER_UDP_PORT: "%d"
    ports:
      - "%d:%d/udp"
    volumes:
      - receiver-data:/data
    security_opt:
      - no-new-privileges:true
volumes:
  receiver-data:
`, image, panelURL, receiverID, bootstrap, port, port, port)
}

func (s *Server) adminUDPReceivers(w http.ResponseWriter, r *http.Request) {
	if !s.requireUDPAdmin(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		receivers, err := s.st.UDPReceivers()
		if err != nil {
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"receivers": receivers})
	case http.MethodPost:
		var input store.CreateUDPReceiverInput
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&input); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		created, err := s.st.CreateUDPReceiver(input, time.Now())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		compose := receiverCompose(s.cfg.ReceiverImage, panelURLForRequest(s.cfg.PanelPublicURL, r), created.Receiver.ID, created.BootstrapToken, created.Receiver.UDPPort)
		writeJSON(w, http.StatusCreated, map[string]any{"receiver": created.Receiver, "bootstrapToken": created.BootstrapToken, "expiresTs": created.ExpiresTS, "dockerCompose": compose})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) adminUDPReceiver(w http.ResponseWriter, r *http.Request) {
	if !s.requireUDPAdmin(w, r) {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/admin/udp/receivers/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 2 && parts[0] != "" && parts[1] == "pairing" && r.Method == http.MethodGet {
		receiver, err := s.st.UDPReceiver(parts[0])
		if err != nil {
			http.Error(w, "receiver not found", http.StatusNotFound)
			return
		}
		age := time.Since(time.Unix(receiver.HeartbeatTS, 0))
		receiverToPanel := receiver.HeartbeatTS > 0 && age >= 0 && age < 10*time.Second
		panelToReceiver := receiver.RegisteredTS > 0
		panelProbeError := ""
		if !panelToReceiver {
			key, keyErr := s.st.UDPReceiverPairingKey(parts[0], time.Now())
			if keyErr == nil {
				if probeErr := probePendingReceiverKey(r.Context(), receiver, key); probeErr == nil {
					panelToReceiver = true
				} else {
					panelProbeError = probeErr.Error()
				}
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"receiver": receiver, "receiverToPanel": receiverToPanel,
			"panelToReceiver": panelToReceiver, "panelProbeError": panelProbeError, "registered": receiver.RegisteredTS > 0,
		})
		return
	}
	if len(parts) == 1 && parts[0] != "" && r.Method == http.MethodDelete {
		if err := s.st.DeleteUDPReceiver(parts[0]); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if len(parts) == 1 && parts[0] != "" && r.Method == http.MethodPut {
		var input store.UDPReceiver
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&input); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := s.st.UpdateUDPReceiver(parts[0], input); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "revision": s.st.UDPRevision()})
		return
	}
	if len(parts) != 2 || parts[1] != "exclusions" || r.Method != http.MethodPut {
		http.NotFound(w, r)
		return
	}
	var body struct {
		NodeSIDs []string `json:"nodeSids"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128<<10)).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := s.st.SetUDPReceiverExclusions(parts[0], body.NodeSIDs); err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) adminUDPSettings(w http.ResponseWriter, r *http.Request) {
	if !s.requireUDPAdmin(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		estimate, err := s.st.UDPPerformance()
		if err != nil {
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		internalURL := "http://127.0.0.1:" + s.cfg.InternalPort
		writeJSON(w, http.StatusOK, map[string]any{
			"settings": s.st.UDPSettings(), "revision": s.st.UDPRevision(), "performance": estimate,
			"connectivity": map[string]string{
				"internalPort":    s.cfg.InternalPort,
				"subscriptionURL": internalURL + "/sub",
				"udpPanelURL":     internalURL,
			},
		})
	case http.MethodPut:
		settings := s.st.UDPSettings()
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128<<10)).Decode(&settings); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := s.st.SetUDPSettings(settings); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"settings": s.st.UDPSettings(), "revision": s.st.UDPRevision()})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) adminUDPNodes(w http.ResponseWriter, r *http.Request) {
	if !s.requireUDPAdmin(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rows, err := s.st.CurrentRows()
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	settings, err := s.st.UDPNodeSettings()
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		item, ok := settings[row.SID]
		if !ok {
			item = store.UDPNodeSetting{NodeSID: row.SID, Enabled: true, ReceiverMode: "all"}
		}
		out = append(out, map[string]any{"sid": row.SID, "name": row.Name, "group": row.Grp, "setting": item})
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": out})
}

func (s *Server) adminUDPNode(w http.ResponseWriter, r *http.Request) {
	if !s.requireUDPAdmin(w, r) {
		return
	}
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sid := strings.TrimPrefix(r.URL.Path, "/api/admin/udp/nodes/")
	if sid == "" {
		http.NotFound(w, r)
		return
	}
	var item store.UDPNodeSetting
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128<<10)).Decode(&item); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	item.NodeSID = sid
	if err := s.st.SetUDPNodeSetting(item); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "revision": s.st.UDPRevision()})
}
