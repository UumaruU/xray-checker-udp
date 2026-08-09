package store

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	udpSettingsKey = "udp_settings_v1"
	udpRevisionKey = "udp_config_revision"
)

type UDPSettings struct {
	Enabled                  bool    `json:"enabled"`
	Preset                   string  `json:"preset"`
	ProbeIntervalMS          int     `json:"probeIntervalMs"`
	ProbeTimeoutMS           int     `json:"probeTimeoutMs"`
	SlidingWindowSeconds     int     `json:"slidingWindowSeconds"`
	MinSamples               int     `json:"minSamples"`
	MinCoverage              float64 `json:"minCoverage"`
	AggregationMS            int     `json:"aggregationMs"`
	CheckerBatchMS           int     `json:"checkerBatchMs"`
	BrowserBatchMS           int     `json:"browserBatchMs"`
	DBPersistMS              int     `json:"dbPersistMs"`
	PlaybackDelayMS          int     `json:"playbackDelayMs"`
	MaxCatchupSpeed          float64 `json:"maxCatchupSpeed"`
	LiveWindowSeconds        int     `json:"liveWindowSeconds"`
	RawRetentionHours        int     `json:"rawRetentionHours"`
	AggregateRetentionDays   int     `json:"aggregateRetentionDays"`
	OnlineLoss               float64 `json:"onlineLoss"`
	DegradedLoss             float64 `json:"degradedLoss"`
	OfflineLoss              float64 `json:"offlineLoss"`
	JitterThresholdMS        int     `json:"jitterThresholdMs"`
	StaleTimeoutMS           int     `json:"staleTimeoutMs"`
	ReceiverConfidence       float64 `json:"receiverConfidence"`
	ReceiverDisagreement     float64 `json:"receiverDisagreement"`
	DegradedStreak           int     `json:"degradedStreak"`
	OfflineStreak            int     `json:"offlineStreak"`
	RecoveryStreak           int     `json:"recoveryStreak"`
	AutoManageRate           bool    `json:"autoManageRate"`
	AutoManageReceivers      bool    `json:"autoManageReceivers"`
	AutoManageDisplayBuffer  bool    `json:"autoManageDisplayBuffer"`
	PrioritizeProblemNodes   bool    `json:"prioritizeProblemNodes"`
	MinProbeHz               float64 `json:"minProbeHz"`
	MaxProbeHz               float64 `json:"maxProbeHz"`
	AutoReceiverLimit        int     `json:"autoReceiverLimit"`
	OverloadLagMS            int     `json:"overloadLagMs"`
	OverloadQueueRatio       float64 `json:"overloadQueueRatio"`
	OverloadSustainSeconds   int     `json:"overloadSustainSeconds"`
	DirectReceiverIntervalMS int     `json:"directReceiverIntervalMs"`
	ReceiverPeerIntervalMS   int     `json:"receiverPeerIntervalMs"`
}

func DefaultUDPSettings() UDPSettings {
	return UDPSettings{
		Enabled: false, Preset: "realtime", ProbeIntervalMS: 100, ProbeTimeoutMS: 1500,
		SlidingWindowSeconds: 30, MinSamples: 20, MinCoverage: 0.70,
		AggregationMS: 1000, CheckerBatchMS: 1000, BrowserBatchMS: 1000,
		DBPersistMS: 10000, PlaybackDelayMS: 1000, MaxCatchupSpeed: 1.5, LiveWindowSeconds: 600,
		RawRetentionHours: 24, AggregateRetentionDays: 30,
		OnlineLoss: 0.01, DegradedLoss: 0.05, OfflineLoss: 0.95,
		JitterThresholdMS: 25, StaleTimeoutMS: 5000, ReceiverConfidence: 0.70,
		ReceiverDisagreement: 0.20, AutoManageRate: true, AutoManageReceivers: true,
		DegradedStreak: 3, OfflineStreak: 3, RecoveryStreak: 3,
		AutoManageDisplayBuffer: true, PrioritizeProblemNodes: true,
		MinProbeHz: 1, MaxProbeHz: 10, AutoReceiverLimit: 5,
		OverloadLagMS: 500, OverloadQueueRatio: 0.75, OverloadSustainSeconds: 5,
		DirectReceiverIntervalMS: 1000, ReceiverPeerIntervalMS: 2000,
	}
}

func (s *Store) UDPSettings() UDPSettings {
	settings := DefaultUDPSettings()
	raw := s.GetSetting(udpSettingsKey, "")
	if raw != "" {
		_ = json.Unmarshal([]byte(raw), &settings)
	}
	return settings
}

func (s *Store) SetUDPSettings(settings UDPSettings) error {
	switch settings.Preset {
	case "realtime":
		settings.ProbeIntervalMS, settings.ProbeTimeoutMS, settings.SlidingWindowSeconds = 100, 1500, 30
		settings.AggregationMS, settings.CheckerBatchMS, settings.BrowserBatchMS, settings.DBPersistMS, settings.PlaybackDelayMS = 1000, 1000, 1000, 10000, 1000
	case "balanced":
		settings.ProbeIntervalMS, settings.ProbeTimeoutMS, settings.SlidingWindowSeconds = 1000, 2000, 60
		settings.AggregationMS, settings.CheckerBatchMS, settings.BrowserBatchMS, settings.DBPersistMS, settings.PlaybackDelayMS = 1000, 1000, 1000, 10000, 1000
	case "low-load":
		settings.ProbeIntervalMS, settings.ProbeTimeoutMS, settings.SlidingWindowSeconds = 5000, 8000, 300
		settings.AggregationMS, settings.CheckerBatchMS, settings.BrowserBatchMS, settings.DBPersistMS, settings.PlaybackDelayMS = 5000, 5000, 5000, 15000, 1000
	case "custom":
		// Preserve explicit operational values.
	default:
		return errors.New("preset must be realtime, balanced, low-load or custom")
	}
	if settings.ProbeIntervalMS < 20 || settings.ProbeIntervalMS > 60000 {
		return errors.New("probe interval must be between 20ms and 60s")
	}
	if settings.ProbeTimeoutMS < settings.ProbeIntervalMS || settings.ProbeTimeoutMS > 60000 {
		return errors.New("probe timeout is outside safe bounds")
	}
	if settings.RawRetentionHours < 1 {
		return errors.New("raw retention must be at least one hour")
	}
	if settings.MaxCatchupSpeed < 1 || settings.MaxCatchupSpeed > 5 {
		return errors.New("max catch-up speed must be between 1x and 5x")
	}
	data, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	if err := s.SetSetting(udpSettingsKey, string(data)); err != nil {
		return err
	}
	_, err = s.bumpUDPRevision()
	return err
}

func (s *Store) UDPRevision() int64 {
	n, _ := strconv.ParseInt(s.GetSetting(udpRevisionKey, "1"), 10, 64)
	if n < 1 {
		n = 1
	}
	return n
}

func (s *Store) bumpUDPRevision() (int64, error) {
	// Settings mutations are infrequent. Serializing through the DB is sufficient
	// for SQLite's single writer and normal Panel administration workloads.
	next := s.UDPRevision() + 1
	if err := s.SetSetting(udpRevisionKey, strconv.FormatInt(next, 10)); err != nil {
		return next, err
	}
	_, err := s.exec(`UPDATE udp_receivers SET desired_revision=?`, next)
	return next, err
}

type UDPReceiver struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Host            string   `json:"host"`
	UDPPort         int      `json:"udpPort"`
	Location        string   `json:"location,omitempty"`
	Description     string   `json:"description,omitempty"`
	Enabled         bool     `json:"enabled"`
	LocalNodeSID    string   `json:"localNodeSid,omitempty"`
	RegisteredTS    int64    `json:"registeredTs"`
	HeartbeatTS     int64    `json:"heartbeatTs"`
	Version         string   `json:"version,omitempty"`
	AppliedRevision int64    `json:"appliedRevision"`
	DesiredRevision int64    `json:"desiredRevision"`
	HealthJSON      string   `json:"health,omitempty"`
	ExcludedNodes   []string `json:"excludedNodes"`
}

type CreateUDPReceiverInput struct {
	Name         string `json:"name"`
	Host         string `json:"host"`
	UDPPort      int    `json:"udpPort"`
	Location     string `json:"location"`
	Description  string `json:"description"`
	LocalNodeSID string `json:"localNodeSid"`
}

type CreatedUDPReceiver struct {
	Receiver       UDPReceiver `json:"receiver"`
	BootstrapToken string      `json:"bootstrapToken"`
	ExpiresTS      int64       `json:"expiresTs"`
}

const udpBootstrapTTL = 24 * time.Hour

func normalizeReceiverHost(host string) string {
	host = strings.TrimSpace(host)
	plain := strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	if addr, err := netip.ParseAddr(plain); err == nil {
		return addr.Unmap().String()
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func splitNodeSIDs(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return unicode.IsSpace(r) || r == ',' || r == ';'
	})
	seen := make(map[string]bool, len(parts))
	out := make([]string, 0, len(parts))
	for _, sid := range parts {
		sid = strings.TrimSpace(sid)
		if sid == "" || seen[sid] {
			continue
		}
		seen[sid] = true
		out = append(out, sid)
	}
	return out
}

func normalizeCreateUDPReceiverInput(input CreateUDPReceiverInput) CreateUDPReceiverInput {
	input.Name = strings.TrimSpace(input.Name)
	input.Host = normalizeReceiverHost(input.Host)
	input.Location = strings.TrimSpace(input.Location)
	input.Description = strings.TrimSpace(input.Description)
	input.LocalNodeSID = strings.Join(splitNodeSIDs(input.LocalNodeSID), " ")
	return input
}

func (s *Store) receiverHostAvailable(host, exceptID string) (bool, error) {
	var count int
	if err := s.queryRow(`SELECT COUNT(1) FROM udp_receivers WHERE host=? AND id<>?`, host, exceptID).Scan(&count); err != nil {
		return false, err
	}
	return count == 0, nil
}

func (s *Store) replacePendingReceiverAtHost(host string) error {
	var id string
	if err := s.queryRow(`SELECT id FROM udp_receivers WHERE host=? AND registered_ts=0 LIMIT 1`, host).Scan(&id); err != nil {
		return nil
	}
	return s.DeleteUDPReceiver(id)
}

func (s *Store) cleanupExpiredUDPPairings(now time.Time) error {
	rows, err := s.query(`SELECT id FROM udp_receivers WHERE registered_ts=0 AND bootstrap_expires>0 AND bootstrap_expires<?`, now.Unix())
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()
	for _, id := range ids {
		if err := s.DeleteUDPReceiver(id); err != nil {
			return err
		}
	}
	return nil
}

func randomToken(bytesN int) (string, error) {
	buf := make([]byte, bytesN)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func tokenMatches(hash, token string) bool {
	want, err := hex.DecodeString(hash)
	if err != nil {
		return false
	}
	sum := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(want, sum[:]) == 1
}

func (s *Store) CreateUDPReceiver(input CreateUDPReceiverInput, now time.Time) (CreatedUDPReceiver, error) {
	if !s.SecretsEnabled() {
		return CreatedUDPReceiver{}, ErrSecretsDisabled
	}
	input = normalizeCreateUDPReceiverInput(input)
	if input.Name == "" || input.Host == "" || input.UDPPort < 1 || input.UDPPort > 65535 {
		return CreatedUDPReceiver{}, errors.New("name, host and valid UDP port are required")
	}
	if err := s.cleanupExpiredUDPPairings(now); err != nil {
		return CreatedUDPReceiver{}, err
	}
	// Starting a fresh pairing for an IP/host replaces an abandoned unfinished
	// pairing, but never an already registered receiver.
	if err := s.replacePendingReceiverAtHost(input.Host); err != nil {
		return CreatedUDPReceiver{}, err
	}
	available, err := s.receiverHostAvailable(input.Host, "")
	if err != nil {
		return CreatedUDPReceiver{}, err
	}
	if !available {
		return CreatedUDPReceiver{}, errors.New("receiver with this IP/host already exists")
	}
	id, err := randomToken(12)
	if err != nil {
		return CreatedUDPReceiver{}, err
	}
	bootstrap, err := randomToken(32)
	if err != nil {
		return CreatedUDPReceiver{}, err
	}
	dataKey := make([]byte, 32)
	if _, err := rand.Read(dataKey); err != nil {
		return CreatedUDPReceiver{}, err
	}
	if err := s.SetSecret("udp.receiver."+id+".key.1", base64.StdEncoding.EncodeToString(dataKey)); err != nil {
		return CreatedUDPReceiver{}, err
	}
	revision := s.UDPRevision()
	expires := now.Add(udpBootstrapTTL).Unix()
	_, err = s.exec(`INSERT INTO udp_receivers(
		id,name,host,udp_port,location,description,enabled,local_node_sid,
		bootstrap_hash,bootstrap_expires,bootstrap_used,desired_revision,created_ts)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, input.Name, input.Host, input.UDPPort, input.Location, input.Description, 1, input.LocalNodeSID,
		tokenHash(bootstrap), expires, 0, revision, now.Unix())
	if err != nil {
		_, _ = s.exec(`DELETE FROM secrets WHERE k=?`, "udp.receiver."+id+".key.1")
		return CreatedUDPReceiver{}, err
	}
	for _, sid := range splitNodeSIDs(input.LocalNodeSID) {
		_, _ = s.exec(`INSERT INTO udp_receiver_exclusions(receiver_id,node_sid) VALUES(?,?) ON CONFLICT DO NOTHING`, id, sid)
	}
	receiver, _ := s.UDPReceiver(id)
	return CreatedUDPReceiver{Receiver: receiver, BootstrapToken: bootstrap, ExpiresTS: expires}, nil
}

func (s *Store) UDPReceiver(id string) (UDPReceiver, error) {
	var r UDPReceiver
	var enabled int
	err := s.queryRow(`SELECT id,name,host,udp_port,COALESCE(location,''),COALESCE(description,''),enabled,
		COALESCE(local_node_sid,''),registered_ts,heartbeat_ts,COALESCE(version,''),applied_revision,desired_revision,COALESCE(health_json,'')
		FROM udp_receivers WHERE id=?`, id).Scan(
		&r.ID, &r.Name, &r.Host, &r.UDPPort, &r.Location, &r.Description, &enabled,
		&r.LocalNodeSID, &r.RegisteredTS, &r.HeartbeatTS, &r.Version, &r.AppliedRevision, &r.DesiredRevision, &r.HealthJSON)
	if err != nil {
		return UDPReceiver{}, err
	}
	r.Enabled = enabled != 0
	r.ExcludedNodes, _ = s.UDPReceiverExclusions(id)
	return r, nil
}

func (s *Store) UDPReceivers() ([]UDPReceiver, error) {
	rows, err := s.query(`SELECT id FROM udp_receivers WHERE registered_ts>0 ORDER BY created_ts,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	var out []UDPReceiver
	for _, id := range ids {
		r, err := s.UDPReceiver(id)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) UpdateUDPReceiver(id string, input UDPReceiver) error {
	input.Name = strings.TrimSpace(input.Name)
	input.Host = normalizeReceiverHost(input.Host)
	input.Location = strings.TrimSpace(input.Location)
	input.Description = strings.TrimSpace(input.Description)
	input.LocalNodeSID = strings.Join(splitNodeSIDs(input.LocalNodeSID), " ")
	if id == "" || input.Name == "" || input.Host == "" || input.UDPPort < 1 || input.UDPPort > 65535 {
		return errors.New("receiver name, host and valid UDP port are required")
	}
	var currentPort int
	if err := s.queryRow(`SELECT udp_port FROM udp_receivers WHERE id=?`, id).Scan(&currentPort); err != nil {
		return errors.New("receiver not found")
	}
	if input.UDPPort != currentPort {
		return errors.New("receiver UDP port is fixed by its deployment; re-pair the receiver to change it")
	}
	available, err := s.receiverHostAvailable(input.Host, id)
	if err != nil {
		return err
	}
	if !available {
		return errors.New("receiver with this IP/host already exists")
	}
	enabled := 0
	if input.Enabled {
		enabled = 1
	}
	result, err := s.exec(`UPDATE udp_receivers SET name=?,host=?,udp_port=?,location=?,description=?,enabled=?,local_node_sid=? WHERE id=?`, input.Name, input.Host, input.UDPPort, input.Location, input.Description, enabled, input.LocalNodeSID, id)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return errors.New("receiver not found")
	}
	for _, sid := range splitNodeSIDs(input.LocalNodeSID) {
		_, _ = s.exec(`INSERT INTO udp_receiver_exclusions(receiver_id,node_sid) VALUES(?,?) ON CONFLICT DO NOTHING`, id, sid)
	}
	_, err = s.bumpUDPRevision()
	return err
}

func (s *Store) UDPReceiverExclusions(receiverID string) ([]string, error) {
	rows, err := s.query(`SELECT node_sid FROM udp_receiver_exclusions WHERE receiver_id=? ORDER BY node_sid`, receiverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var sid string
		if err := rows.Scan(&sid); err != nil {
			return nil, err
		}
		out = append(out, sid)
	}
	return out, rows.Err()
}

func (s *Store) SetUDPReceiverExclusions(receiverID string, nodeSIDs []string) error {
	tx, err := s.begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM udp_receiver_exclusions WHERE receiver_id=?`, receiverID); err != nil {
		_ = tx.Rollback()
		return err
	}
	var local string
	_ = tx.QueryRow(`SELECT COALESCE(local_node_sid,'') FROM udp_receivers WHERE id=?`, receiverID).Scan(&local)
	seen := map[string]bool{}
	if local != "" {
		nodeSIDs = append(nodeSIDs, splitNodeSIDs(local)...)
	}
	for _, sid := range nodeSIDs {
		sid = strings.TrimSpace(sid)
		if sid == "" || seen[sid] {
			continue
		}
		seen[sid] = true
		if _, err := tx.Exec(`INSERT INTO udp_receiver_exclusions(receiver_id,node_sid) VALUES(?,?)`, receiverID, sid); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	revision, err := s.bumpUDPRevision()
	if err == nil {
		_, _ = s.exec(`UPDATE udp_receivers SET desired_revision=?`, revision)
	}
	return err
}

type UDPReceiverRuntimeConfig struct {
	Revision       int64 `json:"revision"`
	PeerIntervalMS int   `json:"peerIntervalMs"`
	Keys           []struct {
		Version uint16 `json:"version"`
		Key     string `json:"key"`
	} `json:"keys"`
	Peers []UDPReceiverPeer `json:"peers"`
}

type UDPReceiverPeer struct {
	ID         string `json:"id"`
	Host       string `json:"host"`
	UDPPort    int    `json:"udpPort"`
	KeyVersion uint16 `json:"keyVersion"`
	Key        string `json:"key"`
}

func (s *Store) receiverRuntimeConfig(id string) (UDPReceiverRuntimeConfig, error) {
	key, err := s.GetSecret("udp.receiver." + id + ".key.1")
	if err != nil {
		return UDPReceiverRuntimeConfig{}, err
	}
	cfg := UDPReceiverRuntimeConfig{Revision: s.UDPRevision(), PeerIntervalMS: s.UDPSettings().ReceiverPeerIntervalMS}
	cfg.Keys = append(cfg.Keys, struct {
		Version uint16 `json:"version"`
		Key     string `json:"key"`
	}{Version: 1, Key: key})
	receivers, err := s.UDPReceivers()
	if err != nil {
		return UDPReceiverRuntimeConfig{}, err
	}
	for _, peer := range receivers {
		if !peer.Enabled || peer.ID == id {
			continue
		}
		peerKey, err := s.ReceiverDataKey(peer.ID, 1)
		if err != nil {
			return UDPReceiverRuntimeConfig{}, err
		}
		cfg.Peers = append(cfg.Peers, UDPReceiverPeer{ID: peer.ID, Host: peer.Host, UDPPort: peer.UDPPort, KeyVersion: 1, Key: peerKey})
	}
	return cfg, nil
}

func (s *Store) PrepareUDPReceiverRegistration(id, bootstrapToken, version string, now time.Time) (UDPReceiver, bool, error) {
	var hash string
	var expires int64
	var used int
	var registered int64
	if err := s.queryRow(`SELECT COALESCE(bootstrap_hash,''),bootstrap_expires,bootstrap_used,registered_ts FROM udp_receivers WHERE id=?`, id).Scan(&hash, &expires, &used, &registered); err != nil {
		return UDPReceiver{}, false, err
	}
	if hash == "" || expires < now.Unix() || !tokenMatches(hash, bootstrapToken) {
		return UDPReceiver{}, false, errors.New("bootstrap token is invalid or expired")
	}
	if used != 0 && registered == 0 {
		return UDPReceiver{}, false, errors.New("receiver registration is inconsistent")
	}
	if used == 0 {
		_, _ = s.exec(`UPDATE udp_receivers SET heartbeat_ts=?,version=? WHERE id=?`, now.Unix(), version, id)
	}
	receiver, err := s.UDPReceiver(id)
	return receiver, used != 0 && registered > 0, err
}

func (s *Store) UDPReceiverPairingKey(id string, now time.Time) ([]byte, error) {
	var hash string
	var expires, registered int64
	if err := s.queryRow(`SELECT COALESCE(bootstrap_hash,''),bootstrap_expires,registered_ts FROM udp_receivers WHERE id=?`, id).Scan(&hash, &expires, &registered); err != nil {
		return nil, err
	}
	if registered > 0 || hash == "" || expires < now.Unix() {
		return nil, errors.New("receiver pairing is not active")
	}
	key, err := hex.DecodeString(hash)
	if err != nil || len(key) != sha256.Size {
		return nil, errors.New("receiver pairing key is invalid")
	}
	return key, nil
}

func (s *Store) FinishUDPReceiverRegistration(id, bootstrapToken, version string, now time.Time) (permanentToken string, cfg UDPReceiverRuntimeConfig, err error) {
	var hash string
	var expires, registered int64
	var used int
	if err = s.queryRow(`SELECT COALESCE(bootstrap_hash,''),bootstrap_expires,bootstrap_used,registered_ts FROM udp_receivers WHERE id=?`, id).Scan(&hash, &expires, &used, &registered); err != nil {
		return "", cfg, err
	}
	if hash == "" || expires < now.Unix() || !tokenMatches(hash, bootstrapToken) {
		return "", cfg, errors.New("bootstrap token is invalid or expired")
	}
	if used != 0 && registered > 0 {
		permanentToken, err = s.GetSecret("udp.receiver." + id + ".control")
		if err != nil {
			return "", cfg, err
		}
		cfg, err = s.receiverRuntimeConfig(id)
		return permanentToken, cfg, err
	}
	permanentToken, err = randomToken(32)
	if err != nil {
		return "", cfg, err
	}
	cfg, err = s.receiverRuntimeConfig(id)
	if err != nil {
		return "", cfg, err
	}
	if err = s.SetSecret("udp.receiver."+id+".control", permanentToken); err != nil {
		return "", cfg, err
	}
	_, err = s.exec(`UPDATE udp_receivers SET bootstrap_used=1,token_hash=?,registered_ts=?,heartbeat_ts=?,version=? WHERE id=?`,
		tokenHash(permanentToken), now.Unix(), now.Unix(), version, id)
	if err != nil {
		return "", cfg, err
	}
	revision, err := s.bumpUDPRevision()
	if err != nil {
		return "", cfg, err
	}
	cfg.Revision = revision
	_, err = s.exec(`UPDATE udp_receivers SET applied_revision=?,desired_revision=? WHERE id=?`, revision, revision, id)
	return permanentToken, cfg, err
}

func (s *Store) AuthenticateUDPReceiver(id, token string) bool {
	var hash string
	if err := s.queryRow(`SELECT COALESCE(token_hash,'') FROM udp_receivers WHERE id=?`, id).Scan(&hash); err != nil || hash == "" {
		return false
	}
	return tokenMatches(hash, token)
}

func (s *Store) HeartbeatUDPReceiver(id, version string, appliedRevision int64, health any, now time.Time) (UDPReceiverRuntimeConfig, bool, error) {
	healthJSON, _ := json.Marshal(health)
	desired := s.UDPRevision()
	_, err := s.exec(`UPDATE udp_receivers SET heartbeat_ts=?,version=?,applied_revision=?,desired_revision=?,health_json=?,bootstrap_hash='',bootstrap_expires=0 WHERE id=?`,
		now.Unix(), version, appliedRevision, desired, string(healthJSON), id)
	if err != nil {
		return UDPReceiverRuntimeConfig{}, false, err
	}
	if appliedRevision == desired {
		return UDPReceiverRuntimeConfig{Revision: desired}, false, nil
	}
	cfg, err := s.receiverRuntimeConfig(id)
	return cfg, true, err
}

func (s *Store) DeleteUDPReceiver(id string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("receiver id is required")
	}
	var registered int64
	if err := s.queryRow(`SELECT registered_ts FROM udp_receivers WHERE id=?`, id).Scan(&registered); err != nil {
		return errors.New("receiver not found")
	}
	tx, err := s.begin()
	if err != nil {
		return err
	}
	rows, err := tx.Query(`SELECT node_sid,selected_receivers FROM udp_node_settings`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	type nodeSelection struct{ sid, raw string }
	var selections []nodeSelection
	for rows.Next() {
		var item nodeSelection
		if err := rows.Scan(&item.sid, &item.raw); err != nil {
			_ = rows.Close()
			_ = tx.Rollback()
			return err
		}
		selections = append(selections, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		_ = tx.Rollback()
		return err
	}
	_ = rows.Close()
	for _, item := range selections {
		var selected []string
		_ = json.Unmarshal([]byte(item.raw), &selected)
		filtered := selected[:0]
		for _, receiverID := range selected {
			if receiverID != id {
				filtered = append(filtered, receiverID)
			}
		}
		if len(filtered) != len(selected) {
			raw, _ := json.Marshal(filtered)
			if _, err := tx.Exec(`UPDATE udp_node_settings SET selected_receivers=? WHERE node_sid=?`, string(raw), item.sid); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}
	for _, q := range []string{
		`DELETE FROM udp_receiver_exclusions WHERE receiver_id=?`,
		`DELETE FROM udp_raw_blocks WHERE receiver_id=?`,
		`DELETE FROM udp_aggregates WHERE receiver_id=?`,
		`DELETE FROM udp_events WHERE receiver_id=?`,
	} {
		if _, err := tx.Exec(q, id); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if _, err := tx.Exec(`DELETE FROM secrets WHERE k=? OR k=?`, "udp.receiver."+id+".key.1", "udp.receiver."+id+".control"); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.Exec(`DELETE FROM udp_receivers WHERE id=?`, id); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if registered > 0 {
		_, err = s.bumpUDPRevision()
	}
	return err
}

type UDPNodeSetting struct {
	NodeSID           string   `json:"nodeSid"`
	Enabled           bool     `json:"enabled"`
	ReceiverMode      string   `json:"receiverMode"`
	SelectedReceivers []string `json:"selectedReceivers"`
	Diagnostic        bool     `json:"diagnostic"`
}

func (s *Store) UDPNodeSettings() (map[string]UDPNodeSetting, error) {
	rows, err := s.query(`SELECT node_sid,enabled,receiver_mode,selected_receivers,diagnostic FROM udp_node_settings`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]UDPNodeSetting{}
	for rows.Next() {
		var item UDPNodeSetting
		var enabled, diagnostic int
		var selected string
		if err := rows.Scan(&item.NodeSID, &enabled, &item.ReceiverMode, &selected, &diagnostic); err != nil {
			return nil, err
		}
		item.Enabled = enabled != 0
		item.Diagnostic = diagnostic != 0
		_ = json.Unmarshal([]byte(selected), &item.SelectedReceivers)
		out[item.NodeSID] = item
	}
	return out, rows.Err()
}

func (s *Store) SetUDPNodeSetting(item UDPNodeSetting) error {
	if item.ReceiverMode != "all" && item.ReceiverMode != "selected" && item.ReceiverMode != "automatic" {
		return errors.New("receiverMode must be all, selected or automatic")
	}
	selected, _ := json.Marshal(item.SelectedReceivers)
	enabled, diagnostic := 0, 0
	if item.Enabled {
		enabled = 1
	}
	if item.Diagnostic {
		diagnostic = 1
	}
	_, err := s.exec(`INSERT INTO udp_node_settings(node_sid,enabled,receiver_mode,selected_receivers,diagnostic)
		VALUES(?,?,?,?,?) ON CONFLICT(node_sid) DO UPDATE SET enabled=excluded.enabled,receiver_mode=excluded.receiver_mode,selected_receivers=excluded.selected_receivers,diagnostic=excluded.diagnostic`,
		item.NodeSID, enabled, item.ReceiverMode, string(selected), diagnostic)
	if err != nil {
		return err
	}
	_, err = s.bumpUDPRevision()
	return err
}

func (s *Store) ReceiverDataKey(id string, version uint16) (string, error) {
	return s.GetSecret(fmt.Sprintf("udp.receiver.%s.key.%d", id, version))
}

type UDPCheckerReceiver struct {
	UDPReceiver
	KeyVersion uint16 `json:"keyVersion"`
	Key        string `json:"key"`
}

type UDPCheckerConfig struct {
	Revision  int64                     `json:"revision"`
	Settings  UDPSettings               `json:"settings"`
	Receivers []UDPCheckerReceiver      `json:"receivers"`
	Nodes     map[string]UDPNodeSetting `json:"nodes"`
}

type UDPPerformanceEstimate struct {
	Nodes                    int     `json:"nodes"`
	Receivers                int     `json:"receivers"`
	ActivePaths              int     `json:"activePaths"`
	ExpectedAssociations     int     `json:"expectedAssociations"`
	ProbesPerSecond          float64 `json:"probesPerSecond"`
	ExpectedRepliesPerSecond float64 `json:"expectedRepliesPerSecond"`
	ApproxPacketsPerSecond   float64 `json:"approxPacketsPerSecond"`
	ApproxNetworkBytesSecond int64   `json:"approxNetworkBytesSecond"`
	SamplesPerWindow         float64 `json:"samplesPerWindow"`
	EstimatedRawRAMBytes     int64   `json:"estimatedRawRamBytes"`
	CompactBytes24H          int64   `json:"compactBytes24h"`
	RawRetentionHours        int     `json:"rawRetentionHours"`
}

// UDPPerformance returns a conservative estimate from the actual node modes,
// exclusions and configured rate. CompactBytes24H is only the two-byte slot
// payload, deliberately excluding database/index overhead.
func (s *Store) UDPPerformance() (UDPPerformanceEstimate, error) {
	settings := s.UDPSettings()
	receivers, err := s.UDPReceivers()
	if err != nil {
		return UDPPerformanceEstimate{}, err
	}
	nodes, err := s.CurrentRows()
	if err != nil {
		return UDPPerformanceEstimate{}, err
	}
	nodeSettings, err := s.UDPNodeSettings()
	if err != nil {
		return UDPPerformanceEstimate{}, err
	}
	out := UDPPerformanceEstimate{Nodes: len(nodes), RawRetentionHours: settings.RawRetentionHours}
	for _, receiver := range receivers {
		if receiver.Enabled {
			out.Receivers++
		}
	}
	if !settings.Enabled {
		return out, nil
	}
	for _, node := range nodes {
		item, configured := nodeSettings[node.SID]
		if !configured {
			item = UDPNodeSetting{NodeSID: node.SID, Enabled: true, ReceiverMode: "all"}
		}
		if !item.Enabled {
			continue
		}
		selected := make(map[string]bool, len(item.SelectedReceivers))
		for _, id := range item.SelectedReceivers {
			selected[id] = true
		}
		eligible := 0
		for _, receiver := range receivers {
			if !receiver.Enabled {
				continue
			}
			excluded := false
			for _, sid := range receiver.ExcludedNodes {
				if sid == node.SID {
					excluded = true
					break
				}
			}
			if excluded || (item.ReceiverMode == "selected" && !selected[receiver.ID]) {
				continue
			}
			eligible++
		}
		if item.ReceiverMode == "automatic" && settings.AutoReceiverLimit > 0 && eligible > settings.AutoReceiverLimit {
			eligible = settings.AutoReceiverLimit
		}
		intervalMS := settings.ProbeIntervalMS
		if item.Diagnostic && intervalMS > 25 {
			intervalMS = 25
		}
		if intervalMS <= 0 {
			intervalMS = 100
		}
		out.ActivePaths += eligible
		out.ProbesPerSecond += float64(eligible) * 1000 / float64(intervalMS)
		if eligible > 0 {
			out.ExpectedAssociations++
		}
	}
	out.ExpectedRepliesPerSecond = out.ProbesPerSecond
	out.ApproxPacketsPerSecond = out.ProbesPerSecond * 2
	// Authenticated probe payload is 96 bytes; add IPv4 UDP/IP framing for a
	// useful lower-bound estimate. SOCKS/Xray overhead depends on the outbound.
	out.ApproxNetworkBytesSecond = int64(out.ProbesPerSecond * 2 * (96 + 28))
	intervalMS := settings.ProbeIntervalMS
	if intervalMS <= 0 {
		intervalMS = 100
	}
	out.SamplesPerWindow = float64(settings.SlidingWindowSeconds*1000) / float64(intervalMS)
	// Sample includes timestamp, RTT, sequence and status; use a conservative
	// 32-byte operational estimate rather than Go's implementation-specific size.
	out.EstimatedRawRAMBytes = int64(float64(out.ActivePaths) * out.SamplesPerWindow * 32)
	out.CompactBytes24H = int64(out.ProbesPerSecond * float64(settings.RawRetentionHours*3600) * 2)
	return out, nil
}

func (s *Store) UDPCheckerConfig() (UDPCheckerConfig, error) {
	receivers, err := s.UDPReceivers()
	if err != nil {
		return UDPCheckerConfig{}, err
	}
	out := UDPCheckerConfig{Revision: s.UDPRevision(), Settings: s.UDPSettings()}
	for _, receiver := range receivers {
		if !receiver.Enabled {
			continue
		}
		key, err := s.ReceiverDataKey(receiver.ID, 1)
		if err != nil {
			return UDPCheckerConfig{}, err
		}
		out.Receivers = append(out.Receivers, UDPCheckerReceiver{UDPReceiver: receiver, KeyVersion: 1, Key: key})
	}
	out.Nodes, err = s.UDPNodeSettings()
	return out, err
}

func (s *Store) CleanupUDPHistory(now time.Time) error {
	settings := s.UDPSettings()
	_, err := s.exec(`DELETE FROM udp_raw_blocks WHERE start_ms < ?`, now.Add(-time.Duration(settings.RawRetentionHours)*time.Hour).UnixMilli())
	if err != nil {
		return err
	}
	_, err = s.exec(`DELETE FROM udp_aggregates WHERE ts < ?`, now.AddDate(0, 0, -settings.AggregateRetentionDays).Unix())
	return err
}

func (s *Store) RecordUDPEvent(kind, nodeSID, receiverID, details string, now time.Time) error {
	_, err := s.exec(`INSERT INTO udp_events(ts,kind,node_sid,receiver_id,details) VALUES(?,?,?,?,?)`, now.Unix(), kind, nodeSID, receiverID, details)
	return err
}
