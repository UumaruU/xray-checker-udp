package udpmon

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ControlSettings struct {
	Enabled                  bool    `json:"enabled"`
	ProbeIntervalMS          int     `json:"probeIntervalMs"`
	ProbeTimeoutMS           int     `json:"probeTimeoutMs"`
	SlidingWindowSeconds     int     `json:"slidingWindowSeconds"`
	MinSamples               int     `json:"minSamples"`
	MinCoverage              float64 `json:"minCoverage"`
	AggregationMS            int     `json:"aggregationMs"`
	CheckerBatchMS           int     `json:"checkerBatchMs"`
	PlaybackDelayMS          int     `json:"playbackDelayMs"`
	ReceiverConfidence       float64 `json:"receiverConfidence"`
	OnlineLoss               float64 `json:"onlineLoss"`
	DegradedLoss             float64 `json:"degradedLoss"`
	OfflineLoss              float64 `json:"offlineLoss"`
	JitterThresholdMS        int     `json:"jitterThresholdMs"`
	StaleTimeoutMS           int     `json:"staleTimeoutMs"`
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

func (s ControlSettings) Thresholds() Thresholds {
	t := DefaultThresholds()
	if s.OnlineLoss >= 0 {
		t.OnlineLoss = s.OnlineLoss
	}
	if s.DegradedLoss > 0 {
		t.DegradedLoss = s.DegradedLoss
	}
	if s.OfflineLoss > 0 {
		t.OfflineLoss = s.OfflineLoss
	}
	if s.JitterThresholdMS > 0 {
		t.Jitter = time.Duration(s.JitterThresholdMS) * time.Millisecond
	}
	if s.MinSamples > 0 {
		t.MinSamples = s.MinSamples
	}
	if s.MinCoverage > 0 {
		t.MinCoverage = s.MinCoverage
	}
	if s.StaleTimeoutMS > 0 {
		t.StaleAfter = time.Duration(s.StaleTimeoutMS) * time.Millisecond
	}
	if s.ReceiverConfidence > 0 {
		t.ReceiverConfidence = s.ReceiverConfidence
	}
	if s.ReceiverDisagreement > 0 {
		t.Disagreement = s.ReceiverDisagreement
	}
	return t
}

type ControlReceiver struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Host          string   `json:"host"`
	UDPPort       int      `json:"udpPort"`
	Location      string   `json:"location"`
	Enabled       bool     `json:"enabled"`
	ExcludedNodes []string `json:"excludedNodes"`
	KeyVersion    uint16   `json:"keyVersion"`
	Key           string   `json:"key"`
	HeartbeatTS   int64    `json:"heartbeatTs"`
	RegisteredTS  int64    `json:"registeredTs"`
	Version       string   `json:"version"`
	Health        string   `json:"health"`
}

type ControlNode struct {
	NodeSID           string   `json:"nodeSid"`
	Enabled           bool     `json:"enabled"`
	ReceiverMode      string   `json:"receiverMode"`
	SelectedReceivers []string `json:"selectedReceivers"`
	Diagnostic        bool     `json:"diagnostic"`
}

type ControlConfig struct {
	Revision  int64                  `json:"revision"`
	Settings  ControlSettings        `json:"settings"`
	Receivers []ControlReceiver      `json:"receivers"`
	Nodes     map[string]ControlNode `json:"nodes"`
}

type NodeDescriptor struct {
	StableID     string
	SocksAddress string
	SupportsUDP  bool
}

type ControlClient struct {
	url   string
	token string
	http  *http.Client
}

func NewControlClient(panelURL, token string) *ControlClient {
	return &ControlClient{
		url: strings.TrimRight(panelURL, "/") + "/api/v1/udp/config", token: token,
		http: &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *ControlClient) Fetch(ctx context.Context) (ControlConfig, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return ControlConfig{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("User-Agent", "xray-checker-udp")
	resp, err := c.http.Do(req)
	if err != nil {
		return ControlConfig{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ControlConfig{}, fmt.Errorf("Panel UDP config returned HTTP %d", resp.StatusCode)
	}
	var cfg ControlConfig
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return ControlConfig{}, err
	}
	return cfg, nil
}

func receiverExcluded(receiver ControlReceiver, nodeID string) bool {
	for _, excluded := range receiver.ExcludedNodes {
		if excluded == nodeID {
			return true
		}
	}
	return false
}

func selectedReceiver(node ControlNode, receiverID string) bool {
	for _, selected := range node.SelectedReceivers {
		if selected == receiverID {
			return true
		}
	}
	return false
}

func receiverScore(nodeID, receiverID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(nodeID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(receiverID))
	return h.Sum64()
}

func chooseReceivers(node ControlNode, nodeID string, receivers []ControlReceiver, limit int) []ControlReceiver {
	eligible := make([]ControlReceiver, 0, len(receivers))
	for _, receiver := range receivers {
		if !receiver.Enabled || receiverExcluded(receiver, nodeID) {
			continue
		}
		if node.ReceiverMode == "selected" && !selectedReceiver(node, receiver.ID) {
			continue
		}
		eligible = append(eligible, receiver)
	}
	if node.ReceiverMode != "automatic" || limit <= 0 || len(eligible) <= limit {
		return eligible
	}
	sort.Slice(eligible, func(i, j int) bool {
		return receiverScore(nodeID, eligible[i].ID) < receiverScore(nodeID, eligible[j].ID)
	})
	// Prefer distinct declared locations first. This is only a diversity hint
	// (Location is not an ASN guarantee), then deterministic hashing fills the
	// remaining slots so config reloads do not randomly reshuffle paths.
	selected := make([]ControlReceiver, 0, limit)
	used := make([]bool, len(eligible))
	locations := make(map[string]bool)
	for i, receiver := range eligible {
		if len(selected) == limit {
			break
		}
		location := strings.TrimSpace(strings.ToLower(receiver.Location))
		if location == "" || locations[location] {
			continue
		}
		locations[location] = true
		used[i] = true
		selected = append(selected, receiver)
	}
	for i, receiver := range eligible {
		if len(selected) == limit {
			break
		}
		if used[i] {
			continue
		}
		selected = append(selected, receiver)
	}
	return selected
}

func BuildPaths(cfg ControlConfig, nodes []NodeDescriptor) ([]PathConfig, error) {
	if !cfg.Settings.Enabled {
		return nil, nil
	}
	interval := time.Duration(cfg.Settings.ProbeIntervalMS) * time.Millisecond
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	timeout := time.Duration(cfg.Settings.ProbeTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 1500 * time.Millisecond
	}
	paths := make([]PathConfig, 0, len(nodes)*len(cfg.Receivers))
	for _, descriptor := range nodes {
		if !descriptor.SupportsUDP || descriptor.StableID == "" {
			continue
		}
		node, configured := cfg.Nodes[descriptor.StableID]
		if !configured {
			node = ControlNode{NodeSID: descriptor.StableID, Enabled: true, ReceiverMode: "all"}
		}
		if !node.Enabled {
			continue
		}
		nodeInterval := interval
		if node.Diagnostic && nodeInterval > 25*time.Millisecond {
			nodeInterval = 25 * time.Millisecond
		}
		for _, receiver := range chooseReceivers(node, descriptor.StableID, cfg.Receivers, cfg.Settings.AutoReceiverLimit) {
			key, err := base64.StdEncoding.DecodeString(receiver.Key)
			if err != nil || len(key) < 16 {
				return nil, fmt.Errorf("receiver %s has invalid data-plane key", receiver.ID)
			}
			paths = append(paths, PathConfig{
				NodeID: descriptor.StableID, ReceiverID: receiver.ID, SocksAddress: descriptor.SocksAddress,
				Target: net.JoinHostPort(receiver.Host, strconv.Itoa(receiver.UDPPort)), KeyVersion: receiver.KeyVersion,
				Key: key, ProbeInterval: nodeInterval, ProbeTimeout: timeout,
			})
		}
	}
	return paths, nil
}

func PathConfigsEqual(a, b []PathConfig) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].NodeID != b[i].NodeID || a[i].ReceiverID != b[i].ReceiverID ||
			a[i].SocksAddress != b[i].SocksAddress || a[i].Target != b[i].Target ||
			a[i].KeyVersion != b[i].KeyVersion || a[i].ProbeInterval != b[i].ProbeInterval ||
			a[i].ProbeTimeout != b[i].ProbeTimeout || !bytes.Equal(a[i].Key, b[i].Key) {
			return false
		}
	}
	return true
}
