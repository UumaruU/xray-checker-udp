package udpmon

import "time"

type State string

const (
	StateOnline      State = "ONLINE"
	StateDegraded    State = "DEGRADED"
	StateOffline     State = "OFFLINE"
	StateUnknown     State = "UNKNOWN"
	StateUnsupported State = "UNSUPPORTED"
	StateDisabled    State = "DISABLED"
)

type ReceiverState string

const (
	ReceiverHealthy     ReceiverState = "HEALTHY"
	ReceiverDegraded    ReceiverState = "DEGRADED"
	ReceiverUnreachable ReceiverState = "UNREACHABLE"
	ReceiverUnknown     ReceiverState = "UNKNOWN"
)

type SampleStatus uint8

const (
	SampleOK SampleStatus = iota
	SampleLoss
	SampleInvalid
)

type Sample struct {
	At       time.Time
	Sequence uint64
	RTT      time.Duration
	Status   SampleStatus
}

type ReceiverHealth struct {
	ID             string        `json:"id"`
	State          ReceiverState `json:"state"`
	Confidence     float64       `json:"confidence"`
	HeartbeatAge   time.Duration `json:"heartbeatAge"`
	DirectRTT      time.Duration `json:"directRtt"`
	DirectLoss     float64       `json:"directLoss"`
	PeerSuccess    float64       `json:"peerSuccess"`
	ConfigRevision int64         `json:"configRevision"`
}

func (h ReceiverHealth) Usable(minConfidence float64) bool {
	if h.State != ReceiverHealthy && h.State != ReceiverDegraded {
		return false
	}
	return h.Confidence >= minConfidence
}

type PathAggregate struct {
	NodeID       string        `json:"nodeId"`
	ReceiverID   string        `json:"receiverId"`
	State        State         `json:"state"`
	Sent         int           `json:"sent"`
	Received     int           `json:"received"`
	Loss         float64       `json:"loss"`
	MedianRTT    time.Duration `json:"medianRtt"`
	P90RTT       time.Duration `json:"p90Rtt"`
	P95RTT       time.Duration `json:"p95Rtt"`
	Jitter       time.Duration `json:"jitter"`
	Coverage     float64       `json:"coverage"`
	LastValid    time.Time     `json:"lastValid"`
	SampleCount  int           `json:"sampleCount"`
	Expected     int           `json:"expectedSamples"`
	HealthyInput bool          `json:"healthyInput"`
}

type NodeAggregate struct {
	NodeID               string          `json:"nodeId"`
	State                State           `json:"state"`
	Confidence           string          `json:"confidence"`
	RTT                   time.Duration   `json:"rtt"`
	P95                   time.Duration   `json:"p95"`
	Jitter                time.Duration   `json:"jitter"`
	LossConsensus         float64         `json:"lossConsensus"`
	LossWorstHealthy      float64         `json:"lossWorstHealthy"`
	ReceiverSuccessRatio  float64         `json:"receiverSuccessRatio"`
	ReceiverDisagreement  float64         `json:"receiverDisagreement"`
	HealthyReceivers      int             `json:"healthyReceivers"`
	TotalReceivers        int             `json:"totalReceivers"`
	LastValid             time.Time       `json:"lastValid"`
	Paths                 []PathAggregate `json:"paths"`
}

type Thresholds struct {
	OnlineLoss          float64
	DegradedLoss        float64
	OfflineLoss         float64
	Jitter              time.Duration
	MinSamples          int
	MinCoverage         float64
	StaleAfter          time.Duration
	ReceiverConfidence  float64
	Disagreement        float64
}

func DefaultThresholds() Thresholds {
	return Thresholds{
		OnlineLoss:         0.01,
		DegradedLoss:       0.05,
		OfflineLoss:        0.95,
		Jitter:             25 * time.Millisecond,
		MinSamples:         20,
		MinCoverage:        0.70,
		StaleAfter:         5 * time.Second,
		ReceiverConfidence: 0.70,
		Disagreement:       0.20,
	}
}
