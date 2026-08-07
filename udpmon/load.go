package udpmon

import (
	"sort"
	"time"
)

type LoadState string

const (
	LoadHealthy    LoadState = "HEALTHY"
	LoadHigh       LoadState = "HIGH_LOAD"
	LoadOverloaded LoadState = "OVERLOADED"
)

type LoadStatus struct {
	State           LoadState `json:"state"`
	Level           int       `json:"level"`
	BasePaths       int       `json:"basePaths"`
	ActivePaths     int       `json:"activePaths"`
	ProbeScale      float64   `json:"probeScale"`
	PlaybackDelayMS int       `json:"playbackDelayMs"`
}

type LoadController struct {
	state         LoadState
	level         int
	overloadedFor int
	healthyFor    int
	lastDropped   uint64
}

func NewLoadController() *LoadController { return &LoadController{state: LoadHealthy} }

func (c *LoadController) Observe(cfg ControlSettings, runtime RuntimeStats, reporterQueue, reporterCapacity int) LoadStatus {
	lagLimit := time.Duration(cfg.OverloadLagMS) * time.Millisecond
	if lagLimit <= 0 {
		lagLimit = 500 * time.Millisecond
	}
	queueLimit := cfg.OverloadQueueRatio
	if queueLimit <= 0 || queueLimit > 1 {
		queueLimit = 0.75
	}
	queueRatio := 0.0
	if runtime.Scheduler.QueueCapacity > 0 {
		queueRatio = float64(runtime.Scheduler.Queue) / float64(runtime.Scheduler.QueueCapacity)
	}
	reporterRatio := 0.0
	if reporterCapacity > 0 {
		reporterRatio = float64(reporterQueue) / float64(reporterCapacity)
	}
	droppedNow := runtime.Scheduler.Dropped > c.lastDropped
	c.lastDropped = runtime.Scheduler.Dropped
	overloadSignal := time.Duration(runtime.Scheduler.LagNS) >= lagLimit || queueRatio >= queueLimit || reporterRatio >= queueLimit || droppedNow
	sustain := cfg.OverloadSustainSeconds
	if sustain < 2 {
		sustain = 5
	}
	if overloadSignal {
		c.overloadedFor++
		c.healthyFor = 0
		if c.overloadedFor < sustain {
			c.state = LoadHigh
		} else {
			c.state = LoadOverloaded
			if c.level < 1 {
				c.level = 1
			}
			if c.overloadedFor > sustain && c.overloadedFor%sustain == 0 && c.level < 4 {
				c.level++
			}
		}
	} else {
		c.overloadedFor = 0
		c.healthyFor++
		if c.healthyFor >= 10 {
			if c.level > 0 {
				c.level--
			}
			c.healthyFor = 0
		}
		if c.level == 0 {
			c.state = LoadHealthy
		} else {
			c.state = LoadHigh
		}
	}
	scale := 1.0
	if c.state == LoadOverloaded {
		scale = loadScale(c.level)
	}
	return LoadStatus{State: c.state, Level: c.level, ProbeScale: scale, PlaybackDelayMS: cfg.PlaybackDelayMS}
}

func loadScale(level int) float64 {
	switch level {
	case 1:
		return 1.25 // 10Hz -> 8Hz
	case 2:
		return 2 // 10Hz -> 5Hz
	case 3:
		return 5 // 10Hz -> 2Hz
	case 4:
		return 10 // 10Hz -> 1Hz
	default:
		return 1
	}
}

func ApplyLoadPolicy(base []PathConfig, cfg ControlSettings, status LoadStatus, problemNodes map[string]bool) ([]PathConfig, LoadStatus) {
	paths := make([]PathConfig, len(base))
	copy(paths, base)
	status.BasePaths = len(base)
	if status.State != LoadOverloaded || status.Level == 0 {
		status.ProbeScale = 1
		status.ActivePaths = len(paths)
		return paths, status
	}
	if cfg.AutoManageDisplayBuffer {
		status.PlaybackDelayMS = cfg.PlaybackDelayMS + status.Level*500
	}
	if cfg.AutoManageRate {
		scale := loadScale(status.Level)
		minHz := cfg.MinProbeHz
		if minHz <= 0 {
			minHz = 1
		}
		maxInterval := time.Duration(float64(time.Second) / minHz)
		for i := range paths {
			if cfg.PrioritizeProblemNodes && problemNodes[paths[i].NodeID] {
				continue
			}
			interval := time.Duration(float64(paths[i].ProbeInterval) * scale)
			if interval > maxInterval {
				interval = maxInterval
			}
			paths[i].ProbeInterval = interval
		}
	}
	if cfg.AutoManageReceivers && status.Level >= 3 {
		limit := cfg.AutoReceiverLimit
		if limit < 1 {
			limit = 5
		}
		if status.Level >= 4 && limit > 3 {
			limit = 3
		}
		byNode := make(map[string][]PathConfig)
		for _, path := range paths {
			byNode[path.NodeID] = append(byNode[path.NodeID], path)
		}
		paths = paths[:0]
		for nodeID, group := range byNode {
			if cfg.PrioritizeProblemNodes && problemNodes[nodeID] {
				paths = append(paths, group...)
				continue
			}
			if len(group) > limit {
				sort.Slice(group, func(i, j int) bool {
					return receiverScore(nodeID, group[i].ReceiverID) < receiverScore(nodeID, group[j].ReceiverID)
				})
				group = group[:limit]
			}
			paths = append(paths, group...)
		}
	}
	status.ActivePaths = len(paths)
	return paths, status
}
