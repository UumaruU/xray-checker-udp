package udpmon

type trackedNodeState struct {
	current   State
	candidate State
	streak    int
}

type StateTracker struct{ nodes map[string]trackedNodeState }

func NewStateTracker() *StateTracker { return &StateTracker{nodes: make(map[string]trackedNodeState)} }

func (t *StateTracker) Apply(values []NodeAggregate, cfg ControlSettings) []NodeAggregate {
	for i := range values {
		value := &values[i]
		tracked := t.nodes[value.NodeID]
		if tracked.current == "" {
			tracked.current = value.State
			t.nodes[value.NodeID] = tracked
			continue
		}
		if value.State == StateUnknown || value.State == StateDisabled || value.State == StateUnsupported {
			tracked.current, tracked.candidate, tracked.streak = value.State, "", 0
			t.nodes[value.NodeID] = tracked
			continue
		}
		if value.State == tracked.current {
			tracked.candidate, tracked.streak = "", 0
			t.nodes[value.NodeID] = tracked
			continue
		}
		if tracked.candidate != value.State {
			tracked.candidate, tracked.streak = value.State, 1
		} else {
			tracked.streak++
		}
		required := 1
		switch value.State {
		case StateOffline:
			required = cfg.OfflineStreak
		case StateDegraded:
			required = cfg.DegradedStreak
		case StateOnline:
			required = cfg.RecoveryStreak
		}
		if required < 1 {
			required = 3
		}
		if tracked.streak >= required {
			tracked.current, tracked.candidate, tracked.streak = value.State, "", 0
		} else {
			value.State = tracked.current
		}
		t.nodes[value.NodeID] = tracked
	}
	return values
}
