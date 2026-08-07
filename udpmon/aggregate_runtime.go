package udpmon

import "time"

func (r *Runtime) Aggregates(now time.Time, health map[string]ReceiverHealth, thresholds Thresholds) ([]PathAggregate, []NodeAggregate) {
	r.pathsMu.RLock()
	paths := make([]*RuntimePath, 0, len(r.paths))
	for _, path := range r.paths {
		paths = append(paths, path)
	}
	r.pathsMu.RUnlock()
	pathAggregates := make([]PathAggregate, 0, len(paths))
	byNode := make(map[string][]PathAggregate)
	for _, path := range paths {
		samples := path.Window.Snapshot(now)
		agg := AggregatePath(path.Config.NodeID, path.Config.ReceiverID, samples, path.Config.ProbeInterval, health[path.Config.ReceiverID], thresholds, now)
		pathAggregates = append(pathAggregates, agg)
		byNode[path.Config.NodeID] = append(byNode[path.Config.NodeID], agg)
	}
	nodes := make([]NodeAggregate, 0, len(byNode))
	for nodeID, nodePaths := range byNode {
		nodes = append(nodes, AggregateNode(nodeID, nodePaths, health, thresholds, true, true))
	}
	return pathAggregates, nodes
}
