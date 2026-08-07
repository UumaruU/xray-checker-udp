package metrics

import (
	"time"

	"xray-checker/udpmon"

	"github.com/prometheus/client_golang/prometheus"
)

// UDPCollector exposes the data-plane and self-monitoring signals needed to
// distinguish network loss from a Checker that is falling behind.
type UDPCollector struct {
	runtime  *udpmon.Runtime
	reporter *udpmon.Reporter
	health   *udpmon.DirectHealthMonitor
}

func NewUDPCollector(runtime *udpmon.Runtime, reporter *udpmon.Reporter, health *udpmon.DirectHealthMonitor) *UDPCollector {
	return &UDPCollector{runtime: runtime, reporter: reporter, health: health}
}

func (c *UDPCollector) Describe(chan<- *prometheus.Desc) {}

func (c *UDPCollector) Collect(ch chan<- prometheus.Metric) {
	if c.runtime == nil || c.reporter == nil || c.health == nil {
		return
	}
	stats := c.runtime.Stats()
	constMetric := func(name, help string, typ prometheus.ValueType, value float64) {
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc(name, help, nil, nil), typ, value)
	}
	constMetric("udp_probe_sent_total", "UDP probes actually sent through Xray", prometheus.CounterValue, float64(stats.Sent))
	constMetric("udp_probe_received_total", "Authenticated UDP probe responses received", prometheus.CounterValue, float64(stats.Received))
	constMetric("udp_probe_timeout_total", "Sent UDP probes that reached the network timeout", prometheus.CounterValue, float64(stats.TimedOut))
	constMetric("udp_probe_invalid_total", "Locally invalid UDP observations; never counted as network loss", prometheus.CounterValue, float64(stats.Invalid))
	constMetric("udp_scheduler_scheduled_total", "Probe jobs scheduled", prometheus.CounterValue, float64(stats.Scheduler.Scheduled))
	constMetric("udp_scheduler_dropped_total", "Probe opportunities omitted locally due to scheduler pressure", prometheus.CounterValue, float64(stats.Scheduler.Dropped))
	constMetric("udp_association_error_total", "SOCKS5 UDP association setup failures", prometheus.CounterValue, float64(stats.AssociationError))
	constMetric("udp_association_reconnect_total", "Successful SOCKS5 UDP association reconnects", prometheus.CounterValue, float64(stats.AssociationReconnect))
	constMetric("udp_active_associations", "Persistent SOCKS5 UDP associations currently active", prometheus.GaugeValue, float64(stats.ActiveAssociations))
	constMetric("udp_pending_probes", "Sent probes awaiting response or timeout", prometheus.GaugeValue, float64(stats.Pending))
	constMetric("udp_scheduler_queue", "Current bounded scheduler queue depth", prometheus.GaugeValue, float64(stats.Scheduler.Queue))
	constMetric("udp_scheduler_queue_capacity", "Bounded scheduler queue capacity", prometheus.GaugeValue, float64(stats.Scheduler.QueueCapacity))
	constMetric("udp_scheduler_lag_seconds", "Current probe worker scheduling lag", prometheus.GaugeValue, float64(stats.Scheduler.LagNS)/float64(time.Second))
	constMetric("udp_reporter_queue", "Current bounded Panel reporter queue depth", prometheus.GaugeValue, float64(c.reporter.QueueLen()))
	constMetric("udp_reporter_dropped_total", "Samples omitted locally because the Panel reporter queue was full", prometheus.CounterValue, float64(c.reporter.Dropped()))
	constMetric("udp_reporter_send_error_total", "Failed Checker to Panel report batches", prometheus.CounterValue, float64(c.reporter.SendErrors()))
	constMetric("udp_config_revision", "Last Panel UDP configuration revision applied to Reporter", prometheus.GaugeValue, float64(c.reporter.Revision()))
	constMetric("udp_config_apply_errors_total", "Panel UDP configuration revisions rejected locally", prometheus.CounterValue, float64(c.reporter.ConfigApplyErrors()))

	now := time.Now()
	health := c.health.Snapshot(now)
	paths, _ := c.runtime.Aggregates(now, health, udpmon.DefaultThresholds())
	for _, path := range paths {
		labels := []string{"node_id", "receiver_id"}
		values := []string{path.NodeID, path.ReceiverID}
		pathMetric := func(name, help string, value float64) {
			ch <- prometheus.MustNewConstMetric(prometheus.NewDesc(name, help, labels, nil), prometheus.GaugeValue, value, values...)
		}
		pathMetric("udp_rtt_milliseconds", "Median RTT over the active UDP window", float64(path.MedianRTT)/float64(time.Millisecond))
		pathMetric("udp_rtt_p95_milliseconds", "P95 RTT over the active UDP window", float64(path.P95RTT)/float64(time.Millisecond))
		pathMetric("udp_jitter_milliseconds", "Median absolute successive RTT difference", float64(path.Jitter)/float64(time.Millisecond))
		pathMetric("udp_loss_ratio", "Network loss among probes actually sent", path.Loss)
		pathMetric("udp_path_coverage_ratio", "Executed-probe coverage of the expected time grid", path.Coverage)
	}
	for receiverID, item := range health {
		labels := []string{"receiver_id", "state"}
		values := []string{receiverID, string(item.State)}
		receiverMetric := func(name, help string, value float64) {
			ch <- prometheus.MustNewConstMetric(prometheus.NewDesc(name, help, labels, nil), prometheus.GaugeValue, value, values...)
		}
		healthy := 0.0
		if item.State == udpmon.ReceiverHealthy {
			healthy = 1
		}
		receiverMetric("udp_receiver_healthy", "Whether Receiver health is trusted as healthy", healthy)
		receiverMetric("udp_receiver_confidence", "Receiver health confidence used for VLESS status weighting", item.Confidence)
		receiverMetric("udp_receiver_direct_loss", "Checker to Receiver direct control-path loss", item.DirectLoss)
		receiverMetric("udp_receiver_direct_rtt", "Checker to Receiver direct control-path RTT milliseconds", float64(item.DirectRTT)/float64(time.Millisecond))
		receiverMetric("udp_receiver_heartbeat_age", "Receiver heartbeat age in seconds", float64(item.HeartbeatAge)/float64(time.Second))
		receiverMetric("udp_receiver_peer_success_ratio", "Receiver peer-mesh corroboration success ratio", item.PeerSuccess)
	}
	load := c.reporter.LoadSnapshot()
	constMetric("udp_load_level", "Adaptive load protection level (0 normal, 1-4 overloaded)", prometheus.GaugeValue, float64(load.Level))
	constMetric("udp_active_paths", "UDP node by Receiver paths currently active", prometheus.GaugeValue, float64(load.ActivePaths))
	constMetric("udp_playback_delay_seconds", "Current live graph playback buffer", prometheus.GaugeValue, float64(load.PlaybackDelayMS)/1000)
}
