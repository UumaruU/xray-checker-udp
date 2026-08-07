package main

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"
	"xray-checker/checker"
	"xray-checker/config"
	"xray-checker/logger"
	"xray-checker/metrics"
	"xray-checker/models"
	"xray-checker/subscription"
	"xray-checker/udpmon"
	"xray-checker/web"
	"xray-checker/xray"

	"github.com/go-co-op/gocron"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	version   = "unknown"
	startTime = time.Now()
)

func main() {
	config.Parse(version)

	logLevel := logger.ParseLevel(config.CLIConfig.LogLevel)
	logger.SetLevel(logLevel)

	logger.Startup("Xray Checker %s", version)
	if logLevel == logger.LevelNone {
		logger.Startup("Log level: none (silent mode)")
	}

	if err := web.InitAssetLoader(config.CLIConfig.Web.CustomAssetsPath); err != nil {
		logger.Fatal("Failed to initialize custom assets: %v", err)
	}

	geoManager := xray.NewGeoFileManager("")
	if err := geoManager.EnsureGeoFiles(); err != nil {
		logger.Fatal("Failed to ensure geo files: %v", err)
	}

	configFile := "xray_config.json"
	proxyConfigs, err := subscription.InitializeConfiguration(configFile, version)
	if err != nil {
		logger.Fatal("Error initializing configuration: %v", err)
	}

	logger.Info("Loaded %d proxy configurations", len(*proxyConfigs))

	if config.CLIConfig.Web.Public {
		if name := subscription.GetSubscriptionName(); name != "" {
			logger.Info("Subscription name for public status page: %s", name)
		}
	} else {
		subNames := web.CollectSubscriptionNames(*proxyConfigs)
		if len(subNames) > 0 {
			logger.Info("Subscriptions: %s", strings.Join(subNames, ", "))
		}
	}

	if logLevel == logger.LevelDebug {
		logger.Debug("=== Parsed Proxy Configurations ===")
		for _, pc := range *proxyConfigs {
			logger.Debug("%s", pc.DebugString())
		}
	}

	xrayRunner := xray.NewRunner(configFile)
	if err := xrayRunner.Start(); err != nil {
		logger.Fatal("Error starting Xray: %v", err)
	}

	defer func() {
		if err := xrayRunner.Stop(); err != nil {
			logger.Error("Error stopping Xray: %v", err)
		}
	}()

	proxyChecker := checker.NewProxyChecker(
		*proxyConfigs,
		config.CLIConfig.Xray.StartPort,
		config.CLIConfig.Proxy.IpCheckUrl,
		config.CLIConfig.Proxy.Timeout,
		config.CLIConfig.Proxy.StatusCheckUrl,
		config.CLIConfig.Proxy.DownloadUrl,
		config.CLIConfig.Proxy.DownloadTimeout,
		config.CLIConfig.Proxy.DownloadMinSize,
		config.CLIConfig.Proxy.CheckMethod,
		config.CLIConfig.Proxy.CheckConcurrency,
	)

	udpCtx, stopUDP := context.WithCancel(context.Background())
	defer stopUDP()
	var udpRuntime *udpmon.Runtime
	var udpReporter *udpmon.Reporter
	var udpReceiverHealth *udpmon.DirectHealthMonitor
	if config.CLIConfig.UDP.PanelToken != "" {
		runtimeCfg := udpmon.DefaultRuntimeConfig()
		runtimeCfg.Workers = runtime.GOMAXPROCS(0)
		if runtimeCfg.Workers < 2 {
			runtimeCfg.Workers = 2
		}
		if runtimeCfg.Workers > 8 {
			runtimeCfg.Workers = 8
		}
		udpRuntime, err = udpmon.NewRuntime(runtimeCfg)
		if err != nil {
			logger.Fatal("Error initializing UDP monitor: %v", err)
		}
		receiverHealth, healthErr := udpmon.NewDirectHealthMonitor()
		if healthErr != nil {
			logger.Fatal("Error initializing UDP receiver health monitor: %v", healthErr)
		}
		reporter := udpmon.NewReporter(config.CLIConfig.UDP.PanelURL, config.CLIConfig.UDP.PanelToken, udpRuntime, receiverHealth)
		udpReporter = reporter
		udpReceiverHealth = receiverHealth
		udpRuntime.OnSample = reporter.Ingest
		go udpRuntime.Run(udpCtx)
		go reporter.Run(udpCtx)
		go func() {
			if err := receiverHealth.Run(udpCtx); err != nil && udpCtx.Err() == nil {
				logger.Error("UDP receiver health monitor stopped: %v", err)
			}
		}()
		controlClient := udpmon.NewControlClient(config.CLIConfig.UDP.PanelURL, config.CLIConfig.UDP.PanelToken)
		refresh := time.Duration(config.CLIConfig.UDP.ConfigRefresh) * time.Second
		if refresh < time.Second {
			refresh = 5 * time.Second
		}
		go runUDPConfigLoop(udpCtx, controlClient, udpRuntime, receiverHealth, reporter, proxyChecker, config.CLIConfig.Xray.StartPort, refresh)
		logger.Info("UDP monitor control enabled; Panel remains source of truth")
	} else {
		logger.Info("UDP monitor bootstrap token is not configured; TCP monitoring continues unchanged")
	}

	// The collector renders metrics from the checker's current proxy snapshot on
	// each scrape, so custom metricsLabels (#124) can change across subscription
	// updates without resetting other series.
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.NewCollector(config.CLIConfig.Metrics.Instance, proxyChecker))
	if udpRuntime != nil {
		registry.MustRegister(metrics.NewUDPCollector(udpRuntime, udpReporter, udpReceiverHealth))
	}

	runCheckIteration := func() {
		logger.Info("Starting proxy check iteration")
		start := time.Now()
		proxyChecker.CheckAllProxies()
		elapsed := time.Since(start)

		// Warn if a cycle overruns the interval: with PROXY_CHECK_CONCURRENCY set,
		// a large/slow proxy set can take longer than PROXY_CHECK_INTERVAL, so checks
		// (and metrics) effectively run less often than configured.
		if interval := config.CLIConfig.Proxy.CheckInterval; interval > 0 && elapsed > time.Duration(interval)*time.Second {
			// When a concurrency cap is set, raising it (or the interval) helps. When
			// unlimited (0), the cycle is already as parallel as it gets, so the only
			// useful lever is a longer interval.
			if config.CLIConfig.Proxy.CheckConcurrency > 0 {
				logger.Warn("Check cycle took %s, longer than PROXY_CHECK_INTERVAL=%ds — raise PROXY_CHECK_CONCURRENCY or PROXY_CHECK_INTERVAL", elapsed.Round(time.Second), interval)
			} else {
				logger.Warn("Check cycle took %s, longer than PROXY_CHECK_INTERVAL=%ds — raise PROXY_CHECK_INTERVAL", elapsed.Round(time.Second), interval)
			}
		}

		if config.CLIConfig.Metrics.PushURL != "" {
			pushConfig, err := metrics.ParseURL(config.CLIConfig.Metrics.PushURL)
			if err != nil {
				logger.Error("Error parsing push URL: %v", err)
				return
			}

			if pushConfig != nil {
				if err := metrics.PushMetrics(pushConfig, registry); err != nil {
					logger.Error("Error pushing metrics: %v", err)
				}
			}
		}
	}

	if config.CLIConfig.RunOnce {
		runCheckIteration()
		logger.Info("Check completed")
		return
	}

	checkScheduler := gocron.NewScheduler(time.UTC)
	// SingletonMode: if a check cycle overruns the interval, the next tick is skipped
	// instead of starting a second concurrent cycle. Without a concurrency limit a
	// cycle is bounded by PROXY_TIMEOUT so this rarely triggers, but with
	// PROXY_CHECK_CONCURRENCY a slow cycle degrades to "runs less often" rather than
	// piling up overlapping runs.
	checkScheduler.Every(config.CLIConfig.Proxy.CheckInterval).Seconds().SingletonMode().Do(func() {
		runCheckIteration()
	})
	checkScheduler.StartAsync()

	if config.CLIConfig.Subscription.Update {
		updateScheduler := gocron.NewScheduler(time.UTC)
		updateScheduler.Every(config.CLIConfig.Subscription.UpdateInterval).Seconds().WaitForSchedule().Do(func() {
			logger.Info("Checking subscriptions for updates...")
			newConfigs, err := subscription.ReadFromMultipleSources(config.CLIConfig.Subscription.URLs)
			if err != nil {
				logger.Error("Error fetching subscriptions: %v", err)
				return
			}

			if config.CLIConfig.Proxy.ResolveDomains {
				resolved, err := subscription.ResolveDomainsForConfigs(newConfigs)
				if err != nil {
					logger.Error("Error resolving domains: %v", err)
				} else {
					newConfigs = resolved
				}
			}

			if !xray.IsConfigsEqual(*proxyConfigs, newConfigs) {
				if err := updateConfiguration(newConfigs, proxyConfigs, xrayRunner, proxyChecker); err != nil {
					logger.Error("Error updating configuration: %v", err)
				} else {
					// Immediately re-check the new proxy set so /metrics is repopulated
					// right away instead of staying empty until the next scheduled check
					// (up to PROXY_CHECK_INTERVAL), then drop series for removed proxies.
					runCheckIteration()
					proxyChecker.PruneStaleResults()
				}
			} else {
				logger.Info("Subscriptions checked, no changes")
			}
		})
		updateScheduler.StartAsync()
	}

	mux, err := web.NewPrefixServeMux(config.CLIConfig.Metrics.BasePath)
	if err != nil {
		logger.Fatal("Error creating web server: %v", err)
	}
	mux.Handle("/health", web.HealthHandler())
	mux.Handle("/static/", web.StaticHandler())
	mux.Handle("/api/v1/public/proxies", web.APIPublicProxiesHandler(proxyChecker))

	web.RegisterConfigEndpoints(*proxyConfigs, proxyChecker, config.CLIConfig.Xray.StartPort)

	protectedHandler := http.NewServeMux()
	protectedHandler.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	protectedHandler.Handle("/config/", web.ConfigStatusHandler(proxyChecker))
	protectedHandler.Handle("/api/v1/proxies/", web.APIProxyHandler(proxyChecker, config.CLIConfig.Xray.StartPort))
	protectedHandler.Handle("/api/v1/proxies", web.APIProxiesHandler(proxyChecker, config.CLIConfig.Xray.StartPort))
	protectedHandler.Handle("/api/v1/config", web.APIConfigHandler(proxyChecker))
	protectedHandler.Handle("/api/v1/status", web.APIStatusHandler(proxyChecker))
	protectedHandler.Handle("/api/v1/system/info", web.APISystemInfoHandler(version, startTime))
	protectedHandler.Handle("/api/v1/system/ip", web.APISystemIPHandler(proxyChecker))
	protectedHandler.Handle("/api/v1/docs", web.APIDocsHandler())
	protectedHandler.Handle("/api/v1/openapi.yaml", web.APIOpenAPIHandler())

	if config.CLIConfig.Web.Public {
		mux.Handle("/", web.IndexHandler(version, proxyChecker))
		mux.Handle("/config/", web.ConfigStatusHandler(proxyChecker))
		middlewareHandler := web.BasicAuthMiddleware(
			config.CLIConfig.Metrics.Username,
			config.CLIConfig.Metrics.Password,
		)(protectedHandler)
		mux.Handle("/metrics", middlewareHandler)
		mux.Handle("/api/", middlewareHandler)
	} else if config.CLIConfig.Metrics.Protected {
		protectedHandler.Handle("/", web.IndexHandler(version, proxyChecker))
		middlewareHandler := web.BasicAuthMiddleware(
			config.CLIConfig.Metrics.Username,
			config.CLIConfig.Metrics.Password,
		)(protectedHandler)
		mux.Handle("/", middlewareHandler)
	} else {
		protectedHandler.Handle("/", web.IndexHandler(version, proxyChecker))
		mux.Handle("/", protectedHandler)
	}

	if !config.CLIConfig.RunOnce {
		logger.Info("Server listening on %s:%s%s",
			config.CLIConfig.Metrics.Host,
			config.CLIConfig.Metrics.Port,
			config.CLIConfig.Metrics.BasePath,
		)
		if err := http.ListenAndServe(config.CLIConfig.Metrics.Host+":"+config.CLIConfig.Metrics.Port, mux); err != nil {
			logger.Fatal("Error starting server: %v", err)
		}
	}
}

func runUDPConfigLoop(ctx context.Context, client *udpmon.ControlClient, runtime *udpmon.Runtime, receiverHealth *udpmon.DirectHealthMonitor, reporter *udpmon.Reporter, proxyChecker *checker.ProxyChecker, startPort int, refresh time.Duration) {
	configTicker := time.NewTicker(refresh)
	loadTicker := time.NewTicker(time.Second)
	defer configTicker.Stop()
	defer loadTicker.Stop()
	loadController := udpmon.NewLoadController()
	var currentCfg udpmon.ControlConfig
	var basePaths []udpmon.PathConfig
	var applied []udpmon.PathConfig
	lastLoadStatus := udpmon.LoadStatus{State: udpmon.LoadHealthy, ProbeScale: 1}
	problemNodes := make(map[string]bool)
	applyPaths := func(paths []udpmon.PathConfig, status udpmon.LoadStatus) {
		if !udpmon.PathConfigsEqual(applied, paths) {
			runtime.SetPaths(paths)
			applied = paths
			logger.Info("UDP runtime applied: %d active paths, load=%s level=%d", len(paths), status.State, status.Level)
		}
		reporter.UpdateLoad(status)
	}
	refreshConfig := func() {
		cfg, err := client.Fetch(ctx)
		if err != nil {
			logger.Warn("UDP config refresh failed: %v", err)
			return
		}
		if err := receiverHealth.SetConfig(cfg); err != nil {
			reporter.MarkConfigApplyError()
			logger.Error("UDP receiver health config revision %d rejected: %v", cfg.Revision, err)
			return
		}
		reporter.UpdateConfig(cfg)
		currentCfg = cfg
		proxies := proxyChecker.GetProxies()
		nodes := make([]udpmon.NodeDescriptor, 0, len(proxies))
		for _, proxy := range proxies {
			if proxy.StableID == "" {
				proxy.StableID = proxy.GenerateStableID()
			}
			nodes = append(nodes, udpmon.NodeDescriptor{
				StableID:     proxy.StableID,
				SocksAddress: fmt.Sprintf("127.0.0.1:%d", startPort+proxy.Index),
				SupportsUDP:  proxy.Protocol != "http",
			})
		}
		paths, err := udpmon.BuildPaths(cfg, nodes)
		if err != nil {
			reporter.MarkConfigApplyError()
			logger.Error("UDP config revision %d rejected: %v", cfg.Revision, err)
			return
		}
		basePaths = paths
		lastLoadStatus.PlaybackDelayMS = cfg.Settings.PlaybackDelayMS
		effective, status := udpmon.ApplyLoadPolicy(basePaths, cfg.Settings, lastLoadStatus, problemNodes)
		applyPaths(effective, status)
	}
	refreshConfig()
	for {
		select {
		case <-ctx.Done():
			return
		case <-configTicker.C:
			refreshConfig()
		case now := <-loadTicker.C:
			if currentCfg.Revision == 0 {
				continue
			}
			status := loadController.Observe(currentCfg.Settings, runtime.Stats(), reporter.QueueLen(), reporter.QueueCapacity())
			health := receiverHealth.Snapshot(now)
			_, nodes := runtime.Aggregates(now, health, currentCfg.Settings.Thresholds())
			problemNodes = make(map[string]bool)
			for _, node := range nodes {
				if node.State == udpmon.StateDegraded || node.State == udpmon.StateOffline {
					problemNodes[node.NodeID] = true
				}
			}
			effective, status := udpmon.ApplyLoadPolicy(basePaths, currentCfg.Settings, status, problemNodes)
			lastLoadStatus = status
			applyPaths(effective, status)
		}
	}
}

func updateConfiguration(newConfigs []*models.ProxyConfig, currentConfigs *[]*models.ProxyConfig,
	xrayRunner *xray.Runner, proxyChecker *checker.ProxyChecker) error {

	logger.Info("Subscription changed, updating configuration...")

	xray.PrepareProxyConfigs(newConfigs)

	configFile := "xray_config.json"
	configGenerator := xray.NewConfigGenerator()
	validProxies, err := configGenerator.GenerateValidatedConfig(
		newConfigs,
		config.CLIConfig.Xray.StartPort,
		configFile,
		config.CLIConfig.Xray.LogLevel,
	)
	if err != nil {
		return err
	}
	newConfigs = validProxies

	if err := xrayRunner.Stop(); err != nil {
		return err
	}

	if err := xrayRunner.Start(); err != nil {
		return err
	}

	proxyChecker.UpdateProxies(newConfigs)

	*currentConfigs = newConfigs

	web.RegisterConfigEndpoints(newConfigs, proxyChecker, config.CLIConfig.Xray.StartPort)

	logger.Info("Configuration updated: %d proxies", len(newConfigs))
	return nil
}
