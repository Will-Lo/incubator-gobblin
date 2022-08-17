package org.apache.gobblin.service.monitoring;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.flowgraph.FSPathAlterationFlowGraphListener;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphMonitor;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;
import org.apache.gobblin.util.filesystem.PathAlterationObserver;
import org.apache.gobblin.util.filesystem.PathAlterationObserverScheduler;
import org.apache.hadoop.fs.Path;


@Slf4j
public class FsFlowGraphMonitor extends AbstractIdleService implements FlowGraphMonitor {
  public static final String FS_FLOWGRAPH_MONITOR_PREFIX = "gobblin.service.fsFlowGraphMonitor";
  private static long DEFAULT_FLOWGRAPH_POLLING_INTERVAL = 60;
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_REPO_DIR = "git-flowgraph";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_FLOWGRAPH_DIR = "gobblin-flowgraph";
  private boolean isActive;
  private long pollingInterval;
  private PathAlterationObserverScheduler pathAlterationDetector;
  private PathAlterationObserver observer;
  private Path flowGraphPath;

  private static final Config DEFAULT_FALLBACK = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
      .put(ConfigurationKeys.FLOWGRAPH_REPO_DIR, DEFAULT_GIT_FLOWGRAPH_MONITOR_REPO_DIR)
      .put(ConfigurationKeys.FLOWGRAPH_BASE_DIR, DEFAULT_GIT_FLOWGRAPH_MONITOR_FLOWGRAPH_DIR)
      .put(ConfigurationKeys.FLOWGRAPH_POLLING_INTERVAL, DEFAULT_FLOWGRAPH_POLLING_INTERVAL)
      .put(ConfigurationKeys.JAVA_PROPS_EXTENSIONS, ConfigurationKeys.DEFAULT_PROPERTIES_EXTENSIONS)
      .put(ConfigurationKeys.HOCON_FILE_EXTENSIONS, ConfigurationKeys.DEFAULT_CONF_EXTENSIONS)
      .build());

  public FsFlowGraphMonitor(Config config, Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog,
      FlowGraph graph, Map<URI, TopologySpec> topologySpecMap, CountDownLatch initComplete) throws IOException {
    Config configWithFallbacks = config.getConfig(FS_FLOWGRAPH_MONITOR_PREFIX).withFallback(DEFAULT_FALLBACK);
    this.pollingInterval = TimeUnit.SECONDS.toMillis(configWithFallbacks.getLong(ConfigurationKeys.FLOWGRAPH_POLLING_INTERVAL));
    this.flowGraphPath = new Path(configWithFallbacks.getString(ConfigurationKeys.FLOWGRAPH_REPO_DIR));
    this.observer = new FsFlowGraphPathAlterationObserver(flowGraphPath, initComplete);
    if (pollingInterval == ConfigurationKeys.DISABLED_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL) {
      this.pathAlterationDetector = null;
    }
    else {
      this.pathAlterationDetector = new PathAlterationObserverScheduler(pollingInterval);
      // If absent, the Optional object will be created automatically by addPathAlterationObserver
      Optional<PathAlterationObserver> observerOptional = Optional.fromNullable(observer);
      this.pathAlterationDetector.addPathAlterationObserver(new FSPathAlterationFlowGraphListener(
          flowTemplateCatalog, graph, topologySpecMap, flowGraphPath.toString(), configWithFallbacks.getString(ConfigurationKeys.FLOWGRAPH_BASE_DIR),
          configWithFallbacks.getString(ConfigurationKeys.JAVA_PROPS_EXTENSIONS),
          configWithFallbacks.getString(ConfigurationKeys.HOCON_FILE_EXTENSIONS)
      ), observerOptional, this.flowGraphPath);
    }
  }

  @Override
  protected void startUp() throws IOException {
    if (this.pathAlterationDetector != null) {
      log.info("Starting the " + getClass().getSimpleName());
      log.info("Polling flowgraph folder with interval {} ", this.pollingInterval);
      this.pathAlterationDetector.start();
    } else {
      log.warn("No path monitor detected");
    }
  }

  @Override
  public synchronized void setActive(boolean isActive) {
    if (this.isActive == isActive) {
      // No-op if already in correct state
      return;
    }
    this.isActive = isActive;
  }

  /** Stop the service. */
  @Override
  protected void shutDown() throws Exception {
    this.pathAlterationDetector.stop();
  }

  private class FsFlowGraphPathAlterationObserver extends PathAlterationObserver {
    CountDownLatch initComplete;

    public FsFlowGraphPathAlterationObserver(final Path directoryName, CountDownLatch initComplete)
        throws IOException {
      super(directoryName);
      this.initComplete = initComplete;
    }

    @Override
    public void checkAndNotify() throws IOException {
      super.checkAndNotify();
      // After the graph is first initialized, this becomes a noop
      this.initComplete.countDown();
    }
  }

}
