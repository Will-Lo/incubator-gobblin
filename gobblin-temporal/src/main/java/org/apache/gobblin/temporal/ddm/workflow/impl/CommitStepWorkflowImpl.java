package org.apache.gobblin.temporal.ddm.workflow.impl;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.commit.DeliverySemantics;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateCollectorService;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.work.WorkUnitClaimCheck;
import org.apache.gobblin.temporal.ddm.workflow.CommitStepWorkflow;
import org.apache.gobblin.temporal.util.nesting.work.Workload;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.Either;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.executors.IteratorExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


@Slf4j
public class CommitStepWorkflowImpl implements CommitStepWorkflow {

  int numDeserializationThreads = 1;

  @Override
  public int commit(Workload<WorkUnitClaimCheck> workload) {
    Config config = readConfigFromSpec(workload);
    FileSystem fs = buildFileSystem(config);
    Path jobOutputPath = new Path("/tmp/${user.to.proxy}/${azkaban.flow.flowid}/output");

    StateStore<TaskState> taskStateStore = new FsStateStore<>(fs, jobOutputPath.toString(), TaskState.class);

    try {
      Queue<TaskState> taskStateQueue =
          TaskStateCollectorService.deserializeTaskStatesFromFolder(taskStateStore, jobOutputPath, this.numDeserializationThreads);


    } catch (IOException e) {
      e.printStackTrace();
      log.warn("Could not deserialize task states from folder: " + jobOutputPath.toString());
    }
  }

  private StateStore<TaskState> createTaskStateStore(Config config) {
    String stateStoreType = ConfigUtils.getString(config, ConfigurationKeys.INTERMEDIATE_STATE_STORE_TYPE_KEY,
        ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_TYPE_KEY,
            ConfigurationKeys.DEFAULT_STATE_STORE_TYPE));

    ClassAliasResolver<StateStore.Factory> resolver = new ClassAliasResolver<>(StateStore.Factory.class);
    StateStore.Factory stateStoreFactory = resolver.resolveClass(stateStoreType).newInstance();

    StateStore<TaskState> taskStateStore = stateStoreFactory.createStateStore(config, TaskState.class);

  }

  /**
   * TODO: Read from the WorkUnitClaimCheck or config file that is serialized
   * @param workload
   * @return
   */
  private Config readConfigFromSpec(Workload<WorkUnitClaimCheck> workload) {
    Config config = ConfigFactory.empty()
        .withValue();

  }

  private static FileSystem buildFileSystem(Configuration configuration) throws IOException {
    URI fsUri = URI.create(configuration.get(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
    return FileSystem.newInstance(fsUri, configuration);
  }

  void commit(final boolean isJobCancelled)
      throws IOException {
    this.datasetStatesByUrns = Optional.of(computeDatasetStatesByUrns());
    final boolean shouldCommitDataInJob = shouldCommitDataInJob(this.jobState);
    final DeliverySemantics deliverySemantics = DeliverySemantics.parse(this.jobState);
    final int numCommitThreads = numCommitThreads();

    if (!shouldCommitDataInJob) {
      this.logger.info("Job will not commit data since data are committed by tasks.");
    }

    try {
      if (this.datasetStatesByUrns.isPresent()) {
        this.logger.info("Persisting dataset urns.");
        this.datasetStateStore.persistDatasetURNs(this.jobName, this.datasetStatesByUrns.get().keySet());
      }

      List<Either<Void, ExecutionException>> result = new IteratorExecutor<>(Iterables
          .transform(this.datasetStatesByUrns.get().entrySet(),
              new Function<Map.Entry<String, JobState.DatasetState>, Callable<Void>>() {
                @Nullable
                @Override
                public Callable<Void> apply(final Map.Entry<String, JobState.DatasetState> entry) {
                  return createSafeDatasetCommit(shouldCommitDataInJob, isJobCancelled, deliverySemantics,
                      entry.getKey(), entry.getValue(), numCommitThreads > 1, JobContext.this);
                }
              }).iterator(), numCommitThreads,
          ExecutorsUtils.newThreadFactory(Optional.of(this.logger), Optional.of("Commit-thread-%d")))
          .executeAndGetResults();

      IteratorExecutor.logFailures(result, LOG, 10);

      if (!IteratorExecutor.verifyAllSuccessful(result)) {
        this.jobState.setState(JobState.RunningState.FAILED);
        String errMsg = "Failed to commit dataset state for some dataset(s) of job " + this.jobId;
        this.jobState.setJobFailureMessage(errMsg);
        throw new IOException(errMsg);
      }
    } catch (InterruptedException exc) {
      throw new IOException(exc);
    }
    this.jobState.setState(JobState.RunningState.COMMITTED);
  }
}
