package org.apache.gobblin.temporal.ddm.workflow.impl;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.temporal.failure.ApplicationFailure;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.commit.DeliverySemantics;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.SafeDatasetCommit;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateCollectorService;
import org.apache.gobblin.source.extractor.JobCommitPolicy;
import org.apache.gobblin.temporal.ddm.activity.impl.ProcessWorkUnitImpl;
import org.apache.gobblin.temporal.ddm.work.WorkUnitClaimCheck;
import org.apache.gobblin.temporal.ddm.workflow.CommitStepWorkflow;
import org.apache.gobblin.temporal.util.nesting.work.Workload;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.Either;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JobConfigurationUtils;
import org.apache.gobblin.util.SerializationUtils;
import org.apache.gobblin.util.executors.IteratorExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


@Slf4j
public class CommitStepWorkflowImpl implements CommitStepWorkflow {
  int numDeserializationThreads = 1;

  @Override
  public int commit(WorkUnitClaimCheck wu) {
    try {
      FileSystem fs = loadFileSystemForUri(wu.getNameNodeUri(), wu.getStateConfig());
      State jobState = loadJobState(wu, fs);
      SharedResourcesBroker<GobblinScopeTypes> instanceBroker = createDefaultInstanceBroker(jobState.getProperties());
      JobContext globalGobblinContext = new JobContext(jobState.getProperties(), log, instanceBroker, null);
      // TODO: READ FROM CONFIG
      Path jobOutputPath = new Path(jobState.getProp(ProcessWorkUnitImpl.WRITER_OUTPUT_DIR_KEY));
      StateStore<TaskState> taskStateStore = new FsStateStore<>(fs, jobOutputPath.toString(), TaskState.class);
      Collection<TaskState> taskStateQueue =
          ImmutableList.copyOf(TaskStateCollectorService.deserializeTaskStatesFromFolder(taskStateStore, jobOutputPath, this.numDeserializationThreads));
      commitTaskStates(jobState, taskStateQueue, globalGobblinContext);

    } catch (Exception e) {
      //TODO: IMPROVE GRANULARITY OF RETRIES
      throw ApplicationFailure.newNonRetryableFailureWithCause(
          "Failed to commit dataset state for some dataset(s) of job <jobStub>",
          IOException.class.toString(),
          new IOException("Failed to commit dataset state for some dataset(s) of job <jobStub>"),
          null
      );
    }

    return 0;
  }

  /**
   * TODO: Read from the WorkUnitClaimCheck or config file that is serialized
   * @param workload
   * @return
   */
  private Config readConfigFromSpec(Workload<WorkUnitClaimCheck> workload) {
    workload.getSpan()

  }

  protected JobState loadJobState(WorkUnitClaimCheck wu, FileSystem fs) throws IOException {
    Path jobStatePath = new Path(wu.getJobStatePath());
    JobState jobState = new JobState();
    // TODO/WARNING: `fs` runs the risk of expiring while executing!!! -
    SerializationUtils.deserializeState(fs, jobStatePath, jobState);
    return jobState;
  }

  protected FileSystem loadFileSystemForUri(URI fsUri, State stateConfig) throws IOException {
    return HadoopUtils.getFileSystem(fsUri, stateConfig);
  }

  void commitTaskStates(State jobState, Collection<TaskState> taskStates, JobContext jobContext) throws IOException {
    Map<String, JobState.DatasetState> datasetStatesByUrns = createDatasetStatesByUrns(taskStates);
    final boolean shouldCommitDataInJob = shouldCommitDataInJob(jobState);
    final DeliverySemantics deliverySemantics = DeliverySemantics.AT_LEAST_ONCE;
    final int numCommitThreads = 1;

    if (!shouldCommitDataInJob) {
      log.info("Job will not commit data since data are committed by tasks.");
    }

    try {
      if (!datasetStatesByUrns.isEmpty()) {
        log.info("Persisting dataset urns.");
      }

      List<Either<Void, ExecutionException>> result = new IteratorExecutor<>(Iterables
          .transform(datasetStatesByUrns.entrySet(),
              new Function<Map.Entry<String, JobState.DatasetState>, Callable<Void>>() {
                @Nullable
                @Override
                public Callable<Void> apply(final Map.Entry<String, JobState.DatasetState> entry) {
                  return new SafeDatasetCommit(shouldCommitDataInJob, false, deliverySemantics, entry.getKey(),
                      entry.getValue(), false, jobContext);
                }
              }).iterator(), numCommitThreads,
          ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("Commit-thread-%d")))
          .executeAndGetResults();

      IteratorExecutor.logFailures(result, null, 10);

      if (!IteratorExecutor.verifyAllSuccessful(result)) {
        // TODO: propagate cause of failure
        throw new IOException("Failed to commit dataset state for some dataset(s) of job <jobStub>");
      }
    } catch (InterruptedException exc) {
      throw new IOException(exc);
    }
  }

  public Map<String, JobState.DatasetState> createDatasetStatesByUrns(Collection<TaskState> taskStates) {
    Map<String, JobState.DatasetState> datasetStatesByUrns = Maps.newHashMap();

    //TODO: handle skipped tasks?
    for (TaskState taskState : taskStates) {
      String datasetUrn = createDatasetUrn(datasetStatesByUrns, taskState);
      datasetStatesByUrns.get(datasetUrn).incrementTaskCount();
      datasetStatesByUrns.get(datasetUrn).addTaskState(taskState);
    }

    return datasetStatesByUrns;
  }

  private String createDatasetUrn(Map<String, JobState.DatasetState> datasetStatesByUrns, TaskState taskState) {
    String datasetUrn = taskState.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN);
    if (!datasetStatesByUrns.containsKey(datasetUrn)) {
      JobState.DatasetState datasetState = new JobState.DatasetState();
      datasetState.setDatasetUrn(datasetUrn);
      datasetStatesByUrns.put(datasetUrn, datasetState);
    }
    return datasetUrn;
  }

  private static boolean shouldCommitDataInJob(State state) {
    boolean jobCommitPolicyIsFull =
        JobCommitPolicy.getCommitPolicy(state.getProperties()) == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS;
    boolean publishDataAtJobLevel = state.getPropAsBoolean(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL,
        ConfigurationKeys.DEFAULT_PUBLISH_DATA_AT_JOB_LEVEL);
    boolean jobDataPublisherSpecified =
        !Strings.isNullOrEmpty(state.getProp(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE));
    return jobCommitPolicyIsFull || publishDataAtJobLevel || jobDataPublisherSpecified;
  }

  private static SharedResourcesBroker<GobblinScopeTypes> createDefaultInstanceBroker(Properties jobProps) {
    log.warn("Creating a job specific {}. Objects will only be shared at the job level.",
        SharedResourcesBroker.class.getSimpleName());
    return SharedResourcesBrokerFactory.createDefaultTopLevelBroker(ConfigFactory.parseProperties(jobProps),
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());
  }

}
