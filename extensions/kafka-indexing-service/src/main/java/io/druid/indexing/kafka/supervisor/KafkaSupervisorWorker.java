/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.kafka.supervisor;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.TaskResource;
import io.druid.indexing.kafka.KafkaDataSourceMetadata;
import io.druid.indexing.kafka.KafkaIOConfig;
import io.druid.indexing.kafka.KafkaIndexTask;
import io.druid.indexing.kafka.KafkaPartitions;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.SupervisorWorker;
import io.druid.metadata.EntryExistsException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Worker responsible for managing the KafkaIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link KafkaSupervisorJob} which includes the Kafka topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Kafka topic's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Kafka offsets.
 */
public class KafkaSupervisorWorker implements SupervisorWorker<KafkaSupervisorJob>
{
  /**
   * Keeps track of the offset range and indexing state of the partitions in our topic. The offset range is only
   * modified after a task including that partition completes successfully so retries and replicas will always use the
   * same range. When a new partition is discovered, it will be added with active=false and this partition will only be
   * included in the next task after a successful run completes. Both of these behaviors are intended to prevent data
   * duplication since the partition numbers and offset ranges are used in the generation of the sequenceName.
   */
  private class PartitionOffsets
  {
    long startingOffset;
    long endingOffset;
    boolean active;

    public PartitionOffsets(long startingOffset, long endingOffset, boolean active)
    {
      this.startingOffset = startingOffset;
      this.endingOffset = endingOffset;
      this.active = active;
    }
  }

  private class TaskGroup
  {
    Map<Integer, PartitionOffsets> partitions = new HashMap<>();
    Map<String, Optional<TaskStatus>> taskStatuses = new HashMap<>();
    boolean initialized = false;
  }

  private static final EmittingLogger log = new EmittingLogger(KafkaSupervisorWorker.class);
  private static final Random RANDOM = new Random();
  private static final String SUPERVISOR_CONTEXT_KEY = "supervisorId";

  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final KafkaSupervisorConfig config;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;

  private String supervisorRunId;
  private KafkaSupervisorJob job;
  private String dataSource;
  private KafkaConsumer consumer;

  private volatile boolean initialized = false;
  private volatile boolean stopped = false;
  private ConcurrentMap<Integer, TaskGroup> taskGroups = new ConcurrentHashMap<>();

  @Inject
  public KafkaSupervisorWorker(
      TaskStorage taskStorage,
      TaskMaster taskMaster,
      KafkaSupervisorConfig config,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator
  )
  {
    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.config = config;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
  }

  @Override
  public void init(KafkaSupervisorJob job)
  {
    this.supervisorRunId = getRandomId();
    log.info("Initialized with supervisor run ID: [%s]", this.supervisorRunId);

    this.job = job;
    this.dataSource = job.getDataSchema().getDataSource();
    this.consumer = getKafkaConsumer(job.getIoConfig().getKafkaBrokers());
    this.initialized = true;
  }

  @Override
  public ScheduledExecutors.Signal call() throws Exception
  {
    if (stopped) {
      log.info("Kafka supervisor worker for dataSource [%s] has stopped.", dataSource);
      return ScheduledExecutors.Signal.STOP;
    }

    if (!initialized) {
      throw new IllegalStateException("Call init() first");
    }

    killUnownedKafkaTasks();
    updatePartitionDataFromKafka();
    updateTaskMetadata();
    checkTaskState();

    return ScheduledExecutors.Signal.REPEAT;
  }

  @Override
  public void stop()
  {
    if (initialized) {
      log.info("Kafka supervisor worker for dataSource [%s] is stopping.", dataSource);
      consumer.close();
    }

    this.stopped = true;
  }

  @Override
  public Optional<KafkaSupervisorJob> getJob()
  {
    return Optional.fromNullable(this.job);
  }

  private static String getRandomId()
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Ints.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((RANDOM.nextInt() >>> (i * 4)) & 0x0F)));
    }
    return suffix.toString();
  }

  private KafkaConsumer<byte[], byte[]> getKafkaConsumer(String bootstrapServers)
  {
    final Properties props = new Properties();
    props.putAll(job.getIoConfig().getConsumerProperties());

    props.setProperty("enable.auto.commit", "false");
    props.setProperty("metadata.max.age.ms", "10000");
    props.setProperty("bootstrap.servers", bootstrapServers);
    props.setProperty("group.id", String.format("kafka-supervisor-%s", supervisorRunId));

    return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  private void updatePartitionDataFromKafka()
  {
    consumer.listTopics(); // updates the consumer's list of partitions from the brokers
    List<PartitionInfo> partitions = consumer.partitionsFor(job.getIoConfig().getTopic());
    int numPartitions = (partitions != null ? partitions.size() : 0);

    log.debug("Found [%d] Kafka partitions for topic [%s].", numPartitions, job.getIoConfig().getTopic());

    for (int partition = 0; partition < numPartitions; partition++) {
      int taskGroupId = getTaskGroupIdForPartition(partition);
      if (taskGroups.get(taskGroupId) == null) {
        log.info("Creating new task group [%d]", taskGroupId);
        taskGroups.put(taskGroupId, new TaskGroup());
      }

      if (!taskGroups.get(taskGroupId).partitions.containsKey(partition)) {
        log.info(
            "New partition [%d] discovered for topic [%s], adding to task group [%d]",
            partition,
            job.getIoConfig().getTopic(),
            taskGroupId
        );
        taskGroups.get(taskGroupId).partitions.put(partition, new PartitionOffsets(0, 0, false));
      }
    }
  }

  private void updateTaskMetadata()
  {
    int taskCount = 0;
    List<Task> tasks = taskStorage.getActiveTasks();

    for (Task task : tasks) {
      if (task instanceof KafkaIndexTask && dataSource.equals(task.getDataSource())) {
        taskCount++;
        KafkaIndexTask kafkaTask = (KafkaIndexTask) task;

        // determine which task group this task belongs to and do a consistency check on partitions
        Integer taskGroupId = null;
        for (Integer partition : kafkaTask.getIoConfig().getStartPartitions().getPartitionOffsetMap().keySet()) {
          if (taskGroupId == null) {
            taskGroupId = getTaskGroupIdForPartition(partition);
          } else if (taskGroupId != getTaskGroupIdForPartition(partition)) {
            log.error("Discovered task [%s] has different partition allocation than expected!", task.getId());
            taskGroupId = null;
            break;
          }
        }

        if (taskGroupId != null) {
          if (taskGroups.get(taskGroupId) == null) {
            log.info("Creating new task group [%d]", taskGroupId);
            taskGroups.put(taskGroupId, new TaskGroup());
          }

          if (!taskGroups.get(taskGroupId).taskStatuses.containsKey(task.getId())) {
            taskGroups.get(taskGroupId).taskStatuses.put(task.getId(), Optional.<TaskStatus>absent());
          }
        }
      }
    }

    // update TaskStatus for tracked tasks
    for (Integer key : taskGroups.keySet()) {
      for (String taskId : taskGroups.get(key).taskStatuses.keySet()) {
        taskGroups.get(key).taskStatuses.put(taskId, taskStorage.getStatus(taskId));
      }
    }

    log.debug("Found [%d] Kafka indexing tasks for dataSource [%s].", taskCount, dataSource);
  }

  private void checkTaskState()
  {
    for (Integer groupId : taskGroups.keySet()) {
      TaskGroup taskGroup = taskGroups.get(groupId);

      // Iterate the list of known tasks in this group and:
      //   1) Remove any tasks which are no longer "current" (have the same partitions and offset ranges as the latest
      //      known set.
      //   2) Determine if any task completed successfully.
      //   3) Remove any tasks that have failed from the list.
      //
      // When we're done this loop, we will either have:
      //   1) Found a successful current task, which will trigger a shutdown of the remaining running replicas and will
      //      queue the next set of tasks.
      //   2) Not found a successful task, and have a taskStatuses list that only contains current tasks that are in
      //      running state. We will then compare the number of running tasks to the expected number of replicas and
      //      create additional tasks as necessary.

      log.debug("Task group [%d] pre-pruning: %s", groupId, taskGroup.taskStatuses);
      String successfulTaskId = null;
      Iterator<Map.Entry<String, Optional<TaskStatus>>> it = taskGroup.taskStatuses.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<String, Optional<TaskStatus>> pair = it.next();
        if (pair.getValue().isPresent() && isTaskCurrent(groupId, pair.getKey())) {
          if (pair.getValue().get().isSuccess()) {
            successfulTaskId = pair.getKey();
          } else if (pair.getValue().get().isFailure()) {
            it.remove();
          }
        } else {
          it.remove();
        }
      }
      log.debug("Task group [%d] post-pruning: %s", groupId, taskGroup.taskStatuses);

      // If this is the first time processing this group or if a task in this group completed successfully, terminate
      // the remaining running replicas, generate the next set of partitions and offsets, and queue {numReplica} tasks.
      if (!taskGroup.initialized || successfulTaskId != null) {
        killRunningTasksInGroup(groupId);
        taskGroup.taskStatuses.clear();
        setNextOffsetRangeForTaskGroup(groupId);
        createKafkaTasksForGroup(groupId, job.getIoConfig().getReplicas());

        if (!taskGroup.initialized) {
          log.info("Initialized task group [%d]", groupId);
          taskGroup.initialized = true;
        }

        continue; // move onto the next group of tasks in taskGroups
      }

      // If we didn't find a successful task, make sure we have the expected number of replicas running and queue
      // additional tasks as necessary.
      if (job.getIoConfig().getReplicas() > taskGroup.taskStatuses.size()) {
        log.info(
            "Number of tasks [%d] does not match configured numReplicas [%d] in task group [%d], creating more tasks.",
            taskGroup.taskStatuses.size(), job.getIoConfig().getReplicas(), groupId
        );
        createKafkaTasksForGroup(groupId, job.getIoConfig().getReplicas() - taskGroup.taskStatuses.size());
      }
    }
  }

  private void createKafkaTasksForGroup(int groupId, int replicas)
  {
    TaskGroup taskGroup = taskGroups.get(groupId);
    Map<Integer, Long> startPartitions = new HashMap<>();
    Map<Integer, Long> endPartitions = new HashMap<>();
    StringBuilder sb = new StringBuilder();

    // Generate a string from the partitions and offsets handled by this task to be used in the sequenceName and
    // availability group. This must be deterministic so that all tasks handling these partitions/offsets, 1) have the
    // same sequenceName and hence generate the same segment IDs, and 2) have the same availability group and are thus
    // assigned to different workers for replication. We only assign "active" partitions, so that replicas and retries
    // operate on the same set of partitions, even if new partitions were created in the middle of creating tasks.
    // Partitions become marked as active once the task group they are assigned to has a task complete successfully.
    for (int partition : taskGroup.partitions.keySet()) {
      PartitionOffsets offsets = taskGroup.partitions.get(partition);
      if (offsets.active) {
        startPartitions.put(partition, offsets.startingOffset);
        endPartitions.put(partition, offsets.endingOffset);
        sb.append(String.format("+%d(%d-%d)", partition, offsets.startingOffset, offsets.endingOffset));
      }
    }
    String partitionOffsetStr = sb.toString().substring(1);

    String availabilityGroup = Joiner.on("_")
                                     .join(
                                         "index_kafka",
                                         dataSource,
                                         (config.taskIdShowOffsets()
                                          ? partitionOffsetStr
                                          : Math.abs(partitionOffsetStr.hashCode()))
                                     );

    Map<String, String> consumerProperties = Maps.newHashMap(job.getIoConfig().getConsumerProperties());
    consumerProperties.put("bootstrap.servers", job.getIoConfig().getKafkaBrokers());

    KafkaIOConfig ioConfig = new KafkaIOConfig(
        availabilityGroup,
        new KafkaPartitions(job.getIoConfig().getTopic(), startPartitions),
        new KafkaPartitions(job.getIoConfig().getTopic(), endPartitions),
        consumerProperties,
        true
    );

    for (int i = 0; i < replicas; i++) {
      String taskId = Joiner.on("_").join(availabilityGroup, getRandomId());
      KafkaIndexTask indexTask = new KafkaIndexTask(
          taskId,
          new TaskResource(availabilityGroup, 1),
          job.getDataSchema(),
          job.getTuningConfig(),
          ioConfig,
          // used to track which tasks were created by this instance of the supervisor so we can terminate unowned tasks
          ImmutableMap.<String, Object>of(SUPERVISOR_CONTEXT_KEY, supervisorRunId)
      );

      if (taskMaster.getTaskQueue().isPresent()) {
        try {
          taskMaster.getTaskQueue().get().add(indexTask);
        }
        catch (EntryExistsException e) {
          log.error("Tried to add task [%s] but it already exists", indexTask.getId());
        }
      } else {
        log.error("Failed to get task queue because I'm not the leader!");
      }
    }
  }

  /**
   * Determines next set of offset ranges based on number of partitions handled by the task, the previous endingOffsets,
   * and the messagesPerTask configuration. Also sets any inactive partitions as active so they will be processed by
   * the next indexing task.
   * <p>
   * Queries the dataSource metadata table to see if there is information on previous endingOffsets for completed
   * segments for this dataSource. If this is the first run, if it finds this information, it will use it to initialize
   * the starting offset. If it doesn't find any data, it will start at either the latest or earliest Kafka offsets
   * depending on configuration. If this is a subsequent run, it will use this information to do a consistency check.
   */
  private void setNextOffsetRangeForTaskGroup(int groupId)
  {
    Map<Integer, Long> storedOffsets = getOffsetsFromMetadataStorage();
    Map<Integer, PartitionOffsets> partitions = taskGroups.get(groupId).partitions;
    long messagesPerPartition = job.getIoConfig().getMessagesPerTask() / partitions.size();

    for (Integer partition : partitions.keySet()) {
      PartitionOffsets partitionOffsets = partitions.get(partition);

      if (!partitionOffsets.active) { // first run
        if (storedOffsets.get(partition) != null) { // found valid previous offset from metadata store
          partitionOffsets.startingOffset = storedOffsets.get(partition);
          log.info(
              "Setting initial starting offset to [%d] for partition [%d] from metadata storage.",
              partitionOffsets.startingOffset,
              partition
          );
        } else { // no previous offset so get starting offset from Kafka
          partitionOffsets.startingOffset = getOffsetFromKafkaForPartition(partition);
          log.info(
              "Setting initial starting offset to [%d] for partition [%d] from Kafka.",
              partitionOffsets.startingOffset,
              partition
          );
        }
      } else { // subsequent run, use metadata storage to check consistency
        partitionOffsets.startingOffset = partitionOffsets.endingOffset;
        if (storedOffsets.get(partition) == null || storedOffsets.get(partition) != partitionOffsets.endingOffset) {
          log.error(
              "endingOffset from metadata storage [%d] does not match expected offset [%d] for partition [%d]",
              (storedOffsets.get(partition) == null ? -1 : storedOffsets.get(partition)),
              partitionOffsets.endingOffset,
              partition
          );
        }
      }

      partitionOffsets.endingOffset = partitionOffsets.startingOffset + messagesPerPartition;
      partitionOffsets.active = true;
    }
  }

  /**
   * Compares the list of partitions/offsets for taskId from TaskStorage and the data stored in taskGroup.partitions
   * (which is the current set of partitions/offsets being processed) and returns false if these do not match.
   */
  private boolean isTaskCurrent(int taskGroupId, String taskId)
  {
    TaskGroup taskGroup = taskGroups.get(taskGroupId);
    if (!taskStorage.getTask(taskId).isPresent() || taskGroup == null) {
      return false;
    }

    KafkaIndexTask task = (KafkaIndexTask) taskStorage.getTask(taskId).get();
    Map<Integer, Long> taskStartPartitions = task.getIoConfig().getStartPartitions().getPartitionOffsetMap();
    Map<Integer, Long> taskEndPartitions = task.getIoConfig().getEndPartitions().getPartitionOffsetMap();

    for (Integer partition : taskStartPartitions.keySet()) {
      if (taskGroup.partitions.get(partition) == null
          || taskGroup.partitions.get(partition).startingOffset != taskStartPartitions.get(partition)
          || taskGroup.partitions.get(partition).endingOffset != taskEndPartitions.get(partition)) {
        return false;
      }
    }

    return true;
  }

  private void killRunningTasksInGroup(int taskGroupId)
  {
    if (taskMaster.getTaskQueue().isPresent()) {
      Map<String, Optional<TaskStatus>> taskStatuses = taskGroups.get(taskGroupId).taskStatuses;
      for (String taskId : taskStatuses.keySet()) {
        if (taskStatuses.get(taskId).isPresent() && !taskStatuses.get(taskId).get().isComplete()) {
          taskMaster.getTaskQueue().get().shutdown(taskId);
        }
      }
    } else {
      log.error("Failed to get task queue because I'm not the leader!");
    }
  }

  private void killUnownedKafkaTasks()
  {
    if (taskMaster.getTaskQueue().isPresent()) {
      List<Task> tasks = taskStorage.getActiveTasks();
      for (Task task : tasks) {
        if (task instanceof KafkaIndexTask
            && dataSource.equals(task.getDataSource())
            && !supervisorRunId.equals(task.getContextValue(SUPERVISOR_CONTEXT_KEY))) {
          log.info(
              "Killing unowned KafkaIndexTask [%s] (supervisorId: [%s] != [%s])",
              task.getId(),
              (task.getContextValue(SUPERVISOR_CONTEXT_KEY) == null
               ? "null"
               : task.getContextValue(SUPERVISOR_CONTEXT_KEY)),
              supervisorRunId
          );
          taskMaster.getTaskQueue().get().shutdown(task.getId());
        }
      }
    } else {
      log.error("Failed to get task queue because I'm not the leader!");
    }
  }

  private int getTaskGroupIdForPartition(int partition)
  {
    return partition % job.getIoConfig().getTaskCount();
  }

  private Map<Integer, Long> getOffsetsFromMetadataStorage()
  {
    DataSourceMetadata dataSourceMetadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);
    if (dataSourceMetadata != null && dataSourceMetadata instanceof KafkaDataSourceMetadata) {
      KafkaPartitions partitions = ((KafkaDataSourceMetadata) dataSourceMetadata).getKafkaPartitions();
      if (partitions != null) {
        if (!job.getIoConfig().getTopic().equals(partitions.getTopic())) {
          log.warn(
              "Topic in metadata storage [%s] doesn't match job topic [%s], ignoring stored offsets.",
              partitions.getTopic(),
              job.getIoConfig().getTopic()
          );
          return ImmutableMap.of();
        } else if (partitions.getPartitionOffsetMap() != null) {
          return partitions.getPartitionOffsetMap();
        }
      }
    }

    return ImmutableMap.of();
  }

  private long getOffsetFromKafkaForPartition(int partition)
  {
    TopicPartition topicPartition = new TopicPartition(job.getIoConfig().getTopic(), partition);
    if (!consumer.assignment().contains(topicPartition)) {
      consumer.assign(Lists.newArrayList(topicPartition));
    }

    if (config.useEarliestOffsets()) {
      consumer.seekToBeginning(topicPartition);
    } else {
      consumer.seekToEnd(topicPartition);
    }

    return consumer.position(topicPartition);
  }
}
