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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
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
import io.druid.indexing.overlord.TaskQueue;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.Supervisor;
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
import java.util.concurrent.ExecutorService;

/**
 * Supervisor responsible for managing the KafkaIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link KafkaSupervisorSpec} which includes the Kafka topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Kafka topic's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Kafka offsets.
 */
public class KafkaSupervisor implements Supervisor
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

  /**
   * A TaskGroup contains offset and task status information for the Kafka partitions that have been assigned to this
   * group. When a new partition is discovered in Kafka, it is assigned to a TaskGroup by updatePartitionDataFromKafka()
   * which uses the formula in getTaskGroupIdForPartition() to determine how the partitions are to be grouped together
   * and assigns a task group ID. A task group may contain metadata for multiple tasks if more than one replica is
   * configured, but all tasks in a task group handle the same partitions with the same offset ranges. This class is
   * involved in all of the main steps of the run loop (in runInternal()):
   * 1) Check if there are any new Kafka partitions and include them in a task group's [partitions] map.
   * 2) Add all tasks covering these partitions to [taskStatuses] and refresh their TaskStatus.
   * 3) Using this updated view of the Kafka partitions and indexing task statuses, ensure that there are running tasks
   * reading all of the Kafka partitions according to the supervisor spec provided (number of tasks, number of
   * replicas, messages to ingest per task).
   */
  private class TaskGroup
  {
    Map<Integer, PartitionOffsets> partitions = new HashMap<>();
    Map<String, Optional<TaskStatus>> taskStatuses = new HashMap<>();
    boolean initialized = false;
  }

  @VisibleForTesting
  static final String SUPERVISOR_CONTEXT_KEY = "supervisorId";

  private static final EmittingLogger log = new EmittingLogger(KafkaSupervisor.class);
  private static final Random RANDOM = new Random();

  private final String supervisorRunId;
  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final KafkaSupervisorSpec spec;
  private final String dataSource;
  private final KafkaSupervisorIOConfig ioConfig;
  private final ExecutorService exec;

  // the key here is a task group ID, see documentation for TaskGroup class
  private final HashMap<Integer, TaskGroup> taskGroups = new HashMap<>();

  private KafkaConsumer consumer;
  private volatile boolean started = false;

  public KafkaSupervisor(
      TaskStorage taskStorage,
      TaskMaster taskMaster,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      KafkaSupervisorSpec spec
  )
  {
    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.spec = spec;

    this.dataSource = spec.getDataSchema().getDataSource();
    this.ioConfig = spec.getIoConfig();
    this.supervisorRunId = getRandomId();
    this.exec = Execs.singleThreaded(String.format("KafkaSupervisor-%s", dataSource));

    log.info("Initialized with supervisor run ID: [%s]", this.supervisorRunId);
  }

  @Override
  public void start()
  {
    Preconditions.checkState(!started, "already started");
    Preconditions.checkState(!exec.isShutdown(), "already stopped");

    try {
      this.consumer = getKafkaConsumer(ioConfig.getKafkaBrokers());
      exec.submit(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                Thread.sleep(ioConfig.getStartDelay().getMillis());
                while (!Thread.currentThread().isInterrupted()) {
                  final long startTime = System.currentTimeMillis();
                  runInternal();
                  Thread.sleep(ioConfig.getPeriod().getMillis() - (System.currentTimeMillis() - startTime));
                }
              }
              catch (InterruptedException e) {
                log.info("KafkaSupervisor[%s] interrupted, exiting.", dataSource);
              }
            }
          }
      );
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception starting KafkaSupervisor[%s]", dataSource)
         .emit();
      throw Throwables.propagate(e);
    }

    started = true;
    log.info("Started KafkaSupervisor[%s].", dataSource);
  }

  @Override
  public void stop()
  {
    Preconditions.checkState(started, "not started");

    try {
      exec.shutdownNow();
      consumer.close();
      started = false;

      log.info("KafkaSupervisor[%s] has stopped.", dataSource);
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception stopping KafkaSupervisor[%s]", dataSource)
         .emit();
    }
  }

  void runInternal()
  {
    try {
      killUnownedKafkaTasks();
      updatePartitionDataFromKafka();
      updateTaskMetadata();
      checkTaskState();
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception in KafkaSupervisor[%s]", dataSource)
         .emit();
    }
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
    props.putAll(ioConfig.getConsumerProperties());

    props.setProperty("enable.auto.commit", "false");
    props.setProperty("metadata.max.age.ms", "10000");
    props.setProperty("bootstrap.servers", bootstrapServers);
    props.setProperty("group.id", String.format("kafka-supervisor-%s", supervisorRunId));

    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  private void updatePartitionDataFromKafka()
  {
    Map<String, List<PartitionInfo>> topics;
    try {
      topics = consumer.listTopics(); // updates the consumer's list of partitions from the brokers
    }
    catch (Exception e) { // calls to the consumer throw NPEs when the broker doesn't respond
      log.warn(
          e,
          "Unable to get partition data from Kafka for brokers [%s], are the brokers up?",
          ioConfig.getKafkaBrokers()
      );
      return;
    }

    List<PartitionInfo> partitions = topics.get(ioConfig.getTopic());
    int numPartitions = (partitions != null ? partitions.size() : 0);

    log.debug("Found [%d] Kafka partitions for topic [%s].", numPartitions, ioConfig.getTopic());

    for (int partition = 0; partition < numPartitions; partition++) {
      int taskGroupId = getTaskGroupIdForPartition(partition);
      if (taskGroups.get(taskGroupId) == null) {
        log.info("Creating new task group [%d]", taskGroupId);
        taskGroups.put(taskGroupId, new TaskGroup());
      }

      Map<Integer, PartitionOffsets> taskGroupPartitions = taskGroups.get(taskGroupId).partitions;
      if (!taskGroupPartitions.containsKey(partition)) {
        log.info(
            "New partition [%d] discovered for topic [%s], adding to task group [%d]",
            partition,
            ioConfig.getTopic(),
            taskGroupId
        );
        taskGroupPartitions.put(partition, new PartitionOffsets(0, 0, false));
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
        for (Integer partition : kafkaTask.getIOConfig().getStartPartitions().getPartitionOffsetMap().keySet()) {
          if (taskGroupId == null) {
            taskGroupId = getTaskGroupIdForPartition(partition);
          } else if (!taskGroupId.equals(getTaskGroupIdForPartition(partition))) {
            ISE ise = new ISE(
                "Discovered task [%s] has unexpected partition allocation, this may result in duplicate data!",
                task.getId()
            );
            log.makeAlert(ise, "Task [%s] has unrecognized partition allocation", task.getId()).emit();
            throw ise;
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
        createKafkaTasksForGroup(groupId, ioConfig.getReplicas());

        if (!taskGroup.initialized) {
          log.info("Initialized task group [%d]", groupId);
          taskGroup.initialized = true;
        }

        continue; // move onto the next group of tasks in taskGroups
      }

      // If we didn't find a successful task, make sure we have the expected number of replicas running and queue
      // additional tasks as necessary.
      if (ioConfig.getReplicas() > taskGroup.taskStatuses.size()) {
        log.info(
            "Number of tasks [%d] does not match configured numReplicas [%d] in task group [%d], creating more tasks.",
            taskGroup.taskStatuses.size(), ioConfig.getReplicas(), groupId
        );
        createKafkaTasksForGroup(groupId, ioConfig.getReplicas() - taskGroup.taskStatuses.size());
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
    String availabilityGroup = Joiner.on("_").join("index_kafka", dataSource, Math.abs(partitionOffsetStr.hashCode()));

    Map<String, String> consumerProperties = Maps.newHashMap(ioConfig.getConsumerProperties());
    consumerProperties.put("bootstrap.servers", ioConfig.getKafkaBrokers());

    KafkaIOConfig kafkaIOConfig = new KafkaIOConfig(
        availabilityGroup,
        new KafkaPartitions(ioConfig.getTopic(), startPartitions),
        new KafkaPartitions(ioConfig.getTopic(), endPartitions),
        consumerProperties,
        true
    );

    for (int i = 0; i < replicas; i++) {
      String taskId = Joiner.on("_").join(availabilityGroup, getRandomId());
      KafkaIndexTask indexTask = new KafkaIndexTask(
          taskId,
          new TaskResource(availabilityGroup, 1),
          spec.getDataSchema(),
          spec.getTuningConfig(),
          kafkaIOConfig,
          // used to track which tasks were created by this instance of the supervisor so we can terminate unowned tasks
          ImmutableMap.<String, Object>of(SUPERVISOR_CONTEXT_KEY, supervisorRunId)
      );

      Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
      if (taskQueue.isPresent()) {
        try {
          taskQueue.get().add(indexTask);
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
    long messagesPerPartition = ioConfig.getMessagesPerTask() / partitions.size();

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
    Optional<Task> taskOptional = taskStorage.getTask(taskId);
    if (!taskOptional.isPresent() || taskGroup == null) {
      return false;
    }

    KafkaIndexTask task = (KafkaIndexTask) taskOptional.get();
    Map<Integer, Long> taskStartPartitions = task.getIOConfig().getStartPartitions().getPartitionOffsetMap();
    Map<Integer, Long> taskEndPartitions = task.getIOConfig().getEndPartitions().getPartitionOffsetMap();

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
    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      Map<String, Optional<TaskStatus>> taskStatuses = taskGroups.get(taskGroupId).taskStatuses;
      for (String taskId : taskStatuses.keySet()) {
        if (taskStatuses.get(taskId).isPresent() && !taskStatuses.get(taskId).get().isComplete()) {
          taskQueue.get().shutdown(taskId);
        }
      }
    } else {
      log.error("Failed to get task queue because I'm not the leader!");
    }
  }

  private void killUnownedKafkaTasks()
  {
    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
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
          taskQueue.get().shutdown(task.getId());
        }
      }
    } else {
      log.error("Failed to get task queue because I'm not the leader!");
    }
  }

  private int getTaskGroupIdForPartition(int partition)
  {
    return partition % ioConfig.getTaskCount();
  }

  private Map<Integer, Long> getOffsetsFromMetadataStorage()
  {
    DataSourceMetadata dataSourceMetadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);
    if (dataSourceMetadata != null && dataSourceMetadata instanceof KafkaDataSourceMetadata) {
      KafkaPartitions partitions = ((KafkaDataSourceMetadata) dataSourceMetadata).getKafkaPartitions();
      if (partitions != null) {
        if (!ioConfig.getTopic().equals(partitions.getTopic())) {
          log.warn(
              "Topic in metadata storage [%s] doesn't match spec topic [%s], ignoring stored offsets.",
              partitions.getTopic(),
              ioConfig.getTopic()
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
    TopicPartition topicPartition = new TopicPartition(ioConfig.getTopic(), partition);
    if (!consumer.assignment().contains(topicPartition)) {
      consumer.assign(Lists.newArrayList(topicPartition));
    }

    if (ioConfig.isUseEarliestOffset()) {
      consumer.seekToBeginning(topicPartition);
    } else {
      consumer.seekToEnd(topicPartition);
    }

    return consumer.position(topicPartition);
  }

  @VisibleForTesting
  String getSupervisorRunId()
  {
    return supervisorRunId;
  }
}
