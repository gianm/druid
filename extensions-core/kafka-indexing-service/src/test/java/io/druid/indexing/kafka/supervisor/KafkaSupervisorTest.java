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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.Granularity;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.JSONPathFieldSpec;
import io.druid.data.input.impl.JSONPathSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.RealtimeIndexTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.kafka.KafkaDataSourceMetadata;
import io.druid.indexing.kafka.KafkaIOConfig;
import io.druid.indexing.kafka.KafkaIndexTask;
import io.druid.indexing.kafka.KafkaPartitions;
import io.druid.indexing.kafka.KafkaTuningConfig;
import io.druid.indexing.kafka.test.TestBroker;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskQueue;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.FireDepartment;
import org.apache.curator.test.TestingCluster;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;

@RunWith(EasyMockRunner.class)
public class KafkaSupervisorTest extends EasyMockSupport
{
  private static final ObjectMapper objectMapper = new DefaultObjectMapper();
  private static final String KAFKA_TOPIC = "testTopic";
  private static final String DATASOURCE = "testDS";
  private static final int NUM_PARTITIONS = 3;

  private TestingCluster zkServer;
  private TestBroker kafkaServer;
  private KafkaSupervisor supervisor;
  private String kafkaHost;
  private DataSchema dataSchema;
  private KafkaTuningConfig tuningConfig;

  @Mock
  private TaskStorage taskStorage;

  @Mock
  private TaskMaster taskMaster;

  @Mock
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;

  @Mock
  private TaskQueue taskQueue;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception
  {
    zkServer = new TestingCluster(1);
    zkServer.start();

    kafkaServer = new TestBroker(
        zkServer.getConnectString(),
        tempFolder.newFolder(),
        1,
        ImmutableMap.of("num.partitions", String.valueOf(NUM_PARTITIONS))
    );
    kafkaServer.start();
    kafkaHost = String.format("localhost:%d", kafkaServer.getPort());

    dataSchema = getDataSchema(DATASOURCE);
    tuningConfig = new KafkaTuningConfig(
        1000,
        50000,
        new Period("P1Y"),
        null,
        null,
        null,
        true,
        false,
        null
    );
  }

  @After
  public void tearDown() throws Exception
  {
    kafkaServer.close();
    kafkaServer = null;

    zkServer.stop();
    zkServer = null;

    supervisor = null;
  }

  @Test
  public void testCallNoInitialState() throws Exception
  {
    supervisor = getSupervisor(1, 1, true);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task = captured.getValue();
    Assert.assertEquals(dataSchema, task.getDataSchema());
    Assert.assertEquals(tuningConfig, task.getTuningConfig());
    Assert.assertEquals(
        supervisor.getSupervisorRunId(), task.getContextValue(KafkaSupervisor.SUPERVISOR_CONTEXT_KEY)
    );

    KafkaIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals(kafkaHost, taskConfig.getConsumerProperties().get("bootstrap.servers"));
    Assert.assertEquals("myCustomValue", taskConfig.getConsumerProperties().get("myCustomKey"));
    Assert.assertEquals(
        String.format("index_kafka_%s_1868520470", DATASOURCE),
        taskConfig.getSequenceName()
    );
    Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());

    Assert.assertEquals(KAFKA_TOPIC, taskConfig.getStartPartitions().getTopic());
    Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(0L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(2));

    Assert.assertEquals(KAFKA_TOPIC, taskConfig.getEndPartitions().getTopic());
    Assert.assertEquals(333L, (long) taskConfig.getEndPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(333L, (long) taskConfig.getEndPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(333L, (long) taskConfig.getEndPartitions().getPartitionOffsetMap().get(2));
  }

  @Test
  public void testCallMultiTask() throws Exception
  {
    supervisor = getSupervisor(1, 2, true);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(2, task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(2, task1.getIOConfig().getEndPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(500L, (long) task1.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(2));
    Assert.assertEquals(500L, (long) task1.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(2));

    KafkaIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(1, task2.getIOConfig().getStartPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(1, task2.getIOConfig().getEndPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(0L, (long) task2.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(1000L, (long) task2.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(1));
  }

  @Test
  public void testCallReplicas() throws Exception
  {
    supervisor = getSupervisor(2, 1, true);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(3, task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(3, task1.getIOConfig().getEndPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(333L, (long) task1.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(333L, (long) task1.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(0L, (long) task1.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(2));
    Assert.assertEquals(333L, (long) task1.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(2));

    KafkaIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(3, task2.getIOConfig().getStartPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(3, task2.getIOConfig().getEndPartitions().getPartitionOffsetMap().size());
    Assert.assertEquals(0L, (long) task2.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(333L, (long) task2.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(0L, (long) task2.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(333L, (long) task2.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(0L, (long) task2.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(2));
    Assert.assertEquals(333L, (long) task2.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(2));
  }

  @Test
  /**
   * Test generating the starting offsets from the partition high water marks in Kafka.
   */
  public void testCallLatestOffset() throws Exception
  {
    supervisor = getSupervisor(1, 1, false);
    addSomeEvents(1100);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task = captured.getValue();
    Assert.assertEquals(1100L, (long) task.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(1433L, (long) task.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(1100L, (long) task.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(1433L, (long) task.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(1100L, (long) task.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(2));
    Assert.assertEquals(1433L, (long) task.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(2));
  }

  @Test
  /**
   * Test generating the starting offsets from the partition data stored in druid_dataSource which contains the
   * offsets of the last built segments.
   */
  public void testCallDatasourceMetadata() throws Exception
  {
    supervisor = getSupervisor(1, 1, true);
    addSomeEvents(1);

    Capture<KafkaIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            new KafkaPartitions(KAFKA_TOPIC, ImmutableMap.of(0, 10L, 1, 20L, 2, 30L))
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    KafkaIndexTask task = captured.getValue();
    KafkaIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals(
        String.format("index_kafka_%s_1305841926", DATASOURCE),
        taskConfig.getSequenceName()
    );
    Assert.assertEquals(10L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(20L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(30L, (long) taskConfig.getStartPartitions().getPartitionOffsetMap().get(2));
    Assert.assertEquals(343L, (long) taskConfig.getEndPartitions().getPartitionOffsetMap().get(0));
    Assert.assertEquals(353L, (long) taskConfig.getEndPartitions().getPartitionOffsetMap().get(1));
    Assert.assertEquals(363L, (long) taskConfig.getEndPartitions().getPartitionOffsetMap().get(2));
  }

  @Test
  /**
   * Test that only KafkaIndexTasks for our dataSource that do not have our supervisorRunId are killed (id3, id4).
   */
  public void testCallAndKillOtherPeoplesTasks() throws Exception
  {
    supervisor = getSupervisor(1, 1, true);
    addSomeEvents(1);

    List<Task> existingTasks = Lists.newArrayList();
    existingTasks.add(createKafkaIndexTask("id1", DATASOURCE, "sequence1", supervisor.getSupervisorRunId()));
    existingTasks.add(createKafkaIndexTask("id2", "other-datasource", "sequence2", supervisor.getSupervisorRunId()));
    existingTasks.add(createKafkaIndexTask("id3", DATASOURCE, "sequence3", "other-supervisor-id"));
    existingTasks.add(createKafkaIndexTask("id4", DATASOURCE, "sequence4", "other-supervisor-id-2"));
    existingTasks.add(
        new RealtimeIndexTask(
            "id5",
            null,
            new FireDepartment(
                dataSchema,
                new RealtimeIOConfig(null, null, null),
                null
            ),
            null
        )
    );

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(existingTasks).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    taskQueue.shutdown("id3");
    taskQueue.shutdown("id4");
    expect(taskQueue.add(anyObject(Task.class))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testCallRequeueTaskWhenFailed() throws Exception
  {
    supervisor = getSupervisor(2, 2, true);
    addSomeEvents(1);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(4);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    // test that invoking call() again checks the status of the tasks that were created and does nothing if they are
    // all still running
    reset(taskStorage);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    replay(taskStorage);

    supervisor.runInternal();
    verifyAll();

    // test that a task failing causes a new task to be re-queued with the same parameters
    Capture<Task> aNewTaskCapture = Capture.newInstance();
    List<Task> imStillAlive = tasks.subList(0, 3);
    KafkaIndexTask iHaveFailed = (KafkaIndexTask) tasks.get(3);
    reset(taskStorage);
    reset(taskQueue);
    expect(taskStorage.getActiveTasks()).andReturn(imStillAlive).anyTimes();
    for (Task task : imStillAlive) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskStorage.getStatus(iHaveFailed.getId())).andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
    expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of((Task) iHaveFailed)).anyTimes();
    expect(taskQueue.add(capture(aNewTaskCapture))).andReturn(true);
    replay(taskStorage);
    replay(taskQueue);

    supervisor.runInternal();
    verifyAll();

    Assert.assertNotEquals(iHaveFailed.getId(), aNewTaskCapture.getValue().getId());
    Assert.assertEquals(
        iHaveFailed.getIOConfig().getSequenceName(),
        ((KafkaIndexTask) aNewTaskCapture.getValue()).getIOConfig().getSequenceName()
    );
  }

  @Test
  public void testCallQueueNextTasksOnSuccess() throws Exception
  {
    supervisor = getSupervisor(2, 2, true);
    addSomeEvents(1);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new KafkaDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(4);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    reset(taskStorage);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    replay(taskStorage);

    supervisor.runInternal();
    verifyAll();

    // test that a task succeeding causes a new task to be re-queued with the next offset range and causes any replica
    // tasks to be shutdown
    Capture<Task> newTasksCapture = Capture.newInstance(CaptureType.ALL);
    Capture<String> shutdownTaskIdCapture = Capture.newInstance();
    List<Task> imStillRunning = tasks.subList(1, 4);
    KafkaIndexTask iAmSuccess = (KafkaIndexTask) tasks.get(0);
    reset(taskStorage);
    reset(taskQueue);
    expect(taskStorage.getActiveTasks()).andReturn(imStillRunning).anyTimes();
    for (Task task : imStillRunning) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskStorage.getStatus(iAmSuccess.getId())).andReturn(Optional.of(TaskStatus.success(iAmSuccess.getId())));
    expect(taskStorage.getTask(iAmSuccess.getId())).andReturn(Optional.of((Task) iAmSuccess)).anyTimes();
    taskQueue.shutdown(capture(shutdownTaskIdCapture));
    expect(taskQueue.add(capture(newTasksCapture))).andReturn(true).times(2);
    replay(taskStorage);
    replay(taskQueue);

    supervisor.runInternal();
    verifyAll();

    // make sure we killed the right task (sequenceName for replicas are the same)
    Assert.assertTrue(shutdownTaskIdCapture.getValue().contains(iAmSuccess.getIOConfig().getSequenceName()));

    // make sure we started up 2 replica tasks to cover the next offset range
    for (Integer partition : iAmSuccess.getIOConfig().getEndPartitions().getPartitionOffsetMap().keySet()) {
      Long previousEndOffset = iAmSuccess.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(partition);
      for (Task task : newTasksCapture.getValues()) {
        KafkaIndexTask kiTask = (KafkaIndexTask) task;
        Assert.assertEquals(
            previousEndOffset,
            kiTask.getIOConfig().getStartPartitions().getPartitionOffsetMap().get(partition)
        );
        Assert.assertEquals(
            previousEndOffset + 500L,
            (long) kiTask.getIOConfig().getEndPartitions().getPartitionOffsetMap().get(partition)
        );
      }
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testStopNotStarted() throws Exception
  {
    supervisor = getSupervisor(1, 1, true);
    supervisor.stop();
  }

  @Test
  public void testStop() throws Exception
  {
    supervisor = getSupervisor(1, 1, true);
    supervisor.start();
    supervisor.stop();
  }

  private void addSomeEvents(int numEventsPerPartition) throws Exception
  {
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (int i = 0; i < NUM_PARTITIONS; i++) {
        for (int j = 0; j < numEventsPerPartition; j++) {
          kafkaProducer.send(
              new ProducerRecord<byte[], byte[]>(
                  KAFKA_TOPIC,
                  i,
                  null,
                  String.format("event-%d", j).getBytes()
              )
          ).get();
        }
      }
    }
  }

  private KafkaSupervisor getSupervisor(int replicas, int taskCount, boolean useEarliestOffset)
  {
    KafkaSupervisorIOConfig kafkaSupervisorIOConfig = new KafkaSupervisorIOConfig(
        KAFKA_TOPIC,
        kafkaHost,
        replicas,
        taskCount,
        1000,
        ImmutableMap.of("myCustomKey", "myCustomValue"),
        new Period("PT1H"),
        new Period("PT30S"),
        useEarliestOffset
    );

    return new KafkaSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        new KafkaSupervisorSpec(
            dataSchema,
            tuningConfig,
            kafkaSupervisorIOConfig,
            taskStorage,
            taskMaster,
            indexerMetadataStorageCoordinator
        )
    );
  }

  private DataSchema getDataSchema(String dataSource)
  {
    return new DataSchema(
        dataSource,
        objectMapper.convertValue(
            new StringInputRowParser(
                new JSONParseSpec(
                    new TimestampSpec("timestamp", "iso", null),
                    new DimensionsSpec(
                        ImmutableList.of("dim1", "dim2"),
                        null,
                        null
                    ),
                    new JSONPathSpec(true, ImmutableList.<JSONPathFieldSpec>of()),
                    ImmutableMap.<String, Boolean>of()
                ),
                Charsets.UTF_8.name()
            ),
            Map.class
        ),
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        new UniformGranularitySpec(
            Granularity.HOUR,
            QueryGranularity.NONE,
            ImmutableList.<Interval>of()
        ),
        objectMapper
    );
  }

  private KafkaIndexTask createKafkaIndexTask(String id, String dataSource, String sequenceName, String runId)
  {
    return new KafkaIndexTask(
        id,
        null,
        getDataSchema(dataSource),
        tuningConfig,
        new KafkaIOConfig(
            sequenceName,
            new KafkaPartitions(KAFKA_TOPIC, ImmutableMap.<Integer, Long>of()),
            new KafkaPartitions(KAFKA_TOPIC, ImmutableMap.<Integer, Long>of()),
            ImmutableMap.<String, String>of(),
            true
        ),
        ImmutableMap.<String, Object>of(
            KafkaSupervisor.SUPERVISOR_CONTEXT_KEY,
            runId
        )
    );
  }
}
