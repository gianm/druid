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

package io.druid.indexing.kafka;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.CompressionUtils;
import com.metamx.common.Granularity;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.LoggingEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.MonitorScheduler;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.concurrent.Execs;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.JSONPathFieldSpec;
import io.druid.data.input.impl.JSONPathSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LocalTaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.kafka.test.TestBroker;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.MetadataTaskStorage;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.test.TestDataSegmentAnnouncer;
import io.druid.indexing.test.TestDataSegmentKiller;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.EntryExistsException;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.metadata.SQLMetadataStorageActionHandlerFactory;
import io.druid.metadata.TestDerbyConnector;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusherConfig;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.timeline.DataSegment;
import org.apache.curator.test.TestingCluster;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class KafkaIndexTaskTest
{
  private TestingCluster zkServer;
  private TestBroker kafkaServer;
  private ServiceEmitter emitter;
  private ListeningExecutorService taskExec;
  private TaskToolboxFactory toolboxFactory;
  private IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private TaskStorage taskStorage;
  private TaskLockbox taskLockbox;
  private File directory;

  private static final Logger log = new Logger(KafkaIndexTaskTest.class);

  private static final DataSchema DATA_SCHEMA;

  private static final List<ProducerRecord<byte[], byte[]>> RECORDS = ImmutableList.of(
      new ProducerRecord<byte[], byte[]>("topic0", 0, null, JB("2008", "a", "y", 1.0f)),
      new ProducerRecord<byte[], byte[]>("topic0", 0, null, JB("2009", "b", "y", 1.0f)),
      new ProducerRecord<byte[], byte[]>("topic0", 0, null, JB("2010", "c", "y", 1.0f)),
      new ProducerRecord<byte[], byte[]>("topic0", 0, null, JB("2011", "d", "y", 1.0f)),
      new ProducerRecord<byte[], byte[]>("topic0", 0, null, JB("2011", "e", "y", 1.0f)),
      new ProducerRecord<byte[], byte[]>("topic0", 0, null, JB("2013", "f", "y", 1.0f)),
      new ProducerRecord<byte[], byte[]>("topic0", 1, null, JB("2012", "g", "y", 1.0f))
  );

  static {
    ObjectMapper objectMapper = new DefaultObjectMapper();
    DATA_SCHEMA = new DataSchema(
        "test_ds",
        objectMapper.convertValue(
            new StringInputRowParser(
                new JSONParseSpec(
                    new TimestampSpec("timestamp", "iso", null),
                    new DimensionsSpec(
                        ImmutableList.<String>of("dim1", "dim2"),
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
        new UniformGranularitySpec(Granularity.DAY, QueryGranularity.NONE, null),
        objectMapper
    );
  }

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derby = new TestDerbyConnector.DerbyConnectorRule();

  @Before
  public void setUp() throws Exception
  {
    emitter = new ServiceEmitter(
        "service",
        "host",
        new LoggingEmitter(
            log,
            LoggingEmitter.Level.ERROR,
            new DefaultObjectMapper()
        )
    );
    emitter.start();
    EmittingLogger.registerEmitter(emitter);

    makeToolboxFactory();

    zkServer = new TestingCluster(1);
    zkServer.start();

    kafkaServer = new TestBroker(
        zkServer.getConnectString(),
        tempFolder.newFolder(),
        1,
        ImmutableMap.of("num.partitions", "2")
    );
    kafkaServer.start();

    taskExec = MoreExecutors.listeningDecorator(
        Executors.newCachedThreadPool(
            Execs.makeThreadFactory("kafka-task-test-%d")
        )
    );
  }

  @After
  public void tearDown() throws Exception
  {
    emitter.close();

    taskExec.shutdownNow();

    // TODO: For some reason the interrupt above seems to stall out the task thread
//    taskExec.awaitTermination(9999, TimeUnit.DAYS);

    kafkaServer.close();
    kafkaServer = null;

    zkServer.stop();
    zkServer = null;

    destroyToolboxFactory();
  }

  @Test(timeout = 60_000L)
  public void testRunSimple() throws Exception
  {
    final KafkaIndexTask task = createTask(
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions("topic0", ImmutableMap.of(0, 2L)),
            new KafkaPartitions("topic0", ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true
        ),
        null
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for task to exit
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions("topic0", ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunReplicas() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions("topic0", ImmutableMap.of(0, 2L)),
            new KafkaPartitions("topic0", ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true
        ),
        null
    );
    final KafkaIndexTask task2 = createTask(
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions("topic0", ImmutableMap.of(0, 2L)),
            new KafkaPartitions("topic0", ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true
        ),
        null
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for tasks to exit
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get().getStatusCode());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions("topic0", ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunConflicting() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions("topic0", ImmutableMap.of(0, 2L)),
            new KafkaPartitions("topic0", ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true
        ),
        null
    );
    final KafkaIndexTask task2 = createTask(
        new KafkaIOConfig(
            "sequence1",
            new KafkaPartitions("topic0", ImmutableMap.of(0, 3L)),
            new KafkaPartitions("topic0", ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true
        ),
        null
    );

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
        kafkaProducer.send(record).get();
      }
    }

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskStatus.Status.FAILED, future2.get().getStatusCode());

    // Check published segments & metadata, should all be from the first task
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions("topic0", ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunConflictingWithoutTransactions() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions("topic0", ImmutableMap.of(0, 2L)),
            new KafkaPartitions("topic0", ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            false
        ),
        null
    );
    final KafkaIndexTask task2 = createTask(
        new KafkaIOConfig(
            "sequence1",
            new KafkaPartitions("topic0", ImmutableMap.of(0, 3L)),
            new KafkaPartitions("topic0", ImmutableMap.of(0, 6L)),
            kafkaServer.consumerProperties(),
            false
        ),
        null
    );

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
        kafkaProducer.send(record).get();
      }
    }

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get().getStatusCode());

    // Check published segments & metadata
    SegmentDescriptor desc3 = SD(task2, "2011/P1D", 1);
    SegmentDescriptor desc4 = SD(task2, "2013/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc3));
    Assert.assertEquals(ImmutableList.of("f"), readSegmentDim1(desc4));
  }

  @Test(timeout = 60_000L)
  public void testRunOneTaskTwoPartitions() throws Exception
  {
    final KafkaIndexTask task = createTask(
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions("topic0", ImmutableMap.of(0, 2L, 1, 0L)),
            new KafkaPartitions("topic0", ImmutableMap.of(0, 5L, 1, 1L)),
            kafkaServer.consumerProperties(),
            true
        ),
        null
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
        RecordMetadata recordMetadata = kafkaProducer.send(record).get();
      }
    }

    // Wait for tasks to exit
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2012/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions("topic0", ImmutableMap.of(0, 5L, 1, 1L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
    Assert.assertEquals(ImmutableList.of("g"), readSegmentDim1(desc3));
  }

  @Test(timeout = 60_000L)
  public void testRunTwoTasksTwoPartitions() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions("topic0", ImmutableMap.of(0, 2L)),
            new KafkaPartitions("topic0", ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true
        ),
        null
    );
    final KafkaIndexTask task2 = createTask(
        new KafkaIOConfig(
            "sequence1",
            new KafkaPartitions("topic0", ImmutableMap.of(1, 0L)),
            new KafkaPartitions("topic0", ImmutableMap.of(1, 1L)),
            kafkaServer.consumerProperties(),
            true
        ),
        null
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : RECORDS) {
        RecordMetadata recordMetadata = kafkaProducer.send(record).get();
      }
    }

    // Wait for tasks to exit
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get().getStatusCode());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    SegmentDescriptor desc3 = SD(task2, "2012/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions("topic0", ImmutableMap.of(0, 5L, 1, 1L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
    Assert.assertEquals(ImmutableList.of("g"), readSegmentDim1(desc3));
  }

  private ListenableFuture<TaskStatus> runTask(final Task task)
  {
    try {
      taskStorage.insert(task, TaskStatus.running(task.getId()));
    }
    catch (EntryExistsException e) {
      // suppress
    }
    taskLockbox.syncFromStorage();
    final TaskToolbox toolbox = toolboxFactory.build(task);
    return taskExec.submit(
        new Callable<TaskStatus>()
        {
          @Override
          public TaskStatus call() throws Exception
          {
            try {
              if (task.isReady(toolbox.getTaskActionClient())) {
                return task.run(toolbox);
              } else {
                throw new ISE("Task is not ready");
              }
            }
            catch (Exception e) {
              log.warn(e, "Task failed");
              return TaskStatus.failure(task.getId());
            }
          }
        }
    );
  }

  private TaskLock getLock(final Task task, final Interval interval)
  {
    return Iterables.find(
        taskLockbox.findLocksForTask(task),
        new Predicate<TaskLock>()
        {
          @Override
          public boolean apply(TaskLock lock)
          {
            return lock.getInterval().contains(interval);
          }
        }
    );
  }

  private KafkaIndexTask createTask(
      final KafkaIOConfig ioConfig,
      final Integer maxRowsPerSegment
  )
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final KafkaTuningConfig tuningConfig = new KafkaTuningConfig(
        1000,
        maxRowsPerSegment,
        new Period("P1Y"),
        null,
        null,
        null
    );
    return new KafkaIndexTask(
        null,
        null,
        DATA_SCHEMA,
        tuningConfig,
        ioConfig,
        null
    );
  }

  private QueryRunnerFactoryConglomerate makeTimeseriesOnlyConglomerate()
  {
    return new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>of(
            TimeseriesQuery.class,
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(
                    new IntervalChunkingQueryRunnerDecorator(null, null, null)
                    {
                      @Override
                      public <T> QueryRunner<T> decorate(
                          QueryRunner<T> delegate, QueryToolChest<T, ? extends Query<T>> toolChest
                      )
                      {
                        return delegate;
                      }
                    }
                ),
                new TimeseriesQueryEngine(),
                new QueryWatcher()
                {
                  @Override
                  public void registerQuery(Query query, ListenableFuture future)
                  {
                    // do nothing
                  }
                }
            )
        )
    );
  }

  private void makeToolboxFactory() throws IOException
  {
    directory = tempFolder.newFolder();
    final TestUtils testUtils = new TestUtils();
    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
    for (Module module : new KafkaIndexTaskModule().getJacksonModules()) {
      objectMapper.registerModule(module);
    }
    final TaskConfig taskConfig = new TaskConfig(
        new File(directory, "taskBaseDir").getPath(),
        null,
        null,
        50000,
        null,
        false,
        null,
        null
    );
    final TestDerbyConnector derbyConnector = derby.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createPendingSegmentsTable();
    derbyConnector.createSegmentTable();
    derbyConnector.createRulesTable();
    derbyConnector.createConfigTable();
    derbyConnector.createTaskTables();
    derbyConnector.createAuditTable();
    taskStorage = new MetadataTaskStorage(
        derbyConnector,
        new TaskStorageConfig(null),
        new SQLMetadataStorageActionHandlerFactory(
            derbyConnector,
            derby.metadataTablesConfigSupplier().get(),
            objectMapper
        )
    );
    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        testUtils.getTestObjectMapper(),
        derby.metadataTablesConfigSupplier().get(),
        derbyConnector
    );
    taskLockbox = new TaskLockbox(taskStorage);
    final TaskActionToolbox taskActionToolbox = new TaskActionToolbox(
        taskLockbox,
        metadataStorageCoordinator,
        emitter
    );
    final TaskActionClientFactory taskActionClientFactory = new LocalTaskActionClientFactory(
        taskStorage,
        taskActionToolbox
    );
    final SegmentHandoffNotifierFactory handoffNotifierFactory = new SegmentHandoffNotifierFactory()
    {
      @Override
      public SegmentHandoffNotifier createSegmentHandoffNotifier(String dataSource)
      {
        return new SegmentHandoffNotifier()
        {
          @Override
          public boolean registerSegmentHandoffCallback(
              SegmentDescriptor descriptor, Executor exec, Runnable handOffRunnable
          )
          {
            // Simulate immediate handoff
            handOffRunnable.run();
            return true;
          }

          @Override
          public void start()
          {
            //Noop
          }

          @Override
          public void close()
          {
            //Noop
          }
        };
      }
    };
    final LocalDataSegmentPusherConfig dataSegmentPusherConfig = new LocalDataSegmentPusherConfig();
    dataSegmentPusherConfig.storageDirectory = getSegmentDirectory();
    final DataSegmentPusher dataSegmentPusher = new LocalDataSegmentPusher(dataSegmentPusherConfig, objectMapper);
    toolboxFactory = new TaskToolboxFactory(
        taskConfig,
        taskActionClientFactory,
        emitter,
        dataSegmentPusher,
        new TestDataSegmentKiller(),
        null, // DataSegmentMover
        null, // DataSegmentArchiver
        new TestDataSegmentAnnouncer(),
        handoffNotifierFactory,
        makeTimeseriesOnlyConglomerate(),
        MoreExecutors.sameThreadExecutor(), // queryExecutorService
        EasyMock.createMock(MonitorScheduler.class),
        new SegmentLoaderFactory(
            new SegmentLoaderLocalCacheManager(
                null,
                new SegmentLoaderConfig()
                {
                  @Override
                  public List<StorageLocationConfig> getLocations()
                  {
                    return Lists.newArrayList();
                  }
                }, testUtils.getTestObjectMapper()
            )
        ),
        testUtils.getTestObjectMapper(),
        testUtils.getTestIndexMerger(),
        testUtils.getTestIndexIO(),
        MapCache.create(1024),
        new CacheConfig(),
        testUtils.getTestIndexMergerV9()
    );
  }

  private void destroyToolboxFactory()
  {
    toolboxFactory = null;
    taskStorage = null;
    taskLockbox = null;
    metadataStorageCoordinator = null;
  }

  private Set<SegmentDescriptor> publishedDescriptors() throws IOException
  {
    return FluentIterable.from(
        metadataStorageCoordinator.getUsedSegmentsForInterval(
            DATA_SCHEMA.getDataSource(),
            new Interval("0000/3000")
        )
    ).transform(
        new Function<DataSegment, SegmentDescriptor>()
        {
          @Override
          public SegmentDescriptor apply(DataSegment input)
          {
            return input.toDescriptor();
          }
        }
    ).toSet();
  }

  private File getSegmentDirectory()
  {
    return new File(directory, "segments");
  }

  private List<String> readSegmentDim1(final SegmentDescriptor descriptor) throws IOException
  {
    File indexZip = new File(
        String.format(
            "%s/%s/%s_%s/%s/%d/index.zip",
            getSegmentDirectory(),
            DATA_SCHEMA.getDataSource(),
            descriptor.getInterval().getStart(),
            descriptor.getInterval().getEnd(),
            descriptor.getVersion(),
            descriptor.getPartitionNumber()
        )
    );
    File outputLocation = new File(
        directory,
        String.format(
            "%s_%s_%s_%s",
            descriptor.getInterval().getStart(),
            descriptor.getInterval().getEnd(),
            descriptor.getVersion(),
            descriptor.getPartitionNumber()
        )
    );
    outputLocation.mkdir();
    CompressionUtils.unzip(
        Files.asByteSource(indexZip),
        outputLocation,
        Predicates.<Throwable>alwaysFalse(),
        false
    );
    IndexIO indexIO = new TestUtils().getTestIndexIO();
    QueryableIndex index = indexIO.loadIndex(outputLocation);
    DictionaryEncodedColumn dim1 = index.getColumn("dim1").getDictionaryEncoding();
    List<String> values = Lists.newArrayList();
    for (int i = 0; i < dim1.length(); i++) {
      int id = dim1.getSingleValueRow(i);
      String value = dim1.lookupName(id);
      values.add(value);
    }
    return values;
  }

  private static byte[] JB(String timestamp, String dim1, String dim2, double met1)
  {
    try {
      return new ObjectMapper().writeValueAsBytes(
          ImmutableMap.of("timestamp", timestamp, "dim1", dim1, "dim2", dim2, "met1", met1)
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private SegmentDescriptor SD(final Task task, final String intervalString, final int partitionNum)
  {
    final Interval interval = new Interval(intervalString);
    return new SegmentDescriptor(interval, getLock(task, interval).getVersion(), partitionNum);
  }
}
