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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentAllocateAction;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.common.task.TaskResource;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.FiniteAppenderatorDriver;
import io.druid.segment.realtime.appenderator.SegmentAllocator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.timeline.DataSegment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class KafkaIndexTask extends AbstractTask
{
  private static final Logger log = new Logger(KafkaIndexTask.class);
  private static final String TYPE = "index_kafka";
  private static final Random RANDOM = new Random();
  private static final long POLL_TIMEOUT = 100;

  private final DataSchema dataSchema;
  private final InputRowParser<ByteBuffer> parser;
  private final KafkaTuningConfig tuningConfig;
  private final KafkaIOConfig ioConfig;

  private volatile Appenderator appenderator = null;

  @JsonCreator
  public KafkaIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KafkaTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KafkaIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id == null ? makeTaskId(dataSchema.getDataSource(), RANDOM.nextInt()) : id,
        String.format("%s_%s", TYPE, dataSchema.getDataSource()),
        taskResource,
        dataSchema.getDataSource(),
        context
    );

    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.parser = Preconditions.checkNotNull((InputRowParser<ByteBuffer>) dataSchema.getParser(), "parser");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");
  }

  private static String makeTaskId(String dataSource, int randomBits)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Ints.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((randomBits >>> (i * 4)) & 0x0F)));
    }
    return Joiner.on("_").join(TYPE, dataSource, suffix);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty
  public KafkaTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty
  public KafkaIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    log.info("Starting up!");

    final FireDepartmentMetrics metrics = new FireDepartmentMetrics();
    try (
        final Appenderator appenderator0 = newAppenderator(metrics, toolbox);
        final FiniteAppenderatorDriver driver = newDriver(appenderator0, toolbox);
        final KafkaConsumer<byte[], byte[]> consumer = newConsumer()
    ) {
      appenderator = appenderator0;

      final String topic = ioConfig.getStartPartitions().getTopic();

      // Start up, set up initial offsets.
      final Object metadata = appenderator.startJob();
      final Map<Integer, Long> nextOffsets = Maps.newHashMap();
      if (metadata == null) {
        nextOffsets.putAll(ioConfig.getStartPartitions().getPartitionOffsetMap());
      } else {
        final Map<Integer, Long> metadataAsMap = (Map<Integer, Long>) metadata;
        nextOffsets.putAll(metadataAsMap);

        // Sanity check.
        if (!nextOffsets.keySet().equals(ioConfig.getStartPartitions().getPartitionOffsetMap().keySet())) {
          throw new ISE(
              "WTF?! Restored partitions[%s] but expected partitions[%s]",
              nextOffsets.keySet(),
              ioConfig.getStartPartitions().getPartitionOffsetMap().keySet()
          );
        }
      }

      // Set up committer.
      final Supplier<Committer> committerSupplier = new Supplier<Committer>()
      {
        @Override
        public Committer get()
        {
          final Map<Integer, Long> snapshot = ImmutableMap.copyOf(nextOffsets);

          return new Committer()
          {
            @Override
            public Object getMetadata()
            {
              return snapshot;
            }

            @Override
            public void run()
            {
              // Do nothing.
            }
          };
        }
      };

      // Initialize consumer assignment.
      final Set<Integer> assignment = Sets.newHashSet();
      for (Map.Entry<Integer, Long> entry : nextOffsets.entrySet()) {
        final long endOffset = ioConfig.getEndPartitions().getPartitionOffsetMap().get(entry.getKey());
        if (entry.getValue() < endOffset) {
          assignment.add(entry.getKey());
        } else if (entry.getValue() == endOffset) {
          log.info("Finished reading partition[%d].", entry.getKey());
        } else {
          throw new ISE(
              "WTF?! Cannot start from offset[%,d] > endOffset[%,d]",
              entry.getValue(),
              endOffset
          );
        }
      }

      assignPartitions(consumer, topic, assignment);

      // Seek to starting offsets.
      for (final int partition : assignment) {
        final long offset = nextOffsets.get(partition);
        log.info("Seeking partition[%d] to offset[%,d].", partition, offset);
        consumer.seek(new TopicPartition(topic, partition), offset);
      }

      // Main loop.
      boolean stillReading = true;
      while (stillReading) {
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException();
        }

        final ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);
        for (ConsumerRecord<byte[], byte[]> record : records) {
          if (log.isTraceEnabled()) {
            log.trace("Got topic[%s] partition[%d] offset[%,d].", record.topic(), record.partition(), record.offset());
          }

          if (record.offset() < ioConfig.getEndPartitions().getPartitionOffsetMap().get(record.partition())) {
            if (record.offset() != nextOffsets.get(record.partition())) {
              throw new ISE(
                  "WTF?! Got offset[%,d] after offset[%,d] in partition[%d].",
                  record.offset(),
                  nextOffsets.get(record.partition()),
                  record.partition()
              );
            }

            InputRow row;
            try {
              row = Preconditions.checkNotNull(parser.parse(ByteBuffer.wrap(record.value())), "row");
            }
            catch (Exception e) {
              if (log.isDebugEnabled()) {
                log.debug(
                    e,
                    "Dropping unparseable row from partition[%d] offset[%,d].",
                    record.partition(),
                    record.offset()
                );
              }
              row = null;
            }

            if (row != null) {
              driver.add(row, committerSupplier);
              metrics.incrementProcessed();
            } else {
              metrics.incrementUnparseable();
            }

            final long nextOffset = record.offset() + 1;
            final long endOffset = ioConfig.getEndPartitions().getPartitionOffsetMap().get(record.partition());

            nextOffsets.put(record.partition(), nextOffset);

            if (nextOffset == endOffset && assignment.remove(record.partition())) {
              log.info("Finished reading topic[%s], partition[%,d].", record.topic(), record.partition());
              assignPartitions(consumer, topic, assignment);
              stillReading = !assignment.isEmpty();
            }
          }
        }
      }

      final TransactionalSegmentPublisher publisher = new TransactionalSegmentPublisher()
      {
        @Override
        public boolean publishSegments(Set<DataSegment> segments, Object commitMetadata) throws IOException
        {
          // Sanity check, we should only be publishing things that match our desired end state.
          if (!ioConfig.getEndPartitions().getPartitionOffsetMap().equals(commitMetadata)) {
            throw new ISE("WTF?! Driver attempted to publish invalid metadata[%s].", commitMetadata);
          }

          final SegmentInsertAction action;

          if (ioConfig.isUseTransaction()) {
            action = new SegmentInsertAction(
                segments,
                new KafkaDataSourceMetadata(ioConfig.getStartPartitions()),
                new KafkaDataSourceMetadata(ioConfig.getEndPartitions())
            );
          } else {
            action = new SegmentInsertAction(segments, null, null);
          }

          log.info("Publishing with isTransaction[%s].", ioConfig.isUseTransaction());

          return toolbox.getTaskActionClient().submit(action).isSuccess();
        }
      };

      final SegmentsAndMetadata published = driver.finish(publisher, committerSupplier.get());
      if (published == null) {
        throw new ISE("Transaction failure publishing segments, aborting");
      } else {
        log.info(
            "Published segments[%s] with metadata[%s].",
            Joiner.on(", ").join(
                Iterables.transform(
                    published.getSegments(),
                    new Function<DataSegment, String>()
                    {
                      @Override
                      public String apply(DataSegment input)
                      {
                        return input.getIdentifier();
                      }
                    }
                )
            ),
            published.getCommitMetadata()
        );
      }
    }

    return success();
  }

  @Override
  public boolean canRestore()
  {
    // TODO
    return false;
  }

  @Override
  public void stopGracefully()
  {
    // TODO
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    if (appenderator == null) {
      return null;
    }

    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
      {
        return query.run(appenderator, responseContext);
      }
    };
  }

  private Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox)
  {
    return Appenderators.createRealtime(
        dataSchema,
        tuningConfig.withBasePersistDirectory(new File(toolbox.getTaskWorkDir(), "persist")),
        metrics,
        toolbox.getSegmentPusher(),
        toolbox.getObjectMapper(),
        toolbox.getIndexIO(),
        toolbox.getIndexMerger(),
        toolbox.getQueryRunnerFactoryConglomerate(),
        toolbox.getSegmentAnnouncer(),
        toolbox.getEmitter(),
        toolbox.getQueryExecutorService(),
        toolbox.getCache(),
        toolbox.getCacheConfig()
    );
  }

  private FiniteAppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox
  )
  {
    final SegmentAllocator segmentAllocator = new SegmentAllocator()
    {
      private final Object lock = new Object();
      private String previousSegmentId = null;

      @Override
      public SegmentIdentifier allocate(DateTime timestamp) throws IOException
      {
        synchronized (lock) {
          final SegmentIdentifier identifier = toolbox.getTaskActionClient().submit(
              new SegmentAllocateAction(
                  getDataSource(),
                  timestamp,
                  dataSchema.getGranularitySpec().getQueryGranularity(),
                  dataSchema.getGranularitySpec().getSegmentGranularity(),
                  ioConfig.getSequenceName(),
                  previousSegmentId
              )
          );

          previousSegmentId = identifier.getIdentifierAsString();

          return identifier;
        }
      }
    };

    return new FiniteAppenderatorDriver(
        appenderator,
        segmentAllocator,
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        tuningConfig.getMaxRowsPerSegment()
    );
  }

  private KafkaConsumer<byte[], byte[]> newConsumer()
  {
    final Properties props = new Properties();

    for (Map.Entry<String, String> entry : ioConfig.getConsumerProperties().entrySet()) {
      props.setProperty(entry.getKey(), entry.getValue());
    }

    props.setProperty("enable.auto.commit", "false");
    props.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
    props.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());

    return new KafkaConsumer<>(props);
  }

  private static void assignPartitions(
      final KafkaConsumer consumer,
      final String topic,
      final Set<Integer> partitions
  )
  {
    consumer.assign(
        Lists.newArrayList(
            Iterables.transform(
                partitions,
                new Function<Integer, TopicPartition>()
                {
                  @Override
                  public TopicPartition apply(Integer n)
                  {
                    return new TopicPartition(topic, n);
                  }
                }
            )
        )
    );
  }
}
