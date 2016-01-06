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

package io.druid.indexing.common.task.rttd;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.SegmentAllocateAction;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.common.task.TaskResource;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.FiniteAppenderatorDriver;
import io.druid.segment.realtime.appenderator.SegmentAllocator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentNotWritableException;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.segment.realtime.firehose.ChatHandler;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Push-based realtime task that uses Appenderators to write data.
 */
public class RealtimeIndexTaskTokyoDrift extends AbstractTask
{
  private static final String TYPE = "index_realtime_tokyo_drift";
  private static final Logger log = new Logger(RealtimeIndexTaskTokyoDrift.class);
  private static final Random RANDOM = new Random();
  private static final long POLL_TIMEOUT = 100;
  private static final int DEFAULT_BUFFER_SIZE = 100000;
  private static final int MAX_SEGMENT_ROWS = 10000;

  private final InputRowParser<Map<String, Object>> parser;
  private final int slotNum;
  private final DataSchema dataSchema;
  private final RealtimeTuningConfig tuningConfig;
  private final ChatHandlerProvider chatHandlerProvider;
  private final Committer committer = Committers.nil();
  private final Supplier<Committer> committerSupplier = Suppliers.ofInstance(committer);
  private final RequestBuffer buffer;

  private volatile Appenderator appenderator = null;
  private volatile FiniteAppenderatorDriver driver = null;
  private volatile long currentEpoch = 0;
  private volatile State currentState = State.FOLLOWING;

  enum State
  {
    FOLLOWING,
    LEADING,
    HANDOFF,
    SHUTDOWN
  }

  @JsonCreator
  public RealtimeIndexTaskTokyoDrift(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("maxBufferSize") Integer maxBufferSize,
      @JsonProperty("slotNum") Integer slotNum,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") RealtimeTuningConfig tuningConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider
  )
  {
    super(
        id == null ? makeTaskId(dataSchema.getDataSource(), slotNum, RANDOM.nextInt()) : id,
        String.format("%s_%s", TYPE, dataSchema.getDataSource()),
        taskResource,
        dataSchema.getDataSource(),
        context
    );

    this.parser = Preconditions.checkNotNull((InputRowParser<Map<String, Object>>) dataSchema.getParser(), "parser");
    this.buffer = new RequestBuffer(maxBufferSize == null ? DEFAULT_BUFFER_SIZE : maxBufferSize);
    this.slotNum = Preconditions.checkNotNull(slotNum, "slotNum");
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.chatHandlerProvider = chatHandlerProvider;
  }

  private static String makeTaskId(String dataSource, int slotNum, int randomBits)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Ints.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((randomBits >>> (i * 4)) & 0x0F)));
    }
    return String.format(
        "%s_%s_%d_%s",
        TYPE,
        dataSource,
        slotNum,
        suffix
    );
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public String getNodeType()
  {
    return "realtime";
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

  @JsonProperty
  public int getSlotNum()
  {
    return slotNum;
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty
  public RealtimeTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    log.info("Starting up!");

    final FireDepartmentMetrics metrics = new FireDepartmentMetrics();
    try (
        final Appenderator appenderator0 = newAppenderator(metrics, toolbox);
        final Resource resource = new Resource()
    ) {
      this.appenderator = appenderator0;
      try {
        while (currentState == State.LEADING || currentState == State.FOLLOWING) {
          final RttdRequest untypedRequest = buffer.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);

          if (untypedRequest instanceof AppenderationRequest) {
            final AppenderationRequest request = (AppenderationRequest) untypedRequest;
            final AppenderationResponse response = handleAppenderationRequest(request, metrics, toolbox);
            request.getResponse().set(response);
          } else if (untypedRequest instanceof HandoffRequest) {
            final HandoffRequest request = (HandoffRequest) untypedRequest;
            final HandoffResponse response = handleHandoffRequest(request);
            request.getResponse().set(response);
          } else if (untypedRequest instanceof ShutDownRequest) {
            final ShutDownRequest request = (ShutDownRequest) untypedRequest;
            final ShutDownResponse response = handleShutDownRequest(request);
            request.getResponse().set(response);
          } else if (untypedRequest != null) {
            log.error("WTF?! Unrecognized request type.");
            untypedRequest.getResponse().setException(new ISE("Unrecognized request type."));
          }
        }

        // Stopped
        RttdRequest request;
        while ((request = buffer.poll()) != null) {
          request.getResponse().setException(new ISE("Stopped"));
        }

        if (currentState == State.HANDOFF) {
          final SegmentsAndMetadata result = driver.finish(
              makeTransactionalSegmentPublisher(toolbox),
              committerSupplier.get()
          );
          if (result == null) {
            throw new ISE("Failed to publish segments");
          }
        }
      }
      finally {
        if (driver != null) {
          driver.close();
        }
      }
    }

    return TaskStatus.success(getId());
  }

  private TransactionalSegmentPublisher makeTransactionalSegmentPublisher(final TaskToolbox toolbox)
  {
    return new TransactionalSegmentPublisher()
    {
      @Override
      public boolean publishSegments(Set<DataSegment> segments, Object commitMetadata) throws IOException
      {
        if (commitMetadata != null) {
          throw new ISE("WTF?! commitMetadata should be null, but was[%s]", commitMetadata);
        }
        return toolbox.getTaskActionClient().submit(new SegmentInsertAction(segments, null, null)).isSuccess();
      }
    };
  }

  public <T> T performRequest(final RttdRequest<T> request) throws InterruptedException
  {
    return buffer.perform(request);
  }

  private AppenderationResponse handleAppenderationRequest(
      final AppenderationRequest request,
      final FireDepartmentMetrics metrics,
      final TaskToolbox toolbox
  ) throws IOException, SegmentNotWritableException
  {
    if (request.isLeader()) {
      if (transition(State.LEADING, request.getEpoch())) {
        // TODO: Use dataSource metadata to ensure clean transition to a new epoch? Prevent old leader from publishing.
        log.info("You have raised your voices in an unmistakable chorus.");

        // Create a new driver.
        assert driver == null;
        driver = newDriver(appenderator, toolbox);

        // Acquire locks for all pending segments.
        for (final SegmentIdentifier identifier : appenderator.getSegments()) {
          final TaskLock tryLock = toolbox.getTaskActionClient()
                                          .submit(new LockTryAcquireAction(identifier.getInterval()));

          final String tryLockVersion = tryLock == null ? null : tryLock.getVersion();

          if (tryLockVersion != null && tryLockVersion.compareTo(identifier.getVersion()) >= 0) {
            log.info("Taking over segment[%s] from prior epoch.", identifier);
          } else {
            // TODO: Priority locking would help here, so we can always get the lock.
            throw new ISE(
                "Could not reacquire lock for segment[%s] (got lock version[%s])",
                identifier,
                tryLockVersion
            );
          }
        }

        log.info(
            "Leading slotNum[%,d], epoch[%,d] with %,d segments from prior epochs to manage.",
            slotNum,
            currentEpoch,
            appenderator.getSegments().size()
        );
      }

      if (currentState == State.LEADING && currentEpoch == request.getEpoch()) {
        final List<SegmentIdentifier> retVal = Lists.newArrayList();

        for (SegmentAndRows segmentAndRows : request.getRows()) {
          for (InputRow row : segmentAndRows.getInputRows(log, parser)) {
            if (row == null) {
              metrics.incrementUnparseable();
              retVal.add(null);
            } else {
              // We're leading; use the driver.
              final SegmentIdentifier identifier = driver.add(row, committerSupplier);
              metrics.incrementProcessed();
              retVal.add(identifier);
            }
          }
        }

        if (retVal.size() != request.getRowCount()) {
          throw new ISE("WTF?! Return size[%,d] does not match input size[%,d].", retVal.size(), request.getRowCount());
        }

        return new AppenderationResponse(true, currentEpoch, retVal);
      } else {
        return new AppenderationResponse(false, currentEpoch, ImmutableList.<SegmentIdentifier>of());
      }
    } else {
      if (currentState == State.FOLLOWING) {
        final List<SegmentIdentifier> retVal = Lists.newArrayList();

        for (SegmentAndRows segmentAndRows : request.getRows()) {
          for (InputRow row : segmentAndRows.getInputRows(log, parser)) {
            if (row == null) {
              metrics.incrementUnparseable();
              retVal.add(null);
            } else {
              // We're following; use the appenderator directly rather than the driver.
              final SegmentIdentifier identifier = Preconditions.checkNotNull(
                  segmentAndRows.getIdentifier(),
                  "identifier"
              );
              appenderator.add(identifier, row, committerSupplier);
              metrics.incrementProcessed();
              retVal.add(identifier);
            }
          }
        }

        return new AppenderationResponse(true, currentEpoch, retVal);
      } else {
        return new AppenderationResponse(false, currentEpoch, ImmutableList.<SegmentIdentifier>of());
      }
    }
  }

  private HandoffResponse handleHandoffRequest(
      final HandoffRequest request
  ) throws IOException
  {
    if (transition(State.HANDOFF, request.getEpoch())) {
      log.info("Starting handoff.");
      buffer.close();
      return new HandoffResponse(true, currentEpoch);
    } else {
      return new HandoffResponse(false, currentEpoch);
    }
  }

  private ShutDownResponse handleShutDownRequest(
      final ShutDownRequest request
  ) throws IOException
  {
    if (transition(State.SHUTDOWN, request.getEpoch())) {
      log.info("Shutting down.");
      buffer.close();
      return new ShutDownResponse(true, currentEpoch);
    } else {
      return new ShutDownResponse(false, currentEpoch);
    }
  }

  // Transition to a state; true if state or epoch changed after this call, false if it didn't
  private boolean transition(final State state, final long epoch)
  {
    final boolean retVal;

    if (epoch <= currentEpoch) {
      retVal = false;
    } else if (state == State.LEADING && currentState == State.FOLLOWING) {
      retVal = true;
    } else if (state == State.HANDOFF && (currentState == State.FOLLOWING || currentState == State.LEADING)) {
      retVal = true;
    } else if (state == State.SHUTDOWN && currentState != State.SHUTDOWN) {
      retVal = true;
    } else {
      retVal = false;
    }

    if (retVal) {
      log.info("Accepted transition of epoch %s -> %s, state %s -> %s.", currentEpoch, epoch, currentState, state);
      currentEpoch = epoch;
      currentState = state;
    } else {
      log.info("Rejected transition of epoch %s -> %s, state %s -> %s.", currentEpoch, epoch, currentState, state);
    }

    return retVal;
  }

  private Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox)
  {
    final Appenderator appenderator = Appenderators.createRealtime(
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

    log.info("We have an appenderator. Starting the job!");
    appenderator.startJob();

    return appenderator;
  }

  private FiniteAppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox
  )
  {
    final SegmentAllocator segmentAllocator = new SegmentAllocator()
    {
      @Override
      public SegmentIdentifier allocate(DateTime timestamp) throws IOException
      {
        // Random sequenceName- we just want a segment that has never been used before.
        final String sequenceName = UUID.randomUUID().toString();

        return toolbox.getTaskActionClient().submit(
            new SegmentAllocateAction(
                getDataSource(),
                timestamp,
                dataSchema.getGranularitySpec().getQueryGranularity(),
                dataSchema.getGranularitySpec().getSegmentGranularity(),
                sequenceName,
                null
            )
        );
      }
    };

    return new FiniteAppenderatorDriver(
        appenderator,
        segmentAllocator,
        toolbox.getSegmentHandoffNotifierFactory(),
        MAX_SEGMENT_ROWS
    );
  }

  public class Resource implements ChatHandler, Closeable
  {
    public Resource()
    {
      // TODO: Causes clutter in service discovery; want some better way of getting this info out of the overlord
      chatHandlerProvider.register(getId(), this);
    }

    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getStatus()
    {
      return ImmutableMap.of(
          "bufferSize", buffer.size(),
          "activeSegments", driver == null ? ImmutableList.of() : Ordering.natural().sortedCopy(
              Iterables.transform(
                  driver.getActiveSegments(),
                  new Function<SegmentIdentifier, String>()
                  {
                    @Override
                    public String apply(SegmentIdentifier identifier)
                    {
                      return identifier.getIdentifierAsString();
                    }
                  }
              )
          ),
          "segments", Ordering.natural().sortedCopy(
              Iterables.transform(
                  appenderator.getSegments(),
                  new Function<SegmentIdentifier, String>()
                  {
                    @Override
                    public String apply(SegmentIdentifier identifier)
                    {
                      return identifier.getIdentifierAsString();
                    }
                  }
              )
          ),
          "currentEpoch", currentEpoch,
          "currentState", currentState
      );
    }

    @POST
    @Path("/do")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Object doRequest(final RttdRequest<?> request) throws InterruptedException
    {
      // TODO: Server threads can all get blocked if the main loop is exerting backpressure
      // TODO: Confirm nothing important is going to get blocked by that
      return performRequest(request);
    }

    @Override
    public void close()
    {
      chatHandlerProvider.unregister(getId());
    }
  }
}
