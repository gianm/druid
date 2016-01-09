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

package io.druid.segment.realtime.appenderator;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

/**
 * A FiniteAppenderatorDriver drives an Appenderator to index a finite stream of data. This class does not help you
 * index unbounded streams. All handoff is done at the end of indexing.
 * <p/>
 * This class helps with doing things that Appenderators don't, including deciding which segments to use (with a
 * SegmentAllocator), publishing segments to the metadata store (with a SegmentPublisher), and monitoring handoff (with
 * a SegmentHandoffNotifier).
 */
public class FiniteAppenderatorDriver implements Closeable
{
  private static final Logger log = new Logger(FiniteAppenderatorDriver.class);

  private final Appenderator appenderator;
  private final SegmentAllocator segmentAllocator;
  private final SegmentHandoffNotifier handoffNotifier;
  private final int maxRowsPerSegment;

  // Key = Start of segment intervals. Value = Segment we're currently adding data to.
  // All access must be synchronized.
  private final NavigableMap<Long, SegmentIdentifier> activeSegments = new TreeMap<>();

  // Notified when segments are dropped.
  private final Object handoffMonitor = new Object();

  public FiniteAppenderatorDriver(
      Appenderator appenderator,
      SegmentAllocator segmentAllocator,
      SegmentHandoffNotifierFactory handoffNotifierFactory,
      int maxRowsPerSegment
  )
  {
    this.appenderator = Preconditions.checkNotNull(appenderator, "appenderator");
    this.segmentAllocator = Preconditions.checkNotNull(segmentAllocator, "segmentAllocator");
    this.handoffNotifier = Preconditions.checkNotNull(handoffNotifierFactory, "handoffNotifierFactory")
                                        .createSegmentHandoffNotifier(appenderator.getDataSource());
    this.maxRowsPerSegment = maxRowsPerSegment;
  }

  /**
   * Clears out all our state and also calls {@link Appenderator#clear()} on the underlying Appenderator.
   */
  public void clear() throws InterruptedException
  {
    synchronized (activeSegments) {
      activeSegments.clear();
    }
    appenderator.clear();
  }

  /**
   * Add a row. Must not be called concurrently from multiple threads.
   *
   * @param row               the row to add
   * @param committerSupplier supplier of a committer associated with all data that has been added, including this row
   *
   * @return segment to which this row was added, or null if segment allocator returned null for this row
   *
   * @throws IndexSizeExceededException if this row cannot be added because it is too large
   * @throws IOException                if there is an I/O error while allocating a new segment
   */
  public SegmentIdentifier add(
      final InputRow row,
      final Supplier<Committer> committerSupplier
  ) throws IndexSizeExceededException, IOException
  {
    final SegmentIdentifier identifier = getSegment(row.getTimestamp());

    if (identifier != null) {
      try {
        final int numRows = appenderator.add(identifier, row, committerSupplier);
        if (numRows >= maxRowsPerSegment) {
          moveSegmentOut(ImmutableList.of(identifier));
        }
      }
      catch (SegmentNotWritableException e) {
        throw new ISE(e, "WTF?! Segment[%s] not writable when it should have been.", identifier);
      }
    }

    return identifier;
  }

  public int getActiveSegmentCount()
  {
    synchronized (activeSegments) {
      return activeSegments.size();
    }
  }

  public List<SegmentIdentifier> getActiveSegments()
  {
    synchronized (activeSegments) {
      return ImmutableList.copyOf(activeSegments.values());
    }
  }

  /**
   * Publish all data indexed through this driver so far, and waits for it to be handed off. Blocks until complete.
   * Retries forever on transient failures, but may exit early on permanent failures.
   *
   * @param publisher publisher to use for this set of segments
   * @param committer committer representing all data that has been added so far
   *
   * @return segments and metadata published if successful, or null if segments could not be handed off due to
   * transaction failure with commit metadata.
   */
  public SegmentsAndMetadata finish(
      final TransactionalSegmentPublisher publisher,
      final Committer committer
  ) throws InterruptedException
  {
    final SegmentsAndMetadata segmentsAndMetadata = publishAll(publisher, committer);

    if (segmentsAndMetadata != null) {
      log.info("Awaiting handoff...");
      synchronized (handoffMonitor) {
        while (!appenderator.getSegments().isEmpty()) {
          handoffMonitor.wait();
        }
      }

      return segmentsAndMetadata;
    } else {
      return null;
    }
  }

  /**
   * Closes this driver. Does not close the underlying Appenderator; you should do that yourself.
   */
  @Override
  public void close()
  {
    handoffNotifier.close();
  }

  private SegmentIdentifier getActiveSegment(final DateTime timestamp)
  {
    synchronized (activeSegments) {
      final Map.Entry<Long, SegmentIdentifier> candidateEntry = activeSegments.floorEntry(timestamp.getMillis());
      if (candidateEntry != null && candidateEntry.getValue().getInterval().contains(timestamp)) {
        return candidateEntry.getValue();
      } else {
        return null;
      }
    }
  }

  private SegmentIdentifier getSegment(final DateTime timestamp) throws IOException
  {
    synchronized (activeSegments) {
      final SegmentIdentifier existing = getActiveSegment(timestamp);
      if (existing != null) {
        return existing;
      } else {
        // Allocate new segment.
        final SegmentIdentifier newSegment = segmentAllocator.allocate(timestamp);

        if (newSegment != null) {
          final Long key = newSegment.getInterval().getStartMillis();
          final SegmentIdentifier conflicting = activeSegments.get(key);
          if (conflicting != null) {
            throw new ISE(
                "WTF?! Allocated segment[%s] which conflicts with existing segment[%s].",
                newSegment,
                conflicting
            );
          }

          log.info("New segment[%s].", newSegment);
          activeSegments.put(key, newSegment);
        } else {
          // Well, we tried.
          // TODO: Blacklist the interval for a while, so we don't waste time continuously trying?
          log.warn("Cannot allocate segment for timestamp[%s].", timestamp);
        }

        return newSegment;
      }
    }
  }

  /**
   * Move a set of identifiers out from "active", making way for newer segments.
   */
  private void moveSegmentOut(final List<SegmentIdentifier> identifiers)
  {
    synchronized (activeSegments) {
      for (final SegmentIdentifier identifier : identifiers) {
        log.info("Moving segment[%s] out of active list.", identifier);
        final long key = identifier.getInterval().getStartMillis();
        if (activeSegments.remove(key) != identifier) {
          throw new ISE("WTF?! Asked to remove segment[%s] that didn't exist...", identifier);
        }
      }
    }
  }

  private SegmentsAndMetadata publishAll(
      final TransactionalSegmentPublisher publisher,
      final Committer committer
  ) throws InterruptedException
  {
    final List<SegmentIdentifier> theSegments;

    synchronized (activeSegments) {
      activeSegments.clear();
      theSegments = ImmutableList.copyOf(appenderator.getSegments());
    }

    long nTry = 0;
    boolean trying = true;
    while (trying) {
      try {
        log.info("Pushing segments: [%s]", Joiner.on(", ").join(theSegments));
        final SegmentsAndMetadata segmentsAndMetadata = appenderator.push(theSegments, committer).get();

        if (segmentsAndMetadata.getSegments().size() != theSegments.size()) {
          throw new ISE(
              "WTF?! Pushed fewer segments than requested. Missing segments: [%s]",
              Joiner.on(", ").join(
                  Sets.difference(
                      ImmutableSet.copyOf(theSegments),
                      ImmutableSet.copyOf(segmentsAndMetadata.getSegments())
                  )
              )
          );
        }

        log.info(
            "Publishing segments with commitMetadata[%s]: [%s]",
            segmentsAndMetadata.getCommitMetadata(),
            Joiner.on(", ").join(segmentsAndMetadata.getSegments())
        );

        final boolean published = publisher.publishSegments(
            ImmutableSet.copyOf(segmentsAndMetadata.getSegments()),
            segmentsAndMetadata.getCommitMetadata()
        );

        if (!published) {
          // TODO This happens on the replica that lost the publish race, but we still want it to wait for handoff...
          log.info("Transaction failure while publishing segments, giving up.");
          trying = false;
        } else {
          log.info(
              "Published segments, awaiting handoff: [%s]",
              segmentsAndMetadata.getCommitMetadata(),
              Joiner.on(", ").join(segmentsAndMetadata.getSegments())
          );

          for (final DataSegment dataSegment : segmentsAndMetadata.getSegments()) {
            handoffNotifier.registerSegmentHandoffCallback(
                new SegmentDescriptor(
                    dataSegment.getInterval(),
                    dataSegment.getVersion(),
                    dataSegment.getShardSpec().getPartitionNum()
                ),
                MoreExecutors.sameThreadExecutor(),
                new Runnable()
                {
                  @Override
                  public void run()
                  {
                    final SegmentIdentifier identifier = SegmentIdentifier.fromDataSegment(dataSegment);
                    log.info("Segment[%s] successfully handed off, dropping.", identifier);
                    final ListenableFuture<?> dropFuture = appenderator.drop(identifier);
                    Futures.addCallback(
                        dropFuture,
                        new FutureCallback<Object>()
                        {
                          @Override
                          public void onSuccess(Object result)
                          {
                            synchronized (handoffMonitor) {
                              handoffMonitor.notifyAll();
                            }
                          }

                          @Override
                          public void onFailure(Throwable e)
                          {
                            log.warn(e, "Failed to drop segment[%s]?!");
                            synchronized (handoffMonitor) {
                              handoffMonitor.notifyAll();
                            }
                          }
                        }
                    );
                  }
                }
            );
          }

          return segmentsAndMetadata;
        }
      }
      catch (Exception e) {
        final long sleepMillis = computeNextRetrySleep(++nTry);
        log.warn(e, "Failed publishAll (try %d), retrying in %,dms.", nTry, sleepMillis);
        Thread.sleep(sleepMillis);
      }
    }

    return null;
  }

  private static long computeNextRetrySleep(final long nTry)
  {
    final long baseSleepMillis = 1000;
    final long maxSleepMillis = 60000;
    final double fuzzyMultiplier = Math.min(Math.max(1 + 0.2 * new Random().nextGaussian(), 0), 2);
    return (long) (Math.min(maxSleepMillis, baseSleepMillis * Math.pow(2, nTry)) * fuzzyMultiplier);
  }
}
