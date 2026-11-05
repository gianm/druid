/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.querykit;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.DataServerQueryHandler;
import org.apache.druid.msq.exec.std.StandardPartitionReader;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.AcquiredView;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.SegmentViewFetcher;
import org.apache.druid.segment.WrappedSegment;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * Queue for returning {@link ReadableInput} from a list of {@link PhysicalInputSlice}.
 * <p>
 * Segments are loaded up to {@link #loadahead} ahead of callers. When started with a {@link SegmentViewAcquirer}, the
 * queue also maps each loaded segment and acquires its views (see {@link SegmentViewFetcher}), and returns one input
 * per view once the view is resident. Acquiring views starts downloading the columns they need, if segments were
 * acquired with {@link AcquireMode#PARTIAL}, so views of up to {@link #loadahead} segments are downloaded ahead of
 * callers too.
 * <p>
 * When closed, this object cancels all pending segment loads and releases all segments and views that have not yet
 * been acquired by callers through {@link SegmentReferenceHolder#getSegmentReferenceOnce()}. Callers that have
 * acquired segment references are responsible for closing those references, they will not be closed by this class.
 */
public class ReadableInputQueue implements Closeable
{
  private static final Logger log = new Logger(ReadableInputQueue.class);

  /**
   * Partitions to be read.
   */
  @GuardedBy("this")
  private final Queue<NumberedItem<ReadablePartition>> readablePartitions = new ArrayDeque<>();

  /**
   * Segments to be loaded.
   */
  @GuardedBy("this")
  private final Queue<NumberedItem<LoadableSegment>> loadableSegments = new ArrayDeque<>();

  /**
   * Realtime servers to be queried.
   */
  @GuardedBy("this")
  private final Queue<NumberedItem<DataServerQueryHandler>> queryableServers = new ArrayDeque<>();

  /**
   * Segments currently being loaded: the acquire handle mapped to the segment it loads.
   */
  @GuardedBy("this")
  private final LinkedHashMap<AcquireSegmentAction, LoadingSegment> loadingSegments = new LinkedHashMap<>();

  /**
   * Holders of loaded segments and of ready views. These are tracked here so we can close them if needed.
   */
  @GuardedBy("this")
  private final Set<SegmentReferenceHolder> loadedSegments = new LinkedHashSet<>();

  /**
   * Loaded segments whose views have not yet been acquired, in the order they were loaded. Their holders are also in
   * {@link #loadedSegments}.
   */
  @GuardedBy("this")
  private final Queue<LoadedSegment> unexpandedSegments = new ArrayDeque<>();

  /**
   * Views that are not yet resident.
   */
  @GuardedBy("this")
  private final Set<PendingView> pendingViews = new HashSet<>();

  /**
   * Segment inputs that are ready to be handed out by {@link #nextInput()}, as completed futures, which may have
   * failed.
   */
  @GuardedBy("this")
  private final Queue<ReadyInput> readyInputs = new ArrayDeque<>();

  /**
   * Futures returned by {@link #nextInput()} that are waiting for a segment input to become ready.
   */
  @GuardedBy("this")
  private final Queue<SettableFuture<NumberedReadableInput>> waiters = new ArrayDeque<>();

  /**
   * Number of segments that are loading, or loaded and not yet partly handed out. New loads start only while this is
   * below {@link #loadahead}, plus one per waiter, so that waiting callers get a load of their own, as they would if
   * there were no loadahead.
   */
  @GuardedBy("this")
  private int segmentsInWindow;

  /**
   * Number of segments whose views are being acquired, or were acquired and are not yet all handed out. Views are
   * acquired only while this is below {@link #loadahead}, or one if it is zero, which bounds the views that are
   * downloaded or held ahead of callers.
   */
  @GuardedBy("this")
  private int expandedSegments;

  /**
   * Number of segments whose views are being acquired outside the monitor.
   */
  @GuardedBy("this")
  private int expandingSegments;

  /**
   * Set by {@link #close()}.
   */
  @GuardedBy("this")
  private boolean closed = false;

  private final StandardPartitionReader partitionReader;
  private final int loadahead;

  /**
   * How segments in this queue are acquired (fully up front, or partially with on-demand column loading at query
   * time). Threaded through to {@link LoadableSegment#acquire(AcquireMode)} and
   * {@link LoadableSegment#acquireIfCached(AcquireMode)}.
   */
  private final AcquireMode acquireMode;

  /**
   * Whether the caller needs no more inputs. Once true, {@link #nextInput()} returns null and new segment loads start
   * only to answer callers that are already waiting; inputs that were already acquired are released by
   * {@link #close()}.
   */
  private final BooleanSupplier canSkipRemaining;
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Acquires the views of loaded segments, if set by {@link #start(SegmentViewAcquirer)}.
   */
  @Nullable
  private volatile SegmentViewAcquirer viewAcquirer;

  /**
   * Creates a new queue.
   *
   * @param partitionReader Reader for {@link StageInputSlice}
   * @param inputNumbers array of input numbers, one per input
   * @param slices array of slices, one per input (same length as inputNumbers)
   * @param loadahead number of items to load ahead of the queue
   * @param acquireMode acquire mode for {@link SegmentCacheManager}
   * @param canSkipRemaining whether the caller needs no more inputs; see {@link #canSkipRemaining}
   */
  public ReadableInputQueue(
      final StandardPartitionReader partitionReader,
      final IntList inputNumbers,
      final List<PhysicalInputSlice> slices,
      final int loadahead,
      final AcquireMode acquireMode,
      final BooleanSupplier canSkipRemaining
  )
  {
    this.partitionReader = partitionReader;
    this.loadahead = loadahead;
    this.acquireMode = acquireMode;
    this.canSkipRemaining = canSkipRemaining;

    if (inputNumbers.size() != slices.size()) {
      throw DruidException.defensive("Got input numbers[%s] for slices[%s]", inputNumbers, slices);
    }

    for (int i = 0; i < slices.size(); i++) {
      final int inputNumber = inputNumbers.getInt(i);
      final PhysicalInputSlice slice = slices.get(i);

      for (final LoadableSegment segment : slice.getLoadableSegments()) {
        loadableSegments.add(new NumberedItem<>(inputNumber, segment));
      }

      for (final DataServerQueryHandler server : slice.getQueryableServers()) {
        queryableServers.add(new NumberedItem<>(inputNumber, server));
      }

      for (final ReadablePartition partition : slice.getReadablePartitions()) {
        readablePartitions.add(new NumberedItem<>(inputNumber, partition));
      }
    }
  }

  /**
   * Same as {@link #start(SegmentViewAcquirer)}, without acquiring views: each segment is returned as loaded.
   */
  public void start()
  {
    start(null);
  }

  /**
   * If this method has not yet been called, then:
   * (1) transition all locally-cached segments out of {@link #loadableSegments}
   * (2) start loading up to {@link #loadahead} additional segments for future calls to {@link #nextInput()}
   * (3) start acquiring the views of loaded segments using {@code viewAcquirer}, if it is nonnull
   * If this method has previously been called, subsequent calls do nothing.
   * This is separated from the constructor because we don't want to acquire resources immediately on construction.
   */
  public void start(@Nullable final SegmentViewAcquirer viewAcquirer)
  {
    if (started.compareAndSet(false, true)) {
      this.viewAcquirer = viewAcquirer;

      synchronized (this) {
        // (1) acquire all locally-cached segments
        final List<NumberedItem<LoadableSegment>> toLoad = new ArrayList<>();
        NumberedItem<LoadableSegment> nextItem;
        while ((nextItem = loadableSegments.poll()) != null) {
          final LoadableSegment loadableSegment = nextItem.item();
          final Optional<Segment> cachedSegment = loadableSegment.acquireIfCached(acquireMode);
          if (cachedSegment.isPresent()) {
            final SegmentReferenceHolder holder = new SegmentReferenceHolder(
                new SegmentReference(loadableSegment.descriptor(), cachedSegment),
                loadableSegment.description()
            );
            addLoadedSegment(new LoadedSegment(nextItem.inputNumber(), holder, new SegmentProgress()));
            segmentsInWindow++;
          } else {
            toLoad.add(nextItem);
          }
        }
        loadableSegments.addAll(toLoad); // Put non-cached segments back into loadableSegments

        // (2) start loading up to "loadahead" additional segments, regardless of how many were cached
        for (int i = 0; i < loadahead && !loadableSegments.isEmpty() && !canSkipRemaining.getAsBoolean(); i++) {
          loadNextSegment();
        }
      }

      // (3) acquire views, and keep loading as segments are handed out
      pump();
    }
  }

  /**
   * Returns the number of remaining inputs that can be returned by calls to {@link #nextInput()}. Each segment counts
   * once until its views are acquired, so there may be more.
   */
  public int remaining()
  {
    synchronized (this) {
      return readablePartitions.size() + queryableServers.size() + remainingSegmentInputs() - waiters.size();
    }
  }

  /**
   * Returns the next {@link NumberedReadableInput}. The future resolves when the input is ready to read. Returns null
   * if no inputs remain.
   */
  @Nullable
  public ListenableFuture<NumberedReadableInput> nextInput()
  {
    if (!started.get()) {
      throw DruidException.defensive("Not started, must call start() first");
    }

    if (canSkipRemaining.getAsBoolean()) {
      return null;
    }

    ListenableFuture<NumberedReadableInput> future;

    future = nextServerInput();
    if (future != null) {
      return future;
    }

    future = nextChannelInput();
    if (future != null) {
      return future;
    }

    future = nextSegmentInput();
    if (future != null) {
      return future;
    }

    return null;
  }

  /**
   * Returns the next input from {@link #queryableServers}, if any. Returns null if none remain.
   */
  @Nullable
  private ListenableFuture<NumberedReadableInput> nextServerInput()
  {
    final NumberedItem<DataServerQueryHandler> handler;
    synchronized (this) {
      handler = queryableServers.poll();
    }

    if (handler == null) {
      return null;
    }

    return Futures.immediateFuture(
        new NumberedReadableInput(handler.inputNumber(), ReadableInput.dataServerQuery(handler.item()))
    );
  }

  /**
   * Returns the next input from {@link #readablePartitions}, if any. Returns null if none remain.
   */
  @Nullable
  private ListenableFuture<NumberedReadableInput> nextChannelInput()
  {
    final NumberedItem<ReadablePartition> nextItem;
    synchronized (this) {
      nextItem = readablePartitions.poll();
    }

    if (nextItem == null) {
      return null;
    }

    final ReadablePartition readablePartition = nextItem.item();
    ReadableFrameChannel channel = null;
    try {
      channel = partitionReader.openChannel(readablePartition);
      return Futures.immediateFuture(
          new NumberedReadableInput(
              nextItem.inputNumber(),
              ReadableInput.channel(
                  channel,
                  partitionReader.frameReader(readablePartition.getStageNumber()),
                  readablePartition.getStageNumber(),
                  readablePartition.getPartitionNumber()
              )
          )
      );
    }
    catch (IOException e) {
      throw CloseableUtils.closeAndWrapInCatch(e, channel);
    }
  }

  /**
   * Returns a future for the next segment input to become ready, preferring ones that are already ready. Returns
   * null if none remain.
   */
  @Nullable
  private ListenableFuture<NumberedReadableInput> nextSegmentInput()
  {
    final SettableFuture<NumberedReadableInput> waiter = SettableFuture.create();
    synchronized (this) {
      // Each remaining segment, view, or ready input yields at least one input, so this waiter will be answered.
      if (closed || remainingSegmentInputs() <= waiters.size()) {
        return null;
      }
      waiters.add(waiter);
    }
    pump();
    return waiter;
  }

  /**
   * Returns a lower bound on the number of segment inputs that remain, including those that waiters will receive.
   */
  @GuardedBy("this")
  private int remainingSegmentInputs()
  {
    return loadableSegments.size()
           + loadingSegments.size()
           + unexpandedSegments.size()
           + expandingSegments
           + pendingViews.size()
           + readyInputs.size();
  }

  /**
   * Hands ready inputs to waiters, starts loads and acquires views while there is room for them, and repeats until
   * nothing changes. Must not be called while holding the monitor, since views are acquired outside it.
   */
  private void pump()
  {
    boolean changed = true;
    while (changed) {
      changed = false;
      final List<Runnable> handOffs = new ArrayList<>();
      LoadedSegment toExpand = null;

      synchronized (this) {
        if (closed) {
          return;
        }

        while (!waiters.isEmpty() && !readyInputs.isEmpty()) {
          final SettableFuture<NumberedReadableInput> waiter = waiters.poll();
          // Skip waiters that their callers canceled.
          if (!waiter.isDone()) {
            final ReadyInput ready = readyInputs.poll();
            markHandedOut(ready.progress());
            handOffs.add(() -> waiter.setFuture(ready.future()));
          }
          changed = true;
        }

        // Once the caller can skip remaining inputs, load and expand only for waiters, which must still be answered.
        final boolean skipping = canSkipRemaining.getAsBoolean();
        while (!loadableSegments.isEmpty() && segmentsInWindow < (skipping ? 0 : loadahead) + waiters.size()) {
          loadNextSegment();
          changed = true;
        }

        if (!unexpandedSegments.isEmpty()
            && expandedSegments < Math.max(loadahead, 1)
            && (!skipping || !waiters.isEmpty())) {
          toExpand = unexpandedSegments.poll();
          loadedSegments.remove(toExpand.holder());
          toExpand.progress().expanded = true;
          expandedSegments++;
          expandingSegments++;
        }
      }

      // Complete futures outside the monitor, so their listeners never run while holding it.
      handOffs.forEach(Runnable::run);

      if (toExpand != null) {
        expand(toExpand);
        changed = true;
      }
    }
  }

  /**
   * Starts loading the next segment from {@link #loadableSegments}, which must not be empty.
   */
  @GuardedBy("this")
  private void loadNextSegment()
  {
    final NumberedItem<LoadableSegment> nextItem = loadableSegments.poll();
    final LoadingSegment loading = new LoadingSegment(nextItem.inputNumber(), nextItem.item(), new SegmentProgress());
    final AcquireSegmentAction acquireSegmentAction = nextItem.item().acquire(acquireMode);
    loadingSegments.put(acquireSegmentAction, loading);
    segmentsInWindow++;
    // may fire immediately on this thread for an already-ready acquire; the queue monitor is reentrant
    acquireSegmentAction.addReadyCallback(() -> onSegmentReady(loading, acquireSegmentAction));
  }

  /**
   * Transfers ownership of a loaded segment out of its acquire handle and into a {@link SegmentReferenceHolder} in
   * {@link #loadedSegments}, or records the failure to load it as a ready input, so a waiter receives it.
   */
  private void onSegmentReady(final LoadingSegment loading, final AcquireSegmentAction acquireSegmentAction)
  {
    synchronized (this) {
      if (loadingSegments.remove(acquireSegmentAction) == null) {
        // close() already processed this handle, closing it (and any delivered result); nothing left to do.
        return;
      }

      // tracks the released result until ownership transfers to a SegmentReferenceHolder in loadedSegments. If a step
      // after release() throws while this is still non-null, we own the orphaned result and must close it (close on
      // the released action is a no-op, so closing the action alone would leak the segment and its folded holds).
      AcquireSegmentResult releasedResult = null;
      try {
        // Ownership transfer; the delivered segment's close releases everything the acquire placed, and release()
        // runs the producer's delivery-accounting hook.
        releasedResult = acquireSegmentAction.release();
        final SegmentReferenceHolder referenceHolder = new SegmentReferenceHolder(
            new SegmentReference(loading.loadableSegment().descriptor(), releasedResult.getSegment()),
            loading.loadableSegment().description()
        );
        addLoadedSegment(new LoadedSegment(loading.inputNumber(), referenceHolder, loading.progress()));
        // ownership is now with loadedSegments; close() will close the holder if it is never handed out
        releasedResult = null;
      }
      catch (Throwable t) {
        if (releasedResult != null) {
          // release() succeeded but a later step threw before ownership transferred; close the orphaned result so its
          // segment reference and folded cache holds are released rather than leaked.
          CloseableUtils.closeAndSuppressExceptions(
              releasedResult,
              e -> log.warn(
                  e,
                  "Failed to close acquired segment for segment[%s]",
                  loading.loadableSegment().description()
              )
          );
        } else {
          // release() itself surfaced the producer's load failure; close the handle so it reaches a terminal state
          // (a no-op close of the null result for a failed load).
          CloseableUtils.closeAndSuppressExceptions(
              acquireSegmentAction,
              e -> log.warn(
                  e,
                  "Failed to close acquire action for segment[%s]",
                  loading.loadableSegment().description()
              )
          );
        }
        readyInputs.add(new ReadyInput(Futures.immediateFailedFuture(t), loading.progress()));
      }
    }

    // When called from loadNextSegment(), its caller pumps instead.
    if (!Thread.holdsLock(this)) {
      pump();
    }
  }

  @GuardedBy("this")
  private void addLoadedSegment(final LoadedSegment loaded)
  {
    loadedSegments.add(loaded.holder());
    unexpandedSegments.add(loaded);
  }

  /**
   * Maps {@code loaded} and acquires its views using {@link #viewAcquirer}, then records them as pending views, or
   * records the mapped segment as a ready input if there are no views. Called outside the monitor, after
   * {@link #pump()} took {@code loaded} out of {@link #unexpandedSegments} and {@link #loadedSegments}.
   */
  private void expand(final LoadedSegment loaded)
  {
    final SegmentReferenceHolder holder = loaded.holder();
    final SegmentViewAcquirer acquirer = viewAcquirer;
    SegmentReference mapped = null;
    List<AcquiredView> views = List.of();
    Throwable failure = null;
    try {
      final SegmentReference reference = holder.getSegmentReferenceOnce();
      if (reference == null) {
        throw DruidException.defensive("Segment[%s] was already taken", holder.getDescriptor());
      }
      if (acquirer == null) {
        mapped = reference;
      } else {
        final AcquiredSegmentViews acquired = acquirer.acquireViews(loaded.inputNumber(), reference);
        mapped = acquired.mapped();
        views = acquired.views();
      }
    }
    catch (Throwable t) {
      failure = t;
    }

    final SegmentProgress progress = loaded.progress();
    final List<PendingView> newViews = new ArrayList<>(views.size());
    if (!views.isEmpty()) {
      // Every view holds part of the reference to the mapped segment, which is released when all of them are.
      final SegmentReference mappedReference = mapped;
      final AtomicInteger openViews = new AtomicInteger(views.size());
      final Closeable releaseMapped = () -> {
        if (openViews.decrementAndGet() == 0) {
          mappedReference.close();
        }
      };
      for (final AcquiredView view : views) {
        newViews.add(
            new PendingView(
                loaded.inputNumber(),
                mappedReference.getSegmentDescriptor(),
                holder.description(),
                view.view(),
                releaseMapped,
                progress
            )
        );
      }
    }

    final boolean queueClosed;
    synchronized (this) {
      expandingSegments--;
      queueClosed = closed;
      if (!queueClosed) {
        if (failure != null) {
          readyInputs.add(new ReadyInput(Futures.immediateFailedFuture(failure), progress));
        } else if (newViews.isEmpty()) {
          // Process the mapped segment as a whole.
          final SegmentReferenceHolder mappedHolder = new SegmentReferenceHolder(mapped, holder.description());
          loadedSegments.add(mappedHolder);
          readyInputs.add(
              new ReadyInput(
                  Futures.immediateFuture(
                      new NumberedReadableInput(loaded.inputNumber(), ReadableInput.segment(mappedHolder))
                  ),
                  progress
              )
          );
        } else {
          progress.notHandedOut = newViews.size();
          pendingViews.addAll(newViews);
          for (final PendingView view : newViews) {
            // may fire immediately on this thread for a resident view; the queue monitor is reentrant
            view.resource().addReadyCallback(() -> onViewReady(view));
          }
        }
      }
    }

    if (queueClosed) {
      // close() ran while we acquired views, so release what we acquired.
      final Closeable toRelease = newViews.isEmpty() ? mapped : CloseableUtils.forIterable(newViews);
      CloseableUtils.closeAndSuppressExceptions(
          toRelease,
          e -> log.warn(e, "Failed to release segment[%s]", holder.description())
      );
    }
  }

  /**
   * Records {@code view} as a ready input once it is resident, or the failure to make it resident.
   */
  private void onViewReady(final PendingView view)
  {
    synchronized (this) {
      if (!pendingViews.remove(view)) {
        // close() already released the view, which fired this callback.
        return;
      }

      ListenableFuture<NumberedReadableInput> future;
      try {
        final SegmentReferenceHolder viewHolder = new SegmentReferenceHolder(
            new SegmentReference(view.descriptor(), Optional.of(new ViewSegment(view.resource().get(), view))),
            view.description()
        );
        loadedSegments.add(viewHolder);
        future = Futures.immediateFuture(
            new NumberedReadableInput(view.inputNumber(), ReadableInput.segment(viewHolder))
        );
      }
      catch (Throwable t) {
        CloseableUtils.closeAndSuppressExceptions(view, t::addSuppressed);
        future = Futures.immediateFailedFuture(t);
      }
      readyInputs.add(new ReadyInput(future, view.progress()));
    }

    // When called from expand(), its caller pumps instead.
    if (!Thread.holdsLock(this)) {
      pump();
    }
  }

  /**
   * Records that one input of the segment of {@code progress} was handed out, making room for more loads once the
   * segment's first input is handed out, as before views existed, and for acquiring more views once its last input is.
   */
  @GuardedBy("this")
  private void markHandedOut(final SegmentProgress progress)
  {
    if (!progress.anyHandedOut) {
      progress.anyHandedOut = true;
      segmentsInWindow--;
    }
    if (--progress.notHandedOut == 0 && progress.expanded) {
      expandedSegments--;
    }
  }

  @Override
  public void close()
  {
    final List<AcquireSegmentAction> handlesToClose;
    final List<SegmentReferenceHolder> holdersToDrain;
    final List<PendingView> viewsToRelease;
    final List<SettableFuture<NumberedReadableInput>> waitersToFail;

    // Snapshot and clear everything under the monitor, but do the actual closing OUTSIDE it: closing a handle can
    // synchronously run its canceler (a deferred acquire's canceler takes its own stage lock and may complete the
    // handle), and holding the queue monitor across that work would stall every concurrent delivery and nextInput()
    // call behind it. Clearing the loading map first means a late-arriving onSegmentReady finds no entry and returns
    // without touching queue state; clearing pendingViews does the same for onViewReady.
    synchronized (this) {
      closed = true;
      readablePartitions.clear();
      queryableServers.clear();
      loadableSegments.clear();

      handlesToClose = new ArrayList<>(loadingSegments.keySet());
      loadingSegments.clear();

      // Includes the holders of unexpanded segments and ready inputs that were never handed out.
      holdersToDrain = new ArrayList<>(loadedSegments);
      loadedSegments.clear();
      unexpandedSegments.clear();
      readyInputs.clear();

      viewsToRelease = new ArrayList<>(pendingViews);
      pendingViews.clear();

      waitersToFail = new ArrayList<>(waiters);
      waiters.clear();
    }

    // Cancel all pending segment loads: closing a NEW handle runs its canceler (aborting the load); closing a
    // ready but unclaimed handle (its ready callback hasn't reached the queue monitor yet) closes the delivered
    // result.
    for (final AcquireSegmentAction acquireSegmentAction : handlesToClose) {
      CloseableUtils.closeAndSuppressExceptions(
          acquireSegmentAction,
          // AcquireSegmentAction currently doesn't have a meaningful toString method, so if this message
          // ever actually gets logged, it won't mention the specific segment that had a problem. Perhaps
          // one day this will change.
          e -> log.warn(e, "Failed to close loadingSegment[%s]", acquireSegmentAction)
      );
    }

    // Close all segments that have been loaded and not yet transferred to callers. (Segments transferred to
    // callers must be closed by the callers.)
    for (SegmentReferenceHolder referenceHolder : holdersToDrain) {
      final SegmentReference ref = referenceHolder.getSegmentReferenceOnce();
      if (ref != null) {
        CloseableUtils.closeAndSuppressExceptions(
            ref,
            e -> log.warn(e, "Failed to close loadedSegment[%s]", ref.getSegmentDescriptor())
        );
      }
    }

    // Release views that are not yet resident, canceling their downloads.
    for (final PendingView view : viewsToRelease) {
      CloseableUtils.closeAndSuppressExceptions(
          view,
          e -> log.warn(e, "Failed to release view of segment[%s]", view.description())
      );
    }

    // Explicitly fail the futures handed to callers that are still waiting, so the frame processors awaiting them
    // don't hang forever.
    for (final SettableFuture<NumberedReadableInput> waiter : waitersToFail) {
      waiter.setException(DruidException.defensive("Input queue closed while segment load was in flight"));
    }
  }

  /**
   * Maps loaded segments and acquires the views that processors read in their place; see {@link SegmentViewFetcher}.
   */
  @FunctionalInterface
  public interface SegmentViewAcquirer
  {
    /**
     * Maps the segment of {@code reference}, from input {@code inputNumber}, and acquires its views, which starts any
     * downloads they need. Takes ownership of {@code reference}, and closes whatever it acquired if it throws.
     */
    AcquiredSegmentViews acquireViews(int inputNumber, SegmentReference reference);
  }

  /**
   * Result of {@link SegmentViewAcquirer#acquireViews}: the mapped segment, along with the views that processors read
   * in its place, or no views if they read the mapped segment as a whole. The views read from the mapped segment, which
   * the queue releases once all of them are released.
   */
  public record AcquiredSegmentViews(SegmentReference mapped, List<AcquiredView> views)
  {
  }

  /**
   * A {@link ReadableInput} along with the input number it came from.
   */
  public record NumberedReadableInput(int inputNumber, ReadableInput input)
  {
  }

  /**
   * An item that is yet to be turned into a {@link ReadableInput}, along with the input number it came from.
   */
  private record NumberedItem<T>(int inputNumber, T item)
  {
  }

  /**
   * A segment that is being loaded.
   */
  private record LoadingSegment(int inputNumber, LoadableSegment loadableSegment, SegmentProgress progress)
  {
  }

  /**
   * A segment that was loaded, whose views have not yet been acquired.
   */
  private record LoadedSegment(int inputNumber, SegmentReferenceHolder holder, SegmentProgress progress)
  {
  }

  /**
   * A segment input that is ready to be handed out, along with the progress of the segment it is part of.
   */
  private record ReadyInput(ListenableFuture<NumberedReadableInput> future, SegmentProgress progress)
  {
  }

  /**
   * How far the inputs of one segment have been handed out. Synchronized by the queue's monitor.
   */
  private static final class SegmentProgress
  {
    /**
     * Whether {@link #expandedSegments} counts this segment.
     */
    private boolean expanded;

    /**
     * Whether any input of this segment was handed out.
     */
    private boolean anyHandedOut;

    /**
     * Number of inputs of this segment not yet handed out. One until its views are acquired.
     */
    private int notHandedOut = 1;
  }

  /**
   * A view that is not yet resident, or is resident and has not yet been taken by a processor. Closing it releases the
   * view and its part of the reference to the mapped segment, exactly once.
   */
  private static final class PendingView implements Closeable
  {
    private final int inputNumber;
    private final SegmentDescriptor descriptor;
    @Nullable
    private final String description;
    private final AsyncResource<Segment> resource;
    private final Closeable releaseMapped;
    private final SegmentProgress progress;
    private final AtomicBoolean released = new AtomicBoolean(false);

    PendingView(
        int inputNumber,
        SegmentDescriptor descriptor,
        @Nullable String description,
        AsyncResource<Segment> resource,
        Closeable releaseMapped,
        SegmentProgress progress
    )
    {
      this.inputNumber = inputNumber;
      this.descriptor = descriptor;
      this.description = description;
      this.resource = resource;
      this.releaseMapped = releaseMapped;
      this.progress = progress;
    }

    int inputNumber()
    {
      return inputNumber;
    }

    SegmentDescriptor descriptor()
    {
      return descriptor;
    }

    @Nullable
    String description()
    {
      return description;
    }

    AsyncResource<Segment> resource()
    {
      return resource;
    }

    SegmentProgress progress()
    {
      return progress;
    }

    @Override
    public void close() throws IOException
    {
      if (released.compareAndSet(false, true)) {
        CloseableUtils.closeAll(resource, releaseMapped);
      }
    }
  }

  /**
   * Segment that a processor reads for a view. Closing it releases the view rather than closing the view itself; see
   * {@link SegmentViewFetcher}.
   */
  private static class ViewSegment extends WrappedSegment
  {
    private final Closeable release;

    ViewSegment(Segment view, Closeable release)
    {
      super(view);
      this.release = release;
    }

    @Nullable
    @Override
    public <T> T as(@Nonnull Class<T> clazz)
    {
      return delegate.as(clazz);
    }

    @Override
    public String getDebugString()
    {
      return delegate.getDebugString();
    }

    @Override
    public void close() throws IOException
    {
      release.close();
    }
  }
}
