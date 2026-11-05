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
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.common.asyncresource.SettableAsyncResource;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.AcquiredView;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * Tests for {@link ReadableInputQueue}'s segment-load lifecycle: close-while-loading must promptly fail the futures
 * handed to frame processors (the queue claims each pending load before closing its handle, so the close-fired ready
 * callback finds nothing to deliver and the queue's explicit failure is what completes the future), delivered
 * segments transfer exactly once, and everything un-transferred is closed by the queue.
 */
class ReadableInputQueueTest
{
  private static final SegmentDescriptor DESCRIPTOR = SegmentId.dummy("test").toDescriptor();

  private static class CountingSegment implements Segment
  {
    final AtomicInteger closes = new AtomicInteger();
    private final String name;

    CountingSegment()
    {
      this("test");
    }

    CountingSegment(String name)
    {
      this.name = name;
    }

    @Override
    public String getDebugString()
    {
      return name;
    }

    @Override
    public SegmentId getId()
    {
      return SegmentId.dummy("test");
    }

    @Override
    public Interval getDataInterval()
    {
      return Intervals.ETERNITY;
    }

    @Nullable
    @Override
    public <T> T as(@Nonnull Class<T> clazz)
    {
      return null;
    }

    @Override
    public void close()
    {
      closes.incrementAndGet();
    }
  }

  private static class TestLoadableSegment implements LoadableSegment
  {
    private final AcquireSegmentAction action;
    private final Optional<Segment> cached;
    private final boolean throwOnDescriptor;
    final AtomicInteger countDeliveredCalls = new AtomicInteger();

    TestLoadableSegment(AcquireSegmentAction action)
    {
      this(action, Optional.empty(), false);
    }

    TestLoadableSegment(AcquireSegmentAction action, Optional<Segment> cached)
    {
      this(action, cached, false);
    }

    TestLoadableSegment(AcquireSegmentAction action, Optional<Segment> cached, boolean throwOnDescriptor)
    {
      this.action = action;
      this.cached = cached;
      this.throwOnDescriptor = throwOnDescriptor;
    }

    @Override
    public SegmentDescriptor descriptor()
    {
      // Simulates a post-release failure in the queue's delivery path: onSegmentReady calls descriptor() after
      // release() has already transferred ownership of the result.
      if (throwOnDescriptor) {
        throw new IllegalStateException("descriptor blew up");
      }
      return DESCRIPTOR;
    }

    @Nullable
    @Override
    public String description()
    {
      return "test-loadable";
    }

    @Override
    public Optional<Segment> acquireIfCached(AcquireMode acquireMode)
    {
      return cached;
    }

    @Override
    public AcquireSegmentAction acquire(AcquireMode acquireMode)
    {
      // install delivery accounting the same way real implementations do: a successful release counts exactly once
      action.setOnRelease(result -> countDeliveredCalls.incrementAndGet());
      return action;
    }

    @Override
    public ListenableFuture<DataSegment> dataSegmentFuture()
    {
      return Futures.immediateFailedFuture(DruidException.defensive("not available"));
    }
  }

  /**
   * Acquires views of each segment, without mapping it, that are resident only once tests make them so.
   */
  private static class TestViewAcquirer implements ReadableInputQueue.SegmentViewAcquirer
  {
    private final int viewsPerSegment;
    final List<List<SettableAsyncResource<Segment>>> viewsBySegment = new ArrayList<>();
    final AtomicInteger canceledViews = new AtomicInteger();

    TestViewAcquirer(int viewsPerSegment)
    {
      this.viewsPerSegment = viewsPerSegment;
    }

    @Override
    public synchronized ReadableInputQueue.AcquiredSegmentViews acquireViews(
        int inputNumber,
        SegmentReference reference
    )
    {
      final List<SettableAsyncResource<Segment>> resources = new ArrayList<>();
      final List<AcquiredView> views = new ArrayList<>();
      for (int i = 0; i < viewsPerSegment; i++) {
        final SettableAsyncResource<Segment> resource = new SettableAsyncResource<>();
        resource.setCanceler(canceledViews::incrementAndGet);
        resources.add(resource);
        views.add(new AcquiredView(resource, 0));
      }
      viewsBySegment.add(resources);
      return new ReadableInputQueue.AcquiredSegmentViews(reference, views);
    }

    synchronized int acquireCount()
    {
      return viewsBySegment.size();
    }

    synchronized SettableAsyncResource<Segment> view(int segmentNumber, int viewNumber)
    {
      return viewsBySegment.get(segmentNumber).get(viewNumber);
    }
  }

  private static TestLoadableSegment loaded(CountingSegment segment)
  {
    return new TestLoadableSegment(AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(segment))));
  }

  private static ReadableInputQueue makeQueue(int loadahead, LoadableSegment... segments)
  {
    return makeQueue(loadahead, () -> false, segments);
  }

  private static ReadableInputQueue makeQueue(
      int loadahead,
      BooleanSupplier canSkipRemaining,
      LoadableSegment... segments
  )
  {
    return new ReadableInputQueue(
        null,
        IntList.of(0),
        List.of(new PhysicalInputSlice(ReadablePartitions.empty(), List.of(segments), List.of())),
        loadahead,
        AcquireMode.FULL,
        canSkipRemaining
    );
  }

  @Test
  void testCloseWhileLoadingFailsPendingFutureWithoutHanging() throws Exception
  {
    final AtomicInteger cancels = new AtomicInteger();
    final AcquireSegmentAction pending = new AcquireSegmentAction(cancels::incrementAndGet);
    final TestLoadableSegment loadable = new TestLoadableSegment(pending);

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertFalse(future.isDone());

    queue.close();

    // the future must fail promptly; a dropped ready-callback with no explicit failure would hang forever
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertTrue(e.getCause().getMessage().contains("Input queue closed"), e.getCause().getMessage());
    Assertions.assertEquals(1, cancels.get(), "canceler must run exactly once");
    Assertions.assertEquals(0, loadable.countDeliveredCalls.get(), "cancelled loads must not be counted");
  }

  @Test
  void testCloseWithReentrantDeliveringCancelerFailsAllFutures() throws Exception
  {
    // Reproduces the deferred-acquire close race: closing a NEW handle runs a canceler that synchronously completes
    // the SAME handle (mimicking dsFuture.cancel(true) -> onFailure -> outer.setException). With multiple such handles
    // in flight, close() must not throw ConcurrentModificationException from a reentrant onSegmentReady, and must fail
    // every pending future rather than leaving frame processors hung.
    final int count = 4;
    final List<ListenableFuture<ReadableInputQueue.NumberedReadableInput>> futures = new ArrayList<>();
    final TestLoadableSegment[] loadables = new TestLoadableSegment[count];
    for (int i = 0; i < count; i++) {
      final AcquireSegmentAction[] holder = new AcquireSegmentAction[1];
      // canceler completes the handle in place, as the real deferred canceler does via the coordinator future
      final AcquireSegmentAction action = new AcquireSegmentAction(
          () -> holder[0].setException(DruidException.defensive("canceled"))
      );
      holder[0] = action;
      loadables[i] = new TestLoadableSegment(action);
    }

    final ReadableInputQueue queue = makeQueue(0, loadables);
    queue.start();
    for (int i = 0; i < count; i++) {
      final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
      Assertions.assertNotNull(future);
      Assertions.assertFalse(future.isDone());
      futures.add(future);
    }

    // Must not throw ConcurrentModificationException.
    queue.close();

    for (final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future : futures) {
      final ExecutionException e = Assertions.assertThrows(
          ExecutionException.class,
          () -> future.get(10, TimeUnit.SECONDS)
      );
      Assertions.assertTrue(e.getCause().getMessage().contains("Input queue closed"), e.getCause().getMessage());
    }
    for (final TestLoadableSegment loadable : loadables) {
      Assertions.assertEquals(0, loadable.countDeliveredCalls.get(), "canceled loads must not be counted");
    }
  }

  @Test
  void testPostReleaseFailureClosesResultInsteadOfLeaking() throws Exception
  {
    // release() succeeds, then a later delivery step (descriptor()) throws: the released result must be closed, not
    // leaked (closing the RELEASED action is a no-op, so the segment + its folded holds would otherwise leak forever).
    final CountingSegment segment = new CountingSegment();
    final AcquireSegmentAction completed =
        AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(segment)));
    final TestLoadableSegment loadable = new TestLoadableSegment(completed, Optional.empty(), true);

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);

    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertTrue(e.getCause().getMessage().contains("descriptor blew up"), e.getCause().getMessage());
    Assertions.assertEquals(1, segment.closes.get(), "post-release failure must close the delivered segment");
    // release succeeded, so the delivery was counted even though the downstream handoff then failed
    Assertions.assertEquals(1, loadable.countDeliveredCalls.get());

    // nothing left to close on queue teardown
    queue.close();
    Assertions.assertEquals(1, segment.closes.get(), "segment must not be double-closed");
  }

  @Test
  void testProducerOwnsResultDeliveredAfterQueueClose()
  {
    final AcquireSegmentAction pending = new AcquireSegmentAction();
    final TestLoadableSegment loadable = new TestLoadableSegment(pending);

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    queue.close();

    // the producer's delivery loses the race with close: set() returns false and the producer closes the orphan
    final CountingSegment segment = new CountingSegment();
    final AcquireSegmentResult result = AcquireSegmentResult.of(Optional.of(segment));
    Assertions.assertFalse(pending.set(result));
    result.close();
    Assertions.assertEquals(1, segment.closes.get());
    Assertions.assertEquals(0, loadable.countDeliveredCalls.get());
  }

  @Test
  void testReadyButUncollectedSegmentClosedOnQueueClose()
  {
    final CountingSegment segment = new CountingSegment();
    final AcquireSegmentAction completed =
        AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(segment)));
    final TestLoadableSegment loadable = new TestLoadableSegment(completed);

    // loadahead=1: start() drives the load and the immediate-fire ready callback delivers into loadedSegments
    final ReadableInputQueue queue = makeQueue(1, loadable);
    queue.start();
    Assertions.assertEquals(1, loadable.countDeliveredCalls.get(), "delivery must be counted exactly once");

    // never call nextInput(): the loaded-but-untransferred segment must be closed by the queue
    queue.close();
    Assertions.assertEquals(1, segment.closes.get());
  }

  @Test
  void testTransferredReferenceIsCallersResponsibility() throws Exception
  {
    final CountingSegment segment = new CountingSegment();
    final AcquireSegmentAction completed =
        AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(segment)));
    final TestLoadableSegment loadable = new TestLoadableSegment(completed);

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    final ReadableInput input = future.get(10, TimeUnit.SECONDS).input();
    Assertions.assertTrue(input.hasSegment());

    final SegmentReference ref = input.getSegment().getSegmentReferenceOnce();
    Assertions.assertNotNull(ref);
    Assertions.assertEquals(1, loadable.countDeliveredCalls.get());

    // queue close must not close a transferred reference
    queue.close();
    Assertions.assertEquals(0, segment.closes.get());

    ref.close();
    Assertions.assertEquals(1, segment.closes.get());
  }

  @Test
  void testLoadFailurePropagatesToFuture()
  {
    final AcquireSegmentAction failed = new AcquireSegmentAction();
    failed.setException(new IllegalStateException("load went sideways"));
    final TestLoadableSegment loadable = new TestLoadableSegment(failed);

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertTrue(future.isDone());
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertTrue(e.getCause().getMessage().contains("load went sideways"));
    Assertions.assertEquals(0, loadable.countDeliveredCalls.get(), "failed loads must not be counted");
    queue.close();
  }

  @Test
  void testMissingSegmentDelivery() throws Exception
  {
    final TestLoadableSegment loadable = new TestLoadableSegment(AcquireSegmentAction.missingSegment());

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    final ReadableInput input = future.get(10, TimeUnit.SECONDS).input();
    Assertions.assertTrue(input.hasSegment());
    final SegmentReference ref = input.getSegment().getSegmentReferenceOnce();
    Assertions.assertNotNull(ref);
    Assertions.assertTrue(ref.getSegmentReference().isEmpty());
    Assertions.assertEquals(1, loadable.countDeliveredCalls.get());
    ref.close();
    queue.close();
  }

  @Test
  void testCachedAtStartPathBypassesAcquire()
      throws Exception
  {
    final CountingSegment cachedSegment = new CountingSegment();
    // the action would throw if released; the cached path must never touch it
    final TestLoadableSegment loadable = new TestLoadableSegment(
        new AcquireSegmentAction(),
        Optional.of(cachedSegment)
    );

    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertTrue(future.isDone());
    final ReadableInput input = future.get(10, TimeUnit.SECONDS).input();
    final SegmentReference ref = input.getSegment().getSegmentReferenceOnce();
    Assertions.assertNotNull(ref);
    Assertions.assertSame(cachedSegment, ref.getSegmentReference().orElseThrow());
    Assertions.assertEquals(0, loadable.countDeliveredCalls.get(), "acquireIfCached counts inline, not via the hook");
    ref.close();
    queue.close();
  }

  @Test
  void testCloseClearsPendingLoadaheadInputs() throws Exception
  {
    final CountingSegment segment = new CountingSegment();
    final AcquireSegmentAction completed =
        AcquireSegmentAction.completed(AcquireSegmentResult.of(Optional.of(segment)));
    final TestLoadableSegment loadable = new TestLoadableSegment(completed);

    // loadahead=1: start() loads and parks a done future in pendingNextInputs that is never handed out
    final ReadableInputQueue queue = makeQueue(1, loadable);
    queue.start();
    Assertions.assertEquals(1, queue.remaining());

    queue.close();
    Assertions.assertEquals(1, segment.closes.get(), "unclaimed loadahead holder must be drained on close");
    Assertions.assertEquals(0, queue.remaining(), "close must clear pending loadahead inputs");
    Assertions.assertNull(queue.nextInput(), "no inputs may be handed out after close");
  }

  @Test
  void testCanSkipRemainingStopsHandingOutInputs()
  {
    final CountingSegment cachedSegment = new CountingSegment();
    final TestLoadableSegment loadable = new TestLoadableSegment(new AcquireSegmentAction(), Optional.of(cachedSegment));
    final AtomicBoolean canSkipRemaining = new AtomicBoolean(false);

    final ReadableInputQueue queue = makeQueue(0, canSkipRemaining::get, loadable);
    queue.start();
    Assertions.assertEquals(1, queue.remaining());

    canSkipRemaining.set(true);
    Assertions.assertNull(queue.nextInput(), "no inputs may be handed out once remaining inputs can be skipped");

    queue.close();
    Assertions.assertEquals(1, cachedSegment.closes.get(), "skipped cached segment must be closed on close");
  }

  @Test
  void testCanSkipRemainingStopsLoadahead()
  {
    final AtomicInteger cancels = new AtomicInteger();
    final TestLoadableSegment loadable = new TestLoadableSegment(new AcquireSegmentAction(cancels::incrementAndGet));

    final ReadableInputQueue queue = makeQueue(1, () -> true, loadable);
    queue.start();
    Assertions.assertNull(queue.nextInput());

    queue.close();
    Assertions.assertEquals(0, cancels.get(), "loadahead must not start a load once remaining inputs can be skipped");
  }

  @Test
  void testRemainingCountsPendingInputs()
  {
    final TestLoadableSegment loadable = new TestLoadableSegment(AcquireSegmentAction.missingSegment());
    final ReadableInputQueue queue = makeQueue(0, loadable);
    queue.start();
    Assertions.assertEquals(1, queue.remaining());
    Assertions.assertNotNull(queue.nextInput());
    Assertions.assertEquals(0, queue.remaining());
    Assertions.assertNull(queue.nextInput());
    queue.close();
  }

  @Test
  void testViewsOfLoadaheadSegmentsAreAcquiredBeforeTheyAreNeeded() throws Exception
  {
    final CountingSegment[] segments = {new CountingSegment(), new CountingSegment(), new CountingSegment()};
    final TestViewAcquirer acquirer = new TestViewAcquirer(1);
    final ReadableInputQueue queue = makeQueue(2, loaded(segments[0]), loaded(segments[1]), loaded(segments[2]));

    // Views of the loadahead segments are acquired, which starts their downloads, before any input is requested.
    queue.start(acquirer);
    Assertions.assertEquals(2, acquirer.acquireCount());

    // The first view to become resident is handed out first.
    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    Assertions.assertFalse(future.isDone());
    acquirer.view(1, 0).set(new CountingSegment("second"), null);
    Assertions.assertTrue(future.isDone());
    final SegmentReference ref = future.get().input().getSegment().getSegmentReferenceOnce();
    Assertions.assertNotNull(ref);
    Assertions.assertEquals("second", ref.getSegmentReference().orElseThrow().getDebugString());

    // Handing out the second segment's only view makes room to acquire views of the third.
    Assertions.assertEquals(3, acquirer.acquireCount());

    // Releasing the view releases the segment it came from.
    ref.close();
    Assertions.assertEquals(1, segments[1].closes.get());

    // Closing the queue cancels the views that are not yet resident, and releases their segments.
    queue.close();
    Assertions.assertEquals(2, acquirer.canceledViews.get());
    Assertions.assertEquals(1, segments[0].closes.get());
    Assertions.assertEquals(1, segments[2].closes.get());
  }

  @Test
  void testSegmentIsReleasedOnceAllOfItsViewsAre() throws Exception
  {
    final CountingSegment segment = new CountingSegment();
    final TestViewAcquirer acquirer = new TestViewAcquirer(3);
    final ReadableInputQueue queue = makeQueue(0, loaded(segment));
    queue.start(acquirer);

    final List<SegmentReference> refs = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
      Assertions.assertNotNull(future);
      acquirer.view(0, i).set(new CountingSegment(), null);
      refs.add(future.get(10, TimeUnit.SECONDS).input().getSegment().getSegmentReferenceOnce());
    }
    Assertions.assertNull(queue.nextInput(), "each view is one input");

    refs.get(0).close();
    refs.get(1).close();
    Assertions.assertEquals(0, segment.closes.get());
    refs.get(2).close();
    Assertions.assertEquals(1, segment.closes.get());
    queue.close();
    Assertions.assertEquals(1, segment.closes.get());
  }

  @Test
  void testViewFailurePropagatesToFutureAndReleasesSegment()
  {
    final CountingSegment segment = new CountingSegment();
    final TestViewAcquirer acquirer = new TestViewAcquirer(1);
    final ReadableInputQueue queue = makeQueue(1, loaded(segment));
    queue.start(acquirer);

    final ListenableFuture<ReadableInputQueue.NumberedReadableInput> future = queue.nextInput();
    Assertions.assertNotNull(future);
    acquirer.view(0, 0).setException(new IllegalStateException("download went sideways"));
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    Assertions.assertTrue(e.getCause().getMessage().contains("download went sideways"), e.getCause().getMessage());
    Assertions.assertEquals(1, segment.closes.get());
    queue.close();
    Assertions.assertEquals(1, segment.closes.get());
  }

  @Test
  void testCloseReleasesResidentViewsThatWereNotTaken()
  {
    final CountingSegment segment = new CountingSegment();
    final TestViewAcquirer acquirer = new TestViewAcquirer(2);
    final ReadableInputQueue queue = makeQueue(1, loaded(segment));
    queue.start(acquirer);
    acquirer.view(0, 0).set(new CountingSegment(), null);

    // One view is resident and was never taken, the other is still downloading.
    queue.close();
    Assertions.assertEquals(1, acquirer.canceledViews.get());
    Assertions.assertEquals(1, segment.closes.get());
  }
}
