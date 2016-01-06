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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.common.IAE;
import com.metamx.common.ISE;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RequestBuffer implements Closeable
{
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition await = lock.newCondition();
  private final ArrayDeque<RttdRequest> deque = new ArrayDeque<>();
  private final int maxBufferSize;

  private volatile int bufferedMessages = 0;
  private volatile boolean open = true;

  public RequestBuffer(int maxBufferSize)
  {
    this.maxBufferSize = maxBufferSize;
  }

  public <T> T perform(RttdRequest<T> request) throws InterruptedException
  {
    put(request);

    // TODO: Bail out if buffer is closed while waiting for a resposne
    try {
      return request.getResponse().get();
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, InterruptedException.class);
      throw Throwables.propagate(e);
    }
  }

  public void put(RttdRequest request) throws InterruptedException
  {
    Preconditions.checkNotNull(request, "request");

    final int requestMessages = request.getRowCount();
    if (requestMessages > maxBufferSize) {
      throw new IAE("Cannot add [%,d] messages with maxBufferSize[%,d].", requestMessages, maxBufferSize);
    }

    lock.lockInterruptibly();
    try {
      while (open && requestMessages + bufferedMessages > maxBufferSize) {
        await.await();
      }

      if (!open) {
        throw new ISE("Cannot add to closed buffer");
      }

      deque.add(request);
      bufferedMessages += requestMessages;
    }
    finally {
      lock.unlock();
    }
  }

  public RttdRequest poll()
  {
    lock.lock();
    try {
      return deque.poll();
    }
    finally {
      lock.unlock();
    }
  }

  public RttdRequest poll(long timeout, TimeUnit unit) throws InterruptedException
  {
    long timeoutNanos = unit.toNanos(timeout);

    lock.lockInterruptibly();
    try {
      while (deque.isEmpty()) {
        if (timeoutNanos <= 0) {
          return null;
        } else {
          timeoutNanos = await.awaitNanos(timeoutNanos);
        }
      }

      final RttdRequest retVal = Preconditions.checkNotNull(deque.poll(), "WTF?! Null poll?");
      bufferedMessages -= retVal.getRowCount();
      return retVal;
    }
    finally {
      lock.unlock();
    }
  }

  public int size()
  {
    return bufferedMessages;
  }

  @Override
  public void close() throws IOException
  {
    lock.lock();
    try {
      open = false;
      await.signalAll();
    }
    finally {
      lock.unlock();
    }
  }
}
