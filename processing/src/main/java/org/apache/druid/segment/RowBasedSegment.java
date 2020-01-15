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

package org.apache.druid.segment;

import org.apache.druid.segment.column.ValueType;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

public class RowBasedSegment<RowType> extends AbstractSegment
{
  private final SegmentId segmentId;
  private final StorageAdapter storageAdapter;

  public RowBasedSegment(
      final SegmentId segmentId,
      final Iterable<RowType> rowIterable,
      final RowAdapter<RowType> rowAdapter,
      final Map<String, ValueType> rowSignature
  )
  {
    this.segmentId = segmentId;
    this.storageAdapter = new RowBasedStorageAdapter<>(
        rowIterable,
        rowAdapter,
        rowSignature
    );
  }

  @Override
  @Nonnull
  public SegmentId getId()
  {
    return segmentId;
  }

  @Override
  @Nonnull
  public Interval getDataInterval()
  {
    return storageAdapter.getInterval();
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    return null;
  }

  @Override
  @Nonnull
  public StorageAdapter asStorageAdapter()
  {
    return storageAdapter;
  }

  @Override
  public void close() throws IOException
  {
    // Do nothing.
  }
}
