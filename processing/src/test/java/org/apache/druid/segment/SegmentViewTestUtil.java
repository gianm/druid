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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Helpers for tests that read segments through {@link SegmentViewFetcher}.
 */
public class SegmentViewTestUtil
{
  private static final Interval THREE_GROUP_INTERVAL = Intervals.of("2025-01-01/2025-01-02");

  private SegmentViewTestUtil()
  {
    // no instantiation
  }

  /**
   * Returns the views of {@code segment} for {@code spec}, or for any spec if it is null, which must be resident. They
   * need not be released, since resident views hold nothing.
   */
  public static List<Segment> residentViews(Segment segment, @Nullable CursorBuildSpec spec)
  {
    final List<Segment> views = new ArrayList<>();
    for (AcquiredView view : segment.as(SegmentViewFetcher.class).acquireViews(spec)) {
      if (!view.view().isReady()) {
        throw DruidException.defensive("View of segment[%s] is not resident", segment.getId());
      }
      views.add(view.view().get());
    }
    return views;
  }

  /**
   * Returns the cursor factories of the views of {@code index} for {@link CursorBuildSpec#FULL_SCAN}, which are one
   * per cluster group if {@code index} is clustered.
   */
  public static List<CursorFactory> viewCursorFactories(QueryableIndex index)
  {
    final Segment segment = new QueryableIndexSegment(index, SegmentId.dummy("test"));
    final List<CursorFactory> factories = new ArrayList<>();
    for (Segment view : residentViews(segment, CursorBuildSpec.FULL_SCAN)) {
      factories.add(view.as(CursorFactory.class));
    }
    return factories;
  }

  /**
   * Builds a clustered index, clustered on {@code tenant}, with three cluster groups: {@code acme} (two rows),
   * {@code globex}, and {@code initech}, in that order. Callers must close it.
   */
  public static QueryableIndex buildThreeGroupIndex(File tmpDir)
  {
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(tmpDir)
                       .schema(clusteredSchema())
                       .rows(List.of(
                           row("globex", "2025-01-01T00:30:00", "eu-west-1"),
                           row("acme", "2025-01-01T00:00:00", "us-east-1"),
                           row("initech", "2025-01-01T02:00:00", "ap-south-1"),
                           row("acme", "2025-01-01T01:00:00", "us-west-2")
                       ))
                       .buildMMappedIndex(THREE_GROUP_INTERVAL);
  }

  /**
   * Builds an empty realtime index with the schema of {@link #buildThreeGroupIndex}. Callers must close it.
   */
  public static IncrementalIndex buildEmptyClusteredIncrementalIndex()
  {
    return new OnheapIncrementalIndex.Builder()
        .setIndexSchema(clusteredSchema())
        .setMaxRowCount(1000)
        .build();
  }

  /**
   * Returns a row for an index from {@link #buildThreeGroupIndex} or {@link #buildEmptyClusteredIncrementalIndex}.
   */
  public static InputRow row(String tenant, String ts, String region)
  {
    return new MapBasedInputRow(
        DateTimes.of(ts),
        List.of("tenant", "region"),
        Map.of("tenant", tenant, "region", region)
    );
  }

  private static IncrementalIndexSchema clusteredSchema()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec clusterSpec =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .columns(
                new StringDimensionSchema("tenant"),
                new StringDimensionSchema("region"),
                new LongDimensionSchema("__time")
            )
            .clusteringColumns("tenant")
            .build();
    return IncrementalIndexSchema.builder()
                                 .withMinTimestamp(THREE_GROUP_INTERVAL.getStartMillis())
                                 .withTimestampSpec(new TimestampSpec("__time", "auto", null))
                                 .withQueryGranularity(Granularities.NONE)
                                 .withDimensionsSpec(clusterSpec.getDimensionsSpec())
                                 .withRollup(false)
                                 .withClusterSpec(clusterSpec)
                                 .build();
  }
}
