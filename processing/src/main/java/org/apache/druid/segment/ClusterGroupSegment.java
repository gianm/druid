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

import com.google.common.base.Suppliers;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * View of one cluster group of a clustered segment, from {@link SegmentViewFetcher}. Cluster groups have their own
 * dictionaries, so a cursor never reads more than one group. Instead, callers acquire one view per group, and process
 * each group as if it were a separate segment.
 * <p>
 * Specs that match an aggregate projection are handled according to {@link ClusterGroupView.ProjectionRole}.
 * <p>
 * This view shares the segment's id, data interval, metadata, and time boundaries, and closing it does nothing. Callers
 * must hold a reference to the clustered segment for as long as they use this view.
 */
public class ClusterGroupSegment implements ReferenceCountedSegmentProvider.LeafReference
{
  private final Segment segment;
  private final ClusterGroupView clusterGroup;
  private final CursorFactory cursorFactory;
  private final Supplier<PhysicalSegmentInspector> physicalSegmentInspectorSupplier;
  private final Class<?> rowSelectorClass;
  private final Supplier<?> rowSelectorSupplier;

  /**
   * Creates a view of {@code clusterGroup} of the clustered {@code segment}, reading rows through {@code cursorFactory}.
   *
   * @param physicalSegmentInspectorSupplier supplies an inspector for the group's own rows and columns, or null if
   *                                         the group's columns are not available without downloading them
   * @param rowSelectorClass                 class that {@link #as} exposes the group's rows as
   * @param rowSelectorSupplier              supplies the group's rows, for callers that read columns directly. Called
   *                                         at most once, and only if a caller asks for them.
   */
  public <T> ClusterGroupSegment(
      Segment segment,
      ClusterGroupView clusterGroup,
      CursorFactory cursorFactory,
      Supplier<PhysicalSegmentInspector> physicalSegmentInspectorSupplier,
      Class<T> rowSelectorClass,
      Supplier<T> rowSelectorSupplier
  )
  {
    this.segment = segment;
    this.clusterGroup = clusterGroup;
    this.cursorFactory = cursorFactory;
    this.physicalSegmentInspectorSupplier = physicalSegmentInspectorSupplier;
    this.rowSelectorClass = rowSelectorClass;
    this.rowSelectorSupplier = Suppliers.memoize(rowSelectorSupplier::get);
  }

  @Nullable
  @Override
  public SegmentId getId()
  {
    return segment.getId();
  }

  @Override
  public Interval getDataInterval()
  {
    return segment.getDataInterval();
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    if (CursorFactory.class.equals(clazz)) {
      return (T) cursorFactory;
    } else if (ClusterGroupView.class.equals(clazz)) {
      return (T) clusterGroup;
    } else if (rowSelectorClass.equals(clazz)) {
      return (T) rowSelectorSupplier.get();
    } else if (TimeBoundaryInspector.class.equals(clazz)) {
      return (T) WrappedTimeBoundaryInspector.create(segment.as(TimeBoundaryInspector.class));
    } else if (Metadata.class.equals(clazz) || MaxIngestedEventTimeInspector.class.equals(clazz)) {
      return segment.as(clazz);
    } else if (RowCountInspector.class.equals(clazz)
               || PhysicalSegmentColumnInspector.class.equals(clazz)
               || PhysicalSegmentInspector.class.equals(clazz)) {
      return (T) physicalSegmentInspectorSupplier.get();
    } else if (TopNOptimizationInspector.class.equals(clazz)) {
      // Each group's dictionaries are built from the group's own rows.
      return (T) new SimpleTopNOptimizationInspector(true);
    }
    return null;
  }

  @Override
  public void validateOrElseThrow(PolicyEnforcer policyEnforcer)
  {
    // a segment cannot directly have any policies, so use the enforcer directly
    policyEnforcer.validateOrElseThrow(this, null);
  }

  @Override
  public String getDebugString()
  {
    // Deliberately omit clustering values, since debug strings can reach users who may not be allowed to see them.
    return "clusterGroup[" + clusterGroup.groupNumber() + "]->" + segment.getDebugString();
  }

  @Override
  public void close()
  {
    // Do nothing: this is a view, and the clustered segment it came from owns the resources.
  }
}
