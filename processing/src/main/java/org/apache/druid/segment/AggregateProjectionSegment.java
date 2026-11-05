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

import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * View of a segment, from {@link SegmentViewFetcher}, that reads one of its aggregate projections. Cursors come from
 * the segment's own {@link CursorFactory}, which reads the projection for specs that match it. {@link #as} also
 * exposes the projection's row selector, for callers that read columns directly.
 * <p>
 * Closing this view does nothing, since the segment it came from owns the resources.
 */
public class AggregateProjectionSegment implements ReferenceCountedSegmentProvider.LeafReference
{
  private final Segment segment;
  private final Class<?> rowSelectorClass;
  private final Object rowSelector;

  /**
   * Creates a view of the projection of {@code segment} whose rows {@code rowSelector} reads, exposed through
   * {@link #as} as {@code rowSelectorClass}.
   */
  public <T> AggregateProjectionSegment(Segment segment, Class<T> rowSelectorClass, T rowSelector)
  {
    this.segment = segment;
    this.rowSelectorClass = rowSelectorClass;
    this.rowSelector = rowSelector;
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

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    if (rowSelectorClass.equals(clazz)) {
      return (T) rowSelector;
    } else if (CursorFactory.class.equals(clazz)
               || TimeBoundaryInspector.class.equals(clazz)
               || Metadata.class.equals(clazz)
               || MaxIngestedEventTimeInspector.class.equals(clazz)
               || TopNOptimizationInspector.class.equals(clazz)) {
      return segment.as(clazz);
    }
    // Deliberately hide everything else, such as physical inspectors, since they describe the base table.
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
    return "projection->" + segment.getDebugString();
  }

  @Override
  public void close()
  {
    // Do nothing: this is a view, and the segment it came from owns the resources.
  }
}
