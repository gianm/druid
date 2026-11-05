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

import com.google.common.collect.Iterables;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class SegmentViewFetcherTest extends InitializedNullHandlingTest
{
  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

  private QueryableIndex index;

  @AfterEach
  void tearDown()
  {
    if (index != null) {
      index.close();
    }
  }

  @Test
  void testNonClusteredSegmentIsOneView()
  {
    final QueryableIndex testIndex = TestIndex.getMMappedTestIndex();
    final Segment segment = new QueryableIndexSegment(testIndex, SegmentId.dummy("test"));

    final List<AcquiredView> views = segment.as(SegmentViewFetcher.class).acquireViews(CursorBuildSpec.FULL_SCAN);
    Assertions.assertEquals(1, views.size());
    Assertions.assertTrue(views.get(0).view().isReady());
    Assertions.assertEquals(0, views.get(0).fetchedBytes());

    final Segment view = views.get(0).view().get();
    Assertions.assertEquals(segment.getId(), view.getId());
    Assertions.assertSame(testIndex, view.as(QueryableIndex.class));
    Assertions.assertNull(view.as(ClusterGroupView.class));
  }

  @Test
  void testClusteredSegmentIsOneViewPerGroup()
  {
    final Segment segment = buildThreeGroupSegment();
    final List<Segment> views = SegmentViewTestUtil.residentViews(segment, CursorBuildSpec.FULL_SCAN);

    final List<Integer> groupNumbers = new ArrayList<>();
    final List<List<Object>> clusteringValues = new ArrayList<>();
    final List<Integer> numRows = new ArrayList<>();
    final List<List<String>> tenants = new ArrayList<>();
    for (Segment view : views) {
      Assertions.assertEquals(segment.getId(), view.getId());
      final ClusterGroupView clusterGroup = view.as(ClusterGroupView.class);
      groupNumbers.add(clusterGroup.groupNumber());
      clusteringValues.add(Arrays.asList(clusterGroup.group().lookupClusteringValues()));
      // Callers that read columns directly get the group's own rows.
      numRows.add(view.as(QueryableIndex.class).getNumRows());
      tenants.add(readTenants(view));
      Assertions.assertFalse(clusterGroup.servesProjections());
      // Debug strings omit clustering values, which users may not be allowed to see.
      final String tenant = clusterGroup.group().lookupClusteringValues()[0].toString();
      Assertions.assertFalse(view.getDebugString().contains(tenant));
    }
    Assertions.assertEquals(List.of(0, 1, 2), groupNumbers);
    Assertions.assertEquals(List.of(List.of("acme"), List.of("globex"), List.of("initech")), clusteringValues);
    Assertions.assertEquals(List.of(2, 1, 1), numRows);
    Assertions.assertEquals(List.of(List.of("acme", "acme"), List.of("globex"), List.of("initech")), tenants);
  }

  @Test
  void testSpecPrunesClusterGroups()
  {
    final Segment segment = buildThreeGroupSegment();

    // The segment has no aggregate projections, so the first group is pruned like any other.
    Assertions.assertEquals(List.of(2), groupNumbers(segment, tenantSpec("initech")));
    Assertions.assertEquals(List.of(0), groupNumbers(segment, tenantSpec("acme")));
    Assertions.assertEquals(List.of(), groupNumbers(segment, tenantSpec("nobody")));
  }

  @Test
  void testFilteredSegmentRewritesSpecAndWrapsViews()
  {
    final Segment filtered = new FilteredSegment(
        buildThreeGroupSegment(),
        new EqualityFilter("tenant", ColumnType.STRING, "globex", null)
    );

    // The filter prunes cluster groups even though the caller's spec has none.
    final List<Segment> views = SegmentViewTestUtil.residentViews(filtered, CursorBuildSpec.FULL_SCAN);
    Assertions.assertEquals(1, views.size());
    Assertions.assertInstanceOf(FilteredSegment.class, views.get(0));
    // Reading columns directly would skip the filter.
    Assertions.assertNull(views.get(0).as(QueryableIndex.class));
    Assertions.assertEquals(List.of("globex"), readTenants(views.get(0)));
  }

  @Test
  void testRestrictedSegmentWrapsViewsEvenWithoutRestriction()
  {
    final Segment restricted = new RestrictedSegment(
        new ReferenceCountedSegmentProvider.UnmanagedReference(buildThreeGroupSegment()),
        NoRestrictionPolicy.instance()
    );

    final List<Segment> views = SegmentViewTestUtil.residentViews(restricted, CursorBuildSpec.FULL_SCAN);
    Assertions.assertEquals(3, views.size());
    for (Segment view : views) {
      Assertions.assertInstanceOf(RestrictedSegment.class, view);
    }
  }

  @Test
  void testRestrictedSegmentViewsApplyRowFilterPolicy()
  {
    // The policy filters on a non-clustering column, so it rules out no groups, but each view applies it.
    final Segment restricted = new RestrictedSegment(
        new ReferenceCountedSegmentProvider.UnmanagedReference(buildThreeGroupSegment()),
        RowFilterPolicy.from(new EqualityFilter("region", ColumnType.STRING, "us-west-2", null))
    );

    final List<Segment> views = SegmentViewTestUtil.residentViews(restricted, CursorBuildSpec.FULL_SCAN);
    final List<List<String>> tenantsByView = new ArrayList<>();
    for (Segment view : views) {
      Assertions.assertInstanceOf(RestrictedSegment.class, view);
      // Reading columns directly would skip the policy.
      Assertions.assertNull(view.as(QueryableIndex.class));
      tenantsByView.add(readTenants(view));
    }
    Assertions.assertEquals(List.of(List.of("acme"), List.of(), List.of()), tenantsByView);
  }

  @Test
  void testRestrictedSegmentPolicyOnClusteringColumnRulesOutGroups()
  {
    final Segment restricted = new RestrictedSegment(
        new ReferenceCountedSegmentProvider.UnmanagedReference(buildThreeGroupSegment()),
        RowFilterPolicy.from(new EqualityFilter("tenant", ColumnType.STRING, "initech", null))
    );

    // Views are acquired with the spec rewritten by the policy, so its filter rules out the other groups.
    final List<Segment> views = SegmentViewTestUtil.residentViews(restricted, CursorBuildSpec.FULL_SCAN);
    Assertions.assertEquals(List.of("initech"), readTenants(Iterables.getOnlyElement(views)));

    // A query filter that the policy contradicts rules out every group.
    Assertions.assertEquals(List.of(), SegmentViewTestUtil.residentViews(restricted, tenantSpec("acme")));
  }

  private Segment buildThreeGroupSegment()
  {
    index = SegmentViewTestUtil.buildThreeGroupIndex(temporaryFolder.getRoot());
    return new QueryableIndexSegment(index, SegmentId.dummy("test"));
  }

  private static CursorBuildSpec tenantSpec(String tenant)
  {
    return CursorBuildSpec.builder()
                          .setFilter(new EqualityFilter("tenant", ColumnType.STRING, tenant, null))
                          .build();
  }

  private static List<Integer> groupNumbers(Segment segment, CursorBuildSpec spec)
  {
    final List<Integer> groupNumbers = new ArrayList<>();
    for (Segment view : SegmentViewTestUtil.residentViews(segment, spec)) {
      groupNumbers.add(view.as(ClusterGroupView.class).groupNumber());
    }
    return groupNumbers;
  }

  private static List<String> readTenants(Segment segment)
  {
    final List<String> tenants = new ArrayList<>();
    try (CursorHolder holder = segment.as(CursorFactory.class).makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      final Cursor cursor = holder.asCursor();
      final DimensionSelector selector =
          cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
      while (!cursor.isDone()) {
        tenants.add(selector.lookupName(selector.getRow().get(0)));
        cursor.advance();
      }
    }
    return tenants;
  }
}
