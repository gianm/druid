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

package org.apache.druid.segment.incremental;

import com.google.common.collect.Iterables;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.AcquiredView;
import org.apache.druid.segment.AggregateProjectionSegment;
import org.apache.druid.segment.ClusterGroupSegment;
import org.apache.druid.segment.ClusterGroupView;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Cursors;
import org.apache.druid.segment.EmptyCursorHolder;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.RemapColumnSelectorFactory;
import org.apache.druid.segment.ResidentCursorFactory;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentViewFetcher;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.projections.ClusterGroupQueryPlan;
import org.apache.druid.segment.projections.ClusteredColumnInspector;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.projections.TableClusterGroupSpec;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class IncrementalIndexCursorFactory implements ResidentCursorFactory
{
  private static final ColumnCapabilities.CoercionLogic COERCE_LOGIC =
      new ColumnCapabilities.CoercionLogic()
      {
        @Override
        public boolean dictionaryEncoded()
        {
          return false;
        }

        @Override
        public boolean dictionaryValuesSorted()
        {
          return false;
        }

        @Override
        public boolean dictionaryValuesUnique()
        {
          return true;
        }

        @Override
        public boolean multipleValues()
        {
          return false;
        }

        @Override
        public boolean hasNulls()
        {
          return false;
        }
      };

  private final IncrementalIndex index;

  /**
   * Cluster group this factory is restricted to, or null if it covers the whole index. See {@link #forClusterGroup}.
   */
  @Nullable
  private final OnHeapClusterGroup clusterGroup;

  /**
   * Read-side spec of {@link #clusterGroup}. Null if {@link #clusterGroup} is null.
   */
  @Nullable
  private final TableClusterGroupSpec clusterGroupSpec;

  /**
   * How cursors treat specs that match one of the index's aggregate projections. A factory that covers the whole index
   * serves them.
   */
  private final ClusterGroupView.ProjectionRole projectionRole;

  /**
   * Matches specs to the index's aggregate projections. Factories of the cluster groups of one segment share a matcher
   * from {@link IncrementalIndex#makeProjectionMatcher()}, so they agree on whether a spec matches a projection, even
   * as rows are added. Otherwise, the group that serves a projection could read it while another group reads its own
   * rows, counting them twice, or the reverse, losing them.
   */
  private final Function<CursorBuildSpec, QueryableProjection<IncrementalIndexRowSelector>> projectionMatcher;

  public IncrementalIndexCursorFactory(IncrementalIndex index)
  {
    this(index, null, null, ClusterGroupView.ProjectionRole.SERVES, index::getProjection);
  }

  private IncrementalIndexCursorFactory(
      IncrementalIndex index,
      @Nullable OnHeapClusterGroup clusterGroup,
      @Nullable TableClusterGroupSpec clusterGroupSpec,
      ClusterGroupView.ProjectionRole projectionRole,
      Function<CursorBuildSpec, QueryableProjection<IncrementalIndexRowSelector>> projectionMatcher
  )
  {
    this.index = index;
    this.clusterGroup = clusterGroup;
    this.clusterGroupSpec = clusterGroupSpec;
    this.projectionRole = projectionRole;
    this.projectionMatcher = projectionMatcher;
  }

  /**
   * Returns the views of {@code segment}, a segment over {@code index}, that are needed to answer {@code spec}, or any
   * spec if it is null; see {@link SegmentViewFetcher}. The views are resident, so they are ready immediately and fetch
   * nothing.
   */
  public static List<AcquiredView> acquireViews(
      IncrementalIndex index,
      Segment segment,
      @Nullable CursorBuildSpec spec
  )
  {
    final Function<CursorBuildSpec, QueryableProjection<IncrementalIndexRowSelector>> projectionMatcher =
        index.makeProjectionMatcher();
    final QueryableProjection<IncrementalIndexRowSelector> projection =
        spec == null ? null : projectionMatcher.apply(spec);
    if (projection != null) {
      final Segment view =
          new AggregateProjectionSegment(segment, IncrementalIndexRowSelector.class, projection.getRowSelector());
      return List.of(new AcquiredView(AsyncResources.unmanaged(view), 0));
    }

    final OnHeapClusteredBaseTable clusteredBaseTable = getClusteredBaseTable(index);
    if (clusteredBaseTable == null) {
      final Segment view = new ReferenceCountedSegmentProvider.UnmanagedReference(segment);
      return List.of(new AcquiredView(AsyncResources.unmanaged(view), 0));
    }

    // Groups may be added concurrently, so read a snapshot of them.
    final List<TableClusterGroupSpec> groupSpecs = clusteredBaseTable.toMetadataSchema().getClusterGroups();
    final Map<List<Object>, OnHeapClusterGroup> groupsByClusteringValues = new HashMap<>();
    for (OnHeapClusterGroup group : clusteredBaseTable.getGroups().values()) {
      groupsByClusteringValues.put(Arrays.asList(group.getClusteringValues()), group);
    }

    final List<ClusterGroupView> clusterGroups =
        spec == null
        ? ClusterGroupView.forAllGroups(groupSpecs, ClusterGroupView.hasProjections(index.getMetadata()))
        : ClusterGroupView.forSurvivingGroups(
            groupSpecs,
            Projections.planClusterGroupQuery(groupSpecs, spec).survivingGroups()
        );
    final List<AcquiredView> retVal = new ArrayList<>(clusterGroups.size());
    for (ClusterGroupView clusterGroup : clusterGroups) {
      final Object[] clusteringValues = clusterGroup.group().lookupClusteringValues();
      final OnHeapClusterGroup group = groupsByClusteringValues.get(Arrays.asList(clusteringValues));
      if (group == null) {
        throw DruidException.defensive("No cluster group for clustering values [%s]", Arrays.toString(clusteringValues));
      }
      final Segment view = new ClusterGroupSegment(
          segment,
          clusterGroup,
          new IncrementalIndexCursorFactory(
              index,
              group,
              clusterGroup.group(),
              clusterGroup.projectionRole(),
              projectionMatcher
          ),
          () -> IncrementalIndexPhysicalSegmentInspector.forClusterGroup(
              group,
              clusteredBaseTable.getClusteringColumns(),
              index::getMetadata
          ),
          IncrementalIndexRowSelector.class,
          () -> group
      );
      retVal.add(new AcquiredView(AsyncResources.unmanaged(view), 0));
    }
    return retVal;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    final QueryableProjection<IncrementalIndexRowSelector> projection =
        projectionRole == ClusterGroupView.ProjectionRole.IGNORES ? null : projectionMatcher.apply(spec);
    if (projection == null) {
      if (clusterGroup != null) {
        return makeClusterGroupCursorHolder(
            spec,
            clusterGroup,
            Projections.planClusterGroupQuery(List.of(clusterGroupSpec), spec),
            this
        );
      }

      // Clustered base tables keep their rows in per-tuple cluster groups, not the base facts holder (which is
      // empty), so a plain IncrementalIndexCursorHolder over the base index would yield no rows. Dispatch to the
      // clustered path, mirroring historical equivalent.
      final OnHeapClusteredBaseTable clusteredBaseTable = getClusteredBaseTable(index);
      if (clusteredBaseTable != null) {
        return makeClusteredCursorHolder(spec, clusteredBaseTable);
      }
      return new IncrementalIndexCursorHolder(index, spec);
    } else if (projectionRole == ClusterGroupView.ProjectionRole.DEFERS) {
      // Another group of this index serves the projection, which spans every group.
      return EmptyCursorHolder.forSpec(spec, this);
    } else {
      // currently we only have aggregated projections, so isPreAggregated is always true
      return new IncrementalIndexCursorHolder(
          projection.getRowSelector(),
          projection.getCursorBuildSpec()
      )
      {
        @Override
        public ColumnSelectorFactory makeSelectorFactory(CursorBuildSpec buildSpec, IncrementalIndexRowHolder currEntry)
        {
          return projection.wrapColumnSelectorFactory(super.makeSelectorFactory(buildSpec, currEntry));
        }

        @Override
        public boolean isPreAggregated()
        {
          return true;
        }

        @Override
        public List<AggregatorFactory> getAggregatorsForPreAggregated()
        {
          return projection.getCursorBuildSpec().getAggregators();
        }

        @Override
        public List<OrderBy> getOrdering()
        {
          return Cursors.trimOrdering(super.getOrdering(), spec.getVirtualColumns());
        }
      };
    }
  }

  /**
   * Build a cursor holder for a clustered base table using {@link Projections#planClusterGroupQuery}. Refuses tables
   * with more than one group; see
   * {@link org.apache.druid.segment.QueryableIndexCursorFactory#makeClusteredCursorHolder}.
   */
  private static CursorHolder makeClusteredCursorHolder(
      CursorBuildSpec spec,
      OnHeapClusteredBaseTable clusteredBaseTable
  )
  {
    final ClusteredValueGroupsBaseTableSchema summary = clusteredBaseTable.toMetadataSchema();
    final ClusterGroupQueryPlan plan = Projections.planClusterGroupQuery(summary.getClusterGroups(), spec);
    final List<TableClusterGroupSpec> surviving = plan.survivingGroups();
    // Capabilities come from the groups rather than the base index, since the base index's dimensions never see values
    // computed by the cluster spec's virtual columns. Snapshot them like the per-group selector factories do.
    final ColumnInspector allGroupsInspector =
        column -> ColumnCapabilitiesImpl.snapshot(clusteredBaseTable.getColumnCapabilities(column), COERCE_LOGIC);

    if (surviving.isEmpty()) {
      return EmptyCursorHolder.forSpec(spec, allGroupsInspector);
    } else if (summary.getClusterGroups().size() > 1) {
      throw DruidException.defensive(
          "Cannot build a cursor over a segment with [%d] cluster groups; split it into cluster groups first",
          summary.getClusterGroups().size()
      );
    }

    final Object[] groupClusteringValues = surviving.getFirst().lookupClusteringValues();
    final OnHeapClusterGroup group = clusteredBaseTable.getGroupForClusteringValues(groupClusteringValues);
    if (group == null) {
      throw DruidException.defensive(
          "No cluster group for clustering values [%s]",
          Arrays.toString(groupClusteringValues)
      );
    }
    return makeClusterGroupCursorHolder(spec, group, plan, allGroupsInspector);
  }

  /**
   * Build a cursor holder over {@code group}, which must be the only group that {@code plan} may let survive.
   * {@code inspector} supplies capabilities for the empty holder returned when no group survives.
   */
  private static CursorHolder makeClusterGroupCursorHolder(
      CursorBuildSpec spec,
      OnHeapClusterGroup group,
      ClusterGroupQueryPlan plan,
      ColumnInspector inspector
  )
  {
    if (plan.survivingGroups().isEmpty()) {
      return EmptyCursorHolder.forSpec(spec, inspector);
    }

    final TableClusterGroupSpec groupSpec = plan.survivingGroups().getFirst();
    final ClusteredValueGroupsBaseTableSchema summary = groupSpec.getSummary();
    final RowSignature clusteringColumns = summary.getClusteringColumns();
    final Object[] groupClusteringValues = group.getClusteringValues();
    return new IncrementalIndexCursorHolder(group, plan.rebuildCursorBuildSpec(spec, groupSpec))
    {
      @Override
      public List<OrderBy> getOrdering()
      {
        // Like the historical cluster-group holder, report the full segment ordering (the group's clustering columns
        // are constant) unless the caller is granted time ordering.
        return Projections.useTimeOrderedCursors(spec, summary)
               ? super.getOrdering()
               : Cursors.trimOrdering(summary.getOrdering(), spec.getVirtualColumns());
      }

      @Override
      public ColumnSelectorFactory makeSelectorFactory(CursorBuildSpec buildSpec, IncrementalIndexRowHolder currEntry)
      {
        final ColumnSelectorFactory factory = new ClusteringAwareIncrementalIndexColumnSelectorFactory(
            group,
            currEntry,
            buildSpec,
            getTimeOrder(),
            clusteringColumns,
            groupClusteringValues
        );
        return plan.virtualColumnRemap().isEmpty()
               ? factory
               : new RemapColumnSelectorFactory(factory, plan.virtualColumnRemap());
      }
    };
  }

  @Nullable
  private static OnHeapClusteredBaseTable getClusteredBaseTable(IncrementalIndex index)
  {
    return index instanceof OnheapIncrementalIndex onheapIndex ? onheapIndex.getClusteredBaseTable() : null;
  }

  @Override
  public RowSignature getRowSignature()
  {
    final RowSignature.Builder builder = RowSignature.builder();

    if (clusterGroup != null) {
      // The group's rows do not store its clustering columns, so add those separately.
      final LinkedHashSet<String> columns = new LinkedHashSet<>(clusterGroup.getDimensionNames(true));
      columns.addAll(clusterGroupSpec.getSummary().getClusteringColumns().getColumnNames());
      columns.addAll(clusterGroup.getMetricNames());
      for (final String column : columns) {
        builder.add(column, ColumnType.fromCapabilities(getClusterGroupColumnCapabilities(column)));
      }
      return builder.build();
    }

    for (final String column : Iterables.concat(index.getDimensionNames(true), index.getMetricNames())) {
      builder.add(column, ColumnType.fromCapabilities(index.getColumnCapabilities(column)));
    }

    return builder.build();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    if (clusterGroup != null && projectionRole == ClusterGroupView.ProjectionRole.SERVES) {
      // Capabilities of every group, since this group's cursors may read a projection that spans all of them.
      final OnHeapClusteredBaseTable clusteredBaseTable = Objects.requireNonNull(getClusteredBaseTable(index));
      return ColumnCapabilitiesImpl.snapshot(clusteredBaseTable.getColumnCapabilities(column), COERCE_LOGIC);
    } else if (clusterGroup != null) {
      return getClusterGroupColumnCapabilities(column);
    }
    return snapshotColumnCapabilities(index, column);
  }

  /**
   * Returns capabilities of a column of {@link #clusterGroup}, including its clustering columns.
   */
  @Nullable
  private ColumnCapabilities getClusterGroupColumnCapabilities(String column)
  {
    final ColumnType clusteringType =
        clusterGroupSpec.getSummary().getClusteringColumns().getColumnType(column).orElse(null);
    if (clusteringType != null) {
      return ClusteredColumnInspector.clusteringColumnCapabilities(clusteringType);
    }
    return snapshotColumnCapabilities(clusterGroup, column);
  }

  static ColumnCapabilities snapshotColumnCapabilities(IncrementalIndexRowSelector selector, String column)
  {
    return ColumnCapabilitiesImpl.snapshot(
        selector.getColumnCapabilities(column),
        COERCE_LOGIC
    );
  }
}
