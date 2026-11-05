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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.projections.ClusterGroupQueryPlan;
import org.apache.druid.segment.projections.ClusteredColumnInspector;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.apache.druid.segment.vector.RemapVectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorOffset;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public class QueryableIndexCursorFactory implements ResidentCursorFactory
{
  private final QueryableIndex index;
  private final TimeBoundaryInspector timeBoundaryInspector;

  /**
   * Cluster group this factory is restricted to, or null if it covers the whole index. See {@link #forClusterGroup}.
   */
  @Nullable
  private final TableClusterGroupSpec clusterGroup;

  /**
   * How cursors treat specs that match one of the index's aggregate projections. A factory that covers the whole index
   * serves them.
   */
  private final ClusterGroupView.ProjectionRole projectionRole;

  /**
   * Capabilities of the columns of {@link #clusterGroup}. Null if {@link #clusterGroup} is null.
   */
  @Nullable
  private final Supplier<ColumnInspector> clusterGroupInspector;

  /**
   * Constructor that accepts a {@link QueryableIndexTimeBoundaryInspector} that is in use elsewhere, promoting
   * efficient re-use.
   */
  public QueryableIndexCursorFactory(QueryableIndex index, TimeBoundaryInspector timeBoundaryInspector)
  {
    this(index, timeBoundaryInspector, null, ClusterGroupView.ProjectionRole.SERVES);
  }

  /**
   * Constructor that creates a new {@link QueryableIndexTimeBoundaryInspector}.
   */
  public QueryableIndexCursorFactory(QueryableIndex index)
  {
    this(index, QueryableIndexTimeBoundaryInspector.create(index));
  }

  private QueryableIndexCursorFactory(
      QueryableIndex index,
      TimeBoundaryInspector timeBoundaryInspector,
      @Nullable TableClusterGroupSpec clusterGroup,
      ClusterGroupView.ProjectionRole projectionRole
  )
  {
    this.index = index;
    this.timeBoundaryInspector = timeBoundaryInspector;
    this.clusterGroup = clusterGroup;
    this.projectionRole = projectionRole;
    this.clusterGroupInspector =
        clusterGroup == null ? null : Suppliers.memoize(() -> index.getClusterGroupColumnInspector(clusterGroup));
  }

  /**
   * Returns a factory for a {@link ClusterGroupSegment}, which reads only {@code group} of the clustered {@code index}.
   * Its capabilities are the group's own, unless it serves projections, whose cursors read every group.
   */
  public static QueryableIndexCursorFactory forClusterGroup(
      QueryableIndex index,
      TimeBoundaryInspector timeBoundaryInspector,
      TableClusterGroupSpec group,
      ClusterGroupView.ProjectionRole projectionRole
  )
  {
    if (index.getClusteredBaseSummary() == null) {
      throw DruidException.defensive("Cannot restrict a non-clustered index to a cluster group");
    }
    return new QueryableIndexCursorFactory(index, timeBoundaryInspector, group, projectionRole);
  }

  /**
   * Whether this factory was created by {@link #forClusterGroup}.
   */
  public boolean isClusterGroup()
  {
    return clusterGroup != null;
  }

  /**
   * Returns how this factory treats specs that match one of the index's aggregate projections.
   */
  public ClusterGroupView.ProjectionRole getProjectionRole()
  {
    return projectionRole;
  }

  /**
   * Returns the aggregate projection that {@code spec} matches, or null if it matches none, or if this factory ignores
   * projections.
   */
  @Nullable
  public QueryableProjection<QueryableIndex> matchProjection(CursorBuildSpec spec)
  {
    return projectionRole == ClusterGroupView.ProjectionRole.IGNORES ? null : index.getProjection(spec);
  }

  /**
   * Returns the views of {@code segment}, a segment over this factory's index, that are needed to answer {@code spec},
   * or any spec if it is null; see {@link SegmentViewFetcher}. The views are resident, so they are ready immediately
   * and fetch nothing.
   */
  public List<AcquiredView> acquireViews(Segment segment, @Nullable CursorBuildSpec spec)
  {
    final ViewPlan plan = planViews(spec);
    if (plan.projection() != null) {
      return List.of(
          residentView(new AggregateProjectionSegment(segment, QueryableIndex.class, plan.projection().getRowSelector()))
      );
    } else if (plan.clusterGroups() != null) {
      final List<AcquiredView> retVal = new ArrayList<>(plan.clusterGroups().size());
      for (ClusterGroupView clusterGroup : plan.clusterGroups()) {
        retVal.add(
            residentView(
                new ClusterGroupSegment(
                    segment,
                    clusterGroup,
                    forClusterGroup(index, timeBoundaryInspector, clusterGroup.group(), clusterGroup.projectionRole()),
                    () -> new QueryableIndexPhysicalSegmentInspector(
                        index.getClusterGroupQueryableIndex(clusterGroup.group(), true)
                    ),
                    QueryableIndex.class,
                    () -> index.getClusterGroupQueryableIndex(clusterGroup.group(), true)
                )
            )
        );
      }
      return retVal;
    } else {
      return List.of(residentView(new ReferenceCountedSegmentProvider.UnmanagedReference(segment)));
    }
  }

  /**
   * Resolves the views of this factory's index that are needed to answer {@code spec}: the aggregate projection that
   * matches it, or else the cluster groups that it does not rule out, or else the base table. If {@code spec} is null,
   * the views can answer any spec: every cluster group, or else the base table. The factory must cover the whole index.
   */
  ViewPlan planViews(@Nullable CursorBuildSpec spec)
  {
    if (clusterGroup != null) {
      throw DruidException.defensive("Cannot plan views of a single cluster group");
    }

    final QueryableProjection<QueryableIndex> projection = spec == null ? null : index.getProjection(spec);
    if (projection != null) {
      return new ViewPlan(projection, null);
    } else if (index.getClusteredBaseSummary() == null) {
      return new ViewPlan(null, null);
    } else if (spec == null) {
      return new ViewPlan(
          null,
          ClusterGroupView.forAllGroups(
              index.getClusterGroupSchemas(),
              ClusterGroupView.hasProjections(index.getMetadata())
          )
      );
    } else {
      return new ViewPlan(
          null,
          ClusterGroupView.forSurvivingGroups(index.getClusterGroupSchemas(), planClusterGroups(spec).survivingGroups())
      );
    }
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    return makeCursorHolderForProjection(spec, matchProjection(spec));
  }

  public CursorHolder makeCursorHolderForProjection(
      CursorBuildSpec spec,
      @Nullable QueryableProjection<QueryableIndex> projection
  )
  {
    if (projection != null) {
      if (projectionRole == ClusterGroupView.ProjectionRole.DEFERS) {
        // Another group of this index serves the projection, which spans every group.
        return EmptyCursorHolder.forSpec(spec, this);
      }
      return makeAggregateProjectionCursorHolder(spec, projection);
    }

    // Cluster-group dispatch runs after aggregate-projection match, before the regular base-table fallback
    final ClusteredValueGroupsBaseTableSchema clusterSummary = index.getClusteredBaseSummary();
    if (clusterSummary != null) {
      return makeClusteredCursorHolder(spec, planClusterGroups(spec));
    }

    // No projections, no clustering, regular full-segment cursor.
    return new QueryableIndexCursorHolder(index, spec, timeBoundaryInspector);
  }

  /**
   * Plans the cluster groups this factory covers (all of them, or just its {@link #forClusterGroup} group) against
   * {@code spec}. The index must be clustered.
   */
  public ClusterGroupQueryPlan planClusterGroups(CursorBuildSpec spec)
  {
    return Projections.planClusterGroupQuery(
        clusterGroup != null ? List.of(clusterGroup) : new ArrayList<>(index.getClusterGroupSchemas()),
        spec
    );
  }

  /**
   * Build a clustered-base-table cursor holder from an already-computed {@link ClusterGroupQueryPlan}. Exposed so the
   * partial (on-demand) cursor factory can plan the cluster groups once — to decide which group bundles to download —
   * and reuse the same plan to build the holder, rather than re-running {@link Projections#planClusterGroupQuery}.
   * <p>
   * A cursor spans at most one cluster group, since groups do not share dictionaries. Unless this factory is restricted
   * to one group, it refuses indexes with more than one group unless {@code plan} rules out all of them, so callers
   * that do not read one view per group from {@link SegmentViewFetcher} fail even if their filters leave one group.
   */
  public CursorHolder makeClusteredCursorHolder(CursorBuildSpec spec, ClusterGroupQueryPlan plan)
  {
    final List<TableClusterGroupSpec> survivingGroups = plan.survivingGroups();
    if (survivingGroups.isEmpty()) {
      return EmptyCursorHolder.forSpec(spec, this);
    } else if (clusterGroup == null && index.getClusterGroupSchemas().size() > 1) {
      throw DruidException.defensive(
          "Cannot build a cursor over a segment with [%d] cluster groups; split it into cluster groups first",
          index.getClusterGroupSchemas().size()
      );
    }
    return makeSingleGroupClusteredCursorHolder(spec, plan, survivingGroups.getFirst());
  }

  @Override
  public RowSignature getRowSignature()
  {
    final ClusteredValueGroupsBaseTableSchema clusterSummary = index.getClusteredBaseSummary();
    if (clusterSummary != null) {
      return getClusteredRowSignature(
          clusterSummary,
          clusterGroupInspector != null ? clusterGroupInspector.get() : index
      );
    }

    final LinkedHashSet<String> columns = new LinkedHashSet<>();

    for (final OrderBy orderBy : index.getOrdering()) {
      columns.add(orderBy.getColumnName());
    }

    // Add __time after the defined ordering, if __time wasn't part of it.
    columns.add(ColumnHolder.TIME_COLUMN_NAME);
    columns.addAll(index.getColumnNames());

    final RowSignature.Builder builder = RowSignature.builder();
    for (final String column : columns) {
      final ColumnType columnType = ColumnType.fromCapabilities(index.getColumnCapabilities(column));

      // index.getOrdering() may include columns that don't exist, such as if they were omitted due to
      // being 100% nulls. Don't add those to the row signature.
      if (columnType != null) {
        builder.add(column, columnType);
      }
    }

    return builder.build();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    if (clusterGroupInspector != null && projectionRole != ClusterGroupView.ProjectionRole.SERVES) {
      return clusterGroupInspector.get().getColumnCapabilities(column);
    }
    return index.getColumnCapabilities(column);
  }

  private CursorHolder makeSingleGroupClusteredCursorHolder(
      CursorBuildSpec spec,
      ClusterGroupQueryPlan plan,
      TableClusterGroupSpec valueGroup
  )
  {
    final QueryableIndex groupIndex = index.getClusterGroupQueryableIndex(valueGroup, true);
    if (groupIndex == null) {
      throw DruidException.defensive(
          "No cluster-group sub-index resolvable for clustering values "
          + Arrays.toString(valueGroup.lookupClusteringValues())
      );
    }

    // Omit cluster key from ordering if the caller requests (and is granted) time ordering.
    final ClusteredValueGroupsBaseTableSchema summary = valueGroup.getSummary();
    final List<OrderBy> ordering = Cursors.trimOrdering(
        Projections.useTimeOrderedCursors(spec, summary) ? summary.getGroupOrdering() : summary.getOrdering(),
        spec.getVirtualColumns()
    );

    final Map<String, String> remap = plan.virtualColumnRemap();
    return new QueryableIndexCursorHolder(
        groupIndex,
        plan.rebuildCursorBuildSpec(spec, valueGroup),
        QueryableIndexTimeBoundaryInspector.create(groupIndex),
        ordering
    )
    {
      @Override
      protected ColumnSelectorFactory makeColumnSelectorFactoryForOffset(
          ColumnCache columnCache,
          Offset baseOffset
      )
      {
        final ColumnSelectorFactory factory = super.makeColumnSelectorFactoryForOffset(columnCache, baseOffset);
        return remap.isEmpty() ? factory : new RemapColumnSelectorFactory(factory, remap);
      }

      @Override
      protected VectorColumnSelectorFactory makeVectorColumnSelectorFactoryForOffset(
          ColumnCache columnCache,
          VectorOffset baseOffset
      )
      {
        final VectorColumnSelectorFactory factory =
            super.makeVectorColumnSelectorFactoryForOffset(columnCache, baseOffset);
        return remap.isEmpty() ? factory : new RemapVectorColumnSelectorFactory(factory, remap);
      }
    };
  }

  /**
   * Build the row signature for a clustered segment. Top-level columns are empty, so column types are sourced from:
   *   - the summary's clustering {@link RowSignature} for clustering columns;
   *   - the cluster groups' column descriptors for everything else, combined across groups by
   *     {@link ClusteredColumnInspector}.
   * <p>
   * Types come from {@code inspector}, so a factory created by {@link #forClusterGroup} reports only the columns of
   * its own group.
   */
  private static RowSignature getClusteredRowSignature(
      ClusteredValueGroupsBaseTableSchema clusterSummary,
      ColumnInspector inspector
  )
  {
    final LinkedHashSet<String> columns = new LinkedHashSet<>();

    for (final OrderBy orderBy : clusterSummary.getOrdering()) {
      columns.add(orderBy.getColumnName());
    }
    columns.add(ColumnHolder.TIME_COLUMN_NAME);
    columns.addAll(clusterSummary.getColumnNames());

    final RowSignature.Builder builder = RowSignature.builder();
    for (final String column : columns) {
      final ColumnType columnType = ColumnType.fromCapabilities(inspector.getColumnCapabilities(column));
      if (columnType != null) {
        builder.add(column, columnType);
      }
    }
    return builder.build();
  }

  private static CursorHolder makeAggregateProjectionCursorHolder(
      CursorBuildSpec spec,
      QueryableProjection<QueryableIndex> projection
  )
  {
    return new QueryableIndexCursorHolder(
        projection.getRowSelector(),
        projection.getCursorBuildSpec(),
        QueryableIndexTimeBoundaryInspector.create(projection.getRowSelector())
    )
    {
      @Override
      protected ColumnSelectorFactory makeColumnSelectorFactoryForOffset(
          ColumnCache columnCache,
          Offset baseOffset
      )
      {
        return projection.wrapColumnSelectorFactory(
            super.makeColumnSelectorFactoryForOffset(columnCache, baseOffset)
        );
      }

      @Override
      protected VectorColumnSelectorFactory makeVectorColumnSelectorFactoryForOffset(
          ColumnCache columnCache,
          VectorOffset baseOffset
      )
      {
        return projection.wrapVectorColumnSelectorFactory(
            super.makeVectorColumnSelectorFactoryForOffset(columnCache, baseOffset)
        );
      }

      @Override
      public boolean isPreAggregated()
      {
        return true;
      }

      @Nullable
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

  private static AcquiredView residentView(Segment view)
  {
    return new AcquiredView(AsyncResources.unmanaged(view), 0);
  }

  /**
   * Result of {@link #planViews}: the matched aggregate projection, or else the cluster groups to read, or else (both
   * null) the base table.
   */
  record ViewPlan(
      @Nullable QueryableProjection<QueryableIndex> projection,
      @Nullable List<ClusterGroupView> clusterGroups
  )
  {
  }
}
