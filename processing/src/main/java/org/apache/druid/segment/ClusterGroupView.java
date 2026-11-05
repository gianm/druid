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

import org.apache.druid.query.filter.SegmentPruner;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.apache.druid.timeline.ClusterGroupTuples;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Description of the cluster group that a view from {@link SegmentViewFetcher} was scoped by, exposed by the view
 * through {@link Segment#as}.
 *
 * @param groupNumber    position of the group among the cluster groups of its segment. Stable for immutable segments,
 *                       so it can tell groups apart in cache keys.
 * @param group          the group, including its clustering columns and values
 * @param projectionRole how cursors over this group treat specs that match one of the segment's aggregate projections
 */
public record ClusterGroupView(int groupNumber, TableClusterGroupSpec group, ProjectionRole projectionRole)
{
  /**
   * Returns views of {@code groups} that are in {@code survivingGroups}, which must hold elements of {@code groups},
   * in the order of {@code groups}. For views acquired with the spec their cursors will use, which matched no
   * aggregate projection, so their cursors ignore projections.
   */
  public static List<ClusterGroupView> forSurvivingGroups(
      List<TableClusterGroupSpec> groups,
      List<TableClusterGroupSpec> survivingGroups
  )
  {
    final Set<TableClusterGroupSpec> surviving = Collections.newSetFromMap(new IdentityHashMap<>());
    surviving.addAll(survivingGroups);
    final List<ClusterGroupView> retVal = new ArrayList<>(survivingGroups.size());
    for (int i = 0; i < groups.size(); i++) {
      if (surviving.contains(groups.get(i))) {
        retVal.add(new ClusterGroupView(i, groups.get(i), ProjectionRole.IGNORES));
      }
    }
    return retVal;
  }

  /**
   * Returns views of every one of {@code groups}, for views acquired without knowing the spec their cursors will use.
   * Since a spec may match an aggregate projection, which spans every group, the first group serves the projections
   * if {@code hasProjections}, and the others defer to it.
   */
  public static List<ClusterGroupView> forAllGroups(List<TableClusterGroupSpec> groups, boolean hasProjections)
  {
    final List<ClusterGroupView> retVal = new ArrayList<>(groups.size());
    for (int i = 0; i < groups.size(); i++) {
      // Defer, rather than ignore, even without projections, so specs that require one fail as on the whole segment.
      final ProjectionRole role = i == 0 && hasProjections ? ProjectionRole.SERVES : ProjectionRole.DEFERS;
      retVal.add(new ClusterGroupView(i, groups.get(i), role));
    }
    return retVal;
  }

  /**
   * Whether {@code metadata} lists any aggregate projections.
   */
  public static boolean hasProjections(@Nullable Metadata metadata)
  {
    return metadata != null && metadata.getProjections() != null && !metadata.getProjections().isEmpty();
  }

  /**
   * Whether cursors over this group read the whole projection for specs that match one; see
   * {@link ProjectionRole#SERVES}.
   */
  public boolean servesProjections()
  {
    return projectionRole == ProjectionRole.SERVES;
  }

  /**
   * Returns the clustering values of this group as {@link ClusterGroupTuples}, for
   * {@link SegmentPruner#includeClusterGroups}.
   */
  public ClusterGroupTuples toClusterGroupTuples()
  {
    final ClusteredValueGroupsBaseTableSchema summary = group.getSummary();
    return new ClusterGroupTuples(
        summary.getClusteringColumns(),
        summary.getVirtualColumns().isEmpty() ? null : summary.getVirtualColumns(),
        List.of(Arrays.asList(group.lookupClusteringValues()))
    );
  }

  /**
   * How cursors over a cluster group treat specs that match one of the segment's aggregate projections. Projections
   * span every group, so when views are acquired without knowing the spec, one group must serve them, and the others
   * must not read their own rows for those specs.
   */
  public enum ProjectionRole
  {
    /**
     * Cursors read the whole projection for specs that match one, including rows of other groups.
     */
    SERVES,

    /**
     * Cursors are empty for specs that match a projection, since another group serves it.
     */
    DEFERS,

    /**
     * Cursors read the group's own rows for every spec. Used for views acquired with the spec their cursors will use,
     * which already resolved any matching projection to a view of its own.
     */
    IGNORES
  }
}
