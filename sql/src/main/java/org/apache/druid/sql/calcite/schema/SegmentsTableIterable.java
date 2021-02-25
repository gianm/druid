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

package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentWithOvershadowedStatus;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SegmentsTableIterable implements Iterable<Object[]>
{
  private static final Function<SegmentWithOvershadowedStatus, Iterable<ResourceAction>>
      SEGMENT_WITH_OVERSHADOWED_STATUS_RA_GENERATOR = segment ->
      Collections.singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(
          segment.getDataSegment().getDataSource())
      );

  /**
   * Booleans constants represented as long type,
   * where 1 = true and 0 = false to make it easy to count number of segments
   * which are published, available etc.
   */
  private static final long IS_PUBLISHED_FALSE = 0L;
  private static final long IS_PUBLISHED_TRUE = 1L;
  private static final long IS_AVAILABLE_TRUE = 1L;
  private static final long IS_OVERSHADOWED_FALSE = 0L;
  private static final long IS_OVERSHADOWED_TRUE = 1L;

  static final int SEGMENT_COLUMN_ID = 0;
  static final int SEGMENT_COLUMN_DATASOURCE = 1;
  static final int SEGMENT_COLUMN_START = 2;
  static final int SEGMENT_COLUMN_END = 3;
  static final int SEGMENT_COLUMN_SIZE = 4;
  static final int SEGMENT_COLUMN_VERSION = 5;
  static final int SEGMENT_COLUMN_PARTITION_NUM = 6;
  static final int SEGMENT_COLUMN_NUM_REPLICAS = 7;
  static final int SEGMENT_COLUMN_NUM_ROWS = 8;
  static final int SEGMENT_COLUMN_IS_PUBLISHED = 9;
  static final int SEGMENT_COLUMN_IS_AVAILABLE = 10;
  static final int SEGMENT_COLUMN_IS_REALTIME = 11;
  static final int SEGMENT_COLUMN_IS_OVERSHADOWED = 12;
  static final int SEGMENT_COLUMN_SHARD_SPEC = 13;
  static final int SEGMENT_COLUMN_DIMENSIONS = 14;
  static final int SEGMENT_COLUMN_METRICS = 15;
  static final int SEGMENT_COLUMN_LAST_COMPACTION_STATE = 16;

  private final DruidSchema druidSchema;
  private final ObjectMapper jsonMapper;
  private final AuthorizerMapper authorizerMapper;
  private final MetadataSegmentView metadataView;
  private final AuthenticationResult authenticationResult;

  SegmentsTableIterable(
      DruidSchema druidSchema,
      ObjectMapper jsonMapper,
      AuthorizerMapper authorizerMapper,
      MetadataSegmentView metadataView,
      AuthenticationResult authenticationResult
  )
  {
    this.druidSchema = druidSchema;
    this.jsonMapper = jsonMapper;
    this.authorizerMapper = authorizerMapper;
    this.metadataView = metadataView;
    this.authenticationResult = authenticationResult;
  }

  @Override
  public Iterator<Object[]> iterator()
  {
    //get available segments from druidSchema
    final Map<SegmentId, AvailableSegmentMetadata> availableSegmentMetadata =
        druidSchema.getSegmentMetadataSnapshot();
    final Iterator<Map.Entry<SegmentId, AvailableSegmentMetadata>> availableSegmentEntries =
        availableSegmentMetadata.entrySet().iterator();

    // in memory map to store segment data from available segments
    final Map<SegmentId, PartialSegmentData> partialSegmentDataMap =
        Maps.newHashMapWithExpectedSize(druidSchema.getTotalSegments());
    for (AvailableSegmentMetadata h : availableSegmentMetadata.values()) {
      PartialSegmentData partialSegmentData =
          new PartialSegmentData(IS_AVAILABLE_TRUE, h.isRealtime(), h.getNumReplicas(), h.getNumRows());
      partialSegmentDataMap.put(h.getSegment().getId(), partialSegmentData);
    }

    // Get published segments from metadata segment cache (if enabled in SQL planner config), else directly from
    // Coordinator.
    final Iterator<SegmentWithOvershadowedStatus> metadataStoreSegments = metadataView.getPublishedSegments();

    final Set<SegmentId> segmentsAlreadySeen = new HashSet<>();

    final FluentIterable<Object[]> publishedSegments = FluentIterable
        .from(() -> getAuthorizedPublishedSegments(metadataStoreSegments, authenticationResult))
        .transform(val -> {
          final DataSegment segment = val.getDataSegment();
          segmentsAlreadySeen.add(segment.getId());
          final PartialSegmentData partialSegmentData = partialSegmentDataMap.get(segment.getId());
          long numReplicas = 0L, numRows = 0L, isRealtime = 0L, isAvailable = 0L;
          if (partialSegmentData != null) {
            numReplicas = partialSegmentData.getNumReplicas();
            numRows = partialSegmentData.getNumRows();
            isAvailable = partialSegmentData.isAvailable();
            isRealtime = partialSegmentData.isRealtime();
          }
          try {
            final Object[] row = new Object[SegmentsTable.SIGNATURE.size()];

            for (int i = 0; i < row.length; i++) {
              switch (i) {
                case SEGMENT_COLUMN_ID:
                  row[i] = segment.getId().toString();
                  break;
                case SEGMENT_COLUMN_DATASOURCE:
                  row[i] = segment.getDataSource();
                  break;
                case SEGMENT_COLUMN_START:
                  row[i] = segment.getInterval().getStart().toString();
                  break;
                case SEGMENT_COLUMN_END:
                  row[i] = segment.getInterval().getEnd().toString();
                  break;
                case SEGMENT_COLUMN_SIZE:
                  row[i] = segment.getSize();
                  break;
                case SEGMENT_COLUMN_VERSION:
                  row[i] = segment.getVersion();
                  break;
                case SEGMENT_COLUMN_PARTITION_NUM:
                  row[i] = (long) segment.getShardSpec().getPartitionNum();
                  break;
                case SEGMENT_COLUMN_NUM_REPLICAS:
                  row[i] = numReplicas;
                  break;
                case SEGMENT_COLUMN_NUM_ROWS:
                  row[i] = numRows;
                  break;
                case SEGMENT_COLUMN_IS_PUBLISHED:
                  row[i] = IS_PUBLISHED_TRUE; //is_published is true for published segment
                  break;
                case SEGMENT_COLUMN_IS_AVAILABLE:
                  row[i] = isAvailable;
                  break;
                case SEGMENT_COLUMN_IS_REALTIME:
                  row[i] = isRealtime;
                  break;
                case SEGMENT_COLUMN_IS_OVERSHADOWED:
                  row[i] = val.isOvershadowed() ? IS_OVERSHADOWED_TRUE : IS_OVERSHADOWED_FALSE;
                  break;
                case SEGMENT_COLUMN_SHARD_SPEC:
                  if (segment.getShardSpec() != null) {
                    row[i] = jsonMapper.writeValueAsString(segment.getShardSpec());
                  }
                  break;
                case SEGMENT_COLUMN_DIMENSIONS:
                  if (segment.getDimensions() != null) {
                    row[i] = jsonMapper.writeValueAsString(segment.getDimensions());
                  }
                  break;
                case SEGMENT_COLUMN_METRICS:
                  if (segment.getMetrics() != null) {
                    row[i] = jsonMapper.writeValueAsString(segment.getMetrics());
                  }
                  break;
                case SEGMENT_COLUMN_LAST_COMPACTION_STATE:
                  if (segment.getLastCompactionState() != null) {
                    row[i] = jsonMapper.writeValueAsString(segment.getLastCompactionState());
                  }
                  break;
                default:
                  // Should never happen. It's a bug if you see this message.
                  throw new ISE("Cannot project");
              }
            }

            return row;
          }
          catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        });

    final FluentIterable<Object[]> availableSegments = FluentIterable
        .from(() -> getAuthorizedAvailableSegments(availableSegmentEntries, authenticationResult))
        .transform(val -> {
          if (segmentsAlreadySeen.contains(val.getKey())) {
            return null;
          }
          final PartialSegmentData partialSegmentData = partialSegmentDataMap.get(val.getKey());
          final long numReplicas = partialSegmentData == null ? 0L : partialSegmentData.getNumReplicas();
          try {
            final Object[] row = new Object[SegmentsTable.SIGNATURE.size()];

            for (int i = 0; i < row.length; i++) {
              switch (i) {
                case SEGMENT_COLUMN_ID:
                  row[i] = val.getKey().toString();
                  break;
                case SEGMENT_COLUMN_DATASOURCE:
                  row[i] = val.getKey().getDataSource();
                  break;
                case SEGMENT_COLUMN_START:
                  row[i] = val.getKey().getInterval().getStart().toString();
                  break;
                case SEGMENT_COLUMN_END:
                  row[i] = val.getKey().getInterval().getEnd().toString();
                  break;
                case SEGMENT_COLUMN_SIZE:
                  row[i] = val.getValue().getSegment().getSize();
                  break;
                case SEGMENT_COLUMN_VERSION:
                  row[i] = val.getKey().getVersion();
                  break;
                case SEGMENT_COLUMN_PARTITION_NUM:
                  row[i] = (long) val.getValue().getSegment().getShardSpec().getPartitionNum();
                  break;
                case SEGMENT_COLUMN_NUM_REPLICAS:
                  row[i] = numReplicas;
                  break;
                case SEGMENT_COLUMN_NUM_ROWS:
                  row[i] = val.getValue().getNumRows();
                  break;
                case SEGMENT_COLUMN_IS_PUBLISHED:
                  // is_published is false for unpublished segments
                  row[i] = IS_PUBLISHED_FALSE;
                  break;
                case SEGMENT_COLUMN_IS_AVAILABLE:
                  // is_available is assumed to be always true for segments announced by historicals or realtime tasks
                  row[i] = IS_AVAILABLE_TRUE;
                  break;
                case SEGMENT_COLUMN_IS_REALTIME:
                  row[i] = val.getValue().isRealtime();
                  break;
                case SEGMENT_COLUMN_IS_OVERSHADOWED:
                  // there is an assumption here that unpublished segments are never overshadowed
                  row[i] = IS_OVERSHADOWED_FALSE;
                  break;
                case SEGMENT_COLUMN_SHARD_SPEC:
                  if (val.getValue().getSegment().getShardSpec() != null) {
                    row[i] = jsonMapper.writeValueAsString(val.getValue().getSegment().getShardSpec());
                  }
                  break;
                case SEGMENT_COLUMN_DIMENSIONS:
                  if (val.getValue().getSegment().getDimensions() != null) {
                    row[i] = jsonMapper.writeValueAsString(val.getValue().getSegment().getDimensions());
                  }
                  break;
                case SEGMENT_COLUMN_METRICS:
                  if (val.getValue().getSegment().getMetrics() != null) {
                    row[i] = jsonMapper.writeValueAsString(val.getValue().getSegment().getMetrics());
                  }
                  break;
                case SEGMENT_COLUMN_LAST_COMPACTION_STATE:
                  // unpublished segments from realtime tasks will not be compacted yet
                  break;
                default:
                  // Should never happen. It's a bug if you see this message.
                  throw new ISE("Cannot project");
              }
            }

            return row;
          }
          catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        });

    final Iterable<Object[]> allSegments = Iterables.unmodifiableIterable(
        Iterables.concat(publishedSegments, availableSegments)
    );

    return Iterables.filter(allSegments, Objects::nonNull).iterator();
  }

  private Iterator<SegmentWithOvershadowedStatus> getAuthorizedPublishedSegments(
      Iterator<SegmentWithOvershadowedStatus> it,
      AuthenticationResult authenticationResult
  )
  {
    final Iterable<SegmentWithOvershadowedStatus> authorizedSegments = AuthorizationUtils
        .filterAuthorizedResources(
            authenticationResult,
            () -> it,
            SEGMENT_WITH_OVERSHADOWED_STATUS_RA_GENERATOR,
            authorizerMapper
        );
    return authorizedSegments.iterator();
  }

  private Iterator<Map.Entry<SegmentId, AvailableSegmentMetadata>> getAuthorizedAvailableSegments(
      Iterator<Map.Entry<SegmentId, AvailableSegmentMetadata>> availableSegmentEntries,
      AuthenticationResult authenticationResult
  )
  {
    Function<Map.Entry<SegmentId, AvailableSegmentMetadata>, Iterable<ResourceAction>> raGenerator = segment ->
        Collections.singletonList(
            AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(segment.getKey().getDataSource())
        );

    final Iterable<Map.Entry<SegmentId, AvailableSegmentMetadata>> authorizedSegments =
        AuthorizationUtils.filterAuthorizedResources(
            authenticationResult,
            () -> availableSegmentEntries,
            raGenerator,
            authorizerMapper
        );

    return authorizedSegments.iterator();
  }

  private static class PartialSegmentData
  {
    private final long isAvailable;
    private final long isRealtime;
    private final long numReplicas;
    private final long numRows;

    public PartialSegmentData(
        final long isAvailable,
        final long isRealtime,
        final long numReplicas,
        final long numRows
    )

    {
      this.isAvailable = isAvailable;
      this.isRealtime = isRealtime;
      this.numReplicas = numReplicas;
      this.numRows = numRows;
    }

    public long isAvailable()
    {
      return isAvailable;
    }

    public long isRealtime()
    {
      return isRealtime;
    }

    public long getNumReplicas()
    {
      return numReplicas;
    }

    public long getNumRows()
    {
      return numRows;
    }
  }
}
