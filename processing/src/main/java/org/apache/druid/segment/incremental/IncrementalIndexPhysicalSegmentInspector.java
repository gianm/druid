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

import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.projections.ClusteredColumnInspector;

import javax.annotation.Nullable;
import java.util.function.Supplier;

@SuppressWarnings("deprecation")
public class IncrementalIndexPhysicalSegmentInspector implements PhysicalSegmentInspector
{
  static final ColumnCapabilities.CoercionLogic SNAPSHOT_COERCE_LOGIC =
      new ColumnCapabilities.CoercionLogic()
      {
        @Override
        public boolean dictionaryEncoded()
        {
          return true;
        }

        @Override
        public boolean dictionaryValuesSorted()
        {
          return true;
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

  private final IncrementalIndexRowSelector rowSelector;
  private final Supplier<Metadata> metadataSupplier;

  /**
   * Columns served as per-group constants, for an inspector created by {@link #forClusterGroup}. Empty otherwise.
   */
  private final RowSignature clusteringColumns;
  private final Object[] clusteringValues;

  public IncrementalIndexPhysicalSegmentInspector(IncrementalIndex index)
  {
    this(index, index::getMetadata, RowSignature.empty(), new Object[0]);
  }

  private IncrementalIndexPhysicalSegmentInspector(
      IncrementalIndexRowSelector rowSelector,
      Supplier<Metadata> metadataSupplier,
      RowSignature clusteringColumns,
      Object[] clusteringValues
  )
  {
    this.rowSelector = rowSelector;
    this.metadataSupplier = metadataSupplier;
    this.clusteringColumns = clusteringColumns;
    this.clusteringValues = clusteringValues;
  }

  /**
   * Returns an inspector for one cluster group of a clustered index. The group's rows do not store its clustering
   * columns, so the inspector reports them as constants. {@code metadataSupplier} supplies the whole index's
   * {@link Metadata}.
   */
  static IncrementalIndexPhysicalSegmentInspector forClusterGroup(
      OnHeapClusterGroup group,
      RowSignature clusteringColumns,
      Supplier<Metadata> metadataSupplier
  )
  {
    return new IncrementalIndexPhysicalSegmentInspector(
        group,
        metadataSupplier,
        clusteringColumns,
        group.getClusteringValues()
    );
  }

  @Nullable
  @Override
  public Metadata getMetadata()
  {
    return metadataSupplier.get();
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    final int clusteringIndex = clusteringColumns.indexOf(column);
    if (clusteringIndex >= 0) {
      return (Comparable) clusteringValues[clusteringIndex];
    }

    IncrementalIndex.DimensionDesc desc = rowSelector.getDimension(column);
    if (desc == null) {
      return null;
    }

    DimensionIndexer indexer = desc.getIndexer();
    return indexer.getMinValue();
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    final int clusteringIndex = clusteringColumns.indexOf(column);
    if (clusteringIndex >= 0) {
      return (Comparable) clusteringValues[clusteringIndex];
    }

    IncrementalIndex.DimensionDesc desc = rowSelector.getDimension(column);
    if (desc == null) {
      return null;
    }

    DimensionIndexer indexer = desc.getIndexer();
    return indexer.getMaxValue();
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    if (column.equals(ColumnHolder.TIME_COLUMN_NAME)) {
      return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
    } else if (clusteringColumns.indexOf(column) >= 0) {
      return 1;
    }

    IncrementalIndex.DimensionDesc desc = rowSelector.getDimension(column);
    if (desc == null) {
      // non-existent dimension has cardinality = 1 (one null, nothing else).
      return 1;
    }

    return desc.getIndexer().getCardinality();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    final ColumnType clusteringType = clusteringColumns.getColumnType(column).orElse(null);
    if (clusteringType != null) {
      return ClusteredColumnInspector.clusteringColumnCapabilities(clusteringType);
    }

    return ColumnCapabilitiesImpl.snapshot(
        rowSelector.getColumnCapabilities(column),
        SNAPSHOT_COERCE_LOGIC
    );
  }

  @Override
  public int getNumRows()
  {
    return rowSelector.numRows();
  }
}
