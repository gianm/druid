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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.ToLongFunction;

public class InlineDataSource implements DataSource
{
  private final List<String> columnNames;
  private final List<ValueType> columnTypes;
  private final Iterable<Object[]> rows;

  private InlineDataSource(
      final List<String> columnNames,
      final List<ValueType> columnTypes,
      final Iterable<Object[]> rows
  )
  {
    this.columnNames = Preconditions.checkNotNull(columnNames, "columnNames");
    this.columnTypes = Preconditions.checkNotNull(columnTypes, "columnTypes");
    this.rows = Preconditions.checkNotNull(rows, "rows");

    if (columnNames.size() != columnTypes.size()) {
      throw new IAE("columnNames and columnTypes must be the same length");
    }
  }

  @JsonCreator
  public static InlineDataSource fromJson(
      @JsonProperty("columnNames") List<String> columnNames,
      @JsonProperty("columnTypes") List<ValueType> columnTypes,
      @JsonProperty("rows") List<Object[]> rows
  )
  {
    return new InlineDataSource(columnNames, columnTypes, rows);
  }

  @JsonCreator
  public static InlineDataSource fromIterable(
      final List<String> columnNames,
      final List<ValueType> columnTypes,
      final Iterable<Object[]> rows
  )
  {
    return new InlineDataSource(columnNames, columnTypes, rows);
  }

  @Override
  public List<String> getNames()
  {
    return Collections.emptyList();
  }

  @JsonProperty
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  @JsonProperty
  public List<ValueType> getColumnTypes()
  {
    return columnTypes;
  }

  @JsonProperty("rows")
  public List<Object[]> getRowsAsList()
  {
    return rows instanceof List ? ((List<Object[]>) rows) : ImmutableList.copyOf(rows);
  }

  @JsonIgnore
  public Iterable<Object[]> getRows()
  {
    return rows;
  }

  @Override
  public List<DataSource> getChildren()
  {
    return Collections.emptyList();
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (!children.isEmpty()) {
      throw new IAE("Cannot accept children");
    }

    return this;
  }

  @Override
  public boolean isCacheable()
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return true;
  }

  @Override
  public boolean isConcrete()
  {
    return true;
  }

  public Map<String, ValueType> getRowSignature()
  {
    final ImmutableMap.Builder<String, ValueType> retVal = ImmutableMap.builder();

    for (int i = 0; i < columnNames.size(); i++) {
      retVal.put(columnNames.get(i), columnTypes.get(i));
    }

    return retVal.build();
  }

  public RowAdapter<Object[]> rowAdapter()
  {
    return new RowAdapter<Object[]>()
    {
      @Override
      public ToLongFunction<Object[]> timestampFunction()
      {
        final int columnNumber = columnNames.indexOf(ColumnHolder.TIME_COLUMN_NAME);

        if (columnNumber >= 0) {
          return row -> (long) row[columnNumber];
        } else {
          return row -> 0L;
        }
      }

      @Override
      public Function<Object[], Object> columnFunction(String columnName)
      {
        final int columnNumber = columnNames.indexOf(columnName);

        if (columnNumber >= 0) {
          return row -> row[columnNumber];
        } else {
          return row -> null;
        }
      }
    };
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InlineDataSource that = (InlineDataSource) o;
    return Objects.equals(columnNames, that.columnNames) &&
           Objects.equals(columnTypes, that.columnTypes) &&
           Objects.equals(rows, that.rows);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnNames, columnTypes, rows);
  }

  @Override
  public String toString()
  {
    return "InlineDataSource{" +
           "columnNames=" + columnNames +
           ", columnTypes=" + columnTypes +
           '}';
  }
}
