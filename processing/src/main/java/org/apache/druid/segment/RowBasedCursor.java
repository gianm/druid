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

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;

public class RowBasedCursor<RowType> implements Cursor
{
  private final Iterable<RowType> rowIterable;
  private final RowAdapter<RowType> rowAdapter;
  private final Interval interval;
  private final DateTime cursorTime;
  private final ColumnSelectorFactory columnSelectorFactory;
  private final ValueMatcher valueMatcher;

  private Iterator<RowType> iterator;

  @Nullable
  private RowType current;

  RowBasedCursor(
      final Iterable<RowType> rowIterable,
      final RowAdapter<RowType> rowAdapter,
      @Nullable final Filter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final Granularity gran,
      final Map<String, ValueType> rowSignature
  )
  {
    this.rowIterable = rowIterable;
    this.rowAdapter = rowAdapter;
    this.interval = interval;
    this.cursorTime = gran.toDateTime(interval.getStartMillis());
    this.columnSelectorFactory = virtualColumns.wrap(
        RowBasedColumnSelectorFactory.create(
            rowAdapter,
            () -> current,
            rowSignature
        )
    );

    if (filter == null) {
      this.valueMatcher = BooleanValueMatcher.of(true);
    } else {
      this.valueMatcher = filter.makeMatcher(this.columnSelectorFactory);
    }

    reset();
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return columnSelectorFactory;
  }

  @Override
  public DateTime getTime()
  {
    return cursorTime;
  }

  @Override
  public void advance()
  {
    advanceUninterruptibly();
    BaseQuery.checkInterrupted();
  }

  @Override
  public void advanceUninterruptibly()
  {
    if (iterator.hasNext()) {
      current = iterator.next();
    } else {
      current = null;
      return;
    }

    while (!interval.contains(rowAdapter.timestampFunction().applyAsLong(current)) || !valueMatcher.matches()) {
      if (iterator.hasNext()) {
        current = iterator.next();
      } else {
        current = null;
        return;
      }
    }
  }

  @Override
  public boolean isDone()
  {
    return current == null;
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone() || Thread.currentThread().isInterrupted();
  }

  @Override
  public void reset()
  {
    iterator = rowIterable.iterator();
    advanceUninterruptibly();
  }
}
