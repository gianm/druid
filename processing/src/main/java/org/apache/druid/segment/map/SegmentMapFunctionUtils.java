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

package org.apache.druid.segment.map;

import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.query.filter.TrueDimFilter;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Predicate;

public class SegmentMapFunctionUtils
{
  /**
   * TODO(gianm): javadocs
   *
   * @param segmentMapFunction
   * @param baseFilter
   * @param filter
   *
   * @return
   */
  @Nullable
  public static DimFilter makePushDownFilter(
      @Nullable final DimFilter filter,
      final Predicate<String> isPushDownColumn
  )
  {
    if (filter == null) {
      return null;
    }

    final List<DimFilter> retVal = new ArrayList<>();

    for (final DimFilter andChild : flattenAndChildren(Collections.singletonList(filter))) {
      if (andChild.getRequiredColumns().stream().allMatch(isPushDownColumn)) {
        retVal.add(andChild);
      }
    }

    return DimFilters.maybeAnd(retVal).orElse(null);
  }

  /**
   * Flattens children of an AND, removes duplicates, and removes literally-true filters.
   */
  private static LinkedHashSet<DimFilter> flattenAndChildren(final List<DimFilter> filters)
  {
    final LinkedHashSet<DimFilter> retVal = new LinkedHashSet<>();

    for (DimFilter child : filters) {
      if (child instanceof AndDimFilter) {
        retVal.addAll(flattenAndChildren(((AndDimFilter) child).getFields()));
      } else if (!(child instanceof TrueDimFilter)) {
        retVal.add(child);
      }
    }

    return retVal;
  }
}
