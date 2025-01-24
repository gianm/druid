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

package org.apache.druid.query.filter;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.error.DruidException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 *
 */
public class DimFilters
{
  public static SelectorDimFilter dimEquals(String dimension, String value)
  {
    return new SelectorDimFilter(dimension, value, null);
  }

  public static Optional<DimFilter> maybeAnd(List<DimFilter> filters)
  {
    final List<DimFilter> flattened = new ArrayList<>();
    for (final DimFilter filter : filters) {
      if (filter instanceof AndDimFilter) {
        flattened.addAll(((AndDimFilter) filter).getFields());
      } else if (filter != null) {
        flattened.add(filter);
      }
    }

    switch (flattened.size()) {
      case 0:
        return Optional.empty();
      case 1:
        return Optional.of(flattened.get(0));
      default:
        return Optional.of(new AndDimFilter(filters));
    }
  }

  public static DimFilter and(DimFilter... filters)
  {
    return and(Arrays.asList(filters));
  }

  public static DimFilter and(List<DimFilter> filters)
  {
    return maybeAnd(filters)
        .orElseThrow(() -> DruidException.defensive("Must have at least one non-null filter"));
  }

  public static DimFilter or(DimFilter... filters)
  {
    return or(Arrays.asList(filters));
  }

  public static DimFilter or(List<DimFilter> filters)
  {
    final List<DimFilter> flattened = new ArrayList<>();
    for (final DimFilter filter : filters) {
      if (filter instanceof OrDimFilter) {
        flattened.addAll(((OrDimFilter) filter).getFields());
      } else if (filter != null) {
        flattened.add(filter);
      }
    }

    if (flattened.size() == 1) {
      return flattened.get(0);
    } else {
      return new OrDimFilter(filters);
    }
  }

  public static NotDimFilter not(DimFilter filter)
  {
    return new NotDimFilter(filter);
  }

  public static List<DimFilter> optimize(List<DimFilter> filters, boolean mayIncludeUnknown)
  {
    return filterNulls(Lists.transform(filters, filter -> filter.optimize(mayIncludeUnknown)));
  }

  public static List<DimFilter> filterNulls(List<DimFilter> optimized)
  {
    return Lists.newArrayList(Iterables.filter(optimized, Predicates.notNull()));
  }
}
