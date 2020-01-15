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

import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.apache.druid.query.lookup.LookupSegment;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.Optional;

public class DefaultSegmentWrangler implements SegmentWrangler
{
  private final LookupReferencesManager lookupReferencesManager;

  @Inject
  public DefaultSegmentWrangler(final LookupReferencesManager lookupReferencesManager)
  {
    this.lookupReferencesManager = lookupReferencesManager;
  }

  @Override
  public Iterable<Segment> getSegmentsForIntervals(final DataSource dataSource, final Iterable<Interval> intervals)
  {
    if (dataSource instanceof LookupDataSource) {
      final LookupExtractorFactoryContainer container =
          lookupReferencesManager.get(((LookupDataSource) dataSource).getLookupName());

      final Optional<Segment> segment = Optional.ofNullable(container).map(
          c -> new LookupSegment(
              ((LookupDataSource) dataSource).getLookupName(),
              c.getLookupExtractorFactory()
          )
      );

      return segment.map(Collections::singletonList).orElse(Collections.emptyList());
    } else if (dataSource instanceof InlineDataSource) {
      return Collections.singletonList(new InlineSegment((InlineDataSource) dataSource));
    } else {
      throw new ISE("TODO(gianm)");
    }
  }
}
