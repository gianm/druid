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

package org.apache.druid.segment.join;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.segment.join.table.RowBasedIndexedTable;
import org.apache.druid.server.ClientQuerySegmentWalker;
import org.apache.druid.server.SimpleQuerySegmentWalker;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DefaultJoinableFactory implements JoinableFactory
{
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final SegmentWrangler segmentWrangler;
  private final LookupReferencesManager lookupReferencesManager;

  @Inject
  public DefaultJoinableFactory(
      QueryRunnerFactoryConglomerate conglomerate,
      SegmentWrangler segmentWrangler,
      LookupReferencesManager lookupReferencesManager
  )
  {
    this.conglomerate = conglomerate;
    this.segmentWrangler = segmentWrangler;
    this.lookupReferencesManager = lookupReferencesManager;
  }

  private static IndexedTable tableFromInlineDataSource(
      final InlineDataSource dataSource,
      final List<String> keyColumns
  )
  {
    final Iterable<Object[]> rowIterable = dataSource.getRows();

    return new RowBasedIndexedTable<>(
        rowIterable instanceof List ? (List<Object[]>) rowIterable : Lists.newArrayList(rowIterable),
        dataSource.rowAdapter(),
        dataSource.getRowSignature(),
        keyColumns
    );
  }

  @Override
  public Optional<Joinable> build(final DataSource dataSource, final JoinConditionAnalysis condition)
  {
    if (!condition.canHashJoin()) {
      // Currently we can only handle hash joins.
      return Optional.empty();
    }

    final List<String> rightKeyColumns = condition.getEquiConditions()
                                                  .stream()
                                                  .map(Equality::getRightColumn)
                                                  .distinct()
                                                  .collect(Collectors.toList());

    if (dataSource instanceof LookupDataSource) {
      final LookupExtractorFactoryContainer container =
          lookupReferencesManager.get(((LookupDataSource) dataSource).getLookupName());

      return Optional.ofNullable(container).map(c -> LookupJoinable.wrap(c.getLookupExtractorFactory().get()));
    } else if (dataSource instanceof InlineDataSource) {
      return Optional.of(
          new IndexedTableJoinable(
              tableFromInlineDataSource((InlineDataSource) dataSource, rightKeyColumns)
          )
      );
    } else if (dataSource instanceof QueryDataSource) {
      final Query query = ((QueryDataSource) dataSource).getQuery();
      final QueryRunner runner = query.getRunner(new SimpleQuerySegmentWalker(conglomerate, segmentWrangler, this));
      final QueryToolChest toolChest = conglomerate.findFactory(query).getToolchest();

      final InlineDataSource inlined = ClientQuerySegmentWalker.inline(toolChest, runner, query);

      return Optional.of(new IndexedTableJoinable(tableFromInlineDataSource(inlined, rightKeyColumns)));
    } else {
      return Optional.empty();
    }
  }
}
