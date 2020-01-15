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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.lookup.LookupColumnSelectorFactory;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.RowSignature;

import java.util.Map;

public class LookupSchema extends AbstractSchema
{
  public static final String NAME = "lookup";

  private static final RowSignature ROW_SIGNATURE =
      RowSignature.builder()
                  .add(LookupColumnSelectorFactory.KEY_COLUMN, ValueType.STRING)
                  .add(LookupColumnSelectorFactory.VALUE_COLUMN, ValueType.STRING)
                  .build();

  private final LookupReferencesManager lookupManager;

  @Inject
  public LookupSchema(final LookupReferencesManager lookupManager)
  {
    this.lookupManager = lookupManager;
  }

  @Override
  protected Map<String, Table> getTableMap()
  {
    final ImmutableMap.Builder<String, Table> tableMapBuilder = ImmutableMap.builder();

    for (final String lookupName : lookupManager.getAllLookupsState().getCurrent().keySet()) {
      tableMapBuilder.put(lookupName, new DruidTable(new LookupDataSource(lookupName), ROW_SIGNATURE));
    }

    return tableMapBuilder.build();
  }
}
