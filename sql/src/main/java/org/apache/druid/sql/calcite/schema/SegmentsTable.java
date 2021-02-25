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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.RowSignatures;

/**
 * This table contains row per segment from metadata store as well as served segments.
 */
public class SegmentsTable extends DruidTable
{
  static final RowSignature SIGNATURE = RowSignature
      .builder()
      .add("segment_id", ValueType.STRING)
      .add("datasource", ValueType.STRING)
      .add("start", ValueType.STRING)
      .add("end", ValueType.STRING)
      .add("size", ValueType.LONG)
      .add("version", ValueType.STRING)
      .add("partition_num", ValueType.LONG)
      .add("num_replicas", ValueType.LONG)
      .add("num_rows", ValueType.LONG)
      .add("is_published", ValueType.LONG)
      .add("is_available", ValueType.LONG)
      .add("is_realtime", ValueType.LONG)
      .add("is_overshadowed", ValueType.LONG)
      .add("shard_spec", ValueType.STRING)
      .add("dimensions", ValueType.STRING)
      .add("metrics", ValueType.STRING)
      .add("last_compaction_state", ValueType.STRING)
      .build();

  public SegmentsTable(
      DruidSchema druidSchema,
      MetadataSegmentView metadataView,
      ObjectMapper jsonMapper,
      AuthorizerMapper authorizerMapper
  )
  {
    // TODO(gianm): Must use a different datasource every time, for authentication purposes. TableMacro?
    super(
        new VirtualDataSource(
            new SegmentsTableIterable(
                druidSchema,
                jsonMapper,
                authorizerMapper,
                metadataView,
                new AuthenticationResult("testSuperuser", "allowAll", null, null)
            ),
            SIGNATURE
        ),
        SIGNATURE,
        true,
        false
    );
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory)
  {
    return RowSignatures.toRelDataType(SIGNATURE, typeFactory);
  }

  @Override
  public Schema.TableType getJdbcTableType()
  {
    return Schema.TableType.SYSTEM_TABLE;
  }
}
