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

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class SystemSchemaQueryTest extends BaseCalciteQueryTest
{
  @Test
  public void testSegmentsCount() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM sys.segments",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{9L}
        )
    );
  }

  @Test
  public void testSegmentsQueryFromWebConsoleDatasourcesTab() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  datasource,\n"
        + "  COUNT(*) FILTER (WHERE (is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AS num_segments,\n"
        + "  COUNT(*) FILTER (WHERE is_available = 1 AND ((is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1)) AS num_available_segments,\n"
        + "  COUNT(*) FILTER (WHERE is_published = 1 AND is_overshadowed = 0 AND is_available = 0) AS num_segments_to_load,\n"
        + "  COUNT(*) FILTER (WHERE is_available = 1 AND NOT ((is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1)) AS num_segments_to_drop,\n"
        + "  COUNT(*) FILTER (WHERE ((is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AND \"start\" LIKE '%:00.000Z' AND \"end\" LIKE '%:00.000Z') AS minute_aligned_segments,\n"
        + "  COUNT(*) FILTER (WHERE ((is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AND \"start\" LIKE '%:00:00.000Z' AND \"end\" LIKE '%:00:00.000Z') AS hour_aligned_segments,\n"
        + "  COUNT(*) FILTER (WHERE ((is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AND \"start\" LIKE '%T00:00:00.000Z' AND \"end\" LIKE '%T00:00:00.000Z') AS day_aligned_segments,\n"
        + "  COUNT(*) FILTER (WHERE ((is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AND \"start\" LIKE '%-01T00:00:00.000Z' AND \"end\" LIKE '%-01T00:00:00.000Z') AS month_aligned_segments,\n"
        + "  COUNT(*) FILTER (WHERE ((is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AND \"start\" LIKE '%-01-01T00:00:00.000Z' AND \"end\" LIKE '%-01-01T00:00:00.000Z') AS year_aligned_segments,\n"
        + "  SUM(\"size\") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS total_data_size,\n"
        + "  SUM(\"size\" * \"num_replicas\") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS replicated_size,\n"
        + "  MIN(\"num_rows\") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS min_segment_rows,\n"
        + "  AVG(\"num_rows\") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS avg_segment_rows,\n"
        + "  MAX(\"num_rows\") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS max_segment_rows,\n"
        + "  SUM(\"num_rows\") FILTER (WHERE (is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AS total_rows,\n"
        + "  CASE\n"
        + "    WHEN SUM(\"num_rows\") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) <> 0\n"
        + "    THEN (\n"
        + "      SUM(\"size\") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) /\n"
        + "      SUM(\"num_rows\") FILTER (WHERE is_published = 1 AND is_overshadowed = 0)\n"
        + "    )\n"
        + "    ELSE 0\n"
        + "  END AS avg_row_size\n"
        + "FROM sys.segments\n"
        + "GROUP BY 1\n"
        + "ORDER BY 1",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{9L}
        )
    );
  }

  @Test
  public void testSegmentsQueryFromWebConsoleSegmentsTabGroupByNone() throws Exception
  {
    testQuery(
        "WITH s AS (\n"
        + "  SELECT\n"
        + "    \"segment_id\", \"datasource\", \"start\", \"end\", \"size\", \"version\",\n"
        + "    CASE\n"
        + "      WHEN \"start\" LIKE '%-01-01T00:00:00.000Z' AND \"end\" LIKE '%-01-01T00:00:00.000Z' THEN 'Year'\n"
        + "      WHEN \"start\" LIKE '%-01T00:00:00.000Z' AND \"end\" LIKE '%-01T00:00:00.000Z' THEN 'Month'\n"
        + "      WHEN \"start\" LIKE '%T00:00:00.000Z' AND \"end\" LIKE '%T00:00:00.000Z' THEN 'Day'\n"
        + "      WHEN \"start\" LIKE '%:00:00.000Z' AND \"end\" LIKE '%:00:00.000Z' THEN 'Hour'\n"
        + "      WHEN \"start\" LIKE '%:00.000Z' AND \"end\" LIKE '%:00.000Z' THEN 'Minute'\n"
        + "      ELSE 'Sub minute'\n"
        + "    END AS \"time_span\",\n"
        + "    CASE\n"
        + "      WHEN \"shard_spec\" LIKE '%\"type\":\"numbered\"%' THEN 'dynamic'\n"
        + "      WHEN \"shard_spec\" LIKE '%\"type\":\"hashed\"%' THEN 'hashed'\n"
        + "      WHEN \"shard_spec\" LIKE '%\"type\":\"single\"%' THEN 'single_dim'\n"
        + "      WHEN \"shard_spec\" LIKE '%\"type\":\"none\"%' THEN 'none'\n"
        + "      WHEN \"shard_spec\" LIKE '%\"type\":\"linear\"%' THEN 'linear'\n"
        + "      WHEN \"shard_spec\" LIKE '%\"type\":\"numbered_overwrite\"%' THEN 'numbered_overwrite'\n"
        + "      ELSE '-'\n"
        + "    END AS \"partitioning\",\n"
        + "    \"partition_num\", \"num_replicas\", \"num_rows\",\n"
        + "    \"is_published\", \"is_available\", \"is_realtime\", \"is_overshadowed\"\n"
        + "  FROM sys.segments\n"
        + ")\n"
        + "SELECT *\n"
        + "FROM s\n"
        + "ORDER BY \"start\" DESC\n"
        + "LIMIT 25",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{9L}
        )
    );
  }

  @Test
  public void testSegmentsQueryFromWebConsoleSegmentsTabGroupByInterval() throws Exception
  {
    testQuery(
        "WITH s AS (\n"
        + "  SELECT\n"
        + "    \"segment_id\", \"datasource\", \"start\", \"end\", \"size\", \"version\",\n"
        + "    CASE\n"
        + "      WHEN \"start\" LIKE '%-01-01T00:00:00.000Z' AND \"end\" LIKE '%-01-01T00:00:00.000Z' THEN 'Year'\n"
        + "      WHEN \"start\" LIKE '%-01T00:00:00.000Z' AND \"end\" LIKE '%-01T00:00:00.000Z' THEN 'Month'\n"
        + "      WHEN \"start\" LIKE '%T00:00:00.000Z' AND \"end\" LIKE '%T00:00:00.000Z' THEN 'Day'\n"
        + "      WHEN \"start\" LIKE '%:00:00.000Z' AND \"end\" LIKE '%:00:00.000Z' THEN 'Hour'\n"
        + "      WHEN \"start\" LIKE '%:00.000Z' AND \"end\" LIKE '%:00.000Z' THEN 'Minute'\n"
        + "      ELSE 'Sub minute'\n"
        + "    END AS \"time_span\",\n"
        + "    CASE\n"
        + "      WHEN \"shard_spec\" LIKE '%\"type\":\"numbered\"%' THEN 'dynamic'\n"
        + "      WHEN \"shard_spec\" LIKE '%\"type\":\"hashed\"%' THEN 'hashed'\n"
        + "      WHEN \"shard_spec\" LIKE '%\"type\":\"single\"%' THEN 'single_dim'\n"
        + "      WHEN \"shard_spec\" LIKE '%\"type\":\"none\"%' THEN 'none'\n"
        + "      WHEN \"shard_spec\" LIKE '%\"type\":\"linear\"%' THEN 'linear'\n"
        + "      WHEN \"shard_spec\" LIKE '%\"type\":\"numbered_overwrite\"%' THEN 'numbered_overwrite'\n"
        + "      ELSE '-'\n"
        + "    END AS \"partitioning\",\n"
        + "    \"partition_num\", \"num_replicas\", \"num_rows\",\n"
        + "    \"is_published\", \"is_available\", \"is_realtime\", \"is_overshadowed\"\n"
        + "  FROM sys.segments\n"
        + ")\n"
        + "SELECT \"start\" || '/' || \"end\" AS \"interval\", *\n"
        + "FROM s\n"
        + "WHERE\n"
        + "  (\"start\" || '/' || \"end\") IN (\n"
        + "  SELECT \"start\" || '/' || \"end\" AS \"interval\"\n"
        + "  FROM sys.segments\n"
        + "  GROUP BY 1\n"
        + "  ORDER BY 1 DESC\n"
        + "  LIMIT 25\n"
        + ")\n"
        + "ORDER BY \"start\" DESC\n"
        + "LIMIT 25000",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{9L}
        )
    );
  }

  @Test
  public void testSegmentsFilterOnRegexp() throws Exception
  {
    testQuery(
        "SELECT segment_id FROM sys.segments WHERE REGEXP_LIKE(datasource, '^f.*')",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"foo_1970-01-01T00:00:00.000Z_2001-01-03T00:00:00.001Z_1"},
            new Object[]{"forbiddenDatasource_1970-01-01T00:00:00.000Z_2000-01-02T00:00:00.001Z_1"},
            new Object[]{"foo2_1970-01-01T00:00:00.000Z_2000-01-01T00:00:00.001Z_1"},
            new Object[]{"foo4_1970-01-01T00:00:00.000Z_2000-01-18T10:51:45.696Z_1"}
        )
    );
  }

  @Test
  public void testJoinSegmentsToServers() throws Exception
  {
    // Example query from the docs (querying/sql.md)

    testQuery(
        "SELECT count(segments.segment_id) as num_segments from sys.segments as segments\n"
        + "INNER JOIN sys.server_segments as server_segments\n"
        + "ON segments.segment_id  = server_segments.segment_id\n"
        + "INNER JOIN sys.servers as servers\n"
        + "ON servers.server = server_segments.server\n"
        + "WHERE segments.datasource = 'wikipedia'\n"
        + "GROUP BY servers.server",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"foo_1970-01-01T00:00:00.000Z_2001-01-03T00:00:00.001Z_1"},
            new Object[]{"forbiddenDatasource_1970-01-01T00:00:00.000Z_2000-01-02T00:00:00.001Z_1"},
            new Object[]{"foo2_1970-01-01T00:00:00.000Z_2000-01-01T00:00:00.001Z_1"},
            new Object[]{"foo4_1970-01-01T00:00:00.000Z_2000-01-18T10:51:45.696Z_1"}
        )
    );
  }

  @Test
  public void testJoinRegularTableToSegmentsTable() throws Exception
  {
    testQuery(
        "SELECT foo.m1, segment_id FROM foo inner join sys.segments ON foo.m1 = CHARACTER_LENGTH(segments.datasource)",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{3.0f, "foo_1970-01-01T00:00:00.000Z_2001-01-03T00:00:00.001Z_1"},
            new Object[]{4.0f, "foo2_1970-01-01T00:00:00.000Z_2000-01-01T00:00:00.001Z_1"},
            new Object[]{4.0f, "foo4_1970-01-01T00:00:00.000Z_2000-01-18T10:51:45.696Z_1"},
            new Object[]{6.0f, "numfoo_1970-01-01T00:00:00.000Z_2001-01-03T00:00:00.001Z_1"}
        )
    );
  }
}
