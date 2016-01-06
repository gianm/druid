/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task.rttd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;

import java.util.List;
import java.util.Map;

public class SegmentAndRows
{
  private final SegmentIdentifier identifier;
  private final List<Map<String, Object>> rows;

  @JsonCreator
  public SegmentAndRows(
      @JsonProperty("identifier") SegmentIdentifier identifier,
      @JsonProperty("rows") List<Map<String, Object>> rows
  )
  {
    this.identifier = identifier;
    this.rows = rows;
  }

  @JsonProperty
  public SegmentIdentifier getIdentifier()
  {
    return identifier;
  }

  @JsonProperty
  public List<Map<String, Object>> getRows()
  {
    return rows;
  }

  public Iterable<InputRow> getInputRows(
      final Logger log,
      final InputRowParser<Map<String, Object>> parser
  )
  {
    return Iterables.transform(
        rows,
        new Function<Map<String, Object>, InputRow>()
        {
          @Override
          public InputRow apply(Map<String, Object> object)
          {
            try {
              return Preconditions.checkNotNull(parser.parse(object), "row");
            }
            catch (Exception e) {
              if (log.isDebugEnabled()) {
                log.debug(e, "Dropping unparseable row: %s", object);
              }
              return null;
            }
          }
        }
    );
  }
}
