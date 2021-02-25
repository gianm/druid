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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.column.RowSignature;

import java.util.Collections;
import java.util.List;
import java.util.Set;

@JsonTypeName("virtual")
public class VirtualDataSource implements DataSource
{
  private final InlineDataSource inlineDataSource;

  public VirtualDataSource(final Iterable<Object[]> rows, final RowSignature signature)
  {
    this.inlineDataSource = InlineDataSource.fromIterable(rows, signature);
  }

  @JsonCreator
  private static VirtualDataSource noDeserialization()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getTableNames()
  {
    return Collections.emptySet();
  }

  @Override
  public List<DataSource> getChildren()
  {
    return Collections.emptyList();
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (children.isEmpty()) {
      return this;
    }

    throw new IAE("Cannot accept children");
  }

  @Override
  public boolean isGlobal()
  {
    return false;
  }

  @Override
  public boolean isOnBroker()
  {
    return true;
  }

  @Override
  public boolean isConcrete()
  {
    return true;
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return false;
  }

  public InlineDataSource getInlineDataSource()
  {
    return inlineDataSource;
  }

  @Override
  public String toString()
  {
    return "[virtual]";
  }
}
