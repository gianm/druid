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

package org.apache.druid.msq.querykit.join;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.join.InlineJoinableFactory;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.JoinableFactory;

import java.util.Optional;

public class BroadcastJoinableFactory implements JoinableFactory
{
  /**
   * Map from input number -> inline data.
   */
  private final Int2ObjectMap<InlineDataSource> inlineDataSources;

  BroadcastJoinableFactory(final Int2ObjectMap<InlineDataSource> inlineDataSources)
  {
    this.inlineDataSources = inlineDataSources;
  }

  @Override
  public boolean isDirectlyJoinable(DataSource dataSource)
  {
    return dataSource instanceof InputNumberDataSource;
  }

  @Override
  public Optional<Joinable> build(DataSource dataSource, JoinConditionAnalysis condition)
  {
    final int inputNumber = ((InputNumberDataSource) dataSource).getInputNumber();
    final InlineDataSource inlineDataSource = inlineDataSources.get(inputNumber);

    if (inlineDataSource == null) {
      return Optional.empty();
    }

    return new InlineJoinableFactory().build(inlineDataSource, condition);
  }
}
