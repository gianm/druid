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

import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.JoinableFactory;

import java.util.Optional;

/**
 * TODO(gianm); javadoc
 */
public class DecoratedJoinableFactory implements JoinableFactory
{
  private final JoinableFactory baseJoinableFactory;
  private final BroadcastJoinableFactory broadcastJoinableFactory;

  DecoratedJoinableFactory(
      final JoinableFactory baseJoinableFactory,
      final BroadcastJoinableFactory broadcastJoinableFactory
  )
  {
    this.baseJoinableFactory = baseJoinableFactory;
    this.broadcastJoinableFactory = broadcastJoinableFactory;
  }

  @Override
  public boolean isDirectlyJoinable(DataSource dataSource)
  {
    return dataSource instanceof InputNumberDataSource || baseJoinableFactory.isDirectlyJoinable(dataSource);
  }

  @Override
  public Optional<Joinable> build(DataSource dataSource, JoinConditionAnalysis condition)
  {
    if (dataSource instanceof InputNumberDataSource) {
      return broadcastJoinableFactory.build(dataSource, condition);
    } else {
      return baseJoinableFactory.build(dataSource, condition);
    }
  }
}
