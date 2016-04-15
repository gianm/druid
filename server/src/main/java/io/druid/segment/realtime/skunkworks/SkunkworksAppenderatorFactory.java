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

package io.druid.segment.realtime.skunkworks;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.druid.guice.annotations.Processing;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.Appenderator;

import java.util.concurrent.ExecutorService;

public class SkunkworksAppenderatorFactory implements AppenderatorFactory
{
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ExecutorService queryExecutorService;

  @JsonCreator
  public SkunkworksAppenderatorFactory(
      @JacksonInject QueryRunnerFactoryConglomerate conglomerate,
      @JacksonInject @Processing ExecutorService queryExecutorService
  )
  {
    this.conglomerate = conglomerate;
    this.queryExecutorService = queryExecutorService;
  }

  @Override
  public Appenderator build(
      DataSchema schema,
      RealtimeTuningConfig config,
      FireDepartmentMetrics metrics
  )
  {
    return new SkunkworksAppenderator(schema, conglomerate, queryExecutorService);
  }
}
