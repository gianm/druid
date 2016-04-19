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

package io.druid.query.aggregation;

import io.druid.segment.column.ValueType;

public class JaninoAggregator implements Aggregator
{
  private final ValueType valueType;
  private final String script;
  private final Object parameter;

  public JaninoAggregator(ValueType valueType, String script, Object parameter)
  {
    this.valueType = valueType;
    this.script = script;
    this.parameter = parameter;
  }

  @Override
  public void aggregate()
  {

  }

  @Override
  public void reset()
  {

  }

  @Override
  public Object get()
  {
    return null;
  }

  @Override
  public float getFloat()
  {
    return 0;
  }

  @Override
  public String getName()
  {
    return null;
  }

  @Override
  public void close()
  {

  }

  @Override
  public long getLong()
  {
    return 0;
  }
}
