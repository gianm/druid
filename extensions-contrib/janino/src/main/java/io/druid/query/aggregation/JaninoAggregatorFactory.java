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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import io.druid.janino.JaninoCompiler;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.ValueType;
import org.codehaus.janino.ScriptEvaluator;

import java.util.Comparator;
import java.util.List;

public class JaninoAggregatorFactory extends AggregatorFactory
{
  private final ValueType valueType;
  private final String name;
  private final List<String> fieldNames;
  private final String fnAggregate;
  private final String fnReset;
  private final String fnCombine;
  private final Object parameter;

  private final ScriptHolder scriptHolder;

  public JaninoAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("valueType") final String valueType,
      @JsonProperty("fnAggregate") final String fnAggregate,
      @JsonProperty("fnReset") final String fnReset,
      @JsonProperty("fnCombine") final String fnCombine,
      @JsonProperty("parameter") final Object parameter,
      @JacksonInject JaninoCompiler compiler
  )
  {
    this.valueType = ValueType.valueOf(valueType.toUpperCase());
    this.name = name;
    this.fieldNames = fieldNames;
    this.fnAggregate = fnAggregate;
    this.fnReset = fnReset;
    this.fnCombine = fnCombine;
    this.parameter = parameter;
//    this.scriptHolder = new ScriptHolder(fnAggregate, fnCombine, fnReset);
    this.scriptHolder = null;
  }

  public static class ScriptHolder
  {
    private final Function<Object[], Object> aggregate;
    private final Function<Object[], Object> combine;
    private final Function<Object[], Object> reset;

    public ScriptHolder(
        Function<Object[], Object> aggregate,
        Function<Object[], Object> combine,
        Function<Object[], Object> reset
    )
    {
      this.aggregate = aggregate;
      this.combine = combine;
      this.reset = reset;
    }
  }

  @JsonProperty
  public String getValueType()
  {
    return valueType.toString().toLowerCase();
  }

  @JsonProperty
  public Object getParameter()
  {
    return parameter;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return null;
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return null;
  }

  @Override
  public Comparator getComparator()
  {
    return null;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return null;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return null;
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return null;
  }

  @Override
  public Object deserialize(Object object)
  {
    return null;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return null;
  }

  @Override
  public String getName()
  {
    return null;
  }

  @Override
  public List<String> requiredFields()
  {
    return null;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[0];
  }

  @Override
  public String getTypeName()
  {
    return null;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return 0;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return null;
  }
}
