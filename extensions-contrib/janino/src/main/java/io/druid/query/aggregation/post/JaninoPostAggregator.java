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

package io.druid.query.aggregation.post;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import io.druid.janino.JaninoCompiler;
import io.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class JaninoPostAggregator implements PostAggregator
{
  private final String name;
  private final List<String> fieldNames;
  private final String script;
  private final Object parameter;
  private final Function<Object[], Object> fn;

  public JaninoPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldNames") List<String> fieldNames,
      @JsonProperty("script") String script,
      @JsonProperty("parameter") Object parameter,
      @JacksonInject JaninoCompiler compiler
  )
  {
    this.name = name;
    this.fieldNames = fieldNames;
    this.script = script;
    this.parameter = parameter;
    this.fn = compiler.compile(
        script,
        new String[]{"parameter", "fields"},
        new Class[]{Object.class, Object[].class},
        Object.class
    );
  }

  @Override
  public Set<String> getDependentFields()
  {
    return ImmutableSet.copyOf(fieldNames);
  }

  @Override
  public Comparator getComparator()
  {
    return Ordering.natural().nullsFirst();
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    final Object[] fieldValues = new Object[fieldNames.size()];
    int i = 0;

    for (String field : fieldNames) {
      fieldValues[i++] = combinedAggregators.get(field);
    }

    return fn.apply(new Object[]{parameter, fieldValues});
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @JsonProperty
  public String getScript()
  {
    return script;
  }

  @JsonProperty
  public Object getParameter()
  {
    return parameter;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JaninoPostAggregator that = (JaninoPostAggregator) o;
    return Objects.equals(name, that.name) &&
           Objects.equals(fieldNames, that.fieldNames) &&
           Objects.equals(script, that.script) &&
           Objects.equals(parameter, that.parameter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldNames, script, parameter);
  }

  @Override
  public String toString()
  {
    return "JaninoPostAggregator{" +
           "parameter=" + parameter +
           ", script='" + script + '\'' +
           ", fieldNames=" + fieldNames +
           ", name='" + name + '\'' +
           '}';
  }
}
