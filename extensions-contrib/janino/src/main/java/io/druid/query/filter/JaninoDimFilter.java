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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.janino.JaninoCompiler;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.filter.JaninoFilter;

import java.util.Objects;

@JsonTypeName("janino")
public class JaninoDimFilter implements DimFilter
{
  private final String dimension;
  private final String script;
  private final Object parameter;
  private final ExtractionFn extractionFn;
  private final JaninoCompiler compiler;

  public JaninoDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("script") String script,
      @JsonProperty("parameter") Object parameter,
      @JsonProperty("extractionFn") ExtractionFn extractionFn,
      @JacksonInject JaninoCompiler compiler
  )
  {
    this.dimension = dimension;
    this.script = script;
    this.parameter = parameter;
    this.extractionFn = extractionFn;
    this.compiler = compiler;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
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

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    // TODO
    return new byte[0];
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new JaninoFilter(dimension, script, parameter, extractionFn, compiler);
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
    JaninoDimFilter that = (JaninoDimFilter) o;
    return Objects.equals(dimension, that.dimension) &&
           Objects.equals(script, that.script) &&
           Objects.equals(extractionFn, that.extractionFn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, script, extractionFn);
  }

  @Override
  public String toString()
  {
    return "JaninoDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", script='" + script + '\'' +
           ", extractionFn=" + extractionFn +
           '}';
  }
}
