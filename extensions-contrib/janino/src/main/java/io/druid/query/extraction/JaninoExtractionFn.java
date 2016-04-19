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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import io.druid.janino.JaninoCompiler;

import java.util.Objects;

@JsonTypeName("janino")
public class JaninoExtractionFn extends DimExtractionFn
{
  private final String script;
  private final Object parameter;
  private final boolean injective;
  private final Function<Object[], String> fn;

  public JaninoExtractionFn(
      @JsonProperty("script") String script,
      @JsonProperty("parameter") Object parameter,
      @JsonProperty("injective") boolean injective,
      @JacksonInject JaninoCompiler compiler
  )
  {
    this.script = script;
    this.parameter = parameter;
    this.injective = injective;
    this.fn = compiler.compile(
        script,
        new String[]{"parameter", "value"},
        new Class[]{Object.class, String.class},
        String.class
    );
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
  public boolean isInjective()
  {
    return injective;
  }

  @Override
  public byte[] getCacheKey()
  {
    // TODO
    return new byte[0];
  }

  @Override
  public String apply(String value)
  {
    return fn.apply(new Object[]{parameter, value});
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return injective ? ExtractionType.ONE_TO_ONE : ExtractionType.MANY_TO_ONE;
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
    JaninoExtractionFn that = (JaninoExtractionFn) o;
    return injective == that.injective &&
           Objects.equals(script, that.script) &&
           Objects.equals(parameter, that.parameter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(script, parameter, injective);
  }

  @Override
  public String toString()
  {
    return "JaninoExtractionFn{" +
           "script='" + script + '\'' +
           ", parameter=" + parameter +
           ", injective=" + injective +
           '}';
  }
}
