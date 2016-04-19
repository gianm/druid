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

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import io.druid.janino.JaninoCompiler;
import io.druid.query.extraction.ExtractionFn;

public class JaninoFilter extends DimensionPredicateFilter
{
  public JaninoFilter(
      String dimension,
      String script,
      Object parameter,
      ExtractionFn extractionFn,
      JaninoCompiler compiler
  )
  {
    super(dimension, makePredicate(compiler, script, parameter), extractionFn);
  }

  private static Predicate<String> makePredicate(
      final JaninoCompiler compiler,
      final String script,
      final Object parameter
  )
  {
    final Function<Object[], Boolean> fn = compiler.compile(
        script,
        new String[]{"parameter", "value"},
        new Class[]{Object.class, String.class},
        boolean.class
    );
    return new Predicate<String>()
    {
      @Override
      public boolean apply(final String value)
      {
        return fn.apply(new Object[]{parameter, value});
      }
    };
  }
}
