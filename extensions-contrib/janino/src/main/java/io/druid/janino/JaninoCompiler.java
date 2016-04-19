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

package io.druid.janino;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.metamx.common.ISE;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ScriptEvaluator;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class JaninoCompiler
{
  private final Cache<CacheKey, ScriptEvaluator> cache;
  private final JaninoCompilerConfig config;

  public JaninoCompiler(JaninoCompilerConfig config)
  {
    this.config = config;

    if (!config.isDisable() && config.isCache()) {
      cache = CacheBuilder.newBuilder()
                          .maximumSize(config.getCacheMaximumSize())
                          .expireAfterAccess(config.getCacheExpireAfterAccess(), TimeUnit.MILLISECONDS)
                          .concurrencyLevel(config.getCacheConcurrencyLevel())
                          .build();
    } else {
      cache = null;
    }
  }

  /**
   * Compiles "script" into an evaluator that accepts parameters given by "parameterNames" and "parameterTypes",
   * and returns a type given by "returnType". Returns null if compiling is disabled.
   *
   * @param script         text of the function
   * @param parameterNames parameters to the function
   * @param parameterTypes parameter types, should be the same length as parameterNames
   * @param returnType     return type
   *
   * @return compiled function
   */
  public <T> Function<Object[], T> compile(
      final String script,
      final String[] parameterNames,
      final Class[] parameterTypes,
      final Class<T> returnType
  )
  {
    Preconditions.checkNotNull(script, "script");
    Preconditions.checkNotNull(parameterNames, "parameterNames");
    Preconditions.checkNotNull(parameterTypes, "parameterTypes");
    Preconditions.checkNotNull(returnType, "returnType");

    Preconditions.checkArgument(
        parameterNames.length == parameterTypes.length,
        "parameterNames and parameterTypes must be the same length"
    );

    if (config.isDisable()) {
      return null;
    }

    final ScriptEvaluator evaluator;

    if (cache == null) {
      evaluator = compile0(script, parameterNames, parameterTypes, returnType);
    } else {
      try {
        evaluator = cache.get(
            new CacheKey(script, parameterNames, parameterTypes, returnType),
            new Callable<ScriptEvaluator>()
            {
              @Override
              public ScriptEvaluator call()
              {
                return compile0(script, parameterNames, parameterTypes, returnType);
              }
            }
        );
      }
      catch (ExecutionException e) {
        throw Throwables.propagate(e);
      }
    }

    return new Function<Object[], T>()
    {
      @Override
      public T apply(Object[] input)
      {
        try {
          return (T) evaluator.evaluate(input);
        }
        catch (InvocationTargetException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private ScriptEvaluator compile0(
      final String script,
      final String[] parameterNames,
      final Class[] parameterTypes,
      final Class returnType
  )
  {
    try {
      final ScriptEvaluator scriptEvaluator = new ScriptEvaluator();
      scriptEvaluator.setParameters(parameterNames, parameterTypes);
      scriptEvaluator.setReturnType(returnType);
      scriptEvaluator.cook(script);
      return scriptEvaluator;
    }
    catch (CompileException e) {
      throw new ISE(e, "Failed to compile script");
    }
  }

  private static class CacheKey
  {
    private final String script;
    private final String[] parameterNames;
    private final Class[] parameterTypes;
    private final Class returnType;

    CacheKey(String script, String[] parameterNames, Class[] parameterTypes, Class returnType)
    {
      this.script = script;
      this.parameterNames = parameterNames;
      this.parameterTypes = parameterTypes;
      this.returnType = returnType;
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
      CacheKey cacheKey = (CacheKey) o;
      return Objects.equals(script, cacheKey.script) &&
             Arrays.equals(parameterNames, cacheKey.parameterNames) &&
             Arrays.equals(parameterTypes, cacheKey.parameterTypes) &&
             Objects.equals(returnType, cacheKey.returnType);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(script, parameterNames, parameterTypes, returnType);
    }
  }
}
