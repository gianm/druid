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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.janino.JaninoCompiler;
import io.druid.janino.JaninoCompilerConfig;
import io.druid.janino.JaninoModule;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class JaninoExtractionFnTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final JaninoCompiler compiler = new JaninoCompiler(new JaninoCompilerConfig());
    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(JaninoCompiler.class, compiler)
    );
    objectMapper.registerModules((List<Module>) new JaninoModule().getJacksonModules());

    final JaninoExtractionFn extractionFn = new JaninoExtractionFn(
        "return String.valueOf(parameter);",
        "abc",
        false,
        compiler
    );

    final ExtractionFn extractionFn2 = objectMapper.readValue(
        objectMapper.writeValueAsBytes(extractionFn),
        ExtractionFn.class
    );

    Assert.assertEquals(extractionFn, extractionFn2);
  }
}
