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

import com.fasterxml.jackson.annotation.JsonProperty;

public class JaninoCompilerConfig
{
  @JsonProperty
  private boolean disable = false;

  @JsonProperty
  private boolean cache = false;

  @JsonProperty
  private int cacheMaximumSize = 100;

  @JsonProperty
  private long cacheExpireAfterAccess = 90 * 60_000;

  @JsonProperty
  private int cacheConcurrencyLevel = 4;

  public boolean isDisable()
  {
    return disable;
  }

  public boolean isCache()
  {
    return cache;
  }

  public int getCacheMaximumSize()
  {
    return cacheMaximumSize;
  }

  public long getCacheExpireAfterAccess()
  {
    return cacheExpireAfterAccess;
  }

  public int getCacheConcurrencyLevel()
  {
    return cacheConcurrencyLevel;
  }
}
