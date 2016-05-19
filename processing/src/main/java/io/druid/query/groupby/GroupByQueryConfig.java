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

package io.druid.query.groupby;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class GroupByQueryConfig
{
  @JsonProperty
  private String defaultStrategy = "oldFaithful";

  @JsonProperty
  private boolean singleThreaded = false;

  @JsonProperty
  private int maxIntermediateRows = 50000;

  @JsonProperty
  private int maxResults = 500000;

  @JsonProperty
  // Not documented, only used for tests to force spilling
  private int maxBufferGrouperSize = Integer.MAX_VALUE;

  @JsonProperty
  // Size of on-heap string dictionary for merging, per-query; when exceeded, partial results will be spilled to disk
  private long maxMergingDictionarySize = 25 * 1024 * 1024;

  @JsonProperty
  // Max on-disk temporary storage, per-query; when exceeded, the query fails
  private long maxOnDiskStorage = 1024 * 1024 * 1024;

  public String getDefaultStrategy()
  {
    return defaultStrategy;
  }

  public boolean isSingleThreaded()
  {
    return singleThreaded;
  }

  public void setSingleThreaded(boolean singleThreaded)
  {
    this.singleThreaded = singleThreaded;
  }

  public int getMaxIntermediateRows()
  {
    return maxIntermediateRows;
  }

  public void setMaxIntermediateRows(int maxIntermediateRows)
  {
    this.maxIntermediateRows = maxIntermediateRows;
  }

  public int getMaxResults()
  {
    return maxResults;
  }

  public int getMaxBufferGrouperSize()
  {
    return maxBufferGrouperSize;
  }

  public long getMaxMergingDictionarySize()
  {
    return maxMergingDictionarySize;
  }

  public long getMaxOnDiskStorage()
  {
    return maxOnDiskStorage;
  }

  public void setMaxResults(int maxResults)
  {
    this.maxResults = maxResults;
  }
}
