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

package io.druid.indexing.common.task.rttd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ShutDownResponse
{
  private final boolean success;
  private final long epoch;

  @JsonCreator
  public ShutDownResponse(
      @JsonProperty("success") boolean success,
      @JsonProperty("epoch") long epoch
  )
  {
    this.success = success;
    this.epoch = epoch;
  }

  @JsonProperty
  public boolean isSuccess()
  {
    return success;
  }

  @JsonProperty
  public long getEpoch()
  {
    return epoch;
  }
}
