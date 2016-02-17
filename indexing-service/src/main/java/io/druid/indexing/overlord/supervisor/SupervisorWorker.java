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

package io.druid.indexing.overlord.supervisor;

import com.google.common.base.Optional;
import com.metamx.common.concurrent.ScheduledExecutors;

import java.util.concurrent.Callable;

/**
 * A worker accepts a job in init() and should be managed by {@link ScheduledExecutors}. {@link #call()} should return
 * ScheduledExecutors.Signal.REPEAT until stop() is called, at which time it should return
 * ScheduledExecutors.Signal.STOP to end the run loop.
 */
public interface SupervisorWorker<T> extends Callable<ScheduledExecutors.Signal>
{
  void init(T job);

  void stop();

  @Override
  ScheduledExecutors.Signal call() throws Exception;

  Optional<T> getJob();
}
