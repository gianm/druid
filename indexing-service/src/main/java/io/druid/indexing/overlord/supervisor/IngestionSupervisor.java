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
import com.google.inject.Binder;

import java.util.Map;

/**
 * Supervisors manage a set of workers that are responsible for the creation and monitoring of certain types of
 * indexing tasks. Implementations of this interface must be registered with {@link SupervisorModule#register(Class)}
 * so that they will be added to the overlord's leader lifecycle. Make sure the registration happens before
 * {@link SupervisorModule#configure(Binder)} is called.
 */
public interface IngestionSupervisor<T>
{
  void start();

  void stop();

  Map<String, SupervisorWorker> getWorkers();

  Optional<SupervisorWorker> getWorker(String jobId);

  String createWorker(T job);

  void stopWorker(String jobId);
}
