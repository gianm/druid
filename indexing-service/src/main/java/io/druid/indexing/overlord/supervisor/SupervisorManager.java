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
import com.metamx.common.Pair;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Manages the creation and lifetime of {@link Supervisor}.
 */
public class SupervisorManager
{
  private static final EmittingLogger log = new EmittingLogger(SupervisorManager.class);

  private final Map<String, Pair<Supervisor, SupervisorSpec>> supervisors = new HashMap<>();

  public Set<String> getSupervisors()
  {
    return supervisors.keySet();
  }

  public Optional<SupervisorSpec> getSupervisorSpec(String id)
  {
    return supervisors.get(id) == null
           ? Optional.<SupervisorSpec>absent()
           : Optional.fromNullable(supervisors.get(id).rhs);
  }

  public boolean hasSupervisor(String id)
  {
    return supervisors.containsKey(id);
  }

  public String createSupervisor(SupervisorSpec spec)
  {
    String id = spec.getId();
    if (!supervisors.keySet().contains(id)) {
      supervisors.put(id, Pair.of(spec.createSupervisor(), spec));
      supervisors.get(id).lhs.start();
    }

    return id;
  }

  public void stopSupervisor(String id)
  {
    Pair<Supervisor, SupervisorSpec> pair = supervisors.get(id);
    if (pair != null && pair.lhs != null) {
      pair.lhs.stop();
      supervisors.remove(id);
    }
  }

  @LifecycleStart
  public void start()
  {
    log.info("SupervisorManager started and is ready to accept jobs.");
  }

  @LifecycleStop
  public void stop()
  {
    for (String id : supervisors.keySet()) {
      supervisors.get(id).lhs.stop();
    }

    supervisors.clear();
    log.info("SupervisorManager stopped.");
  }
}
