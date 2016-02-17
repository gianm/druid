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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import io.druid.guice.Jerseys;
import io.druid.guice.LazySingleton;

import java.util.HashSet;
import java.util.Set;

/**
 * Allows the registration of multiple supervisors that will be managed by TaskMaster's leader lifecycle and can be
 * accessed through TaskMaster.getSupervisor(clazz). Supervisors implement the {@link IngestionSupervisor} interface
 * and are intended to manage the creation and monitoring of indexing tasks.
 */
public class SupervisorModule implements Module
{
  private static Set<Class<? extends IngestionSupervisor>> supervisors = new HashSet<>();
  private static Set<Class> supervisorResources = new HashSet<>();

  public static void register(Class<? extends IngestionSupervisor> clazz)
  {
    supervisors.add(clazz);
  }

  public static void registerResource(Class clazz)
  {
    supervisorResources.add(clazz);
  }

  @Override
  public void configure(Binder binder)
  {
    Multibinder<IngestionSupervisor> multibinder = Multibinder.newSetBinder(binder, IngestionSupervisor.class);

    for (Class<? extends IngestionSupervisor> clazz : supervisors) {
      multibinder.addBinding().to(clazz);
    }

    for (Class clazz : supervisorResources) {
      Jerseys.addResource(binder, clazz);
    }
  }
}
