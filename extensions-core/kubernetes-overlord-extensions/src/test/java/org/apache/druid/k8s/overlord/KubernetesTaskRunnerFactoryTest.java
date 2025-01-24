/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.k8s.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;
import org.apache.druid.k8s.overlord.taskadapter.TaskAdapter;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogs;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KubernetesTaskRunnerFactoryTest
{
  private ObjectMapper objectMapper;
  private KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig;
  private TaskLogs taskLogs;

  private DruidKubernetesClient druidKubernetesClient;
  @Mock private ServiceEmitter emitter;
  @Mock private TaskAdapter taskAdapter;

  @Before
  public void setup()
  {
    objectMapper = new TestUtils().getTestObjectMapper();
    kubernetesTaskRunnerConfig = KubernetesTaskRunnerConfig.builder()
        .withCapacity(1)
        .build();
    taskLogs = new NoopTaskLogs();
    druidKubernetesClient = new DruidKubernetesClient();
  }

  @Test
  public void test_get_returnsSameKuberentesTaskRunner_asBuild()
  {
    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        null,
        kubernetesTaskRunnerConfig,
        taskLogs,
        druidKubernetesClient,
        emitter,
        taskAdapter
    );

    KubernetesTaskRunner expectedRunner = factory.build();
    KubernetesTaskRunner actualRunner = factory.get();

    Assert.assertEquals(expectedRunner, actualRunner);
  }
}