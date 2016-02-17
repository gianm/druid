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

package io.druid.indexing.kafka.supervisor;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.overlord.supervisor.IngestionSupervisor;
import io.druid.indexing.overlord.supervisor.SupervisorWorker;
import org.joda.time.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages the creation and execution of {@link KafkaSupervisorWorker}.
 */
public class KafkaSupervisor implements IngestionSupervisor<KafkaSupervisorJob>
{
  private static final EmittingLogger log = new EmittingLogger(KafkaSupervisor.class);

  private final Provider<KafkaSupervisorWorker> workerProvider;
  private final ScheduledExecutorFactory executorFactory;
  private final KafkaSupervisorConfig config;

  private final AtomicBoolean started = new AtomicBoolean();
  private final Map<String, SupervisorWorker> workers = new HashMap<>();

  private ScheduledExecutorService executorService;

  @Inject
  public KafkaSupervisor(
      Provider<KafkaSupervisorWorker> workerProvider,
      ScheduledExecutorFactory executorFactory,
      KafkaSupervisorConfig config
  )
  {
    this.workerProvider = workerProvider;
    this.executorFactory = executorFactory;
    this.config = config;
  }

  @Override
  public Map<String, SupervisorWorker> getWorkers()
  {
    return workers;
  }

  @Override
  public Optional<SupervisorWorker> getWorker(String jobId)
  {
    return Optional.fromNullable(workers.get(jobId));
  }

  @Override
  public String createWorker(KafkaSupervisorJob job)
  {
    if (!started.get()) {
      throw new IllegalStateException("Supervisor must be started before it can accept jobs.");
    }

    String dataSource = job.getDataSchema().getDataSource();
    if (!workers.keySet().contains(dataSource)) {
      SupervisorWorker worker = workerProvider.get();
      workers.put(dataSource, worker);
      worker.init(job);

      ScheduledExecutors.scheduleWithFixedDelay(executorService, Duration.ZERO, config.getPeriod(), worker);
    }

    return dataSource;
  }

  @Override
  public void stopWorker(String jobId)
  {
    SupervisorWorker worker = workers.get(jobId);
    if (worker != null) {
      worker.stop();
      workers.remove(jobId);
    }
  }

  @LifecycleStart
  @Override
  public void start()
  {
    if (started.getAndSet(true)) {
      return;
    }

    executorService = executorFactory.create(config.getNumThreads(), "KafkaSupervisorWorker-%d");
    log.info(
        "KafkaSupervisor started with monitorPeriod [%s] / numThreads [%d] and is ready to accept jobs.",
        config.getPeriod().toString(),
        config.getNumThreads()
    );
  }

  @LifecycleStop
  @Override
  public void stop()
  {
    if (!started.getAndSet(false)) {
      return;
    }

    workers.clear();
    executorService.shutdownNow();
    log.info("KafkaSupervisor stopped.");
  }
}
