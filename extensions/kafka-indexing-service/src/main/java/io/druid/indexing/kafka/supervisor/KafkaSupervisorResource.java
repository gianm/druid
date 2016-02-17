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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.supervisor.IngestionSupervisor;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Endpoints for submitting a {@link KafkaSupervisorJob} to start a Kafka supervisor worker, getting running workers,
 * and stopping workers.
 */
@Path("/druid/indexer/v1/supervisor/kafka/job")
public class KafkaSupervisorResource
{
  private final TaskMaster taskMaster;

  @Inject
  public KafkaSupervisorResource(TaskMaster taskMaster)
  {
    this.taskMaster = taskMaster;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response jobPost(final KafkaSupervisorJob job)
  {
    return asLeaderWithKafkaSupervisor(
        new Function<IngestionSupervisor<KafkaSupervisorJob>, Response>()
        {
          @Override
          public Response apply(IngestionSupervisor supervisor)
          {
            if (supervisor.getWorker(job.getDataSchema().getDataSource()).isPresent()) {
              return Response.status(Response.Status.CONFLICT).entity(
                  ImmutableMap.of(
                      "error",
                      String.format(
                          "Supervisor already exists for dataSource [%s]",
                          job.getDataSchema().getDataSource()
                      )
                  )
              ).build();
            }

            supervisor.createWorker(job);
            return Response.ok(ImmutableMap.of("jobId", job.getDataSchema().getDataSource())).build();
          }
        }
    );
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response jobGetAll()
  {
    return asLeaderWithKafkaSupervisor(
        new Function<IngestionSupervisor<KafkaSupervisorJob>, Response>()
        {
          @Override
          public Response apply(IngestionSupervisor supervisor)
          {
            return Response.ok(supervisor.getWorkers().keySet()).build();
          }
        }
    );
  }

  @GET
  @Path("/{jobId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response jobGet(@PathParam("jobId") final String jobId)
  {
    return asLeaderWithKafkaSupervisor(
        new Function<IngestionSupervisor<KafkaSupervisorJob>, Response>()
        {
          @Override
          public Response apply(IngestionSupervisor<KafkaSupervisorJob> supervisor)
          {
            if (!supervisor.getWorker(jobId).isPresent() || !supervisor.getWorker(jobId).get().getJob().isPresent()) {
              return Response.status(Response.Status.NOT_FOUND)
                             .entity(ImmutableMap.of("error", String.format("jobId [%s] does not exist", jobId)))
                             .build();
            }

            return Response.ok(supervisor.getWorker(jobId).get().getJob().get()).build();
          }
        }
    );
  }

  @POST
  @Path("/{jobId}/shutdown")
  @Produces(MediaType.APPLICATION_JSON)
  public Response jobShutdown(@PathParam("jobId") final String jobId)
  {
    return asLeaderWithKafkaSupervisor(
        new Function<IngestionSupervisor<KafkaSupervisorJob>, Response>()
        {
          @Override
          public Response apply(IngestionSupervisor supervisor)
          {
            if (!supervisor.getWorker(jobId).isPresent()) {
              return Response.status(Response.Status.NOT_FOUND).entity(
                  ImmutableMap.of(
                      "error",
                      String.format(
                          "jobId [%s] does not exist",
                          jobId
                      )
                  )
              ).build();
            }

            supervisor.stopWorker(jobId);
            return Response.ok(ImmutableMap.of("jobId", jobId)).build();
          }
        }
    );
  }

  private Response asLeaderWithKafkaSupervisor(Function<IngestionSupervisor<KafkaSupervisorJob>, Response> f)
  {
    if (taskMaster.getSupervisor(KafkaSupervisor.class).isPresent()) {
      return f.apply(taskMaster.getSupervisor(KafkaSupervisor.class).get());
    } else {
      // Encourage client to try again soon, when we'll likely have a redirect set up
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
  }
}
