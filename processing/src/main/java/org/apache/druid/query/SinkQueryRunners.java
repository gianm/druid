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

package org.apache.druid.query;

import org.apache.druid.java.util.common.Pair;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Runners for the segments of a data server, each paired with the interval of the segment it reads, and with a key for
 * the segment partition it reads. Mergers that order results by time, such as
 * {@link org.apache.druid.query.scan.ScanQueryRunnerFactory}, use these when runners do not correspond one-to-one with
 * the segments of a query: for example, when a realtime segment has several hydrants, or a clustered segment has
 * several cluster groups.
 */
public class SinkQueryRunners<T> implements Iterable<QueryRunner<T>>
{
  private final List<PartitionRunner<T>> runners;

  /**
   * Creates runners that each read a different segment partition, each paired with the interval of its segment.
   */
  public SinkQueryRunners(Iterable<Pair<Interval, QueryRunner<T>>> runners)
  {
    this(toPartitionRunners(runners));
  }

  private SinkQueryRunners(List<PartitionRunner<T>> runners)
  {
    this.runners = runners;
  }

  /**
   * Creates runners from {@code runners}, where runners that read parts of the same segment partition share a
   * partition key.
   */
  public static <T> SinkQueryRunners<T> fromPartitionRunners(List<PartitionRunner<T>> runners)
  {
    return new SinkQueryRunners<>(List.copyOf(runners));
  }

  public Iterator<Pair<Interval, QueryRunner<T>>> runnerIntervalMappingIterator()
  {
    return runners.stream().map(runner -> Pair.of(runner.interval(), runner.runner())).iterator();
  }

  /**
   * Returns the runners along with their intervals and partition keys.
   */
  public List<PartitionRunner<T>> getPartitionRunners()
  {
    return runners;
  }

  @Override
  public Iterator<QueryRunner<T>> iterator()
  {
    return runners.stream().map(PartitionRunner::runner).iterator();
  }

  private static <T> List<PartitionRunner<T>> toPartitionRunners(Iterable<Pair<Interval, QueryRunner<T>>> runners)
  {
    final List<PartitionRunner<T>> retVal = new ArrayList<>();
    for (final Pair<Interval, QueryRunner<T>> runner : runners) {
      // Use a new key for each runner, since the same runner instance may appear more than once.
      retVal.add(new PartitionRunner<>(runner.lhs, new Object(), runner.rhs));
    }
    return retVal;
  }

  /**
   * A runner, the interval of the segment it reads, and a key for the segment partition it reads. Runners that read
   * parts of the same segment partition, such as the cluster groups of one segment, have equal keys.
   */
  public record PartitionRunner<T>(Interval interval, Object partition, QueryRunner<T> runner)
  {
  }
}
