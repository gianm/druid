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

package io.druid.query.groupby.epinephelinae;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class SpillingGrouper<KeyType extends Comparable<KeyType>> implements Grouper<KeyType>
{
  private static final Logger log = new Logger(SpillingGrouper.class);

  private final BufferGrouper<KeyType> grouper;
  private final KeySerde<KeyType> keySerde;
  private final File spillDirectory;
  private final String spillPrefix;
  private final ObjectMapper spillMapper;
  private final int spillEvery;
  private final AggregatorFactory[] aggregatorFactories;

  private final List<File> files = Lists.newArrayList();
  private final List<Closeable> closeables = Lists.newArrayList();

  private int rowsAdded = 0;

  public SpillingGrouper(
      final ByteBuffer buffer,
      final KeySerdeFactory<KeyType> keySerdeFactory,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final File spillDirectory,
      final ObjectMapper spillMapper,
      final int spillEvery
  )
  {
    this.keySerde = keySerdeFactory.factorize();
    this.grouper = new BufferGrouper<>(
        buffer,
        keySerde,
        columnSelectorFactory,
        aggregatorFactories
    );
    this.aggregatorFactories = aggregatorFactories;
    this.spillDirectory = spillDirectory;
    this.spillMapper = spillMapper;
    this.spillPrefix = UUID.randomUUID().toString();
    this.spillEvery = spillEvery;
  }

  @Override
  public boolean aggregate(KeyType key, int keyHash)
  {
    if (rowsAdded < spillEvery && grouper.aggregate(key, keyHash)) {
      rowsAdded++;
      return true;
    } else {
      spill();
      rowsAdded = 0;
      return grouper.aggregate(key, keyHash);
    }
  }

  @Override
  public boolean aggregate(KeyType key)
  {
    return aggregate(key, Groupers.hash(key));
  }

  @Override
  public void reset()
  {
    rowsAdded = 0;
    grouper.reset();
    deleteFiles();
  }

  @Override
  public void close()
  {
    grouper.close();
    deleteFiles();
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    final List<Iterator<Entry<KeyType>>> iterators = new ArrayList<>(1 + files.size());

    iterators.add(grouper.iterator(sorted));

    for (final File file : files) {
      final MappingIterator<Entry<KeyType>> fileIterator = read(file, keySerde.keyClazz());
      iterators.add(
          Iterators.transform(
              fileIterator,
              new Function<Entry<KeyType>, Entry<KeyType>>()
              {
                @Override
                public Entry<KeyType> apply(Entry<KeyType> entry)
                {
                  final Object[] deserializedValues = new Object[entry.getValues().length];
                  for (int i = 0; i < deserializedValues.length; i++) {
                    deserializedValues[i] = aggregatorFactories[i].deserialize(entry.getValues()[i]);
                    if (deserializedValues[i] instanceof Integer) {
                      // TODO(gianm): This is a hack to satisfy the groupBy tests, maybe we can do better.
                      deserializedValues[i] = ((Integer) deserializedValues[i]).longValue();
                    }
                  }
                  return new Entry<>(entry.getKey(), deserializedValues);
                }
              }
          )
      );
      closeables.add(fileIterator);
    }

    return Groupers.mergeIterators(iterators, sorted);
  }

  private void spill()
  {
    // TODO(gianm): Limit on disk use
    // TODO(gianm): Compress spill files?

    final EnumSet<StandardOpenOption> openOptions = EnumSet.of(
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE
    );

    final File file = new File(spillDirectory, String.format("%s_%04d", spillPrefix, files.size()));
    files.add(file);

    try (
        final FileChannel channel = FileChannel.open(file.toPath(), openOptions);
        final OutputStream out = Channels.newOutputStream(channel);
        final JsonGenerator jsonGenerator = spillMapper.getFactory().createGenerator(out)
    ) {
      final Iterator<Entry<KeyType>> it = grouper.iterator(true);
      while (it.hasNext()) {
        jsonGenerator.writeObject(it.next());
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    grouper.reset();
  }

  private MappingIterator<Entry<KeyType>> read(final File file, final Class<KeyType> keyClazz)
  {
    try {
      return spillMapper.readValues(
          spillMapper.getFactory().createParser(file),
          spillMapper.getTypeFactory().constructParametricType(Entry.class, keyClazz)
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void deleteFiles()
  {
    for (Closeable closeable : closeables) {
      // CloseQuietly is OK on readable streams
      CloseQuietly.close(closeable);
    }
    for (final File file : files) {
      if (!file.delete()) {
        log.warn("Could not delete file[%s]", file);
      }
    }

    files.clear();
  }
}
