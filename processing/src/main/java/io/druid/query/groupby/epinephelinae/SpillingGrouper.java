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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.metamx.common.ByteBufferUtils;
import com.metamx.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class SpillingGrouper<KeyType> implements Grouper<KeyType>
{
  private static final Logger log = new Logger(SpillingGrouper.class);

  private final BufferGrouper<KeyType> grouper;
  private final KeySerde<KeyType> keySerde;
  private final File spillDirectory;
  private final String spillPrefix;
  private final List<File> files = Lists.newArrayList();
  private final List<MappedByteBuffer> mmaps = Lists.newArrayList();

  public SpillingGrouper(
      final ByteBuffer buffer,
      final KeySerde<KeyType> keySerde,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final File spillDirectory
  )
  {
    this.grouper = new BufferGrouper<>(
        buffer,
        keySerde,
        columnSelectorFactory,
        aggregatorFactories
    );
    this.keySerde = keySerde;
    this.spillDirectory = spillDirectory;
    this.spillPrefix = UUID.randomUUID().toString();
  }

  @Override
  public boolean aggregate(ByteBuffer key, int keyHash)
  {
    if (grouper.aggregate(key, keyHash)) {
      return true;
    } else {
      spill();
      return grouper.aggregate(key, keyHash);
    }
  }

  @Override
  public boolean aggregate(KeyType key)
  {
    final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);
    return aggregate(keyBuffer, Groupers.hash(keyBuffer));
  }

  @Override
  public void reset()
  {
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
  public Iterator<Entry<KeyType>> iterator(final boolean deserialize)
  {
    final List<Iterator<Entry<KeyType>>> iterators = new ArrayList<>(1 + files.size());

    iterators.add(grouper.iterator(deserialize));
    for (int fileNum = 0; fileNum < files.size(); fileNum++) {
      iterators.add(read(fileNum, deserialize));
    }

    return Groupers.mergeIterators(iterators, keySerde.comparator());
  }

  private void spill()
  {
    // TODO(gianm): Dictionary (DimDim) stays in-heap after spilling, probs should convert that somehow
    // TODO(gianm): Limit on disk use
    // TODO(gianm): Compress spill files?
    // TODO(gianm): Write aggregators in serialized form rather than raw memory form?

    final EnumSet<StandardOpenOption> openOptions = EnumSet.of(
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE
    );

    final File file = new File(spillDirectory, String.format("%s_%04d", spillPrefix, files.size()));
    files.add(file);

    try (final FileChannel out = FileChannel.open(file.toPath(), openOptions)) {
      final Iterator<Entry<KeyType>> it = grouper.iterator(false);
      while (it.hasNext()) {
        out.write(it.next().getBuffer());
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    grouper.reset();
  }

  private Iterator<Entry<KeyType>> read(final int fileNum, final boolean deserialize)
  {
    final File file = files.get(fileNum);

    if (mmaps.size() < fileNum + 1) {
      for (int i = mmaps.size(); i <= fileNum; i++) {
        try {
          mmaps.add(Files.map(file, FileChannel.MapMode.READ_ONLY));
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    }

    final MappedByteBuffer mmap = mmaps.get(fileNum);

    return new Iterator<Entry<KeyType>>()
    {
      int position = 0;

      @Override
      public boolean hasNext()
      {
        return position < mmap.capacity();
      }

      @Override
      public Entry<KeyType> next()
      {
        final ByteBuffer entryBuffer = mmap.duplicate();
        entryBuffer.position(position);
        entryBuffer.limit(position + grouper.getEntryBufferSize());
        position += grouper.getEntryBufferSize();
        return grouper.bucketEntryForBuffer(entryBuffer.slice(), deserialize);
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  private void deleteFiles()
  {
    for (int i = 0; i < files.size(); i++) {
      // I really hope you aren't iterating while you call close()...
      final MappedByteBuffer mmap = mmaps.get(i);
      if (mmap != null) {
        ByteBufferUtils.unmap(mmap);
      }

      final File file = files.get(i);
      if (!file.delete()) {
        log.warn("Could not delete file[%s]", file);
      }
    }

    files.clear();
  }
}
