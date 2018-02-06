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

package io.druid.benchmark;

import io.druid.java.util.common.io.Closer;
import io.druid.segment.data.ColumnarInts;
import io.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.FastPforIntsSerializer;
import io.druid.segment.data.FastPforIntsSupplier;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.VSizeColumnarInts;
import io.druid.segment.data.WritableSupplier;
import io.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import io.druid.segment.writeout.SegmentWriteOutMedium;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
public class CompressedColumnarIntsBenchmark
{
  private IndexedInts uncompressed;
  private IndexedInts compressed;
  private IndexedInts fastPfor;

  @Param({"15"})
  int bits;

  @Param({"3000000"})
  int rows;

  // Number of rows to read, the test will read random rows
  @Param({"300000", "2850000", "3000000"})
  int filteredRowCount;

  private BitSet filter;

  @Setup
  public void setup() throws IOException
  {
    Random rand = new Random(0);
    int[] vals = new int[rows];
    final int bound = 1 << bits;
    for (int i = 0; i < vals.length; ++i) {
      vals[i] = rand.nextInt(bound);
    }
    final ByteBuffer bufferCompressed = serialize(
        CompressedVSizeColumnarIntsSupplier.fromList(
            IntArrayList.wrap(vals),
            bound - 1,
            CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(bound),
            ByteOrder.nativeOrder(),
            CompressionStrategy.LZ4,
            Closer.create()
        )
    );
    System.out.println("compressed size = " + bufferCompressed.remaining());
    this.compressed = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
        bufferCompressed,
        ByteOrder.nativeOrder()
    ).get();

    final ByteBuffer bufferUncompressed = serialize(VSizeColumnarInts.fromArray(vals));
    System.out.println("uncompressed size = " + bufferUncompressed.remaining());
    this.uncompressed = VSizeColumnarInts.readFromByteBuffer(bufferUncompressed);

    final SegmentWriteOutMedium segmentWriteOutMedium = new OnHeapMemorySegmentWriteOutMedium();
    final FastPforIntsSerializer fastPforSerializer = new FastPforIntsSerializer(segmentWriteOutMedium, 1 << 14);
    fastPforSerializer.open();
    for (int val : vals) {
      fastPforSerializer.add(val);
    }
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    fastPforSerializer.writeTo(Channels.newChannel(baos), null);
    final byte[] fastPforBytes = baos.toByteArray();
    System.out.println("fastPfor size = " + fastPforBytes.length);
    this.fastPfor = FastPforIntsSupplier.fromByteBuffer(ByteBuffer.wrap(fastPforBytes)).get();

    if (filteredRowCount < rows) {
      filter = new BitSet();
      for (int i = 0; i < filteredRowCount; i++) {
        int rowToAccess = rand.nextInt(vals.length);
        // Skip already selected rows if any
        while (filter.get(rowToAccess)) {
          rowToAccess = (rowToAccess + 1) % vals.length;
        }
        filter.set(rowToAccess);
      }
    } else {
      filter = null;
    }

  }

  private static ByteBuffer serialize(WritableSupplier<ColumnarInts> writableSupplier) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocateDirect((int) writableSupplier.getSerializedSize());

    WritableByteChannel channel = new WritableByteChannel()
    {
      @Override
      public int write(ByteBuffer src) throws IOException
      {
        int size = src.remaining();
        buffer.put(src);
        return size;
      }

      @Override
      public boolean isOpen()
      {
        return true;
      }

      @Override
      public void close() throws IOException
      {
      }
    };

    writableSupplier.writeTo(channel, null);
    buffer.rewind();
    return buffer;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void uncompressed(Blackhole blackhole)
  {
    if (filter == null) {
      for (int i = 0; i < rows; i++) {
        blackhole.consume(uncompressed.get(i));
      }
    } else {
      for (int i = filter.nextSetBit(0); i >= 0; i = filter.nextSetBit(i + 1)) {
        blackhole.consume(uncompressed.get(i));
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void compressed(Blackhole blackhole)
  {
    if (filter == null) {
      for (int i = 0; i < rows; i++) {
        blackhole.consume(compressed.get(i));
      }
    } else {
      for (int i = filter.nextSetBit(0); i >= 0; i = filter.nextSetBit(i + 1)) {
        blackhole.consume(compressed.get(i));
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void fastPfor(Blackhole blackhole)
  {
    if (filter == null) {
      for (int i = 0; i < rows; i++) {
        blackhole.consume(fastPfor.get(i));
      }
    } else {
      for (int i = filter.nextSetBit(0); i >= 0; i = filter.nextSetBit(i + 1)) {
        blackhole.consume(fastPfor.get(i));
      }
    }
  }
}
