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

package io.druid.segment.data;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.SkippableIntegerCODEC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class FastPforIntsSupplier implements WritableSupplier<ColumnarInts>
{
  private final ByteBuffer buffer;
  private final int numChunks;
  private final int numValues;
  private final int intsInChunk;
  private final int offsetsSize;

  private FastPforIntsSupplier(
      final ByteBuffer buffer,
      final int numChunks,
      final int numValues,
      final int intsInChunk,
      final int offsetsSize
  )
  {
    this.buffer = buffer;
    this.numChunks = numChunks;
    this.numValues = numValues;
    this.intsInChunk = intsInChunk;
    this.offsetsSize = offsetsSize;
  }

  public static FastPforIntsSupplier fromByteBuffer(final ByteBuffer buffer)
  {
    final ByteBuffer ourBuffer = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
    final int numChunks = ourBuffer.getInt(1);
    final int numValues = ourBuffer.getInt(1 + Ints.BYTES);
    final int intsInChunk = ourBuffer.getInt(1 + 2 * Ints.BYTES);
    final int offsetsSize = ourBuffer.getInt(1 + 3 * Ints.BYTES);
//    for (int i = 0; i < numChunks + 1; i++) {
//      System.out.println(i + "/" + numChunks + " : " +
//                         (ourBuffer.getInt(FastPforIntsSerializer.HEADER_BYTES
//                                           + Ints.BYTES * i) + FastPforIntsSerializer.HEADER_BYTES + offsetsSize));
//    }
    ourBuffer.limit(
        FastPforIntsSerializer.HEADER_BYTES + offsetsSize +
        ourBuffer.getInt(FastPforIntsSerializer.HEADER_BYTES + numChunks * Ints.BYTES)
    );
    buffer.position(buffer.position() + ourBuffer.remaining());
    return new FastPforIntsSupplier(
        ourBuffer.slice().order(ByteOrder.LITTLE_ENDIAN),
        numChunks,
        numValues,
        intsInChunk,
        offsetsSize
    );
  }

  @Override
  public ColumnarInts get()
  {
    // FastPforColumnarInts below currently assumes chunk size is 16384 ints
    Preconditions.checkState(intsInChunk == 16384);
    return new FastPforColumnarInts();
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return buffer.remaining();
  }

  @Override
  public void writeTo(
      final WritableByteChannel channel,
      final FileSmoosher smoosher
  ) throws IOException
  {
    throw new UnsupportedOperationException();
  }

  private class FastPforColumnarInts implements ColumnarInts
  {
    private int currentChunk = -1;
    private final int[] compressedInts = new int[intsInChunk + FastPforIntsSerializer.SHOULD_BE_ENOUGH];
    private final int[] decompressedInts = new int[intsInChunk];
    private final SkippableIntegerCODEC codec = FastPforIntsSerializer.makeCodec();

    @Override
    public int size()
    {
      return numValues;
    }

    @Override
    public int get(final int index)
    {
      final int desiredChunk = index >> 14;

      if (desiredChunk != currentChunk) {
        loadChunk(desiredChunk);
      }

      return decompressedInts[index & 16383];
    }

    @Override
    public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
    {
      // TODO(gianm): Something?
    }

    @Override
    public void close() throws IOException
    {
      // Nothing to close.
    }

    private void loadChunk(int desiredChunk)
    {
      Preconditions.checkArgument(
          desiredChunk < numChunks,
          "desiredChunk[%s] < numChunks[%s]",
          desiredChunk,
          numChunks
      );

      currentChunk = -1;

      // Determine chunk size.
      final int chunkStartReadFrom = FastPforIntsSerializer.HEADER_BYTES + Ints.BYTES * desiredChunk;
      final int chunkStartByte = buffer.getInt(chunkStartReadFrom) + FastPforIntsSerializer.HEADER_BYTES + offsetsSize;
      final int chunkEndByte = buffer.getInt(chunkStartReadFrom + Ints.BYTES)
                               + FastPforIntsSerializer.HEADER_BYTES
                               + offsetsSize;

      final int chunkNumValues;

      if (desiredChunk == numChunks - 1) {
        chunkNumValues = (numValues - ((numChunks - 1) * intsInChunk));
      } else {
        chunkNumValues = intsInChunk;
      }

      if ((chunkEndByte - chunkStartByte) % Ints.BYTES != 0) {
        throw new ISE(
            "Expected to read a whole number of integers, but got[%d] to [%d] for chunk[%d/%d]",
            chunkStartByte,
            chunkEndByte,
            desiredChunk,
            numChunks
        );
      }

      // Copy chunk into an int array.
      final int chunkNumCompressedInts = (chunkEndByte - chunkStartByte) / Ints.BYTES;
      for (int i = 0, bufferPos = chunkStartByte; i < chunkNumCompressedInts; i += 1, bufferPos += Ints.BYTES) {
        compressedInts[i] = buffer.getInt(bufferPos);
      }

      // Decompress the chunk.
      final IntWrapper inPos = new IntWrapper(0);
      final IntWrapper outPos = new IntWrapper(0);

      codec.headlessUncompress(
          compressedInts,
          inPos,
          (chunkEndByte - chunkStartByte) / Ints.BYTES,
          decompressedInts,
          outPos,
          chunkNumValues
      );

      // Sanity checks.
      if (inPos.get() != (chunkEndByte - chunkStartByte) / Ints.BYTES) {
        throw new ISE(
            "Expected to read[%d] ints but actually read[%d]",
            (chunkEndByte - chunkStartByte) / Ints.BYTES,
            inPos.get()
        );
      }

      if (outPos.get() != chunkNumValues) {
        throw new ISE("Expected to get[%d] ints but actually got[%d]", chunkNumValues, outPos.get());
      }

      currentChunk = desiredChunk;
    }
  }
}
