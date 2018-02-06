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
import io.druid.segment.writeout.SegmentWriteOutMedium;
import io.druid.segment.writeout.WriteOutBytes;
import me.lemire.integercompression.BinaryPacking;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.JustCopy;
import me.lemire.integercompression.Simple16;
import me.lemire.integercompression.SkippableComposition;
import me.lemire.integercompression.SkippableIntegerCODEC;
import me.lemire.integercompression.VariableByte;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class FastPforIntsSerializer extends SingleValueColumnarIntsSerializer
{
  static final int HEADER_BYTES = 1 + 4 * Ints.BYTES;

  // Straight from the horse's mouth (https://github.com/lemire/JavaFastPFOR/blob/master/example.java).
  static final int SHOULD_BE_ENOUGH = 1024;

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final SkippableIntegerCODEC codec;
  private final int intsInChunk;
  private final ByteBuffer intToBytesHelperBuffer = ByteBuffer.allocate(Ints.BYTES).order(ByteOrder.LITTLE_ENDIAN);

  private WriteOutBytes offsetsOut;
  private WriteOutBytes valuesOut;
  private int[] currentChunk;
  private int[] compressedIntsTmp;
  private int currentChunkPos = 0;
  private int numChunks = 0;
  private int numValues = 0;
  private boolean wroteFinalOffset = false;

  public FastPforIntsSerializer(final SegmentWriteOutMedium segmentWriteOutMedium, final int intsInChunk)
  {
    this.segmentWriteOutMedium = Preconditions.checkNotNull(segmentWriteOutMedium, "segmentWriteOutMedium");
    this.intsInChunk = intsInChunk;
    this.codec = makeCodec();
  }

  static SkippableIntegerCODEC makeCodec()
  {
    return new SkippableComposition(new BinaryPacking(), new VariableByte());
//    return new Simple16();
  }

  @Override
  public void open() throws IOException
  {
    offsetsOut = segmentWriteOutMedium.makeWriteOutBytes();
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
    currentChunk = new int[intsInChunk];
    compressedIntsTmp = new int[intsInChunk + SHOULD_BE_ENOUGH];
  }

  @Override
  protected void addValue(final int val) throws IOException
  {
    if (currentChunkPos == intsInChunk) {
      flushCurrentChunk();
    }

    currentChunk[currentChunkPos++] = val;
    numValues++;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    if (currentChunkPos > 0) {
      flushCurrentChunk();
    }

    writeFinalOffset();

    return HEADER_BYTES + offsetsOut.size() + valuesOut.size();
  }

  @Override
  public void writeTo(
      final WritableByteChannel channel,
      final FileSmoosher smoosher
  ) throws IOException
  {
    // TODO(gianm): version probably. something about the codec, so we can use different codecs in different cases.

    if (currentChunkPos > 0) {
      flushCurrentChunk();
    }

    writeFinalOffset();

    channel.write(ByteBuffer.wrap(new byte[]{0}));
    channel.write(toBytes(numChunks));
    channel.write(toBytes(numValues));
    channel.write(toBytes(intsInChunk));
    channel.write(toBytes(Ints.checkedCast(offsetsOut.size())));

    offsetsOut.writeTo(channel);
    valuesOut.writeTo(channel);
  }

  private void flushCurrentChunk() throws IOException
  {
    Preconditions.checkState(!wroteFinalOffset, "!wroteFinalOffset");
    Preconditions.checkState(currentChunkPos > 0, "currentChunkPos > 0");
    Preconditions.checkState(offsetsOut.isOpen(), "offsetsOut.isOpen");
    Preconditions.checkState(valuesOut.isOpen(), "valuesOut.isOpen");

    final IntWrapper inPos = new IntWrapper(0);
    final IntWrapper outPos = new IntWrapper(0);

    codec.headlessCompress(currentChunk, inPos, currentChunkPos, compressedIntsTmp, outPos);

    if (inPos.get() != currentChunkPos) {
      throw new ISE("Expected to compress[%d] ints, but read[%d]", currentChunkPos, inPos.get());
    }

    offsetsOut.write(toBytes(Ints.checkedCast(valuesOut.size())));

    for (int i = 0; i < outPos.get(); i++) {
      valuesOut.write(toBytes(compressedIntsTmp[i]));
    }

    numChunks++;
    currentChunkPos = 0;
  }

  private void writeFinalOffset() throws IOException
  {
    if (!wroteFinalOffset) {
      offsetsOut.write(toBytes(Ints.checkedCast(valuesOut.size())));
      wroteFinalOffset = true;
    }
  }

  private ByteBuffer toBytes(final int n)
  {
    intToBytesHelperBuffer.putInt(0, n);
    intToBytesHelperBuffer.rewind();
    return intToBytesHelperBuffer;
  }
}
