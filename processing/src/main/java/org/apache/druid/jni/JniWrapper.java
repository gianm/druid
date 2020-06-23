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

package org.apache.druid.jni;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class JniWrapper
{
  public static final JniWrapper INSTANCE = new JniWrapper();

  static {
    System.loadLibrary("druid-processing");
  }

  public native void hello();

  public native boolean memoryEquals(
      byte[] array1,
      ByteBuffer buffer1,
      long offset1,
      byte[] array2,
      ByteBuffer buffer2,
      long offset2,
      int length
  );

  public native void aggregateCount(
      ByteBuffer buf,
      byte[] bufArray,
      int numRows,
      int[] positions,
      int positionOffset
  );

  public native void aggregateDoubleSum(
      double[] vec,
      ByteBuffer buf,
      byte[] bufArray,
      int numRows,
      int[] positions,
      @Nullable int[] rows,
      int positionOffset
  );

  public static void main(String[] args)
  {
    INSTANCE.hello();
  }
}
