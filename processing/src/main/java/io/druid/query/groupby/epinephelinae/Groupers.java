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

import com.google.common.collect.Iterators;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;

public class Groupers
{
  private Groupers()
  {
    // No instantiation
  }

  public static int hash(final ByteBuffer keyBuffer)
  {
    // Similar to what the built-in HashMap does.
    final int h = keyBuffer.hashCode();
    return h ^ (h >>> 16);
  }

  public static <KeyType> Iterator<Grouper.Entry<KeyType>> mergeIterators(
      final Iterable<Iterator<Grouper.Entry<KeyType>>> iterators,
      final Grouper.KeyComparator comparator
  )
  {
    if (comparator == null) {
      // Unsorted iteration
      return Iterators.concat(iterators.iterator());
    } else {
      // Sorted iteration
      return Iterators.mergeSorted(
          iterators,
          new Comparator<Grouper.Entry<KeyType>>()
          {
            @Override
            public int compare(Grouper.Entry<KeyType> lhs, Grouper.Entry<KeyType> rhs)
            {
              return comparator.compare(
                  lhs.getBuffer(),
                  rhs.getBuffer(),
                  lhs.getBuffer().position(),
                  rhs.getBuffer().position()
              );
            }
          }
      );
    }
  }
}
