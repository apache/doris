/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.io.filter.FilterContext;

import java.io.EOFException;
import java.io.IOException;

public final class BitFieldReader {
  private final RunLengthByteReader input;
  private int current;
  private byte currentIdx = 8;

  public BitFieldReader(InStream input) {
    this.input = new RunLengthByteReader(input);
  }

  private void readByte() throws IOException {
    if (input.hasNext()) {
      current = 0xff & input.next();
      currentIdx = 0;
    } else {
      throw new EOFException("Read past end of bit field from " + this);
    }
  }

  public int next() throws IOException {
    if (currentIdx > 7) {
      readByte();
    }

    currentIdx++;
    // Highest bit is the first val
    return ((current >>> (8 - currentIdx)) & 1);
  }

  public void nextVector(LongColumnVector previous,
                         FilterContext filterContext,
                         long previousLen) throws IOException {
    previous.isRepeating = false;
    int previousIdx = 0;
    if (previous.noNulls) {
      for (int i = 0; i != filterContext.getSelectedSize(); i++) {
        int idx = filterContext.getSelected()[i];
        if (idx - previousIdx > 0) {
          skip(idx - previousIdx);
        }
        previous.vector[idx] = next();
        previousIdx = idx + 1;
      }
      skip(previousLen - previousIdx);
    } else {
      for (int i = 0; i != filterContext.getSelectedSize(); i++) {
        int idx = filterContext.getSelected()[i];
        if (idx - previousIdx > 0) {
          skip(TreeReaderFactory.TreeReader.countNonNullRowsInRange(
              previous.isNull, previousIdx, idx));
        }
        if (!previous.isNull[idx]) {
          previous.vector[idx] = next();
        } else {
          previous.vector[idx] = 1;
        }
        previousIdx = idx + 1;
      }
      skip(TreeReaderFactory.TreeReader.countNonNullRowsInRange(
          previous.isNull, previousIdx, (int)previousLen));
    }
  }

  public void nextVector(LongColumnVector previous,
                         long previousLen) throws IOException {
    previous.isRepeating = true;
    for (int i = 0; i < previousLen; i++) {
      if (previous.noNulls || !previous.isNull[i]) {
        previous.vector[i] = next();
      } else {
        // The default value of null for int types in vectorized
        // processing is 1, so set that if the value is null
        previous.vector[i] = 1;
      }

      // The default value for nulls in Vectorization for int types is 1
      // and given that non null value can also be 1, we need to check for isNull also
      // when determining the isRepeating flag.
      if (previous.isRepeating && i > 0 && ((previous.vector[0] != previous.vector[i]) ||
          (previous.isNull[0] != previous.isNull[i]))) {
        previous.isRepeating = false;
      }
    }
  }

  public void seek(PositionProvider index) throws IOException {
    input.seek(index);
    int consumed = (int) index.getNext();
    if (consumed > 8) {
      throw new IllegalArgumentException("Seek past end of byte at " +
          consumed + " in " + input);
    } else if (consumed != 0) {
      readByte();
      currentIdx = (byte) consumed;
    } else {
      currentIdx = 8;
    }
  }

  public void skip(long totalBits) throws IOException {
    final int availableBits = 8 - currentIdx;
    if (totalBits <= availableBits) {
      currentIdx += totalBits;
    } else {
      final long bitsToSkip = (totalBits - availableBits);
      input.skip(bitsToSkip / 8);
      // Edge case: when skipping the last bits of a bitField there is nothing more to read!
      if (input.hasNext()) {
        current = input.next();
        currentIdx = (byte) (bitsToSkip % 8);
      }
    }
  }

  @Override
  public String toString() {
    return "bit reader current: " + current
        + " current bit index: " + currentIdx
        + " from " + input;
  }
}
