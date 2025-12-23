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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

import java.io.EOFException;
import java.io.IOException;

/**
 * A reader that reads a sequence of bytes. A control byte is read before
 * each run with positive values 0 to 127 meaning 3 to 130 repetitions. If the
 * byte is -1 to -128, 1 to 128 literal byte values follow.
 */
public class RunLengthByteReader {
  private InStream input;
  private final byte[] literals =
      new byte[RunLengthByteWriter.MAX_LITERAL_SIZE];
  private int numLiterals = 0;
  private int used = 0;
  private boolean repeat = false;

  public RunLengthByteReader(InStream input) {
    this.input = input;
  }

  public void setInStream(InStream input) {
    this.input = input;
  }

  private void readValues(boolean ignoreEof, int numSkipRows) throws IOException {
    int control = input.read();
    used = 0;
    if (control == -1) {
      if (!ignoreEof) {
        throw new EOFException("Read past end of buffer RLE byte from " + input);
      }
      used = numLiterals = 0;
    } else if (control < 0x80) {
      repeat = true;
      numLiterals = control + RunLengthByteWriter.MIN_REPEAT_SIZE;
      if (numSkipRows >= numLiterals) {
        IOUtils.skipFully(input,1);
      } else {
        int val = input.read();
        if (val == -1) {
          throw new EOFException("Reading RLE byte got EOF");
        }
        literals[0] = (byte) val;
      }
    } else {
      repeat = false;
      numLiterals = 0x100 - control;
      numSkipRows = Math.min(numSkipRows, numLiterals);
      if (numSkipRows > 0) {
        IOUtils.skipFully(input, numSkipRows);
      }
      int bytes = numSkipRows;
      while (bytes < numLiterals) {
        int result = input.read(literals, bytes, numLiterals - bytes);
        if (result == -1) {
          throw new EOFException("Reading RLE byte literal got EOF in " + this);
        }
        bytes += result;
      }
    }
  }

  public boolean hasNext() throws IOException {
    return used != numLiterals || input.available() > 0;
  }

  public byte next() throws IOException {
    byte result;
    if (used == numLiterals) {
      readValues(false, 0);
    }
    if (repeat) {
      result = literals[0];
    } else {
      result = literals[used];
    }
    ++used;
    return result;
  }

  public void nextVector(ColumnVector previous, long[] data, long size)
      throws IOException {
    previous.isRepeating = true;
    for (int i = 0; i < size; i++) {
      if (!previous.isNull[i]) {
        data[i] = next();
      } else {
        // The default value of null for int types in vectorized
        // processing is 1, so set that if the value is null
        data[i] = 1;
      }

      // The default value for nulls in Vectorization for int types is 1
      // and given that non null value can also be 1, we need to check for isNull also
      // when determining the isRepeating flag.
      if (previous.isRepeating && i > 0 &&
          ((data[0] != data[i]) || (previous.isNull[0] != previous.isNull[i]))) {
        previous.isRepeating = false;
      }
    }
  }

  /**
   * Read the next size bytes into the data array, skipping over any slots
   * where isNull is true.
   * @param isNull if non-null, skip any rows where isNull[r] is true
   * @param data the array to read into
   * @param size the number of elements to read
   * @throws IOException
   */
  public void nextVector(boolean[] isNull, int[] data,
                         long size) throws IOException {
    if (isNull == null) {
      for(int i=0; i < size; ++i) {
        data[i] = next();
      }
    } else {
      for(int i=0; i < size; ++i) {
        if (!isNull[i]) {
          data[i] = next();
        }
      }
    }
  }

  public void seek(PositionProvider index) throws IOException {
    input.seek(index);
    int consumed = (int) index.getNext();
    if (consumed != 0) {
      // a loop is required for cases where we break the run into two parts
      while (consumed > 0) {
        readValues(false, 0);
        used = consumed;
        consumed -= numLiterals;
      }
    } else {
      used = 0;
      numLiterals = 0;
    }
  }

  public void skip(long items) throws IOException {
    while (items > 0) {
      if (used == numLiterals) {
        readValues(false, (int) items);
      }
      long consume = Math.min(items, numLiterals - used);
      used += consume;
      items -= consume;
    }
  }

  @Override
  public String toString() {
    return "byte rle " + (repeat ? "repeat" : "literal") + " used: " +
        used + "/" + numLiterals + " from " + input;
  }
}
