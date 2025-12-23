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

import org.apache.hadoop.io.Text;
import org.apache.orc.OrcConf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A class that is a growable array of bytes. Growth is managed in terms of
 * chunks that are allocated when needed.
 */
public final class DynamicByteArray {
  static final int DEFAULT_CHUNKSIZE = 32 * 1024;
  static final int DEFAULT_NUM_CHUNKS = 128;

  private final int chunkSize;        // our allocation sizes
  private byte[][] data;              // the real data
  private int length;                 // max set element index +1
  private int initializedChunks = 0;  // the number of chunks created

  public DynamicByteArray() {
    this(DEFAULT_NUM_CHUNKS, DEFAULT_CHUNKSIZE);
  }

  public DynamicByteArray(int numChunks, int chunkSize) {
    if (chunkSize == 0) {
      throw new IllegalArgumentException("bad chunksize");
    }
    this.chunkSize = chunkSize;
    data = new byte[numChunks][];
  }

  /**
   * Ensure that the given index is valid.
   * Throws an exception if chunkIndex is negative.
   */
  private void grow(int chunkIndex) {
    if (chunkIndex < 0) {
      throw new RuntimeException(String.format("chunkIndex overflow:%d. " +
        "You can set %s=columnName, or %s=0 to turn off dictionary encoding.",
        chunkIndex,
        OrcConf.DIRECT_ENCODING_COLUMNS.getAttribute(),
        OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getAttribute()));
    }
    if (chunkIndex >= initializedChunks) {
      if (chunkIndex >= data.length) {
        int newSize = Math.max(chunkIndex + 1, 2 * data.length);
        data = Arrays.copyOf(data, newSize);
      }
      for (int i = initializedChunks; i <= chunkIndex; ++i) {
        data[i] = new byte[chunkSize];
      }
      initializedChunks = chunkIndex + 1;
    }
  }

  public byte get(int index) {
    if (index >= length) {
      throw new IndexOutOfBoundsException("Index " + index +
                                            " is outside of 0.." +
                                            (length - 1));
    }
    int i = index / chunkSize;
    int j = index % chunkSize;
    return data[i][j];
  }

  public void set(int index, byte value) {
    int i = index / chunkSize;
    int j = index % chunkSize;
    grow(i);
    if (index >= length) {
      length = index + 1;
    }
    data[i][j] = value;
  }

  public int add(byte value) {
    int i = length / chunkSize;
    int j = length % chunkSize;
    grow(i);
    data[i][j] = value;
    int result = length;
    length += 1;
    return result;
  }

  /**
   * Copy a slice of a byte array into our buffer.
   * @param value the array to copy from
   * @param valueOffset the first location to copy from value
   * @param valueLength the number of bytes to copy from value
   * @return the offset of the start of the value
   */
  public int add(byte[] value, int valueOffset, int valueLength) {
    int i = length / chunkSize;
    int j = length % chunkSize;
    grow((length + valueLength) / chunkSize);
    int remaining = valueLength;
    while (remaining > 0) {
      int size = Math.min(remaining, chunkSize - j);
      System.arraycopy(value, valueOffset, data[i], j, size);
      remaining -= size;
      valueOffset += size;
      i += 1;
      j = 0;
    }
    int result = length;
    length += valueLength;
    return result;
  }

  /**
   * Read the entire stream into this array.
   * @param in the stream to read from
   * @throws IOException
   */
  public void readAll(InputStream in) throws IOException {
    int currentChunk = length / chunkSize;
    int currentOffset = length % chunkSize;
    grow(currentChunk);
    int currentLength = in.read(data[currentChunk], currentOffset,
        chunkSize - currentOffset);
    while (currentLength > 0) {
      length += currentLength;
      currentOffset = length % chunkSize;
      if (currentOffset == 0) {
        currentChunk = length / chunkSize;
        grow(currentChunk);
      }
      currentLength = in.read(data[currentChunk], currentOffset,
        chunkSize - currentOffset);
    }
  }

  /**
   * Byte compare a set of bytes against the bytes in this dynamic array.
   * @param other source of the other bytes
   * @param otherOffset start offset in the other array
   * @param otherLength number of bytes in the other array
   * @param ourOffset the offset in our array
   * @param ourLength the number of bytes in our array
   * @return negative for less, 0 for equal, positive for greater
   */
  public int compare(byte[] other, int otherOffset, int otherLength,
                     int ourOffset, int ourLength) {
    int currentChunk = ourOffset / chunkSize;
    int currentOffset = ourOffset % chunkSize;
    int maxLength = Math.min(otherLength, ourLength);
    while (maxLength > 0 &&
      other[otherOffset] == data[currentChunk][currentOffset]) {
      otherOffset += 1;
      currentOffset += 1;
      if (currentOffset == chunkSize) {
        currentChunk += 1;
        currentOffset = 0;
      }
      maxLength -= 1;
    }
    if (maxLength == 0) {
      return otherLength - ourLength;
    }
    int otherByte = 0xff & other[otherOffset];
    int ourByte = 0xff & data[currentChunk][currentOffset];
    return otherByte > ourByte ? 1 : -1;
  }

  /**
   * Get the size of the array.
   * @return the number of bytes in the array
   */
  public int size() {
    return length;
  }

  /**
   * Clear the array to its original pristine state.
   */
  public void clear() {
    length = 0;
    for(int i=0; i < data.length; ++i) {
      data[i] = null;
    }
    initializedChunks = 0;
  }

  /**
   * Set a text value from the bytes in this dynamic array.
   * @param result the value to set
   * @param offset the start of the bytes to copy
   * @param length the number of bytes to copy
   */
  public void setText(Text result, int offset, int length) {
    result.clear();
    int currentChunk = offset / chunkSize;
    int currentOffset = offset % chunkSize;
    int currentLength = Math.min(length, chunkSize - currentOffset);
    while (length > 0) {
      result.append(data[currentChunk], currentOffset, currentLength);
      length -= currentLength;
      currentChunk += 1;
      currentOffset = 0;
      currentLength = Math.min(length, chunkSize - currentOffset);
    }
  }

  /**
   * Write out a range of this dynamic array to an output stream.
   * @param out the stream to write to
   * @param offset the first offset to write
   * @param length the number of bytes to write
   * @throws IOException
   */
  public void write(OutputStream out, int offset,
                    int length) throws IOException {
    int currentChunk = offset / chunkSize;
    int currentOffset = offset % chunkSize;
    while (length > 0) {
      int currentLength = Math.min(length, chunkSize - currentOffset);
      out.write(data[currentChunk], currentOffset, currentLength);
      length -= currentLength;
      currentChunk += 1;
      currentOffset = 0;
    }
  }

  @Override
  public String toString() {
    int i;
    StringBuilder sb = new StringBuilder(length * 3);

    sb.append('{');
    int l = length - 1;
    for (i=0; i<l; i++) {
      sb.append(Integer.toHexString(get(i)));
      sb.append(',');
    }
    sb.append(get(i));
    sb.append('}');

    return sb.toString();
  }

  public void setByteBuffer(ByteBuffer result, int offset, int length) {
    result.clear();
    int currentChunk = offset / chunkSize;
    int currentOffset = offset % chunkSize;
    int currentLength = Math.min(length, chunkSize - currentOffset);
    while (length > 0) {
      result.put(data[currentChunk], currentOffset, currentLength);
      length -= currentLength;
      currentChunk += 1;
      currentOffset = 0;
      currentLength = Math.min(length, chunkSize - currentOffset);
    }
  }

  /**
   * Gets all the bytes of the array.
   *
   * @return Bytes of the array
   */
  public byte[] get() {
    byte[] result = null;
    if (length > 0) {
      int currentChunk = 0;
      int currentOffset = 0;
      int currentLength = Math.min(length, chunkSize);
      int destOffset = 0;
      result = new byte[length];
      int totalLength = length;
      while (totalLength > 0) {
        System.arraycopy(data[currentChunk], currentOffset, result, destOffset, currentLength);
        destOffset += currentLength;
        totalLength -= currentLength;
        currentChunk += 1;
        currentOffset = 0;
        currentLength = Math.min(totalLength, chunkSize - currentOffset);
      }
    }
    return result;
  }

  public ByteBuffer get(int offset, int length) {
    final int currentChunk = offset / chunkSize;
    final int currentOffset = offset % chunkSize;
    final int currentLength = Math.min(length, chunkSize - currentOffset);
    if (currentLength == length) {
      return ByteBuffer.wrap(data[currentChunk], currentOffset, length);
    }
    ByteBuffer bb = ByteBuffer.allocate(length);
    setByteBuffer(bb, offset, length);
    return (ByteBuffer) bb.flip();
  }

  /**
   * Get the size of the buffers.
   */
  public long getSizeInBytes() {
    return (long) initializedChunks * chunkSize;
  }
}
