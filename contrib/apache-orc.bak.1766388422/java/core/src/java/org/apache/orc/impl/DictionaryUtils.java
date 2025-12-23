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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class DictionaryUtils {
  private DictionaryUtils() {
    // Utility class does nothing in constructor
  }

  /**
   * Obtain the UTF8 string from the byteArray using the offset in index-array.
   * @param result Container for the UTF8 String.
   * @param position position in the keyOffsets
   * @param keyOffsets starting offset of the key (in byte) in the byte array.
   * @param byteArray storing raw bytes of all keys seen in dictionary
   */
  public static void getTextInternal(Text result, int position,
      DynamicIntArray keyOffsets, DynamicByteArray byteArray) {
    int offset = keyOffsets.get(position);
    int length;
    if (position + 1 == keyOffsets.size()) {
      length = byteArray.size() - offset;
    } else {
      length = keyOffsets.get(position + 1) - offset;
    }
    byteArray.setText(result, offset, length);
  }

  /**
   * Return a {@code ByteBuffer} containing the data at a certain offset within a
   * {@code DynamicByteArray}.
   *
   * @param position position in the keyOffsets
   * @param keyOffsets starting offset of the key (in byte) in the byte array
   * @param byteArray storing raw bytes of all keys seen in dictionary
   * @return the number of bytes written to the output stream
   */
  public static ByteBuffer getTextInternal(int position, DynamicIntArray keyOffsets,
      DynamicByteArray byteArray) {
    final int offset = keyOffsets.get(position);
    final int length;
    if (position + 1 == keyOffsets.size()) {
      length = byteArray.size() - offset;
    } else {
      length = keyOffsets.get(position + 1) - offset;
    }
    return byteArray.get(offset, length);
  }

  /**
   * Write a UTF8 string from the byteArray, using the offset in index-array,
   * into an OutputStream
   *
   * @param out the output stream
   * @param position position in the keyOffsets
   * @param keyOffsets starting offset of the key (in byte) in the byte array
   * @param byteArray storing raw bytes of all keys seen in dictionary
   * @return the number of bytes written to the output stream
   * @throws IOException if an I/O error occurs
   */
  public static int writeToTextInternal(OutputStream out, int position,
      DynamicIntArray keyOffsets, DynamicByteArray byteArray)
      throws IOException {
    int offset = keyOffsets.get(position);
    int length;
    if (position + 1 == keyOffsets.size()) {
      length = byteArray.size() - offset;
    } else {
      length = keyOffsets.get(position + 1) - offset;
    }
    byteArray.write(out, offset, length);
    return length;
  }

  /**
   * Compare a UTF8 string from the byteArray using the offset in index-array.
   *
   * @param bytes an array containing bytes to search for
   * @param offset the offset in the array
   * @param length the number of bytes to search for
   * @param position position in the keyOffsets
   * @param keyOffsets starting offset of the key (in byte) in the byte array
   * @param byteArray storing raw bytes of all key seen in dictionary
   * @return true if the text is equal to the value within the byteArray; false
   *         otherwise
   */
  public static boolean equalsInternal(byte[] bytes, int offset, int length, int position,
      DynamicIntArray keyOffsets, DynamicByteArray byteArray) {
    final int byteArrayOffset = keyOffsets.get(position);
    final int keyLength;
    if (position + 1 == keyOffsets.size()) {
      keyLength = byteArray.size() - byteArrayOffset;
    } else {
      keyLength = keyOffsets.get(position + 1) - byteArrayOffset;
    }
    return 0 == byteArray.compare(bytes, offset, length, byteArrayOffset,
      keyLength);
  }
}
