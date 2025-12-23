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

import java.nio.charset.StandardCharsets;

public final class Utf8Utils {

  public static int charLength(byte[] data, int offset, int length) {
    int chars = 0;
    for (int i = 0; i < length; i++) {
      if (isUtfStartByte(data[offset +i ])) {
        chars++;
      }
    }
    return chars;
  }

  /**
   * Return the number of bytes required to read at most
   * maxLength characters in full from a utf-8 encoded byte array provided
   * by data[offset:offset+length]. This does not validate utf-8 data, but
   * operates correctly on already valid utf-8 data.
   *
   * @param maxCharLength
   * @param data
   * @param offset
   * @param length
   */
  public static int truncateBytesTo(int maxCharLength, byte[] data, int offset, int length) {
    int chars = 0;
    if (length <= maxCharLength) {
      return length;
    }
    for (int i = 0; i < length; i++) {
      if (isUtfStartByte(data[offset +i ])) {
        chars++;
      }
      if (chars > maxCharLength) {
        return i;
      }
    }
    // everything fits
    return length;
  }

  /**
   * Checks if b is the first byte of a UTF-8 character.
   *
   */
  public static boolean isUtfStartByte(byte b) {
    return (b & 0xC0) != 0x80;
  }

  /**
   * Find the start of the last character that ends in the current string.
   * @param text the bytes of the utf-8
   * @param from the first byte location
   * @param until the last byte location
   * @return the index of the last character
   */
  public static int findLastCharacter(byte[] text, int from, int until) {
    int posn = until;
    /* we don't expect characters more than 5 bytes */
    while (posn >= from) {
      if (isUtfStartByte(text[posn])) {
        return posn;
      }
      posn -= 1;
    }
    /* beginning of a valid char not found */
    throw new IllegalArgumentException(
        "Could not truncate string, beginning of a valid char not found");
  }

  /**
   * Get the code point at a given location in the byte array.
   * @param source the bytes of the string
   * @param from the offset to start at
   * @param len the number of bytes in the character
   * @return the code point
   */
  public static int getCodePoint(byte[] source, int from, int len) {
    return new String(source, from, len, StandardCharsets.UTF_8)
        .codePointAt(0);
  }

}
