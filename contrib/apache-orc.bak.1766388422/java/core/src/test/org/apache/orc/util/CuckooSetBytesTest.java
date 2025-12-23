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

package org.apache.orc.util;


import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class CuckooSetBytesTest {

  // maximum table size
  private static final int MAX_SIZE = 65437;

  @Test
  public void testSetBytes() {
    String[] strings = {"foo", "bar", "baz", "a", "", "x1341", "Z"};
    String[] negativeStrings = {"not", "in", "the", "set", "foobar"};
    byte[][] values = getByteArrays(strings);
    byte[][] negatives = getByteArrays(negativeStrings);

    // load set
    CuckooSetBytes s = new CuckooSetBytes(strings.length);
    for(byte[] v : values) {
      s.insert(v);
    }

    // test that the values we added are there
    for(byte[] v : values) {
      assertTrue(s.lookup(v, 0, v.length));
    }

    // test that values that we know are missing are shown to be absent
    for (byte[] v : negatives) {
      assertFalse(s.lookup(v, 0, v.length));
    }

    // Test that we can search correctly using a buffer and pulling
    // a sequence of bytes out of the middle of it. In this case it
    // is the 3 letter sequence "foo".
    byte[] buf = getUTF8Bytes("thewordfooisinhere");
    assertTrue(s.lookup(buf, 7, 3));
  }

  @Test
  public void testSetBytesLargeRandom() {
    byte[][] values;
    Random gen = new Random(98763537);
    for(int i = 0; i < 200;) {

      // Make a random array of byte arrays
      int size = gen.nextInt() % MAX_SIZE;
      if (size <= 0) {   // ensure size is >= 1, otherwise try again
        continue;
      }
      i++;
      values = new byte[size][];
      loadRandomBytes(values, gen);

      // load them into a set
      CuckooSetBytes s = new CuckooSetBytes(size);
      loadSet(s, values);

      // look them up to make sure they are all there
      for (int j = 0; j != size; j++) {
        assertTrue(s.lookup(values[j], 0, values[j].length));
      }
    }
  }

  public void loadRandomBytes(byte[][] values, Random gen) {
    for (int i = 0; i != values.length; i++) {
      values[i] = getUTF8Bytes(Integer.toString(gen.nextInt()));
    }
  }

  private byte[] getUTF8Bytes(String s) {
    byte[] v = null;
    try {
      v = s.getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      ; // won't happen
    }
    return v;
  }

  // Get an array of UTF-8 byte arrays from an array of strings
  private byte[][] getByteArrays(String[] strings) {
    byte[][] values = new byte[strings.length][];
    for(int i = 0; i != strings.length; i++) {
      try {
        values[i] = strings[i].getBytes(StandardCharsets.UTF_8);
      } catch (Exception e) {
        ; // can't happen
      }
    }
    return values;
  }

  private void loadSet(CuckooSetBytes s, byte[][] values) {
    for (byte[] v: values) {
      s.insert(v);
    }
  }

}
