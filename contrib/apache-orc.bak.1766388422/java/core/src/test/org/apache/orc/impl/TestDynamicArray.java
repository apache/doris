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

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestDynamicArray {

  @Test
  public void testByteArray() throws Exception {
    DynamicByteArray dba = new DynamicByteArray(3, 10);
    dba.add((byte) 0);
    dba.add((byte) 1);
    dba.set(3, (byte) 3);
    dba.set(2, (byte) 2);
    dba.add((byte) 4);
    assertEquals("{0,1,2,3,4}", dba.toString());
    assertEquals(5, dba.size());
    byte[] val;
    val = new byte[0];
    assertEquals(0, dba.compare(val, 0, 0, 2, 0));
    assertEquals(-1, dba.compare(val, 0, 0, 2, 1));
    val = new byte[]{3,42};
    assertEquals(1, dba.compare(val, 0, 1, 2, 0));
    assertEquals(1, dba.compare(val, 0, 1, 2, 1));
    assertEquals(0, dba.compare(val, 0, 1, 3, 1));
    assertEquals(-1, dba.compare(val, 0, 1, 3, 2));
    assertEquals(1, dba.compare(val, 0, 2, 3, 1));
    val = new byte[256];
    for(int b=-128; b < 128; ++b) {
      dba.add((byte) b);
      val[b+128] = (byte) b;
    }
    assertEquals(0, dba.compare(val, 0, 256, 5, 256));
    assertEquals(1, dba.compare(val, 0, 1, 0, 1));
    assertEquals(1, dba.compare(val, 254, 1, 0, 1));
    assertEquals(1, dba.compare(val, 120, 1, 64, 1));
    val = new byte[1024];
    Random rand = new Random(1701);
    for(int i = 0; i < val.length; ++i) {
      rand.nextBytes(val);
    }
    dba.add(val, 0, 1024);
    assertEquals(1285, dba.size());
    assertEquals(0, dba.compare(val, 0, 1024, 261, 1024));
  }

  @Test
  public void testIntArray() throws Exception {
    DynamicIntArray dia = new DynamicIntArray(10);
    for(int i=0; i < 10000; ++i) {
      dia.add(2*i);
    }
    assertEquals(10000, dia.size());
    for(int i=0; i < 10000; ++i) {
      assertEquals(2*i, dia.get(i));
    }
    dia.clear();
    assertEquals(0, dia.size());
    dia.add(3);
    dia.add(12);
    dia.add(65);
    assertEquals("{3,12,65}", dia.toString());
    for(int i=0; i < 5; ++i) {
      dia.increment(i, 3);
    }
    assertEquals("{6,15,68,3,3}", dia.toString());
  }

  @Test
  public void testEmptyIntArrayToString() {
    DynamicIntArray dia = new DynamicIntArray();
    assertEquals("{}", dia.toString());
  }

  @Test
  public void testByteArrayOverflow() {
    DynamicByteArray dba = new DynamicByteArray();

    byte[] val = new byte[1024];
    dba.add(val, 0, val.length);

    byte[] bigVal = new byte[2048];
    RuntimeException exception = assertThrows(
      RuntimeException.class,
      // Need to construct a large array, limited by the heap limit of UT, may cause OOM.
      // The add method does not check whether byte[] and length are consistent,
      // so it is a bit hacky.
      () -> dba.add(bigVal, 0, Integer.MAX_VALUE - 16));

    assertEquals("chunkIndex overflow:-65535. " +
      "You can set orc.column.encoding.direct=columnName, " +
      "or orc.dictionary.key.threshold=0 to turn off dictionary encoding.",
      exception.getMessage());
  }
}
