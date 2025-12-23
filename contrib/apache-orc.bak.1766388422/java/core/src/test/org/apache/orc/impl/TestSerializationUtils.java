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

import com.google.common.math.LongMath;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestSerializationUtils {

  private InputStream fromBuffer(ByteArrayOutputStream buffer) {
    return new ByteArrayInputStream(buffer.toByteArray());
  }

  @Test
  public void testDoubles() throws Exception {
    double tolerance = 0.0000000000000001;
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    SerializationUtils utils = new SerializationUtils();
    utils.writeDouble(buffer, 1343822337.759);
    assertEquals(1343822337.759, utils.readDouble(fromBuffer(buffer)), tolerance);
    buffer = new ByteArrayOutputStream();
    utils.writeDouble(buffer, 0.8);
    double got = utils.readDouble(fromBuffer(buffer));
    assertEquals(0.8, got, tolerance);
  }

  @Test
  public void testBigIntegers() throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(0));
    assertArrayEquals(new byte[]{0}, buffer.toByteArray());
    assertEquals(0L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(1));
    assertArrayEquals(new byte[]{2}, buffer.toByteArray());
    assertEquals(1L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(-1));
    assertArrayEquals(new byte[]{1}, buffer.toByteArray());
    assertEquals(-1L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(50));
    assertArrayEquals(new byte[]{100}, buffer.toByteArray());
    assertEquals(50L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(-50));
    assertArrayEquals(new byte[]{99}, buffer.toByteArray());
    assertEquals(-50L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    for(int i=-8192; i < 8192; ++i) {
      buffer.reset();
        SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(i));
      assertEquals(i >= -64 && i < 64 ? 1 : 2, buffer.size(),
          "compare length for " + i);
      assertEquals(i, SerializationUtils.readBigInteger(fromBuffer(buffer)).intValue(),
          "compare result for " + i);
    }
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer,
        new BigInteger("123456789abcdef0",16));
    assertEquals(new BigInteger("123456789abcdef0",16),
        SerializationUtils.readBigInteger(fromBuffer(buffer)));
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer,
        new BigInteger("-123456789abcdef0",16));
    assertEquals(new BigInteger("-123456789abcdef0",16),
        SerializationUtils.readBigInteger(fromBuffer(buffer)));
    StringBuilder buf = new StringBuilder();
    for(int i=0; i < 256; ++i) {
      String num = Integer.toHexString(i);
      if (num.length() == 1) {
        buf.append('0');
      }
      buf.append(num);
    }
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer,
        new BigInteger(buf.toString(),16));
    assertEquals(new BigInteger(buf.toString(),16),
        SerializationUtils.readBigInteger(fromBuffer(buffer)));
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer,
        new BigInteger("ff000000000000000000000000000000000000000000ff",16));
    assertEquals(
        new BigInteger("ff000000000000000000000000000000000000000000ff",16),
        SerializationUtils.readBigInteger(fromBuffer(buffer)));
  }

  @Test
  public void testSubtractionOverflow() {
    // cross check results with Guava results below
    SerializationUtils utils = new SerializationUtils();
    assertFalse(utils.isSafeSubtract(22222222222L, Long.MIN_VALUE));
    assertFalse(utils.isSafeSubtract(-22222222222L, Long.MAX_VALUE));
    assertFalse(utils.isSafeSubtract(Long.MIN_VALUE, Long.MAX_VALUE));
    assertTrue(utils.isSafeSubtract(-1553103058346370095L, 6553103058346370095L));
    assertTrue(utils.isSafeSubtract(0, Long.MAX_VALUE));
    assertTrue(utils.isSafeSubtract(Long.MIN_VALUE, 0));
  }

  @Test
  public void testSubtractionOverflowGuava() {
    try {
      LongMath.checkedSubtract(22222222222L, Long.MIN_VALUE);
      fail("expected ArithmeticException for overflow");
    } catch (ArithmeticException ex) {
      assertEquals("overflow: checkedSubtract(22222222222, -9223372036854775808)", ex.getMessage());
    }

    try {
      LongMath.checkedSubtract(-22222222222L, Long.MAX_VALUE);
      fail("expected ArithmeticException for overflow");
    } catch (ArithmeticException ex) {
      assertEquals("overflow: checkedSubtract(-22222222222, 9223372036854775807)", ex.getMessage());
    }

    try {
      LongMath.checkedSubtract(Long.MIN_VALUE, Long.MAX_VALUE);
      fail("expected ArithmeticException for overflow");
    } catch (ArithmeticException ex) {
      assertEquals("overflow: checkedSubtract(-9223372036854775808, 9223372036854775807)", ex.getMessage());
    }

    assertEquals(-8106206116692740190L,
        LongMath.checkedSubtract(-1553103058346370095L, 6553103058346370095L));
    assertEquals(-Long.MAX_VALUE, LongMath.checkedSubtract(0, Long.MAX_VALUE));
    assertEquals(Long.MIN_VALUE, LongMath.checkedSubtract(Long.MIN_VALUE, 0));
  }

  @Test
  public void testRandomFloats() throws Exception {
    float tolerance = 0.0000000000000001f;
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    SerializationUtils utils = new SerializationUtils();
    Random rand = new Random();
    int n = 100_000;
    float[] expected = new float[n];
    for (int i = 0; i < n; i++) {
      float f = rand.nextFloat();
      expected[i] = f;
      utils.writeFloat(buffer, f);
    }
    InputStream newBuffer = fromBuffer(buffer);
    for (int i = 0; i < n; i++) {
      float got = utils.readFloat(newBuffer);
      assertEquals(expected[i], got, tolerance);
    }
  }

  @Test
  public void testRandomDoubles() throws Exception {
    double tolerance = 0.0000000000000001;
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    SerializationUtils utils = new SerializationUtils();
    Random rand = new Random();
    int n = 100_000;
    double[] expected = new double[n];
    for (int i = 0; i < n; i++) {
      double d = rand.nextDouble();
      expected[i] = d;
      utils.writeDouble(buffer, d);
    }
    InputStream newBuffer = fromBuffer(buffer);
    for (int i = 0; i < n; i++) {
      double got = utils.readDouble(newBuffer);
      assertEquals(expected[i], got, tolerance);
    }
  }
}
