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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for Murmur3 variants.
 */
public class TestMurmur3 {

  @Test
  public void testHashCodesM3_32_string() {
    String key = "test";
    int seed = 123;
    HashFunction hf = Hashing.murmur3_32(seed);
    int hc1 = hf.hashBytes(key.getBytes(StandardCharsets.UTF_8)).asInt();
    int hc2 = Murmur3.hash32(key.getBytes(StandardCharsets.UTF_8),
        key.getBytes(StandardCharsets.UTF_8).length, seed);
    assertEquals(hc1, hc2);

    key = "testkey";
    hc1 = hf.hashBytes(key.getBytes(StandardCharsets.UTF_8)).asInt();
    hc2 = Murmur3.hash32(key.getBytes(StandardCharsets.UTF_8),
        key.getBytes(StandardCharsets.UTF_8).length, seed);
    assertEquals(hc1, hc2);
  }

  @Test
  public void testHashCodesM3_32_ints() {
    int seed = 123;
    Random rand = new Random(seed);
    HashFunction hf = Hashing.murmur3_32(seed);
    for (int i = 0; i < 1000; i++) {
      int val = rand.nextInt();
      byte[] data = ByteBuffer.allocate(4).putInt(val).array();
      int hc1 = hf.hashBytes(data).asInt();
      int hc2 = Murmur3.hash32(data, data.length, seed);
      assertEquals(hc1, hc2);
    }
  }

  @Test
  public void testHashCodesM3_32_longs() {
    int seed = 123;
    Random rand = new Random(seed);
    HashFunction hf = Hashing.murmur3_32(seed);
    for (int i = 0; i < 1000; i++) {
      long val = rand.nextLong();
      byte[] data = ByteBuffer.allocate(8).putLong(val).array();
      int hc1 = hf.hashBytes(data).asInt();
      int hc2 = Murmur3.hash32(data, data.length, seed);
      assertEquals(hc1, hc2);
    }
  }

  @Test
  public void testHashCodesM3_32_double() {
    int seed = 123;
    Random rand = new Random(seed);
    HashFunction hf = Hashing.murmur3_32(seed);
    for (int i = 0; i < 1000; i++) {
      double val = rand.nextDouble();
      byte[] data = ByteBuffer.allocate(8).putDouble(val).array();
      int hc1 = hf.hashBytes(data).asInt();
      int hc2 = Murmur3.hash32(data, data.length, seed);
      assertEquals(hc1, hc2);
    }
  }

  @Test
  public void testHashCodesM3_128_string() {
    String key = "test";
    int seed = 123;
    HashFunction hf = Hashing.murmur3_128(seed);
    // guava stores the hashcodes in little endian order
    ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
    buf.put(hf.hashBytes(key.getBytes(StandardCharsets.UTF_8)).asBytes());
    buf.flip();
    long gl1 = buf.getLong();
    long gl2 = buf.getLong(8);
    long[] hc = Murmur3.hash128(key.getBytes(StandardCharsets.UTF_8), 0,
        key.getBytes(StandardCharsets.UTF_8).length, seed);
    long m1 = hc[0];
    long m2 = hc[1];
    assertEquals(gl1, m1);
    assertEquals(gl2, m2);

    key = "testkey128_testkey128";
    buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
    buf.put(hf.hashBytes(key.getBytes(StandardCharsets.UTF_8)).asBytes());
    buf.flip();
    gl1 = buf.getLong();
    gl2 = buf.getLong(8);
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    hc = Murmur3.hash128(keyBytes, 0, keyBytes.length, seed);
    m1 = hc[0];
    m2 = hc[1];
    assertEquals(gl1, m1);
    assertEquals(gl2, m2);

    byte[] offsetKeyBytes = new byte[keyBytes.length + 35];
    Arrays.fill(offsetKeyBytes, (byte) -1);
    System.arraycopy(keyBytes, 0, offsetKeyBytes, 35, keyBytes.length);
    hc = Murmur3.hash128(offsetKeyBytes, 35, keyBytes.length, seed);
    assertEquals(gl1, hc[0]);
    assertEquals(gl2, hc[1]);
  }

  @Test
  public void testHashCodeM3_64() {
    byte[] origin = ("It was the best of times, it was the worst of times," +
        " it was the age of wisdom, it was the age of foolishness," +
        " it was the epoch of belief, it was the epoch of incredulity," +
        " it was the season of Light, it was the season of Darkness," +
        " it was the spring of hope, it was the winter of despair," +
        " we had everything before us, we had nothing before us," +
        " we were all going direct to Heaven," +
        " we were all going direct the other way.").getBytes(StandardCharsets.UTF_8);
    long hash = Murmur3.hash64(origin, 0, origin.length);
    assertEquals(305830725663368540L, hash);

    byte[] originOffset = new byte[origin.length + 150];
    Arrays.fill(originOffset, (byte) 123);
    System.arraycopy(origin, 0, originOffset, 150, origin.length);
    hash = Murmur3.hash64(originOffset, 150, origin.length);
    assertEquals(305830725663368540L, hash);
  }

  @Test
  public void testHashCodesM3_128_ints() {
    int seed = 123;
    Random rand = new Random(seed);
    HashFunction hf = Hashing.murmur3_128(seed);
    for (int i = 0; i < 1000; i++) {
      int val = rand.nextInt();
      byte[] data = ByteBuffer.allocate(4).putInt(val).array();
      // guava stores the hashcodes in little endian order
      ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
      buf.put(hf.hashBytes(data).asBytes());
      buf.flip();
      long gl1 = buf.getLong();
      long gl2 = buf.getLong(8);
      long[] hc = Murmur3.hash128(data, 0, data.length, seed);
      long m1 = hc[0];
      long m2 = hc[1];
      assertEquals(gl1, m1);
      assertEquals(gl2, m2);

      byte[] offsetData = new byte[data.length + 50];
      System.arraycopy(data, 0, offsetData, 50, data.length);
      hc = Murmur3.hash128(offsetData, 50, data.length, seed);
      assertEquals(gl1, hc[0]);
      assertEquals(gl2, hc[1]);
    }
  }

  @Test
  public void testHashCodesM3_128_longs() {
    int seed = 123;
    Random rand = new Random(seed);
    HashFunction hf = Hashing.murmur3_128(seed);
    for (int i = 0; i < 1000; i++) {
      long val = rand.nextLong();
      byte[] data = ByteBuffer.allocate(8).putLong(val).array();
      // guava stores the hashcodes in little endian order
      ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
      buf.put(hf.hashBytes(data).asBytes());
      buf.flip();
      long gl1 = buf.getLong();
      long gl2 = buf.getLong(8);
      long[] hc = Murmur3.hash128(data, 0, data.length, seed);
      long m1 = hc[0];
      long m2 = hc[1];
      assertEquals(gl1, m1);
      assertEquals(gl2, m2);
    }
  }

  @Test
  public void testHashCodesM3_128_double() {
    int seed = 123;
    Random rand = new Random(seed);
    HashFunction hf = Hashing.murmur3_128(seed);
    for (int i = 0; i < 1000; i++) {
      double val = rand.nextDouble();
      byte[] data = ByteBuffer.allocate(8).putDouble(val).array();
      // guava stores the hashcodes in little endian order
      ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
      buf.put(hf.hashBytes(data).asBytes());
      buf.flip();
      long gl1 = buf.getLong();
      long gl2 = buf.getLong(8);
      long[] hc = Murmur3.hash128(data, 0, data.length, seed);
      long m1 = hc[0];
      long m2 = hc[1];
      assertEquals(gl1, m1);
      assertEquals(gl2, m2);
    }
  }
}
