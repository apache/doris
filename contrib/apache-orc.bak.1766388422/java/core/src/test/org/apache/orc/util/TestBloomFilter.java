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

import com.google.protobuf.ByteString;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for BloomFilter
 */
public class TestBloomFilter {

  @Test
  public void testBitset() {
    BloomFilter.BitSet bitset = new BloomFilter.BitSet(128);
    // set every 9th bit for a rotating pattern
    for(int l=0; l < 8; ++l) {
      bitset.set(l*9);
    }
    // set every non-9th bit
    for(int l=8; l < 16; ++l) {
      for(int b=0; b < 8; ++b) {
        if (b != l - 8) {
          bitset.set(l*8+b);
        }
      }
    }
    for(int b=0; b < 64; ++b) {
      assertEquals(b % 9 == 0, bitset.get(b));
    }
    for(int b=64; b < 128; ++b) {
      assertEquals((b % 8) != (b - 64) / 8, bitset.get(b));
    }
    // test that the longs are mapped correctly
    long[] longs = bitset.getData();
    assertEquals(2, longs.length);
    assertEquals(0x8040201008040201L, longs[0]);
    assertEquals(~0x8040201008040201L, longs[1]);
  }

  /**
   * Same test as TestBloomFilter_testLongHash in C++ codes. Make sure the hash values
   * are consistent between the Java client and C++ client.
   * TODO(ORC-1025): Add exhaustive test on all numbers.
   */
  @Test
  public void testLongHash() {
    assertEquals(0, BloomFilter.getLongHash(0));
    assertEquals(6614246905173314819L, BloomFilter.getLongHash(-1));
    assertEquals(-5218250166726157773L, BloomFilter.getLongHash(-2));
    assertEquals(1396019780946710816L, BloomFilter.getLongHash(-3));

    assertEquals(3691278333958578070L, BloomFilter.getLongHash(-9223372036854775805L));
    assertEquals(-1192099642781211952L, BloomFilter.getLongHash(-9223372036854775806L));
    assertEquals(-9102499068535824902L, BloomFilter.getLongHash(-9223372036854775807L));

    assertEquals(1499534499340523007L, BloomFilter.getLongHash(790302201));
    assertEquals(-5108695154500810163L, BloomFilter.getLongHash(790302202));
    assertEquals(-2450623810987162260L, BloomFilter.getLongHash(790302203));
    assertEquals(-1097054448615658549L, BloomFilter.getLongHash(18000000000L));

    assertEquals(-4986173376161118712L, BloomFilter.getLongHash(9223372036064673413L));
    assertEquals(3785699328822078862L, BloomFilter.getLongHash(9223372036064673414L));
    assertEquals(294188322706112357L, BloomFilter.getLongHash(9223372036064673415L));
  }

  private void checkBitSet(BloomFilter bf, int[] pos) {
    for (int i : pos) {
      assertTrue(bf.testBitSetPos(i));
    }
  }

  /**
   * Same test as TestBloomFilter_testBloomFilterBasicOperations in C++ codes. We also
   * verifies the bitSet positions that are set, to make sure both the Java and C++ codes
   * hash the same value into the same position.
   */
  @Test
  public void testBasicOperations() {
    BloomFilter bloomFilter = new BloomFilterUtf8(128, BloomFilter.DEFAULT_FPP);

    // test integers
    bloomFilter.reset();
    assertFalse(bloomFilter.testLong(1));
    assertFalse(bloomFilter.testLong(11));
    assertFalse(bloomFilter.testLong(111));
    assertFalse(bloomFilter.testLong(1111));
    assertFalse(bloomFilter.testLong(0));
    assertFalse(bloomFilter.testLong(-1));
    assertFalse(bloomFilter.testLong(-11));
    assertFalse(bloomFilter.testLong(-111));
    assertFalse(bloomFilter.testLong(-1111));

    bloomFilter.addLong(1);
    checkBitSet(bloomFilter, new int[]{567, 288, 246, 306, 228});
    bloomFilter.addLong(11);
    checkBitSet(bloomFilter, new int[]{228, 285, 342, 399, 456});
    bloomFilter.addLong(111);
    checkBitSet(bloomFilter, new int[]{802, 630, 458, 545, 717});
    bloomFilter.addLong(1111);
    checkBitSet(bloomFilter, new int[]{826, 526, 40, 480, 86});
    bloomFilter.addLong(0);
    checkBitSet(bloomFilter, new int[]{0, 0, 0, 0, 0});
    bloomFilter.addLong(-1);
    checkBitSet(bloomFilter, new int[]{120, 308, 335, 108, 535});
    bloomFilter.addLong(-11);
    checkBitSet(bloomFilter, new int[]{323, 685, 215, 577, 107});
    bloomFilter.addLong(-111);
    checkBitSet(bloomFilter, new int[]{357, 318, 279, 15, 54});
    bloomFilter.addLong(-1111);
    checkBitSet(bloomFilter, new int[]{572, 680, 818, 434, 232});

    assertTrue(bloomFilter.testLong(1));
    assertTrue(bloomFilter.testLong(11));
    assertTrue(bloomFilter.testLong(111));
    assertTrue(bloomFilter.testLong(1111));
    assertTrue(bloomFilter.testLong(0));
    assertTrue(bloomFilter.testLong(-1));
    assertTrue(bloomFilter.testLong(-11));
    assertTrue(bloomFilter.testLong(-111));
    assertTrue(bloomFilter.testLong(-1111));

    // test doubles
    bloomFilter.reset();
    assertFalse(bloomFilter.testDouble(1.1));
    assertFalse(bloomFilter.testDouble(11.11));
    assertFalse(bloomFilter.testDouble(111.111));
    assertFalse(bloomFilter.testDouble(1111.1111));
    assertFalse(bloomFilter.testDouble(0.0));
    assertFalse(bloomFilter.testDouble(-1.1));
    assertFalse(bloomFilter.testDouble(-11.11));
    assertFalse(bloomFilter.testDouble(-111.111));
    assertFalse(bloomFilter.testDouble(-1111.1111));

    bloomFilter.addDouble(1.1);
    checkBitSet(bloomFilter, new int[]{522, 692, 12, 370, 753});
    bloomFilter.addDouble(11.11);
    checkBitSet(bloomFilter,  new int[]{210, 188, 89, 720, 389});
    bloomFilter.addDouble(111.111);
    checkBitSet(bloomFilter, new int[]{831, 252, 583, 500, 335});
    bloomFilter.addDouble(1111.1111);
    checkBitSet(bloomFilter, new int[]{725, 175, 374, 92, 642});
    bloomFilter.addDouble(0.0);
    checkBitSet(bloomFilter, new int[]{0, 0, 0, 0, 0});
    bloomFilter.addDouble(-1.1);
    checkBitSet(bloomFilter, new int[]{636, 163, 565, 206, 679});
    bloomFilter.addDouble(-11.11);
    checkBitSet(bloomFilter, new int[]{473, 192, 743, 462, 181});
    bloomFilter.addDouble(-111.111);
    checkBitSet(bloomFilter, new int[]{167, 152, 472, 295, 24});
    bloomFilter.addDouble(-1111.1111);
    checkBitSet(bloomFilter, new int[]{308, 346, 384, 422, 371});

    assertTrue(bloomFilter.testDouble(1.1));
    assertTrue(bloomFilter.testDouble(11.11));
    assertTrue(bloomFilter.testDouble(111.111));
    assertTrue(bloomFilter.testDouble(1111.1111));
    assertTrue(bloomFilter.testDouble(0.0));
    assertTrue(bloomFilter.testDouble(-1.1));
    assertTrue(bloomFilter.testDouble(-11.11));
    assertTrue(bloomFilter.testDouble(-111.111));
    assertTrue(bloomFilter.testDouble(-1111.1111));

    // test strings
    bloomFilter.reset();
    String emptyStr = "";
    String enStr = "english";
    String cnStr = "中国字";

    assertFalse(bloomFilter.testString(emptyStr));
    assertFalse(bloomFilter.testString(enStr));
    assertFalse(bloomFilter.testString(cnStr));

    bloomFilter.addString(emptyStr);
    checkBitSet(bloomFilter, new int[]{656, 807, 480, 151, 304});
    bloomFilter.addString(enStr);
    checkBitSet(bloomFilter, new int[]{576, 221, 68, 729, 392});
    bloomFilter.addString(cnStr);
    checkBitSet(bloomFilter, new int[]{602, 636, 44, 362, 318});

    assertTrue(bloomFilter.testString(emptyStr));
    assertTrue(bloomFilter.testString(enStr));
    assertTrue(bloomFilter.testString(cnStr));
  }

  @Test
  public void testBloomFilterSerialize() {
    long[] bits = new long[]{0x8040201008040201L, ~0x8040201008040201L};
    BloomFilter bloom = new BloomFilterUtf8(bits, 1);
    OrcProto.BloomFilter.Builder builder = OrcProto.BloomFilter.newBuilder();
    BloomFilterIO.serialize(builder, bloom);
    OrcProto.BloomFilter proto = builder.build();
    assertEquals(1, proto.getNumHashFunctions());
    assertEquals(0, proto.getBitsetCount());
    ByteString bs = proto.getUtf8Bitset();
    byte[] expected = new byte[]{0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40,
        (byte) 0x80, ~0x01, ~0x02, ~0x04, ~0x08, ~0x10, ~0x20, ~0x40,
        (byte) ~0x80};
    OrcProto.ColumnEncoding.Builder encoding =
        OrcProto.ColumnEncoding.newBuilder();
    encoding.setKind(OrcProto.ColumnEncoding.Kind.DIRECT)
        .setBloomEncoding(BloomFilterIO.Encoding.UTF8_UTC.getId());
    assertArrayEquals(expected, bs.toByteArray());
    BloomFilter rebuilt = BloomFilterIO.deserialize(
        OrcProto.Stream.Kind.BLOOM_FILTER_UTF8,
        encoding.build(),
        OrcFile.WriterVersion.ORC_135,
        TypeDescription.Category.INT,
        proto);
    assertEquals(bloom, rebuilt);
  }

  @Test
  public void testBloomFilterEquals() {
    long[] bits = new long[]{0x8040201008040201L, ~0x8040201008040201L};
    BloomFilter bloom = new BloomFilterUtf8(bits, 1);
    BloomFilter other = new BloomFilterUtf8(new long[]{0,0}, 1);
    assertFalse(bloom.equals(other));
  }
}
