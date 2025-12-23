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

/**
 * Murmur3 is successor to Murmur2 fast non-crytographic hash algorithms.
 * <p>
 * Murmur3 32 and 128 bit variants.
 * 32-bit Java port of https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#94
 * 128-bit Java port of https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#255
 * <p>
 * This is a public domain code with no copyrights.
 * From homepage of MurmurHash (https://code.google.com/p/smhasher/),
 * "All MurmurHash versions are public domain software, and the author disclaims all copyright
 * to their code."
 */
public class Murmur3 {
  // from 64-bit linear congruential generator
  public static final long NULL_HASHCODE = 2862933555777941757L;

  // Constants for 32 bit variant
  private static final int C1_32 = 0xcc9e2d51;
  private static final int C2_32 = 0x1b873593;
  private static final int R1_32 = 15;
  private static final int R2_32 = 13;
  private static final int M_32 = 5;
  private static final int N_32 = 0xe6546b64;

  // Constants for 128 bit variant
  private static final long C1 = 0x87c37b91114253d5L;
  private static final long C2 = 0x4cf5ad432745937fL;
  private static final int R1 = 31;
  private static final int R2 = 27;
  private static final int R3 = 33;
  private static final int M = 5;
  private static final int N1 = 0x52dce729;
  private static final int N2 = 0x38495ab5;

  private static final int DEFAULT_SEED = 104729;

  /**
   * Murmur3 32-bit variant.
   *
   * @param data - input byte array
   * @return - hashcode
   */
  public static int hash32(byte[] data) {
    return hash32(data, data.length, DEFAULT_SEED);
  }

  /**
   * Murmur3 32-bit variant.
   *
   * @param data   - input byte array
   * @param length - length of array
   * @param seed   - seed. (default 0)
   * @return - hashcode
   */
  public static int hash32(byte[] data, int length, int seed) {
    int hash = seed;
    final int nblocks = length >> 2;

    // body
    for (int i = 0; i < nblocks; i++) {
      int i_4 = i << 2;
      int k = (data[i_4] & 0xff)
          | ((data[i_4 + 1] & 0xff) << 8)
          | ((data[i_4 + 2] & 0xff) << 16)
          | ((data[i_4 + 3] & 0xff) << 24);

      // mix functions
      k *= C1_32;
      k = Integer.rotateLeft(k, R1_32);
      k *= C2_32;
      hash ^= k;
      hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;
    }

    // tail
    int idx = nblocks << 2;
    int k1 = 0;
    switch (length - idx) {
      case 3:
        k1 ^= data[idx + 2] << 16;
      case 2:
        k1 ^= data[idx + 1] << 8;
      case 1:
        k1 ^= data[idx];

        // mix functions
        k1 *= C1_32;
        k1 = Integer.rotateLeft(k1, R1_32);
        k1 *= C2_32;
        hash ^= k1;
    }

    // finalization
    hash ^= length;
    hash ^= (hash >>> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >>> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >>> 16);

    return hash;
  }

  /**
   * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
   *
   * @param data - input byte array
   * @return - hashcode
   */
  public static long hash64(byte[] data) {
    return hash64(data, 0, data.length, DEFAULT_SEED);
  }

  public static long hash64(byte[] data, int offset, int length) {
    return hash64(data, offset, length, DEFAULT_SEED);
  }

  /**
   * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
   *
   * @param data   - input byte array
   * @param length - length of array
   * @param seed   - seed. (default is 0)
   * @return - hashcode
   */
  public static long hash64(byte[] data, int offset, int length, int seed) {
    long hash = seed;
    final int nblocks = length >> 3;

    // body
    for (int i = 0; i < nblocks; i++) {
      final int i8 = i << 3;
      long k = ((long) data[offset + i8] & 0xff)
          | (((long) data[offset + i8 + 1] & 0xff) << 8)
          | (((long) data[offset + i8 + 2] & 0xff) << 16)
          | (((long) data[offset + i8 + 3] & 0xff) << 24)
          | (((long) data[offset + i8 + 4] & 0xff) << 32)
          | (((long) data[offset + i8 + 5] & 0xff) << 40)
          | (((long) data[offset + i8 + 6] & 0xff) << 48)
          | (((long) data[offset + i8 + 7] & 0xff) << 56);

      // mix functions
      k *= C1;
      k = Long.rotateLeft(k, R1);
      k *= C2;
      hash ^= k;
      hash = Long.rotateLeft(hash, R2) * M + N1;
    }

    // tail
    long k1 = 0;
    int tailStart = nblocks << 3;
    switch (length - tailStart) {
      case 7:
        k1 ^= ((long) data[offset + tailStart + 6] & 0xff) << 48;
      case 6:
        k1 ^= ((long) data[offset + tailStart + 5] & 0xff) << 40;
      case 5:
        k1 ^= ((long) data[offset + tailStart + 4] & 0xff) << 32;
      case 4:
        k1 ^= ((long) data[offset + tailStart + 3] & 0xff) << 24;
      case 3:
        k1 ^= ((long) data[offset + tailStart + 2] & 0xff) << 16;
      case 2:
        k1 ^= ((long) data[offset + tailStart + 1] & 0xff) << 8;
      case 1:
        k1 ^= ((long) data[offset + tailStart] & 0xff);
        k1 *= C1;
        k1 = Long.rotateLeft(k1, R1);
        k1 *= C2;
        hash ^= k1;
    }

    // finalization
    hash ^= length;
    hash = fmix64(hash);

    return hash;
  }

  /**
   * Murmur3 128-bit variant.
   *
   * @param data - input byte array
   * @return - hashcode (2 longs)
   */
  public static long[] hash128(byte[] data) {
    return hash128(data, 0, data.length, DEFAULT_SEED);
  }

  /**
   * Murmur3 128-bit variant.
   *
   * @param data   - input byte array
   * @param offset - the first element of array
   * @param length - length of array
   * @param seed   - seed. (default is 0)
   * @return - hashcode (2 longs)
   */
  public static long[] hash128(byte[] data, int offset, int length, int seed) {
    long h1 = seed;
    long h2 = seed;
    final int nblocks = length >> 4;

    // body
    for (int i = 0; i < nblocks; i++) {
      final int i16 = i << 4;
      long k1 = ((long) data[offset + i16] & 0xff)
          | (((long) data[offset + i16 + 1] & 0xff) << 8)
          | (((long) data[offset + i16 + 2] & 0xff) << 16)
          | (((long) data[offset + i16 + 3] & 0xff) << 24)
          | (((long) data[offset + i16 + 4] & 0xff) << 32)
          | (((long) data[offset + i16 + 5] & 0xff) << 40)
          | (((long) data[offset + i16 + 6] & 0xff) << 48)
          | (((long) data[offset + i16 + 7] & 0xff) << 56);

      long k2 = ((long) data[offset + i16 + 8] & 0xff)
          | (((long) data[offset + i16 + 9] & 0xff) << 8)
          | (((long) data[offset + i16 + 10] & 0xff) << 16)
          | (((long) data[offset + i16 + 11] & 0xff) << 24)
          | (((long) data[offset + i16 + 12] & 0xff) << 32)
          | (((long) data[offset + i16 + 13] & 0xff) << 40)
          | (((long) data[offset + i16 + 14] & 0xff) << 48)
          | (((long) data[offset + i16 + 15] & 0xff) << 56);

      // mix functions for k1
      k1 *= C1;
      k1 = Long.rotateLeft(k1, R1);
      k1 *= C2;
      h1 ^= k1;
      h1 = Long.rotateLeft(h1, R2);
      h1 += h2;
      h1 = h1 * M + N1;

      // mix functions for k2
      k2 *= C2;
      k2 = Long.rotateLeft(k2, R3);
      k2 *= C1;
      h2 ^= k2;
      h2 = Long.rotateLeft(h2, R1);
      h2 += h1;
      h2 = h2 * M + N2;
    }

    // tail
    long k1 = 0;
    long k2 = 0;
    int tailStart = nblocks << 4;
    switch (length - tailStart) {
      case 15:
        k2 ^= (long) (data[offset + tailStart + 14] & 0xff) << 48;
      case 14:
        k2 ^= (long) (data[offset + tailStart + 13] & 0xff) << 40;
      case 13:
        k2 ^= (long) (data[offset + tailStart + 12] & 0xff) << 32;
      case 12:
        k2 ^= (long) (data[offset + tailStart + 11] & 0xff) << 24;
      case 11:
        k2 ^= (long) (data[offset + tailStart + 10] & 0xff) << 16;
      case 10:
        k2 ^= (long) (data[offset + tailStart + 9] & 0xff) << 8;
      case 9:
        k2 ^= (long) (data[offset + tailStart + 8] & 0xff);
        k2 *= C2;
        k2 = Long.rotateLeft(k2, R3);
        k2 *= C1;
        h2 ^= k2;

      case 8:
        k1 ^= (long) (data[offset + tailStart + 7] & 0xff) << 56;
      case 7:
        k1 ^= (long) (data[offset + tailStart + 6] & 0xff) << 48;
      case 6:
        k1 ^= (long) (data[offset + tailStart + 5] & 0xff) << 40;
      case 5:
        k1 ^= (long) (data[offset + tailStart + 4] & 0xff) << 32;
      case 4:
        k1 ^= (long) (data[offset + tailStart + 3] & 0xff) << 24;
      case 3:
        k1 ^= (long) (data[offset + tailStart + 2] & 0xff) << 16;
      case 2:
        k1 ^= (long) (data[offset + tailStart + 1] & 0xff) << 8;
      case 1:
        k1 ^= (long) (data[offset + tailStart] & 0xff);
        k1 *= C1;
        k1 = Long.rotateLeft(k1, R1);
        k1 *= C2;
        h1 ^= k1;
    }

    // finalization
    h1 ^= length;
    h2 ^= length;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    return new long[]{h1, h2};
  }

  private static long fmix64(long h) {
    h ^= (h >>> 33);
    h *= 0xff51afd7ed558ccdL;
    h ^= (h >>> 33);
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= (h >>> 33);
    return h;
  }
}
