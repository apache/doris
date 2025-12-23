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

import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;

import java.util.Random;

/**
 * A high-performance set implementation used to support fast set membership testing,
 * using Cuckoo hashing. This is used to support fast tests of the form
 * <p>
 *       column IN ( list-of-values )
 * <p>
 * For details on the algorithm, see R. Pagh and F. F. Rodler, "Cuckoo Hashing,"
 * Elsevier Science preprint, Dec. 2003. http://www.itu.dk/people/pagh/papers/cuckoo-jour.pdf.
 * <p>
 * Copied from CuckooSetBytes@Apache Hive project for convenience
 */
public class CuckooSetBytes {
  private byte[][] t1;
  private byte[][] t2;
  private byte[][] prev1 = null; // used for rehashing to get last set of values
  private byte[][] prev2 = null; // " "
  private int n; // current array size
  private static final double PADDING_FACTOR = 1.0/0.40; // have minimum 40% fill factor
  private int salt = 0;
  private final Random gen = new Random(676983475);
  private int rehashCount = 0;
  private static final long INT_MASK  = 0x00000000ffffffffL;
  private static final long BYTE_MASK = 0x00000000000000ffL;
  // some prime numbers spaced about at powers of 2 in magnitude
  static final int[] primes = {7, 13, 17, 23, 31, 53, 67, 89, 127, 269, 571, 1019, 2089,
    4507, 8263, 16361, 32327, 65437, 131111, 258887, 525961, 999983, 2158909, 4074073,
    8321801, 15485863, 32452867, 67867967, 122949829, 256203221, 553105253, 982451653,
    1645333507, 2147483647};

  /**
   * Allocate a new set to hold expectedSize values. Re-allocation to expand
   * the set is not implemented, so the expected size must be at least the
   * size of the set to be inserted.
   * @param expectedSize At least the size of the set of values that will be inserted.
   */
  public CuckooSetBytes(int expectedSize) {

    // Choose array size. We have two hash tables to hold entries, so the sum
    // of the two should have a bit more than twice as much space as the
    // minimum required.
    n = (int) (expectedSize * PADDING_FACTOR / 2.0);

    // some prime numbers spaced about at powers of 2 in magnitude

    // try to get prime number table size to have less dependence on good hash function
    for (int i = 0; i != primes.length; i++) {
      if (n <= primes[i]) {
        n = primes[i];
        break;
      }
    }

    t1 = new byte[n][];
    t2 = new byte[n][];
    updateHashSalt();
  }

  /**
   * Return true if and only if the value in byte array b beginning at start
   * and ending at start+len is present in the set.
   */
  public boolean lookup(byte[] b, int start, int len) {

    return entryEqual(t1, h1(b, start, len), b, start, len) ||
           entryEqual(t2, h2(b, start, len), b, start, len);
  }

  private static boolean entryEqual(byte[][] t, int hash, byte[] b, int start, int len) {
    return t[hash] != null && StringExpr.equal(t[hash], 0, t[hash].length, b, start, len);
  }

  public void insert(byte[] x) {
    byte[] temp;
    if (lookup(x, 0, x.length)) {
      return;
    }

    // Try to insert up to n times. Rehash if that fails.
    for(int i = 0; i != n; i++) {
      int hash1 = h1(x, 0, x.length);
      if (t1[hash1] == null) {
        t1[hash1] = x;
        return;
      }

      // swap x and t1[h1(x)]
      temp = t1[hash1];
      t1[hash1] = x;
      x = temp;

      int hash2 = h2(x, 0, x.length);
      if (t2[hash2] == null) {
        t2[hash2] = x;
        return;
      }

      // swap x and t2[h2(x)]
      temp = t2[hash2];
      t2[hash2] = x;
      x = temp;
    }
    rehash();
    insert(x);
  }

  /**
   * Insert all values in the input array into the set.
   */
  public void load(byte[][] a) {
    for (byte[] x : a) {
      insert(x);
    }
  }

  /**
   * Try to insert with up to n value's "poked out". Return the last value poked out.
   * If the value is not blank then we assume there was a cycle.
   * Don't try to insert the same value twice. This is for use in rehash only,
   * so you won't see the same value twice.
   */
  private byte[] tryInsert(byte[] x) {
    byte[] temp;

    for(int i = 0; i != n; i++) {
      int hash1 = h1(x, 0, x.length);
      if (t1[hash1] == null) {
        t1[hash1] = x;
        return null;
      }

      // swap x and t1[h1(x)]
      temp = t1[hash1];
      t1[hash1] = x;
      x = temp;

      int hash2 = h2(x, 0, x.length);
      if (t2[hash2] == null) {
        t2[hash2] = x;
        return null;
      }

      // swap x and t2[h2(x)]
      temp = t2[hash2];
      t2[hash2] = x;
      x = temp;
      if (x == null) {
        break;
      }
    }
    return x;
  }

  /**
   * first hash function
   */
  private int h1(byte[] b, int start, int len) {

    // AND hash with mask to 0 out sign bit to make sure it's positive.
    // Then we know taking the result mod n is in the range (0..n-1).
    return (hash(b, start, len, 0) & 0x7FFFFFFF) % n;
  }

  /**
   * second hash function
   */
  private int h2(byte[] b, int start, int len) {

    // AND hash with mask to 0 out sign bit to make sure it's positive.
    // Then we know taking the result mod n is in the range (0..n-1).
    // Include salt as argument so this hash function can be varied
    // if we need to rehash.
    return (hash(b, start, len, salt) & 0x7FFFFFFF) % n;
  }

  /**
   * In case of rehash, hash function h2 is changed by updating the
   * salt value passed in to the function hash().
   */
  private void updateHashSalt() {
    salt = gen.nextInt(0x7FFFFFFF);
  }

  private void rehash() {
    rehashCount++;
    if (rehashCount > 20) {
      throw new RuntimeException("Too many rehashes");
    }
    updateHashSalt();

    // Save original values
    if (prev1 == null) {
      prev1 = t1;
      prev2 = t2;
    }
    t1 = new byte[n][];
    t2 = new byte[n][];
    for (byte[] v  : prev1) {
      if (v != null) {
        byte[] x = tryInsert(v);
        if (x != null) {
          rehash();
          return;
        }
      }
    }
    for (byte[] v  : prev2) {
      if (v != null) {
        byte[] x = tryInsert(v);
        if (x != null) {
          rehash();
          return;
        }
      }
    }

    // We succeeded in adding all the values, so
    // clear the previous values recorded.
    prev1 = null;
    prev2 = null;
  }

  /**
   * This is adapted from the org.apache.hadoop.util.hash.JenkinsHash package.
   * The interface needed to be modified to suit the use here, by adding
   * a start offset parameter to the hash function.
   *
   * In the future, folding this back into the original Hadoop package should
   * be considered. This could could them import that package and use it.
   * The original comments from the source are below.
   *
   * taken from  hashlittle() -- hash a variable-length key into a 32-bit value
   *
   * @param key the key (the unaligned variable-length array of bytes)
   * @param nbytes number of bytes to include in hash
   * @param initval can be any integer value
   * @return a 32-bit value.  Every bit of the key affects every bit of the
   * return value.  Two keys differing by one or two bits will have totally
   * different hash values.
   *
   * <p>The best hash table sizes are powers of 2.  There is no need to do mod
   * a prime (mod is sooo slow!).  If you need less than 32 bits, use a bitmask.
   * For example, if you need only 10 bits, do
   * <code>h = (h & hashmask(10));</code>
   * In which case, the hash table should have hashsize(10) elements.
   *
   * <p>If you are hashing n strings byte[][] k, do it like this:
   * for (int i = 0, h = 0; i < n; ++i) h = hash( k[i], h);
   *
   * <p>By Bob Jenkins, 2006.  bob_jenkins@burtleburtle.net.  You may use this
   * code any way you wish, private, educational, or commercial.  It's free.
   *
   * <p>Use for hash table lookup, or anything where one collision in 2^^32 is
   * acceptable.  Do NOT use for cryptographic purposes.
  */
  @SuppressWarnings("fallthrough")
  private int hash(byte[] key, int start, int nbytes, int initval) {
    int length = nbytes;
    long a, b, c;       // We use longs because we don't have unsigned ints
    a = b = c = (0x00000000deadbeefL + length + initval) & INT_MASK;
    int offset = start;
    for (; length > 12; offset += 12, length -= 12) {
      a = (a + (key[offset] & BYTE_MASK)) & INT_MASK;
      a = (a + (((key[offset + 1]  & BYTE_MASK) <<  8) & INT_MASK)) & INT_MASK;
      a = (a + (((key[offset + 2]  & BYTE_MASK) << 16) & INT_MASK)) & INT_MASK;
      a = (a + (((key[offset + 3]  & BYTE_MASK) << 24) & INT_MASK)) & INT_MASK;
      b = (b + (key[offset + 4]    & BYTE_MASK)) & INT_MASK;
      b = (b + (((key[offset + 5]  & BYTE_MASK) <<  8) & INT_MASK)) & INT_MASK;
      b = (b + (((key[offset + 6]  & BYTE_MASK) << 16) & INT_MASK)) & INT_MASK;
      b = (b + (((key[offset + 7]  & BYTE_MASK) << 24) & INT_MASK)) & INT_MASK;
      c = (c + (key[offset + 8]    & BYTE_MASK)) & INT_MASK;
      c = (c + (((key[offset + 9]  & BYTE_MASK) <<  8) & INT_MASK)) & INT_MASK;
      c = (c + (((key[offset + 10] & BYTE_MASK) << 16) & INT_MASK)) & INT_MASK;
      c = (c + (((key[offset + 11] & BYTE_MASK) << 24) & INT_MASK)) & INT_MASK;

      /*
       * mix -- mix 3 32-bit values reversibly.
       * This is reversible, so any information in (a,b,c) before mix() is
       * still in (a,b,c) after mix().
       *
       * If four pairs of (a,b,c) inputs are run through mix(), or through
       * mix() in reverse, there are at least 32 bits of the output that
       * are sometimes the same for one pair and different for another pair.
       *
       * This was tested for:
       * - pairs that differed by one bit, by two bits, in any combination
       *   of top bits of (a,b,c), or in any combination of bottom bits of
       *   (a,b,c).
       * - "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
       *   the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
       *    is commonly produced by subtraction) look like a single 1-bit
       *    difference.
       * - the base values were pseudorandom, all zero but one bit set, or
       *   all zero plus a counter that starts at zero.
       *
       * Some k values for my "a-=c; a^=rot(c,k); c+=b;" arrangement that
       * satisfy this are
       *     4  6  8 16 19  4
       *     9 15  3 18 27 15
       *    14  9  3  7 17  3
       * Well, "9 15 3 18 27 15" didn't quite get 32 bits diffing for
       * "differ" defined as + with a one-bit base and a two-bit delta.  I
       * used http://burtleburtle.net/bob/hash/avalanche.html to choose
       * the operations, constants, and arrangements of the variables.
       *
       * This does not achieve avalanche.  There are input bits of (a,b,c)
       * that fail to affect some output bits of (a,b,c), especially of a.
       * The most thoroughly mixed value is c, but it doesn't really even
       * achieve avalanche in c.
       *
       * This allows some parallelism.  Read-after-writes are good at doubling
       * the number of bits affected, so the goal of mixing pulls in the
       * opposite direction as the goal of parallelism.  I did what I could.
       * Rotates seem to cost as much as shifts on every machine I could lay
       * my hands on, and rotates are much kinder to the top and bottom bits,
       * so I used rotates.
       *
       * #define mix(a,b,c) \
       * { \
       *   a -= c;  a ^= rot(c, 4);  c += b; \
       *   b -= a;  b ^= rot(a, 6);  a += c; \
       *   c -= b;  c ^= rot(b, 8);  b += a; \
       *   a -= c;  a ^= rot(c,16);  c += b; \
       *   b -= a;  b ^= rot(a,19);  a += c; \
       *   c -= b;  c ^= rot(b, 4);  b += a; \
       * }
       *
       * mix(a,b,c);
       */
      a = (a - c) & INT_MASK;
      a ^= rot(c, 4);
      c = (c + b) & INT_MASK;
      b = (b - a) & INT_MASK;
      b ^= rot(a, 6);
      a = (a + c) & INT_MASK;
      c = (c - b) & INT_MASK;
      c ^= rot(b, 8);
      b = (b + a) & INT_MASK;
      a = (a - c) & INT_MASK;
      a ^= rot(c,16);
      c = (c + b) & INT_MASK;
      b = (b - a) & INT_MASK;
      b ^= rot(a,19);
      a = (a + c) & INT_MASK;
      c = (c - b) & INT_MASK;
      c ^= rot(b, 4);
      b = (b + a) & INT_MASK;
    }

    //-------------------------------- last block: affect all 32 bits of (c)
    switch (length) {                   // all the case statements fall through
      case 12:
        c = (c + (((key[offset + 11] & BYTE_MASK) << 24) & INT_MASK)) & INT_MASK;
      case 11:
        c = (c + (((key[offset + 10] & BYTE_MASK) << 16) & INT_MASK)) & INT_MASK;
      case 10:
        c = (c + (((key[offset + 9]  & BYTE_MASK) <<  8) & INT_MASK)) & INT_MASK;
      case  9:
        c = (c + (key[offset + 8]    & BYTE_MASK)) & INT_MASK;
      case  8:
        b = (b + (((key[offset + 7]  & BYTE_MASK) << 24) & INT_MASK)) & INT_MASK;
      case  7:
        b = (b + (((key[offset + 6]  & BYTE_MASK) << 16) & INT_MASK)) & INT_MASK;
      case  6:
        b = (b + (((key[offset + 5]  & BYTE_MASK) <<  8) & INT_MASK)) & INT_MASK;
      case  5:
        b = (b + (key[offset + 4]    & BYTE_MASK)) & INT_MASK;
      case  4:
        a = (a + (((key[offset + 3]  & BYTE_MASK) << 24) & INT_MASK)) & INT_MASK;
      case  3:
        a = (a + (((key[offset + 2]  & BYTE_MASK) << 16) & INT_MASK)) & INT_MASK;
      case  2:
        a = (a + (((key[offset + 1]  & BYTE_MASK) <<  8) & INT_MASK)) & INT_MASK;
      case  1:
        a = (a + (key[offset] & BYTE_MASK)) & INT_MASK;
        break;
      case  0:
        return (int)(c & INT_MASK);
    }
    /*
     * final -- final mixing of 3 32-bit values (a,b,c) into c
     *
     * Pairs of (a,b,c) values differing in only a few bits will usually
     * produce values of c that look totally different.  This was tested for
     * - pairs that differed by one bit, by two bits, in any combination
     *   of top bits of (a,b,c), or in any combination of bottom bits of
     *   (a,b,c).
     *
     * - "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
     *   the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
     *   is commonly produced by subtraction) look like a single 1-bit
     *   difference.
     *
     * - the base values were pseudorandom, all zero but one bit set, or
     *   all zero plus a counter that starts at zero.
     *
     * These constants passed:
     *   14 11 25 16 4 14 24
     *   12 14 25 16 4 14 24
     * and these came close:
     *    4  8 15 26 3 22 24
     *   10  8 15 26 3 22 24
     *   11  8 15 26 3 22 24
     *
     * #define final(a,b,c) \
     * {
     *   c ^= b; c -= rot(b,14); \
     *   a ^= c; a -= rot(c,11); \
     *   b ^= a; b -= rot(a,25); \
     *   c ^= b; c -= rot(b,16); \
     *   a ^= c; a -= rot(c,4);  \
     *   b ^= a; b -= rot(a,14); \
     *   c ^= b; c -= rot(b,24); \
     * }
     *
     */
    c ^= b;
    c = (c - rot(b,14)) & INT_MASK;
    a ^= c;
    a = (a - rot(c,11)) & INT_MASK;
    b ^= a;
    b = (b - rot(a,25)) & INT_MASK;
    c ^= b;
    c = (c - rot(b,16)) & INT_MASK;
    a ^= c;
    a = (a - rot(c,4))  & INT_MASK;
    b ^= a;
    b = (b - rot(a,14)) & INT_MASK;
    c ^= b;
    c = (c - rot(b,24)) & INT_MASK;

    return (int)(c & INT_MASK);
  }

  private static long rot(long val, int pos) {
    return ((Integer.rotateLeft(
        (int)(val & INT_MASK), pos)) & INT_MASK);
  }
}
