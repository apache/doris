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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.orc.CompressionCodec;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.writer.StreamOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.Arrays;
import java.util.TimeZone;

public final class SerializationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SerializationUtils.class);

  private static final int BUFFER_SIZE = 64;
  private final byte[] readBuffer;
  private final byte[] writeBuffer;

  /**
   * Buffer for histogram that store the encoded bit requirement for each bit
   * size. Maximum number of discrete bits that can encoded is ordinal of
   * FixedBitSizes.
   *
   * @see FixedBitSizes
   */
  private final int[] histBuffer = new int[32];

  public SerializationUtils() {
    this.readBuffer = new byte[BUFFER_SIZE];
    this.writeBuffer = new byte[BUFFER_SIZE];
  }

  public void writeVulong(OutputStream output,
                          long value) throws IOException {
    int posn = 0;
    while (true) {
      if ((value & ~0x7f) == 0) {
        writeBuffer[posn++] = (byte) value;
        break;
      } else {
        writeBuffer[posn++] = (byte)(0x80 | (value & 0x7f));
        value >>>= 7;
      }
    }
    output.write(writeBuffer, 0, posn);
  }

  public void writeVslong(OutputStream output,
                          long value) throws IOException {
    writeVulong(output, (value << 1) ^ (value >> 63));
  }


  public static long readVulong(InputStream in) throws IOException {
    long result = 0;
    long b;
    int offset = 0;
    do {
      b = in.read();
      if (b == -1) {
        throw new EOFException("Reading Vulong past EOF");
      }
      result |= (0x7f & b) << offset;
      offset += 7;
    } while (b >= 0x80);
    return result;
  }

  public static long readVslong(InputStream in) throws IOException {
    long result = readVulong(in);
    return (result >>> 1) ^ -(result & 1);
  }

  public float readFloat(InputStream in) throws IOException {
    readFully(in, readBuffer, 4);
    int val = (((readBuffer[0] & 0xff) << 0)
        + ((readBuffer[1] & 0xff) << 8)
        + ((readBuffer[2] & 0xff) << 16)
        + ((readBuffer[3] & 0xff) << 24));
    return Float.intBitsToFloat(val);
  }

  public void skipFloat(InputStream in, int numOfFloats) throws IOException {
    IOUtils.skipFully(in, numOfFloats * 4L);
  }

  public void writeFloat(OutputStream output,
                         float value) throws IOException {
    int ser = Float.floatToIntBits(value);
    writeBuffer[0] = (byte) ((ser >> 0)  & 0xff);
    writeBuffer[1] = (byte) ((ser >> 8)  & 0xff);
    writeBuffer[2] = (byte) ((ser >> 16) & 0xff);
    writeBuffer[3] = (byte) ((ser >> 24) & 0xff);
    output.write(writeBuffer, 0, 4);
  }

  public double readDouble(InputStream in) throws IOException {
    return Double.longBitsToDouble(readLongLE(in));
  }

  public long readLongLE(InputStream in) throws IOException {
    readFully(in, readBuffer, 8);
    return (((readBuffer[0] & 0xff) << 0)
        + ((readBuffer[1] & 0xff) << 8)
        + ((readBuffer[2] & 0xff) << 16)
        + ((long) (readBuffer[3] & 0xff) << 24)
        + ((long) (readBuffer[4] & 0xff) << 32)
        + ((long) (readBuffer[5] & 0xff) << 40)
        + ((long) (readBuffer[6] & 0xff) << 48)
        + ((long) (readBuffer[7] & 0xff) << 56));
  }

  private void readFully(final InputStream in, final byte[] buffer, int len)
      throws IOException {
    int offset = 0;
    for (;;) {
      final int n = in.read(buffer, offset, len);
      if (n == len) {
        return;
      }
      if (n < 0) {
        throw new EOFException("Read past EOF for " + in);
      }
      offset += n;
      len -= n;
    }
  }

  public void skipDouble(InputStream in, int numOfDoubles) throws IOException {
    IOUtils.skipFully(in, numOfDoubles * 8L);
  }

  public void writeDouble(OutputStream output, double value)
      throws IOException {
    final long bits = Double.doubleToLongBits(value);
    final int first = (int) (bits & 0xFFFFFFFF);
    final int second = (int) ((bits >>> 32) & 0xFFFFFFFF);
    // Implementation taken from Apache Avro (org.apache.avro.io.BinaryData)
    // the compiler seems to execute this order the best, likely due to
    // register allocation -- the lifetime of constants is minimized.
    writeBuffer[0] = (byte) (first);
    writeBuffer[4] = (byte) (second);
    writeBuffer[5] = (byte) (second >>> 8);
    writeBuffer[1] = (byte) (first >>> 8);
    writeBuffer[2] = (byte) (first >>> 16);
    writeBuffer[6] = (byte) (second >>> 16);
    writeBuffer[7] = (byte) (second >>> 24);
    writeBuffer[3] = (byte) (first >>> 24);
    output.write(writeBuffer, 0, 8);
  }

  /**
   * Write the arbitrarily sized signed BigInteger in vint format.
   *
   * Signed integers are encoded using the low bit as the sign bit using zigzag
   * encoding.
   *
   * Each byte uses the low 7 bits for data and the high bit for stop/continue.
   *
   * Bytes are stored LSB first.
   * @param output the stream to write to
   * @param value the value to output
   * @throws IOException
   */
  public static void writeBigInteger(OutputStream output,
                                     BigInteger value) throws IOException {
    // encode the signed number as a positive integer
    value = value.shiftLeft(1);
    int sign = value.signum();
    if (sign < 0) {
      value = value.negate();
      value = value.subtract(BigInteger.ONE);
    }
    int length = value.bitLength();
    while (true) {
      long lowBits = value.longValue() & 0x7fffffffffffffffL;
      length -= 63;
      // write out the next 63 bits worth of data
      for(int i=0; i < 9; ++i) {
        // if this is the last byte, leave the high bit off
        if (length <= 0 && (lowBits & ~0x7f) == 0) {
          output.write((byte) lowBits);
          return;
        } else {
          output.write((byte) (0x80 | (lowBits & 0x7f)));
          lowBits >>>= 7;
        }
      }
      value = value.shiftRight(63);
    }
  }

  /**
   * Read the signed arbitrary sized BigInteger BigInteger in vint format
   * @param input the stream to read from
   * @return the read BigInteger
   * @throws IOException
   */
  public static BigInteger readBigInteger(InputStream input) throws IOException {
    BigInteger result = BigInteger.ZERO;
    long work = 0;
    int offset = 0;
    long b;
    do {
      b = input.read();
      if (b == -1) {
        throw new EOFException("Reading BigInteger past EOF from " + input);
      }
      work |= (0x7f & b) << (offset % 63);
      offset += 7;
      // if we've read 63 bits, roll them into the result
      if (offset == 63) {
        result = BigInteger.valueOf(work);
        work = 0;
      } else if (offset % 63 == 0) {
        result = result.or(BigInteger.valueOf(work).shiftLeft(offset-63));
        work = 0;
      }
    } while (b >= 0x80);
    if (work != 0) {
      result = result.or(BigInteger.valueOf(work).shiftLeft((offset/63)*63));
    }
    // convert back to a signed number
    boolean isNegative = result.testBit(0);
    if (isNegative) {
      result = result.add(BigInteger.ONE);
      result = result.negate();
    }
    result = result.shiftRight(1);
    return result;
  }

  public enum FixedBitSizes {
    ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE,
    THIRTEEN, FOURTEEN, FIFTEEN, SIXTEEN, SEVENTEEN, EIGHTEEN, NINETEEN,
    TWENTY, TWENTYONE, TWENTYTWO, TWENTYTHREE, TWENTYFOUR, TWENTYSIX,
    TWENTYEIGHT, THIRTY, THIRTYTWO, FORTY, FORTYEIGHT, FIFTYSIX, SIXTYFOUR;
  }

  /**
   * Count the number of bits required to encode the given value
   * @param value
   * @return bits required to store value
   */
  public int findClosestNumBits(long value) {
    return getClosestFixedBits(findNumBits(value));
  }

  private int findNumBits(long value) {
    return 64 - Long.numberOfLeadingZeros(value);
  }

  /**
   * zigzag encode the given value
   * @param val
   * @return zigzag encoded value
   */
  public long zigzagEncode(long val) {
    return (val << 1) ^ (val >> 63);
  }

  /**
   * zigzag decode the given value
   * @param val
   * @return zizag decoded value
   */
  public long zigzagDecode(long val) {
    return (val >>> 1) ^ -(val & 1);
  }

  /**
   * Compute the bits required to represent pth percentile value
   * @param data - array
   * @param p - percentile value (&gt;=0.0 to &lt;=1.0)
   * @return pth percentile bits
   */
  public int percentileBits(long[] data, int offset, int length, double p) {
    if ((p > 1.0) || (p <= 0.0)) {
      return -1;
    }

    Arrays.fill(this.histBuffer, 0);

    // compute the histogram
    for(int i = offset; i < (offset + length); i++) {
      int idx = encodeBitWidth(findNumBits(data[i]));
      this.histBuffer[idx] += 1;
    }

    int perLen = (int) (length * (1.0 - p));

    // return the bits required by pth percentile length
    for(int i = this.histBuffer.length - 1; i >= 0; i--) {
      perLen -= this.histBuffer[i];
      if (perLen < 0) {
        return decodeBitWidth(i);
      }
    }

    return 0;
  }

  /**
   * Read n bytes in big endian order and convert to long
   * @return long value
   */
  public long bytesToLongBE(InStream input, int n) throws IOException {
    long out = 0;
    long val = 0;
    while (n > 0) {
      n--;
      // store it in a long and then shift else integer overflow will occur
      val = input.read();
      out |= (val << (n * 8));
    }
    return out;
  }

  /**
   * Calculate the number of bytes required
   * @param n - number of values
   * @param numBits - bit width
   * @return number of bytes required
   */
  int getTotalBytesRequired(int n, int numBits) {
    return (n * numBits + 7) / 8;
  }

  /**
   * For a given fixed bit this function will return the closest available fixed
   * bit
   * @param n
   * @return closest valid fixed bit
   */
  public int getClosestFixedBits(int n) {
    if (n == 0) {
      return 1;
    }
    if (n <= 24) {
      return n;
    }
    if (n <= 26) {
      return 26;
    }
    if (n <= 28) {
      return 28;
    }
    if (n <= 30) {
      return 30;
    }
    if (n <= 32) {
      return 32;
    }
    if (n <= 40) {
      return 40;
    }
    if (n <= 48) {
      return 48;
    }
    if (n <= 56) {
      return 56;
    }
    return 64;
  }

  public int getClosestAlignedFixedBits(int n) {
    if (n == 0 ||  n == 1) {
      return 1;
    } else if (n > 1 && n <= 2) {
      return 2;
    } else if (n > 2 && n <= 4) {
      return 4;
    } else if (n > 4 && n <= 8) {
      return 8;
    } else if (n > 8 && n <= 16) {
      return 16;
    } else if (n > 16 && n <= 24) {
      return 24;
    } else if (n > 24 && n <= 32) {
      return 32;
    } else if (n > 32 && n <= 40) {
      return 40;
    } else if (n > 40 && n <= 48) {
      return 48;
    } else if (n > 48 && n <= 56) {
      return 56;
    } else {
      return 64;
    }
  }

  /**
   * Finds the closest available fixed bit width match and returns its encoded
   * value (ordinal).
   *
   * @param n fixed bit width to encode
   * @return encoded fixed bit width
   */
  public int encodeBitWidth(int n) {
    n = getClosestFixedBits(n);

    if (n >= 1 && n <= 24) {
      return n - 1;
    }
    if (n <= 26) {
      return FixedBitSizes.TWENTYSIX.ordinal();
    }
    if (n <= 28) {
      return FixedBitSizes.TWENTYEIGHT.ordinal();
    }
    if (n <= 30) {
      return FixedBitSizes.THIRTY.ordinal();
    }
    if (n <= 32) {
      return FixedBitSizes.THIRTYTWO.ordinal();
    }
    if (n <= 40) {
      return FixedBitSizes.FORTY.ordinal();
    }
    if (n <= 48) {
      return FixedBitSizes.FORTYEIGHT.ordinal();
    }
    if (n <= 56) {
      return FixedBitSizes.FIFTYSIX.ordinal();
    }
    return FixedBitSizes.SIXTYFOUR.ordinal();
  }

  /**
   * Decodes the ordinal fixed bit value to actual fixed bit width value
   * @param n - encoded fixed bit width
   * @return decoded fixed bit width
   */
  public static int decodeBitWidth(int n) {
    if (n >= FixedBitSizes.ONE.ordinal() &&
        n <= FixedBitSizes.TWENTYFOUR.ordinal()) {
      return n + 1;
    } else if (n == FixedBitSizes.TWENTYSIX.ordinal()) {
      return 26;
    } else if (n == FixedBitSizes.TWENTYEIGHT.ordinal()) {
      return 28;
    } else if (n == FixedBitSizes.THIRTY.ordinal()) {
      return 30;
    } else if (n == FixedBitSizes.THIRTYTWO.ordinal()) {
      return 32;
    } else if (n == FixedBitSizes.FORTY.ordinal()) {
      return 40;
    } else if (n == FixedBitSizes.FORTYEIGHT.ordinal()) {
      return 48;
    } else if (n == FixedBitSizes.FIFTYSIX.ordinal()) {
      return 56;
    } else {
      return 64;
    }
  }

  /**
   * Bitpack and write the input values to underlying output stream
   * @param input - values to write
   * @param offset - offset
   * @param len - length
   * @param bitSize - bit width
   * @param output - output stream
   * @throws IOException
   */
  public void writeInts(long[] input, int offset, int len, int bitSize,
                        OutputStream output) throws IOException {
    if (input == null || input.length < 1 || offset < 0 || len < 1 || bitSize < 1) {
      return;
    }

    switch (bitSize) {
      case 1:
        unrolledBitPack1(input, offset, len, output);
        return;
      case 2:
        unrolledBitPack2(input, offset, len, output);
        return;
      case 4:
        unrolledBitPack4(input, offset, len, output);
        return;
      case 8:
        unrolledBitPack8(input, offset, len, output);
        return;
      case 16:
        unrolledBitPack16(input, offset, len, output);
        return;
      case 24:
        unrolledBitPack24(input, offset, len, output);
        return;
      case 32:
        unrolledBitPack32(input, offset, len, output);
        return;
      case 40:
        unrolledBitPack40(input, offset, len, output);
        return;
      case 48:
        unrolledBitPack48(input, offset, len, output);
        return;
      case 56:
        unrolledBitPack56(input, offset, len, output);
        return;
      case 64:
        unrolledBitPack64(input, offset, len, output);
        return;
      default:
        break;
    }

    int bitsLeft = 8;
    byte current = 0;
    for(int i = offset; i < (offset + len); i++) {
      long value = input[i];
      int bitsToWrite = bitSize;
      while (bitsToWrite > bitsLeft) {
        // add the bits to the bottom of the current word
        current |= value >>> (bitsToWrite - bitsLeft);
        // subtract out the bits we just added
        bitsToWrite -= bitsLeft;
        // zero out the bits above bitsToWrite
        value &= (1L << bitsToWrite) - 1;
        output.write(current);
        current = 0;
        bitsLeft = 8;
      }
      bitsLeft -= bitsToWrite;
      current |= value << bitsLeft;
      if (bitsLeft == 0) {
        output.write(current);
        current = 0;
        bitsLeft = 8;
      }
    }

    // flush
    if (bitsLeft != 8) {
      output.write(current);
      current = 0;
      bitsLeft = 8;
    }
  }

  private void unrolledBitPack1(long[] input, int offset, int len,
      OutputStream output) throws IOException {
    final int numHops = 8;
    final int remainder = len % numHops;
    final int endOffset = offset + len;
    final int endUnroll = endOffset - remainder;
    int val = 0;
    for (int i = offset; i < endUnroll; i = i + numHops) {
      val = (int) (val | ((input[i] & 1) << 7)
          | ((input[i + 1] & 1) << 6)
          | ((input[i + 2] & 1) << 5)
          | ((input[i + 3] & 1) << 4)
          | ((input[i + 4] & 1) << 3)
          | ((input[i + 5] & 1) << 2)
          | ((input[i + 6] & 1) << 1)
          | (input[i + 7]) & 1);
      output.write(val);
      val = 0;
    }

    if (remainder > 0) {
      int startShift = 7;
      for (int i = endUnroll; i < endOffset; i++) {
        val = (int) (val | (input[i] & 1) << startShift);
        startShift -= 1;
      }
      output.write(val);
    }
  }

  private void unrolledBitPack2(long[] input, int offset, int len,
      OutputStream output) throws IOException {
    final int numHops = 4;
    final int remainder = len % numHops;
    final int endOffset = offset + len;
    final int endUnroll = endOffset - remainder;
    int val = 0;
    for (int i = offset; i < endUnroll; i = i + numHops) {
      val = (int) (val | ((input[i] & 3) << 6)
          | ((input[i + 1] & 3) << 4)
          | ((input[i + 2] & 3) << 2)
          | (input[i + 3]) & 3);
      output.write(val);
      val = 0;
    }

    if (remainder > 0) {
      int startShift = 6;
      for (int i = endUnroll; i < endOffset; i++) {
        val = (int) (val | (input[i] & 3) << startShift);
        startShift -= 2;
      }
      output.write(val);
    }
  }

  private void unrolledBitPack4(long[] input, int offset, int len,
      OutputStream output) throws IOException {
    final int numHops = 2;
    final int remainder = len % numHops;
    final int endOffset = offset + len;
    final int endUnroll = endOffset - remainder;
    int val = 0;
    for (int i = offset; i < endUnroll; i = i + numHops) {
      val = (int) (val | ((input[i] & 15) << 4) | (input[i + 1]) & 15);
      output.write(val);
      val = 0;
    }

    if (remainder > 0) {
      int startShift = 4;
      for (int i = endUnroll; i < endOffset; i++) {
        val = (int) (val | (input[i] & 15) << startShift);
        startShift -= 4;
      }
      output.write(val);
    }
  }

  private void unrolledBitPack8(long[] input, int offset, int len,
      OutputStream output) throws IOException {
    unrolledBitPackBytes(input, offset, len, output, 1);
  }

  private void unrolledBitPack16(long[] input, int offset, int len,
      OutputStream output) throws IOException {
    unrolledBitPackBytes(input, offset, len, output, 2);
  }

  private void unrolledBitPack24(long[] input, int offset, int len,
      OutputStream output) throws IOException {
    unrolledBitPackBytes(input, offset, len, output, 3);
  }

  private void unrolledBitPack32(long[] input, int offset, int len,
      OutputStream output) throws IOException {
    unrolledBitPackBytes(input, offset, len, output, 4);
  }

  private void unrolledBitPack40(long[] input, int offset, int len,
      OutputStream output) throws IOException {
    unrolledBitPackBytes(input, offset, len, output, 5);
  }

  private void unrolledBitPack48(long[] input, int offset, int len,
      OutputStream output) throws IOException {
    unrolledBitPackBytes(input, offset, len, output, 6);
  }

  private void unrolledBitPack56(long[] input, int offset, int len,
      OutputStream output) throws IOException {
    unrolledBitPackBytes(input, offset, len, output, 7);
  }

  private void unrolledBitPack64(long[] input, int offset, int len,
      OutputStream output) throws IOException {
    unrolledBitPackBytes(input, offset, len, output, 8);
  }

  private void unrolledBitPackBytes(long[] input, int offset, int len,
      OutputStream output, int numBytes) throws IOException {
    final int numHops = 8;
    final int remainder = len % numHops;
    final int endOffset = offset + len;
    final int endUnroll = endOffset - remainder;
    int i = offset;
    for (; i < endUnroll; i = i + numHops) {
      writeLongBE(output, input, i, numHops, numBytes);
    }

    if (remainder > 0) {
      writeRemainingLongs(output, i, input, remainder, numBytes);
    }
  }

  private void writeRemainingLongs(OutputStream output, int offset, long[] input, int remainder,
      int numBytes) throws IOException {
    final int numHops = remainder;

    int idx = 0;
    switch (numBytes) {
      case 1:
        while (remainder > 0) {
          writeBuffer[idx] = (byte) (input[offset + idx] & 255);
          remainder--;
          idx++;
        }
        break;
      case 2:
        while (remainder > 0) {
          writeLongBE2(output, input[offset + idx], idx * 2);
          remainder--;
          idx++;
        }
        break;
      case 3:
        while (remainder > 0) {
          writeLongBE3(output, input[offset + idx], idx * 3);
          remainder--;
          idx++;
        }
        break;
      case 4:
        while (remainder > 0) {
          writeLongBE4(output, input[offset + idx], idx * 4);
          remainder--;
          idx++;
        }
        break;
      case 5:
        while (remainder > 0) {
          writeLongBE5(output, input[offset + idx], idx * 5);
          remainder--;
          idx++;
        }
        break;
      case 6:
        while (remainder > 0) {
          writeLongBE6(output, input[offset + idx], idx * 6);
          remainder--;
          idx++;
        }
        break;
      case 7:
        while (remainder > 0) {
          writeLongBE7(output, input[offset + idx], idx * 7);
          remainder--;
          idx++;
        }
        break;
      case 8:
        while (remainder > 0) {
          writeLongBE8(output, input[offset + idx], idx * 8);
          remainder--;
          idx++;
        }
        break;
      default:
        break;
    }

    final int toWrite = numHops * numBytes;
    output.write(writeBuffer, 0, toWrite);
  }

  private void writeLongBE(OutputStream output, long[] input, int offset,
                           int numHops, int numBytes) throws IOException {

    switch (numBytes) {
      case 1:
        writeBuffer[0] = (byte) (input[offset + 0] & 255);
        writeBuffer[1] = (byte) (input[offset + 1] & 255);
        writeBuffer[2] = (byte) (input[offset + 2] & 255);
        writeBuffer[3] = (byte) (input[offset + 3] & 255);
        writeBuffer[4] = (byte) (input[offset + 4] & 255);
        writeBuffer[5] = (byte) (input[offset + 5] & 255);
        writeBuffer[6] = (byte) (input[offset + 6] & 255);
        writeBuffer[7] = (byte) (input[offset + 7] & 255);
        break;
      case 2:
        writeLongBE2(output, input[offset + 0], 0);
        writeLongBE2(output, input[offset + 1], 2);
        writeLongBE2(output, input[offset + 2], 4);
        writeLongBE2(output, input[offset + 3], 6);
        writeLongBE2(output, input[offset + 4], 8);
        writeLongBE2(output, input[offset + 5], 10);
        writeLongBE2(output, input[offset + 6], 12);
        writeLongBE2(output, input[offset + 7], 14);
        break;
      case 3:
        writeLongBE3(output, input[offset + 0], 0);
        writeLongBE3(output, input[offset + 1], 3);
        writeLongBE3(output, input[offset + 2], 6);
        writeLongBE3(output, input[offset + 3], 9);
        writeLongBE3(output, input[offset + 4], 12);
        writeLongBE3(output, input[offset + 5], 15);
        writeLongBE3(output, input[offset + 6], 18);
        writeLongBE3(output, input[offset + 7], 21);
        break;
      case 4:
        writeLongBE4(output, input[offset + 0], 0);
        writeLongBE4(output, input[offset + 1], 4);
        writeLongBE4(output, input[offset + 2], 8);
        writeLongBE4(output, input[offset + 3], 12);
        writeLongBE4(output, input[offset + 4], 16);
        writeLongBE4(output, input[offset + 5], 20);
        writeLongBE4(output, input[offset + 6], 24);
        writeLongBE4(output, input[offset + 7], 28);
        break;
      case 5:
        writeLongBE5(output, input[offset + 0], 0);
        writeLongBE5(output, input[offset + 1], 5);
        writeLongBE5(output, input[offset + 2], 10);
        writeLongBE5(output, input[offset + 3], 15);
        writeLongBE5(output, input[offset + 4], 20);
        writeLongBE5(output, input[offset + 5], 25);
        writeLongBE5(output, input[offset + 6], 30);
        writeLongBE5(output, input[offset + 7], 35);
        break;
      case 6:
        writeLongBE6(output, input[offset + 0], 0);
        writeLongBE6(output, input[offset + 1], 6);
        writeLongBE6(output, input[offset + 2], 12);
        writeLongBE6(output, input[offset + 3], 18);
        writeLongBE6(output, input[offset + 4], 24);
        writeLongBE6(output, input[offset + 5], 30);
        writeLongBE6(output, input[offset + 6], 36);
        writeLongBE6(output, input[offset + 7], 42);
        break;
      case 7:
        writeLongBE7(output, input[offset + 0], 0);
        writeLongBE7(output, input[offset + 1], 7);
        writeLongBE7(output, input[offset + 2], 14);
        writeLongBE7(output, input[offset + 3], 21);
        writeLongBE7(output, input[offset + 4], 28);
        writeLongBE7(output, input[offset + 5], 35);
        writeLongBE7(output, input[offset + 6], 42);
        writeLongBE7(output, input[offset + 7], 49);
        break;
      case 8:
        writeLongBE8(output, input[offset + 0], 0);
        writeLongBE8(output, input[offset + 1], 8);
        writeLongBE8(output, input[offset + 2], 16);
        writeLongBE8(output, input[offset + 3], 24);
        writeLongBE8(output, input[offset + 4], 32);
        writeLongBE8(output, input[offset + 5], 40);
        writeLongBE8(output, input[offset + 6], 48);
        writeLongBE8(output, input[offset + 7], 56);
        break;
      default:
        break;
    }

    final int toWrite = numHops * numBytes;
    output.write(writeBuffer, 0, toWrite);
  }

  private void writeLongBE2(OutputStream output, long val, int wbOffset) {
    writeBuffer[wbOffset + 0] =  (byte) (val >>> 8);
    writeBuffer[wbOffset + 1] =  (byte) (val >>> 0);
  }

  private void writeLongBE3(OutputStream output, long val, int wbOffset) {
    writeBuffer[wbOffset + 0] =  (byte) (val >>> 16);
    writeBuffer[wbOffset + 1] =  (byte) (val >>> 8);
    writeBuffer[wbOffset + 2] =  (byte) (val >>> 0);
  }

  private void writeLongBE4(OutputStream output, long val, int wbOffset) {
    writeBuffer[wbOffset + 0] =  (byte) (val >>> 24);
    writeBuffer[wbOffset + 1] =  (byte) (val >>> 16);
    writeBuffer[wbOffset + 2] =  (byte) (val >>> 8);
    writeBuffer[wbOffset + 3] =  (byte) (val >>> 0);
  }

  private void writeLongBE5(OutputStream output, long val, int wbOffset) {
    writeBuffer[wbOffset + 0] =  (byte) (val >>> 32);
    writeBuffer[wbOffset + 1] =  (byte) (val >>> 24);
    writeBuffer[wbOffset + 2] =  (byte) (val >>> 16);
    writeBuffer[wbOffset + 3] =  (byte) (val >>> 8);
    writeBuffer[wbOffset + 4] =  (byte) (val >>> 0);
  }

  private void writeLongBE6(OutputStream output, long val, int wbOffset) {
    writeBuffer[wbOffset + 0] =  (byte) (val >>> 40);
    writeBuffer[wbOffset + 1] =  (byte) (val >>> 32);
    writeBuffer[wbOffset + 2] =  (byte) (val >>> 24);
    writeBuffer[wbOffset + 3] =  (byte) (val >>> 16);
    writeBuffer[wbOffset + 4] =  (byte) (val >>> 8);
    writeBuffer[wbOffset + 5] =  (byte) (val >>> 0);
  }

  private void writeLongBE7(OutputStream output, long val, int wbOffset) {
    writeBuffer[wbOffset + 0] =  (byte) (val >>> 48);
    writeBuffer[wbOffset + 1] =  (byte) (val >>> 40);
    writeBuffer[wbOffset + 2] =  (byte) (val >>> 32);
    writeBuffer[wbOffset + 3] =  (byte) (val >>> 24);
    writeBuffer[wbOffset + 4] =  (byte) (val >>> 16);
    writeBuffer[wbOffset + 5] =  (byte) (val >>> 8);
    writeBuffer[wbOffset + 6] =  (byte) (val >>> 0);
  }

  private void writeLongBE8(OutputStream output, long val, int wbOffset) {
    writeBuffer[wbOffset + 0] =  (byte) (val >>> 56);
    writeBuffer[wbOffset + 1] =  (byte) (val >>> 48);
    writeBuffer[wbOffset + 2] =  (byte) (val >>> 40);
    writeBuffer[wbOffset + 3] =  (byte) (val >>> 32);
    writeBuffer[wbOffset + 4] =  (byte) (val >>> 24);
    writeBuffer[wbOffset + 5] =  (byte) (val >>> 16);
    writeBuffer[wbOffset + 6] =  (byte) (val >>> 8);
    writeBuffer[wbOffset + 7] =  (byte) (val >>> 0);
  }

  /**
   * Read bitpacked integers from input stream
   * @param buffer - input buffer
   * @param offset - offset
   * @param len - length
   * @param bitSize - bit width
   * @param input - input stream
   * @throws IOException
   */
  public void readInts(long[] buffer, int offset, int len, int bitSize,
                       InStream input) throws IOException {
    int bitsLeft = 0;
    int current = 0;

    switch (bitSize) {
      case 1:
        unrolledUnPack1(buffer, offset, len, input);
        return;
      case 2:
        unrolledUnPack2(buffer, offset, len, input);
        return;
      case 4:
        unrolledUnPack4(buffer, offset, len, input);
        return;
      case 8:
        unrolledUnPack8(buffer, offset, len, input);
        return;
      case 16:
        unrolledUnPack16(buffer, offset, len, input);
        return;
      case 24:
        unrolledUnPack24(buffer, offset, len, input);
        return;
      case 32:
        unrolledUnPack32(buffer, offset, len, input);
        return;
      case 40:
        unrolledUnPack40(buffer, offset, len, input);
        return;
      case 48:
        unrolledUnPack48(buffer, offset, len, input);
        return;
      case 56:
        unrolledUnPack56(buffer, offset, len, input);
        return;
      case 64:
        unrolledUnPack64(buffer, offset, len, input);
        return;
      default:
        break;
    }

    for(int i = offset; i < (offset + len); i++) {
      long result = 0;
      int bitsLeftToRead = bitSize;
      while (bitsLeftToRead > bitsLeft) {
        result <<= bitsLeft;
        result |= current & ((1 << bitsLeft) - 1);
        bitsLeftToRead -= bitsLeft;
        current = input.read();
        bitsLeft = 8;
      }

      // handle the left over bits
      if (bitsLeftToRead > 0) {
        result <<= bitsLeftToRead;
        bitsLeft -= bitsLeftToRead;
        result |= (current >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
      }
      buffer[i] = result;
    }
  }


  private void unrolledUnPack1(long[] buffer, int offset, int len,
      InStream input) throws IOException {
    final int numHops = 8;
    final int remainder = len % numHops;
    final int endOffset = offset + len;
    final int endUnroll = endOffset - remainder;
    int val = 0;
    for (int i = offset; i < endUnroll; i = i + numHops) {
      val = input.read();
      buffer[i] = (val >>> 7) & 1;
      buffer[i + 1] = (val >>> 6) & 1;
      buffer[i + 2] = (val >>> 5) & 1;
      buffer[i + 3] = (val >>> 4) & 1;
      buffer[i + 4] = (val >>> 3) & 1;
      buffer[i + 5] = (val >>> 2) & 1;
      buffer[i + 6] = (val >>> 1) & 1;
      buffer[i + 7] = val & 1;
    }

    if (remainder > 0) {
      int startShift = 7;
      val = input.read();
      for (int i = endUnroll; i < endOffset; i++) {
        buffer[i] = (val >>> startShift) & 1;
        startShift -= 1;
      }
    }
  }

  private void unrolledUnPack2(long[] buffer, int offset, int len,
      InStream input) throws IOException {
    final int numHops = 4;
    final int remainder = len % numHops;
    final int endOffset = offset + len;
    final int endUnroll = endOffset - remainder;
    int val = 0;
    for (int i = offset; i < endUnroll; i = i + numHops) {
      val = input.read();
      buffer[i] = (val >>> 6) & 3;
      buffer[i + 1] = (val >>> 4) & 3;
      buffer[i + 2] = (val >>> 2) & 3;
      buffer[i + 3] = val & 3;
    }

    if (remainder > 0) {
      int startShift = 6;
      val = input.read();
      for (int i = endUnroll; i < endOffset; i++) {
        buffer[i] = (val >>> startShift) & 3;
        startShift -= 2;
      }
    }
  }

  private void unrolledUnPack4(long[] buffer, int offset, int len,
      InStream input) throws IOException {
    final int numHops = 2;
    final int remainder = len % numHops;
    final int endOffset = offset + len;
    final int endUnroll = endOffset - remainder;
    int val = 0;
    for (int i = offset; i < endUnroll; i = i + numHops) {
      val = input.read();
      buffer[i] = (val >>> 4) & 15;
      buffer[i + 1] = val & 15;
    }

    if (remainder > 0) {
      int startShift = 4;
      val = input.read();
      for (int i = endUnroll; i < endOffset; i++) {
        buffer[i] = (val >>> startShift) & 15;
        startShift -= 4;
      }
    }
  }

  private void unrolledUnPack8(long[] buffer, int offset, int len,
      InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 1);
  }

  private void unrolledUnPack16(long[] buffer, int offset, int len,
      InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 2);
  }

  private void unrolledUnPack24(long[] buffer, int offset, int len,
      InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 3);
  }

  private void unrolledUnPack32(long[] buffer, int offset, int len,
      InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 4);
  }

  private void unrolledUnPack40(long[] buffer, int offset, int len,
      InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 5);
  }

  private void unrolledUnPack48(long[] buffer, int offset, int len,
      InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 6);
  }

  private void unrolledUnPack56(long[] buffer, int offset, int len,
      InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 7);
  }

  private void unrolledUnPack64(long[] buffer, int offset, int len,
      InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 8);
  }

  private void unrolledUnPackBytes(long[] buffer, int offset, int len, InStream input, int numBytes)
      throws IOException {
    final int numHops = 8;
    final int remainder = len % numHops;
    final int endOffset = offset + len;
    final int endUnroll = endOffset - remainder;
    int i = offset;
    for (; i < endUnroll; i = i + numHops) {
      readLongBE(input, buffer, i, numHops, numBytes);
    }

    if (remainder > 0) {
      readRemainingLongs(buffer, i, input, remainder, numBytes);
    }
  }

  private void readRemainingLongs(long[] buffer, int offset, InStream input, int remainder,
      int numBytes) throws IOException {
    final int toRead = remainder * numBytes;
    // bulk read to buffer
    int bytesRead = input.read(readBuffer, 0, toRead);
    while (bytesRead != toRead) {
      bytesRead += input.read(readBuffer, bytesRead, toRead - bytesRead);
    }

    int idx = 0;
    switch (numBytes) {
      case 1:
        while (remainder > 0) {
          buffer[offset++] = readBuffer[idx] & 255;
          remainder--;
          idx++;
        }
        break;
      case 2:
        while (remainder > 0) {
          buffer[offset++] = readLongBE2(input, idx * 2);
          remainder--;
          idx++;
        }
        break;
      case 3:
        while (remainder > 0) {
          buffer[offset++] = readLongBE3(input, idx * 3);
          remainder--;
          idx++;
        }
        break;
      case 4:
        while (remainder > 0) {
          buffer[offset++] = readLongBE4(input, idx * 4);
          remainder--;
          idx++;
        }
        break;
      case 5:
        while (remainder > 0) {
          buffer[offset++] = readLongBE5(input, idx * 5);
          remainder--;
          idx++;
        }
        break;
      case 6:
        while (remainder > 0) {
          buffer[offset++] = readLongBE6(input, idx * 6);
          remainder--;
          idx++;
        }
        break;
      case 7:
        while (remainder > 0) {
          buffer[offset++] = readLongBE7(input, idx * 7);
          remainder--;
          idx++;
        }
        break;
      case 8:
        while (remainder > 0) {
          buffer[offset++] = readLongBE8(input, idx * 8);
          remainder--;
          idx++;
        }
        break;
      default:
        break;
    }
  }

  private void readLongBE(InStream in, long[] buffer, int start, int numHops, int numBytes)
      throws IOException {
    final int toRead = numHops * numBytes;
    // bulk read to buffer
    int bytesRead = in.read(readBuffer, 0, toRead);
    while (bytesRead != toRead) {
      bytesRead += in.read(readBuffer, bytesRead, toRead - bytesRead);
    }

    switch (numBytes) {
      case 1:
        buffer[start + 0] = readBuffer[0] & 255;
        buffer[start + 1] = readBuffer[1] & 255;
        buffer[start + 2] = readBuffer[2] & 255;
        buffer[start + 3] = readBuffer[3] & 255;
        buffer[start + 4] = readBuffer[4] & 255;
        buffer[start + 5] = readBuffer[5] & 255;
        buffer[start + 6] = readBuffer[6] & 255;
        buffer[start + 7] = readBuffer[7] & 255;
        break;
      case 2:
        buffer[start + 0] = readLongBE2(in, 0);
        buffer[start + 1] = readLongBE2(in, 2);
        buffer[start + 2] = readLongBE2(in, 4);
        buffer[start + 3] = readLongBE2(in, 6);
        buffer[start + 4] = readLongBE2(in, 8);
        buffer[start + 5] = readLongBE2(in, 10);
        buffer[start + 6] = readLongBE2(in, 12);
        buffer[start + 7] = readLongBE2(in, 14);
        break;
      case 3:
        buffer[start + 0] = readLongBE3(in, 0);
        buffer[start + 1] = readLongBE3(in, 3);
        buffer[start + 2] = readLongBE3(in, 6);
        buffer[start + 3] = readLongBE3(in, 9);
        buffer[start + 4] = readLongBE3(in, 12);
        buffer[start + 5] = readLongBE3(in, 15);
        buffer[start + 6] = readLongBE3(in, 18);
        buffer[start + 7] = readLongBE3(in, 21);
        break;
      case 4:
        buffer[start + 0] = readLongBE4(in, 0);
        buffer[start + 1] = readLongBE4(in, 4);
        buffer[start + 2] = readLongBE4(in, 8);
        buffer[start + 3] = readLongBE4(in, 12);
        buffer[start + 4] = readLongBE4(in, 16);
        buffer[start + 5] = readLongBE4(in, 20);
        buffer[start + 6] = readLongBE4(in, 24);
        buffer[start + 7] = readLongBE4(in, 28);
        break;
      case 5:
        buffer[start + 0] = readLongBE5(in, 0);
        buffer[start + 1] = readLongBE5(in, 5);
        buffer[start + 2] = readLongBE5(in, 10);
        buffer[start + 3] = readLongBE5(in, 15);
        buffer[start + 4] = readLongBE5(in, 20);
        buffer[start + 5] = readLongBE5(in, 25);
        buffer[start + 6] = readLongBE5(in, 30);
        buffer[start + 7] = readLongBE5(in, 35);
        break;
      case 6:
        buffer[start + 0] = readLongBE6(in, 0);
        buffer[start + 1] = readLongBE6(in, 6);
        buffer[start + 2] = readLongBE6(in, 12);
        buffer[start + 3] = readLongBE6(in, 18);
        buffer[start + 4] = readLongBE6(in, 24);
        buffer[start + 5] = readLongBE6(in, 30);
        buffer[start + 6] = readLongBE6(in, 36);
        buffer[start + 7] = readLongBE6(in, 42);
        break;
      case 7:
        buffer[start + 0] = readLongBE7(in, 0);
        buffer[start + 1] = readLongBE7(in, 7);
        buffer[start + 2] = readLongBE7(in, 14);
        buffer[start + 3] = readLongBE7(in, 21);
        buffer[start + 4] = readLongBE7(in, 28);
        buffer[start + 5] = readLongBE7(in, 35);
        buffer[start + 6] = readLongBE7(in, 42);
        buffer[start + 7] = readLongBE7(in, 49);
        break;
      case 8:
        buffer[start + 0] = readLongBE8(in, 0);
        buffer[start + 1] = readLongBE8(in, 8);
        buffer[start + 2] = readLongBE8(in, 16);
        buffer[start + 3] = readLongBE8(in, 24);
        buffer[start + 4] = readLongBE8(in, 32);
        buffer[start + 5] = readLongBE8(in, 40);
        buffer[start + 6] = readLongBE8(in, 48);
        buffer[start + 7] = readLongBE8(in, 56);
        break;
      default:
        break;
    }
  }

  private long readLongBE2(InStream in, int rbOffset) {
    return (((readBuffer[rbOffset] & 255) << 8)
        + ((readBuffer[rbOffset + 1] & 255) << 0));
  }

  private long readLongBE3(InStream in, int rbOffset) {
    return (((readBuffer[rbOffset] & 255) << 16)
        + ((readBuffer[rbOffset + 1] & 255) << 8)
        + ((readBuffer[rbOffset + 2] & 255) << 0));
  }

  private long readLongBE4(InStream in, int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 24)
        + ((readBuffer[rbOffset + 1] & 255) << 16)
        + ((readBuffer[rbOffset + 2] & 255) << 8)
        + ((readBuffer[rbOffset + 3] & 255) << 0));
  }

  private long readLongBE5(InStream in, int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 32)
        + ((long) (readBuffer[rbOffset + 1] & 255) << 24)
        + ((readBuffer[rbOffset + 2] & 255) << 16)
        + ((readBuffer[rbOffset + 3] & 255) << 8)
        + ((readBuffer[rbOffset + 4] & 255) << 0));
  }

  private long readLongBE6(InStream in, int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 40)
        + ((long) (readBuffer[rbOffset + 1] & 255) << 32)
        + ((long) (readBuffer[rbOffset + 2] & 255) << 24)
        + ((readBuffer[rbOffset + 3] & 255) << 16)
        + ((readBuffer[rbOffset + 4] & 255) << 8)
        + ((readBuffer[rbOffset + 5] & 255) << 0));
  }

  private long readLongBE7(InStream in, int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 48)
        + ((long) (readBuffer[rbOffset + 1] & 255) << 40)
        + ((long) (readBuffer[rbOffset + 2] & 255) << 32)
        + ((long) (readBuffer[rbOffset + 3] & 255) << 24)
        + ((readBuffer[rbOffset + 4] & 255) << 16)
        + ((readBuffer[rbOffset + 5] & 255) << 8)
        + ((readBuffer[rbOffset + 6] & 255) << 0));
  }

  private long readLongBE8(InStream in, int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 56)
        + ((long) (readBuffer[rbOffset + 1] & 255) << 48)
        + ((long) (readBuffer[rbOffset + 2] & 255) << 40)
        + ((long) (readBuffer[rbOffset + 3] & 255) << 32)
        + ((long) (readBuffer[rbOffset + 4] & 255) << 24)
        + ((readBuffer[rbOffset + 5] & 255) << 16)
        + ((readBuffer[rbOffset + 6] & 255) << 8)
        + ((readBuffer[rbOffset + 7] & 255) << 0));
  }

  // Do not want to use Guava LongMath.checkedSubtract() here as it will throw
  // ArithmeticException in case of overflow
  public boolean isSafeSubtract(long left, long right) {
    return (left ^ right) >= 0 || (left ^ (left - right)) >= 0;
  }

  /**
   * Convert a UTC time to a local timezone
   * @param local the local timezone
   * @param time the number of seconds since 1970
   * @return the converted timestamp
   */
  public static double convertFromUtc(TimeZone local, double time) {
    int offset = local.getOffset((long) (time*1000) - local.getRawOffset());
    return time - offset / 1000.0;
  }

  public static long convertFromUtc(TimeZone local, long time) {
    int offset = local.getOffset(time - local.getRawOffset());
    return time - offset;
  }

  public static long convertToUtc(TimeZone local, long time) {
    int offset = local.getOffset(time);
    return time + offset;
  }

  /**
   * Get the stream options with the compression tuned for the particular
   * kind of stream.
   * @param base the original options
   * @param strategy the compression strategy
   * @param kind the stream kind
   * @return the tuned options or the original if it is the same
   */
  public static StreamOptions getCustomizedCodec(StreamOptions base,
                                                 OrcFile.CompressionStrategy strategy,
                                                 OrcProto.Stream.Kind kind) {
    if (base.getCodec() != null) {
      CompressionCodec.Options options = base.getCodecOptions();
      switch (kind) {
        case BLOOM_FILTER:
        case DATA:
        case DICTIONARY_DATA:
        case BLOOM_FILTER_UTF8:
          options = options.copy().setData(CompressionCodec.DataKind.TEXT);
          if (strategy == OrcFile.CompressionStrategy.SPEED) {
            options.setSpeed(CompressionCodec.SpeedModifier.FAST);
          } else {
            options.setSpeed(CompressionCodec.SpeedModifier.DEFAULT);
          }
          break;
        case LENGTH:
        case DICTIONARY_COUNT:
        case PRESENT:
        case ROW_INDEX:
        case SECONDARY:
          options = options.copy()
                        .setSpeed(CompressionCodec.SpeedModifier.FASTEST)
                        .setData(CompressionCodec.DataKind.BINARY);
          break;
        default:
          LOG.info("Missing ORC compression modifiers for " + kind);
          break;
      }
      if (!base.getCodecOptions().equals(options)) {
        StreamOptions result = new StreamOptions(base)
                                   .withCodec(base.getCodec(), options);
        return result;
      }
    }
    return base;
  }

  /**
   * Find the relative offset when moving between timezones at a particular
   * point in time.
   *
   * This is a function of ORC v0 and v1 writing timestamps relative to the
   * local timezone. Therefore, when we read, we need to convert from the
   * writer's timezone to the reader's timezone.
   *
   * @param writer the timezone we are moving from
   * @param reader the timezone we are moving to
   * @param millis the point in time
   * @return the change in milliseconds
   */
  public static long convertBetweenTimezones(TimeZone writer, TimeZone reader,
                                             long millis) {
    final long writerOffset = writer.getOffset(millis);
    final long readerOffset = reader.getOffset(millis);
    long adjustedMillis = millis + writerOffset - readerOffset;
    // If the timezone adjustment moves the millis across a DST boundary, we
    // need to reevaluate the offsets.
    long adjustedReader = reader.getOffset(adjustedMillis);
    return writerOffset - adjustedReader;
  }

  /**
   * Convert a bytes vector element into a String.
   * @param vector the vector to use
   * @param elementNum the element number to stringify
   * @return a string or null if the value was null
   */
  public static String bytesVectorToString(BytesColumnVector vector,
                                           int elementNum) {
    if (vector.isRepeating) {
      elementNum = 0;
    }
    return vector.noNulls || !vector.isNull[elementNum] ?
               new String(vector.vector[elementNum], vector.start[elementNum],
                   vector.length[elementNum], StandardCharsets.UTF_8) : null;
  }

  /**
   * Parse a date from a string.
   * @param string the date to parse (YYYY-MM-DD)
   * @return the Date parsed, or null if there was a parse error.
   */
  public static Date parseDateFromString(String string) {
    try {
      Date value = Date.valueOf(string);
      return value;
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
