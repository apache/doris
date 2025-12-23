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

import java.io.IOException;
import java.util.function.Consumer;

/**
 * <p>A writer that performs light weight compression over sequence of integers.
 * </p>
 * <p>There are four types of lightweight integer compression</p>
 * <ul>
 * <li>SHORT_REPEAT</li>
 * <li>DIRECT</li>
 * <li>PATCHED_BASE</li>
 * <li>DELTA</li>
 * </ul>
 * <p>The description and format for these types are as below:
 * <b>SHORT_REPEAT:</b> Used for short repeated integer sequences.</p>
 * <ul>
 * <li>1 byte header
 * <ul>
 * <li>2 bits for encoding type</li>
 * <li>3 bits for bytes required for repeating value</li>
 * <li>3 bits for repeat count (MIN_REPEAT + run length)</li>
 * </ul>
 * </li>
 * <li>Blob - repeat value (fixed bytes)</li>
 * </ul>
 * <p>
 * <b>DIRECT:</b> Used for random integer sequences whose number of bit
 * requirement doesn't vary a lot.</p>
 * <ul>
 * <li>2 byte header (1st byte)
 * <ul>
 * <li>2 bits for encoding type</li>
 * <li>5 bits for fixed bit width of values in blob</li>
 * <li>1 bit for storing MSB of run length</li>
 * </ul></li>
 * <li>2nd byte
 * <ul>
 * <li>8 bits for lower run length bits</li>
 * </ul>
 * </li>
 * <li>Blob - stores the direct values using fixed bit width. The length of the
 * data blob is (fixed width * run length) bits long</li>
 * </ul>
 * <p>
 * <b>PATCHED_BASE:</b> Used for random integer sequences whose number of bit
 * requirement varies beyond a threshold.</p>
 * <ul>
 * <li>4 bytes header (1st byte)
 * <ul>
 * <li>2 bits for encoding type</li>
 * <li>5 bits for fixed bit width of values in blob</li>
 * <li>1 bit for storing MSB of run length</li>
 * </ul></li>
 * <li>2nd byte
 * <ul>
 * <li>8 bits for lower run length bits</li>
 * </ul></li>
 * <li>3rd byte
 * <ul>
 * <li>3 bits for bytes required to encode base value</li>
 * <li>5 bits for patch width</li>
 * </ul></li>
 * <li>4th byte
 * <ul>
 * <li>3 bits for patch gap width</li>
 * <li>5 bits for patch length</li>
 * </ul>
 * </li>
 * <li>Base value - Stored using fixed number of bytes. If MSB is set, base
 * value is negative else positive. Length of base value is (base width * 8)
 * bits.</li>
 * <li>Data blob - Base reduced values as stored using fixed bit width. Length
 * of data blob is (fixed width * run length) bits.</li>
 * <li>Patch blob - Patch blob is a list of gap and patch value. Each entry in
 * the patch list is (patch width + patch gap width) bits long. Gap between the
 * subsequent elements to be patched are stored in upper part of entry whereas
 * patch values are stored in lower part of entry. Length of patch blob is
 * ((patch width + patch gap width) * patch length) bits.</li>
 * </ul>
 * <p>
 * <b>DELTA</b> Used for monotonically increasing or decreasing sequences,
 * sequences with fixed delta values or long repeated sequences.
 * <ul>
 * <li>2 bytes header (1st byte)
 * <ul>
 * <li>2 bits for encoding type</li>
 * <li>5 bits for fixed bit width of values in blob</li>
 * <li>1 bit for storing MSB of run length</li>
 * </ul></li>
 * <li>2nd byte
 * <ul>
 * <li>8 bits for lower run length bits</li>
 * </ul></li>
 * <li>Base value - zigzag encoded value written as varint</li>
 * <li>Delta base - zigzag encoded value written as varint</li>
 * <li>Delta blob - only positive values. monotonicity and orderness are decided
 * based on the sign of the base value and delta base</li>
 * </ul>
 */
public class RunLengthIntegerWriterV2 implements IntegerWriter {

  public enum EncodingType {
    SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA
  }

  static final int MAX_SCOPE = 512;
  static final int MIN_REPEAT = 3;
  static final long BASE_VALUE_LIMIT = 1L << 56;
  private static final int MAX_SHORT_REPEAT_LENGTH = 10;
  private long prevDelta = 0;
  private int fixedRunLength = 0;
  private int variableRunLength = 0;
  private final long[] literals = new long[MAX_SCOPE];
  private final PositionedOutputStream output;
  private final boolean signed;
  private EncodingType encoding;
  private int numLiterals;
  private final long[] zigzagLiterals;
  private final long[] baseRedLiterals = new long[MAX_SCOPE];
  private final long[] adjDeltas = new long[MAX_SCOPE];
  private long fixedDelta;
  private int zzBits90p;
  private int zzBits100p;
  private int brBits95p;
  private int brBits100p;
  private int bitsDeltaMax;
  private int patchWidth;
  private int patchGapWidth;
  private int patchLength;
  private long[] gapVsPatchList;
  private long min;
  private boolean isFixedDelta;
  private SerializationUtils utils;
  private boolean alignedBitpacking;

  RunLengthIntegerWriterV2(PositionedOutputStream output, boolean signed) {
    this(output, signed, true);
  }

  public RunLengthIntegerWriterV2(PositionedOutputStream output, boolean signed,
      boolean alignedBitpacking) {
    this.output = output;
    this.signed = signed;
    this.zigzagLiterals = signed ? new long[MAX_SCOPE] : null;
    this.alignedBitpacking = alignedBitpacking;
    this.utils = new SerializationUtils();
    clear();
  }

  private void writeValues() throws IOException {
    if (numLiterals != 0) {

      if (encoding.equals(EncodingType.SHORT_REPEAT)) {
        writeShortRepeatValues();
      } else if (encoding.equals(EncodingType.DIRECT)) {
        writeDirectValues();
      } else if (encoding.equals(EncodingType.PATCHED_BASE)) {
        writePatchedBaseValues();
      } else {
        writeDeltaValues();
      }

      // clear all the variables
      clear();
    }
  }

  private void writeDeltaValues() throws IOException {
    int len = 0;
    int fb = bitsDeltaMax;
    int efb = 0;

    if (alignedBitpacking) {
      fb = utils.getClosestAlignedFixedBits(fb);
    }

    if (isFixedDelta) {
      // if fixed run length is greater than threshold then it will be fixed
      // delta sequence with delta value 0 else fixed delta sequence with
      // non-zero delta value
      if (fixedRunLength > MIN_REPEAT) {
        // ex. sequence: 2 2 2 2 2 2 2 2
        len = fixedRunLength - 1;
        fixedRunLength = 0;
      } else {
        // ex. sequence: 4 6 8 10 12 14 16
        len = variableRunLength - 1;
        variableRunLength = 0;
      }
    } else {
      // fixed width 0 is used for long repeating values.
      // sequences that require only 1 bit to encode will have an additional bit
      if (fb == 1) {
        fb = 2;
      }
      efb = utils.encodeBitWidth(fb);
      efb = efb << 1;
      len = variableRunLength - 1;
      variableRunLength = 0;
    }

    // extract the 9th bit of run length
    final int tailBits = (len & 0x100) >>> 8;

    // create first byte of the header
    final int headerFirstByte = getOpcode() | efb | tailBits;

    // second byte of the header stores the remaining 8 bits of runlength
    final int headerSecondByte = len & 0xff;

    // write header
    output.write(headerFirstByte);
    output.write(headerSecondByte);

    // store the first value from zigzag literal array
    if (signed) {
      utils.writeVslong(output, literals[0]);
    } else {
      utils.writeVulong(output, literals[0]);
    }

    if (isFixedDelta) {
      // if delta is fixed then we don't need to store delta blob
      utils.writeVslong(output, fixedDelta);
    } else {
      // store the first value as delta value using zigzag encoding
      utils.writeVslong(output, adjDeltas[0]);

      // adjacent delta values are bit packed. The length of adjDeltas array is
      // always one less than the number of literals (delta difference for n
      // elements is n-1). We have already written one element, write the
      // remaining numLiterals - 2 elements here
      utils.writeInts(adjDeltas, 1, numLiterals - 2, fb, output);
    }
  }

  private void writePatchedBaseValues() throws IOException {

    // NOTE: Aligned bit packing cannot be applied for PATCHED_BASE encoding
    // because patch is applied to MSB bits. For example: If fixed bit width of
    // base value is 7 bits and if patch is 3 bits, the actual value is
    // constructed by shifting the patch to left by 7 positions.
    // actual_value = patch << 7 | base_value
    // So, if we align base_value then actual_value can not be reconstructed.

    // write the number of fixed bits required in next 5 bits
    final int fb = brBits95p;
    final int efb = utils.encodeBitWidth(fb) << 1;

    // adjust variable run length, they are one off
    variableRunLength -= 1;

    // extract the 9th bit of run length
    final int tailBits = (variableRunLength & 0x100) >>> 8;

    // create first byte of the header
    final int headerFirstByte = getOpcode() | efb | tailBits;

    // second byte of the header stores the remaining 8 bits of runlength
    final int headerSecondByte = variableRunLength & 0xff;

    // if the min value is negative toggle the sign
    final boolean isNegative = min < 0;
    if (isNegative) {
      min = -min;
    }

    // find the number of bytes required for base and shift it by 5 bits
    // to accommodate patch width. The additional bit is used to store the sign
    // of the base value.
    final int baseWidth = utils.findClosestNumBits(min) + 1;
    final int baseBytes = baseWidth % 8 == 0 ? baseWidth / 8 : (baseWidth / 8) + 1;
    final int bb = (baseBytes - 1) << 5;

    // if the base value is negative then set MSB to 1
    if (isNegative) {
      min |= (1L << ((baseBytes * 8) - 1));
    }

    // third byte contains 3 bits for number of bytes occupied by base
    // and 5 bits for patchWidth
    final int headerThirdByte = bb | utils.encodeBitWidth(patchWidth);

    // fourth byte contains 3 bits for page gap width and 5 bits for
    // patch length
    final int headerFourthByte = (patchGapWidth - 1) << 5 | patchLength;

    // write header
    output.write(headerFirstByte);
    output.write(headerSecondByte);
    output.write(headerThirdByte);
    output.write(headerFourthByte);

    // write the base value using fixed bytes in big endian order
    for(int i = baseBytes - 1; i >= 0; i--) {
      byte b = (byte) ((min >>> (i * 8)) & 0xff);
      output.write(b);
    }

    // base reduced literals are bit packed
    int closestFixedBits = utils.getClosestFixedBits(fb);

    utils.writeInts(baseRedLiterals, 0, numLiterals, closestFixedBits,
        output);

    // write patch list
    closestFixedBits = utils.getClosestFixedBits(patchGapWidth + patchWidth);

    utils.writeInts(gapVsPatchList, 0, gapVsPatchList.length, closestFixedBits,
        output);

    // reset run length
    variableRunLength = 0;
  }

  /**
   * Store the opcode in 2 MSB bits
   * @return opcode
   */
  private int getOpcode() {
    return encoding.ordinal() << 6;
  }

  private void writeDirectValues() throws IOException {

    // write the number of fixed bits required in next 5 bits
    int fb = zzBits100p;

    if (alignedBitpacking) {
      fb = utils.getClosestAlignedFixedBits(fb);
    }

    final int efb = utils.encodeBitWidth(fb) << 1;

    // adjust variable run length
    variableRunLength -= 1;

    // extract the 9th bit of run length
    final int tailBits = (variableRunLength & 0x100) >>> 8;

    // create first byte of the header
    final int headerFirstByte = getOpcode() | efb | tailBits;

    // second byte of the header stores the remaining 8 bits of runlength
    final int headerSecondByte = variableRunLength & 0xff;

    // write header
    output.write(headerFirstByte);
    output.write(headerSecondByte);

    // bit packing the zigzag encoded literals
    long[] currentZigzagLiterals = signed ? zigzagLiterals : literals;
    utils.writeInts(currentZigzagLiterals, 0, numLiterals, fb, output);

    // reset run length
    variableRunLength = 0;
  }

  private void writeShortRepeatValues() throws IOException {
    // get the value that is repeating, compute the bits and bytes required
    long repeatVal = 0;
    if (signed) {
      repeatVal = utils.zigzagEncode(literals[0]);
    } else {
      repeatVal = literals[0];
    }

    final int numBitsRepeatVal = utils.findClosestNumBits(repeatVal);
    final int numBytesRepeatVal = numBitsRepeatVal % 8 == 0 ? numBitsRepeatVal >>> 3
        : (numBitsRepeatVal >>> 3) + 1;

    // write encoding type in top 2 bits
    int header = getOpcode();

    // write the number of bytes required for the value
    header |= ((numBytesRepeatVal - 1) << 3);

    // write the run length
    fixedRunLength -= MIN_REPEAT;
    header |= fixedRunLength;

    // write the header
    output.write(header);

    // write the repeating value in big endian byte order
    for(int i = numBytesRepeatVal - 1; i >= 0; i--) {
      int b = (int) ((repeatVal >>> (i * 8)) & 0xff);
      output.write(b);
    }

    fixedRunLength = 0;
  }

  /**
   * Prepare for Direct or PatchedBase encoding
   * compute zigZagLiterals and zzBits100p (Max number of encoding bits required)
   * @return zigzagLiterals
   */
  private long[] prepareForDirectOrPatchedBase() {
    // only signed numbers need to compute zigzag values
    if (signed) {
      computeZigZagLiterals();
    }
    long[] currentZigzagLiterals = signed ? zigzagLiterals : literals;
    zzBits100p = utils.percentileBits(currentZigzagLiterals, 0, numLiterals, 1.0);
    return currentZigzagLiterals;
  }

  private void determineEncoding() {
    // we need to compute zigzag values for DIRECT encoding if we decide to
    // break early for delta overflows or for shorter runs

    // not a big win for shorter runs to determine encoding
    if (numLiterals <= MIN_REPEAT) {
      prepareForDirectOrPatchedBase();
      encoding = EncodingType.DIRECT;
      return;
    }

    // DELTA encoding check

    // for identifying monotonic sequences
    boolean isIncreasing = true;
    boolean isDecreasing = true;
    this.isFixedDelta = true;

    this.min = literals[0];
    long max = literals[0];
    final long initialDelta = literals[1] - literals[0];
    long currDelta = 0;
    long deltaMax = 0;
    this.adjDeltas[0] = initialDelta;

    for (int i = 1; i < numLiterals; i++) {
      final long l1 = literals[i];
      final long l0 = literals[i - 1];
      currDelta = l1 - l0;
      min = Math.min(min, l1);
      max = Math.max(max, l1);

      isIncreasing &= (l0 <= l1);
      isDecreasing &= (l0 >= l1);

      isFixedDelta &= (currDelta == initialDelta);
      if (i > 1) {
        adjDeltas[i - 1] = Math.abs(currDelta);
        deltaMax = Math.max(deltaMax, adjDeltas[i - 1]);
      }
    }

    // its faster to exit under delta overflow condition without checking for
    // PATCHED_BASE condition as encoding using DIRECT is faster and has less
    // overhead than PATCHED_BASE
    if (!utils.isSafeSubtract(max, min)) {
      prepareForDirectOrPatchedBase();
      encoding = EncodingType.DIRECT;
      return;
    }

    // invariant - subtracting any number from any other in the literals after
    // this point won't overflow

    // if min is equal to max then the delta is 0, this condition happens for
    // fixed values run >10 which cannot be encoded with SHORT_REPEAT
    if (min == max) {
      assert isFixedDelta : min + "==" + max +
          ", isFixedDelta cannot be false";
      assert currDelta == 0 : min + "==" + max + ", currDelta should be zero";
      fixedDelta = 0;
      encoding = EncodingType.DELTA;
      return;
    }

    if (isFixedDelta) {
      assert currDelta == initialDelta
          : "currDelta should be equal to initialDelta for fixed delta encoding";
      encoding = EncodingType.DELTA;
      fixedDelta = currDelta;
      return;
    }

    // if initialDelta is 0 then we cannot delta encode as we cannot identify
    // the sign of deltas (increasing or decreasing)
    if (initialDelta != 0) {
      // stores the number of bits required for packing delta blob in
      // delta encoding
      bitsDeltaMax = utils.findClosestNumBits(deltaMax);

      // monotonic condition
      if (isIncreasing || isDecreasing) {
        encoding = EncodingType.DELTA;
        return;
      }
    }

    // PATCHED_BASE encoding check

    // percentile values are computed for the zigzag encoded values. if the
    // number of bit requirement between 90th and 100th percentile varies
    // beyond a threshold then we need to patch the values. if the variation
    // is not significant then we can use direct encoding
    long[] currentZigzagLiterals = prepareForDirectOrPatchedBase();
    zzBits90p = utils.percentileBits(currentZigzagLiterals, 0, numLiterals, 0.9);
    int diffBitsLH = zzBits100p - zzBits90p;

    // if the difference between 90th percentile and 100th percentile fixed
    // bits is > 1 then we need patch the values
    if (diffBitsLH > 1) {

      // patching is done only on base reduced values.
      // remove base from literals
      for (int i = 0; i < numLiterals; i++) {
        baseRedLiterals[i] = literals[i] - min;
      }

      // 95th percentile width is used to determine max allowed value
      // after which patching will be done
      brBits95p = utils.percentileBits(baseRedLiterals, 0, numLiterals, 0.95);

      // 100th percentile is used to compute the max patch width
      brBits100p = utils.percentileBits(baseRedLiterals, 0, numLiterals, 1.0);

      // after base reducing the values, if the difference in bits between
      // 95th percentile and 100th percentile value is zero then there
      // is no point in patching the values, in which case we will
      // fallback to DIRECT encoding.
      // The decision to use patched base was based on zigzag values, but the
      // actual patching is done on base reduced literals.
      if ((brBits100p - brBits95p) != 0 && Math.abs(min) < BASE_VALUE_LIMIT) {
        encoding = EncodingType.PATCHED_BASE;
        preparePatchedBlob();
      } else {
        encoding = EncodingType.DIRECT;
      }
    } else {
      // if difference in bits between 95th percentile and 100th percentile is
      // 0, then patch length will become 0. Hence we will fallback to direct
      encoding = EncodingType.DIRECT;
    }
  }

  private void computeZigZagLiterals() {
    // populate zigzag encoded literals
    assert signed : "only signed numbers need to compute zigzag values";
    for (int i = 0; i < numLiterals; i++) {
      zigzagLiterals[i] = utils.zigzagEncode(literals[i]);
    }
  }

  private void preparePatchedBlob() {
    // mask will be max value beyond which patch will be generated
    long mask = (1L << brBits95p) - 1;

    // since we are considering only 95 percentile, the size of gap and
    // patch array can contain only be 5% values
    patchLength = (int) Math.ceil((numLiterals * 0.05));

    int[] gapList = new int[patchLength];
    long[] patchList = new long[patchLength];

    // #bit for patch
    patchWidth = brBits100p - brBits95p;
    patchWidth = utils.getClosestFixedBits(patchWidth);

    // if patch bit requirement is 64 then it will not possible to pack
    // gap and patch together in a long. To make sure gap and patch can be
    // packed together adjust the patch width
    if (patchWidth == 64) {
      patchWidth = 56;
      brBits95p = 8;
      mask = (1L << brBits95p) - 1;
    }

    int gapIdx = 0;
    int patchIdx = 0;
    int prev = 0;
    int gap = 0;
    int maxGap = 0;

    for(int i = 0; i < numLiterals; i++) {
      // if value is above mask then create the patch and record the gap
      if (baseRedLiterals[i] > mask) {
        gap = i - prev;
        if (gap > maxGap) {
          maxGap = gap;
        }

        // gaps are relative, so store the previous patched value index
        prev = i;
        gapList[gapIdx++] = gap;

        // extract the most significant bits that are over mask bits
        long patch = baseRedLiterals[i] >>> brBits95p;
        patchList[patchIdx++] = patch;

        // strip off the MSB to enable safe bit packing
        baseRedLiterals[i] &= mask;
      }
    }

    // adjust the patch length to number of entries in gap list
    patchLength = gapIdx;

    // if the element to be patched is the first and only element then
    // max gap will be 0, but to store the gap as 0 we need atleast 1 bit
    if (maxGap == 0 && patchLength != 0) {
      patchGapWidth = 1;
    } else {
      patchGapWidth = utils.findClosestNumBits(maxGap);
    }

    // special case: if the patch gap width is greater than 256, then
    // we need 9 bits to encode the gap width. But we only have 3 bits in
    // header to record the gap width. To deal with this case, we will save
    // two entries in patch list in the following way
    // 256 gap width => 0 for patch value
    // actual gap - 256 => actual patch value
    // We will do the same for gap width = 511. If the element to be patched is
    // the last element in the scope then gap width will be 511. In this case we
    // will have 3 entries in the patch list in the following way
    // 255 gap width => 0 for patch value
    // 255 gap width => 0 for patch value
    // 1 gap width => actual patch value
    if (patchGapWidth > 8) {
      patchGapWidth = 8;
      // for gap = 511, we need two additional entries in patch list
      if (maxGap == 511) {
        patchLength += 2;
      } else {
        patchLength += 1;
      }
    }

    // create gap vs patch list
    gapIdx = 0;
    patchIdx = 0;
    gapVsPatchList = new long[patchLength];
    for(int i = 0; i < patchLength; i++) {
      long g = gapList[gapIdx++];
      long p = patchList[patchIdx++];
      while (g > 255) {
        gapVsPatchList[i++] = (255L << patchWidth);
        g -= 255;
      }

      // store patch value in LSBs and gap in MSBs
      gapVsPatchList[i] = (g << patchWidth) | p;
    }
  }

  /**
   * clears all the variables
   */
  private void clear() {
    numLiterals = 0;
    encoding = null;
    prevDelta = 0;
    fixedDelta = 0;
    zzBits90p = 0;
    zzBits100p = 0;
    brBits95p = 0;
    brBits100p = 0;
    bitsDeltaMax = 0;
    patchGapWidth = 0;
    patchLength = 0;
    patchWidth = 0;
    gapVsPatchList = null;
    min = 0;
    isFixedDelta = true;
  }

  @Override
  public void flush() throws IOException {
    if (numLiterals != 0) {
      if (variableRunLength != 0) {
        determineEncoding();
        writeValues();
      } else if (fixedRunLength != 0) {
        if (fixedRunLength < MIN_REPEAT) {
          variableRunLength = fixedRunLength;
          fixedRunLength = 0;
          determineEncoding();
        } else if (fixedRunLength <= MAX_SHORT_REPEAT_LENGTH) {
          encoding = EncodingType.SHORT_REPEAT;
        } else {
          encoding = EncodingType.DELTA;
          isFixedDelta = true;
        }
        writeValues();
      }
    }
    output.flush();
  }

  @Override
  public void write(long val) throws IOException {
    if (numLiterals == 0) {
      initializeLiterals(val);
    } else {
      if (numLiterals == 1) {
        prevDelta = val - literals[0];
        literals[numLiterals++] = val;
        // if both values are same count as fixed run else variable run
        if (val == literals[0]) {
          fixedRunLength = 2;
          variableRunLength = 0;
        } else {
          fixedRunLength = 0;
          variableRunLength = 2;
        }
      } else {
        long currentDelta = val - literals[numLiterals - 1];
        if (prevDelta == 0 && currentDelta == 0) {
          // fixed delta run

          literals[numLiterals++] = val;

          // if variable run is non-zero then we are seeing repeating
          // values at the end of variable run in which case keep
          // updating variable and fixed runs
          if (variableRunLength > 0) {
            fixedRunLength = 2;
          }
          fixedRunLength += 1;

          // if fixed run met the minimum condition and if variable
          // run is non-zero then flush the variable run and shift the
          // tail fixed runs to start of the buffer
          if (fixedRunLength >= MIN_REPEAT && variableRunLength > 0) {
            numLiterals -= MIN_REPEAT;
            variableRunLength -= MIN_REPEAT - 1;
            // copy the tail fixed runs
            long[] tailVals = new long[MIN_REPEAT];
            System.arraycopy(literals, numLiterals, tailVals, 0, MIN_REPEAT);

            // determine variable encoding and flush values
            determineEncoding();
            writeValues();

            // shift tail fixed runs to beginning of the buffer
            for(long l : tailVals) {
              literals[numLiterals++] = l;
            }
          }

          // if fixed runs reached max repeat length then write values
          if (fixedRunLength == MAX_SCOPE) {
            encoding = EncodingType.DELTA;
            isFixedDelta = true;
            writeValues();
          }
        } else {
          // variable delta run

          // if fixed run length is non-zero and if it satisfies the
          // short repeat conditions then write the values as short repeats
          // else use delta encoding
          if (fixedRunLength >= MIN_REPEAT) {
            if (fixedRunLength <= MAX_SHORT_REPEAT_LENGTH) {
              encoding = EncodingType.SHORT_REPEAT;
            } else {
              encoding = EncodingType.DELTA;
              isFixedDelta = true;
            }
            writeValues();
          }

          // if fixed run length is <MIN_REPEAT and current value is
          // different from previous then treat it as variable run
          if (fixedRunLength > 0 && fixedRunLength < MIN_REPEAT) {
            if (val != literals[numLiterals - 1]) {
              variableRunLength = fixedRunLength;
              fixedRunLength = 0;
            }
          }

          // after writing values re-initialize the variables
          if (numLiterals == 0) {
            initializeLiterals(val);
          } else {
            // keep updating variable run lengths
            prevDelta = val - literals[numLiterals - 1];
            literals[numLiterals++] = val;
            variableRunLength += 1;

            // if variable run length reach the max scope, write it
            if (variableRunLength == MAX_SCOPE) {
              determineEncoding();
              writeValues();
            }
          }
        }
      }
    }
  }

  private void initializeLiterals(long val) {
    literals[numLiterals++] = val;
    fixedRunLength = 1;
    variableRunLength = 1;
  }

  @Override
  public void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(numLiterals);
  }

  @Override
  public long estimateMemory() {
    return output.getBufferSize();
  }

  @Override
  public void changeIv(Consumer<byte[]> modifier) {
    output.changeIv(modifier);
  }
}
