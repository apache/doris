/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "Adaptor.hh"
#include "Compression.hh"
#include "RLEV2Util.hh"
#include "RLEv2.hh"

#define MAX_SHORT_REPEAT_LENGTH 10

namespace orc {

  /**
   * Compute the bits required to represent pth percentile value
   * @param data - array
   * @param p - percentile value (>=0.0 to <=1.0)
   * @return pth percentile bits
   */
  uint32_t RleEncoderV2::percentileBits(int64_t* data, size_t offset, size_t length, double p,
                                        bool reuseHist) {
    if ((p > 1.0) || (p <= 0.0)) {
      throw InvalidArgument("Invalid p value: " + to_string(p));
    }

    if (!reuseHist) {
      // histogram that store the encoded bit requirement for each values.
      // maximum number of bits that can encoded is 32 (refer FixedBitSizes)
      memset(histgram, 0, FixedBitSizes::SIZE * sizeof(int32_t));
      // compute the histogram
      for (size_t i = offset; i < (offset + length); i++) {
        uint32_t idx = encodeBitWidth(findClosestNumBits(data[i]));
        histgram[idx] += 1;
      }
    }

    int32_t perLen = static_cast<int32_t>(static_cast<double>(length) * (1.0 - p));

    // return the bits required by pth percentile length
    for (int32_t i = HIST_LEN - 1; i >= 0; i--) {
      perLen -= histgram[i];
      if (perLen < 0) {
        return decodeBitWidth(static_cast<uint32_t>(i));
      }
    }
    return 0;
  }

  RleEncoderV2::RleEncoderV2(std::unique_ptr<BufferedOutputStream> outStream, bool hasSigned,
                             bool alignBitPacking)
      : RleEncoder(std::move(outStream), hasSigned),
        alignedBitPacking(alignBitPacking),
        prevDelta(0) {
    literals = new int64_t[MAX_LITERAL_SIZE];
    gapVsPatchList = new int64_t[MAX_LITERAL_SIZE];
    zigzagLiterals = hasSigned ? new int64_t[MAX_LITERAL_SIZE] : nullptr;
    baseRedLiterals = new int64_t[MAX_LITERAL_SIZE];
    adjDeltas = new int64_t[MAX_LITERAL_SIZE];
  }

  void RleEncoderV2::write(int64_t val) {
    if (numLiterals == 0) {
      initializeLiterals(val);
      return;
    }

    if (numLiterals == 1) {
      prevDelta = val - literals[0];
      literals[numLiterals++] = val;

      if (val == literals[0]) {
        fixedRunLength = 2;
        variableRunLength = 0;
      } else {
        fixedRunLength = 0;
        variableRunLength = 2;
      }
      return;
    }

    int64_t currentDelta = val - literals[numLiterals - 1];
    EncodingOption option = {};
    if (prevDelta == 0 && currentDelta == 0) {
      // case 1: fixed delta run
      literals[numLiterals++] = val;

      if (variableRunLength > 0) {
        // if variable run is non-zero then we are seeing repeating
        // values at the end of variable run in which case fixed Run
        // length is 2
        fixedRunLength = 2;
      }
      fixedRunLength++;

      // if fixed run met the minimum condition and if variable
      // run is non-zero then flush the variable run and shift the
      // tail fixed runs to start of the buffer
      if (fixedRunLength >= MIN_REPEAT && variableRunLength > 0) {
        numLiterals -= MIN_REPEAT;
        variableRunLength -= (MIN_REPEAT - 1);

        determineEncoding(option);
        writeValues(option);

        // shift tail fixed runs to beginning of the buffer
        for (size_t i = 0; i < MIN_REPEAT; ++i) {
          literals[i] = val;
        }
        numLiterals = MIN_REPEAT;
      }

      if (fixedRunLength == MAX_LITERAL_SIZE) {
        option.encoding = DELTA;
        option.isFixedDelta = true;
        writeValues(option);
      }
      return;
    }

    // case 2: variable delta run

    // if fixed run length is non-zero and if it satisfies the
    // short repeat conditions then write the values as short repeats
    // else use delta encoding
    if (fixedRunLength >= MIN_REPEAT) {
      if (fixedRunLength <= MAX_SHORT_REPEAT_LENGTH) {
        option.encoding = SHORT_REPEAT;
      } else {
        option.encoding = DELTA;
        option.isFixedDelta = true;
      }
      writeValues(option);
    }

    // if fixed run length is <MIN_REPEAT and current value is
    // different from previous then treat it as variable run
    if (fixedRunLength > 0 && fixedRunLength < MIN_REPEAT && val != literals[numLiterals - 1]) {
      variableRunLength = fixedRunLength;
      fixedRunLength = 0;
    }

    // after writing values re-initialize the variables
    if (numLiterals == 0) {
      initializeLiterals(val);
    } else {
      prevDelta = val - literals[numLiterals - 1];
      literals[numLiterals++] = val;
      variableRunLength++;

      if (variableRunLength == MAX_LITERAL_SIZE) {
        determineEncoding(option);
        writeValues(option);
      }
    }
  }

  void RleEncoderV2::computeZigZagLiterals(EncodingOption& option) {
    assert(isSigned);
    for (size_t i = 0; i < numLiterals; i++) {
      zigzagLiterals[option.zigzagLiteralsCount++] = zigZag(literals[i]);
    }
  }

  void RleEncoderV2::preparePatchedBlob(EncodingOption& option) {
    // mask will be max value beyond which patch will be generated
    int64_t mask = static_cast<int64_t>(static_cast<uint64_t>(1) << option.brBits95p) - 1;

    // since we are considering only 95 percentile, the size of gap and
    // patch array can contain only be 5% values
    option.patchLength = static_cast<uint32_t>(std::ceil((numLiterals / 20)));

    // #bit for patch
    option.patchWidth = option.brBits100p - option.brBits95p;
    option.patchWidth = getClosestFixedBits(option.patchWidth);

    // if patch bit requirement is 64 then it will not possible to pack
    // gap and patch together in a long. To make sure gap and patch can be
    // packed together adjust the patch width
    if (option.patchWidth == 64) {
      option.patchWidth = 56;
      option.brBits95p = 8;
      mask = static_cast<int64_t>(static_cast<uint64_t>(1) << option.brBits95p) - 1;
    }

    uint32_t gapIdx = 0;
    uint32_t patchIdx = 0;
    size_t prev = 0;
    size_t maxGap = 0;

    std::vector<int64_t> gapList;
    std::vector<int64_t> patchList;

    for (size_t i = 0; i < numLiterals; i++) {
      // if value is above mask then create the patch and record the gap
      if (baseRedLiterals[i] > mask) {
        size_t gap = i - prev;
        if (gap > maxGap) {
          maxGap = gap;
        }

        // gaps are relative, so store the previous patched value index
        prev = i;
        gapList.push_back(static_cast<int64_t>(gap));
        gapIdx++;

        // extract the most significant bits that are over mask bits
        int64_t patch = baseRedLiterals[i] >> option.brBits95p;
        patchList.push_back(patch);
        patchIdx++;

        // strip off the MSB to enable safe bit packing
        baseRedLiterals[i] &= mask;
      }
    }

    // adjust the patch length to number of entries in gap list
    option.patchLength = gapIdx;

    // if the element to be patched is the first and only element then
    // max gap will be 0, but to store the gap as 0 we need atleast 1 bit
    if (maxGap == 0 && option.patchLength != 0) {
      option.patchGapWidth = 1;
    } else {
      option.patchGapWidth = findClosestNumBits(static_cast<int64_t>(maxGap));
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
    if (option.patchGapWidth > 8) {
      option.patchGapWidth = 8;
      // for gap = 511, we need two additional entries in patch list
      if (maxGap == 511) {
        option.patchLength += 2;
      } else {
        option.patchLength += 1;
      }
    }

    // create gap vs patch list
    gapIdx = 0;
    patchIdx = 0;
    for (size_t i = 0; i < option.patchLength; i++) {
      int64_t g = gapList[gapIdx++];
      int64_t p = patchList[patchIdx++];
      while (g > 255) {
        gapVsPatchList[option.gapVsPatchListCount++] = (255L << option.patchWidth);
        i++;
        g -= 255;
      }

      // store patch value in LSBs and gap in MSBs
      gapVsPatchList[option.gapVsPatchListCount++] = ((g << option.patchWidth) | p);
    }
  }

  /**
   * Prepare for Direct or PatchedBase encoding
   * compute zigZagLiterals and zzBits100p (Max number of encoding bits required)
   * @return zigzagLiterals
   */
  int64_t* RleEncoderV2::prepareForDirectOrPatchedBase(EncodingOption& option) {
    if (isSigned) {
      computeZigZagLiterals(option);
    }
    int64_t* currentZigzagLiterals = isSigned ? zigzagLiterals : literals;
    option.zzBits100p = percentileBits(currentZigzagLiterals, 0, numLiterals, 1.0);
    return currentZigzagLiterals;
  }

  void RleEncoderV2::determineEncoding(EncodingOption& option) {
    // We need to compute zigzag values for DIRECT and PATCHED_BASE encodings,
    // but not for SHORT_REPEAT or DELTA. So we only perform the zigzag
    // computation when it's determined to be necessary.

    // not a big win for shorter runs to determine encoding
    if (numLiterals <= MIN_REPEAT) {
      // we need to compute zigzag values for DIRECT encoding if we decide to
      // break early for delta overflows or for shorter runs
      prepareForDirectOrPatchedBase(option);
      option.encoding = DIRECT;
      return;
    }

    // DELTA encoding check

    // for identifying monotonic sequences
    bool isIncreasing = true;
    bool isDecreasing = true;
    option.isFixedDelta = true;

    option.min = literals[0];
    int64_t max = literals[0];
    int64_t initialDelta = literals[1] - literals[0];
    int64_t currDelta = 0;
    int64_t deltaMax = 0;
    adjDeltas[option.adjDeltasCount++] = initialDelta;

    for (size_t i = 1; i < numLiterals; i++) {
      const int64_t l1 = literals[i];
      const int64_t l0 = literals[i - 1];
      currDelta = l1 - l0;
      option.min = std::min(option.min, l1);
      max = std::max(max, l1);

      isIncreasing &= (l0 <= l1);
      isDecreasing &= (l0 >= l1);

      option.isFixedDelta &= (currDelta == initialDelta);
      if (i > 1) {
        adjDeltas[option.adjDeltasCount++] = std::abs(currDelta);
        deltaMax = std::max(deltaMax, adjDeltas[i - 1]);
      }
    }

    // it's faster to exit under delta overflow condition without checking for
    // PATCHED_BASE condition as encoding using DIRECT is faster and has less
    // overhead than PATCHED_BASE
    if (!isSafeSubtract(max, option.min)) {
      prepareForDirectOrPatchedBase(option);
      option.encoding = DIRECT;
      return;
    }

    // invariant - subtracting any number from any other in the literals after
    // option point won't overflow

    // if min is equal to max then the delta is 0, option condition happens for
    // fixed values run >10 which cannot be encoded with SHORT_REPEAT
    if (option.min == max) {
      if (!option.isFixedDelta) {
        throw InvalidArgument(to_string(option.min) + "==" + to_string(max) +
                              ", isFixedDelta cannot be false");
      }

      if (currDelta != 0) {
        throw InvalidArgument(to_string(option.min) + "==" + to_string(max) +
                              ", currDelta should be zero");
      }
      option.fixedDelta = 0;
      option.encoding = DELTA;
      return;
    }

    if (option.isFixedDelta) {
      if (currDelta != initialDelta) {
        throw InvalidArgument("currDelta should be equal to initialDelta for fixed delta encoding");
      }

      option.encoding = DELTA;
      option.fixedDelta = currDelta;
      return;
    }

    // if initialDelta is 0 then we cannot delta encode as we cannot identify
    // the sign of deltas (increasing or decreasing)
    if (initialDelta != 0) {
      // stores the number of bits required for packing delta blob in
      // delta encoding
      option.bitsDeltaMax = findClosestNumBits(deltaMax);

      // monotonic condition
      if (isIncreasing || isDecreasing) {
        option.encoding = DELTA;
        return;
      }
    }

    // PATCHED_BASE encoding check

    // percentile values are computed for the zigzag encoded values. if the
    // number of bit requirement between 90th and 100th percentile varies
    // beyond a threshold then we need to patch the values. if the variation
    // is not significant then we can use direct encoding

    int64_t* currentZigzagLiterals = prepareForDirectOrPatchedBase(option);
    option.zzBits90p = percentileBits(currentZigzagLiterals, 0, numLiterals, 0.9, true);
    uint32_t diffBitsLH = option.zzBits100p - option.zzBits90p;

    // if the difference between 90th percentile and 100th percentile fixed
    // bits is > 1 then we need patch the values
    if (diffBitsLH > 1) {
      // patching is done only on base reduced values.
      // remove base from literals
      for (size_t i = 0; i < numLiterals; i++) {
        baseRedLiterals[option.baseRedLiteralsCount++] = (literals[i] - option.min);
      }

      // 95th percentile width is used to determine max allowed value
      // after which patching will be done
      option.brBits95p = percentileBits(baseRedLiterals, 0, numLiterals, 0.95);

      // 100th percentile is used to compute the max patch width
      option.brBits100p = percentileBits(baseRedLiterals, 0, numLiterals, 1.0, true);

      // after base reducing the values, if the difference in bits between
      // 95th percentile and 100th percentile value is zero then there
      // is no point in patching the values, in which case we will
      // fallback to DIRECT encoding.
      // The decision to use patched base was based on zigzag values, but the
      // actual patching is done on base reduced literals.
      if ((option.brBits100p - option.brBits95p) != 0) {
        option.encoding = PATCHED_BASE;
        preparePatchedBlob(option);
        return;
      } else {
        option.encoding = DIRECT;
        return;
      }
    } else {
      // if difference in bits between 95th percentile and 100th percentile is
      // 0, then patch length will become 0. Hence we will fallback to direct
      option.encoding = DIRECT;
      return;
    }
  }

  uint64_t RleEncoderV2::flush() {
    if (numLiterals != 0) {
      EncodingOption option = {};
      if (variableRunLength != 0) {
        determineEncoding(option);
        writeValues(option);
      } else if (fixedRunLength != 0) {
        if (fixedRunLength < MIN_REPEAT) {
          variableRunLength = fixedRunLength;
          fixedRunLength = 0;
          determineEncoding(option);
          writeValues(option);
        } else if (fixedRunLength >= MIN_REPEAT && fixedRunLength <= MAX_SHORT_REPEAT_LENGTH) {
          option.encoding = SHORT_REPEAT;
          writeValues(option);
        } else {
          option.encoding = DELTA;
          option.isFixedDelta = true;
          writeValues(option);
        }
      }
    }

    outputStream->BackUp(static_cast<int>(bufferLength - bufferPosition));
    uint64_t dataSize = outputStream->flush();
    bufferLength = bufferPosition = 0;
    return dataSize;
  }

  void RleEncoderV2::writeValues(EncodingOption& option) {
    if (numLiterals != 0) {
      switch (option.encoding) {
        case SHORT_REPEAT:
          writeShortRepeatValues(option);
          break;
        case DIRECT:
          writeDirectValues(option);
          break;
        case PATCHED_BASE:
          writePatchedBasedValues(option);
          break;
        case DELTA:
          writeDeltaValues(option);
          break;
        default:
          throw NotImplementedYet("Not implemented yet");
      }

      numLiterals = 0;
      prevDelta = 0;
    }
  }

  void RleEncoderV2::writeShortRepeatValues(EncodingOption&) {
    int64_t repeatVal;
    if (isSigned) {
      repeatVal = zigZag(literals[0]);
    } else {
      repeatVal = literals[0];
    }

    const uint32_t numBitsRepeatVal = findClosestNumBits(repeatVal);
    const uint32_t numBytesRepeatVal =
        numBitsRepeatVal % 8 == 0 ? (numBitsRepeatVal >> 3) : ((numBitsRepeatVal >> 3) + 1);

    uint32_t header = getOpCode(SHORT_REPEAT);

    fixedRunLength -= MIN_REPEAT;
    header |= fixedRunLength;
    header |= ((numBytesRepeatVal - 1) << 3);

    writeByte(static_cast<char>(header));

    for (int32_t i = static_cast<int32_t>(numBytesRepeatVal - 1); i >= 0; i--) {
      int64_t b = ((repeatVal >> (i * 8)) & 0xff);
      writeByte(static_cast<char>(b));
    }

    fixedRunLength = 0;
  }

  void RleEncoderV2::writeDirectValues(EncodingOption& option) {
    // write the number of fixed bits required in next 5 bits
    uint32_t fb = option.zzBits100p;
    if (alignedBitPacking) {
      fb = getClosestAlignedFixedBits(fb);
    }

    const uint32_t efb = encodeBitWidth(fb) << 1;

    // adjust variable run length
    variableRunLength -= 1;

    // extract the 9th bit of run length
    const uint32_t tailBits = (variableRunLength & 0x100) >> 8;

    // create first byte of the header
    const char headerFirstByte = static_cast<char>(getOpCode(DIRECT) | efb | tailBits);

    // second byte of the header stores the remaining 8 bits of runlength
    const char headerSecondByte = static_cast<char>(variableRunLength & 0xff);

    // write header
    writeByte(headerFirstByte);
    writeByte(headerSecondByte);

    // bit packing the zigzag encoded literals
    int64_t* currentZigzagLiterals = isSigned ? zigzagLiterals : literals;
    writeInts(currentZigzagLiterals, 0, numLiterals, fb);

    // reset run length
    variableRunLength = 0;
  }

  void RleEncoderV2::writePatchedBasedValues(EncodingOption& option) {
    // NOTE: Aligned bit packing cannot be applied for PATCHED_BASE encoding
    // because patch is applied to MSB bits. For example: If fixed bit width of
    // base value is 7 bits and if patch is 3 bits, the actual value is
    // constructed by shifting the patch to left by 7 positions.
    // actual_value = patch << 7 | base_value
    // So, if we align base_value then actual_value can not be reconstructed.

    // write the number of fixed bits required in next 5 bits
    const uint32_t efb = encodeBitWidth(option.brBits95p) << 1;

    // adjust variable run length, they are one off
    variableRunLength -= 1;

    // extract the 9th bit of run length
    const uint32_t tailBits = (variableRunLength & 0x100) >> 8;

    // create first byte of the header
    const char headerFirstByte = static_cast<char>(getOpCode(PATCHED_BASE) | efb | tailBits);

    // second byte of the header stores the remaining 8 bits of runlength
    const char headerSecondByte = static_cast<char>(variableRunLength & 0xff);

    // if the min value is negative toggle the sign
    const bool isNegative = (option.min < 0);
    if (isNegative) {
      option.min = -option.min;
    }

    // find the number of bytes required for base and shift it by 5 bits
    // to accommodate patch width. The additional bit is used to store the sign
    // of the base value.
    const uint32_t baseWidth = findClosestNumBits(option.min) + 1;
    const uint32_t baseBytes = baseWidth % 8 == 0 ? baseWidth / 8 : (baseWidth / 8) + 1;
    const uint32_t bb = (baseBytes - 1) << 5;

    // if the base value is negative then set MSB to 1
    if (isNegative) {
      option.min |= (1LL << ((baseBytes * 8) - 1));
    }

    // third byte contains 3 bits for number of bytes occupied by base
    // and 5 bits for patchWidth
    const char headerThirdByte = static_cast<char>(bb | encodeBitWidth(option.patchWidth));

    // fourth byte contains 3 bits for page gap width and 5 bits for
    // patch length
    const char headerFourthByte =
        static_cast<char>((option.patchGapWidth - 1) << 5 | option.patchLength);

    // write header
    writeByte(headerFirstByte);
    writeByte(headerSecondByte);
    writeByte(headerThirdByte);
    writeByte(headerFourthByte);

    // write the base value using fixed bytes in big endian order
    for (int32_t i = static_cast<int32_t>(baseBytes - 1); i >= 0; i--) {
      char b = static_cast<char>(((option.min >> (i * 8)) & 0xff));
      writeByte(b);
    }

    // base reduced literals are bit packed
    uint32_t closestFixedBits = getClosestFixedBits(option.brBits95p);

    writeInts(baseRedLiterals, 0, numLiterals, closestFixedBits);

    // write patch list
    closestFixedBits = getClosestFixedBits(option.patchGapWidth + option.patchWidth);

    writeInts(gapVsPatchList, 0, option.patchLength, closestFixedBits);

    // reset run length
    variableRunLength = 0;
  }

  void RleEncoderV2::writeDeltaValues(EncodingOption& option) {
    uint32_t len = 0;
    uint32_t fb = option.bitsDeltaMax;
    uint32_t efb = 0;

    if (alignedBitPacking) {
      fb = getClosestAlignedFixedBits(fb);
    }

    if (option.isFixedDelta) {
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
      efb = encodeBitWidth(fb) << 1;
      len = variableRunLength - 1;
      variableRunLength = 0;
    }

    // extract the 9th bit of run length
    const uint32_t tailBits = (len & 0x100) >> 8;

    // create first byte of the header
    const char headerFirstByte = static_cast<char>(getOpCode(DELTA) | efb | tailBits);

    // second byte of the header stores the remaining 8 bits of runlength
    const char headerSecondByte = static_cast<char>(len & 0xff);

    // write header
    writeByte(headerFirstByte);
    writeByte(headerSecondByte);

    // store the first value from zigzag literal array
    if (isSigned) {
      writeVslong(literals[0]);
    } else {
      writeVulong(literals[0]);
    }

    if (option.isFixedDelta) {
      // if delta is fixed then we don't need to store delta blob
      writeVslong(option.fixedDelta);
    } else {
      // store the first value as delta value using zigzag encoding
      writeVslong(adjDeltas[0]);

      // adjacent delta values are bit packed. The length of adjDeltas array is
      // always one less than the number of literals (delta difference for n
      // elements is n-1). We have already written one element, write the
      // remaining numLiterals - 2 elements here
      writeInts(adjDeltas, 1, numLiterals - 2, fb);
    }
  }

  void RleEncoderV2::writeInts(int64_t* input, uint32_t offset, size_t len, uint32_t bitSize) {
    if (input == nullptr || len < 1 || bitSize < 1) {
      return;
    }

    if (getClosestAlignedFixedBits(bitSize) == bitSize) {
      uint32_t numBytes;
      uint32_t endOffSet = static_cast<uint32_t>(offset + len);
      if (bitSize < 8) {
        char bitMask = static_cast<char>((1 << bitSize) - 1);
        uint32_t numHops = 8 / bitSize;
        uint32_t remainder = static_cast<uint32_t>(len % numHops);
        uint32_t endUnroll = endOffSet - remainder;
        for (uint32_t i = offset; i < endUnroll; i += numHops) {
          char toWrite = 0;
          for (uint32_t j = 0; j < numHops; ++j) {
            toWrite |= static_cast<char>((input[i + j] & bitMask) << (8 - (j + 1) * bitSize));
          }
          writeByte(toWrite);
        }

        if (remainder > 0) {
          uint32_t startShift = 8 - bitSize;
          char toWrite = 0;
          for (uint32_t i = endUnroll; i < endOffSet; ++i) {
            toWrite |= static_cast<char>((input[i] & bitMask) << startShift);
            startShift -= bitSize;
          }
          writeByte(toWrite);
        }

      } else {
        numBytes = bitSize / 8;

        for (uint32_t i = offset; i < endOffSet; ++i) {
          for (uint32_t j = 0; j < numBytes; ++j) {
            char toWrite = static_cast<char>((input[i] >> (8 * (numBytes - j - 1))) & 255);
            writeByte(toWrite);
          }
        }
      }

      return;
    }

    // write for unaligned bit size
    uint32_t bitsLeft = 8;
    char current = 0;
    for (uint32_t i = offset; i < (offset + len); i++) {
      int64_t value = input[i];
      uint32_t bitsToWrite = bitSize;
      while (bitsToWrite > bitsLeft) {
        // add the bits to the bottom of the current word
        current |= static_cast<char>(value >> (bitsToWrite - bitsLeft));
        // subtract out the bits we just added
        bitsToWrite -= bitsLeft;
        // zero out the bits above bitsToWrite
        value &= (static_cast<uint64_t>(1) << bitsToWrite) - 1;
        writeByte(current);
        current = 0;
        bitsLeft = 8;
      }
      bitsLeft -= bitsToWrite;
      current |= static_cast<char>(value << bitsLeft);
      if (bitsLeft == 0) {
        writeByte(current);
        current = 0;
        bitsLeft = 8;
      }
    }

    // flush
    if (bitsLeft != 8) {
      writeByte(current);
    }
  }

  void RleEncoderV2::initializeLiterals(int64_t val) {
    literals[numLiterals++] = val;
    fixedRunLength = 1;
    variableRunLength = 1;
  }
}  // namespace orc
