/**
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

#ifndef ORC_RLEV2_HH
#define ORC_RLEV2_HH

#include "Adaptor.hh"
#include "RLE.hh"
#include "orc/Exceptions.hh"

#include <vector>

#define MAX_LITERAL_SIZE 512
#define MIN_REPEAT 3
#define HIST_LEN 32
namespace orc {

  struct FixedBitSizes {
    enum FBS {
      ONE = 0,
      TWO,
      THREE,
      FOUR,
      FIVE,
      SIX,
      SEVEN,
      EIGHT,
      NINE,
      TEN,
      ELEVEN,
      TWELVE,
      THIRTEEN,
      FOURTEEN,
      FIFTEEN,
      SIXTEEN,
      SEVENTEEN,
      EIGHTEEN,
      NINETEEN,
      TWENTY,
      TWENTYONE,
      TWENTYTWO,
      TWENTYTHREE,
      TWENTYFOUR,
      TWENTYSIX,
      TWENTYEIGHT,
      THIRTY,
      THIRTYTWO,
      FORTY,
      FORTYEIGHT,
      FIFTYSIX,
      SIXTYFOUR,
      SIZE
    };
  };

  enum EncodingType { SHORT_REPEAT = 0, DIRECT = 1, PATCHED_BASE = 2, DELTA = 3 };

  struct EncodingOption {
    EncodingType encoding;
    int64_t fixedDelta;
    int64_t gapVsPatchListCount;
    int64_t zigzagLiteralsCount;
    int64_t baseRedLiteralsCount;
    int64_t adjDeltasCount;
    uint32_t zzBits90p;
    uint32_t zzBits100p;
    uint32_t brBits95p;
    uint32_t brBits100p;
    uint32_t bitsDeltaMax;
    uint32_t patchWidth;
    uint32_t patchGapWidth;
    uint32_t patchLength;
    int64_t min;
    bool isFixedDelta;
  };

  class RleEncoderV2 : public RleEncoder {
   public:
    RleEncoderV2(std::unique_ptr<BufferedOutputStream> outStream, bool hasSigned,
                 bool alignBitPacking = true);

    ~RleEncoderV2() override {
      delete[] literals;
      delete[] gapVsPatchList;
      delete[] zigzagLiterals;
      delete[] baseRedLiterals;
      delete[] adjDeltas;
    }
    /**
     * Flushing underlying BufferedOutputStream
     */
    uint64_t flush() override;

    void write(int64_t val) override;

   private:
    const bool alignedBitPacking;
    uint32_t fixedRunLength;
    uint32_t variableRunLength;
    int64_t prevDelta;
    int32_t histgram[HIST_LEN];

    // The four list below should actually belong to EncodingOption since it only holds temporal
    // values in write(int64_t val), it is move here for performance consideration.
    int64_t* gapVsPatchList;
    int64_t* zigzagLiterals;
    int64_t* baseRedLiterals;
    int64_t* adjDeltas;

    uint32_t getOpCode(EncodingType encoding);
    int64_t* prepareForDirectOrPatchedBase(EncodingOption& option);
    void determineEncoding(EncodingOption& option);
    void computeZigZagLiterals(EncodingOption& option);
    void preparePatchedBlob(EncodingOption& option);

    void writeInts(int64_t* input, uint32_t offset, size_t len, uint32_t bitSize);
    void initializeLiterals(int64_t val);
    void writeValues(EncodingOption& option);
    void writeShortRepeatValues(EncodingOption& option);
    void writeDirectValues(EncodingOption& option);
    void writePatchedBasedValues(EncodingOption& option);
    void writeDeltaValues(EncodingOption& option);
    uint32_t percentileBits(int64_t* data, size_t offset, size_t length, double p,
                            bool reuseHist = false);
  };

  class RleDecoderV2 : public RleDecoder {
   public:
    RleDecoderV2(std::unique_ptr<SeekableInputStream> input, bool isSigned, MemoryPool& pool,
                 ReaderMetrics* metrics);

    /**
     * Seek to a particular spot.
     */
    void seek(PositionProvider&) override;

    /**
     * Seek over a given number of values.
     */
    void skip(uint64_t numValues) override;

    /**
     * Read a number of values into the batch.
     */
    template <typename T>
    void next(T* data, uint64_t numValues, const char* notNull);

    void next(int64_t* data, uint64_t numValues, const char* notNull) override;

    void next(int32_t* data, uint64_t numValues, const char* notNull) override;

    void next(int16_t* data, uint64_t numValues, const char* notNull) override;

   private:
    /**
     * Decode the next gap and patch from 'unpackedPatch' and update the index on it.
     * Used by PATCHED_BASE.
     *
     * @param patchBitSize  bit size of the patch value
     * @param patchMask     mask for the patch value
     * @param resGap        result of gap
     * @param resPatch      result of patch
     * @param patchIdx      current index in the 'unpackedPatch' buffer
     */
    void adjustGapAndPatch(uint32_t patchBitSize, int64_t patchMask, int64_t* resGap,
                           int64_t* resPatch, uint64_t* patchIdx);

    void resetReadLongs() {
      bitsLeft = 0;
      curByte = 0;
    }

    void resetRun() {
      resetReadLongs();
    }

    unsigned char readByte();

    int64_t readLongBE(uint64_t bsz);
    int64_t readVslong();
    uint64_t readVulong();
    void readLongs(int64_t* data, uint64_t offset, uint64_t len, uint64_t fbs);
    void plainUnpackLongs(int64_t* data, uint64_t offset, uint64_t len, uint64_t fbs);

    void unrolledUnpack4(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack8(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack16(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack24(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack32(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack40(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack48(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack56(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack64(int64_t* data, uint64_t offset, uint64_t len);

    template <typename T>
    uint64_t nextShortRepeats(T* data, uint64_t offset, uint64_t numValues, const char* notNull);
    template <typename T>
    uint64_t nextDirect(T* data, uint64_t offset, uint64_t numValues, const char* notNull);
    template <typename T>
    uint64_t nextPatched(T* data, uint64_t offset, uint64_t numValues, const char* notNull);
    template <typename T>
    uint64_t nextDelta(T* data, uint64_t offset, uint64_t numValues, const char* notNull);
    template <typename T>
    uint64_t copyDataFromBuffer(T* data, uint64_t offset, uint64_t numValues, const char* notNull);

    const std::unique_ptr<SeekableInputStream> inputStream;
    const bool isSigned;

    unsigned char firstByte;
    uint64_t runLength;  // Length of the current run
    uint64_t runRead;    // Number of returned values of the current run
    const char* bufferStart;
    const char* bufferEnd;
    uint32_t bitsLeft;                  // Used by readLongs when bitSize < 8
    uint32_t curByte;                   // Used by anything that uses readLongs
    DataBuffer<int64_t> unpackedPatch;  // Used by PATCHED_BASE
    DataBuffer<int64_t> literals;       // Values of the current run
  };
}  // namespace orc

#endif  // ORC_RLEV2_HH
