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

#ifndef ORC_RLE_HH
#define ORC_RLE_HH

#include "io/InputStream.hh"
#include "io/OutputStream.hh"

#include <memory>

namespace orc {

  inline int64_t zigZag(int64_t value) {
    return (value << 1) ^ (value >> 63);
  }

  inline int64_t unZigZag(uint64_t value) {
    return value >> 1 ^ -(value & 1);
  }

  class RleEncoder {
   public:
    // must be non-inline!
    virtual ~RleEncoder();

    RleEncoder(std::unique_ptr<BufferedOutputStream> outStream, bool hasSigned)
        : outputStream(std::move(outStream)),
          bufferPosition(0),
          bufferLength(0),
          numLiterals(0),
          isSigned(hasSigned),
          buffer(nullptr) {
      // pass
    }

    /**
     * Encode the next batch of values.
     * @param data the array to read from
     * @param numValues the number of values to write
     * @param notNull If the pointer is null, all values are read. If the
     *    pointer is not null, positions that are false are skipped.
     */
    template <typename T>
    void add(const T* data, uint64_t numValues, const char* notNull);

    virtual void add(const int64_t* data, uint64_t numValues, const char* notNull);

    virtual void add(const int32_t* data, uint64_t numValues, const char* notNull);

    virtual void add(const int16_t* data, uint64_t numValues, const char* notNull);
    /**
     * Get size of buffer used so far.
     */
    uint64_t getBufferSize() const {
      return outputStream->getSize();
    }

    /**
     * Flushing underlying BufferedOutputStream
     */
    virtual uint64_t flush() = 0;

    /**
     * record current position
     * @param recorder use the recorder to record current positions
     */
    virtual void recordPosition(PositionRecorder* recorder) const;

    virtual void write(int64_t val) = 0;

   protected:
    std::unique_ptr<BufferedOutputStream> outputStream;
    size_t bufferPosition;
    size_t bufferLength;
    size_t numLiterals;
    int64_t* literals;
    bool isSigned;
    char* buffer;

    virtual void writeByte(char c);

    virtual void writeVulong(int64_t val);

    virtual void writeVslong(int64_t val);
  };

  class RleDecoder {
   public:
    // must be non-inline!
    virtual ~RleDecoder();

    RleDecoder(ReaderMetrics* _metrics) : metrics(_metrics) {
      // pass
    }

    /**
     * Seek to a particular spot.
     */
    virtual void seek(PositionProvider&) = 0;

    /**
     * Seek over a given number of values.
     */
    virtual void skip(uint64_t numValues) = 0;

    /**
     * Read a number of values into the batch.
     * @param data the array to read into
     * @param numValues the number of values to read
     * @param notNull If the pointer is null, all values are read. If the
     *    pointer is not null, positions that are false are skipped.
     */
    virtual void next(int64_t* data, uint64_t numValues, const char* notNull) = 0;

    virtual void next(int32_t* data, uint64_t numValues, const char* notNull) = 0;

    virtual void next(int16_t* data, uint64_t numValues, const char* notNull) = 0;

   protected:
    ReaderMetrics* metrics;
  };

  /**
   * Create an RLE encoder.
   * @param output the output stream to write to
   * @param isSigned true if the number sequence is signed
   * @param version version of RLE decoding to do
   * @param pool memory pool to use for allocation
   */
  std::unique_ptr<RleEncoder> createRleEncoder(std::unique_ptr<BufferedOutputStream> output,
                                               bool isSigned, RleVersion version, MemoryPool& pool,
                                               bool alignedBitpacking);

  /**
   * Create an RLE decoder.
   * @param input the input stream to read from
   * @param isSigned true if the number sequence is signed
   * @param version version of RLE decoding to do
   * @param pool memory pool to use for allocation
   */
  std::unique_ptr<RleDecoder> createRleDecoder(std::unique_ptr<SeekableInputStream> input,
                                               bool isSigned, RleVersion version, MemoryPool& pool,
                                               ReaderMetrics* metrics);

}  // namespace orc

#endif  // ORC_RLE_HH
