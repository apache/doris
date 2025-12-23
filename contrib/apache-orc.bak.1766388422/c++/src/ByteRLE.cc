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

#include <string.h>
#include <algorithm>
#include <iostream>
#include <utility>

#include "ByteRLE.hh"
#include "Utils.hh"
#include "orc/Exceptions.hh"

namespace orc {

  const int MINIMUM_REPEAT = 3;
  const int MAXIMUM_REPEAT = 127 + MINIMUM_REPEAT;
  const int MAX_LITERAL_SIZE = 128;

  ByteRleEncoder::~ByteRleEncoder() {
    // PASS
  }

  class ByteRleEncoderImpl : public ByteRleEncoder {
   public:
    ByteRleEncoderImpl(std::unique_ptr<BufferedOutputStream> output);
    virtual ~ByteRleEncoderImpl() override;

    /**
     * Encode the next batch of values.
     * @param data to be encoded
     * @param numValues the number of values to be encoded
     * @param notNull If the pointer is null, all values are read. If the
     *    pointer is not null, positions that are false are skipped.
     */
    virtual void add(const char* data, uint64_t numValues, const char* notNull) override;

    /**
     * Get size of buffer used so far.
     */
    virtual uint64_t getBufferSize() const override;

    /**
     * Flush underlying BufferedOutputStream.
     */
    virtual uint64_t flush() override;

    virtual void recordPosition(PositionRecorder* recorder) const override;

    virtual void suppress() override;

    /**
     * Reset to initial state
     */
    void reset();

   protected:
    std::unique_ptr<BufferedOutputStream> outputStream;
    char* literals;
    int numLiterals;
    bool repeat;
    int tailRunLength;
    int bufferPosition;
    int bufferLength;
    char* buffer;

    void writeByte(char c);
    void writeValues();
    void write(char c);
  };

  ByteRleEncoderImpl::ByteRleEncoderImpl(std::unique_ptr<BufferedOutputStream> output)
      : outputStream(std::move(output)) {
    literals = new char[MAX_LITERAL_SIZE];
    reset();
  }

  ByteRleEncoderImpl::~ByteRleEncoderImpl() {
    // PASS
    delete[] literals;
  }

  void ByteRleEncoderImpl::writeByte(char c) {
    if (bufferPosition == bufferLength) {
      int addedSize = 0;
      if (!outputStream->Next(reinterpret_cast<void**>(&buffer), &addedSize)) {
        throw std::bad_alloc();
      }
      bufferPosition = 0;
      bufferLength = addedSize;
    }
    buffer[bufferPosition++] = c;
  }

  void ByteRleEncoderImpl::add(const char* data, uint64_t numValues, const char* notNull) {
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        write(data[i]);
      }
    }
  }

  void ByteRleEncoderImpl::writeValues() {
    if (numLiterals != 0) {
      if (repeat) {
        writeByte(static_cast<char>(numLiterals - static_cast<int>(MINIMUM_REPEAT)));
        writeByte(literals[0]);
      } else {
        writeByte(static_cast<char>(-numLiterals));
        for (int i = 0; i < numLiterals; ++i) {
          writeByte(literals[i]);
        }
      }
      repeat = false;
      tailRunLength = 0;
      numLiterals = 0;
    }
  }

  uint64_t ByteRleEncoderImpl::flush() {
    writeValues();
    outputStream->BackUp(bufferLength - bufferPosition);
    uint64_t dataSize = outputStream->flush();
    bufferLength = bufferPosition = 0;
    return dataSize;
  }

  void ByteRleEncoderImpl::write(char value) {
    if (numLiterals == 0) {
      literals[numLiterals++] = value;
      tailRunLength = 1;
    } else if (repeat) {
      if (value == literals[0]) {
        numLiterals += 1;
        if (numLiterals == MAXIMUM_REPEAT) {
          writeValues();
        }
      } else {
        writeValues();
        literals[numLiterals++] = value;
        tailRunLength = 1;
      }
    } else {
      if (value == literals[numLiterals - 1]) {
        tailRunLength += 1;
      } else {
        tailRunLength = 1;
      }
      if (tailRunLength == MINIMUM_REPEAT) {
        if (numLiterals + 1 == MINIMUM_REPEAT) {
          repeat = true;
          numLiterals += 1;
        } else {
          numLiterals -= static_cast<int>(MINIMUM_REPEAT - 1);
          writeValues();
          literals[0] = value;
          repeat = true;
          numLiterals = MINIMUM_REPEAT;
        }
      } else {
        literals[numLiterals++] = value;
        if (numLiterals == MAX_LITERAL_SIZE) {
          writeValues();
        }
      }
    }
  }

  uint64_t ByteRleEncoderImpl::getBufferSize() const {
    return outputStream->getSize();
  }

  void ByteRleEncoderImpl::recordPosition(PositionRecorder* recorder) const {
    uint64_t flushedSize = outputStream->getSize();
    uint64_t unflushedSize = static_cast<uint64_t>(bufferPosition);
    if (outputStream->isCompressed()) {
      // start of the compression chunk in the stream
      recorder->add(flushedSize);
      // number of decompressed bytes that need to be consumed
      recorder->add(unflushedSize);
    } else {
      flushedSize -= static_cast<uint64_t>(bufferLength);
      // byte offset of the RLE run’s start location
      recorder->add(flushedSize + unflushedSize);
    }
    recorder->add(static_cast<uint64_t>(numLiterals));
  }

  void ByteRleEncoderImpl::reset() {
    numLiterals = 0;
    tailRunLength = 0;
    repeat = false;
    bufferPosition = 0;
    bufferLength = 0;
    buffer = nullptr;
  }

  void ByteRleEncoderImpl::suppress() {
    // written data can be just ignored because they are only flushed in memory
    outputStream->suppress();
    reset();
  }

  std::unique_ptr<ByteRleEncoder> createByteRleEncoder(
      std::unique_ptr<BufferedOutputStream> output) {
    return std::make_unique<ByteRleEncoderImpl>(std::move(output));
  }

  class BooleanRleEncoderImpl : public ByteRleEncoderImpl {
   public:
    BooleanRleEncoderImpl(std::unique_ptr<BufferedOutputStream> output);
    virtual ~BooleanRleEncoderImpl() override;

    /**
     * Encode the next batch of values
     * @param data to be encoded
     * @param numValues the number of values to be encoded
     * @param notNull If the pointer is null, all values are read. If the
     *    pointer is not null, positions that are false are skipped.
     */
    virtual void add(const char* data, uint64_t numValues, const char* notNull) override;

    /**
     * Flushing underlying BufferedOutputStream
     */
    virtual uint64_t flush() override;

    virtual void recordPosition(PositionRecorder* recorder) const override;

    virtual void suppress() override;

   private:
    int bitsRemained;
    char current;
  };

  BooleanRleEncoderImpl::BooleanRleEncoderImpl(std::unique_ptr<BufferedOutputStream> output)
      : ByteRleEncoderImpl(std::move(output)) {
    bitsRemained = 8;
    current = static_cast<char>(0);
  }

  BooleanRleEncoderImpl::~BooleanRleEncoderImpl() {
    // PASS
  }

  void BooleanRleEncoderImpl::add(const char* data, uint64_t numValues, const char* notNull) {
    for (uint64_t i = 0; i < numValues; ++i) {
      if (bitsRemained == 0) {
        write(current);
        current = static_cast<char>(0);
        bitsRemained = 8;
      }
      if (!notNull || notNull[i]) {
        if (!data || data[i]) {
          current = static_cast<char>(current | (0x80 >> (8 - bitsRemained)));
        }
        --bitsRemained;
      }
    }
    if (bitsRemained == 0) {
      write(current);
      current = static_cast<char>(0);
      bitsRemained = 8;
    }
  }

  uint64_t BooleanRleEncoderImpl::flush() {
    if (bitsRemained != 8) {
      write(current);
    }
    bitsRemained = 8;
    current = static_cast<char>(0);
    return ByteRleEncoderImpl::flush();
  }

  void BooleanRleEncoderImpl::recordPosition(PositionRecorder* recorder) const {
    ByteRleEncoderImpl::recordPosition(recorder);
    recorder->add(static_cast<uint64_t>(8 - bitsRemained));
  }

  void BooleanRleEncoderImpl::suppress() {
    ByteRleEncoderImpl::suppress();
    bitsRemained = 8;
    current = static_cast<char>(0);
  }

  std::unique_ptr<ByteRleEncoder> createBooleanRleEncoder(
      std::unique_ptr<BufferedOutputStream> output) {
    BooleanRleEncoderImpl* encoder = new BooleanRleEncoderImpl(std::move(output));
    return std::unique_ptr<ByteRleEncoder>(reinterpret_cast<ByteRleEncoder*>(encoder));
  }

  ByteRleDecoder::~ByteRleDecoder() {
    // PASS
  }

  class ByteRleDecoderImpl : public ByteRleDecoder {
   public:
    ByteRleDecoderImpl(std::unique_ptr<SeekableInputStream> input, ReaderMetrics* metrics);

    ~ByteRleDecoderImpl() override;

    /**
     * Seek to a particular spot.
     */
    virtual void seek(PositionProvider&) override;

    /**
     * Seek over a given number of values.
     */
    virtual void skip(uint64_t numValues) override;

    /**
     * Read a number of values into the batch.
     */
    virtual void next(char* data, uint64_t numValues, char* notNull) override;

   protected:
    void nextInternal(char* data, uint64_t numValues, char* notNull);
    inline void nextBuffer();
    inline signed char readByte();
    inline void readHeader();
    inline void reset();

    std::unique_ptr<SeekableInputStream> inputStream;
    size_t remainingValues;
    char value;
    const char* bufferStart;
    const char* bufferEnd;
    bool repeating;
    ReaderMetrics* metrics;
  };

  void ByteRleDecoderImpl::nextBuffer() {
    SCOPED_MINUS_STOPWATCH(metrics, ByteDecodingLatencyUs);
    int bufferLength;
    const void* bufferPointer;
    bool result = inputStream->Next(&bufferPointer, &bufferLength);
    if (!result) {
      throw ParseError("bad read in nextBuffer");
    }
    bufferStart = static_cast<const char*>(bufferPointer);
    bufferEnd = bufferStart + bufferLength;
  }

  signed char ByteRleDecoderImpl::readByte() {
    if (bufferStart == bufferEnd) {
      nextBuffer();
    }
    return static_cast<signed char>(*(bufferStart++));
  }

  void ByteRleDecoderImpl::readHeader() {
    signed char ch = readByte();
    if (ch < 0) {
      remainingValues = static_cast<size_t>(-ch);
      repeating = false;
    } else {
      remainingValues = static_cast<size_t>(ch) + MINIMUM_REPEAT;
      repeating = true;
      value = static_cast<char>(readByte());
    }
  }

  void ByteRleDecoderImpl::reset() {
    repeating = false;
    remainingValues = 0;
    value = 0;
    bufferStart = nullptr;
    bufferEnd = nullptr;
  }

  ByteRleDecoderImpl::ByteRleDecoderImpl(std::unique_ptr<SeekableInputStream> input,
                                         ReaderMetrics* _metrics)
      : metrics(_metrics) {
    inputStream = std::move(input);
    reset();
  }

  ByteRleDecoderImpl::~ByteRleDecoderImpl() {
    // PASS
  }

  void ByteRleDecoderImpl::seek(PositionProvider& location) {
    // move the input stream
    inputStream->seek(location);
    // reset the decoder status and lazily call readHeader()
    reset();
    // skip ahead the given number of records
    ByteRleDecoderImpl::skip(location.next());
  }

  void ByteRleDecoderImpl::skip(uint64_t numValues) {
    SCOPED_STOPWATCH(metrics, ByteDecodingLatencyUs, ByteDecodingCall);
    while (numValues > 0) {
      if (remainingValues == 0) {
        readHeader();
      }
      size_t count = std::min(static_cast<size_t>(numValues), remainingValues);
      remainingValues -= count;
      numValues -= count;
      // for literals we need to skip over count bytes, which may involve
      // reading from the underlying stream
      if (!repeating) {
        size_t consumedBytes = count;
        while (consumedBytes > 0) {
          if (bufferStart == bufferEnd) {
            nextBuffer();
          }
          size_t skipSize = std::min(static_cast<size_t>(consumedBytes),
                                     static_cast<size_t>(bufferEnd - bufferStart));
          bufferStart += skipSize;
          consumedBytes -= skipSize;
        }
      }
    }
  }

  void ByteRleDecoderImpl::next(char* data, uint64_t numValues, char* notNull) {
    SCOPED_STOPWATCH(metrics, ByteDecodingLatencyUs, ByteDecodingCall);
    nextInternal(data, numValues, notNull);
  }

  void ByteRleDecoderImpl::nextInternal(char* data, uint64_t numValues, char* notNull) {
    uint64_t position = 0;
    // skip over null values
    while (notNull && position < numValues && !notNull[position]) {
      position += 1;
    }
    while (position < numValues) {
      // if we are out of values, read more
      if (remainingValues == 0) {
        readHeader();
      }
      // how many do we read out of this block?
      size_t count = std::min(static_cast<size_t>(numValues - position), remainingValues);
      uint64_t consumed = 0;
      if (repeating) {
        if (notNull) {
          for (uint64_t i = 0; i < count; ++i) {
            if (notNull[position + i]) {
              data[position + i] = value;
              consumed += 1;
            }
          }
        } else {
          memset(data + position, value, count);
          consumed = count;
        }
      } else {
        if (notNull) {
          for (uint64_t i = 0; i < count; ++i) {
            if (notNull[position + i]) {
              data[position + i] = static_cast<char>(readByte());
              consumed += 1;
            }
          }
        } else {
          uint64_t i = 0;
          while (i < count) {
            if (bufferStart == bufferEnd) {
              nextBuffer();
            }
            uint64_t copyBytes = std::min(static_cast<uint64_t>(count - i),
                                          static_cast<uint64_t>(bufferEnd - bufferStart));
            memcpy(data + position + i, bufferStart, copyBytes);
            bufferStart += copyBytes;
            i += copyBytes;
          }
          consumed = count;
        }
      }
      remainingValues -= consumed;
      position += count;
      // skip over any null values
      while (notNull && position < numValues && !notNull[position]) {
        position += 1;
      }
    }
  }

  std::unique_ptr<ByteRleDecoder> createByteRleDecoder(std::unique_ptr<SeekableInputStream> input,
                                                       ReaderMetrics* metrics) {
    return std::make_unique<ByteRleDecoderImpl>(std::move(input), metrics);
  }

  class BooleanRleDecoderImpl : public ByteRleDecoderImpl {
   public:
    BooleanRleDecoderImpl(std::unique_ptr<SeekableInputStream> input, ReaderMetrics* metrics);

    ~BooleanRleDecoderImpl() override;

    /**
     * Seek to a particular spot.
     */
    virtual void seek(PositionProvider&) override;

    /**
     * Seek over a given number of values.
     */
    virtual void skip(uint64_t numValues) override;

    /**
     * Read a number of values into the batch.
     */
    virtual void next(char* data, uint64_t numValues, char* notNull) override;

   protected:
    size_t remainingBits;
    char lastByte;
  };

  BooleanRleDecoderImpl::BooleanRleDecoderImpl(std::unique_ptr<SeekableInputStream> input,
                                               ReaderMetrics* _metrics)
      : ByteRleDecoderImpl(std::move(input), _metrics) {
    remainingBits = 0;
    lastByte = 0;
  }

  BooleanRleDecoderImpl::~BooleanRleDecoderImpl() {
    // PASS
  }

  void BooleanRleDecoderImpl::seek(PositionProvider& location) {
    ByteRleDecoderImpl::seek(location);
    uint64_t consumed = location.next();
    remainingBits = 0;
    if (consumed > 8) {
      throw ParseError("bad position");
    }
    if (consumed != 0) {
      remainingBits = 8 - consumed;
      ByteRleDecoderImpl::next(&lastByte, 1, nullptr);
    }
  }

  void BooleanRleDecoderImpl::skip(uint64_t numValues) {
    if (numValues <= remainingBits) {
      remainingBits -= numValues;
    } else {
      numValues -= remainingBits;
      uint64_t bytesSkipped = numValues / 8;
      ByteRleDecoderImpl::skip(bytesSkipped);
      if (numValues % 8 != 0) {
        ByteRleDecoderImpl::next(&lastByte, 1, nullptr);
        remainingBits = 8 - (numValues % 8);
      } else {
        remainingBits = 0;
      }
    }
  }

  void BooleanRleDecoderImpl::next(char* data, uint64_t numValues, char* notNull) {
    SCOPED_STOPWATCH(metrics, ByteDecodingLatencyUs, ByteDecodingCall);
    // next spot to fill in
    uint64_t position = 0;

    // use up any remaining bits
    if (notNull) {
      while (remainingBits > 0 && position < numValues) {
        if (notNull[position]) {
          remainingBits -= 1;
          data[position] = (static_cast<unsigned char>(lastByte) >> remainingBits) & 0x1;
        } else {
          data[position] = 0;
        }
        position += 1;
      }
    } else {
      while (remainingBits > 0 && position < numValues) {
        remainingBits -= 1;
        data[position++] = (static_cast<unsigned char>(lastByte) >> remainingBits) & 0x1;
      }
    }

    // count the number of nonNulls remaining
    uint64_t nonNulls = numValues - position;
    if (notNull) {
      for (uint64_t i = position; i < numValues; ++i) {
        if (!notNull[i]) {
          nonNulls -= 1;
        }
      }
    }

    // fill in the remaining values
    if (nonNulls == 0) {
      while (position < numValues) {
        data[position++] = 0;
      }
    } else if (position < numValues) {
      // read the new bytes into the array
      uint64_t bytesRead = (nonNulls + 7) / 8;
      ByteRleDecoderImpl::nextInternal(data + position, bytesRead, nullptr);
      lastByte = data[position + bytesRead - 1];
      remainingBits = bytesRead * 8 - nonNulls;
      // expand the array backwards so that we don't clobber the data
      uint64_t bitsLeft = bytesRead * 8 - remainingBits;
      if (notNull) {
        for (int64_t i = static_cast<int64_t>(numValues) - 1; i >= static_cast<int64_t>(position);
             --i) {
          if (notNull[i]) {
            uint64_t shiftPosn = (-bitsLeft) % 8;
            data[i] = (data[position + (bitsLeft - 1) / 8] >> shiftPosn) & 0x1;
            bitsLeft -= 1;
          } else {
            data[i] = 0;
          }
        }
      } else {
        for (int64_t i = static_cast<int64_t>(numValues) - 1; i >= static_cast<int64_t>(position);
             --i, --bitsLeft) {
          uint64_t shiftPosn = (-bitsLeft) % 8;
          data[i] = (data[position + (bitsLeft - 1) / 8] >> shiftPosn) & 0x1;
        }
      }
    }
  }

  std::unique_ptr<ByteRleDecoder> createBooleanRleDecoder(
      std::unique_ptr<SeekableInputStream> input, ReaderMetrics* metrics) {
    return std::make_unique<BooleanRleDecoderImpl>(std::move(input), metrics);
  }
}  // namespace orc
