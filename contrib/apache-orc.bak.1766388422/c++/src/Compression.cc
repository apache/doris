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

#include "Compression.hh"
#include "Adaptor.hh"
#include "LzoDecompressor.hh"
#include "Utils.hh"
#include "lz4.h"
#include "orc/Exceptions.hh"

#include <algorithm>
#include <array>
#include <iomanip>
#include <iostream>

// Only used on x86 or x86_64
#if defined(__x86_64__) || defined(_M_X64) || defined(i386) || defined(__i386__) || \
  defined(__i386) || defined(_M_IX86)
#include <libdeflate.h>
#endif

#include <sstream>

#include "zlib.h"
#include "zstd.h"

#include "wrap/snappy-wrapper.h"

#ifndef ZSTD_CLEVEL_DEFAULT
#define ZSTD_CLEVEL_DEFAULT 3
#endif

/* These macros are defined in lz4.c */
#ifndef LZ4_ACCELERATION_DEFAULT
#define LZ4_ACCELERATION_DEFAULT 1
#endif

#ifndef LZ4_ACCELERATION_MAX
#define LZ4_ACCELERATION_MAX 65537
#endif

namespace orc {

  class CompressionStreamBase : public BufferedOutputStream {
   public:
    CompressionStreamBase(OutputStream* outStream, int compressionLevel, uint64_t capacity,
                          uint64_t blockSize, MemoryPool& pool, WriterMetrics* metrics);

    virtual bool Next(void** data, int* size) override = 0;
    virtual void BackUp(int count) override;

    virtual std::string getName() const override = 0;
    virtual uint64_t flush() override;
    virtual void suppress() override;

    virtual bool isCompressed() const override {
      return true;
    }
    virtual uint64_t getSize() const override;

   protected:
    void writeData(const unsigned char* data, int size);

    void writeHeader(size_t compressedSize, bool original) {
      *header[0] = static_cast<char>((compressedSize << 1) + (original ? 1 : 0));
      *header[1] = static_cast<char>(compressedSize >> 7);
      *header[2] = static_cast<char>(compressedSize >> 15);
    }

    // ensure enough room for compression block header
    void ensureHeader();

    // Buffer to hold uncompressed data until user calls Next()
    DataBuffer<unsigned char> rawInputBuffer;

    // Compress level
    int level;

    // Compressed data output buffer
    char* outputBuffer;

    // Size for compressionBuffer
    int bufferSize;

    // Compress output position
    int outputPosition;

    // Compress output buffer size
    int outputSize;

    // Compression block header pointer array
    static const uint32_t HEADER_SIZE = 3;
    std::array<char*, HEADER_SIZE> header;
  };

  CompressionStreamBase::CompressionStreamBase(OutputStream* outStream, int compressionLevel,
                                               uint64_t capacity, uint64_t blockSize,
                                               MemoryPool& pool, WriterMetrics* metrics)
      : BufferedOutputStream(pool, outStream, capacity, blockSize, metrics),
        rawInputBuffer(pool, blockSize),
        level(compressionLevel),
        outputBuffer(nullptr),
        bufferSize(0),
        outputPosition(0),
        outputSize(0) {
    // init header pointer array
    header.fill(nullptr);
  }

  void CompressionStreamBase::BackUp(int count) {
    if (count > bufferSize) {
      throw std::logic_error("Can't backup that much!");
    }
    bufferSize -= count;
  }

  uint64_t CompressionStreamBase::flush() {
    void* data;
    int size;
    if (!Next(&data, &size)) {
      throw std::runtime_error("Failed to flush compression buffer.");
    }
    BufferedOutputStream::BackUp(outputSize - outputPosition);
    bufferSize = outputSize = outputPosition = 0;
    return BufferedOutputStream::flush();
  }

  void CompressionStreamBase::suppress() {
    outputBuffer = nullptr;
    bufferSize = outputPosition = outputSize = 0;
    BufferedOutputStream::suppress();
  }

  uint64_t CompressionStreamBase::getSize() const {
    return BufferedOutputStream::getSize() - static_cast<uint64_t>(outputSize - outputPosition);
  }

  // write the data content into outputBuffer
  void CompressionStreamBase::writeData(const unsigned char* data, int size) {
    int offset = 0;
    while (offset < size) {
      if (outputPosition == outputSize) {
        if (!BufferedOutputStream::Next(reinterpret_cast<void**>(&outputBuffer), &outputSize)) {
          throw std::runtime_error("Failed to get next output buffer from output stream.");
        }
        outputPosition = 0;
      } else if (outputPosition > outputSize) {
        // for safety this will unlikely happen
        throw std::logic_error("Write to an out-of-bound place during compression!");
      }
      int currentSize = std::min(outputSize - outputPosition, size - offset);
      memcpy(outputBuffer + outputPosition, data + offset, static_cast<size_t>(currentSize));
      offset += currentSize;
      outputPosition += currentSize;
    }
  }

  void CompressionStreamBase::ensureHeader() {
    // adjust 3 bytes for the compression header
    for (uint32_t i = 0; i < HEADER_SIZE; ++i) {
      if (outputPosition >= outputSize) {
        if (!BufferedOutputStream::Next(reinterpret_cast<void**>(&outputBuffer), &outputSize)) {
          throw std::runtime_error("Failed to get next output buffer from output stream.");
        }
        outputPosition = 0;
      }
      header[i] = outputBuffer + outputPosition;
      ++outputPosition;
    }
  }

  /**
   * Streaming compression base class
   */
  class CompressionStream : public CompressionStreamBase {
   public:
    CompressionStream(OutputStream* outStream, int compressionLevel, uint64_t capacity,
                      uint64_t blockSize, MemoryPool& pool, WriterMetrics* metrics);

    virtual bool Next(void** data, int* size) override;
    virtual std::string getName() const override = 0;

   protected:
    // return total compressed size
    virtual uint64_t doStreamingCompression() = 0;
  };

  CompressionStream::CompressionStream(OutputStream* outStream, int compressionLevel,
                                       uint64_t capacity, uint64_t blockSize, MemoryPool& pool,
                                       WriterMetrics* metrics)
      : CompressionStreamBase(outStream, compressionLevel, capacity, blockSize, pool, metrics) {
    // PASS
  }

  bool CompressionStream::Next(void** data, int* size) {
    if (bufferSize != 0) {
      ensureHeader();

      uint64_t preSize = getSize();
      uint64_t totalCompressedSize = doStreamingCompression();
      if (totalCompressedSize >= static_cast<unsigned long>(bufferSize)) {
        writeHeader(static_cast<size_t>(bufferSize), true);
        // reset output buffer
        outputBuffer = nullptr;
        outputPosition = outputSize = 0;
        uint64_t backup = getSize() - preSize;
        BufferedOutputStream::BackUp(static_cast<int>(backup));

        // copy raw input buffer into block buffer
        writeData(rawInputBuffer.data(), bufferSize);
      } else {
        writeHeader(totalCompressedSize, false);
      }
    }

    *data = rawInputBuffer.data();
    *size = static_cast<int>(rawInputBuffer.size());
    bufferSize = *size;

    return true;
  }

  class ZlibCompressionStream : public CompressionStream {
   public:
    ZlibCompressionStream(OutputStream* outStream, int compressionLevel, uint64_t capacity,
                          uint64_t blockSize, MemoryPool& pool, WriterMetrics* metrics);

    virtual ~ZlibCompressionStream() override {
      end();
    }

    virtual std::string getName() const override;

   protected:
    virtual uint64_t doStreamingCompression() override;

   private:
    void init();
    void end();
    z_stream strm;
  };

  ZlibCompressionStream::ZlibCompressionStream(OutputStream* outStream, int compressionLevel,
                                               uint64_t capacity, uint64_t blockSize,
                                               MemoryPool& pool, WriterMetrics* metrics)
      : CompressionStream(outStream, compressionLevel, capacity, blockSize, pool, metrics) {
    init();
  }

  uint64_t ZlibCompressionStream::doStreamingCompression() {
    if (deflateReset(&strm) != Z_OK) {
      throw std::runtime_error("Failed to reset inflate.");
    }

    strm.avail_in = static_cast<unsigned int>(bufferSize);
    strm.next_in = rawInputBuffer.data();

    do {
      if (outputPosition >= outputSize) {
        if (!BufferedOutputStream::Next(reinterpret_cast<void**>(&outputBuffer), &outputSize)) {
          throw std::runtime_error("Failed to get next output buffer from output stream.");
        }
        outputPosition = 0;
      }
      strm.next_out = reinterpret_cast<unsigned char*>(outputBuffer + outputPosition);
      strm.avail_out = static_cast<unsigned int>(outputSize - outputPosition);

      int ret = deflate(&strm, Z_FINISH);
      outputPosition = outputSize - static_cast<int>(strm.avail_out);

      if (ret == Z_STREAM_END) {
        break;
      } else if (ret == Z_OK) {
        // needs more buffer so will continue the loop
      } else {
        throw std::runtime_error("Failed to deflate input data.");
      }
    } while (strm.avail_out == 0);

    return strm.total_out;
  }

  std::string ZlibCompressionStream::getName() const {
    return "ZlibCompressionStream";
  }

  DIAGNOSTIC_PUSH

#if defined(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wold-style-cast")
#endif

  void ZlibCompressionStream::init() {
    strm.zalloc = nullptr;
    strm.zfree = nullptr;
    strm.opaque = nullptr;
    strm.next_in = nullptr;

    if (deflateInit2(&strm, level, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
      throw std::runtime_error("Error while calling deflateInit2() for zlib.");
    }
  }

  void ZlibCompressionStream::end() {
    (void)deflateEnd(&strm);
  }

  DIAGNOSTIC_PUSH

  enum DecompressState {
    DECOMPRESS_HEADER,
    DECOMPRESS_START,
    DECOMPRESS_CONTINUE,
    DECOMPRESS_ORIGINAL,
    DECOMPRESS_EOF
  };

  std::string decompressStateToString(DecompressState state) {
    switch (state) {
      case DECOMPRESS_HEADER:
        return "DECOMPRESS_HEADER";
      case DECOMPRESS_START:
        return "DECOMPRESS_START";
      case DECOMPRESS_CONTINUE:
        return "DECOMPRESS_CONTINUE";
      case DECOMPRESS_ORIGINAL:
        return "DECOMPRESS_ORIGINAL";
      case DECOMPRESS_EOF:
        return "DECOMPRESS_EOF";
    }
    return "unknown";
  }

  class DecompressionStream : public SeekableInputStream {
   public:
    DecompressionStream(std::unique_ptr<SeekableInputStream> inStream, size_t bufferSize,
                        MemoryPool& pool, ReaderMetrics* metrics);
    virtual ~DecompressionStream() override {}
    virtual bool Next(const void** data, int* size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual int64_t ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override = 0;

   protected:
    virtual void NextDecompress(const void** data, int* size, size_t availableSize) = 0;

    std::string getStreamName() const;
    void readBuffer(bool failOnEof);
    uint32_t readByte(bool failOnEof);
    void readHeader();

    MemoryPool& pool;
    std::unique_ptr<SeekableInputStream> input;

    // uncompressed output
    DataBuffer<char> outputDataBuffer;

    // the current state
    DecompressState state;

    // The starting and current position of the buffer for the uncompressed
    // data. It either points to the data buffer or the underlying input stream.
    const char* outputBufferStart;
    const char* outputBuffer;
    size_t outputBufferLength;
    // The uncompressed buffer length. For compressed chunk, it's the original
    // (ie. the overall) and the actual length of the decompressed data.
    // For uncompressed chunk, it's the length of the loaded data of this chunk.
    size_t uncompressedBufferLength;

    // The remaining size of the current chunk that is not yet consumed
    // ie. decompressed or returned in output if state==DECOMPRESS_ORIGINAL
    size_t remainingLength;

    // the last buffer returned from the input
    const char* inputBufferStart;
    const char* inputBuffer;
    const char* inputBufferEnd;

    // Variables for saving the position of the header and the start of the
    // buffer. Used when we have to seek a position.
    size_t headerPosition;
    size_t inputBufferStartPosition;

    // roughly the number of bytes returned
    off_t bytesReturned;

    ReaderMetrics* metrics;
  };

  DecompressionStream::DecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                                           size_t bufferSize, MemoryPool& _pool,
                                           ReaderMetrics* _metrics)
      : pool(_pool),
        input(std::move(inStream)),
        outputDataBuffer(pool, bufferSize),
        state(DECOMPRESS_HEADER),
        outputBufferStart(nullptr),
        outputBuffer(nullptr),
        outputBufferLength(0),
        uncompressedBufferLength(0),
        remainingLength(0),
        inputBufferStart(nullptr),
        inputBuffer(nullptr),
        inputBufferEnd(nullptr),
        headerPosition(0),
        inputBufferStartPosition(0),
        bytesReturned(0),
        metrics(_metrics) {}

  std::string DecompressionStream::getStreamName() const {
    return input->getName();
  }

  void DecompressionStream::readBuffer(bool failOnEof) {
    SCOPED_MINUS_STOPWATCH(metrics, DecompressionLatencyUs);
    int length;
    if (!input->Next(reinterpret_cast<const void**>(&inputBuffer), &length)) {
      if (failOnEof) {
        throw ParseError("Read past EOF in DecompressionStream::readBuffer");
      }
      state = DECOMPRESS_EOF;
      inputBuffer = nullptr;
      inputBufferEnd = nullptr;
      inputBufferStart = nullptr;
    } else {
      inputBufferEnd = inputBuffer + length;
      inputBufferStartPosition = static_cast<size_t>(input->ByteCount() - length);
      inputBufferStart = inputBuffer;
    }
  }

  uint32_t DecompressionStream::readByte(bool failOnEof) {
    if (inputBuffer == inputBufferEnd) {
      readBuffer(failOnEof);
      if (state == DECOMPRESS_EOF) {
        return 0;
      }
    }
    return static_cast<unsigned char>(*(inputBuffer++));
  }

  void DecompressionStream::readHeader() {
    uint32_t header = readByte(false);
    if (state != DECOMPRESS_EOF) {
      header |= readByte(true) << 8;
      header |= readByte(true) << 16;
      if (header & 1) {
        state = DECOMPRESS_ORIGINAL;
      } else {
        state = DECOMPRESS_START;
      }
      remainingLength = header >> 1;
    } else {
      remainingLength = 0;
    }
  }

  bool DecompressionStream::Next(const void** data, int* size) {
    SCOPED_STOPWATCH(metrics, DecompressionLatencyUs, DecompressionCall);
    // If we are starting a new header, we will have to store its positions
    // after decompressing.
    bool saveBufferPositions = false;
    // If the user pushed back or seeked within the same chunk.
    if (outputBufferLength) {
      *data = outputBuffer;
      *size = static_cast<int>(outputBufferLength);
      outputBuffer += outputBufferLength;
      bytesReturned += static_cast<off_t>(outputBufferLength);
      outputBufferLength = 0;
      return true;
    }
    if (state == DECOMPRESS_HEADER || remainingLength == 0) {
      readHeader();
      // Here we already read the three bytes of the header.
      headerPosition =
          inputBufferStartPosition + static_cast<size_t>(inputBuffer - inputBufferStart) - 3;
      saveBufferPositions = true;
    }
    if (state == DECOMPRESS_EOF) {
      return false;
    }
    if (inputBuffer == inputBufferEnd) {
      readBuffer(true);
    }
    size_t availableSize =
        std::min(static_cast<size_t>(inputBufferEnd - inputBuffer), remainingLength);
    if (state == DECOMPRESS_ORIGINAL) {
      *data = inputBuffer;
      *size = static_cast<int>(availableSize);
      outputBuffer = inputBuffer + availableSize;
      outputBufferLength = 0;
      inputBuffer += availableSize;
      remainingLength -= availableSize;
    } else if (state == DECOMPRESS_START) {
      NextDecompress(data, size, availableSize);
    } else {
      throw std::logic_error(
          "Unknown compression state in "
          "DecompressionStream::Next");
    }
    bytesReturned += static_cast<off_t>(*size);
    if (saveBufferPositions) {
      uncompressedBufferLength = static_cast<size_t>(*size);
      outputBufferStart = reinterpret_cast<const char*>(*data);
    }
    return true;
  }

  void DecompressionStream::BackUp(int count) {
    if (outputBuffer == nullptr || outputBufferLength != 0) {
      throw std::logic_error("Backup without previous Next in " + getName());
    }
    outputBuffer -= static_cast<size_t>(count);
    outputBufferLength = static_cast<size_t>(count);
    bytesReturned -= count;
  }

  int64_t DecompressionStream::ByteCount() const {
    return bytesReturned;
  }

  bool DecompressionStream::Skip(int count) {
    bytesReturned += count;
    // this is a stupid implementation for now.
    // should skip entire blocks without decompressing
    while (count > 0) {
      const void* ptr;
      int len;
      if (!Next(&ptr, &len)) {
        return false;
      }
      if (len > count) {
        BackUp(len - count);
        count = 0;
      } else {
        count -= len;
      }
    }
    return true;
  }

  /** There are four possible scenarios when seeking a position:
   * 1. The chunk of the seeked position is the current chunk that has been read and
   *    decompressed. For uncompressed chunk, it could be partially read. So there are two
   *    sub-cases:
   *    a. The seeked position is inside the uncompressed buffer.
   *    b. The seeked position is outside the uncompressed buffer.
   * 2. The chunk of the seeked position is read from the input stream, but has not been
   *    decompressed yet, ie. it's not in the output stream.
   * 3. The chunk of the seeked position is not read yet from the input stream.
   */
  void DecompressionStream::seek(PositionProvider& position) {
    size_t seekedHeaderPosition = position.current();
    // Case 1: the seeked position is in the current chunk and it's buffered and
    // decompressed/uncompressed. Note that after the headerPosition comes the 3 bytes of
    // the header.
    if (headerPosition == seekedHeaderPosition && inputBufferStartPosition <= headerPosition + 3 &&
        inputBufferStart) {
      position.next();  // Skip the input level position, i.e. seekedHeaderPosition.
      size_t posInChunk = position.next();  // Chunk level position.
      // Case 1.a: The position is in the decompressed/uncompressed buffer. Here we only
      // need to set the output buffer's pointer to the seeked position.
      if (uncompressedBufferLength >= posInChunk) {
        outputBufferLength = uncompressedBufferLength - posInChunk;
        outputBuffer = outputBufferStart + posInChunk;
        return;
      }
      // Case 1.b: The position is outside the decompressed/uncompressed buffer.
      // Skip bytes to seek.
      if (!Skip(static_cast<int>(posInChunk - uncompressedBufferLength))) {
        std::ostringstream ss;
        ss << "Bad seek to (chunkHeader=" << seekedHeaderPosition << ", posInChunk=" << posInChunk
           << ") in " << getName() << ". DecompressionState: " << decompressStateToString(state);
        throw ParseError(ss.str());
      }
      return;
    }
    // Clear state to prepare reading from a new chunk header.
    state = DECOMPRESS_HEADER;
    outputBuffer = nullptr;
    outputBufferLength = 0;
    remainingLength = 0;
    if (seekedHeaderPosition < static_cast<uint64_t>(input->ByteCount()) &&
        seekedHeaderPosition >= inputBufferStartPosition) {
      // Case 2: The input is buffered, but not yet decompressed. No need to
      // force re-reading the inputBuffer, we just have to move it to the
      // seeked position.
      position.next();  // Skip the input level position.
      inputBuffer = inputBufferStart + (seekedHeaderPosition - inputBufferStartPosition);
    } else {
      // Case 3: The seeked position is not in the input buffer, here we are
      // forcing to read it.
      inputBuffer = nullptr;
      inputBufferEnd = nullptr;
      input->seek(position);  // Actually use the input level position.
    }
    bytesReturned = static_cast<off_t>(input->ByteCount());
    if (!Skip(static_cast<int>(position.next()))) {
      throw ParseError("Bad skip in " + getName());
    }
  }

  class ZlibDecompressionStream : public DecompressionStream {
   public:
    ZlibDecompressionStream(std::unique_ptr<SeekableInputStream> inStream, size_t blockSize,
                            MemoryPool& pool, ReaderMetrics* metrics);
    virtual ~ZlibDecompressionStream() override;
    virtual std::string getName() const override;

   protected:
    virtual void NextDecompress(const void** data, int* size, size_t availableSize) override;

   private:
    z_stream zstream;
  };

  DIAGNOSTIC_PUSH

#if defined(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wold-style-cast")
#endif

  ZlibDecompressionStream::ZlibDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                                                   size_t bufferSize, MemoryPool& _pool,
                                                   ReaderMetrics* _metrics)
      : DecompressionStream(std::move(inStream), bufferSize, _pool, _metrics) {
    zstream.next_in = nullptr;
    zstream.avail_in = 0;
    zstream.zalloc = nullptr;
    zstream.zfree = nullptr;
    zstream.opaque = nullptr;
    zstream.next_out = reinterpret_cast<Bytef*>(outputDataBuffer.data());
    zstream.avail_out = static_cast<uInt>(outputDataBuffer.capacity());
    int64_t result = inflateInit2(&zstream, -15);
    switch (result) {
      case Z_OK:
        break;
      case Z_MEM_ERROR:
        throw std::logic_error("Memory error from inflateInit2");
      case Z_VERSION_ERROR:
        throw std::logic_error("Version error from inflateInit2");
      case Z_STREAM_ERROR:
        throw std::logic_error("Stream error from inflateInit2");
      default:
        throw std::logic_error("Unknown error from inflateInit2");
    }
  }

  DIAGNOSTIC_POP

  ZlibDecompressionStream::~ZlibDecompressionStream() {
    int64_t result = inflateEnd(&zstream);
    if (result != Z_OK) {
      // really can't throw in destructors
      std::cout << "Error in ~ZlibDecompressionStream() " << result << "\n";
    }
  }

  void ZlibDecompressionStream::NextDecompress(const void** data, int* size, size_t availableSize) {
    zstream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(inputBuffer));
    zstream.avail_in = static_cast<uInt>(availableSize);
    outputBuffer = outputDataBuffer.data();
    zstream.next_out = reinterpret_cast<Bytef*>(const_cast<char*>(outputBuffer));
    zstream.avail_out = static_cast<uInt>(outputDataBuffer.capacity());
    if (inflateReset(&zstream) != Z_OK) {
      throw std::logic_error(
          "Bad inflateReset in "
          "ZlibDecompressionStream::NextDecompress");
    }
    int64_t result;
    do {
      result = inflate(&zstream, availableSize == remainingLength ? Z_FINISH : Z_SYNC_FLUSH);
      switch (result) {
        case Z_OK:
          remainingLength -= availableSize;
          inputBuffer += availableSize;
          readBuffer(true);
          availableSize =
              std::min(static_cast<size_t>(inputBufferEnd - inputBuffer), remainingLength);
          zstream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(inputBuffer));
          zstream.avail_in = static_cast<uInt>(availableSize);
          break;
        case Z_STREAM_END:
          break;
        case Z_BUF_ERROR:
          throw std::logic_error(
              "Buffer error in "
              "ZlibDecompressionStream::NextDecompress");
        case Z_DATA_ERROR:
          throw std::logic_error(
              "Data error in "
              "ZlibDecompressionStream::NextDecompress");
        case Z_STREAM_ERROR:
          throw std::logic_error(
              "Stream error in "
              "ZlibDecompressionStream::NextDecompress");
        default:
          throw std::logic_error(
              "Unknown error in "
              "ZlibDecompressionStream::NextDecompress");
      }
    } while (result != Z_STREAM_END);
    *size = static_cast<int>(outputDataBuffer.capacity() - zstream.avail_out);
    *data = outputBuffer;
    outputBufferLength = 0;
    outputBuffer += *size;
    inputBuffer += availableSize;
    remainingLength -= availableSize;
  }

  std::string ZlibDecompressionStream::getName() const {
    std::ostringstream result;
    result << "zlib(" << input->getName() << ")";
    return result.str();
  }

  class BlockDecompressionStream : public DecompressionStream {
   public:
    BlockDecompressionStream(std::unique_ptr<SeekableInputStream> inStream, size_t blockSize,
                             MemoryPool& pool, ReaderMetrics* metrics);

    virtual ~BlockDecompressionStream() override {}
    virtual std::string getName() const override = 0;

   protected:
    virtual void NextDecompress(const void** data, int* size, size_t availableSize) override;

    virtual uint64_t decompress(const char* input, uint64_t length, char* output,
                                size_t maxOutputLength) = 0;

   private:
    // may need to stitch together multiple input buffers;
    // to give snappy a contiguous block
    DataBuffer<char> inputDataBuffer;
  };

  BlockDecompressionStream::BlockDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                                                     size_t blockSize, MemoryPool& _pool,
                                                     ReaderMetrics* _metrics)
      : DecompressionStream(std::move(inStream), blockSize, _pool, _metrics),
        inputDataBuffer(pool, blockSize) {}

  void BlockDecompressionStream::NextDecompress(const void** data, int* size,
                                                size_t availableSize) {
    // Get contiguous bytes of compressed block.
    const char* compressed = inputBuffer;
    if (remainingLength == availableSize) {
      inputBuffer += availableSize;
    } else {
      // Did not read enough from input.
      if (inputDataBuffer.capacity() < remainingLength) {
        inputDataBuffer.resize(remainingLength);
      }
      ::memcpy(inputDataBuffer.data(), inputBuffer, availableSize);
      inputBuffer += availableSize;
      compressed = inputDataBuffer.data();

      for (size_t pos = availableSize; pos < remainingLength;) {
        readBuffer(true);
        size_t avail =
            std::min(static_cast<size_t>(inputBufferEnd - inputBuffer), remainingLength - pos);
        ::memcpy(inputDataBuffer.data() + pos, inputBuffer, avail);
        pos += avail;
        inputBuffer += avail;
      }
    }
    outputBufferLength = decompress(compressed, remainingLength, outputDataBuffer.data(),
                                    outputDataBuffer.capacity());
    remainingLength = 0;
    state = DECOMPRESS_HEADER;
    *data = outputDataBuffer.data();
    *size = static_cast<int>(outputBufferLength);
    outputBuffer = outputDataBuffer.data() + outputBufferLength;
    outputBufferLength = 0;
  }

// Only used on x86 or x86_64
#if defined(__x86_64__) || defined(_M_X64) || defined(i386) || defined(__i386__) || \
  defined(__i386) || defined(_M_IX86)
  class ZlibDecompressionStreamByLibDeflate : public BlockDecompressionStream {
   public:
    ZlibDecompressionStreamByLibDeflate(std::unique_ptr<SeekableInputStream> inStream, size_t blockSize, MemoryPool& _pool,
                                  ReaderMetrics* _metrics)
        : BlockDecompressionStream(std::move(inStream), blockSize, _pool, _metrics) {
      decompressor = libdeflate_alloc_decompressor();
      if (decompressor == nullptr) {
        throw std::runtime_error("libdeflate allocate decompressor failed");
      }
    }

    ~ZlibDecompressionStreamByLibDeflate() override { libdeflate_free_decompressor(decompressor); }

    std::string getName() const override {
      std::ostringstream result;
      result << "zlib(" << input->getName() << ")";
      return result.str();
    }

   protected:
    uint64_t decompress(const char* inputPtr, uint64_t length, char* output, size_t maxOutputLength) override {
      size_t actual = 0;
      auto res = libdeflate_deflate_decompress(decompressor, inputPtr, length, output, maxOutputLength, &actual);
      if (res != LIBDEFLATE_SUCCESS) {
        throw ParseError("libdeflate deflate decompress failed");
      }
      return actual;
    }

   private:
    libdeflate_decompressor* decompressor;
  };
#endif

  class SnappyDecompressionStream : public BlockDecompressionStream {
   public:
    SnappyDecompressionStream(std::unique_ptr<SeekableInputStream> inStream, size_t blockSize,
                              MemoryPool& _pool, ReaderMetrics* _metrics)
        : BlockDecompressionStream(std::move(inStream), blockSize, _pool, _metrics) {
      // PASS
    }

    std::string getName() const override {
      std::ostringstream result;
      result << "snappy(" << getStreamName() << ")";
      return result.str();
    }

   protected:
    virtual uint64_t decompress(const char* input, uint64_t length, char* output,
                                size_t maxOutputLength) override;
  };

  uint64_t SnappyDecompressionStream::decompress(const char* _input, uint64_t length, char* output,
                                                 size_t maxOutputLength) {
    size_t outLength;
    if (!snappy::GetUncompressedLength(_input, length, &outLength)) {
      throw ParseError("SnappyDecompressionStream choked on corrupt input");
    }

    if (outLength > maxOutputLength) {
      throw std::logic_error("Snappy length exceeds block size");
    }

    if (!snappy::RawUncompress(_input, length, output)) {
      throw ParseError("SnappyDecompressionStream choked on corrupt input");
    }
    return outLength;
  }

  class LzoDecompressionStream : public BlockDecompressionStream {
   public:
    LzoDecompressionStream(std::unique_ptr<SeekableInputStream> inStream, size_t blockSize,
                           MemoryPool& _pool, ReaderMetrics* _metrics)
        : BlockDecompressionStream(std::move(inStream), blockSize, _pool, _metrics) {
      // PASS
    }

    std::string getName() const override {
      std::ostringstream result;
      result << "lzo(" << getStreamName() << ")";
      return result.str();
    }

   protected:
    virtual uint64_t decompress(const char* input, uint64_t length, char* output,
                                size_t maxOutputLength) override;
  };

  uint64_t LzoDecompressionStream::decompress(const char* inputPtr, uint64_t length, char* output,
                                              size_t maxOutputLength) {
    return lzoDecompress(inputPtr, inputPtr + length, output, output + maxOutputLength);
  }

  class Lz4DecompressionStream : public BlockDecompressionStream {
   public:
    Lz4DecompressionStream(std::unique_ptr<SeekableInputStream> inStream, size_t blockSize,
                           MemoryPool& _pool, ReaderMetrics* _metrics)
        : BlockDecompressionStream(std::move(inStream), blockSize, _pool, _metrics) {
      // PASS
    }

    std::string getName() const override {
      std::ostringstream result;
      result << "lz4(" << getStreamName() << ")";
      return result.str();
    }

   protected:
    virtual uint64_t decompress(const char* input, uint64_t length, char* output,
                                size_t maxOutputLength) override;
  };

  uint64_t Lz4DecompressionStream::decompress(const char* inputPtr, uint64_t length, char* output,
                                              size_t maxOutputLength) {
    int result = LZ4_decompress_safe(inputPtr, output, static_cast<int>(length),
                                     static_cast<int>(maxOutputLength));
    if (result < 0) {
      throw ParseError(getName() + " - failed to decompress");
    }
    return static_cast<uint64_t>(result);
  }

  /**
   * Block compression base class
   */
  class BlockCompressionStream : public CompressionStreamBase {
   public:
    BlockCompressionStream(OutputStream* outStream, int compressionLevel, uint64_t capacity,
                           uint64_t blockSize, MemoryPool& pool, WriterMetrics* metrics)
        : CompressionStreamBase(outStream, compressionLevel, capacity, blockSize, pool, metrics),
          compressorBuffer(pool) {
      // PASS
    }

    virtual bool Next(void** data, int* size) override;
    virtual void suppress() override;
    virtual std::string getName() const override = 0;

   protected:
    // compresses a block and returns the compressed size
    virtual uint64_t doBlockCompression() = 0;

    // return maximum possible compression size for allocating space for
    // compressorBuffer below
    virtual uint64_t estimateMaxCompressionSize() = 0;

    // should allocate max possible compressed size
    DataBuffer<unsigned char> compressorBuffer;
  };

  bool BlockCompressionStream::Next(void** data, int* size) {
    if (bufferSize != 0) {
      ensureHeader();

      // perform compression
      size_t totalCompressedSize = doBlockCompression();

      const unsigned char* dataToWrite = nullptr;
      int totalSizeToWrite = 0;

      if (totalCompressedSize >= static_cast<size_t>(bufferSize)) {
        writeHeader(static_cast<size_t>(bufferSize), true);
        dataToWrite = rawInputBuffer.data();
        totalSizeToWrite = bufferSize;
      } else {
        writeHeader(totalCompressedSize, false);
        dataToWrite = compressorBuffer.data();
        totalSizeToWrite = static_cast<int>(totalCompressedSize);
      }

      writeData(dataToWrite, totalSizeToWrite);
    }

    *data = rawInputBuffer.data();
    *size = static_cast<int>(rawInputBuffer.size());
    bufferSize = *size;
    compressorBuffer.resize(estimateMaxCompressionSize());

    return true;
  }

  void BlockCompressionStream::suppress() {
    compressorBuffer.resize(0);
    CompressionStreamBase::suppress();
  }

  /**
   * LZ4 block compression
   */
  class Lz4CompressionSteam : public BlockCompressionStream {
   public:
    Lz4CompressionSteam(OutputStream* outStream, int compressionLevel, uint64_t capacity,
                        uint64_t blockSize, MemoryPool& pool, WriterMetrics* metrics)
        : BlockCompressionStream(outStream, compressionLevel, capacity, blockSize, pool, metrics) {
      this->init();
    }

    virtual std::string getName() const override {
      return "Lz4CompressionStream";
    }

    virtual ~Lz4CompressionSteam() override {
      this->end();
    }

   protected:
    virtual uint64_t doBlockCompression() override;

    virtual uint64_t estimateMaxCompressionSize() override {
      return static_cast<uint64_t>(LZ4_compressBound(bufferSize));
    }

   private:
    void init();
    void end();
    LZ4_stream_t* state;
  };

  uint64_t Lz4CompressionSteam::doBlockCompression() {
    int result = LZ4_compress_fast_extState(
        static_cast<void*>(state), reinterpret_cast<const char*>(rawInputBuffer.data()),
        reinterpret_cast<char*>(compressorBuffer.data()), bufferSize,
        static_cast<int>(compressorBuffer.size()), level);
    if (result == 0) {
      throw std::runtime_error("Error during block compression using lz4.");
    }
    return static_cast<uint64_t>(result);
  }

  void Lz4CompressionSteam::init() {
    state = LZ4_createStream();
    if (!state) {
      throw std::runtime_error("Error while allocating state for lz4.");
    }
  }

  void Lz4CompressionSteam::end() {
    (void)LZ4_freeStream(state);
    state = nullptr;
  }

  /**
   * Snappy block compression
   */
  class SnappyCompressionStream : public BlockCompressionStream {
   public:
    SnappyCompressionStream(OutputStream* outStream, int compressionLevel, uint64_t capacity,
                            uint64_t blockSize, MemoryPool& pool, WriterMetrics* metrics)
        : BlockCompressionStream(outStream, compressionLevel, capacity, blockSize, pool, metrics) {}

    virtual std::string getName() const override {
      return "SnappyCompressionStream";
    }

    virtual ~SnappyCompressionStream() override {
      // PASS
    }

   protected:
    virtual uint64_t doBlockCompression() override;

    virtual uint64_t estimateMaxCompressionSize() override {
      return static_cast<uint64_t>(snappy::MaxCompressedLength(static_cast<size_t>(bufferSize)));
    }
  };

  uint64_t SnappyCompressionStream::doBlockCompression() {
    size_t compressedLength;
    snappy::RawCompress(reinterpret_cast<const char*>(rawInputBuffer.data()),
                        static_cast<size_t>(bufferSize),
                        reinterpret_cast<char*>(compressorBuffer.data()), &compressedLength);
    return static_cast<uint64_t>(compressedLength);
  }

  /**
   * ZSTD block compression
   */
  class ZSTDCompressionStream : public BlockCompressionStream {
   public:
    ZSTDCompressionStream(OutputStream* outStream, int compressionLevel, uint64_t capacity,
                          uint64_t blockSize, MemoryPool& pool, WriterMetrics* metrics)
        : BlockCompressionStream(outStream, compressionLevel, capacity, blockSize, pool, metrics) {
      this->init();
    }

    virtual std::string getName() const override {
      return "ZstdCompressionStream";
    }

    virtual ~ZSTDCompressionStream() override {
      this->end();
    }

   protected:
    virtual uint64_t doBlockCompression() override;

    virtual uint64_t estimateMaxCompressionSize() override {
      return ZSTD_compressBound(static_cast<size_t>(bufferSize));
    }

   private:
    void init();
    void end();
    ZSTD_CCtx* cctx;
  };

  uint64_t ZSTDCompressionStream::doBlockCompression() {
    return ZSTD_compressCCtx(cctx, compressorBuffer.data(), compressorBuffer.size(),
                             rawInputBuffer.data(), static_cast<size_t>(bufferSize), level);
  }

  DIAGNOSTIC_PUSH

#if defined(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wold-style-cast")
#endif

  void ZSTDCompressionStream::init() {
    cctx = ZSTD_createCCtx();
    if (!cctx) {
      throw std::runtime_error("Error while calling ZSTD_createCCtx() for zstd.");
    }
  }

  void ZSTDCompressionStream::end() {
    (void)ZSTD_freeCCtx(cctx);
    cctx = nullptr;
  }

  DIAGNOSTIC_PUSH

  /**
   * ZSTD block decompression
   */
  class ZSTDDecompressionStream : public BlockDecompressionStream {
   public:
    ZSTDDecompressionStream(std::unique_ptr<SeekableInputStream> inStream, size_t blockSize,
                            MemoryPool& _pool, ReaderMetrics* _metrics)
        : BlockDecompressionStream(std::move(inStream), blockSize, _pool, _metrics) {
      this->init();
    }

    virtual ~ZSTDDecompressionStream() override {
      this->end();
    }

    std::string getName() const override {
      std::ostringstream result;
      result << "zstd(" << getStreamName() << ")";
      return result.str();
    }

   protected:
    virtual uint64_t decompress(const char* input, uint64_t length, char* output,
                                size_t maxOutputLength) override;

   private:
    void init();
    void end();
    ZSTD_DCtx* dctx;
  };

  uint64_t ZSTDDecompressionStream::decompress(const char* inputPtr, uint64_t length, char* output,
                                               size_t maxOutputLength) {
    return static_cast<uint64_t>(
        ZSTD_decompressDCtx(dctx, output, maxOutputLength, inputPtr, length));
  }

  DIAGNOSTIC_PUSH

#if defined(__GNUC__) || defined(__clang__)
  DIAGNOSTIC_IGNORE("-Wold-style-cast")
#endif

  void ZSTDDecompressionStream::init() {
    dctx = ZSTD_createDCtx();
    if (!dctx) {
      throw std::runtime_error("Error while calling ZSTD_createDCtx() for zstd.");
    }
  }

  void ZSTDDecompressionStream::end() {
    (void)ZSTD_freeDCtx(dctx);
    dctx = nullptr;
  }

  DIAGNOSTIC_PUSH

  std::unique_ptr<BufferedOutputStream> createCompressor(CompressionKind kind,
                                                         OutputStream* outStream,
                                                         CompressionStrategy strategy,
                                                         uint64_t bufferCapacity,
                                                         uint64_t compressionBlockSize,
                                                         MemoryPool& pool, WriterMetrics* metrics) {
    switch (static_cast<int64_t>(kind)) {
      case CompressionKind_NONE: {
        return std::make_unique<BufferedOutputStream>(pool, outStream, bufferCapacity,
                                                      compressionBlockSize, metrics);
      }
      case CompressionKind_ZLIB: {
        int level =
            (strategy == CompressionStrategy_SPEED) ? Z_BEST_SPEED + 1 : Z_DEFAULT_COMPRESSION;
        return std::make_unique<ZlibCompressionStream>(outStream, level, bufferCapacity,
                                                       compressionBlockSize, pool, metrics);
      }
      case CompressionKind_ZSTD: {
        int level = (strategy == CompressionStrategy_SPEED) ? 1 : ZSTD_CLEVEL_DEFAULT;
        return std::make_unique<ZSTDCompressionStream>(outStream, level, bufferCapacity,
                                                       compressionBlockSize, pool, metrics);
      }
      case CompressionKind_LZ4: {
        int level = (strategy == CompressionStrategy_SPEED) ? LZ4_ACCELERATION_MAX
                                                            : LZ4_ACCELERATION_DEFAULT;
        return std::make_unique<Lz4CompressionSteam>(outStream, level, bufferCapacity,
                                                     compressionBlockSize, pool, metrics);
      }
      case CompressionKind_SNAPPY: {
        int level = 0;
        return std::make_unique<SnappyCompressionStream>(outStream, level, bufferCapacity,
                                                         compressionBlockSize, pool, metrics);
      }
      case CompressionKind_LZO:
      default:
        throw NotImplementedYet("compression codec");
    }
  }

  std::unique_ptr<SeekableInputStream> createDecompressor(
      CompressionKind kind, std::unique_ptr<SeekableInputStream> input, uint64_t blockSize,
      MemoryPool& pool, ReaderMetrics* metrics) {
    switch (static_cast<int64_t>(kind)) {
      case CompressionKind_NONE:
        return input;
      case CompressionKind_ZLIB:
// Only used on x86 or x86_64
#if defined(__x86_64__) || defined(_M_X64) || defined(i386) || defined(__i386__) || \
  defined(__i386) || defined(_M_IX86)
        return std::make_unique<ZlibDecompressionStreamByLibDeflate>(std::move(input), blockSize, pool,
                                                               metrics);
#else
        return std::make_unique<ZlibDecompressionStream>(std::move(input), blockSize, pool,
                                                         metrics);
#endif
      case CompressionKind_SNAPPY:
        return std::make_unique<SnappyDecompressionStream>(std::move(input), blockSize, pool,
                                                           metrics);
      case CompressionKind_LZO:
        return std::make_unique<LzoDecompressionStream>(std::move(input), blockSize, pool, metrics);
      case CompressionKind_LZ4:
        return std::make_unique<Lz4DecompressionStream>(std::move(input), blockSize, pool, metrics);
      case CompressionKind_ZSTD:
        return std::make_unique<ZSTDDecompressionStream>(std::move(input), blockSize, pool,
                                                         metrics);
      default: {
        std::ostringstream buffer;
        buffer << "Unknown compression codec " << kind;
        throw NotImplementedYet(buffer.str());
      }
    }
  }

}  // namespace orc
