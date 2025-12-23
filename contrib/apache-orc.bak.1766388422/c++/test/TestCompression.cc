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

#include "ColumnWriter.hh"
#include "Compression.hh"
#include "MemoryOutputStream.hh"
#include "RLEv1.hh"

#include "wrap/gtest-wrapper.h"
#include "wrap/orc-proto-wrapper.hh"

#include <algorithm>

namespace orc {
  const int DEFAULT_MEM_STREAM_SIZE = 1024 * 1024 * 2;  // 2M

  void generateRandomData(char* data, size_t size, bool letter) {
    for (size_t i = 0; i < size; ++i) {
      if (letter) {
        bool capitalized = std::rand() % 2 == 0;
        data[i] = capitalized ? static_cast<char>('A' + std::rand() % 26)
                              : static_cast<char>('a' + std::rand() % 26);
      } else {
        data[i] = static_cast<char>(std::rand() % 256);
      }
    }
  }

  void decompressAndVerify(const MemoryOutputStream& memStream, CompressionKind kind,
                           const char* data, size_t size, MemoryPool& pool) {
    auto inputStream =
        std::make_unique<SeekableArrayInputStream>(memStream.getData(), memStream.getLength());

    std::unique_ptr<SeekableInputStream> decompressStream =
        createDecompressor(kind, std::move(inputStream), 1024, pool, getDefaultReaderMetrics());

    const char* decompressedBuffer;
    int decompressedSize;
    int pos = 0;
    while (decompressStream->Next(reinterpret_cast<const void**>(&decompressedBuffer),
                                  &decompressedSize)) {
      for (int i = 0; i < decompressedSize; ++i) {
        EXPECT_LT(static_cast<size_t>(pos), size);
        EXPECT_EQ(data[pos], decompressedBuffer[i]);
        ++pos;
      }
    }
  }

  void compressAndVerify(CompressionKind kind, OutputStream* outStream,
                         CompressionStrategy strategy, uint64_t capacity, uint64_t block,
                         MemoryPool& pool, const char* data, size_t dataSize) {
    std::unique_ptr<BufferedOutputStream> compressStream =
        createCompressor(kind, outStream, strategy, capacity, block, pool, nullptr);

    size_t pos = 0;
    char* compressBuffer;
    int compressBufferSize = 0;
    while (dataSize > 0 &&
           compressStream->Next(reinterpret_cast<void**>(&compressBuffer), &compressBufferSize)) {
      size_t copy_size = std::min(static_cast<size_t>(compressBufferSize), dataSize);
      memcpy(compressBuffer, data + pos, copy_size);

      if (copy_size == dataSize) {
        compressStream->BackUp(compressBufferSize - static_cast<int>(dataSize));
      }

      pos += copy_size;
      dataSize -= copy_size;
    }

    EXPECT_EQ(0, dataSize);
    compressStream->flush();
  }

  void compress_original_string(orc::CompressionKind kind) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 1024;
    uint64_t block = 128;

    // simple, short string which will result in the original being saved
    char testData[] = "hello world!";
    compressAndVerify(kind, &memStream, CompressionStrategy_SPEED, capacity, block, *pool, testData,
                      sizeof(testData));
    decompressAndVerify(memStream, kind, testData, sizeof(testData), *pool);
  }

  TEST(TestCompression, zlib_compress_original_string) {
    compress_original_string(CompressionKind_ZLIB);
  }

  void compress_simple_repeated_string(orc::CompressionKind kind) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 1024;
    uint64_t block = 128;

    // simple repeated string (50 'a's) which should be compressed
    char testData[] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    compressAndVerify(kind, &memStream, CompressionStrategy_SPEED, capacity, block, *pool, testData,
                      sizeof(testData));
    decompressAndVerify(memStream, kind, testData, sizeof(testData), *pool);
  }

  TEST(TestCompression, compress_simple_repeated_string) {
    compress_simple_repeated_string(CompressionKind_ZLIB);
  }

  void compress_two_blocks(orc::CompressionKind kind) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 1024;
    uint64_t block = 128;

    // testData will be compressed in two blocks
    char testData[170];
    for (int i = 0; i < 170; ++i) {
      testData[i] = 'a';
    }
    compressAndVerify(kind, &memStream, CompressionStrategy_SPEED, capacity, block, *pool, testData,
                      170);
    decompressAndVerify(memStream, kind, testData, 170, *pool);
  }

  TEST(TestCompression, zlib_compress_two_blocks) {
    compress_two_blocks(CompressionKind_ZLIB);
  }

  void compress_random_letters(orc::CompressionKind kind) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 4096;
    uint64_t block = 1024;
    size_t dataSize = 1024 * 1024;  // 1M

    // testData will be compressed in two blocks
    char* testData = new char[dataSize];
    generateRandomData(testData, dataSize, true);
    compressAndVerify(kind, &memStream, CompressionStrategy_SPEED, capacity, block, *pool, testData,
                      dataSize);
    decompressAndVerify(memStream, kind, testData, dataSize, *pool);
    delete[] testData;
  }

  TEST(TestCompression, zlib_compress_random_letters) {
    compress_random_letters(CompressionKind_ZLIB);
  }

  void compress_random_bytes(orc::CompressionKind kind) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 4096;
    uint64_t block = 1024;
    size_t dataSize = 1024 * 1024;  // 1M

    // testData will be compressed in two blocks
    char* testData = new char[dataSize];
    generateRandomData(testData, dataSize, false);
    compressAndVerify(kind, &memStream, CompressionStrategy_SPEED, capacity, block, *pool, testData,
                      dataSize);
    decompressAndVerify(memStream, kind, testData, dataSize, *pool);
    delete[] testData;
  }

  TEST(TestCompression, zlib_compress_random_bytes) {
    compress_random_bytes(CompressionKind_ZLIB);
  }

  void protobuff_compression(orc::CompressionKind kind, proto::CompressionKind protoKind) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 4096;
    uint64_t block = 256;

    proto::PostScript ps;
    ps.set_footerlength(197934);
    ps.set_compression(protoKind);
    ps.set_metadatalength(100);
    ps.set_writerversion(789);
    ps.set_magic("protobuff_serialization");
    for (uint32_t i = 0; i < 1024; ++i) {
      ps.add_version(static_cast<uint32_t>(std::rand()));
    }

    std::unique_ptr<BufferedOutputStream> compressStream = createCompressor(
        kind, &memStream, CompressionStrategy_SPEED, capacity, block, *pool, nullptr);

    EXPECT_TRUE(ps.SerializeToZeroCopyStream(compressStream.get()));
    compressStream->flush();

    auto inputStream =
        std::make_unique<SeekableArrayInputStream>(memStream.getData(), memStream.getLength());

    std::unique_ptr<SeekableInputStream> decompressStream =
        createDecompressor(kind, std::move(inputStream), 1024, *pool, getDefaultReaderMetrics());

    proto::PostScript ps2;
    ps2.ParseFromZeroCopyStream(decompressStream.get());

    EXPECT_EQ(ps.footerlength(), ps2.footerlength());
    EXPECT_EQ(ps.compression(), ps2.compression());
    EXPECT_EQ(ps.metadatalength(), ps2.metadatalength());
    EXPECT_EQ(ps.writerversion(), ps2.writerversion());
    EXPECT_EQ(ps.magic(), ps2.magic());
    for (int i = 0; i < 1024; ++i) {
      EXPECT_EQ(ps.version(i), ps2.version(i));
    }
  }

  TEST(TestCompression, zlib_protobuff_compression) {
    protobuff_compression(CompressionKind_ZLIB, proto::ZLIB);
  }

  TEST(Compression, zstd_compress_original_string) {
    compress_original_string(CompressionKind_ZSTD);
  }

  TEST(Compression, zstd_compress_simple_repeated_string) {
    compress_simple_repeated_string(CompressionKind_ZSTD);
  }

  TEST(Compression, zstd_compress_two_blocks) {
    compress_two_blocks(CompressionKind_ZSTD);
  }

  TEST(Compression, zstd_compress_random_letters) {
    compress_random_letters(CompressionKind_ZSTD);
  }

  TEST(Compression, zstd_compress_random_bytes) {
    compress_random_bytes(CompressionKind_ZSTD);
  }

  TEST(Compression, zstd_protobuff_compression) {
    protobuff_compression(CompressionKind_ZSTD, proto::ZSTD);
  }

  TEST(Compression, lz4_compress_original_string) {
    compress_original_string(CompressionKind_LZ4);
  }

  TEST(Compression, lz4_compress_simple_repeated_string) {
    compress_simple_repeated_string(CompressionKind_LZ4);
  }

  TEST(Compression, lz4_compress_two_blocks) {
    compress_two_blocks(CompressionKind_LZ4);
  }

  TEST(Compression, lz4_compress_random_letters) {
    compress_random_letters(CompressionKind_LZ4);
  }

  TEST(Compression, lz4_compress_random_bytes) {
    compress_random_bytes(CompressionKind_LZ4);
  }

  TEST(Compression, lz4_protobuff_compression) {
    protobuff_compression(CompressionKind_LZ4, proto::LZ4);
  }

  TEST(Compression, snappy_compress_original_string) {
    compress_original_string(CompressionKind_SNAPPY);
  }

  TEST(Compression, snappy_compress_simple_repeated_string) {
    compress_simple_repeated_string(CompressionKind_SNAPPY);
  }

  TEST(Compression, snappy_compress_two_blocks) {
    compress_two_blocks(CompressionKind_SNAPPY);
  }

  TEST(Compression, snappy_compress_random_letters) {
    compress_random_letters(CompressionKind_SNAPPY);
  }

  TEST(Compression, snappy_compress_random_bytes) {
    compress_random_bytes(CompressionKind_SNAPPY);
  }

  TEST(Compression, snappy_protobuff_compression) {
    protobuff_compression(CompressionKind_SNAPPY, proto::SNAPPY);
  }

  void testSeekDecompressionStream(CompressionKind kind) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    CompressionStrategy strategy = CompressionStrategy_COMPRESSION;
    uint64_t batchSize = 1024, blockSize = 256;

    AppendOnlyBufferedStream outStream(createCompressor(
        kind, &memStream, strategy, DEFAULT_MEM_STREAM_SIZE, blockSize, *pool, nullptr));

    // write 3 batches of data and record positions between every batch
    size_t row = 0;
    proto::RowIndexEntry rowIndexEntry1, rowIndexEntry2;
    RowIndexPositionRecorder recorder1(rowIndexEntry1), recorder2(rowIndexEntry2);
    for (size_t repeat = 0; repeat != 3; ++repeat) {
      for (size_t i = 0; i != batchSize; ++i) {
        std::string data = to_string(static_cast<int64_t>(row++));
        outStream.write(data.c_str(), data.size());
      }
      if (repeat == 0) {
        outStream.recordPosition(&recorder1);
      } else if (repeat == 1) {
        outStream.recordPosition(&recorder2);
      }
    }
    outStream.flush();

    // try to decompress them
    auto inputStream =
        std::make_unique<SeekableArrayInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<SeekableInputStream> decompressStream = createDecompressor(
        kind, std::move(inputStream), blockSize, *pool, getDefaultReaderMetrics());

    // prepare positions to seek to
    EXPECT_EQ(rowIndexEntry1.positions_size(), rowIndexEntry2.positions_size());
    std::list<uint64_t> pos1, pos2;
    for (int i = 0; i < rowIndexEntry1.positions_size(); ++i) {
      pos1.push_back(rowIndexEntry1.positions(i));
      pos2.push_back(rowIndexEntry2.positions(i));
    }
    PositionProvider provider1(pos1), provider2(pos2);
    const void* data;
    int size;

    // seek to positions between first two batches
    decompressStream->seek(provider1);
    decompressStream->Next(&data, &size);
    std::string data1(static_cast<const char*>(data), 4);
    std::string expected1 = "1024";
    EXPECT_EQ(expected1, data1);

    // seek to positions between last two batches
    decompressStream->seek(provider2);
    decompressStream->Next(&data, &size);
    std::string data2(static_cast<const char*>(data), 4);
    std::string expected2 = "2048";
    EXPECT_EQ(expected2, data2);
  }

  TEST(Compression, seekDecompressionStream) {
    testSeekDecompressionStream(CompressionKind_ZSTD);
    testSeekDecompressionStream(CompressionKind_ZLIB);
    testSeekDecompressionStream(CompressionKind_LZ4);
    testSeekDecompressionStream(CompressionKind_SNAPPY);
  }
}  // namespace orc
