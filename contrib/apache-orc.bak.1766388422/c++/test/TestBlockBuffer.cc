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

#include "BlockBuffer.hh"
#include "MemoryOutputStream.hh"
#include "orc/OrcFile.hh"
#include "wrap/gtest-wrapper.h"

namespace orc {
  const int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;  // 10M

  TEST(TestBlockBuffer, size_and_capacity) {
    MemoryPool* pool = getDefaultPool();
    BlockBuffer buffer(*pool, 1024);

    // block buffer will preallocate one block during initialization
    EXPECT_EQ(buffer.getBlockNumber(), 0);
    EXPECT_EQ(buffer.size(), 0);
    EXPECT_EQ(buffer.capacity(), 1024);

    buffer.reserve(128 * 1024);
    EXPECT_EQ(buffer.getBlockNumber(), 0);
    EXPECT_EQ(buffer.size(), 0);
    EXPECT_EQ(buffer.capacity(), 128 * 1024);

    // new size < old capacity
    buffer.resize(64 * 1024);
    EXPECT_EQ(buffer.getBlockNumber(), 64);
    EXPECT_EQ(buffer.size(), 64 * 1024);
    EXPECT_EQ(buffer.capacity(), 128 * 1024);

    // new size > old capacity
    buffer.resize(256 * 1024);
    EXPECT_EQ(buffer.getBlockNumber(), 256);
    EXPECT_EQ(buffer.size(), 256 * 1024);
    EXPECT_EQ(buffer.capacity(), 256 * 1024);
  }

  TEST(TestBlockBuffer, get_block) {
    MemoryPool* pool = getDefaultPool();
    BlockBuffer buffer(*pool, 1024);

    EXPECT_EQ(buffer.getBlockNumber(), 0);
    for (uint64_t i = 0; i < 10; ++i) {
      BlockBuffer::Block block = buffer.getNextBlock();
      EXPECT_EQ(buffer.getBlockNumber(), i + 1);
      for (uint64_t j = 0; j < block.size; ++j) {
        if (i % 2 == 0) {
          block.data[j] = static_cast<char>('A' + (i + j) % 26);
        } else {
          block.data[j] = static_cast<char>('a' + (i + j) % 26);
        }
      }
    }

    // verify the block data
    for (uint64_t i = 0; i < buffer.getBlockNumber(); ++i) {
      BlockBuffer::Block block = buffer.getBlock(i);
      for (uint64_t j = 0; j < block.size; ++j) {
        if (i % 2 == 0) {
          EXPECT_EQ(block.data[j], 'A' + (i + j) % 26);
        } else {
          EXPECT_EQ(block.data[j], 'a' + (i + j) % 26);
        }
      }
    }
  }

  void writeToOutputStream(uint64_t blockSize) {
    MemoryOutputStream outputStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    BlockBuffer buffer(*pool, blockSize);
    uint64_t totalBufferSize = 10240;
    while (buffer.size() < totalBufferSize) {
      BlockBuffer::Block block = buffer.getNextBlock();
      uint64_t blockNumber = buffer.getBlockNumber();
      for (uint64_t j = 0; j < block.size; ++j) {
        if (blockNumber % 2 == 0) {
          block.data[j] = static_cast<char>('A' + (blockNumber + j) % 26);
        } else {
          block.data[j] = static_cast<char>('a' + (blockNumber + j) % 26);
        }
      }
    }
    buffer.resize(totalBufferSize);
    // flush data buffer into output stream
    buffer.writeTo(&outputStream, nullptr);
    // verify data buffer
    uint64_t dataIndex = 0;
    for (uint64_t i = 0; i < buffer.getBlockNumber(); ++i) {
      BlockBuffer::Block block = buffer.getBlock(i);
      for (uint64_t j = 0; j < block.size; ++j) {
        EXPECT_EQ(outputStream.getData()[dataIndex++], block.data[j]);
      }
    }
  }

  TEST(TestBlockBuffer, write_to) {
    // test block size < natural write size
    writeToOutputStream(1024);
    // test block size = natural write size
    writeToOutputStream(2048);
    // test block size > natural write size
    writeToOutputStream(4096);
  }
}  // namespace orc
