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
#include "orc/OrcFile.hh"
#include "orc/Writer.hh"

#include <algorithm>

namespace orc {

  BlockBuffer::BlockBuffer(MemoryPool& pool, uint64_t _blockSize)
      : memoryPool(pool), currentSize(0), currentCapacity(0), blockSize(_blockSize) {
    if (blockSize == 0) {
      throw std::logic_error("Block size cannot be zero");
    }
    reserve(blockSize);
  }

  BlockBuffer::~BlockBuffer() {
    for (size_t i = 0; i < blocks.size(); ++i) {
      memoryPool.free(blocks[i]);
    }
    blocks.clear();
    currentSize = currentCapacity = 0;
  }

  BlockBuffer::Block BlockBuffer::getBlock(uint64_t blockIndex) const {
    if (blockIndex >= getBlockNumber()) {
      throw std::out_of_range("Block index out of range");
    }
    return Block(blocks[blockIndex], std::min(currentSize - blockIndex * blockSize, blockSize));
  }

  BlockBuffer::Block BlockBuffer::getNextBlock() {
    if (currentSize < currentCapacity) {
      Block emptyBlock(blocks[currentSize / blockSize] + currentSize % blockSize,
                       blockSize - currentSize % blockSize);
      currentSize = (currentSize / blockSize + 1) * blockSize;
      return emptyBlock;
    } else {
      resize(currentSize + blockSize);
      return Block(blocks.back(), blockSize);
    }
  }

  void BlockBuffer::resize(uint64_t size) {
    reserve(size);
    if (currentCapacity >= size) {
      currentSize = size;
    } else {
      throw std::logic_error("Block buffer resize error");
    }
  }

  void BlockBuffer::reserve(uint64_t newCapacity) {
    while (currentCapacity < newCapacity) {
      char* newBlockPtr = memoryPool.malloc(blockSize);
      if (newBlockPtr != nullptr) {
        blocks.push_back(newBlockPtr);
        currentCapacity += blockSize;
      } else {
        break;
      }
    }
  }

  void BlockBuffer::writeTo(OutputStream* output, WriterMetrics* metrics) {
    if (currentSize == 0) {
      return;
    }
    static uint64_t MAX_CHUNK_SIZE = 1024 * 1024 * 1024;
    uint64_t chunkSize = std::min(output->getNaturalWriteSize(), MAX_CHUNK_SIZE);
    if (chunkSize == 0) {
      throw std::logic_error("Natural write size cannot be zero");
    }
    uint64_t ioCount = 0;
    uint64_t blockNumber = getBlockNumber();
    // if only exists one block, currentSize is equal to first block size
    if (blockNumber == 1 && currentSize <= chunkSize) {
      Block block = getBlock(0);
      output->write(block.data, block.size);
      ++ioCount;
    } else {
      char* chunk = memoryPool.malloc(chunkSize);
      uint64_t chunkOffset = 0;
      for (uint64_t i = 0; i < blockNumber; ++i) {
        Block block = getBlock(i);
        uint64_t blockOffset = 0;
        while (blockOffset < block.size) {
          // copy current block into chunk
          uint64_t copySize = std::min(chunkSize - chunkOffset, block.size - blockOffset);
          memcpy(chunk + chunkOffset, block.data + blockOffset, copySize);
          chunkOffset += copySize;
          blockOffset += copySize;

          // chunk is full
          if (chunkOffset >= chunkSize) {
            output->write(chunk, chunkSize);
            chunkOffset = 0;
            ++ioCount;
          }
        }
      }
      if (chunkOffset != 0) {
        output->write(chunk, chunkOffset);
        ++ioCount;
      }
      memoryPool.free(chunk);
    }

    if (metrics != nullptr) {
      metrics->IOCount.fetch_add(ioCount);
    }
  }
}  // namespace orc
