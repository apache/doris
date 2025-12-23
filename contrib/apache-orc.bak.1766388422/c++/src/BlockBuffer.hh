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

#ifndef ORC_BLOCK_BUFFER_HH
#define ORC_BLOCK_BUFFER_HH

#include "orc/MemoryPool.hh"

#include <vector>

namespace orc {

  class OutputStream;
  struct WriterMetrics;
  /**
   * BlockBuffer implements a memory allocation policy based on
   * equal-length blocks. BlockBuffer will reserve multiple blocks
   * for allocation.
   */
  class BlockBuffer {
   private:
    MemoryPool& memoryPool;
    // current buffer size
    uint64_t currentSize;
    // maximal capacity (actual allocated memory)
    uint64_t currentCapacity;
    // unit for buffer expansion
    const uint64_t blockSize;
    // pointers to the start of each block
    std::vector<char*> blocks;

    // non-copy-constructible
    BlockBuffer(BlockBuffer& buffer) = delete;
    BlockBuffer& operator=(BlockBuffer& buffer) = delete;
    BlockBuffer(BlockBuffer&& buffer) = delete;
    BlockBuffer& operator=(BlockBuffer&& buffer) = delete;

   public:
    BlockBuffer(MemoryPool& pool, uint64_t blockSize);

    ~BlockBuffer();

    /**
     * Block points to a section of memory allocated by BlockBuffer,
     * containing the corresponding physical memory address and available size.
     */
    struct Block {
      // the start of block
      char* data;
      // number of bytes available at data
      uint64_t size;

      Block() : data(nullptr), size(0) {}
      Block(char* _data, uint64_t _size) : data(_data), size(_size) {}
      Block(const Block& block) = default;
      ~Block() = default;
    };

    /**
     * Get the allocated block object.
     * The last allocated block size may be less than blockSize,
     * and the rest of the blocks are all of size blockSize.
     * @param blockIndex the index of blocks
     * @return the allocated block object
     */
    Block getBlock(uint64_t blockIndex) const;

    /**
     * Get a empty block or allocate a new block to write.
     * If the last allocated block size is less than blockSize,
     * the size of empty block is equal to blockSize minus the size of
     * the last allocated block size. Otherwise, the size of
     * the empty block is equal to blockSize.
     * @return a empty block object
     */
    Block getNextBlock();

    /**
     * Get the number of blocks that are fully or partially occupied
     */
    uint64_t getBlockNumber() const {
      return (currentSize + blockSize - 1) / blockSize;
    }

    uint64_t size() const {
      return currentSize;
    }

    uint64_t capacity() const {
      return currentCapacity;
    }

    void resize(uint64_t size);
    /**
     * Requests the BlockBuffer to contain at least newCapacity bytes.
     * Reallocation happens if there is need of more space.
     * @param newCapacity new capacity of BlockBuffer
     */
    void reserve(uint64_t newCapacity);
    /**
     * Write the BlockBuffer content into OutputStream
     * @param output the output stream to write to
     * @param metrics the metrics of the writer
     */
    void writeTo(OutputStream* output, WriterMetrics* metrics);
  };
}  // namespace orc

#endif
