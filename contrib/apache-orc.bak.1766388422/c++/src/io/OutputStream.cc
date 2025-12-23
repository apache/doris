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

#include "OutputStream.hh"
#include "Utils.hh"
#include "orc/Exceptions.hh"

#include <sstream>

namespace orc {

  PositionRecorder::~PositionRecorder() {
    // PASS
  }

  BufferedOutputStream::BufferedOutputStream(MemoryPool& pool, OutputStream* outStream,
                                             uint64_t capacity_, uint64_t blockSize_,
                                             WriterMetrics* metrics_)
      : outputStream(outStream), blockSize(blockSize_), metrics(metrics_) {
    dataBuffer.reset(new BlockBuffer(pool, blockSize));
    dataBuffer->reserve(capacity_);
  }

  BufferedOutputStream::~BufferedOutputStream() {
    // PASS
  }

  bool BufferedOutputStream::Next(void** buffer, int* size) {
    auto block = dataBuffer->getNextBlock();
    if (block.data == nullptr) {
      throw std::logic_error("Failed to get next buffer from block buffer.");
    }
    *buffer = block.data;
    *size = static_cast<int>(block.size);
    return true;
  }

  void BufferedOutputStream::BackUp(int count) {
    if (count >= 0) {
      uint64_t unsignedCount = static_cast<uint64_t>(count);
      if (unsignedCount <= dataBuffer->size()) {
        dataBuffer->resize(dataBuffer->size() - unsignedCount);
      } else {
        throw std::logic_error("Can't backup that much!");
      }
    }
  }

  google::protobuf::int64 BufferedOutputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(dataBuffer->size());
  }

  bool BufferedOutputStream::WriteAliasedRaw(const void*, int) {
    throw NotImplementedYet("WriteAliasedRaw is not supported.");
  }

  bool BufferedOutputStream::AllowsAliasing() const {
    return false;
  }

  std::string BufferedOutputStream::getName() const {
    std::ostringstream result;
    result << "BufferedOutputStream " << dataBuffer->size() << " of " << dataBuffer->capacity();
    return result.str();
  }

  uint64_t BufferedOutputStream::getSize() const {
    return dataBuffer->size();
  }

  uint64_t BufferedOutputStream::flush() {
    uint64_t dataSize = dataBuffer->size();
    // flush data buffer into outputStream
    if (dataSize > 0) {
      SCOPED_STOPWATCH(metrics, IOBlockingLatencyUs, nullptr);
      dataBuffer->writeTo(outputStream, metrics);
    }
    dataBuffer->resize(0);
    return dataSize;
  }

  void BufferedOutputStream::suppress() {
    dataBuffer->resize(0);
  }

  void AppendOnlyBufferedStream::write(const char* data, size_t size) {
    size_t dataOffset = 0;
    while (size > 0) {
      if (bufferOffset == bufferLength) {
        if (!outStream->Next(reinterpret_cast<void**>(&buffer), &bufferLength)) {
          throw std::logic_error("Failed to allocate buffer.");
        }
        bufferOffset = 0;
      }
      size_t len = std::min(static_cast<size_t>(bufferLength - bufferOffset), size);
      memcpy(buffer + bufferOffset, data + dataOffset, len);
      bufferOffset += static_cast<int>(len);
      dataOffset += len;
      size -= len;
    }
  }

  uint64_t AppendOnlyBufferedStream::getSize() const {
    return outStream->getSize();
  }

  uint64_t AppendOnlyBufferedStream::flush() {
    outStream->BackUp(bufferLength - bufferOffset);
    bufferOffset = bufferLength = 0;
    buffer = nullptr;
    return outStream->flush();
  }

  void AppendOnlyBufferedStream::recordPosition(PositionRecorder* recorder) const {
    uint64_t flushedSize = outStream->getSize();
    uint64_t unflushedSize = static_cast<uint64_t>(bufferOffset);
    if (outStream->isCompressed()) {
      // start of the compression chunk in the stream
      recorder->add(flushedSize);
      // number of decompressed bytes that need to be consumed
      recorder->add(unflushedSize);
    } else {
      flushedSize -= static_cast<uint64_t>(bufferLength);
      // byte offset of the start location
      recorder->add(flushedSize + unflushedSize);
    }
  }

}  // namespace orc
