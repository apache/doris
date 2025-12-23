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

#ifndef ORC_OUTPUTSTREAM_HH
#define ORC_OUTPUTSTREAM_HH

#include "Adaptor.hh"
#include "BlockBuffer.hh"
#include "orc/OrcFile.hh"
#include "wrap/zero-copy-stream-wrapper.h"

namespace orc {

  /**
   * Record write position for creating index stream
   */
  class PositionRecorder {
   public:
    virtual ~PositionRecorder();
    virtual void add(uint64_t pos) = 0;
  };

  DIAGNOSTIC_PUSH

#ifdef __clang__
  DIAGNOSTIC_IGNORE("-Wunused-private-field")
#endif
  struct WriterMetrics;
  /**
   * A subclass of Google's ZeroCopyOutputStream that supports output to memory
   * buffer, and flushing to OutputStream.
   * By extending Google's class, we get the ability to pass it directly
   * to the protobuf writers.
   */
  class BufferedOutputStream : public google::protobuf::io::ZeroCopyOutputStream {
   private:
    OutputStream* outputStream;
    std::unique_ptr<BlockBuffer> dataBuffer;
    uint64_t blockSize;
    WriterMetrics* metrics;

   public:
    BufferedOutputStream(MemoryPool& pool, OutputStream* outStream, uint64_t capacity,
                         uint64_t block_size, WriterMetrics* metrics);
    virtual ~BufferedOutputStream() override;

    virtual bool Next(void** data, int* size) override;
    virtual void BackUp(int count) override;
    virtual google::protobuf::int64 ByteCount() const override;
    virtual bool WriteAliasedRaw(const void* data, int size) override;
    virtual bool AllowsAliasing() const override;

    virtual std::string getName() const;
    virtual uint64_t getSize() const;
    virtual uint64_t flush();
    virtual void suppress();

    virtual bool isCompressed() const {
      return false;
    }
  };
  DIAGNOSTIC_POP

  /**
   * An append only buffered stream that allows
   * buffer, and flushing to OutputStream.
   * By extending Google's class, we get the ability to pass it directly
   * to the protobuf writers.
   */
  class AppendOnlyBufferedStream {
   private:
    std::unique_ptr<BufferedOutputStream> outStream;
    char* buffer;
    int bufferOffset, bufferLength;

   public:
    AppendOnlyBufferedStream(std::unique_ptr<BufferedOutputStream> _outStream)
        : outStream(std::move(_outStream)) {
      buffer = nullptr;
      bufferOffset = bufferLength = 0;
    }

    void write(const char* data, size_t size);
    uint64_t getSize() const;
    uint64_t flush();

    void recordPosition(PositionRecorder* recorder) const;
  };
}  // namespace orc

#endif  // ORC_OUTPUTSTREAM_HH
