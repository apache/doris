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

#include "RLEv1.hh"
#include "RLEv2.hh"
#include "orc/Exceptions.hh"

namespace orc {

  RleEncoder::~RleEncoder() {
    // PASS
  }

  RleDecoder::~RleDecoder() {
    // PASS
  }

  std::unique_ptr<RleEncoder> createRleEncoder(std::unique_ptr<BufferedOutputStream> output,
                                               bool isSigned, RleVersion version, MemoryPool&,
                                               bool alignedBitpacking) {
    switch (static_cast<int64_t>(version)) {
      case RleVersion_1:
        return std::make_unique<RleEncoderV1>(std::move(output), isSigned);
      case RleVersion_2:
        return std::make_unique<RleEncoderV2>(std::move(output), isSigned, alignedBitpacking);
      default:
        throw NotImplementedYet("Not implemented yet");
    }
  }

  std::unique_ptr<RleDecoder> createRleDecoder(std::unique_ptr<SeekableInputStream> input,
                                               bool isSigned, RleVersion version, MemoryPool& pool,
                                               ReaderMetrics* metrics) {
    switch (static_cast<int64_t>(version)) {
      case RleVersion_1:
        return std::make_unique<RleDecoderV1>(std::move(input), isSigned, metrics);
      case RleVersion_2:
        return std::make_unique<RleDecoderV2>(std::move(input), isSigned, pool, metrics);
      default:
        throw NotImplementedYet("Not implemented yet");
    }
  }

  template <typename T>
  void RleEncoder::add(const T* data, uint64_t numValues, const char* notNull) {
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        write(static_cast<int64_t>(data[i]));
      }
    }
  }

  void RleEncoder::add(const int64_t* data, uint64_t numValues, const char* notNull) {
    add<int64_t>(data, numValues, notNull);
  }

  void RleEncoder::add(const int32_t* data, uint64_t numValues, const char* notNull) {
    add<int32_t>(data, numValues, notNull);
  }

  void RleEncoder::add(const int16_t* data, uint64_t numValues, const char* notNull) {
    add<int16_t>(data, numValues, notNull);
  }

  void RleEncoder::writeVslong(int64_t val) {
    writeVulong((val << 1) ^ (val >> 63));
  }

  void RleEncoder::writeVulong(int64_t val) {
    while (true) {
      if ((val & ~0x7f) == 0) {
        writeByte(static_cast<char>(val));
        return;
      } else {
        writeByte(static_cast<char>(0x80 | (val & 0x7f)));
        // cast val to unsigned so as to force 0-fill right shift
        val = (static_cast<uint64_t>(val) >> 7);
      }
    }
  }

  void RleEncoder::writeByte(char c) {
    if (bufferPosition == bufferLength) {
      int addedSize = 0;
      if (!outputStream->Next(reinterpret_cast<void**>(&buffer), &addedSize)) {
        throw std::bad_alloc();
      }
      bufferPosition = 0;
      bufferLength = static_cast<size_t>(addedSize);
    }
    buffer[bufferPosition++] = c;
  }

  void RleEncoder::recordPosition(PositionRecorder* recorder) const {
    uint64_t flushedSize = outputStream->getSize();
    uint64_t unflushedSize = static_cast<uint64_t>(bufferPosition);
    if (outputStream->isCompressed()) {
      recorder->add(flushedSize);
      recorder->add(unflushedSize);
    } else {
      flushedSize -= static_cast<uint64_t>(bufferLength);
      recorder->add(flushedSize + unflushedSize);
    }
    recorder->add(static_cast<uint64_t>(numLiterals));
  }

}  // namespace orc
