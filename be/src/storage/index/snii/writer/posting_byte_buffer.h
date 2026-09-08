// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>

#include "common/status.h"
#include "storage/index/snii/writer/memory_reporter.h"

namespace doris::snii::io {
class FileWriter;
}

namespace doris::snii::writer {

// Replayable posting bytes with one fixed-size resident buffer. After its first
// spill the same allocation becomes the write buffer; neither append nor replay
// materializes the complete file. All buffers sharing a MemoryReporter compete
// for that reporter's postings budget, including the final encoder's scratch.
class PostingByteBuffer {
public:
    static constexpr size_t kDefaultBufferBytes = 64 * 1024;

    explicit PostingByteBuffer(MemoryReporter* reporter = nullptr,
                               size_t buffer_bytes = kDefaultBufferBytes);
    ~PostingByteBuffer();
    PostingByteBuffer(const PostingByteBuffer&) = delete;
    PostingByteBuffer& operator=(const PostingByteBuffer&) = delete;

    Status append(std::span<const uint8_t> bytes);
    Status append_u32(std::span<const uint32_t> values);
    Status append_varint(uint64_t value);
    Status read_at(uint64_t offset, std::span<uint8_t> destination);
    Status copy_to(PostingByteBuffer* destination);
    Status stream_into(io::FileWriter* destination);
    Status checksum(uint32_t* crc);
    // Freeze a retained inline payload without keeping one cache per entry.
    // Subsequent reads use their own bounded cursor; append remains supported.
    Status spill_and_release_buffer();
    Status flush();
    // Ends the previous logical stream, removing its temp file, while keeping
    // the fixed allocation available to the next term/window.
    Status clear_reuse();
    void release();

    uint64_t size() const { return size_; }
    size_t capacity() const { return capacity_; }
    bool spilled() const { return fd_ >= 0; }
    std::span<const uint8_t> resident_bytes() const;
    MemoryReporter* reporter() const { return reporter_; }

private:
    Status allocate_buffer(size_t required = 1);
    Status open_spill();
    Status write_all(std::span<const uint8_t> bytes);
    void close_and_remove();

    MemoryReporter* reporter_;
    MemoryReporter::Reservation reservation_;
    MemoryReporter::Reservation path_reservation_;
    std::unique_ptr<uint8_t[]> buffer_;
    const size_t buffer_limit_;
    size_t capacity_ = 0;
    size_t buffered_ = 0;
    uint64_t size_ = 0;
    int fd_ = -1;
    std::string path_;
};

// A bounded forward view used by posting decoders and encoder counting/replay
// passes. Its cache is charged to the same postings budget as its source.
class PostingByteCursor {
public:
    explicit PostingByteCursor(PostingByteBuffer* source);
    Status reset(uint64_t offset = 0, uint64_t length = UINT64_MAX);
    Status read_byte(uint8_t* value);
    Status read_varint(uint64_t* value);
    Status read_u32(uint32_t* value);
    Status read(std::span<uint8_t> destination);
    // Consumes a borrowed cache span; invalidated by the next cursor operation.
    Status next_span(std::span<const uint8_t>* bytes, uint64_t maximum_bytes = UINT64_MAX);
    uint64_t remaining() const { return end_ - offset_; }

private:
    Status refill();
    PostingByteBuffer* source_;
    MemoryReporter::Reservation reservation_;
    std::unique_ptr<uint8_t[]> cache_;
    uint64_t offset_ = 0;
    uint64_t end_ = 0;
    size_t position_ = 0;
    size_t available_ = 0;
    size_t capacity_ = 0;
};

} // namespace doris::snii::writer
