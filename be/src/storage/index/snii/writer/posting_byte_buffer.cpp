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

#include "storage/index/snii/writer/posting_byte_buffer.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <climits>
#include <cstring>
#include <limits>

#include "common/check.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/writer/temp_dir.h"

namespace doris::snii::writer {
namespace {

Status posting_io_error(const char* operation, const std::string& path) {
    return Status::Error<ErrorCode::IO_ERROR, false>("posting temporary {} '{}': {}", operation,
                                                     path, std::strerror(errno));
}

Status truncated_posting_bytes() {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
            "posting byte source is truncated");
}

} // namespace

PostingByteBuffer::PostingByteBuffer(MemoryReporter* reporter, size_t buffer_bytes)
        : reporter_(reporter),
          reservation_(reporter == nullptr ? MemoryReporter::Reservation()
                                           : reporter->make_postings_reservation()),
          path_reservation_(reporter == nullptr ? MemoryReporter::Reservation()
                                                : reporter->make_postings_reservation()),
          buffer_limit_(buffer_bytes) {
    DORIS_CHECK(buffer_bytes != 0);
}

PostingByteBuffer::~PostingByteBuffer() {
    release();
}

Status PostingByteBuffer::allocate_buffer(size_t required) {
    required = std::min(required, buffer_limit_);
    if (buffer_ != nullptr && required <= capacity_) {
        return Status::OK();
    }
    const size_t target = std::min(buffer_limit_, std::max({size_t {64}, required, capacity_ * 2}));
    MemoryReporter::Reservation replacement;
    if (reporter_ != nullptr) {
        RETURN_IF_ERROR(reservation_.prepare_replacement(target, &replacement));
    }
    auto buffer = std::make_unique<uint8_t[]>(target);
    if (buffered_ != 0) {
        std::memcpy(buffer.get(), buffer_.get(), buffered_);
    }
    buffer_ = std::move(buffer);
    capacity_ = target;
    if (reporter_ != nullptr) {
        reservation_ = std::move(replacement);
    }
    return Status::OK();
}

Status PostingByteBuffer::open_spill() {
    DCHECK_LT(fd_, 0);
    if (reporter_ != nullptr) {
        RETURN_IF_ERROR(path_reservation_.set_bytes(3 * PATH_MAX));
    }
    {
        const std::string directory = resolve_temp_dir();
        constexpr std::string_view suffix = "/snii_postings_XXXXXX";
        if (directory.size() + suffix.size() >= PATH_MAX) {
            return Status::Error<ErrorCode::IO_ERROR, false>("posting temporary path is too long");
        }
        path_.reserve(directory.size() + suffix.size());
        path_.assign(directory);
        path_.append(suffix);
    }
    if (reporter_ != nullptr) {
        RETURN_IF_ERROR(path_reservation_.set_bytes(path_.capacity() + 1));
    }
    fd_ = ::mkstemp(path_.data());
    if (fd_ < 0) {
        return posting_io_error("open", path_);
    }
    if (::fcntl(fd_, F_SETFD, FD_CLOEXEC) < 0) {
        return posting_io_error("set close-on-exec", path_);
    }
    return Status::OK();
}

Status PostingByteBuffer::write_all(std::span<const uint8_t> bytes) {
    while (!bytes.empty()) {
        const ssize_t written = ::write(fd_, bytes.data(), bytes.size());
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            return posting_io_error("write", path_);
        }
        if (reporter_ != nullptr) {
            reporter_->record_postings_io(0, static_cast<uint64_t>(written));
        }
        bytes = bytes.subspan(static_cast<size_t>(written));
    }
    return Status::OK();
}

Status PostingByteBuffer::flush() {
    if (fd_ >= 0 && buffered_ != 0) {
        RETURN_IF_ERROR(write_all({buffer_.get(), buffered_}));
        buffered_ = 0;
    }
    return Status::OK();
}

Status PostingByteBuffer::spill_and_release_buffer() {
    if (fd_ < 0) {
        RETURN_IF_ERROR(open_spill());
    }
    RETURN_IF_ERROR(flush());
    buffer_.reset();
    capacity_ = 0;
    reservation_.reset();
    return Status::OK();
}

Status PostingByteBuffer::append(std::span<const uint8_t> bytes) {
    if (bytes.empty()) {
        return Status::OK();
    }
    if (bytes.size() > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) - size_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting temporary exceeds the file offset range");
    }
    if (buffer_ == nullptr) {
        RETURN_IF_ERROR(allocate_buffer(bytes.size()));
    }
    while (!bytes.empty()) {
        if (buffered_ == capacity_) {
            if (fd_ < 0 && capacity_ < buffer_limit_) {
                Status growth = allocate_buffer(capacity_ + bytes.size());
                if (growth.ok()) {
                    continue;
                }
                if (!growth.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
                    return growth;
                }
            }
            if (fd_ < 0) {
                RETURN_IF_ERROR(open_spill());
            }
            RETURN_IF_ERROR(flush());
        }
        const size_t count = std::min(bytes.size(), capacity_ - buffered_);
        std::memcpy(buffer_.get() + buffered_, bytes.data(), count);
        buffered_ += count;
        size_ += count;
        bytes = bytes.subspan(count);
    }
    return Status::OK();
}

Status PostingByteBuffer::append_u32(std::span<const uint32_t> values) {
    static_assert(std::endian::native == std::endian::little);
    return append({reinterpret_cast<const uint8_t*>(values.data()), values.size_bytes()});
}

Status PostingByteBuffer::append_varint(uint64_t value) {
    std::array<uint8_t, 10> bytes;
    size_t count = 0;
    while (value >= 0x80) {
        bytes[count++] = static_cast<uint8_t>(value) | 0x80;
        value >>= 7;
    }
    bytes[count++] = static_cast<uint8_t>(value);
    return append({bytes.data(), count});
}

Status PostingByteBuffer::read_at(uint64_t offset, std::span<uint8_t> destination) {
    if (offset > size_ || destination.size() > size_ - offset) {
        return truncated_posting_bytes();
    }
    if (fd_ < 0) {
        if (!destination.empty()) {
            std::memcpy(destination.data(), buffer_.get() + offset, destination.size());
        }
        return Status::OK();
    }
    RETURN_IF_ERROR(flush());
    while (!destination.empty()) {
        const ssize_t count =
                ::pread(fd_, destination.data(), destination.size(), static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            return posting_io_error("read", path_);
        }
        if (count == 0) {
            return truncated_posting_bytes();
        }
        if (reporter_ != nullptr) {
            reporter_->record_postings_io(static_cast<uint64_t>(count), 0);
        }
        destination = destination.subspan(static_cast<size_t>(count));
        offset += static_cast<uint64_t>(count);
    }
    return Status::OK();
}

Status PostingByteBuffer::copy_to(PostingByteBuffer* destination) {
    DORIS_CHECK(destination != this);
    if (size_ > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) - destination->size_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "posting temporary exceeds the file offset range");
    }
    PostingByteCursor cursor(this);
    RETURN_IF_ERROR(cursor.reset());
    while (cursor.remaining() != 0) {
        std::span<const uint8_t> bytes;
        RETURN_IF_ERROR(cursor.next_span(&bytes));
        RETURN_IF_ERROR(destination->append(bytes));
    }
    return Status::OK();
}

Status PostingByteBuffer::stream_into(io::FileWriter* destination) {
    PostingByteCursor cursor(this);
    RETURN_IF_ERROR(cursor.reset());
    while (cursor.remaining() != 0) {
        std::span<const uint8_t> bytes;
        RETURN_IF_ERROR(cursor.next_span(&bytes));
        RETURN_IF_ERROR(destination->append(Slice(bytes.data(), bytes.size())));
    }
    return Status::OK();
}

Status PostingByteBuffer::checksum(uint32_t* crc) {
    *crc = 0;
    PostingByteCursor cursor(this);
    RETURN_IF_ERROR(cursor.reset());
    while (cursor.remaining() != 0) {
        std::span<const uint8_t> bytes;
        RETURN_IF_ERROR(cursor.next_span(&bytes));
        *crc = crc32c_extend(*crc, Slice(bytes.data(), bytes.size()));
    }
    return Status::OK();
}

Status PostingByteCursor::next_span(std::span<const uint8_t>* bytes, uint64_t maximum_bytes) {
    if (remaining() == 0) {
        *bytes = {};
        return Status::OK();
    }
    if (position_ == available_) {
        RETURN_IF_ERROR(refill());
    }
    const auto count = static_cast<size_t>(
            std::min({remaining(), static_cast<uint64_t>(available_ - position_), maximum_bytes}));
    *bytes = {cache_.get() + position_, count};
    position_ += count;
    offset_ += count;
    return Status::OK();
}

std::span<const uint8_t> PostingByteBuffer::resident_bytes() const {
    DORIS_CHECK(fd_ < 0);
    return {buffer_.get(), buffered_};
}

void PostingByteBuffer::close_and_remove() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    if (!path_.empty()) {
        std::remove(path_.c_str());
    }
    std::string().swap(path_);
    path_reservation_.reset();
}

Status PostingByteBuffer::clear_reuse() {
    if (fd_ >= 0) {
        const int closed = ::close(fd_);
        fd_ = -1;
        if (closed != 0) {
            return posting_io_error("close", path_);
        }
    }
    close_and_remove();
    buffered_ = 0;
    size_ = 0;
    return Status::OK();
}

void PostingByteBuffer::release() {
    close_and_remove();
    buffer_.reset();
    capacity_ = 0;
    buffered_ = 0;
    size_ = 0;
    reservation_.reset();
}

PostingByteCursor::PostingByteCursor(PostingByteBuffer* source)
        : source_(source),
          reservation_(source->reporter() == nullptr
                               ? MemoryReporter::Reservation()
                               : source->reporter()->make_postings_reservation()) {}

Status PostingByteCursor::reset(uint64_t offset, uint64_t length) {
    if (offset > source_->size()) {
        return truncated_posting_bytes();
    }
    const uint64_t count = length == UINT64_MAX ? source_->size() - offset : length;
    if (count > source_->size() - offset) {
        return truncated_posting_bytes();
    }
    offset_ = offset;
    end_ = offset + count;
    position_ = 0;
    available_ = 0;
    return Status::OK();
}

Status PostingByteCursor::refill() {
    if (remaining() == 0) {
        return truncated_posting_bytes();
    }
    if (cache_ == nullptr) {
        capacity_ = static_cast<size_t>(
                std::min<uint64_t>(remaining(), PostingByteBuffer::kDefaultBufferBytes));
        if (source_->reporter() != nullptr) {
            RETURN_IF_ERROR(reservation_.set_bytes(capacity_));
        }
        cache_ = std::make_unique<uint8_t[]>(capacity_);
    }
    available_ = static_cast<size_t>(std::min<uint64_t>(remaining(), capacity_));
    position_ = 0;
    return source_->read_at(offset_, {cache_.get(), available_});
}

Status PostingByteCursor::read(std::span<uint8_t> destination) {
    if (destination.size() > remaining()) {
        return truncated_posting_bytes();
    }
    while (!destination.empty()) {
        if (position_ == available_) {
            RETURN_IF_ERROR(refill());
        }
        const size_t count = std::min(destination.size(), available_ - position_);
        std::memcpy(destination.data(), cache_.get() + position_, count);
        destination = destination.subspan(count);
        position_ += count;
        offset_ += count;
    }
    return Status::OK();
}

Status PostingByteCursor::read_byte(uint8_t* value) {
    if (position_ == available_) {
        RETURN_IF_ERROR(refill());
    }
    *value = cache_[position_++];
    ++offset_;
    return Status::OK();
}

Status PostingByteCursor::read_varint(uint64_t* value) {
    *value = 0;
    for (unsigned shift = 0; shift < 64; shift += 7) {
        uint8_t byte = 0;
        RETURN_IF_ERROR(read_byte(&byte));
        if (shift == 63 && byte > 1) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "posting varint overflows uint64");
        }
        *value |= static_cast<uint64_t>(byte & 0x7f) << shift;
        if ((byte & 0x80) == 0) {
            return Status::OK();
        }
    }
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
            "posting varint is unterminated");
}

Status PostingByteCursor::read_u32(uint32_t* value) {
    static_assert(std::endian::native == std::endian::little);
    return read({reinterpret_cast<uint8_t*>(value), sizeof(*value)});
}

} // namespace doris::snii::writer
