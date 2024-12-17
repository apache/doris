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

#include "vec/exec/format/parquet/parquet_column_chunk_file_reader.h"

#include <cstring>
#include <stdexcept>

namespace doris {
namespace vectorized {

const Slice ParquetColumnChunkFileReader::EMPTY_SLICE;

ParquetColumnChunkFileReader::ParquetColumnChunkFileReader(
        std::vector<std::shared_ptr<ChunkReader>> chunks, ChunkReader::Statistics& statistics)
        : chunks_(std::move(chunks)),
          statistics_(statistics),
          current_chunk_index_(-1),
          last_read_offset_(0) {
    if (chunks_.empty()) {
        throw std::invalid_argument("At least one chunk is expected but got none");
    }
    current_slice_ = std::make_shared<Slice>();
}

ParquetColumnChunkFileReader::~ParquetColumnChunkFileReader() {
    static_cast<void>(close());
}

ParquetColumnChunkFileReader::ParquetColumnChunkFileReader(
        ParquetColumnChunkFileReader&& other) noexcept
        : chunks_(std::move(other.chunks_)),
          statistics_(other.statistics_),
          current_chunk_index_(other.current_chunk_index_),
          current_slice_(std::move(other.current_slice_)),
          current_position_(other.current_position_),
          last_read_offset_(other.last_read_offset_) {
    other.current_slice_ = nullptr;
    other.current_position_ = 0;
    other.current_chunk_index_ = -1;
    other.last_read_offset_ = 0;
}

ParquetColumnChunkFileReader& ParquetColumnChunkFileReader::operator=(
        ParquetColumnChunkFileReader&& other) noexcept {
    if (this != &other) {
        static_cast<void>(close());
        chunks_ = std::move(other.chunks_);
        current_chunk_index_ = other.current_chunk_index_;
        current_slice_ = std::move(other.current_slice_);
        current_position_ = other.current_position_;
        last_read_offset_ = other.last_read_offset_;

        other.current_slice_ = nullptr;
        other.current_position_ = 0;
        other.current_chunk_index_ = -1;
        other.last_read_offset_ = 0;
    }
    return *this;
}

Status ParquetColumnChunkFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                                  const io::IOContext* io_ctx) {
    if (result.size == 0) {
        *bytes_read = 0;
        return Status::OK();
    }

    RETURN_IF_ERROR(ensureOpen());

    // 检查 offset 是否满足顺序读取的要求
    if (offset < last_read_offset_) {
        return Status::InternalError(fmt::format(
                "Invalid offset: {}. Must be greater than or equal to last read offset: {}", offset,
                last_read_offset_));
    }

    // 二分查找确定 offset 在哪个 chunk
    size_t left = current_chunk_index_ >= 0 ? current_chunk_index_
                                            : 0; // 从当前 chunk 开始查找，因为 offset 是递增的
    size_t right = chunks_.size();
    size_t target_chunk = right; // 初始化为 right，如果找不到合适的 chunk，就是这个值

    while (left < right) {
        size_t mid = left + (right - left) / 2;
        auto chunk = chunks_[mid];
        if (!chunk) {
            return Status::InternalError("Invalid chunk type");
        }

        size_t chunk_start = chunk->getDiskOffset();
        size_t chunk_size = chunk->size();
        size_t chunk_end = chunk_start + chunk_size;

        if (offset >= chunk_start && offset < chunk_end) {
            target_chunk = mid;
            break;
        } else if (offset < chunk_start) {
            right = mid;
            target_chunk = right; // 更新 target_chunk，因为 offset 可能在这个范围内
        } else {
            left = mid + 1;
        }
    }

    // 如果没找到合适的 chunk，说明 offset 不在任何 chunk 的范围内
    if (target_chunk >= chunks_.size()) {
        *bytes_read = 0;
        return Status::OK();
    }

    // 如果需要跳过一些 chunks，释放掉它们
    bool need_create_new_chunk_reader =
            (current_chunk_index_ != static_cast<int64_t>(target_chunk));
    while (current_chunk_index_ < static_cast<int64_t>(target_chunk)) {
        // 释放掉要跳过的 chunks
        if (current_chunk_index_ >= 0) {
            chunks_[current_chunk_index_]->free();
            chunks_[current_chunk_index_].reset();
        }
        current_chunk_index_++;
    }

    // 确保 current_chunk_index_ 是有效的
    if (current_chunk_index_ < 0 || current_chunk_index_ >= static_cast<int64_t>(chunks_.size())) {
        return Status::InternalError(fmt::format("Invalid chunk index: {}", current_chunk_index_));
    }

    // 只有当需要读取新的 chunk 时才重新创建和读取
    if (need_create_new_chunk_reader) {
        if (current_slice_) {
            current_slice_.reset();
        }
        RETURN_IF_ERROR(chunks_[current_chunk_index_]->read(io_ctx, &current_slice_));
    }

    // 验证 offset 是否在当前 chunk 范围内
    size_t chunk_start = chunks_[current_chunk_index_]->getDiskOffset();
    size_t chunk_end = chunk_start + current_slice_->size;

    if (offset < chunk_start || offset >= chunk_end) {
        return Status::InternalError(
                fmt::format("Invalid offset: {}. Current chunk file range: [{}, {})", offset,
                            chunk_start, chunk_end));
    }

    // 设置读取位置
    current_position_ = offset - chunk_start;

    // 读取数据
    size_t bytes_to_read = std::min(result.size, current_slice_->size - current_position_);
    memcpy(result.data, current_slice_->data + current_position_, bytes_to_read);
    current_position_ += bytes_to_read;
    *bytes_read = bytes_to_read;
    last_read_offset_ = offset; // 更新最后读取的偏移量

    return Status::OK();
}

Status ParquetColumnChunkFileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        // 释放所有的 chunks
        if (current_chunk_index_ >= 0) {
            for (int i = current_chunk_index_; i < chunks_.size(); ++i) {
                if (chunks_[i]) {
                    chunks_[i]->free();
                    chunks_[i].reset();
                }
            }
        }

        if (current_slice_) {
            current_slice_.reset();
        }
    }
    return Status::OK();
}

Status ParquetColumnChunkFileReader::addChunkReader(std::shared_ptr<ChunkReader> reader) {
    if (!reader) {
        return Status::InvalidArgument("ChunkReader cannot be null");
    }
    chunks_.push_back(std::move(reader));
    return Status::OK();
}

Status ParquetColumnChunkFileReader::getSlice(const io::IOContext* io_ctx, size_t length,
                                              Slice* result) {
    //    if (length == 0) {
    //        *result = EMPTY_SLICE;
    //        return Status::OK();
    //    }
    //
    //    RETURN_IF_ERROR(ensureOpen());
    //
    //    while (!current_slice_ || current_slice_->size == current_position_) {
    //        if (current_chunk_index_ + 1 >= static_cast<int64_t>(chunks_.size())) {
    //            return Status::InvalidArgument(
    //                fmt::format("Requested {} bytes but 0 was available", length));
    //        }
    //        RETURN_IF_ERROR(readNextChunk(io_ctx));
    //    }
    //
    //    if (current_slice_->size - current_position_ >= length) {
    //        // We can satisfy the request from the current slice
    //        result->data = current_slice_->data + current_position_;
    //        result->size = length;
    //        current_position_ += length;
    //        return Status::OK();
    //    }
    //
    //    // Need to combine data from multiple chunks
    //    auto buffer = std::make_unique<uint8_t[]>(length);
    //    size_t total_read = 0;
    //    size_t remaining = length;
    //
    //    while (remaining > 0) {
    //        size_t bytes_read;
    //        Slice temp_result(buffer.get() + total_read, remaining);
    //        RETURN_IF_ERROR(read_at_impl(current_position_, temp_result, &bytes_read, nullptr));
    //
    //        if (bytes_read == 0) {
    //            return Status::InvalidArgument(
    //                fmt::format("Failed to read {} bytes", length));
    //        }
    //
    //        total_read += bytes_read;
    //        remaining -= bytes_read;
    //    }
    //
    //    result->data = (char *)buffer.release();
    //    result->size = length;
    return Status::OK();
}

Status ParquetColumnChunkFileReader::ensureOpen() {
    if (!current_slice_) {
        return Status::InternalError("Stream closed");
    }
    return Status::OK();
}

//Status ParquetColumnChunkFileReader::readNextChunk(const io::IOContext* io_ctx) {
//    if (current_chunk_index_ + 1 >= static_cast<int64_t>(chunks_.size())) {
//        return Status::InternalError("No more chunks to read");
//    }
//
//    // 释放当前 chunk
//    if (current_chunk_reader_) {
//        current_chunk_reader_->free();
//        current_chunk_reader_.reset();
//    }
//
//    // 读取新的 chunk
//    current_chunk_index_++;
//    current_chunk_reader_ = std::move(chunks_[current_chunk_index_]);
//    RETURN_IF_ERROR(current_chunk_reader_->read(io_ctx, &current_slice_));
//
//    return Status::OK();
//}

} // namespace vectorized
} // namespace doris
