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

#include <iterator>
#include <memory>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/path.h"
#include "util/slice.h"
#include "vec/exec/format/parquet/reference_counted_reader.h"

namespace doris {
namespace vectorized {

/**
 * A FileReader implementation for reading parquet column chunks.
 * It reads column chunks in limited (small) byte chunks (8MB by default).
 * Column chunks consists of multiple pages.
 * This abstraction is used because the page size is unknown until the page header is read
 * and page header and page data can be split between two or more byte chunks.
 */
class ParquetColumnChunkFileReader : public io::FileReader {
public:
    ParquetColumnChunkFileReader(std::vector<std::shared_ptr<ChunkReader>> chunks,
                                 ChunkReader::Statistics& statistics);
    ~ParquetColumnChunkFileReader() override;

    // Prevent copying
    ParquetColumnChunkFileReader(const ParquetColumnChunkFileReader&) = delete;
    ParquetColumnChunkFileReader& operator=(const ParquetColumnChunkFileReader&) = delete;

    // Allow moving
    ParquetColumnChunkFileReader(ParquetColumnChunkFileReader&&) noexcept;
    ParquetColumnChunkFileReader& operator=(ParquetColumnChunkFileReader&&) noexcept;

    // FileReader interface implementation
    Status close() override;
    //    const Path& path() const override { return path_; }
    size_t size() const override { return chunks_.back()->file_size(); }
    //    bool closed() const override { return current_slice_ == nullptr; }

    // Additional functionality
    Status addChunkReader(std::shared_ptr<ChunkReader> reader);
    Status getSlice(const io::IOContext* io_ctx, size_t length, Slice* result);
    size_t available() const {
        return current_slice_ ? current_slice_->size - current_position_ : 0;
    }

    //    Status close() override {
    //        if (!_closed) {
    //            _closed = true;
    //        }
    //        return Status::OK();
    //    }

    const io::Path& path() const override { return chunks_.back()->path(); }

    bool closed() const override { return _closed.load(std::memory_order_acquire); }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override;

    void _collect_profile_before_close() override {
        //        if (_profile != nullptr) {
        //            COUNTER_UPDATE(_copy_time, _statistics.copy_time);
        //            COUNTER_UPDATE(_read_time, _statistics.read_time);
        //            COUNTER_UPDATE(_request_io, _statistics.request_io);
        //            COUNTER_UPDATE(_merged_io, _statistics.merged_io);
        //            COUNTER_UPDATE(_request_bytes, _statistics.request_bytes);
        //            COUNTER_UPDATE(_merged_bytes, _statistics.merged_bytes);
        //            COUNTER_UPDATE(_apply_bytes, _statistics.apply_bytes);
        //            if (_reader != nullptr) {
        //                _reader->collect_profile_before_close();
        //            }
        //        }
    }

private:
    Status ensureOpen();
    //    Status readNextChunk(const io::IOContext* io_ctx);

private:
    std::vector<std::shared_ptr<ChunkReader>> chunks_;
    ChunkReader::Statistics& statistics_;
    int64_t current_chunk_index_ = -1; // 当前正在读取的 chunk 的索引，-1 表示还没有读取任何 chunk
                                       //    std::shared_ptr<ChunkReader> current_chunk_reader_;
    std::shared_ptr<Slice> current_slice_;
    size_t current_position_ = 0;
    int64_t last_read_offset_ = -1; // 记录上一次读取的文件偏移量，用于确保顺序读取
    std::atomic<bool> _closed = false;

    static const Slice EMPTY_SLICE;
};

} // namespace vectorized
} // namespace doris
