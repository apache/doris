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

#include "storage/index/snii/snii_doris_adapter.h"

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>
#include <limits>

#include "common/cast_set.h"

namespace doris::segment_v2::snii_doris {

thread_local const io::IOContext* DorisSniiFileReader::_scoped_io_ctx = nullptr;

doris::Status DorisSniiFileWriter::append(::snii::Slice data) {
    if (_writer == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "doris writer is null");
    }
    return _writer->append(Slice(reinterpret_cast<const char*>(data.data()), data.size()));
}

doris::Status DorisSniiFileWriter::finalize() {
    if (_writer == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "doris writer is null");
    }
    return doris::Status::OK();
}

uint64_t DorisSniiFileWriter::bytes_written() const {
    return _writer == nullptr ? 0 : _writer->bytes_appended();
}

DorisSniiFileReader::DorisSniiFileReader(io::FileReaderSPtr reader, const io::IOContext* io_ctx)
        : _reader(std::move(reader)), _default_io_ctx(_make_index_io_context(io_ctx)) {}

io::IOContext DorisSniiFileReader::_make_index_io_context(const io::IOContext* io_ctx) {
    io::IOContext index_io_ctx;
    if (io_ctx != nullptr) {
        index_io_ctx = *io_ctx;
    }
    index_io_ctx.is_inverted_index = true;
    // is_index_data is inherited from io_ctx: META scopes set it true at the source
    // (index_file_reader), non-meta reads default to false.
    return index_io_ctx;
}

DorisSniiFileReader::ScopedIOContext::ScopedIOContext(const io::IOContext* io_ctx)
        : _previous(_scoped_io_ctx), _io_ctx(DorisSniiFileReader::_make_index_io_context(io_ctx)) {
    _scoped_io_ctx = &_io_ctx;
}

DorisSniiFileReader::ScopedIOContext::~ScopedIOContext() {
    _scoped_io_ctx = _previous;
}

doris::Status DorisSniiFileReader::read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) {
    RETURN_IF_ERROR(_check_read_range(offset, len));
    RETURN_IF_ERROR(_read_at(offset, len, out, _current_io_ctx()));
    if (len > 0) {
        _record_read_stats(cast_set<int64_t>(len), cast_set<int64_t>(len), 1, 1);
    }
    return doris::Status::OK();
}

// NOLINTNEXTLINE(readability-non-const-parameter): out is the SNII read output buffer.
doris::Status DorisSniiFileReader::_read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out,
                                            const io::IOContext* io_ctx) const {
    if (_reader == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "doris reader is null");
    }
    if (out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "output buffer is null");
    }
    RETURN_IF_ERROR(_check_read_range(offset, len));
    if (len == 0) {
        out->clear();
        return doris::Status::OK();
    }
    out->resize(len);
    size_t bytes_read = 0;
    auto status = _reader->read_at(offset, Slice(out->data(), len), &bytes_read, io_ctx);
    if (!status.ok()) {
        return status;
    }
    if (bytes_read != len) {
        return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>(
                fmt::format("short read at offset {}, expect {}, got {}", offset, len, bytes_read));
    }
    return doris::Status::OK();
}

// NOLINTBEGIN(readability-non-const-parameter): outs is the SNII batch read output buffer.
doris::Status DorisSniiFileReader::read_batch(const std::vector<::snii::io::Range>& ranges,
                                              std::vector<std::vector<uint8_t>>* outs) {
    if (outs == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "output buffers is null");
    }
    outs->clear();
    outs->resize(ranges.size());
    if (ranges.empty()) {
        return doris::Status::OK();
    }

    struct IndexedRange {
        uint64_t offset = 0;
        size_t len = 0;
        size_t index = 0;
    };
    int64_t request_bytes = 0;
    std::vector<IndexedRange> sorted;
    sorted.reserve(ranges.size());
    for (size_t i = 0; i < ranges.size(); ++i) {
        RETURN_IF_ERROR(_check_read_range(ranges[i].offset, ranges[i].len));
        request_bytes += cast_set<int64_t>(ranges[i].len);
        if (ranges[i].len == 0) {
            continue;
        }
        sorted.push_back({ranges[i].offset, ranges[i].len, i});
    }
    if (sorted.empty()) {
        return doris::Status::OK();
    }
    std::sort(sorted.begin(), sorted.end(), [](const IndexedRange& lhs, const IndexedRange& rhs) {
        return lhs.offset < rhs.offset;
    });

    constexpr uint64_t max_coalesced_gap = 4096;
    constexpr uint64_t max_coalesced_read = 1ULL << 20;
    int64_t read_bytes = 0;
    int64_t range_read_count = 0;
    for (size_t begin = 0; begin < sorted.size();) {
        uint64_t read_offset = sorted[begin].offset;
        uint64_t read_end = sorted[begin].offset + sorted[begin].len;
        size_t end = begin + 1;
        while (end < sorted.size()) {
            const uint64_t next_end = sorted[end].offset + sorted[end].len;
            if ((sorted[end].offset > read_end &&
                 sorted[end].offset - read_end > max_coalesced_gap) ||
                next_end - read_offset > max_coalesced_read) {
                break;
            }
            read_end = std::max(read_end, next_end);
            ++end;
        }

        std::vector<uint8_t> bytes;
        const size_t read_len = cast_set<size_t>(read_end - read_offset);
        RETURN_IF_ERROR(_read_at(read_offset, read_len, &bytes, _current_io_ctx()));
        read_bytes += cast_set<int64_t>(read_len);
        ++range_read_count;
        for (size_t i = begin; i < end; ++i) {
            const uint64_t pos = sorted[i].offset - read_offset;
            auto& out = (*outs)[sorted[i].index];
            out.assign(bytes.begin() + cast_set<ptrdiff_t>(pos),
                       bytes.begin() + cast_set<ptrdiff_t>(pos + sorted[i].len));
        }
        begin = end;
    }
    _record_read_stats(request_bytes, read_bytes, range_read_count, range_read_count);
    return doris::Status::OK();
}
// NOLINTEND(readability-non-const-parameter)

uint64_t DorisSniiFileReader::size() const {
    return _reader == nullptr ? 0 : _reader->size();
}

const io::IOContext* DorisSniiFileReader::_current_io_ctx() const {
    return _scoped_io_ctx != nullptr ? _scoped_io_ctx : &_default_io_ctx;
}

void DorisSniiFileReader::_record_read_stats(int64_t request_bytes, int64_t read_bytes,
                                             int64_t range_read_count,
                                             int64_t serial_read_rounds) const {
    const auto* io_ctx = _current_io_ctx();
    if (io_ctx->file_cache_stats == nullptr) {
        return;
    }
    auto* stats = io_ctx->file_cache_stats;
    stats->inverted_index_request_bytes += request_bytes;
    stats->inverted_index_read_bytes += read_bytes;
    stats->inverted_index_range_read_count += range_read_count;
    stats->inverted_index_serial_read_rounds += serial_read_rounds;
}

doris::Status DorisSniiFileReader::_check_read_range(uint64_t offset, size_t len) const {
    if (_reader == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "doris reader is null");
    }
    if (offset > std::numeric_limits<uint64_t>::max() - len) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                fmt::format("read range overflows: offset {}, len {}", offset, len));
    }
    const uint64_t end = offset + len;
    if (end > _reader->size()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                fmt::format("read range exceeds file size: offset {}, len {}, file size {}", offset,
                            len, _reader->size()));
    }
    return doris::Status::OK();
}

} // namespace doris::segment_v2::snii_doris
