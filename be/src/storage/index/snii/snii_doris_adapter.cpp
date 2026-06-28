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
#include <mutex>

#include "common/cast_set.h"
#include "snii/format/per_index_meta.h"

namespace doris::segment_v2::snii_doris {

thread_local const io::IOContext* DorisSniiFileReader::_scoped_io_ctx = nullptr;

Status to_doris_status(const ::snii::Status& status) {
    if (status.ok()) {
        return Status::OK();
    }
    switch (status.code()) {
    case ::snii::StatusCode::kNotFound:
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>("SNII: {}",
                                                                       status.message());
    case ::snii::StatusCode::kUnsupported:
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>("SNII: {}", status.message());
    case ::snii::StatusCode::kInvalidArgument:
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("SNII: {}", status.message());
    case ::snii::StatusCode::kCorruption:
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>("SNII: {}",
                                                                       status.message());
    case ::snii::StatusCode::kIoError:
        return Status::IOError("SNII: {}", status.message());
    case ::snii::StatusCode::kInternal:
        return Status::InternalError("SNII: {}", status.message());
    case ::snii::StatusCode::kOk:
        break;
    }
    return Status::InternalError("SNII: {}", status.message());
}

::snii::Status to_snii_status(const Status& status) {
    if (status.ok()) {
        return ::snii::Status::OK();
    }
    return ::snii::Status::IoError(status.to_string_no_stack());
}

::snii::Status DorisSniiFileWriter::append(::snii::Slice data) {
    if (_writer == nullptr) {
        return ::snii::Status::InvalidArgument("doris writer is null");
    }
    return to_snii_status(
            _writer->append(Slice(reinterpret_cast<const char*>(data.data()), data.size())));
}

::snii::Status DorisSniiFileWriter::finalize() {
    if (_writer == nullptr) {
        return ::snii::Status::InvalidArgument("doris writer is null");
    }
    return ::snii::Status::OK();
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
    index_io_ctx.is_index_data = true;
    return index_io_ctx;
}

io::IOContext DorisSniiFileReader::_make_section_io_context(const io::IOContext* io_ctx,
                                                            uint8_t section_type) {
    io::IOContext section_io_ctx = _make_index_io_context(io_ctx);
    section_io_ctx.snii_section_type = section_type;
    section_io_ctx.is_index_data = section_type != io::SNII_SECTION_POSTING &&
                                   section_type != io::SNII_SECTION_NORMS &&
                                   section_type != io::SNII_SECTION_NULL_BITMAP;
    return section_io_ctx;
}

void DorisSniiFileReader::register_section_refs(const ::snii::format::SectionRefs& refs) {
    const auto add_range = [this](const ::snii::format::RegionRef& ref, uint8_t section_type) {
        if (ref.length == 0) {
            return;
        }
        const SectionRange range {
                .offset = ref.offset, .end = ref.offset + ref.length, .section_type = section_type};
        const auto duplicate = std::find_if(_section_ranges.begin(), _section_ranges.end(),
                                            [&range](const SectionRange& existing) {
                                                return existing.offset == range.offset &&
                                                       existing.end == range.end &&
                                                       existing.section_type == range.section_type;
                                            });
        if (duplicate == _section_ranges.end()) {
            _section_ranges.push_back(range);
        }
    };

    std::unique_lock lock(_section_ranges_mutex);
    add_range(refs.dict_region, io::SNII_SECTION_DICT);
    add_range(refs.posting_region, io::SNII_SECTION_POSTING);
    add_range(refs.bsbf, io::SNII_SECTION_BSBF);
    add_range(refs.norms, io::SNII_SECTION_NORMS);
    add_range(refs.null_bitmap, io::SNII_SECTION_NULL_BITMAP);
}

uint8_t DorisSniiFileReader::_classify_section(uint64_t offset, size_t len) const {
    if (len == 0) {
        return io::SNII_SECTION_UNKNOWN;
    }
    const uint64_t end = offset + len;
    uint64_t best_overlap = 0;
    uint8_t best_type = io::SNII_SECTION_UNKNOWN;
    std::shared_lock lock(_section_ranges_mutex);
    for (const auto& range : _section_ranges) {
        if (range.end <= offset || end <= range.offset) {
            continue;
        }
        const uint64_t overlap_begin = std::max(offset, range.offset);
        const uint64_t overlap_end = std::min(end, range.end);
        const uint64_t overlap = overlap_end - overlap_begin;
        if (overlap > best_overlap) {
            best_overlap = overlap;
            best_type = range.section_type;
        }
    }
    return best_type;
}

DorisSniiFileReader::ScopedIOContext::ScopedIOContext(const io::IOContext* io_ctx)
        : _previous(_scoped_io_ctx), _io_ctx(DorisSniiFileReader::_make_index_io_context(io_ctx)) {
    _scoped_io_ctx = &_io_ctx;
}

DorisSniiFileReader::ScopedIOContext::~ScopedIOContext() {
    _scoped_io_ctx = _previous;
}

::snii::Status DorisSniiFileReader::read_at(uint64_t offset, size_t len,
                                            std::vector<uint8_t>* const out) {
    SNII_RETURN_IF_ERROR(_check_read_range(offset, len));
    const auto* current_io_ctx = _current_io_ctx();
    uint8_t section_type = _classify_section(offset, len);
    if (section_type == io::SNII_SECTION_UNKNOWN) {
        section_type = current_io_ctx->snii_section_type;
    }
    const io::IOContext section_io_ctx = _make_section_io_context(current_io_ctx, section_type);
    SNII_RETURN_IF_ERROR(_read_at(offset, len, out, &section_io_ctx));
    if (len > 0) {
        _record_read_stats(cast_set<int64_t>(len), cast_set<int64_t>(len), 1, 1);
    }
    return ::snii::Status::OK();
}

::snii::Status DorisSniiFileReader::_read_at(uint64_t offset, size_t len,
                                             std::vector<uint8_t>* const out,
                                             const io::IOContext* io_ctx) const {
    if (_reader == nullptr) {
        return ::snii::Status::InvalidArgument("doris reader is null");
    }
    if (out == nullptr) {
        return ::snii::Status::InvalidArgument("output buffer is null");
    }
    SNII_RETURN_IF_ERROR(_check_read_range(offset, len));
    if (len == 0) {
        out->clear();
        return ::snii::Status::OK();
    }
    out->resize(len);
    size_t bytes_read = 0;
    auto status = _reader->read_at(offset, Slice(out->data(), len), &bytes_read, io_ctx);
    if (!status.ok()) {
        return to_snii_status(status);
    }
    if (bytes_read != len) {
        return ::snii::Status::IoError(
                fmt::format("short read at offset {}, expect {}, got {}", offset, len, bytes_read));
    }
    return ::snii::Status::OK();
}

::snii::Status DorisSniiFileReader::read_batch(const std::vector<::snii::io::Range>& ranges,
                                               std::vector<std::vector<uint8_t>>* const outs) {
    if (outs == nullptr) {
        return ::snii::Status::InvalidArgument("output buffers is null");
    }
    outs->clear();
    outs->resize(ranges.size());
    if (ranges.empty()) {
        return ::snii::Status::OK();
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
        SNII_RETURN_IF_ERROR(_check_read_range(ranges[i].offset, ranges[i].len));
        request_bytes += cast_set<int64_t>(ranges[i].len);
        if (ranges[i].len == 0) {
            continue;
        }
        sorted.push_back({ranges[i].offset, ranges[i].len, i});
    }
    if (sorted.empty()) {
        return ::snii::Status::OK();
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
        const auto* current_io_ctx = _current_io_ctx();
        uint8_t section_type = _classify_section(read_offset, read_len);
        if (section_type == io::SNII_SECTION_UNKNOWN) {
            section_type = current_io_ctx->snii_section_type;
        }
        const io::IOContext section_io_ctx = _make_section_io_context(current_io_ctx, section_type);
        SNII_RETURN_IF_ERROR(_read_at(read_offset, read_len, &bytes, &section_io_ctx));
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
    return ::snii::Status::OK();
}

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

::snii::Status DorisSniiFileReader::_check_read_range(uint64_t offset, size_t len) const {
    if (_reader == nullptr) {
        return ::snii::Status::InvalidArgument("doris reader is null");
    }
    if (offset > std::numeric_limits<uint64_t>::max() - len) {
        return ::snii::Status::Corruption(
                fmt::format("read range overflows: offset {}, len {}", offset, len));
    }
    const uint64_t end = offset + len;
    if (end > _reader->size()) {
        return ::snii::Status::Corruption(
                fmt::format("read range exceeds file size: offset {}, len {}, file size {}", offset,
                            len, _reader->size()));
    }
    return ::snii::Status::OK();
}

} // namespace doris::segment_v2::snii_doris
