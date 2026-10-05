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

#include "format_v2/orc/orc_file_input_stream.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include "common/status.h"
#include "core/custom_allocator.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/tracing_file_reader.h"
#include "io/io_common.h"
#include "orc/Exceptions.hh"
#include "runtime/file_scan_profile.h"
#include "runtime/runtime_profile.h"
#include "util/slice.h"

namespace doris::format::orc {
namespace {

struct OrcMergedRangeStatistics {
    int64_t copy_time = 0;
    int64_t read_time = 0;
    int64_t request_io = 0;
    int64_t merged_io = 0;
    int64_t request_bytes = 0;
    int64_t merged_bytes = 0;
    int64_t apply_bytes = 0;
    int64_t cluster_num = 1;
};

class OrcMergedRangeFileReader final : public io::FileReader {
public:
    OrcMergedRangeFileReader(RuntimeProfile* profile, io::FileReaderSPtr file_reader,
                             io::PrefetchRange range)
            : _profile(profile),
              _file_reader(std::move(file_reader)),
              _range(range),
              _size(_file_reader->size()) {
        _statistics.apply_bytes += _range.end_offset - _range.start_offset;
        if (_profile != nullptr) {
            const char* profile_name = "OrcMergedSmallIO";
            _total_time = ADD_CHILD_TIMER_WITH_LEVEL(
                    _profile, profile_name,
                    file_scan_profile::parent_or_root(_profile, file_scan_profile::IO), 1);
            // RuntimeProfile counter lookup is flat, so every child must be format-qualified;
            // a unique parent alone cannot prevent aliasing with Parquet's MergeRange reader.
            _copy_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "OrcMergedCopyTime", profile_name, 1);
            _read_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "OrcMergedReadTime", profile_name, 1);
            _request_io = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "OrcMergedRequestIO", TUnit::UNIT,
                                                       profile_name, 1);
            _merged_io = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "OrcMergedIO", TUnit::UNIT,
                                                      profile_name, 1);
            _request_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "OrcMergedRequestBytes",
                                                          TUnit::BYTES, profile_name, 1);
            _merged_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "OrcMergedBytes", TUnit::BYTES,
                                                         profile_name, 1);
            _apply_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "OrcMergedApplyBytes",
                                                        TUnit::BYTES, profile_name, 1);
            _over_read_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "OrcMergedOverReadBytes",
                                                            TUnit::BYTES, profile_name, 1);
            _cluster_num = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "OrcMergedClusterNum",
                                                        TUnit::UNIT, profile_name, 1);
        }
    }

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const io::Path& path() const override { return _file_reader->path(); }
    size_t size() const override { return _size; }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return _file_reader->mtime(); }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        ++_statistics.request_io;
        _statistics.request_bytes += static_cast<int64_t>(result.size);
        *bytes_read = 0;
        if (result.size == 0) {
            return Status::OK();
        }
        if (offset < _range.start_offset || offset + result.size > _range.end_offset) {
            return Status::IOError("ORC stripe read [{}, {}) is outside merged range [{}, {})",
                                   offset, offset + result.size, _range.start_offset,
                                   _range.end_offset);
        }

        RETURN_IF_ERROR(_load(io_ctx));
        {
            SCOPED_RAW_TIMER(&_statistics.copy_time);
            std::memcpy(result.data, _cache.get() + offset - _range.start_offset, result.size);
        }
        *bytes_read = result.size;
        return Status::OK();
    }

    void _collect_profile_before_close() override {
        if (_profile == nullptr) {
            return;
        }
        COUNTER_UPDATE(_total_time, _statistics.copy_time + _statistics.read_time);
        COUNTER_UPDATE(_copy_time, _statistics.copy_time);
        COUNTER_UPDATE(_read_time, _statistics.read_time);
        COUNTER_UPDATE(_request_io, _statistics.request_io);
        COUNTER_UPDATE(_merged_io, _statistics.merged_io);
        COUNTER_UPDATE(_request_bytes, _statistics.request_bytes);
        COUNTER_UPDATE(_merged_bytes, _statistics.merged_bytes);
        COUNTER_UPDATE(_apply_bytes, _statistics.apply_bytes);
        COUNTER_UPDATE(_over_read_bytes,
                       std::max<int64_t>(_statistics.merged_bytes - _statistics.request_bytes, 0));
        COUNTER_UPDATE(_cluster_num, _statistics.cluster_num);
    }

private:
    Status _load(const io::IOContext* io_ctx) {
        if (_loaded) {
            return Status::OK();
        }

        const size_t range_size = _range.end_offset - _range.start_offset;
        _cache = make_unique_buffer<char>(range_size);
        size_t total_read = 0;
        {
            SCOPED_RAW_TIMER(&_statistics.read_time);
            while (total_read < range_size) {
                size_t loop_read = 0;
                RETURN_IF_ERROR(_file_reader->read_at(
                        _range.start_offset + total_read,
                        Slice(_cache.get() + total_read, range_size - total_read), &loop_read,
                        io_ctx));
                ++_statistics.merged_io;
                _statistics.merged_bytes += static_cast<int64_t>(loop_read);
                if (loop_read == 0) {
                    return Status::IOError("Short read for ORC merged range [{}, {})",
                                           _range.start_offset, _range.end_offset);
                }
                total_read += loop_read;
            }
        }
        _loaded = true;
        return Status::OK();
    }

    RuntimeProfile* _profile = nullptr;
    io::FileReaderSPtr _file_reader;
    io::PrefetchRange _range;
    size_t _size = 0;
    bool _closed = false;
    bool _loaded = false;
    DorisUniqueBufferPtr<char> _cache;
    OrcMergedRangeStatistics _statistics;

    RuntimeProfile::Counter* _copy_time = nullptr;
    RuntimeProfile::Counter* _total_time = nullptr;
    RuntimeProfile::Counter* _read_time = nullptr;
    RuntimeProfile::Counter* _request_io = nullptr;
    RuntimeProfile::Counter* _merged_io = nullptr;
    RuntimeProfile::Counter* _request_bytes = nullptr;
    RuntimeProfile::Counter* _merged_bytes = nullptr;
    RuntimeProfile::Counter* _apply_bytes = nullptr;
    RuntimeProfile::Counter* _over_read_bytes = nullptr;
    RuntimeProfile::Counter* _cluster_num = nullptr;
};

class OrcStripeInputStream final : public ::orc::InputStream {
public:
    OrcStripeInputStream(std::string file_name, io::FileReaderSPtr file_reader,
                         const io::IOContext* io_ctx, uint64_t natural_read_size)
            : _file_name(std::move(file_name)),
              _file_reader(std::move(file_reader)),
              _io_ctx(io_ctx),
              _natural_read_size(natural_read_size) {}

    uint64_t getLength() const override { return _file_reader->size(); }
    uint64_t getNaturalReadSize() const override { return _natural_read_size; }

    void read(void* buf, uint64_t length, uint64_t offset) override {
        uint64_t bytes_read = 0;
        auto* out = static_cast<uint8_t*>(buf);
        while (bytes_read < length) {
            if (_io_ctx != nullptr && _io_ctx->should_stop) {
                throw ::orc::ParseError("stop");
            }
            size_t loop_read = 0;
            Status st = _file_reader->read_at(
                    static_cast<size_t>(offset + bytes_read),
                    Slice(out + bytes_read, static_cast<size_t>(length - bytes_read)), &loop_read,
                    _io_ctx);
            if (!st.ok()) {
                throw ::orc::ParseError("Failed to read " + _file_name + ": " +
                                        st.to_string_no_stack());
            }
            if (loop_read == 0) {
                break;
            }
            bytes_read += loop_read;
        }
        if (bytes_read != length) {
            throw ::orc::ParseError("Short read from " + _file_name);
        }
    }

    const std::string& getName() const override { return _file_name; }

private:
    std::string _file_name;
    io::FileReaderSPtr _file_reader;
    const io::IOContext* _io_ctx = nullptr;
    uint64_t _natural_read_size = 0;
};

struct StripeStreamRange {
    ::orc::StreamId stream_id;
    io::PrefetchRange range;
};

} // namespace

OrcFileInputStream::OrcFileInputStream(std::string file_name, io::FileReaderSPtr file_reader,
                                       const io::IOContext* io_ctx, RuntimeProfile* profile,
                                       OrcFileInputStreamOptions options)
        : _file_name(std::move(file_name)),
          _file_reader(std::move(file_reader)),
          _default_reader(io_ctx != nullptr && io_ctx->file_reader_stats != nullptr
                                  ? std::make_shared<io::TracingFileReader>(
                                            _file_reader, io_ctx->file_reader_stats)
                                  : _file_reader),
          _io_ctx(io_ctx),
          _profile(profile),
          _options(options) {
    DORIS_CHECK_GT(_options.natural_read_size, 0);
    DORIS_CHECK_GE(_options.once_max_read_bytes, 0);
    DORIS_CHECK_GE(_options.max_merge_distance_bytes, 0);
}

OrcFileInputStream::~OrcFileInputStream() {
    _flush_active_clusters();
}

uint64_t OrcFileInputStream::getLength() const {
    return _default_reader->size();
}

uint64_t OrcFileInputStream::getNaturalReadSize() const {
    return _options.natural_read_size;
}

void OrcFileInputStream::read(void* buf, uint64_t length, uint64_t offset) {
    OrcStripeInputStream(_file_name, _default_reader, _io_ctx, _options.natural_read_size)
            .read(buf, length, offset);
}

const std::string& OrcFileInputStream::getName() const {
    return _file_name;
}

void OrcFileInputStream::beforeReadStripe(
        std::unique_ptr<::orc::StripeInformation> current_stripe_information,
        const std::vector<bool>& selected_columns,
        std::unordered_map<::orc::StreamId, std::shared_ptr<::orc::InputStream>>& streams) {
    _flush_active_clusters();
    _active_stripe_streams.clear();

    std::vector<StripeStreamRange> small_streams;
    std::vector<::orc::StreamId> direct_streams;
    uint64_t offset = current_stripe_information->getOffset();
    for (uint64_t stream_index = 0; stream_index < current_stripe_information->getNumberOfStreams();
         ++stream_index) {
        auto stream = current_stripe_information->getStreamInformation(stream_index);
        const uint64_t column_id = stream->getColumnId();
        if (column_id >= selected_columns.size()) {
            throw ::orc::ParseError(
                    fmt::format("Invalid ORC stream column id {} in {}, selected column count {}",
                                column_id, _file_name, selected_columns.size()));
        }
        const uint64_t length = stream->getLength();
        if (selected_columns[column_id]) {
            ::orc::StreamId stream_id(column_id, stream->getKind());
            if (length == 0 || std::cmp_greater(length, _options.once_max_read_bytes)) {
                direct_streams.push_back(stream_id);
            } else {
                small_streams.push_back({stream_id, io::PrefetchRange(offset, offset + length)});
            }
        }
        offset += length;
    }

    for (const auto& stream_id : direct_streams) {
        _add_direct_stream(stream_id, streams);
    }
    if (small_streams.empty()) {
        return;
    }

    std::sort(small_streams.begin(), small_streams.end(),
              [](const StripeStreamRange& left, const StripeStreamRange& right) {
                  return left.range.start_offset < right.range.start_offset;
              });
    std::vector<io::PrefetchRange> small_ranges;
    small_ranges.reserve(small_streams.size());
    for (const auto& stream : small_streams) {
        small_ranges.push_back(stream.range);
    }
    const auto merged_ranges = io::PrefetchRange::merge_adjacent_seq_ranges(
            small_ranges, _options.max_merge_distance_bytes, _options.once_max_read_bytes);

    size_t stream_index = 0;
    for (const auto& merged_range : merged_ranges) {
        std::vector<std::pair<::orc::StreamId, io::PrefetchRange>> cluster_streams;
        while (stream_index < small_streams.size() &&
               small_streams[stream_index].range.start_offset < merged_range.end_offset) {
            DORIS_CHECK_LE(small_streams[stream_index].range.end_offset, merged_range.end_offset);
            cluster_streams.emplace_back(small_streams[stream_index].stream_id,
                                         small_streams[stream_index].range);
            ++stream_index;
        }
        DORIS_CHECK(!cluster_streams.empty());
        if (cluster_streams.size() == 1) {
            _add_direct_stream(cluster_streams.front().first, streams);
        } else {
            _add_clustered_streams(cluster_streams, merged_range, streams);
        }
    }
    DORIS_CHECK_EQ(stream_index, small_streams.size());
}

void OrcFileInputStream::_flush_active_clusters() {
    for (const auto& reader : _active_cluster_readers) {
        reader->collect_profile_before_close();
    }
    _active_cluster_readers.clear();
}

void OrcFileInputStream::_add_direct_stream(
        const ::orc::StreamId& stream_id,
        std::unordered_map<::orc::StreamId, std::shared_ptr<::orc::InputStream>>& streams) {
    auto stream = std::make_shared<OrcStripeInputStream>(_file_name, _default_reader, _io_ctx,
                                                         _options.natural_read_size);
    streams.emplace(stream_id, stream);
    _active_stripe_streams.push_back(std::move(stream));
}

void OrcFileInputStream::_add_clustered_streams(
        const std::vector<std::pair<::orc::StreamId, io::PrefetchRange>>& cluster_streams,
        const io::PrefetchRange& cluster_range,
        std::unordered_map<::orc::StreamId, std::shared_ptr<::orc::InputStream>>& streams) {
    io::FileReaderSPtr cluster_reader =
            std::make_shared<OrcMergedRangeFileReader>(_profile, _file_reader, cluster_range);
    if (_io_ctx != nullptr && _io_ctx->file_reader_stats != nullptr) {
        cluster_reader = std::make_shared<io::TracingFileReader>(std::move(cluster_reader),
                                                                 _io_ctx->file_reader_stats);
    }
    _active_cluster_readers.push_back(cluster_reader);
    for (const auto& [stream_id, range] : cluster_streams) {
        DORIS_CHECK_GE(range.start_offset, cluster_range.start_offset);
        DORIS_CHECK_LE(range.end_offset, cluster_range.end_offset);
        auto stream = std::make_shared<OrcStripeInputStream>(_file_name, cluster_reader, _io_ctx,
                                                             _options.natural_read_size);
        streams.emplace(stream_id, stream);
        _active_stripe_streams.push_back(std::move(stream));
    }
}

} // namespace doris::format::orc
