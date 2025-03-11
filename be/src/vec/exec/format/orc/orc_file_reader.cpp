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

#include "vec/exec/format/orc/orc_file_reader.h"

#include "util/runtime_profile.h"

namespace doris {
namespace vectorized {

OrcMergeRangeFileReader::OrcMergeRangeFileReader(RuntimeProfile* profile,
                                                 io::FileReaderSPtr inner_reader,
                                                 io::PrefetchRange range)
        : _profile(profile), _inner_reader(std::move(inner_reader)), _range(std::move(range)) {
    _size = _inner_reader->size();
    _statistics.apply_bytes += range.end_offset - range.start_offset;
    if (_profile != nullptr) {
        const char* random_profile = "MergedSmallIO";
        ADD_TIMER_WITH_LEVEL(_profile, random_profile, 1);
        _copy_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "CopyTime", random_profile, 1);
        _read_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ReadTime", random_profile, 1);
        _request_io =
                ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "RequestIO", TUnit::UNIT, random_profile, 1);
        _merged_io =
                ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "MergedIO", TUnit::UNIT, random_profile, 1);
        _request_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "RequestBytes", TUnit::BYTES,
                                                      random_profile, 1);
        _merged_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "MergedBytes", TUnit::BYTES,
                                                     random_profile, 1);
        _apply_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "ApplyBytes", TUnit::BYTES,
                                                    random_profile, 1);
    }
}

Status OrcMergeRangeFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                             const io::IOContext* io_ctx) {
    auto request_size = result.size;

    _statistics.request_io++;
    _statistics.request_bytes += request_size;

    if (request_size == 0) {
        *bytes_read = 0;
        return Status::OK();
    }

    if (_cache == nullptr) {
        auto range_size = _range.end_offset - _range.start_offset;
        _cache = std::make_unique<char[]>(range_size);

        {
            SCOPED_RAW_TIMER(&_statistics.read_time);
            Slice cache_slice = {_cache.get(), range_size};
            RETURN_IF_ERROR(
                    _inner_reader->read_at(_range.start_offset, cache_slice, bytes_read, io_ctx));
            _statistics.merged_io++;
            _statistics.merged_bytes += *bytes_read;
        }

        if (*bytes_read != range_size) [[unlikely]] {
            return Status::InternalError(
                    "OrcMergeRangeFileReader use inner reader read bytes {} not eq expect size {}",
                    *bytes_read, range_size);
        }

        _current_start_offset = _range.start_offset;
    }

    SCOPED_RAW_TIMER(&_statistics.copy_time);
    int64_t buffer_offset = offset - _current_start_offset;
    memcpy(result.data, _cache.get() + buffer_offset, request_size);
    *bytes_read = request_size;
    return Status::OK();
}

void OrcMergeRangeFileReader::_collect_profile_before_close() {
    if (_profile != nullptr) {
        COUNTER_UPDATE(_copy_time, _statistics.copy_time);
        COUNTER_UPDATE(_read_time, _statistics.read_time);
        COUNTER_UPDATE(_request_io, _statistics.request_io);
        COUNTER_UPDATE(_merged_io, _statistics.merged_io);
        COUNTER_UPDATE(_request_bytes, _statistics.request_bytes);
        COUNTER_UPDATE(_merged_bytes, _statistics.merged_bytes);
        COUNTER_UPDATE(_apply_bytes, _statistics.apply_bytes);
        if (_inner_reader != nullptr) {
            _inner_reader->collect_profile_before_close();
        }
    }
}

} // namespace vectorized
} // namespace doris