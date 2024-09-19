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

#include <gen_cpp/Types_types.h>

#include <sstream>

namespace doris {

enum class ReaderType : uint8_t {
    READER_QUERY = 0,
    READER_ALTER_TABLE = 1,
    READER_BASE_COMPACTION = 2,
    READER_CUMULATIVE_COMPACTION = 3,
    READER_CHECKSUM = 4,
    READER_COLD_DATA_COMPACTION = 5,
    READER_SEGMENT_COMPACTION = 6,
    READER_FULL_COMPACTION = 7,
    UNKNOWN = 8
};

namespace io {

struct FileCacheStatistics {
    int64_t num_local_io_total = 0;
    int64_t num_remote_io_total = 0;
    int64_t local_io_timer = 0;
    int64_t bytes_read_from_local = 0;
    int64_t bytes_read_from_remote = 0;
    int64_t remote_io_timer = 0;
    int64_t write_cache_io_timer = 0;
    int64_t bytes_write_into_cache = 0;
    int64_t num_skip_cache_io_total = 0;

    void update(const FileCacheStatistics& other) {
        num_local_io_total += other.num_local_io_total;
        num_remote_io_total += other.num_remote_io_total;
        local_io_timer += other.local_io_timer;
        bytes_read_from_local += other.bytes_read_from_local;
        bytes_read_from_remote += other.bytes_read_from_remote;
        remote_io_timer += other.remote_io_timer;
        write_cache_io_timer += other.write_cache_io_timer;
        write_cache_io_timer += other.write_cache_io_timer;
        bytes_write_into_cache += other.bytes_write_into_cache;
        num_skip_cache_io_total += other.num_skip_cache_io_total;
    }

    void reset() {
        num_local_io_total = 0;
        num_remote_io_total = 0;
        local_io_timer = 0;
        bytes_read_from_local = 0;
        bytes_read_from_remote = 0;
        remote_io_timer = 0;
        write_cache_io_timer = 0;
        bytes_write_into_cache = 0;
        num_skip_cache_io_total = 0;
    }

    std::string debug_string() const {
        std::stringstream ss;
        ss << "bytes_read_from_local: " << bytes_read_from_local
           << ", bytes_read_from_remote: " << bytes_read_from_remote;
        return ss.str();
    }
};

struct IOContext {
    ReaderType reader_type = ReaderType::UNKNOWN;
    // FIXME(plat1ko): Seems `is_disposable` can be inferred from the `reader_type`?
    bool is_disposable = false;
    bool is_index_data = false;
    bool read_file_cache = true;
    // TODO(lightman): use following member variables to control file cache
    bool is_persistent = false;
    // stop reader when reading, used in some interrupted operations
    bool should_stop = false;
    int64_t expiration_time = 0;
    const TUniqueId* query_id = nullptr;             // Ref
    FileCacheStatistics* file_cache_stats = nullptr; // Ref

    std::string debug_string() const {
        if (file_cache_stats != nullptr) {
            return file_cache_stats->debug_string();
        } else {
            return "no file cache stats";
        }
    }
};

} // namespace io
} // namespace doris
