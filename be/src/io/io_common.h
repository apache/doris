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

#include "gen_cpp/Types_types.h"

namespace doris {

enum ReaderType {
    READER_QUERY = 0,
    READER_ALTER_TABLE = 1,
    READER_BASE_COMPACTION = 2,
    READER_CUMULATIVE_COMPACTION = 3,
    READER_CHECKSUM = 4,
    READER_COLD_DATA_COMPACTION = 5,
    READER_SEGMENT_COMPACTION = 6,
};

namespace io {

struct FileCacheStatistics {
    int64_t num_io_total = 0;
    int64_t num_io_hit_cache = 0;
    int64_t num_io_bytes_read_total = 0;
    int64_t num_io_bytes_read_from_file_cache = 0;
    int64_t num_io_bytes_read_from_write_cache = 0;
    int64_t num_io_written_in_file_cache = 0;
    int64_t num_io_bytes_written_in_file_cache = 0;
    int64_t num_io_bytes_skip_cache = 0;
};

class IOContext {
public:
    IOContext() = default;

    IOContext(const TUniqueId* query_id_, FileCacheStatistics* stats_, bool is_presistent_,
              bool use_disposable_cache_, bool read_segment_index_, bool enable_file_cache)
            : query_id(query_id_),
              is_persistent(is_presistent_),
              use_disposable_cache(use_disposable_cache_),
              read_segment_index(read_segment_index_),
              file_cache_stats(stats_),
              enable_file_cache(enable_file_cache) {}
    ReaderType reader_type;
    const TUniqueId* query_id = nullptr;
    bool is_persistent = false;
    bool use_disposable_cache = false;
    bool read_segment_index = false;
    FileCacheStatistics* file_cache_stats = nullptr;
    bool enable_file_cache = true;
};

} // namespace io
} // namespace doris
