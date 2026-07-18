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

#include <set>
#include <string>

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
    READER_BINLOG_COMPACTION = 8,
    READER_BINLOG = 9,
    UNKNOWN = 10
};

namespace io {

class RemoteScanCacheWriteLimiter;

enum class FileCacheMissPolicy : uint8_t {
    READ_THROUGH_AND_WRITE_BACK = 0,
    REMOTE_ONLY_ON_MISS = 1,
};

struct FileReaderStats {
    size_t read_calls = 0;
    size_t read_bytes = 0;
    int64_t read_time_ns = 0;
    size_t read_rows = 0;
};

struct FileCacheStatistics {
    int64_t num_local_io_total = 0;
    int64_t num_remote_io_total = 0;
    int64_t num_peer_io_total = 0;
    int64_t local_io_timer = 0;
    int64_t bytes_read_from_local = 0;
    int64_t bytes_read_from_remote = 0;
    int64_t bytes_read_from_peer = 0;
    int64_t remote_io_timer = 0;
    int64_t peer_io_timer = 0;
    int64_t remote_wait_timer = 0;
    int64_t write_cache_io_timer = 0;
    int64_t bytes_write_into_cache = 0;
    int64_t num_skip_cache_io_total = 0;
    int64_t read_cache_file_directly_timer = 0;
    int64_t cache_get_or_set_timer = 0;
    int64_t lock_wait_timer = 0;
    int64_t get_timer = 0;
    int64_t set_timer = 0;

    int64_t inverted_index_num_local_io_total = 0;
    int64_t inverted_index_num_remote_io_total = 0;
    int64_t inverted_index_num_peer_io_total = 0;
    int64_t inverted_index_bytes_read_from_local = 0;
    int64_t inverted_index_bytes_read_from_remote = 0;
    int64_t inverted_index_bytes_read_from_peer = 0;
    int64_t inverted_index_local_io_timer = 0;
    int64_t inverted_index_remote_io_timer = 0;
    int64_t inverted_index_peer_io_timer = 0;
    int64_t inverted_index_io_timer = 0;
    int64_t inverted_index_write_cache_io_timer = 0;
    int64_t inverted_index_bytes_write_into_cache = 0;

    int64_t segment_footer_index_num_local_io_total = 0;
    int64_t segment_footer_index_num_remote_io_total = 0;
    int64_t segment_footer_index_num_peer_io_total = 0;
    int64_t segment_footer_index_bytes_read_from_local = 0;
    int64_t segment_footer_index_bytes_read_from_remote = 0;
    int64_t segment_footer_index_bytes_read_from_peer = 0;
    int64_t segment_footer_index_local_io_timer = 0;
    int64_t segment_footer_index_remote_io_timer = 0;
    int64_t segment_footer_index_peer_io_timer = 0;
    int64_t segment_footer_index_write_cache_io_timer = 0;
    int64_t segment_footer_index_bytes_write_into_cache = 0;
    int64_t remote_only_on_miss_triggered = 0;
    int64_t remote_only_on_miss_threshold_bytes = 0;

    // Cross-CG / Same-CG peer read statistics
    int64_t num_cross_cg_peer_io_total = 0;
    int64_t bytes_read_from_cross_cg_peer = 0;
    int64_t cross_cg_peer_io_timer = 0; // nanoseconds
    int64_t num_same_cg_peer_io_total = 0;
    int64_t bytes_read_from_same_cg_peer = 0;
    int64_t same_cg_peer_io_timer = 0; // nanoseconds
    int64_t num_peer_race_peer_win = 0;
    int64_t num_peer_race_s3_win = 0;
    int64_t num_peer_lazy_fetch = 0;
    int64_t peer_lazy_fetch_timer = 0; // nanoseconds

    std::set<std::string> peer_hosts;

    void merge_from(const FileCacheStatistics& other) {
        num_local_io_total += other.num_local_io_total;
        num_remote_io_total += other.num_remote_io_total;
        num_peer_io_total += other.num_peer_io_total;
        local_io_timer += other.local_io_timer;
        bytes_read_from_local += other.bytes_read_from_local;
        bytes_read_from_remote += other.bytes_read_from_remote;
        bytes_read_from_peer += other.bytes_read_from_peer;
        remote_io_timer += other.remote_io_timer;
        peer_io_timer += other.peer_io_timer;
        remote_wait_timer += other.remote_wait_timer;
        write_cache_io_timer += other.write_cache_io_timer;
        bytes_write_into_cache += other.bytes_write_into_cache;
        num_skip_cache_io_total += other.num_skip_cache_io_total;
        read_cache_file_directly_timer += other.read_cache_file_directly_timer;
        cache_get_or_set_timer += other.cache_get_or_set_timer;
        lock_wait_timer += other.lock_wait_timer;
        get_timer += other.get_timer;
        set_timer += other.set_timer;

        inverted_index_num_local_io_total += other.inverted_index_num_local_io_total;
        inverted_index_num_remote_io_total += other.inverted_index_num_remote_io_total;
        inverted_index_num_peer_io_total += other.inverted_index_num_peer_io_total;
        inverted_index_bytes_read_from_local += other.inverted_index_bytes_read_from_local;
        inverted_index_bytes_read_from_remote += other.inverted_index_bytes_read_from_remote;
        inverted_index_bytes_read_from_peer += other.inverted_index_bytes_read_from_peer;
        inverted_index_local_io_timer += other.inverted_index_local_io_timer;
        inverted_index_remote_io_timer += other.inverted_index_remote_io_timer;
        inverted_index_peer_io_timer += other.inverted_index_peer_io_timer;
        inverted_index_io_timer += other.inverted_index_io_timer;
        inverted_index_write_cache_io_timer += other.inverted_index_write_cache_io_timer;
        inverted_index_bytes_write_into_cache += other.inverted_index_bytes_write_into_cache;

        segment_footer_index_num_local_io_total += other.segment_footer_index_num_local_io_total;
        segment_footer_index_num_remote_io_total += other.segment_footer_index_num_remote_io_total;
        segment_footer_index_num_peer_io_total += other.segment_footer_index_num_peer_io_total;
        segment_footer_index_bytes_read_from_local +=
                other.segment_footer_index_bytes_read_from_local;
        segment_footer_index_bytes_read_from_remote +=
                other.segment_footer_index_bytes_read_from_remote;
        segment_footer_index_bytes_read_from_peer +=
                other.segment_footer_index_bytes_read_from_peer;
        segment_footer_index_local_io_timer += other.segment_footer_index_local_io_timer;
        segment_footer_index_remote_io_timer += other.segment_footer_index_remote_io_timer;
        segment_footer_index_peer_io_timer += other.segment_footer_index_peer_io_timer;
        segment_footer_index_write_cache_io_timer +=
                other.segment_footer_index_write_cache_io_timer;
        segment_footer_index_bytes_write_into_cache +=
                other.segment_footer_index_bytes_write_into_cache;
        remote_only_on_miss_triggered =
                remote_only_on_miss_triggered || other.remote_only_on_miss_triggered;
        if (other.remote_only_on_miss_threshold_bytes > remote_only_on_miss_threshold_bytes) {
            remote_only_on_miss_threshold_bytes = other.remote_only_on_miss_threshold_bytes;
        }

        num_cross_cg_peer_io_total += other.num_cross_cg_peer_io_total;
        bytes_read_from_cross_cg_peer += other.bytes_read_from_cross_cg_peer;
        cross_cg_peer_io_timer += other.cross_cg_peer_io_timer;
        num_same_cg_peer_io_total += other.num_same_cg_peer_io_total;
        bytes_read_from_same_cg_peer += other.bytes_read_from_same_cg_peer;
        same_cg_peer_io_timer += other.same_cg_peer_io_timer;
        num_peer_race_peer_win += other.num_peer_race_peer_win;
        num_peer_race_s3_win += other.num_peer_race_s3_win;
        num_peer_lazy_fetch += other.num_peer_lazy_fetch;
        peer_lazy_fetch_timer += other.peer_lazy_fetch_timer;

        peer_hosts.insert(other.peer_hosts.begin(), other.peer_hosts.end());
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
    FileReaderStats* file_reader_stats = nullptr;    // Ref
    bool is_inverted_index = false;
    // if is_dryrun, read IO will download data to cache but return no data to reader
    // useful to skip cache data read from local disk to accelarate warm up
    bool is_dryrun = false;
    // if `is_warmup` == true, this I/O request is from a warm up task
    bool is_warmup {false};
    int64_t condition_cache_filtered_rows = 0;
    // Rows removed by file-local predicate conjuncts inside FileReader/TableReader. Scanner-level
    // output filtering already records its own unselected rows; this counter carries the rows that
    // were filtered before the block returned to Scanner.
    int64_t predicate_filtered_rows = 0;
    // if true, bypass peer read / peer-vs-S3 race and read directly from remote storage
    bool bypass_peer_read {false};
    FileCacheMissPolicy file_cache_miss_policy = FileCacheMissPolicy::READ_THROUGH_AND_WRITE_BACK;
    RemoteScanCacheWriteLimiter* remote_scan_cache_write_limiter = nullptr; // Ref
};

} // namespace io
} // namespace doris
