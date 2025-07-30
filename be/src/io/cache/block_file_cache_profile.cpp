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

#include "io/cache/block_file_cache_profile.h"

#include <functional>
#include <memory>
#include <string>

#include "util/doris_metrics.h"

namespace doris::io {

std::shared_ptr<AtomicStatistics> FileCacheMetrics::report() {
    std::shared_ptr<AtomicStatistics> output_stats = std::make_shared<AtomicStatistics>();
    std::lock_guard lock(_mtx);
    output_stats->num_io_bytes_read_from_cache += _statistics->num_io_bytes_read_from_cache;
    output_stats->num_io_bytes_read_from_remote += _statistics->num_io_bytes_read_from_remote;
    return output_stats;
}

void FileCacheMetrics::update(FileCacheStatistics* input_stats) {
    if (_statistics == nullptr) {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_statistics == nullptr) {
            _statistics = std::make_shared<AtomicStatistics>();
            register_entity();
        }
    }
    _statistics->num_io_bytes_read_from_cache += input_stats->bytes_read_from_local;
    _statistics->num_io_bytes_read_from_remote += input_stats->bytes_read_from_remote;
}

void FileCacheMetrics::register_entity() {
    DorisMetrics::instance()->server_entity()->register_hook(
            "block_file_cache", [this]() { update_metrics_callback(); });
}

void FileCacheMetrics::update_metrics_callback() {
    std::shared_ptr<AtomicStatistics> stats = report();
    DorisMetrics::instance()->num_io_bytes_read_from_cache->set_value(
            stats->num_io_bytes_read_from_cache);
    DorisMetrics::instance()->num_io_bytes_read_from_remote->set_value(
            stats->num_io_bytes_read_from_remote);
    DorisMetrics::instance()->num_io_bytes_read_total->set_value(
            stats->num_io_bytes_read_from_cache + stats->num_io_bytes_read_from_remote);
}

FileCacheProfileReporter::FileCacheProfileReporter(RuntimeProfile* profile) {
    static const char* cache_profile = "FileCache";
    ADD_TIMER_WITH_LEVEL(profile, cache_profile, 1);
    num_local_io_total =
            ADD_CHILD_COUNTER_WITH_LEVEL(profile, "NumLocalIOTotal", TUnit::UNIT, cache_profile, 1);
    num_remote_io_total = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "NumRemoteIOTotal", TUnit::UNIT,
                                                       cache_profile, 1);
    local_io_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "LocalIOUseTimer", cache_profile, 1);
    remote_io_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "RemoteIOUseTimer", cache_profile, 1);
    write_cache_io_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "WriteCacheIOUseTimer", cache_profile, 1);
    bytes_write_into_cache = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "BytesWriteIntoCache",
                                                          TUnit::BYTES, cache_profile, 1);
    num_skip_cache_io_total = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "NumSkipCacheIOTotal",
                                                           TUnit::UNIT, cache_profile, 1);
    bytes_scanned_from_cache = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "BytesScannedFromCache",
                                                            TUnit::BYTES, cache_profile, 1);
    bytes_scanned_from_remote = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "BytesScannedFromRemote",
                                                             TUnit::BYTES, cache_profile, 1);
    read_cache_file_directly_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "ReadCacheFileDirectlyTimer", cache_profile, 1);
    cache_get_or_set_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "CacheGetOrSetTimer", cache_profile, 1);
    lock_wait_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "LockWaitTimer", cache_profile, 1);
    get_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "GetTimer", cache_profile, 1);
    set_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "SetTimer", cache_profile, 1);

    inverted_index_num_local_io_total = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "InvertedIndexNumLocalIOTotal", TUnit::UNIT, cache_profile, 1);
    inverted_index_num_remote_io_total = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "InvertedIndexNumRemoteIOTotal", TUnit::UNIT, cache_profile, 1);
    inverted_index_bytes_scanned_from_cache = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "InvertedIndexBytesScannedFromCache", TUnit::BYTES, cache_profile, 1);
    inverted_index_bytes_scanned_from_remote = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "InvertedIndexBytesScannedFromRemote", TUnit::BYTES, cache_profile, 1);
    inverted_index_local_io_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "InvertedIndexLocalIOUseTimer", cache_profile, 1);
    inverted_index_remote_io_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "InvertedIndexRemoteIOUseTimer", cache_profile, 1);
    inverted_index_io_timer =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "InvertedIndexIOTimer", cache_profile, 1);
}

void FileCacheProfileReporter::update(const FileCacheStatistics* statistics) const {
    COUNTER_UPDATE(num_local_io_total, statistics->num_local_io_total);
    COUNTER_UPDATE(num_remote_io_total, statistics->num_remote_io_total);
    COUNTER_UPDATE(local_io_timer, statistics->local_io_timer);
    COUNTER_UPDATE(remote_io_timer, statistics->remote_io_timer);
    COUNTER_UPDATE(write_cache_io_timer, statistics->write_cache_io_timer);
    COUNTER_UPDATE(bytes_write_into_cache, statistics->bytes_write_into_cache);
    COUNTER_UPDATE(num_skip_cache_io_total, statistics->num_skip_cache_io_total);
    COUNTER_UPDATE(bytes_scanned_from_cache, statistics->bytes_read_from_local);
    COUNTER_UPDATE(bytes_scanned_from_remote, statistics->bytes_read_from_remote);
    COUNTER_UPDATE(read_cache_file_directly_timer, statistics->read_cache_file_directly_timer);
    COUNTER_UPDATE(cache_get_or_set_timer, statistics->cache_get_or_set_timer);
    COUNTER_UPDATE(lock_wait_timer, statistics->lock_wait_timer);
    COUNTER_UPDATE(get_timer, statistics->get_timer);
    COUNTER_UPDATE(set_timer, statistics->set_timer);

    COUNTER_UPDATE(inverted_index_num_local_io_total,
                   statistics->inverted_index_num_local_io_total);
    COUNTER_UPDATE(inverted_index_num_remote_io_total,
                   statistics->inverted_index_num_remote_io_total);
    COUNTER_UPDATE(inverted_index_bytes_scanned_from_cache,
                   statistics->inverted_index_bytes_read_from_local);
    COUNTER_UPDATE(inverted_index_bytes_scanned_from_remote,
                   statistics->inverted_index_bytes_read_from_remote);
    COUNTER_UPDATE(inverted_index_local_io_timer, statistics->inverted_index_local_io_timer);
    COUNTER_UPDATE(inverted_index_remote_io_timer, statistics->inverted_index_remote_io_timer);
    COUNTER_UPDATE(inverted_index_io_timer, statistics->inverted_index_io_timer);
}

} // namespace doris::io
