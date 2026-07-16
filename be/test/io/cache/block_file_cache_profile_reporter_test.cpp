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

#include <gtest/gtest.h>

#include "io/cache/block_file_cache_profile.h"

namespace doris {
namespace {

io::FileCacheStatistics make_file_cache_stats(int64_t multiplier) {
    io::FileCacheStatistics stats;
    stats.num_local_io_total = multiplier;
    stats.num_remote_io_total = multiplier * 2;
    stats.num_peer_io_total = multiplier * 3;
    stats.local_io_timer = multiplier * 4;
    stats.bytes_read_from_local = multiplier * 5;
    stats.bytes_read_from_remote = multiplier * 6;
    stats.bytes_read_from_peer = multiplier * 7;
    stats.remote_io_timer = multiplier * 8;
    stats.peer_io_timer = multiplier * 9;
    stats.remote_wait_timer = multiplier * 10;
    stats.write_cache_io_timer = multiplier * 11;
    stats.bytes_write_into_cache = multiplier * 12;
    stats.num_skip_cache_io_total = multiplier * 13;
    stats.read_cache_file_directly_timer = multiplier * 14;
    stats.cache_get_or_set_timer = multiplier * 15;
    stats.lock_wait_timer = multiplier * 16;
    stats.get_timer = multiplier * 17;
    stats.set_timer = multiplier * 18;
    stats.inverted_index_num_local_io_total = multiplier * 19;
    stats.inverted_index_num_remote_io_total = multiplier * 20;
    stats.inverted_index_num_peer_io_total = multiplier * 21;
    stats.inverted_index_bytes_read_from_local = multiplier * 22;
    stats.inverted_index_bytes_read_from_remote = multiplier * 23;
    stats.inverted_index_bytes_read_from_peer = multiplier * 24;
    stats.inverted_index_local_io_timer = multiplier * 25;
    stats.inverted_index_remote_io_timer = multiplier * 26;
    stats.inverted_index_peer_io_timer = multiplier * 27;
    stats.inverted_index_io_timer = multiplier * 28;
    stats.remote_only_on_miss_triggered = multiplier * 29;
    stats.remote_only_on_miss_threshold_bytes = multiplier * 30;
    stats.async_cache_write_submitted = multiplier * 31;
    stats.async_cache_write_rejected = multiplier * 32;
    stats.async_cache_write_buffer_alloc_fail = multiplier * 33;
    stats.async_cache_write_drop_stale_epoch = multiplier * 34;
    stats.inflight_write_buffer_index_hit = multiplier * 35;
    stats.inflight_write_buffer_index_miss = multiplier * 36;
    stats.probe_downloaded_hit = multiplier * 37;
    stats.probe_downloading_hit = multiplier * 38;
    stats.probe_miss = multiplier * 39;
    stats.block_wait_success = multiplier * 40;
    stats.block_wait_timeout = multiplier * 41;
    return stats;
}

void expect_file_cache_stats_eq(const io::FileCacheStatistics& actual,
                                const io::FileCacheStatistics& expected) {
    EXPECT_EQ(actual.num_local_io_total, expected.num_local_io_total);
    EXPECT_EQ(actual.num_remote_io_total, expected.num_remote_io_total);
    EXPECT_EQ(actual.num_peer_io_total, expected.num_peer_io_total);
    EXPECT_EQ(actual.local_io_timer, expected.local_io_timer);
    EXPECT_EQ(actual.bytes_read_from_local, expected.bytes_read_from_local);
    EXPECT_EQ(actual.bytes_read_from_remote, expected.bytes_read_from_remote);
    EXPECT_EQ(actual.bytes_read_from_peer, expected.bytes_read_from_peer);
    EXPECT_EQ(actual.remote_io_timer, expected.remote_io_timer);
    EXPECT_EQ(actual.peer_io_timer, expected.peer_io_timer);
    EXPECT_EQ(actual.remote_wait_timer, expected.remote_wait_timer);
    EXPECT_EQ(actual.write_cache_io_timer, expected.write_cache_io_timer);
    EXPECT_EQ(actual.bytes_write_into_cache, expected.bytes_write_into_cache);
    EXPECT_EQ(actual.num_skip_cache_io_total, expected.num_skip_cache_io_total);
    EXPECT_EQ(actual.read_cache_file_directly_timer, expected.read_cache_file_directly_timer);
    EXPECT_EQ(actual.cache_get_or_set_timer, expected.cache_get_or_set_timer);
    EXPECT_EQ(actual.lock_wait_timer, expected.lock_wait_timer);
    EXPECT_EQ(actual.get_timer, expected.get_timer);
    EXPECT_EQ(actual.set_timer, expected.set_timer);
    EXPECT_EQ(actual.inverted_index_num_local_io_total, expected.inverted_index_num_local_io_total);
    EXPECT_EQ(actual.inverted_index_num_remote_io_total,
              expected.inverted_index_num_remote_io_total);
    EXPECT_EQ(actual.inverted_index_num_peer_io_total, expected.inverted_index_num_peer_io_total);
    EXPECT_EQ(actual.inverted_index_bytes_read_from_local,
              expected.inverted_index_bytes_read_from_local);
    EXPECT_EQ(actual.inverted_index_bytes_read_from_remote,
              expected.inverted_index_bytes_read_from_remote);
    EXPECT_EQ(actual.inverted_index_bytes_read_from_peer,
              expected.inverted_index_bytes_read_from_peer);
    EXPECT_EQ(actual.inverted_index_local_io_timer, expected.inverted_index_local_io_timer);
    EXPECT_EQ(actual.inverted_index_remote_io_timer, expected.inverted_index_remote_io_timer);
    EXPECT_EQ(actual.inverted_index_peer_io_timer, expected.inverted_index_peer_io_timer);
    EXPECT_EQ(actual.inverted_index_io_timer, expected.inverted_index_io_timer);
    EXPECT_EQ(actual.remote_only_on_miss_triggered, expected.remote_only_on_miss_triggered);
    EXPECT_EQ(actual.remote_only_on_miss_threshold_bytes,
              expected.remote_only_on_miss_threshold_bytes);
    EXPECT_EQ(actual.async_cache_write_submitted, expected.async_cache_write_submitted);
    EXPECT_EQ(actual.async_cache_write_rejected, expected.async_cache_write_rejected);
    EXPECT_EQ(actual.async_cache_write_buffer_alloc_fail,
              expected.async_cache_write_buffer_alloc_fail);
    EXPECT_EQ(actual.async_cache_write_drop_stale_epoch,
              expected.async_cache_write_drop_stale_epoch);
    EXPECT_EQ(actual.inflight_write_buffer_index_hit, expected.inflight_write_buffer_index_hit);
    EXPECT_EQ(actual.inflight_write_buffer_index_miss, expected.inflight_write_buffer_index_miss);
    EXPECT_EQ(actual.probe_downloaded_hit, expected.probe_downloaded_hit);
    EXPECT_EQ(actual.probe_downloading_hit, expected.probe_downloading_hit);
    EXPECT_EQ(actual.probe_miss, expected.probe_miss);
    EXPECT_EQ(actual.block_wait_success, expected.block_wait_success);
    EXPECT_EQ(actual.block_wait_timeout, expected.block_wait_timeout);
}

} // namespace

TEST(FileCacheProfileReporterTest, DiffReturnsFullStatsWhenPreviousIsZero) {
    const auto current = make_file_cache_stats(3);

    expect_file_cache_stats_eq(io::diff_file_cache_statistics(current, {}), current);
}

TEST(FileCacheProfileReporterTest, DiffReturnsOnlyIncrementalDelta) {
    expect_file_cache_stats_eq(
            io::diff_file_cache_statistics(make_file_cache_stats(5), make_file_cache_stats(3)),
            make_file_cache_stats(2));
}

TEST(FileCacheProfileReporterTest, DiffReturnsZeroWithoutNewData) {
    const auto current = make_file_cache_stats(4);

    expect_file_cache_stats_eq(io::diff_file_cache_statistics(current, current),
                               make_file_cache_stats(0));
}

TEST(FileCacheProfileReporterTest, MergeIncludesEveryAsyncReadAndWriteField) {
    auto aggregate = make_file_cache_stats(2);
    aggregate.merge_from(make_file_cache_stats(3));

    auto expected = make_file_cache_stats(5);
    // These two pre-existing fields merge by OR/max instead of addition.
    expected.remote_only_on_miss_triggered = true;
    expected.remote_only_on_miss_threshold_bytes = 90;
    expect_file_cache_stats_eq(aggregate, expected);
}

TEST(FileCacheProfileReporterTest, ReporterAggregatesDeltaReportsToExactFinalTotals) {
    auto profile = std::make_unique<RuntimeProfile>("test_profile");
    io::FileCacheProfileReporter reporter(profile.get());

    const auto after_first_report = make_file_cache_stats(4);
    const auto after_second_report = make_file_cache_stats(7);
    const auto first_delta = io::diff_file_cache_statistics(after_first_report, {});
    reporter.update(&first_delta);

    const auto second_delta =
            io::diff_file_cache_statistics(after_second_report, after_first_report);
    reporter.update(&second_delta);

    EXPECT_EQ(profile->get_counter("BytesScannedFromCache")->value(),
              after_second_report.bytes_read_from_local);
    EXPECT_EQ(profile->get_counter("BytesScannedFromRemote")->value(),
              after_second_report.bytes_read_from_remote);
    EXPECT_EQ(profile->get_counter("BytesWriteIntoCache")->value(),
              after_second_report.bytes_write_into_cache);
    EXPECT_EQ(profile->get_counter("CacheGetOrSetTimer")->value(),
              after_second_report.cache_get_or_set_timer);
    EXPECT_EQ(profile->get_counter("LockWaitTimer")->value(), after_second_report.lock_wait_timer);
    EXPECT_EQ(profile->get_counter("AsyncCacheWriteSubmitted")->value(),
              after_second_report.async_cache_write_submitted);
    EXPECT_EQ(profile->get_counter("AsyncCacheWriteRejected")->value(),
              after_second_report.async_cache_write_rejected);
    EXPECT_EQ(profile->get_counter("AsyncCacheWriteBufferAllocFail")->value(),
              after_second_report.async_cache_write_buffer_alloc_fail);
    EXPECT_EQ(profile->get_counter("AsyncCacheWriteDropStaleEpoch")->value(),
              after_second_report.async_cache_write_drop_stale_epoch);
    EXPECT_EQ(profile->get_counter("InflightWriteBufferIndexHit")->value(),
              after_second_report.inflight_write_buffer_index_hit);
    EXPECT_EQ(profile->get_counter("InflightWriteBufferIndexMiss")->value(),
              after_second_report.inflight_write_buffer_index_miss);
    EXPECT_EQ(profile->get_counter("ProbeDownloadedHit")->value(),
              after_second_report.probe_downloaded_hit);
    EXPECT_EQ(profile->get_counter("ProbeDownloadingHit")->value(),
              after_second_report.probe_downloading_hit);
    EXPECT_EQ(profile->get_counter("ProbeMiss")->value(), after_second_report.probe_miss);
    EXPECT_EQ(profile->get_counter("BlockWaitSuccess")->value(),
              after_second_report.block_wait_success);
    EXPECT_EQ(profile->get_counter("BlockWaitTimeout")->value(),
              after_second_report.block_wait_timeout);
}

} // namespace doris
