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

#include "olap/inverted_index_profile.h"

#include <gtest/gtest.h>

#include "io/cache/block_file_cache_profile.h"
#include "io/io_common.h"

namespace doris {

class InvertedIndexProfileReporterTest : public testing::Test {
public:
    void SetUp() {}
    void TearDown() {}
};

TEST(InvertedIndexProfileReporterTest, UpdateTest) {
    auto runtime_profile = std::make_unique<RuntimeProfile>("test_profile");

    InvertedIndexStatistics statistics;
    statistics.stats.push_back({"test_column1", 101, 201});
    statistics.stats.push_back({"test_column2", 102, 202});

    InvertedIndexProfileReporter reporter;
    reporter.update(runtime_profile.get(), &statistics);

    ASSERT_EQ(runtime_profile->get_counter("HitRows_test_column1")->value(), 101);
    ASSERT_EQ(runtime_profile->get_counter("ExecTime_test_column1")->value(), 201);
    ASSERT_EQ(runtime_profile->get_counter("HitRows_test_column2")->value(), 102);
    ASSERT_EQ(runtime_profile->get_counter("ExecTime_test_column2")->value(), 202);
}

TEST(InvertedIndexProfileReporterTest, UpdateInvertedIndexCounters) {
    auto profile = std::make_unique<RuntimeProfile>("test_profile");
    auto reporter = std::make_unique<io::FileCacheProfileReporter>(profile.get());

    io::FileCacheStatistics stats;
    stats.inverted_index_num_local_io_total = 10;
    stats.inverted_index_num_remote_io_total = 20;
    stats.inverted_index_bytes_read_from_local = 1024;
    stats.inverted_index_bytes_read_from_remote = 2048;
    stats.inverted_index_local_io_timer = 100;
    stats.inverted_index_remote_io_timer = 200;

    reporter->update(&stats);

    EXPECT_EQ(reporter->inverted_index_num_local_io_total->value(), 10);
    EXPECT_EQ(reporter->inverted_index_num_remote_io_total->value(), 20);
    EXPECT_EQ(reporter->inverted_index_bytes_scanned_from_cache->value(), 1024);
    EXPECT_EQ(reporter->inverted_index_bytes_scanned_from_remote->value(), 2048);
    EXPECT_EQ(reporter->inverted_index_local_io_timer->value(), 100);
    EXPECT_EQ(reporter->inverted_index_remote_io_timer->value(), 200);
}

TEST(InvertedIndexProfileReporterTest, GetInvertedIndexCountersByName) {
    auto profile = std::make_unique<RuntimeProfile>("test_profile");
    auto reporter = std::make_unique<io::FileCacheProfileReporter>(profile.get());

    io::FileCacheStatistics stats;
    stats.inverted_index_num_local_io_total = 5;
    stats.inverted_index_num_remote_io_total = 10;
    stats.inverted_index_bytes_read_from_local = 512;
    stats.inverted_index_bytes_read_from_remote = 1024;
    stats.inverted_index_local_io_timer = 50;
    stats.inverted_index_remote_io_timer = 100;

    reporter->update(&stats);

    auto get_counter = [&](const std::string& name) { return profile->get_counter(name); };

    auto* num_local = get_counter("InvertedIndexNumLocalIOTotal");
    auto* num_remote = get_counter("InvertedIndexNumRemoteIOTotal");
    auto* bytes_cache = get_counter("InvertedIndexBytesScannedFromCache");
    auto* bytes_remote = get_counter("InvertedIndexBytesScannedFromRemote");
    auto* local_timer = get_counter("InvertedIndexLocalIOUseTimer");
    auto* remote_timer = get_counter("InvertedIndexRemoteIOUseTimer");

    ASSERT_NE(num_local, nullptr) << "Counter not found: InvertedIndexNumLocalIOTotal";
    ASSERT_NE(num_remote, nullptr) << "Counter not found: InvertedIndexNumRemoteIOTotal";
    ASSERT_NE(bytes_cache, nullptr) << "Counter not found: InvertedIndexBytesScannedFromCache";
    ASSERT_NE(bytes_remote, nullptr) << "Counter not found: InvertedIndexBytesScannedFromRemote";
    ASSERT_NE(local_timer, nullptr) << "Counter not found: InvertedIndexLocalIOUseTimer";
    ASSERT_NE(remote_timer, nullptr) << "Counter not found: InvertedIndexRemoteIOUseTimer";

    EXPECT_EQ(num_local->value(), 5);
    EXPECT_EQ(num_remote->value(), 10);
    EXPECT_EQ(bytes_cache->value(), 512);
    EXPECT_EQ(bytes_remote->value(), 1024);
    EXPECT_EQ(local_timer->value(), 50);
    EXPECT_EQ(remote_timer->value(), 100);
}

} // namespace doris