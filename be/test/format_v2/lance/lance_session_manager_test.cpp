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

#include "format_v2/lance/lance_session_manager.h"

#include <gtest/gtest.h>
#include <lance/lance.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>

#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"

namespace doris::format::lance {
namespace {

using LanceDatasetPtr = std::unique_ptr<LanceDataset, decltype(&lance_dataset_close)>;

std::filesystem::path lance_fixture_path() {
    return std::filesystem::path(__FILE__).parent_path().parent_path() /
           "table/lance/data/all_types.lance";
}

TEST(LanceSessionManagerTest, OpensThroughHostStorageWithObjectCachesDisabled) {
    LanceSessionManager::Config config {
            .lance_index_cache_size_bytes = 0,
            .lance_metadata_cache_size_bytes = 0,
    };
    LanceSessionManager manager(std::move(config));
    LanceDataset* raw_dataset = nullptr;
    ASSERT_TRUE(manager.open_dataset(lance_fixture_path().c_str(), nullptr, 0, &raw_dataset).ok());
    LanceDatasetPtr dataset(raw_dataset, lance_dataset_close);
    ASSERT_NE(dataset, nullptr);
}

TEST(LanceSessionManagerTest, RejectsNegativeObjectCacheCapacity) {
    for (bool index : {true, false}) {
        LanceSessionManager::Config config;
        if (index)
            config.lance_index_cache_size_bytes = -1;
        else
            config.lance_metadata_cache_size_bytes = -1;
        LanceSessionManager manager(std::move(config));
        LanceDataset* dataset = nullptr;
        const auto status =
                manager.open_dataset(lance_fixture_path().c_str(), nullptr, 0, &dataset);
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(dataset, nullptr);
    }
}

TEST(LanceSessionManagerTest, PublishesSessionCacheMetrics) {
    constexpr int64_t INDEX_CACHE_CAPACITY = 8 * 1024 * 1024;
    constexpr int64_t METADATA_CACHE_CAPACITY = 4 * 1024 * 1024;
    LanceSessionManager::Config config {
            .lance_index_cache_size_bytes = INDEX_CACHE_CAPACITY,
            .lance_metadata_cache_size_bytes = METADATA_CACHE_CAPACITY,
    };
    LanceSessionManager manager(std::move(config));
    LanceDataset* raw_dataset = nullptr;
    ASSERT_TRUE(manager.open_dataset(lance_fixture_path().c_str(), nullptr, 0, &raw_dataset).ok());
    LanceDatasetPtr dataset(raw_dataset, lance_dataset_close);
    ASSERT_NE(dataset, nullptr);

    auto* entity = DorisMetrics::instance()->server_entity();
    const auto gauge = [entity](const char* name) {
        return static_cast<IntGauge*>(entity->get_metric(name));
    };
    auto* index_capacity = gauge("lance_session_index_cache_capacity_bytes");
    auto* metadata_capacity = gauge("lance_session_metadata_cache_capacity_bytes");
    ASSERT_NE(index_capacity, nullptr);
    ASSERT_NE(metadata_capacity, nullptr);
    EXPECT_EQ(index_capacity->value(), INDEX_CACHE_CAPACITY);
    EXPECT_EQ(metadata_capacity->value(), METADATA_CACHE_CAPACITY);
    EXPECT_NE(gauge("lance_session_index_cache_usage_bytes"), nullptr);
    EXPECT_NE(gauge("lance_session_index_cache_entries"), nullptr);
    EXPECT_NE(gauge("lance_session_metadata_cache_usage_bytes"), nullptr);
    EXPECT_NE(gauge("lance_session_metadata_cache_entries"), nullptr);
    EXPECT_NE(entity->get_metric("lance_session_index_cache_hits_total"), nullptr);
    EXPECT_NE(entity->get_metric("lance_session_index_cache_misses_total"), nullptr);
    EXPECT_NE(entity->get_metric("lance_session_metadata_cache_hits_total"), nullptr);
    EXPECT_NE(entity->get_metric("lance_session_metadata_cache_misses_total"), nullptr);
}

} // namespace
} // namespace doris::format::lance
