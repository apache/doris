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
    // __FILE__ is relative under -ffile-prefix-map; UTs run from the repository root.
    return "./be/test/format_v2/table/lance/data/all_types.lance";
}

std::filesystem::path unique_cache_path() {
    const auto suffix = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           ("doris_lance_foyer_cache_" + std::to_string(suffix));
}

TEST(LanceSessionManagerTest, SessionAndDataCacheConfigurationsAreIndependent) {
    LanceSessionManager::Config config {
            .lance_index_cache_size_bytes = 0,
            .lance_metadata_cache_size_bytes = 0,
            .enable_lance_data_cache = false,
            // These are deliberately invalid and must be ignored while the data cache is off.
            .lance_data_cache_path = "",
            .lance_data_cache_disk_capacity_bytes = -1,
            .lance_data_cache_read_block_size_bytes = -1,
    };
    LanceSessionManager manager(std::move(config));
    LanceDataset* raw_dataset = nullptr;
    ASSERT_TRUE(manager.open_dataset(lance_fixture_path().c_str(), nullptr, 0, &raw_dataset).ok());
    LanceDatasetPtr dataset(raw_dataset, lance_dataset_close);
    ASSERT_NE(dataset, nullptr);
}

TEST(LanceSessionManagerTest, RejectsNegativeCacheSizesBeforeCreatingTheSession) {
    const auto open_with = [](LanceSessionManager::Config config) {
        LanceSessionManager manager(std::move(config));
        LanceDataset* raw_dataset = nullptr;
        auto status = manager.open_dataset(lance_fixture_path().c_str(), nullptr, 0, &raw_dataset);
        LanceDatasetPtr dataset(raw_dataset, lance_dataset_close);
        EXPECT_EQ(dataset, nullptr);
        return status;
    };
    // Cast to uint64_t, a negative index or metadata cache size would be accepted by lance-c
    // as a cache of nearly 2^64 bytes, that is no limit at all.
    for (const auto& [index_size, metadata_size] :
         {std::pair<int64_t, int64_t> {-1, 0}, std::pair<int64_t, int64_t> {0, -1}}) {
        const auto status = open_with({
                .lance_index_cache_size_bytes = index_size,
                .lance_metadata_cache_size_bytes = metadata_size,
                .enable_lance_data_cache = false,
                .lance_data_cache_path = "",
                .lance_data_cache_disk_capacity_bytes = 0,
                .lance_data_cache_read_block_size_bytes = 0,
        });
        EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
        EXPECT_NE(status.to_string().find("must not be negative"), std::string::npos) << status;
    }

    // With the data cache enabled, its negative sizes are reported as such instead of as
    // whatever lance-c makes of the wrapped value.
    const auto cache_path = unique_cache_path();
    std::filesystem::create_directories(cache_path);
    for (const auto& [disk_capacity, block_size] :
         {std::pair<int64_t, int64_t> {-1, 64 * 1024},
          std::pair<int64_t, int64_t> {32 * 1024 * 1024, -1}}) {
        const auto status = open_with({
                .lance_index_cache_size_bytes = 0,
                .lance_metadata_cache_size_bytes = 0,
                .enable_lance_data_cache = true,
                .lance_data_cache_path = cache_path.string(),
                .lance_data_cache_disk_capacity_bytes = disk_capacity,
                .lance_data_cache_read_block_size_bytes = block_size,
        });
        EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
        EXPECT_NE(status.to_string().find("must not be negative"), std::string::npos) << status;
    }
    std::error_code error;
    std::filesystem::remove_all(cache_path, error);
}

TEST(LanceSessionManagerTest, PublishesSessionCacheMetrics) {
    constexpr int64_t INDEX_CACHE_CAPACITY = 8 * 1024 * 1024;
    constexpr int64_t METADATA_CACHE_CAPACITY = 4 * 1024 * 1024;
    LanceSessionManager::Config config {
            .lance_index_cache_size_bytes = INDEX_CACHE_CAPACITY,
            .lance_metadata_cache_size_bytes = METADATA_CACHE_CAPACITY,
            .enable_lance_data_cache = false,
            .lance_data_cache_path = "",
            .lance_data_cache_disk_capacity_bytes = 0,
            .lance_data_cache_read_block_size_bytes = 0,
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

TEST(LanceSessionManagerTest, CreatesFoyerBackedSession) {
    const auto cache_path = unique_cache_path();
    std::filesystem::create_directories(cache_path);
    const auto cleanup = [&cache_path] {
        std::error_code error;
        std::filesystem::remove_all(cache_path, error);
    };

    LanceSessionManager::Config config {
            .lance_index_cache_size_bytes = 0,
            .lance_metadata_cache_size_bytes = 0,
            .enable_lance_data_cache = true,
            .lance_data_cache_path = cache_path.string(),
            .lance_data_cache_disk_capacity_bytes = 32 * 1024 * 1024,
            .lance_data_cache_read_block_size_bytes = 64 * 1024,
    };
    {
        LanceSessionManager manager(std::move(config));
        LanceDataset* raw_dataset = nullptr;
        const auto status =
                manager.open_dataset(lance_fixture_path().c_str(), nullptr, 0, &raw_dataset);
        LanceDatasetPtr dataset(raw_dataset, lance_dataset_close);
        EXPECT_TRUE(status.ok()) << status;
        EXPECT_NE(dataset, nullptr);
    }
    cleanup();
}

} // namespace
} // namespace doris::format::lance
