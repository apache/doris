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

namespace doris::format::lance {
namespace {

using LanceDatasetPtr = std::unique_ptr<LanceDataset, decltype(&lance_dataset_close)>;

std::filesystem::path lance_fixture_path() {
    return std::filesystem::path(__FILE__).parent_path().parent_path() /
           "table/lance/data/all_types.lance";
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
    ASSERT_TRUE(manager
                        .open_dataset(lance_fixture_path().c_str(), nullptr, 0, &raw_dataset)
                        .ok());
    LanceDatasetPtr dataset(raw_dataset, lance_dataset_close);
    ASSERT_NE(dataset, nullptr);
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
        const auto status = manager.open_dataset(lance_fixture_path().c_str(), nullptr, 0,
                                                 &raw_dataset);
        LanceDatasetPtr dataset(raw_dataset, lance_dataset_close);
        EXPECT_TRUE(status.ok()) << status;
        EXPECT_NE(dataset, nullptr);
    }
    cleanup();
}

} // namespace
} // namespace doris::format::lance
