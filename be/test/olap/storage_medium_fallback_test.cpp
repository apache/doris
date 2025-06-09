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

#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "common/config.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "util/threadpool.h"

namespace doris {

class StorageMediumFallbackTest : public testing::Test {
public:
    void SetUp() override {
        // Create test directories
        _test_path = "./be/test/olap/test_data/storage_medium_fallback_test";
        _hdd_path = _test_path + "/hdd";
        _ssd_path = _test_path + "/ssd";

        // Clean up existing test directories
        auto st = io::global_local_filesystem()->delete_directory(_test_path);
        st = io::global_local_filesystem()->create_directory(_test_path);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_hdd_path);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_ssd_path);
        ASSERT_TRUE(st.ok()) << st;

        // Create meta directories
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_hdd_path + "/meta").ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_ssd_path + "/meta").ok());

        // Setup storage engine
        EngineOptions options;
        options.backend_uid = UniqueId::gen_uid();
        _storage_engine = std::make_unique<StorageEngine>(options);

        // Store original config values
        _original_fallback_config = config::enable_storage_medium_fallback;
    }

    void TearDown() override {
        // Restore original config
        config::enable_storage_medium_fallback = _original_fallback_config;

        // Clean up test directories
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_test_path).ok());
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

protected:
    // Helper method to setup storage engine with specific configuration
    void setupStorageEngine(bool include_hdd = true, bool include_ssd = true) {
        _storage_engine->_store_map.clear();
        _storage_engine->_available_storage_medium_type_count = 0;

        if (include_hdd) {
            auto hdd_dir = std::make_unique<DataDir>(*_storage_engine, _hdd_path, 100000000,
                                                     TStorageMedium::HDD);
            auto init_status = hdd_dir->init();
            EXPECT_TRUE(init_status.ok()) << "HDD DataDir init failed: " << init_status;
            _storage_engine->_store_map[_hdd_path] = std::move(hdd_dir);
        }

        if (include_ssd) {
            auto ssd_dir = std::make_unique<DataDir>(*_storage_engine, _ssd_path, 100000000,
                                                     TStorageMedium::SSD);
            auto init_status = ssd_dir->init();
            EXPECT_TRUE(init_status.ok()) << "SSD DataDir init failed: " << init_status;
            _storage_engine->_store_map[_ssd_path] = std::move(ssd_dir);
        }

        // Count unique storage medium types
        std::set<TStorageMedium::type> medium_types;
        for (const auto& store : _storage_engine->_store_map) {
            medium_types.insert(store.second->storage_medium());
        }
        _storage_engine->_available_storage_medium_type_count = medium_types.size();
    }

    std::unique_ptr<StorageEngine> _storage_engine;
    std::string _test_path;
    std::string _hdd_path;
    std::string _ssd_path;
    bool _original_fallback_config;
};

TEST_F(StorageMediumFallbackTest, NormalCase_SingleMedium_HDD) {
    setupStorageEngine(true, false); // only HDD
    config::enable_storage_medium_fallback = true;

    std::vector<DirInfo> dir_infos;
    _storage_engine->_get_candidate_stores(TStorageMedium::HDD, dir_infos);

    EXPECT_EQ(dir_infos.size(), 1);
    EXPECT_EQ(dir_infos[0].data_dir->storage_medium(), TStorageMedium::HDD);
}

TEST_F(StorageMediumFallbackTest, NormalCase_MixedMedium_RequestHDD) {
    setupStorageEngine(true, true); // both HDD and SSD
    config::enable_storage_medium_fallback = true;

    std::vector<DirInfo> dir_infos;
    _storage_engine->_get_candidate_stores(TStorageMedium::HDD, dir_infos);

    EXPECT_EQ(dir_infos.size(), 1);
    EXPECT_EQ(dir_infos[0].data_dir->storage_medium(), TStorageMedium::HDD);
}

TEST_F(StorageMediumFallbackTest, NormalCase_MixedMedium_RequestSSD) {
    setupStorageEngine(true, true); // both HDD and SSD
    config::enable_storage_medium_fallback = true;

    std::vector<DirInfo> dir_infos;
    _storage_engine->_get_candidate_stores(TStorageMedium::SSD, dir_infos);

    EXPECT_EQ(dir_infos.size(), 1);
    EXPECT_EQ(dir_infos[0].data_dir->storage_medium(), TStorageMedium::SSD);
}

TEST_F(StorageMediumFallbackTest, FallbackEnabled_SingleMediumInconsistent) {
    setupStorageEngine(true, false); // only HDD
    config::enable_storage_medium_fallback = true;

    std::vector<DirInfo> dir_infos;
    _storage_engine->_get_candidate_stores(TStorageMedium::SSD, dir_infos);

    EXPECT_EQ(dir_infos.size(), 1);
    EXPECT_EQ(dir_infos[0].data_dir->storage_medium(), TStorageMedium::HDD); // fallback to HDD
}

TEST_F(StorageMediumFallbackTest, FallbackEnabled_MixedMediumUnavailable) {
    setupStorageEngine(false, true); // only SSD available
    config::enable_storage_medium_fallback = true;

    std::vector<DirInfo> dir_infos;
    _storage_engine->_get_candidate_stores(TStorageMedium::HDD, dir_infos);

    EXPECT_EQ(dir_infos.size(), 1);
    EXPECT_EQ(dir_infos[0].data_dir->storage_medium(), TStorageMedium::SSD); // fallback to SSD
}

TEST_F(StorageMediumFallbackTest, FallbackDisabled_SingleMediumInconsistent) {
    // Single medium type always forces fallback regardless of config
    setupStorageEngine(true, false); // only HDD
    config::enable_storage_medium_fallback = false;

    std::vector<DirInfo> dir_infos;
    _storage_engine->_get_candidate_stores(TStorageMedium::SSD, dir_infos);

    EXPECT_EQ(dir_infos.size(), 1); // forced fallback
    EXPECT_EQ(dir_infos[0].data_dir->storage_medium(), TStorageMedium::HDD);
}

TEST_F(StorageMediumFallbackTest, FallbackDisabled_MixedMediumUnavailable) {
    // Mixed environment: fallback disabled should be respected
    setupStorageEngine(true, true); // both HDD and SSD
    config::enable_storage_medium_fallback = false;

    // Verify normal operation first
    std::vector<DirInfo> dir_infos;
    _storage_engine->_get_candidate_stores(TStorageMedium::HDD, dir_infos);
    EXPECT_EQ(dir_infos.size(), 1);
    EXPECT_EQ(dir_infos[0].data_dir->storage_medium(), TStorageMedium::HDD);

    // Simulate HDD becoming unavailable but maintain mixed environment count
    _storage_engine->_store_map.erase(_hdd_path);
    _storage_engine->_available_storage_medium_type_count = 2;

    // Request unavailable HDD - should fail without fallback
    dir_infos.clear();
    _storage_engine->_get_candidate_stores(TStorageMedium::HDD, dir_infos);

    EXPECT_EQ(dir_infos.size(), 0); // no fallback when disabled
}

TEST_F(StorageMediumFallbackTest, EmptyStoreMap) {
    _storage_engine->_store_map.clear();
    _storage_engine->_available_storage_medium_type_count = 0;
    config::enable_storage_medium_fallback = true;

    std::vector<DirInfo> dir_infos;
    _storage_engine->_get_candidate_stores(TStorageMedium::HDD, dir_infos);

    EXPECT_EQ(dir_infos.size(), 0);
}

TEST_F(StorageMediumFallbackTest, SingleMediumType_AlwaysFallback) {
    setupStorageEngine(true, false); // only HDD
    _storage_engine->_available_storage_medium_type_count = 1;
    config::enable_storage_medium_fallback = false;

    std::vector<DirInfo> dir_infos;
    _storage_engine->_get_candidate_stores(TStorageMedium::SSD, dir_infos);

    EXPECT_EQ(dir_infos.size(), 1); // forced fallback
    EXPECT_EQ(dir_infos[0].data_dir->storage_medium(), TStorageMedium::HDD);
}

TEST_F(StorageMediumFallbackTest, Config_DefaultValue) {
    config::enable_storage_medium_fallback = false; // default
    setupStorageEngine(true, true);                 // both HDD and SSD

    std::vector<DirInfo> dir_infos;
    _storage_engine->_get_candidate_stores(TStorageMedium::SSD, dir_infos);

    EXPECT_EQ(dir_infos.size(), 1);
    EXPECT_EQ(dir_infos[0].data_dir->storage_medium(), TStorageMedium::SSD);

    // Single medium type forces fallback even with default config
    setupStorageEngine(true, false); // only HDD
    dir_infos.clear();
    _storage_engine->_get_candidate_stores(TStorageMedium::SSD, dir_infos);

    EXPECT_EQ(dir_infos.size(), 1); // forced fallback
    EXPECT_EQ(dir_infos[0].data_dir->storage_medium(), TStorageMedium::HDD);
}

TEST_F(StorageMediumFallbackTest, Config_MultiMediumFallbackControl) {
    setupStorageEngine(true, true); // both HDD and SSD
    config::enable_storage_medium_fallback = false;

    // Normal operation works fine
    std::vector<DirInfo> dir_infos;
    _storage_engine->_get_candidate_stores(TStorageMedium::HDD, dir_infos);
    EXPECT_EQ(dir_infos.size(), 1);
    EXPECT_EQ(dir_infos[0].data_dir->storage_medium(), TStorageMedium::HDD);

    // Simulate HDD unavailable but maintain multi-medium count
    _storage_engine->_store_map.erase(_hdd_path);
    _storage_engine->_available_storage_medium_type_count = 2;

    dir_infos.clear();
    _storage_engine->_get_candidate_stores(TStorageMedium::HDD, dir_infos);
    EXPECT_EQ(dir_infos.size(), 0); // no fallback when disabled

    // Enable fallback and test again
    config::enable_storage_medium_fallback = true;
    dir_infos.clear();
    _storage_engine->_get_candidate_stores(TStorageMedium::HDD, dir_infos);

    EXPECT_EQ(dir_infos.size(), 1); // fallback to SSD
    EXPECT_EQ(dir_infos[0].data_dir->storage_medium(), TStorageMedium::SSD);
}

} // namespace doris