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

#include "storage/storage_engine.h"

#include <gen_cpp/olap_file.pb.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string_view>
#include <unordered_map>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "storage/data_dir.h"
#include "storage/data_dir_sweep_policy.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta_manager.h"
#include "util/threadpool.h"

namespace doris {
using namespace config;

TEST(DataDirSweepPolicyTest, BuildsConsistentTrashAndShutdownPolicies) {
    struct TestCase {
        const char* name;
        bool is_used;
        bool ignore_guard;
        int32_t configured_trash_expire;
        double current_usage;
        double guard_space;
        int32_t expected_effective_expire;
        bool expected_eligible;
        TabletPathGcMode expected_mode;
        TabletPathGcReason expected_reason;
    };

    const std::vector<TestCase> test_cases {
            {"retention_disabled", true, false, 0, 0.1, 0.8, 0, true,
             TabletPathGcMode::DELETE_DIRECTLY, TabletPathGcReason::TRASH_RETENTION_DISABLED},
            {"manual_clean_at_zero_usage", true, true, 3600, 0.0, 0.8, 0, true,
             TabletPathGcMode::DELETE_DIRECTLY, TabletPathGcReason::MANUAL_CLEAN_TRASH},
            {"high_watermark", true, false, 3600, 0.81, 0.8, 0, true,
             TabletPathGcMode::DELETE_DIRECTLY, TabletPathGcReason::HIGH_DISK_WATERMARK},
            {"below_watermark", true, false, 3600, 0.79, 0.8, 3600, true,
             TabletPathGcMode::MOVE_TO_TRASH, TabletPathGcReason::NORMAL_RETENTION},
            {"at_watermark", true, false, 3600, 0.8, 0.8, 3600, true,
             TabletPathGcMode::MOVE_TO_TRASH, TabletPathGcReason::NORMAL_RETENTION},
            {"unused_data_dir", false, true, 3600, 0.9, 0.8, 0, false,
             TabletPathGcMode::MOVE_TO_TRASH, TabletPathGcReason::UNUSED_DATA_DIR},
    };

    for (const auto& test_case : test_cases) {
        SCOPED_TRACE(test_case.name);
        auto policy = build_data_dir_sweep_policy(test_case.is_used, test_case.ignore_guard,
                                                  test_case.configured_trash_expire,
                                                  test_case.current_usage, test_case.guard_space);
        EXPECT_EQ(policy.is_used, test_case.is_used);
        EXPECT_EQ(policy.effective_trash_expire_seconds, test_case.expected_effective_expire);
        EXPECT_EQ(policy.shutdown_tablet_gc.eligible, test_case.expected_eligible);
        EXPECT_EQ(policy.shutdown_tablet_gc.mode, test_case.expected_mode);
        EXPECT_EQ(policy.shutdown_tablet_gc.reason, test_case.expected_reason);
        if (policy.is_used) {
            EXPECT_EQ(policy.effective_trash_expire_seconds <= 0,
                      policy.shutdown_tablet_gc.mode == TabletPathGcMode::DELETE_DIRECTLY);
        }
    }
}

class StorageEngineTest : public testing::Test {
public:
    virtual void SetUp() {
        _engine_data_path = "./be/test/storage/test_data/converter_test_data/tmp";
        auto st = io::global_local_filesystem()->delete_directory(_engine_data_path);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_engine_data_path);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_TRUE(
                io::global_local_filesystem()->create_directory(_engine_data_path + "/meta").ok());

        EngineOptions options;
        options.backend_uid = UniqueId::gen_uid();
        _storage_engine = std::make_unique<StorageEngine>(options);
        _data_dir = std::make_unique<DataDir>(*_storage_engine, _engine_data_path, 100000000);
        static_cast<void>(_data_dir->init());
    }

    virtual void TearDown() {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_engine_data_path).ok());
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    std::unique_ptr<StorageEngine> _storage_engine;
    std::string _engine_data_path;
    std::unique_ptr<DataDir> _data_dir;
};

TEST_F(StorageEngineTest, GcTabletPathUsesExplicitMode) {
    const std::string move_path = _engine_data_path + "/data/0/301/3333";
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(move_path).ok());
    Status status = _data_dir->gc_tablet_path(move_path, TabletPathGcMode::MOVE_TO_TRASH);
    ASSERT_TRUE(status.ok()) << status;

    bool exists = true;
    ASSERT_TRUE(io::global_local_filesystem()->exists(move_path, &exists).ok());
    EXPECT_FALSE(exists);
    std::vector<std::string> trash_paths;
    _data_dir->find_tablet_in_trash(301, &trash_paths);
    EXPECT_EQ(trash_paths.size(), 1);

    const std::string direct_path = _engine_data_path + "/data/0/302/3333";
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(direct_path).ok());
    status = _data_dir->gc_tablet_path(direct_path, TabletPathGcMode::DELETE_DIRECTLY);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(io::global_local_filesystem()->exists(direct_path, &exists).ok());
    EXPECT_FALSE(exists);
    trash_paths.clear();
    _data_dir->find_tablet_in_trash(302, &trash_paths);
    EXPECT_TRUE(trash_paths.empty());

    status = _data_dir->gc_tablet_path(direct_path, TabletPathGcMode::DELETE_DIRECTLY);
    EXPECT_TRUE(status.ok()) << status;
}

TEST_F(StorageEngineTest, TestBrokenDisk) {
    std::string path = config::custom_config_dir + "/be_custom.conf";

    std::error_code ec;
    {
        _storage_engine->add_broken_path("broken_path1");
        EXPECT_EQ(std::filesystem::exists(path, ec), true);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path1"), 1);
        EXPECT_EQ(broken_storage_path, "broken_path1;");
    }

    {
        _storage_engine->add_broken_path("broken_path2");
        EXPECT_EQ(std::filesystem::exists(path, ec), true);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path1"), 1);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path2"), 1);
        EXPECT_EQ(broken_storage_path, "broken_path1;broken_path2;");
    }

    {
        _storage_engine->add_broken_path("broken_path2");
        EXPECT_EQ(std::filesystem::exists(path, ec), true);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path1"), 1);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path2"), 1);
        EXPECT_EQ(broken_storage_path, "broken_path1;broken_path2;");
    }

    {
        _storage_engine->remove_broken_path("broken_path2");
        EXPECT_EQ(std::filesystem::exists(path, ec), true);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path1"), 1);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path2"), 0);
        EXPECT_EQ(broken_storage_path, "broken_path1;");
    }
}

TEST_F(StorageEngineTest, TestAsyncPublish) {
    auto st = ThreadPoolBuilder("TabletPublishTxnThreadPool")
                      .set_min_threads(config::tablet_publish_txn_max_thread)
                      .set_max_threads(config::tablet_publish_txn_max_thread)
                      .build(&_storage_engine->_tablet_publish_txn_thread_pool);
    EXPECT_EQ(st, Status::OK());

    int64_t partition_id = 1;
    int64_t tablet_id = 111;

    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(tablet_id);
    create_tablet_req.__set_version(10);

    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir.get());
    RuntimeProfile profile("CreateTablet");
    st = _storage_engine->tablet_manager()->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_EQ(st, Status::OK());
    TabletSharedPtr tablet = _storage_engine->tablet_manager()->get_tablet(tablet_id);
    EXPECT_EQ(tablet->max_version().second, 10);

    for (int64_t i = 5; i < 12; ++i) {
        _storage_engine->add_async_publish_task(partition_id, tablet_id, i, i, false, i * 10);
    }
    EXPECT_EQ(_storage_engine->_async_publish_tasks[tablet_id].size(), 7);
    EXPECT_EQ(_storage_engine->get_pending_publish_min_version(tablet_id), 5);

    std::unordered_map<int64_t, int64_t> version_to_commit_tso;
    st = TabletMetaManager::traverse_pending_publish(
            _data_dir->get_meta(),
            [&](int64_t traversed_tablet_id, int64_t publish_version, std::string_view info) {
                if (traversed_tablet_id != tablet_id) {
                    return true;
                }
                PendingPublishInfoPB pb;
                bool parsed = pb.ParseFromArray(info.data(), static_cast<int>(info.size()));
                EXPECT_TRUE(parsed);
                version_to_commit_tso[publish_version] = pb.commit_tso();
                return true;
            });
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_EQ(version_to_commit_tso[5], 50);
    EXPECT_EQ(version_to_commit_tso[11], 110);

    for (int64_t i = 1; i < 8; ++i) {
        _storage_engine->_process_async_publish();
        EXPECT_EQ(_storage_engine->_async_publish_tasks[tablet_id].size(), 7 - i);
    }
    _storage_engine->_process_async_publish();
    EXPECT_EQ(_storage_engine->_async_publish_tasks.size(), 0);

    for (int64_t i = 100; i < config::max_tablet_version_num + 120; ++i) {
        _storage_engine->add_async_publish_task(partition_id, tablet_id, i, i, false, -1 /*tso*/);
    }
    EXPECT_EQ(_storage_engine->_async_publish_tasks[tablet_id].size(),
              config::max_tablet_version_num + 20);

    for (int64_t i = 90; i < 120; ++i) {
        _storage_engine->add_async_publish_task(partition_id, tablet_id, i, i, false, -1 /*tso*/);
    }
    EXPECT_EQ(_storage_engine->_async_publish_tasks[tablet_id].size(),
              config::max_tablet_version_num + 30);
    EXPECT_EQ(_storage_engine->get_pending_publish_min_version(tablet_id), 90);

    _storage_engine->_process_async_publish();
    EXPECT_EQ(_storage_engine->_async_publish_tasks[tablet_id].size(),
              config::max_tablet_version_num);
    EXPECT_EQ(_storage_engine->get_pending_publish_min_version(tablet_id), 120);

    st = _storage_engine->tablet_manager()->drop_tablet(tablet_id, 0, false);
    EXPECT_EQ(st, Status::OK());

    EXPECT_EQ(_storage_engine->_async_publish_tasks[tablet_id].size(),
              config::max_tablet_version_num);
    _storage_engine->_process_async_publish();
    EXPECT_EQ(_storage_engine->_async_publish_tasks.size(), 0);
}

} // namespace doris
