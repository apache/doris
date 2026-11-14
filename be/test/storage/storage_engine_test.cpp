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
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta_manager.h"
#include "storage/txn/txn_manager.h"
#include "util/threadpool.h"

namespace doris {
using namespace config;

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

    TabletSharedPtr create_publish_tablet(int64_t tablet_id) {
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
        auto st = _storage_engine->tablet_manager()->create_tablet(create_tablet_req, data_dirs,
                                                                   &profile);
        EXPECT_EQ(st, Status::OK());
        TabletSharedPtr tablet = _storage_engine->tablet_manager()->get_tablet(tablet_id);
        return tablet;
    }

    std::unique_ptr<StorageEngine> _storage_engine;
    std::string _engine_data_path;
    std::unique_ptr<DataDir> _data_dir;
};

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

    auto tablet = create_publish_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    ASSERT_EQ(tablet->max_version().second, 10);

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

    // Keep the oldest transaction alive so this exercises queue trimming at a version gap.
    ASSERT_TRUE(_storage_engine->txn_manager()
                        ->prepare_txn(partition_id, *tablet, 90, PUniqueId())
                        .ok());
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

TEST_F(StorageEngineTest, AsyncPublishRemovesMissingTransactionsAcrossGap) {
    constexpr int64_t tablet_id = 222;
    constexpr int64_t partition_id = 2;
    auto tablet = create_publish_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    ASSERT_EQ(tablet->max_version().second, 10);

    // Neither transaction exists locally. Both versions are beyond the next publishable version.
    _storage_engine->add_async_publish_task(partition_id, tablet_id, 20, 200, false, 2000);
    _storage_engine->add_async_publish_task(partition_id, tablet_id, 30, 300, false, 3000);

    auto persisted_versions = [&]() {
        std::vector<int64_t> versions;
        auto st = TabletMetaManager::traverse_pending_publish(
                _data_dir->get_meta(), [&](int64_t id, int64_t version, std::string_view) {
                    if (id == tablet_id) {
                        versions.push_back(version);
                    }
                    return true;
                });
        EXPECT_TRUE(st.ok()) << st;
        return versions;
    };
    EXPECT_THAT(persisted_versions(), testing::UnorderedElementsAre(20, 30));

    // Simulate the pending queue being reloaded from metadata after a restart.
    _storage_engine->_async_publish_tasks.clear();
    ASSERT_TRUE(TabletMetaManager::traverse_pending_publish(
                        _data_dir->get_meta(),
                        [&](int64_t id, int64_t version, std::string_view info) {
                            PendingPublishInfoPB pb;
                            EXPECT_TRUE(
                                    pb.ParseFromArray(info.data(), static_cast<int>(info.size())));
                            _storage_engine->add_async_publish_task(pb.partition_id(), id, version,
                                                                    pb.transaction_id(), true,
                                                                    pb.commit_tso());
                            return true;
                        })
                        .ok());
    EXPECT_EQ(_storage_engine->get_pending_publish_min_version(tablet_id), 20);

    _storage_engine->_process_async_publish();
    EXPECT_EQ(_storage_engine->get_pending_publish_min_version(tablet_id), 30);
    EXPECT_THAT(persisted_versions(), testing::ElementsAre(30));
    _storage_engine->_process_async_publish();
    EXPECT_EQ(_storage_engine->get_pending_publish_min_version(tablet_id), INT64_MAX);
    EXPECT_TRUE(persisted_versions().empty());
    EXPECT_EQ(tablet->max_version().second, 10);
    _storage_engine->_process_async_publish();
    EXPECT_TRUE(_storage_engine->_async_publish_tasks.empty());
}

TEST_F(StorageEngineTest, AsyncPublishRetainsExistingTransactionUntilCleared) {
    constexpr int64_t tablet_id = 333;
    constexpr int64_t partition_id = 3;
    constexpr int64_t transaction_id = 400;
    auto tablet = create_publish_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    auto* txn_manager = _storage_engine->txn_manager();
    ASSERT_TRUE(txn_manager->prepare_txn(partition_id, *tablet, transaction_id, PUniqueId()).ok());
    _storage_engine->add_async_publish_task(partition_id, tablet_id, 40, transaction_id, false,
                                            4000);

    // An existing transaction must still bound clone while earlier versions are missing.
    _storage_engine->_process_async_publish();
    EXPECT_EQ(_storage_engine->get_pending_publish_min_version(tablet_id), 40);
    EXPECT_EQ(_storage_engine->_async_publish_tasks.at(tablet_id).size(), 1);
    EXPECT_EQ(txn_manager->get_txn_state(partition_id, transaction_id, tablet_id,
                                         tablet->tablet_uid()),
              TxnState::PREPARED);

    ASSERT_TRUE(txn_manager->delete_txn(partition_id, tablet, transaction_id).ok());
    _storage_engine->_process_async_publish();
    EXPECT_EQ(_storage_engine->get_pending_publish_min_version(tablet_id), INT64_MAX);
    EXPECT_EQ(tablet->max_version().second, 10);
}

TEST_F(StorageEngineTest, AsyncPublishChecksTransactionTabletIdentity) {
    constexpr int64_t tablet_id = 444;
    constexpr int64_t partition_id = 4;
    constexpr int64_t transaction_id = 500;
    auto tablet = create_publish_tablet(tablet_id);
    ASSERT_NE(tablet, nullptr);
    auto* txn_manager = _storage_engine->txn_manager();
    auto old_tablet_uid = UniqueId::gen_uid();
    ASSERT_NE(old_tablet_uid, tablet->tablet_uid());
    ASSERT_TRUE(txn_manager
                        ->prepare_txn(partition_id, transaction_id, tablet_id, old_tablet_uid,
                                      PUniqueId())
                        .ok());
    ASSERT_TRUE(
            txn_manager->prepare_txn(partition_id + 1, *tablet, transaction_id, PUniqueId()).ok());
    _storage_engine->add_async_publish_task(partition_id, tablet_id, 50, transaction_id, false,
                                            5000);

    // A transaction for an old replica or another partition cannot publish this tablet's record.
    _storage_engine->_process_async_publish();
    EXPECT_EQ(_storage_engine->get_pending_publish_min_version(tablet_id), INT64_MAX);
    EXPECT_EQ(txn_manager->get_txn_state(partition_id, transaction_id, tablet_id, old_tablet_uid),
              TxnState::PREPARED);
    EXPECT_EQ(txn_manager->get_txn_state(partition_id + 1, transaction_id, tablet_id,
                                         tablet->tablet_uid()),
              TxnState::PREPARED);
}

} // namespace doris
