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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/compaction/cumulative_compaction_policy.h"
#include "storage/compaction/cumulative_compaction_time_series_policy.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_meta_manager.h"
#include "util/uid_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

class TabletMgrTest : public testing::Test {
public:
    virtual void SetUp() {
        _engine_data_path = "./be/test/storage/test_data/converter_test_data/tmp";
        auto st = io::global_local_filesystem()->delete_directory(_engine_data_path);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_engine_data_path);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_TRUE(
                io::global_local_filesystem()->create_directory(_engine_data_path + "/meta").ok());

        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;
        EngineOptions options;
        // won't open engine, options.path is needless
        options.backend_uid = UniqueId::gen_uid();
        auto engine = std::make_unique<StorageEngine>(options);
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        k_engine = &ExecEnv::GetInstance()->storage_engine().to_local();
        _data_dir = new DataDir(*k_engine, _engine_data_path, 1000000000);
        static_cast<void>(_data_dir->init());
        _tablet_mgr = k_engine->tablet_manager();
        _shutdown_backlog_base = _tablet_mgr->_shutdown_tablet_backlog_value();
    }

    virtual void TearDown() {
        if (_tablet_mgr != nullptr) {
            _tablet_mgr->_adjust_shutdown_tablet_backlog(
                    _shutdown_backlog_base - _tablet_mgr->_shutdown_tablet_backlog_value());
        }
        SAFE_DELETE(_data_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_engine_data_path).ok());
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        _tablet_mgr = nullptr;
        config::compaction_num_per_round = 1;
        config::shutdown_tablet_sweep_round_budget = 200;
        config::shutdown_tablet_sweep_interval_ms = 1000;
    }

    // Build a shared_ptr control block without constructing a real Tablet instance.
    // The returned pointer is only valid for ownership and use_count() assertions.
    // Tests must never dereference the Tablet* value behind this aliasing shared_ptr.
    TabletSharedPtr create_mock_shutdown_tablet() {
        auto owner = std::make_shared<int>(1);
        return TabletSharedPtr(owner, reinterpret_cast<Tablet*>(owner.get()));
    }

    // Replace the shutdown tablet queue with the provided test entries.
    void reset_shutdown_tablets(const std::vector<TabletSharedPtr>& tablets) {
        std::lock_guard<std::shared_mutex> wrlock(_tablet_mgr->_shutdown_tablets_lock);
        const int64_t old_size = _tablet_mgr->_shutdown_tablets.size();
        _tablet_mgr->_shutdown_tablets.clear();
        for (const auto& tablet : tablets) {
            _tablet_mgr->_shutdown_tablets.push_back(tablet);
        }
        // Keep the backlog metric aligned with direct queue mutations in unit tests.
        _tablet_mgr->_adjust_shutdown_tablet_backlog(static_cast<int64_t>(tablets.size()) -
                                                     old_size);
    }

    // Snapshot the current shutdown tablet raw pointers for order assertions.
    std::vector<Tablet*> list_shutdown_tablet_ptrs() {
        std::shared_lock<std::shared_mutex> rdlock(_tablet_mgr->_shutdown_tablets_lock);
        std::vector<Tablet*> tablets;
        tablets.reserve(_tablet_mgr->_shutdown_tablets.size());
        for (const auto& tablet : _tablet_mgr->_shutdown_tablets) {
            tablets.push_back(tablet.get());
        }
        return tablets;
    }

    // Read the current shutdown tablet count under the queue lock.
    size_t shutdown_tablet_count() {
        std::shared_lock<std::shared_mutex> rdlock(_tablet_mgr->_shutdown_tablets_lock);
        return _tablet_mgr->_shutdown_tablets.size();
    }

    int64_t shutdown_tablet_backlog_value() {
        return _tablet_mgr->_shutdown_tablet_backlog_value();
    }

    int64_t shutdown_tablet_last_sweep_ms_value() {
        return _tablet_mgr->_shutdown_tablet_last_sweep_ms_value();
    }

    StorageEngine* k_engine;

private:
    DataDir* _data_dir = nullptr;
    std::string _engine_data_path;
    TabletManager* _tablet_mgr = nullptr;
    int64_t _shutdown_backlog_base = 0;
};

TEST_F(TabletMgrTest, CreateTablet) {
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
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    RuntimeProfile profile("CreateTablet");
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet->tablet_path(), &dir_exist).ok());
    EXPECT_TRUE(dir_exist);
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    Status check_meta_st = TabletMetaManager::get_meta(_data_dir, 111, 3333, new_tablet_meta);
    EXPECT_TRUE(check_meta_st == Status::OK());

    // retry create should be successfully
    create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());

    Status drop_st = _tablet_mgr->drop_tablet(111, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet.reset();
    Status trash_st = _tablet_mgr->start_trash_sweep();
    EXPECT_TRUE(trash_st == Status::OK());
}

TEST_F(TabletMgrTest, CreateTabletWithSequence) {
    std::vector<TColumn> cols;
    TColumn col1;
    col1.column_type.type = TPrimitiveType::SMALLINT;
    col1.__set_column_name("col1");
    col1.__set_is_key(true);
    cols.push_back(col1);

    TColumn col2;
    col2.column_type.type = TPrimitiveType::INT;
    col2.__set_column_name(SEQUENCE_COL);
    col2.__set_is_key(false);
    col2.__set_aggregation_type(TAggregationType::REPLACE);
    cols.push_back(col2);

    TColumn col3;
    col3.column_type.type = TPrimitiveType::INT;
    col3.__set_column_name("v1");
    col3.__set_is_key(false);
    col3.__set_aggregation_type(TAggregationType::REPLACE);
    cols.push_back(col3);

    RuntimeProfile profile("CreateTablet");
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    tablet_schema.__set_sequence_col_idx(1);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());

    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet->tablet_path(), &dir_exist).ok());
    EXPECT_TRUE(dir_exist);
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    Status check_meta_st = TabletMetaManager::get_meta(_data_dir, 111, 3333, new_tablet_meta);
    EXPECT_TRUE(check_meta_st == Status::OK());

    Status drop_st = _tablet_mgr->drop_tablet(111, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet.reset();
    Status trash_st = _tablet_mgr->start_trash_sweep();
    EXPECT_TRUE(trash_st == Status::OK());
}

TEST_F(TabletMgrTest, DropTablet) {
    RuntimeProfile profile("CreateTablet");
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
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);

    // drop unexist tablet will be success
    Status drop_st = _tablet_mgr->drop_tablet(1121, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);

    // drop exist tablet will be success
    drop_st = _tablet_mgr->drop_tablet(111, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet == nullptr);
    tablet = _tablet_mgr->get_tablet(111, true);
    EXPECT_TRUE(tablet != nullptr);

    // check dir exist
    std::string tablet_path = tablet->tablet_path();
    bool dir_exist = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &dir_exist).ok());
    EXPECT_TRUE(dir_exist);

    // do trash sweep, tablet will not be garbage collected
    // because tablet ptr referenced it
    Status trash_st = _tablet_mgr->start_trash_sweep();
    EXPECT_TRUE(trash_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111, true);
    EXPECT_TRUE(tablet != nullptr);
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &dir_exist).ok());
    EXPECT_TRUE(dir_exist);

    // reset tablet ptr
    tablet.reset();
    trash_st = _tablet_mgr->start_trash_sweep();
    EXPECT_TRUE(trash_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111, true);
    EXPECT_TRUE(tablet == nullptr);
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &dir_exist).ok());
    EXPECT_FALSE(dir_exist);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsRecordsZeroDurationWhenEmpty) {
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [](const TabletSharedPtr&) { return true; }, [](int) {});

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(shutdown_tablet_last_sweep_ms_value(), 0);
}

TEST_F(TabletMgrTest, DeleteShutdownTabletsRoundStopsAtExactQueueEnd) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 2; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    // Drop external references so the sweep can fetch these queue entries.
    tablets.clear();

    TabletManager::ShutdownTabletIter last_it;
    {
        std::shared_lock<std::shared_mutex> rdlock(_tablet_mgr->_shutdown_tablets_lock);
        last_it = _tablet_mgr->_shutdown_tablets.begin();
    }
    std::list<TabletSharedPtr> failed_tablets;
    auto round_result = _tablet_mgr->_delete_shutdown_tablets_one_round(
            last_it, failed_tablets, [](const TabletSharedPtr&) { return true; }, 2, 2, 200);

    EXPECT_EQ(round_result.resolved_count, 2);
    EXPECT_EQ(round_result.failed_count, 0);
    EXPECT_FALSE(round_result.need_continue);
    EXPECT_TRUE(failed_tablets.empty());
    EXPECT_EQ(shutdown_tablet_count(), 0);
}

TEST_F(TabletMgrTest, FetchShutdownTabletsRespectsScanChunkForReferencedTablets) {
    // Hold extra references for the first 200 entries so the sweep must skip them.
    std::vector<TabletSharedPtr> queue_tablets;
    std::vector<TabletSharedPtr> referenced_tablets;
    for (int i = 0; i < 200; ++i) {
        auto tablet = create_mock_shutdown_tablet();
        queue_tablets.push_back(tablet);
        referenced_tablets.push_back(tablet);
    }
    queue_tablets.push_back(create_mock_shutdown_tablet());
    Tablet* last_tablet_ptr = queue_tablets.back().get();
    reset_shutdown_tablets(queue_tablets);
    // Keep external references only for the entries that should be skipped by use_count().
    queue_tablets.clear();

    TabletManager::ShutdownTabletIter last_it;
    {
        std::shared_lock<std::shared_mutex> rdlock(_tablet_mgr->_shutdown_tablets_lock);
        last_it = _tablet_mgr->_shutdown_tablets.begin();
    }

    auto first_fetch = _tablet_mgr->_fetch_shutdown_tablets(last_it, 1, 200);
    EXPECT_TRUE(first_fetch.tablets.empty());
    EXPECT_FALSE(first_fetch.reached_end);
    EXPECT_EQ(first_fetch.scanned_count, 200);
    EXPECT_EQ(shutdown_tablet_count(), 201);

    auto second_fetch = _tablet_mgr->_fetch_shutdown_tablets(last_it, 1, 200);
    EXPECT_EQ(second_fetch.tablets.size(), 1);
    EXPECT_EQ(second_fetch.tablets[0].get(), last_tablet_ptr);
    EXPECT_TRUE(second_fetch.reached_end);
    EXPECT_EQ(second_fetch.scanned_count, 1);
    EXPECT_EQ(shutdown_tablet_count(), 200);
}

TEST_F(TabletMgrTest, DeleteShutdownTabletsRoundDoesNotChargeBudgetForFailures) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 4; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    Tablet* failed_tablet_ptr = tablets[0].get();
    Tablet* remaining_tablet_ptr = tablets[3].get();
    reset_shutdown_tablets(tablets);
    // Drop external references so the sweep can fetch these queue entries.
    tablets.clear();

    TabletManager::ShutdownTabletIter last_it;
    {
        std::shared_lock<std::shared_mutex> rdlock(_tablet_mgr->_shutdown_tablets_lock);
        last_it = _tablet_mgr->_shutdown_tablets.begin();
    }
    std::list<TabletSharedPtr> failed_tablets;
    int move_attempts = 0;
    auto round_result = _tablet_mgr->_delete_shutdown_tablets_one_round(
            last_it, failed_tablets,
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return move_attempts != 1;
            },
            2, 2, 200);

    EXPECT_EQ(move_attempts, 3);
    EXPECT_EQ(round_result.resolved_count, 2);
    EXPECT_EQ(round_result.failed_count, 1);
    EXPECT_TRUE(round_result.need_continue);
    ASSERT_EQ(failed_tablets.size(), 1);
    EXPECT_EQ(failed_tablets.front().get(), failed_tablet_ptr);
    EXPECT_EQ(list_shutdown_tablet_ptrs(), std::vector<Tablet*>({remaining_tablet_ptr}));
}

TEST_F(TabletMgrTest, SweepShutdownTabletsRequeuesFailedTablets) {
    // Keep the queue order unchanged when every move attempt fails.
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 3; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    std::vector<Tablet*> tablet_ptrs;
    tablet_ptrs.reserve(tablets.size());
    for (const auto& tablet : tablets) {
        tablet_ptrs.push_back(tablet.get());
    }
    reset_shutdown_tablets(tablets);
    const int64_t expected_backlog = shutdown_tablet_backlog_value();
    // Drop external references so the sweep can fetch these queue entries.
    tablets.clear();

    int move_attempts = 0;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return false;
            },
            [](int) {});

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(move_attempts, 3);
    EXPECT_EQ(list_shutdown_tablet_ptrs(), tablet_ptrs);
    EXPECT_EQ(shutdown_tablet_backlog_value(), expected_backlog);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsKeepsNewEntriesAheadOfFailedRetries) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 2; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    Tablet* failed_tablet_ptr = tablets[1].get();
    TabletSharedPtr referenced_new_tablet = create_mock_shutdown_tablet();
    Tablet* new_tablet_ptr = referenced_new_tablet.get();
    reset_shutdown_tablets(tablets);
    // Drop external references so the sweep can fetch the initial queue entries.
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 1;
    int wait_count = 0;
    int move_attempts = 0;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return move_attempts == 1;
            },
            [&](int) {
                ++wait_count;
                _tablet_mgr->_enqueue_shutdown_tablet(referenced_new_tablet);
            });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(wait_count, 1);
    EXPECT_EQ(move_attempts, 2);
    EXPECT_EQ(list_shutdown_tablet_ptrs(),
              std::vector<Tablet*>({new_tablet_ptr, failed_tablet_ptr}));
}

TEST_F(TabletMgrTest, SweepShutdownTabletsReloadsBudgetEachRound) {
    // Update the config after the first round and verify the next round observes it.
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 3; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    // Drop external references so the sweep can move these queue entries across rounds.
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 1;
    int move_attempts = 0;
    int wait_count = 0;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return true;
            },
            [&](int) {
                ++wait_count;
                config::shutdown_tablet_sweep_round_budget = 10;
            });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(move_attempts, 3);
    EXPECT_EQ(wait_count, 1);
    EXPECT_EQ(shutdown_tablet_count(), 0);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsReloadsPositiveIntervalEachRound) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 3; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    // Drop external references so the sweep can move these queue entries across rounds.
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 1;
    config::shutdown_tablet_sweep_interval_ms = 10;
    std::vector<int> wait_intervals;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [](const TabletSharedPtr&) { return true; },
            [&](int interval_ms) {
                wait_intervals.push_back(interval_ms);
                if (wait_intervals.size() == 1) {
                    config::shutdown_tablet_sweep_interval_ms = 20;
                }
            });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(wait_intervals, std::vector<int>({10, 20}));
    EXPECT_EQ(shutdown_tablet_count(), 0);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsFallsBackForInvalidConfigs) {
    const int64_t backlog_base = shutdown_tablet_backlog_value();
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 201; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    tablets.clear();

    ASSERT_TRUE(config::set_config("shutdown_tablet_sweep_round_budget", "0", false, false).ok());
    ASSERT_TRUE(config::set_config("shutdown_tablet_sweep_interval_ms", "-1", false, false).ok());
    int move_attempts = 0;
    std::vector<int> wait_intervals;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return true;
            },
            [&](int interval_ms) { wait_intervals.push_back(interval_ms); });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(move_attempts, 201);
    EXPECT_EQ(wait_intervals, std::vector<int>({1000}));
    EXPECT_EQ(shutdown_tablet_count(), 0);
    EXPECT_EQ(shutdown_tablet_backlog_value(), backlog_base);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsFallsBackForConfigsAboveMaxAndRecovers) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 202; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 10001;
    config::shutdown_tablet_sweep_interval_ms = 10001;
    int move_attempts = 0;
    std::vector<int> wait_intervals;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return true;
            },
            [&](int interval_ms) {
                wait_intervals.push_back(interval_ms);
                if (wait_intervals.size() == 1) {
                    config::shutdown_tablet_sweep_round_budget = 1;
                    config::shutdown_tablet_sweep_interval_ms = 7;
                }
            });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(move_attempts, 202);
    EXPECT_EQ(wait_intervals, std::vector<int>({1000, 7}));
    EXPECT_EQ(shutdown_tablet_count(), 0);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsAcceptsUpperBoundConfigs) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 10001; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 10000;
    config::shutdown_tablet_sweep_interval_ms = 10000;
    int move_attempts = 0;
    std::vector<int> wait_intervals;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [&](const TabletSharedPtr&) {
                ++move_attempts;
                return true;
            },
            [&](int interval_ms) { wait_intervals.push_back(interval_ms); });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(move_attempts, 10001);
    EXPECT_EQ(wait_intervals, std::vector<int>({10000}));
    EXPECT_EQ(shutdown_tablet_count(), 0);
}

TEST_F(TabletMgrTest, SweepShutdownTabletsSupportsZeroInterval) {
    std::vector<TabletSharedPtr> tablets;
    for (int i = 0; i < 2; ++i) {
        tablets.push_back(create_mock_shutdown_tablet());
    }
    reset_shutdown_tablets(tablets);
    tablets.clear();

    config::shutdown_tablet_sweep_round_budget = 1;
    config::shutdown_tablet_sweep_interval_ms = 0;
    std::vector<int> wait_intervals;
    Status sweep_st = _tablet_mgr->_sweep_shutdown_tablets(
            [](const TabletSharedPtr&) { return true; },
            [&](int interval_ms) { wait_intervals.push_back(interval_ms); });

    EXPECT_TRUE(sweep_st.ok());
    EXPECT_EQ(wait_intervals, std::vector<int>({0}));
    EXPECT_EQ(shutdown_tablet_count(), 0);
}

TEST_F(TabletMgrTest, GetRowsetId) {
    // normal case
    {
        std::string path = _engine_data_path + "/data/0/15007/368169781";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(368169781, schema_hash);
    }
    {
        std::string path = _engine_data_path + "/data/0/15007/368169781/";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(368169781, schema_hash);
    }
    // normal case
    {
        std::string path =
                _engine_data_path +
                "/data/0/15007/368169781/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(368169781, schema_hash);

        RowsetId id;
        EXPECT_TRUE(_tablet_mgr->get_rowset_id_from_path(path, &id));
        EXPECT_EQ(2UL << 56 | 1, id.hi);
        EXPECT_EQ(2, id.mi);
        EXPECT_EQ(3, id.lo);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(0, schema_hash);

        RowsetId id;
        EXPECT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007/";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(0, schema_hash);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007abc";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_FALSE(
                _tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
    }
    // not match pattern
    {
        std::string path =
                _engine_data_path +
                "/data/0/15007/123abc/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_FALSE(
                _tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));

        RowsetId id;
        EXPECT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
}

TEST_F(TabletMgrTest, FindTabletWithCompact) {
    auto create_tablet = [this](int64_t tablet_id, int rowset_size) {
        std::vector<TColumn> cols;
        TColumn col1;
        col1.column_type.type = TPrimitiveType::SMALLINT;
        col1.__set_column_name("col1");
        col1.__set_is_key(true);
        cols.push_back(col1);

        TColumn col2;
        col2.column_type.type = TPrimitiveType::INT;
        col2.__set_column_name(SEQUENCE_COL);
        col2.__set_is_key(false);
        col2.__set_aggregation_type(TAggregationType::REPLACE);
        cols.push_back(col2);

        TColumn col3;
        col3.column_type.type = TPrimitiveType::INT;
        col3.__set_column_name("v1");
        col3.__set_is_key(false);
        col3.__set_aggregation_type(TAggregationType::REPLACE);
        cols.push_back(col3);

        RuntimeProfile profile("CreateTablet");
        TTabletSchema tablet_schema;
        tablet_schema.__set_short_key_column_count(1);
        tablet_schema.__set_schema_hash(3333);
        tablet_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
        tablet_schema.__set_storage_type(TStorageType::COLUMN);
        tablet_schema.__set_columns(cols);
        tablet_schema.__set_sequence_col_idx(1);
        TCreateTabletReq create_tablet_req;
        create_tablet_req.__set_tablet_schema(tablet_schema);
        create_tablet_req.__set_tablet_id(tablet_id);
        create_tablet_req.__set_version(1);
        create_tablet_req.__set_replica_id(tablet_id * 10);
        std::vector<DataDir*> data_dirs;
        data_dirs.push_back(_data_dir);
        Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
        ASSERT_TRUE(create_st.ok()) << create_st;

        TabletSharedPtr tablet = _tablet_mgr->get_tablet(tablet_id);
        ASSERT_TRUE(tablet);
        // check dir exist
        bool dir_exist = false;
        Status exist_st = io::global_local_filesystem()->exists(tablet->tablet_path(), &dir_exist);
        ASSERT_TRUE(exist_st.ok()) << exist_st;
        ASSERT_TRUE(dir_exist);
        // check meta has this tablet
        TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
        Status check_meta_st =
                TabletMetaManager::get_meta(_data_dir, tablet_id, 3333, new_tablet_meta);
        ASSERT_TRUE(check_meta_st.ok()) << check_meta_st;
        // insert into rowset
        auto create_rowset = [=, this](int64_t start, int64_t end) {
            auto rowset_meta = std::make_shared<RowsetMeta>();
            Version version(start, end);
            rowset_meta->set_version(version);
            rowset_meta->set_tablet_id(tablet->tablet_id());
            rowset_meta->set_tablet_uid(tablet->tablet_uid());
            rowset_meta->set_rowset_id(k_engine->next_rowset_id());
            return std::make_shared<BetaRowset>(tablet->tablet_schema(), std::move(rowset_meta),
                                                tablet->tablet_path());
        };
        auto st = tablet->init();
        ASSERT_TRUE(st.ok()) << st;
        for (int i = 2; i <= rowset_size; ++i) {
            auto rs = create_rowset(i, i);
            auto st = tablet->add_inc_rowset(rs);
            ASSERT_TRUE(st.ok()) << st;
        }
    };

    int rowset_size = 5;

    // create 10 tablets
    for (int64_t id = 1; id <= 10; ++id) {
        create_tablet(id, rowset_size++);
    }

    std::unordered_set<TabletSharedPtr> cumu_set;
    std::unordered_map<std::string_view, std::shared_ptr<CumulativeCompactionPolicy>>
            cumulative_compaction_policies;
    cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_SIZE_BASED_POLICY);
    cumulative_compaction_policies[CUMULATIVE_TIME_SERIES_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_TIME_SERIES_POLICY);
    uint32_t score = 0;
    auto compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 1);
    ASSERT_EQ(compact_tablets[0].tablet->tablet_id(), 10);
    ASSERT_EQ(score, 14);

    // drop all tablets
    for (int64_t id = 1; id <= 10; ++id) {
        Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
        ASSERT_TRUE(drop_st.ok()) << drop_st;
    }

    {
        k_engine->_compaction_num_per_round = 10;
        for (int64_t i = 1; i <= 100; ++i) {
            create_tablet(10000 + i, i);
        }

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
                cumulative_compaction_policies);
        ASSERT_EQ(compact_tablets.size(), 10);
        int index = 0;
        for (auto& t : compact_tablets) {
            ASSERT_EQ(t.tablet->tablet_id(), 10100 - index);
            ASSERT_EQ(t.tablet->calc_compaction_score(CompactionType::CUMULATIVE_COMPACTION),
                      100 - index);
            index++;
        }
        k_engine->_compaction_num_per_round = 1;
        // drop all tablets
        for (int64_t id = 10001; id <= 10100; ++id) {
            Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
            ASSERT_TRUE(drop_st.ok()) << drop_st;
        }
    }

    {
        k_engine->_compaction_num_per_round = 10;
        for (int64_t i = 1; i <= 5; ++i) {
            create_tablet(30000 + i, i + 5);
        }

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score,
                cumulative_compaction_policies);
        ASSERT_EQ(compact_tablets.size(), 5);
        for (int i = 0; i < 5; ++i) {
            ASSERT_EQ(compact_tablets[i].tablet->tablet_id(), 30000 + 5 - i);
            ASSERT_EQ(compact_tablets[i].tablet->calc_compaction_score(
                              CompactionType::CUMULATIVE_COMPACTION),
                      10 - i);
        }

        k_engine->_compaction_num_per_round = 1;
        // drop all tablets
        for (int64_t id = 30001; id <= 30005; ++id) {
            Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
            ASSERT_TRUE(drop_st.ok()) << drop_st;
        }
    }

    Status trash_st = _tablet_mgr->start_trash_sweep();
    ASSERT_TRUE(trash_st.ok()) << trash_st;
}

TEST_F(TabletMgrTest, LoadTabletFromMeta) {
    TTabletId tablet_id = 111;
    TSchemaHash schema_hash = 3333;
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
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    RuntimeProfile profile("CreateTablet");
    Status create_st =
            k_engine->tablet_manager()->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());
    TabletSharedPtr tablet = k_engine->tablet_manager()->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);

    std::string serialized_tablet_meta;
    tablet->tablet_meta()->serialize(&serialized_tablet_meta);
    bool update_meta = true;
    bool force = true;
    bool restore = false;
    bool check_path = true;
    Status st = _tablet_mgr->load_tablet_from_meta(_data_dir, tablet_id, schema_hash,
                                                   serialized_tablet_meta, update_meta, force,
                                                   restore, check_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // After reload, the original tablet should not be allowed to save meta.
    ASSERT_FALSE(tablet->do_tablet_meta_checkpoint());
}

} // namespace doris
