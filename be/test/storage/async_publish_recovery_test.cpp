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
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <vector>

#include "io/fs/local_file_system.h"
#include "storage/data_dir.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta_manager.h"
#include "storage/task/engine_publish_version_task.h"
#include "storage/txn/txn_manager.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris {

class AsyncPublishRecoveryTest : public testing::Test {
public:
    void SetUp() override {
        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(_path).ok());
        ASSERT_TRUE(fs->create_directory(_path).ok());
        EngineOptions options;
        options.backend_uid = UniqueId::gen_uid();
        _engine = std::make_unique<StorageEngine>(options);
        _dir = std::make_unique<DataDir>(*_engine, _path, 100000000);
        ASSERT_TRUE(_dir->init().ok());
        ASSERT_TRUE(ThreadPoolBuilder("AsyncPublishRecoveryTest")
                            .set_min_threads(1)
                            .set_max_threads(1)
                            .build(&_engine->_tablet_publish_txn_thread_pool)
                            .ok());

        TColumnType type;
        type.__set_type(TPrimitiveType::SMALLINT);
        TColumn key;
        key.__set_column_name("k");
        key.__set_column_type(type);
        key.__set_is_key(true);
        TTabletSchema schema;
        schema.__set_short_key_column_count(1);
        schema.__set_schema_hash(3333);
        schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
        schema.__set_storage_type(TStorageType::COLUMN);
        schema.__set_columns({key});
        TCreateTabletReq req;
        req.__set_tablet_schema(schema);
        req.__set_tablet_id(TABLET_ID);
        req.__set_partition_id(PARTITION_ID);
        req.__set_version(10);
        req.__set_enable_unique_key_merge_on_write(true);
        std::vector<DataDir*> dirs {_dir.get()};
        RuntimeProfile profile("CreateTablet");
        ASSERT_TRUE(_engine->tablet_manager()->create_tablet(req, dirs, &profile).ok());
        _tablet = _engine->tablet_manager()->get_tablet(TABLET_ID);
        ASSERT_NE(_tablet, nullptr);
    }

    void TearDown() override {
        if (_engine && _engine->_tablet_publish_txn_thread_pool) {
            _engine->_tablet_publish_txn_thread_pool->shutdown();
        }
        _tablet.reset();
        _engine.reset();
        _dir.reset();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_path).ok());
    }

    Status enqueue(int64_t version = 11, int64_t txn = TXN_ID) {
        return _engine->add_async_publish_task(PARTITION_ID, TABLET_ID, version, txn, false, -1);
    }

    size_t marker_count() {
        size_t count = 0;
        auto st = TabletMetaManager::traverse_pending_publish(
                _dir->get_meta(), [&](int64_t tablet_id, int64_t, std::string_view) {
                    count += tablet_id == TABLET_ID;
                    return true;
                });
        EXPECT_TRUE(st.ok()) << st;
        return count;
    }

    std::shared_ptr<AsyncTabletPublishTask> attempt() {
        return _engine->_async_publish_tasks.at(TABLET_ID).begin()->second.attempt;
    }

    void allow_retry() {
        ASSERT_NE(attempt(), nullptr);
        ASSERT_TRUE(attempt()->finished());
        // No wall-clock sleep: the previous worker has finished and published its state.
        attempt()->_finish_time_ms -= AsyncTabletPublishTask::RETRY_INTERVAL_MS;
    }

    Status commit_empty_rowset(bool install_mow_context = true, int64_t txn = TXN_ID) {
        RowsetMetaPB pb;
        pb.set_rowset_id(0);
        pb.set_rowset_id_v2(_engine->next_rowset_id().to_string());
        pb.set_tablet_id(TABLET_ID);
        pb.set_partition_id(PARTITION_ID);
        pb.set_txn_id(txn);
        pb.set_tablet_schema_hash(_tablet->schema_hash());
        pb.set_rowset_type(BETA_ROWSET);
        pb.set_rowset_state(COMMITTED);
        pb.set_num_segments(0);
        pb.set_num_rows(0);
        pb.set_empty(true);
        pb.set_creation_time(UnixSeconds());
        auto meta = std::make_shared<RowsetMeta>();
        meta->init_from_pb(pb);
        meta->set_tablet_uid(_tablet->tablet_uid());
        meta->set_tablet_schema(_tablet->tablet_schema());
        RowsetSharedPtr rowset;
        RETURN_IF_ERROR(RowsetFactory::create_rowset(_tablet->tablet_schema(),
                                                     _tablet->tablet_path(), meta, &rowset));
        auto guard = _engine->pending_local_rowsets().add(rowset->rowset_id());
        PUniqueId load_id;
        load_id.set_hi(0);
        load_id.set_lo(txn);
        RETURN_IF_ERROR(_engine->txn_manager()->commit_txn(PARTITION_ID, *_tablet, txn, load_id,
                                                           rowset, std::move(guard), false));
        if (install_mow_context) {
            install_mow(txn);
        }
        return Status::OK();
    }

    void install_mow(int64_t txn = TXN_ID) {
        _engine->txn_manager()->set_txn_related_delete_bitmap(
                PARTITION_ID, txn, TABLET_ID, _tablet->tablet_uid(), true,
                std::make_shared<DeleteBitmap>(TABLET_ID), RowsetIdUnorderedSet {}, nullptr);
    }

    void run_attempt() {
        _engine->_process_async_publish();
        _engine->_tablet_publish_txn_thread_pool->wait();
    }

    static constexpr int64_t TABLET_ID = 111;
    static constexpr int64_t PARTITION_ID = 1;
    static constexpr int64_t TXN_ID = 1001;
    std::string _path = "./be/test/storage/test_data/async_publish_recovery_tmp";
    std::unique_ptr<StorageEngine> _engine;
    std::unique_ptr<DataDir> _dir;
    TabletSharedPtr _tablet;
};

TEST_F(AsyncPublishRecoveryTest, MissingTransactionRetainsDurableRequest) {
    ASSERT_TRUE(enqueue().ok());
    run_attempt();
    ASSERT_NE(attempt(), nullptr);
    ASSERT_TRUE(attempt()->finished());
    EXPECT_FALSE(attempt()->result().ok());
    EXPECT_EQ(marker_count(), 1);
    EXPECT_EQ(_engine->get_pending_publish_min_version(TABLET_ID), INT64_MAX);
    auto old_attempt = attempt();
    _engine->_process_async_publish();
    EXPECT_EQ(attempt(), old_attempt); // retry backoff, not a 30ms busy loop
}

TEST_F(AsyncPublishRecoveryTest, PreparedRowsetIsRetriedAfterLocalCommit) {
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(TXN_ID);
    ASSERT_TRUE(_engine->txn_manager()->prepare_txn(PARTITION_ID, *_tablet, TXN_ID, load_id).ok());
    ASSERT_TRUE(enqueue().ok());
    run_attempt();
    ASSERT_TRUE(attempt()->finished());
    EXPECT_FALSE(attempt()->result().ok());
    EXPECT_FALSE(_tablet->check_version_exist({11, 11}));
    EXPECT_EQ(marker_count(), 1);

    ASSERT_TRUE(commit_empty_rowset().ok());
    allow_retry();
    run_attempt();
    ASSERT_TRUE(attempt()->finished());
    EXPECT_TRUE(attempt()->result().ok()) << attempt()->result();
    EXPECT_TRUE(_tablet->check_version_exist({11, 11}));
    // Completion itself does not discard the durable request. The producer acknowledges it.
    EXPECT_EQ(marker_count(), 1);
    _engine->_process_async_publish();
    EXPECT_EQ(marker_count(), 0);
    EXPECT_EQ(_engine->get_pending_publish_min_version(TABLET_ID), INT64_MAX);
}

TEST_F(AsyncPublishRecoveryTest, MowContextMustBeInstalledBeforePublish) {
    ASSERT_TRUE(commit_empty_rowset(false).ok());
    ASSERT_TRUE(enqueue().ok());
    run_attempt();
    ASSERT_TRUE(attempt()->finished());
    EXPECT_FALSE(attempt()->result().ok());
    EXPECT_FALSE(_tablet->check_version_exist({11, 11}));
    install_mow();
    allow_retry();
    run_attempt();
    EXPECT_TRUE(attempt()->result().ok()) << attempt()->result();
    EXPECT_TRUE(_tablet->check_version_exist({11, 11}));
}

TEST_F(AsyncPublishRecoveryTest, DuplicateRegistrationKeepsQueuedAttempt) {
    std::promise<void> release;
    auto ready = release.get_future().share();
    ASSERT_TRUE(
            _engine->_tablet_publish_txn_thread_pool->submit_func([ready] { ready.wait(); }).ok());
    Defer unblock {[&] { release.set_value(); }};
    ASSERT_TRUE(enqueue().ok());
    _engine->_process_async_publish();
    auto first = attempt();
    ASSERT_NE(first, nullptr);
    EXPECT_FALSE(first->finished());
    ASSERT_TRUE(enqueue().ok());
    for (int i = 0; i < 10; ++i) {
        _engine->_process_async_publish();
    }
    EXPECT_EQ(attempt(), first);
    EXPECT_EQ(_engine->get_pending_publish_min_version(TABLET_ID), 11);
    EXPECT_EQ(_engine->_async_publish_tasks.at(TABLET_ID).size(), 1);
    EXPECT_EQ(marker_count(), 1);
}

TEST_F(AsyncPublishRecoveryTest, ThreadPoolRejectionRetainsRequest) {
    _engine->_tablet_publish_txn_thread_pool->shutdown();
    ASSERT_TRUE(enqueue().ok());
    _engine->_process_async_publish();
    ASSERT_NE(attempt(), nullptr);
    ASSERT_TRUE(attempt()->finished());
    EXPECT_FALSE(attempt()->result().ok());
    EXPECT_EQ(marker_count(), 1);
    EXPECT_EQ(_engine->get_pending_publish_min_version(TABLET_ID), INT64_MAX);
}

TEST_F(AsyncPublishRecoveryTest, OutOfOrderVersionWaitsForPredecessor) {
    ASSERT_TRUE(enqueue(12, TXN_ID + 1).ok());
    _engine->_process_async_publish();
    EXPECT_EQ(attempt(), nullptr);
    EXPECT_EQ(marker_count(), 1);
    ASSERT_TRUE(enqueue().ok());
    ASSERT_TRUE(commit_empty_rowset().ok());
    run_attempt();
    EXPECT_TRUE(attempt()->result().ok());
    _engine->_process_async_publish();
    EXPECT_EQ(_engine->get_pending_publish_min_version(TABLET_ID), 12);
    ASSERT_TRUE(commit_empty_rowset(true, TXN_ID + 1).ok());
    run_attempt();
    EXPECT_TRUE(attempt()->result().ok()) << attempt()->result();
    EXPECT_TRUE(_tablet->check_version_exist({12, 12}));
}

TEST_F(AsyncPublishRecoveryTest, RecoverMarkerAfterFailedAttempt) {
    ASSERT_TRUE(enqueue().ok());
    run_attempt();
    EXPECT_EQ(marker_count(), 1);
    // Exercise the same marker replay interface used by DataDir::load(), without a BE restart.
    _engine->_async_publish_tasks.clear();
    ASSERT_TRUE(TabletMetaManager::traverse_pending_publish(
                        _dir->get_meta(),
                        [&](int64_t tablet_id, int64_t version, std::string_view data) {
                            PendingPublishInfoPB info;
                            EXPECT_TRUE(info.ParseFromArray(data.data(),
                                                            static_cast<int>(data.size())));
                            auto st = _engine->add_async_publish_task(
                                    info.partition_id(), tablet_id, version, info.transaction_id(),
                                    true, info.commit_tso());
                            EXPECT_TRUE(st.ok()) << st;
                            return st.ok();
                        })
                        .ok());
    EXPECT_EQ(_engine->get_pending_publish_min_version(TABLET_ID), 11);
    EXPECT_EQ(attempt(), nullptr);
    ASSERT_TRUE(commit_empty_rowset().ok());
    run_attempt();
    EXPECT_TRUE(attempt()->result().ok());
    _engine->_process_async_publish();
    EXPECT_EQ(marker_count(), 0);
}

TEST_F(AsyncPublishRecoveryTest, AlreadyPublishedVersionIsIdempotent) {
    ASSERT_TRUE(commit_empty_rowset().ok());
    AsyncTabletPublishTask first(*_engine, _tablet, PARTITION_ID, TXN_ID, 11, -1);
    first.handle();
    ASSERT_TRUE(first.result().ok()) << first.result();
    // The txn mapping has been removed by publish, but retrying the version is successful.
    ASSERT_TRUE(enqueue().ok());
    AsyncTabletPublishTask duplicate(*_engine, _tablet, PARTITION_ID, TXN_ID, 11, -1);
    duplicate.handle();
    EXPECT_TRUE(duplicate.result().ok()) << duplicate.result();
    _engine->_process_async_publish();
    EXPECT_EQ(marker_count(), 0);
}

TEST_F(AsyncPublishRecoveryTest, PublishBeforeLocalCommitQueuesPreparedMowTablet) {
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(TXN_ID);
    ASSERT_TRUE(_engine->txn_manager()->prepare_txn(PARTITION_ID, *_tablet, TXN_ID, load_id).ok());
    TPartitionVersionInfo partition;
    partition.__set_partition_id(PARTITION_ID);
    partition.__set_version(11);
    partition.__set_commit_tso(-1);
    TPublishVersionRequest request;
    request.__set_transaction_id(TXN_ID);
    request.__set_partition_version_infos({partition});
    std::set<TTabletId> errors;
    std::map<TTabletId, TVersion> successes;
    std::vector<DiscontinuousVersionTablet> discontinuous;
    std::map<TTableId, std::map<TTabletId, int64_t>> delta_rows;
    EnginePublishVersionTask task(*_engine, request, &errors, &successes, &discontinuous,
                                  &delta_rows);
    EXPECT_FALSE(task.execute().ok());
    EXPECT_TRUE(errors.contains(TABLET_ID));
    EXPECT_TRUE(successes.empty());
    EXPECT_EQ(marker_count(), 1);
    ASSERT_TRUE(commit_empty_rowset().ok());
    run_attempt();
    ASSERT_TRUE(attempt()->finished());
    EXPECT_TRUE(attempt()->result().ok()) << attempt()->result();
    EXPECT_TRUE(_tablet->check_version_exist({11, 11}));
}

TEST_F(AsyncPublishRecoveryTest, AbortedPreparedTransactionCannotPublish) {
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(TXN_ID);
    ASSERT_TRUE(_engine->txn_manager()->prepare_txn(PARTITION_ID, *_tablet, TXN_ID, load_id).ok());
    ASSERT_TRUE(enqueue().ok());
    _engine->txn_manager()->abort_txn(PARTITION_ID, TXN_ID, TABLET_ID, _tablet->tablet_uid());
    run_attempt();
    ASSERT_TRUE(attempt()->finished());
    EXPECT_FALSE(attempt()->result().ok());
    EXPECT_FALSE(_tablet->check_version_exist({11, 11}));
    EXPECT_EQ(marker_count(), 1); // failure never claims a successful publish
}

TEST_F(AsyncPublishRecoveryTest, FailedAttemptDoesNotBlockReplicaRepair) {
    ASSERT_TRUE(enqueue().ok());
    ASSERT_TRUE(enqueue(12, TXN_ID + 1).ok());
    EXPECT_EQ(_engine->get_pending_publish_min_version(TABLET_ID), 11);
    run_attempt();
    ASSERT_TRUE(attempt()->finished());
    ASSERT_FALSE(attempt()->result().ok());
    // Version 11 has no local rowset. Clone may repair it up to the next pending
    // version's predecessor, without discarding the local recovery intention.
    EXPECT_EQ(_engine->get_pending_publish_min_version(TABLET_ID), 12);
    EXPECT_EQ(marker_count(), 2);
    EXPECT_EQ(_engine->_async_publish_tasks.at(TABLET_ID).size(), 2);
}

} // namespace doris
