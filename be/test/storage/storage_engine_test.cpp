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

#include <chrono>
#include <filesystem>
#include <memory>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "storage/data_dir.h"
#include "storage/data_dir_sweep_worker.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta_manager.h"
#include "util/countdown_latch.h"
#include "util/thread.h"
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

    std::unique_ptr<StorageEngine> _storage_engine;
    std::string _engine_data_path;
    std::unique_ptr<DataDir> _data_dir;
};

TEST_F(StorageEngineTest, TestBrokenDisk) {
    DEFINE_mString(broken_storage_path, "");
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

TEST_F(StorageEngineTest, TrashSweepDoesNotStartAfterEngineStop) {
    _storage_engine->stop();

    auto status = _storage_engine->start_trash_sweep(nullptr);
    EXPECT_TRUE(status.is<ErrorCode::CANCELLED>()) << status;
}

TEST_F(StorageEngineTest, StopWaitsForCoordinatorBeforeDrainingSweepWorkers) {
    auto worker = std::make_unique<DataDirSweepWorker>(*_storage_engine, _data_dir.get());
    auto* worker_ptr = worker.get();
    ASSERT_TRUE(worker->start().ok());
    _storage_engine->_data_dir_sweep_workers.emplace(_data_dir.get(), std::move(worker));

    constexpr uint64_t sweep_epoch = 19;
    auto in_flight_context = std::make_shared<DataDirSweepPhaseContext>(sweep_epoch, 1);
    CountDownLatch job_entered(1);
    CountDownLatch release_job(1);
    auto owner = std::make_shared<int>(1);
    TabletSharedPtr tablet(owner, reinterpret_cast<Tablet*>(owner.get()));
    DataDirSweepJob in_flight_job;
    in_flight_job.sweep_epoch = sweep_epoch;
    in_flight_job.type = DataDirSweepJobType::SHUTDOWN_TABLET_MOVE;
    in_flight_job.data_dir = _data_dir.get();
    in_flight_job.payload = ShutdownTabletMovePayload {.tablets = {std::move(tablet)},
                                                       .move_tablet = [&](const TabletSharedPtr&) {
                                                           job_entered.count_down();
                                                           release_job.wait();
                                                           return true;
                                                       }};
    in_flight_job.context = in_flight_context;
    in_flight_job.result_index = 0;
    auto submit_status = worker_ptr->submit(std::move(in_flight_job));
    if (!submit_status.ok()) {
        ADD_FAILURE() << submit_status;
        _storage_engine->stop();
        return;
    }
    if (!job_entered.wait_for(std::chrono::seconds(5))) {
        ADD_FAILURE() << "sweep worker did not start the in-flight job";
        release_job.count_down();
        _storage_engine->stop();
        return;
    }

    auto coordinator_status = Thread::create(
            "StorageEngineTest", "mock_sweep_coordinator",
            [in_flight_context]() { in_flight_context->completion_latch.wait(); },
            &_storage_engine->_garbage_sweeper_thread);
    if (!coordinator_status.ok()) {
        ADD_FAILURE() << coordinator_status;
        release_job.count_down();
        _storage_engine->stop();
        return;
    }

    CountDownLatch stop_started(1);
    CountDownLatch stop_completed(1);
    std::thread stop_thread([&] {
        stop_started.count_down();
        _storage_engine->stop();
        stop_completed.count_down();
    });
    EXPECT_TRUE(stop_started.wait_for(std::chrono::seconds(5)));
    const auto stop_request_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (_storage_engine->_stop_background_threads_latch.count() != 0 &&
           std::chrono::steady_clock::now() < stop_request_deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(_storage_engine->_stop_background_threads_latch.count(), 0);
    EXPECT_FALSE(stop_completed.wait_for(std::chrono::milliseconds(100)));

    auto followup_context = std::make_shared<DataDirSweepPhaseContext>(sweep_epoch, 1);
    DataDirSweepJob followup_job;
    followup_job.sweep_epoch = sweep_epoch;
    followup_job.type = DataDirSweepJobType::TRASH_CAPACITY_REFRESH;
    followup_job.data_dir = _data_dir.get();
    followup_job.payload = TrashCapacityRefreshPayload {};
    followup_job.context = followup_context;
    followup_job.result_index = 0;
    auto followup_submit_status = worker_ptr->submit(std::move(followup_job));
    EXPECT_TRUE(followup_submit_status.ok()) << followup_submit_status;

    release_job.count_down();
    EXPECT_TRUE(stop_completed.wait_for(std::chrono::seconds(5)));
    stop_thread.join();

    EXPECT_EQ(in_flight_context->results[0].shutdown_resolved, 1);
    if (followup_submit_status.ok()) {
        followup_context->completion_latch.wait();
        EXPECT_TRUE(followup_context->results[0].status.ok())
                << followup_context->results[0].status;
    }
    EXPECT_TRUE(_storage_engine->_data_dir_sweep_workers.empty());
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
