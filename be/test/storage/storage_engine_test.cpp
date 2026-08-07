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
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "cpp/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "storage/data_dir.h"
#include "storage/data_dir_sweep_worker.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_meta_manager.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"
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
        _shutdown_backlog_base =
                _storage_engine->tablet_manager()->_shutdown_tablet_backlog_value();
    }

    virtual void TearDown() {
        if (!_storage_engine->_data_dir_sweep_workers.empty()) {
            _storage_engine->_stop_data_dir_sweep_workers();
        }
        auto* tablet_manager = _storage_engine->tablet_manager();
        {
            std::lock_guard<std::shared_mutex> wrlock(tablet_manager->_shutdown_tablets_lock);
            tablet_manager->_shutdown_tablets.clear();
        }
        tablet_manager->_adjust_shutdown_tablet_backlog(
                _shutdown_backlog_base - tablet_manager->_shutdown_tablet_backlog_value());
        detach_data_dir();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_engine_data_path).ok());
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    DataDir* attach_data_dir() {
        DORIS_CHECK(_data_dir != nullptr);
        auto* data_dir = _data_dir.get();
        auto [_, inserted] =
                _storage_engine->_store_map.emplace(_engine_data_path, std::move(_data_dir));
        DORIS_CHECK(inserted);
        return data_dir;
    }

    void detach_data_dir() {
        auto data_dir_it = _storage_engine->_store_map.find(_engine_data_path);
        if (data_dir_it == _storage_engine->_store_map.end()) {
            return;
        }
        DORIS_CHECK(_data_dir == nullptr);
        _data_dir = std::move(data_dir_it->second);
        _storage_engine->_store_map.erase(data_dir_it);
    }

    TabletSharedPtr create_shutdown_tablet(DataDir* data_dir, int64_t tablet_id) {
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_tablet_id = tablet_id;
        static_cast<void>(tablet_meta->set_partition_id(10000));
        tablet_meta->set_tablet_uid({tablet_id, 0});
        tablet_meta->set_shard_id(tablet_id % 4);
        tablet_meta->_schema_hash = tablet_id;
        return std::make_shared<Tablet>(*_storage_engine, std::move(tablet_meta), data_dir);
    }

    void reset_shutdown_tablets(std::vector<TabletSharedPtr> tablets) {
        auto* tablet_manager = _storage_engine->tablet_manager();
        std::lock_guard<std::shared_mutex> wrlock(tablet_manager->_shutdown_tablets_lock);
        const int64_t old_size = tablet_manager->_shutdown_tablets.size();
        tablet_manager->_shutdown_tablets.clear();
        for (auto& tablet : tablets) {
            tablet_manager->_shutdown_tablets.push_back(std::move(tablet));
        }
        tablet_manager->_adjust_shutdown_tablet_backlog(static_cast<int64_t>(tablets.size()) -
                                                        old_size);
    }

    std::vector<Tablet*> list_shutdown_tablets() {
        auto* tablet_manager = _storage_engine->tablet_manager();
        std::shared_lock<std::shared_mutex> rdlock(tablet_manager->_shutdown_tablets_lock);
        std::vector<Tablet*> tablets;
        tablets.reserve(tablet_manager->_shutdown_tablets.size());
        for (const auto& tablet : tablet_manager->_shutdown_tablets) {
            tablets.push_back(tablet.get());
        }
        return tablets;
    }

    std::unique_ptr<StorageEngine> _storage_engine;
    std::string _engine_data_path;
    std::unique_ptr<DataDir> _data_dir;
    int64_t _shutdown_backlog_base = 0;
};

TEST_F(StorageEngineTest, TestBrokenDisk) {
    // ConfigTest clears the shared registry. Re-register this field with process-lifetime storage
    // instead of defining a stack-backed config value.
    config::Register broken_storage_path_register("std::string", "broken_storage_path",
                                                  &config::broken_storage_path, "", true);
    static_cast<void>(broken_storage_path_register);
    const std::string original_broken_storage_path = config::broken_storage_path;
    Defer restore_broken_storage_path {[&] {
        auto status = config::set_config("broken_storage_path", original_broken_storage_path, true);
        EXPECT_TRUE(status.ok()) << status;
    }};
    std::string path = config::custom_config_dir + "/be_custom.conf";

    std::error_code ec;
    {
        _storage_engine->add_broken_path("broken_path1");
        EXPECT_EQ(std::filesystem::exists(path, ec), true);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path1"), 1);
        EXPECT_EQ(config::broken_storage_path, "broken_path1;");
    }

    {
        _storage_engine->add_broken_path("broken_path2");
        EXPECT_EQ(std::filesystem::exists(path, ec), true);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path1"), 1);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path2"), 1);
        EXPECT_EQ(config::broken_storage_path, "broken_path1;broken_path2;");
    }

    {
        _storage_engine->add_broken_path("broken_path2");
        EXPECT_EQ(std::filesystem::exists(path, ec), true);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path1"), 1);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path2"), 1);
        EXPECT_EQ(config::broken_storage_path, "broken_path1;broken_path2;");
    }

    {
        _storage_engine->remove_broken_path("broken_path2");
        EXPECT_EQ(std::filesystem::exists(path, ec), true);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path1"), 1);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path2"), 0);
        EXPECT_EQ(config::broken_storage_path, "broken_path1;");
    }
}

TEST_F(StorageEngineTest, TrashSweepDoesNotStartAfterEngineStop) {
    _storage_engine->stop();

    auto status = _storage_engine->start_trash_sweep(nullptr);
    EXPECT_TRUE(status.is<ErrorCode::CANCELLED>()) << status;
}

TEST_F(StorageEngineTest, StopWaitsForCoordinatorBeforeDrainingSweepWorkers) {
    const bool enable_worker = config::enable_data_dir_sweep_worker;
    config::enable_data_dir_sweep_worker = true;
    Defer restore_config {[&] { config::enable_data_dir_sweep_worker = enable_worker; }};

    auto* data_dir = attach_data_dir();
    ASSERT_TRUE(_storage_engine->_start_data_dir_sweep_workers().ok());
    auto shutdown_tablet = create_shutdown_tablet(data_dir, 19001);
    reset_shutdown_tablets({std::move(shutdown_tablet)});

    CountDownLatch job_entered(1);
    CountDownLatch release_job(1);
    std::vector<DataDirSweepJobType> executed_jobs;
    std::mutex executed_jobs_lock;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard callback_guard;
    sync_point->set_call_back(
            "StorageEngine::_execute_data_dir_sweep_job",
            [&](auto&& args) {
                auto* job = try_any_cast<DataDirSweepJob*>(args[0]);
                {
                    std::lock_guard lock(executed_jobs_lock);
                    executed_jobs.push_back(job->type);
                }
                if (job->type == DataDirSweepJobType::SHUTDOWN_TABLET_MOVE) {
                    job_entered.count_down();
                    release_job.wait();
                }
            },
            &callback_guard);
    sync_point->enable_processing();
    Defer disable_sync_point {[&] { sync_point->disable_processing(); }};

    Status sweep_status;
    CountDownLatch sweep_completed(1);
    std::thread sweep_thread([&] {
        sweep_status = _storage_engine->start_trash_sweep(nullptr);
        sweep_completed.count_down();
    });
    if (!job_entered.wait_for(std::chrono::seconds(5))) {
        ADD_FAILURE() << "start_trash_sweep did not reach the shutdown-tablet phase";
        release_job.count_down();
        sweep_thread.join();
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

    release_job.count_down();
    EXPECT_TRUE(sweep_completed.wait_for(std::chrono::seconds(5)));
    EXPECT_TRUE(stop_completed.wait_for(std::chrono::seconds(5)));
    sweep_thread.join();
    stop_thread.join();

    EXPECT_TRUE(sweep_status.ok()) << sweep_status;
    EXPECT_EQ(executed_jobs,
              std::vector<DataDirSweepJobType>(
                      {DataDirSweepJobType::SNAPSHOT_SWEEP, DataDirSweepJobType::TRASH_SWEEP,
                       DataDirSweepJobType::SHUTDOWN_TABLET_MOVE, DataDirSweepJobType::REMOTE_GC,
                       DataDirSweepJobType::TRASH_CAPACITY_REFRESH}));
    EXPECT_TRUE(list_shutdown_tablets().empty());
    EXPECT_TRUE(_storage_engine->_data_dir_sweep_workers.empty());
}

TEST_F(StorageEngineTest, TrashSweepUsesSynchronousFallbackWhenWorkerDisabled) {
    const bool enable_worker = config::enable_data_dir_sweep_worker;
    config::enable_data_dir_sweep_worker = false;
    Defer restore_config {[&] { config::enable_data_dir_sweep_worker = enable_worker; }};

    auto* data_dir = attach_data_dir();
    auto shutdown_tablet = create_shutdown_tablet(data_dir, 20001);
    reset_shutdown_tablets({std::move(shutdown_tablet)});

    std::vector<DataDirSweepJobType> executed_jobs;
    std::vector<std::thread::id> execution_threads;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard callback_guard;
    sync_point->set_call_back(
            "StorageEngine::_execute_data_dir_sweep_job",
            [&](auto&& args) {
                auto* job = try_any_cast<DataDirSweepJob*>(args[0]);
                executed_jobs.push_back(job->type);
                execution_threads.push_back(std::this_thread::get_id());
            },
            &callback_guard);
    sync_point->enable_processing();
    Defer disable_sync_point {[&] { sync_point->disable_processing(); }};

    const auto coordinator_thread = std::this_thread::get_id();
    auto sweep_status = _storage_engine->start_trash_sweep(nullptr);

    EXPECT_TRUE(sweep_status.ok()) << sweep_status;
    EXPECT_EQ(executed_jobs,
              std::vector<DataDirSweepJobType>(
                      {DataDirSweepJobType::SNAPSHOT_SWEEP, DataDirSweepJobType::TRASH_SWEEP,
                       DataDirSweepJobType::SHUTDOWN_TABLET_MOVE, DataDirSweepJobType::REMOTE_GC,
                       DataDirSweepJobType::TRASH_CAPACITY_REFRESH}));
    EXPECT_EQ(execution_threads,
              std::vector<std::thread::id>(execution_threads.size(), coordinator_thread));
    EXPECT_TRUE(list_shutdown_tablets().empty());
    EXPECT_TRUE(_storage_engine->_data_dir_sweep_workers.empty());
}

TEST_F(StorageEngineTest, ShutdownSweepUsesWorkerAfterDataDirBecomesUnused) {
    const bool enable_worker = config::enable_data_dir_sweep_worker;
    config::enable_data_dir_sweep_worker = true;
    Defer restore_config {[&] { config::enable_data_dir_sweep_worker = enable_worker; }};

    auto* data_dir = attach_data_dir();
    ASSERT_TRUE(_storage_engine->_start_data_dir_sweep_workers().ok());

    const std::string test_file_path = data_dir->path() + "/" + kTestFilePath;
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(test_file_path + "/nested").ok());
    Defer restore_broken_path {[&] { _storage_engine->remove_broken_path(data_dir->path()); }};
    data_dir->health_check();
    ASSERT_FALSE(data_dir->is_used());
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(test_file_path).ok());

    auto shutdown_tablet = create_shutdown_tablet(data_dir, 20501);
    reset_shutdown_tablets({std::move(shutdown_tablet)});

    std::vector<DataDirSweepJobType> executed_jobs;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard callback_guard;
    sync_point->set_call_back(
            "StorageEngine::_execute_data_dir_sweep_job",
            [&](auto&& args) {
                auto* job = try_any_cast<DataDirSweepJob*>(args[0]);
                executed_jobs.push_back(job->type);
            },
            &callback_guard);
    sync_point->enable_processing();
    Defer disable_sync_point {[&] { sync_point->disable_processing(); }};

    auto sweep_status = _storage_engine->start_trash_sweep(nullptr);

    EXPECT_TRUE(sweep_status.ok()) << sweep_status;
    EXPECT_EQ(executed_jobs,
              std::vector<DataDirSweepJobType>({DataDirSweepJobType::SHUTDOWN_TABLET_MOVE}));
    EXPECT_TRUE(list_shutdown_tablets().empty());
    EXPECT_NE(_storage_engine->_data_dir_sweep_workers.find(data_dir),
              _storage_engine->_data_dir_sweep_workers.end());
}

TEST_F(StorageEngineTest, TrashSweepRequeuesShutdownTabletsWhenWorkerSubmitFails) {
    const bool enable_worker = config::enable_data_dir_sweep_worker;
    config::enable_data_dir_sweep_worker = true;
    Defer restore_config {[&] { config::enable_data_dir_sweep_worker = enable_worker; }};

    auto* data_dir = attach_data_dir();
    ASSERT_TRUE(_storage_engine->_start_data_dir_sweep_workers().ok());
    _storage_engine->_data_dir_sweep_workers.at(data_dir)->stop_accepting_jobs();
    auto shutdown_tablet = create_shutdown_tablet(data_dir, 21001);
    auto* shutdown_tablet_ptr = shutdown_tablet.get();
    reset_shutdown_tablets({std::move(shutdown_tablet)});
    const int64_t completed_jobs = data_dir->disks_sweep_worker_completed_jobs->value();
    const int64_t failed_jobs = data_dir->disks_sweep_worker_failed_jobs->value();

    auto sweep_status = _storage_engine->start_trash_sweep(nullptr);

    EXPECT_FALSE(sweep_status.ok());
    EXPECT_EQ(list_shutdown_tablets(), std::vector<Tablet*>({shutdown_tablet_ptr}));
    EXPECT_EQ(data_dir->disks_sweep_worker_completed_jobs->value(), completed_jobs + 5);
    EXPECT_EQ(data_dir->disks_sweep_worker_failed_jobs->value(), failed_jobs + 5);
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
