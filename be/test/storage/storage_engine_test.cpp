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
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string_view>
#include <unordered_map>

#include "common/status.h"
#include "cpp/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "storage/data_dir.h"
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

#if !defined(THREAD_SANITIZER)

class StorageEngineWatchdogDeathTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_style = ::testing::FLAGS_gtest_death_test_style;
        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
        _engine_data_path = std::string("./be/test/storage/test_data/watchdog/") +
                            ::testing::UnitTest::GetInstance()->current_test_info()->name();
    }

    void TearDown() override {
        ::testing::FLAGS_gtest_death_test_style = _saved_style;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_engine_data_path).ok());
    }

    // Called inside the re-executed child, so no StorageEngine or RocksDB state
    // is inherited from a multithreaded parent process.
    std::unique_ptr<StorageEngine> create_engine(
            bool with_data_dir = true, std::chrono::seconds timeout = std::chrono::seconds(1)) {
        signal(SIGALRM, SIG_DFL);
        alarm(10);
        EngineOptions options;
        options.backend_uid = UniqueId::gen_uid();
        auto engine = std::make_unique<StorageEngine>(options);
        if (with_data_dir) {
            auto fs = io::global_local_filesystem();
            if (!fs->create_directory(_engine_data_path).ok()) {
                _exit(2);
            }
            auto data_dir = std::make_unique<DataDir>(*engine, _engine_data_path, 100000000);
            // A health probe does not require a RocksDB metadata database.
            if (!data_dir->init(false).ok()) {
                _exit(2);
            }
            engine->_store_map.emplace(_engine_data_path, std::move(data_dir));
        }
        if (!engine->_disk_health_check_watchdog.start(timeout).ok()) {
            _exit(2);
        }
        return engine;
    }

    std::string _engine_data_path;

private:
    std::string _saved_style;
};

TEST_F(StorageEngineWatchdogDeathTest, ExitsWhenRuntimeProbeHangs) {
    ASSERT_EXIT(
            {
                auto engine = create_engine();
                auto* sync_point = SyncPoint::get_instance();
                sync_point->set_call_back("LocalFileSystem::create_file_impl", [](auto&&) {
                    while (true) {
                        pause();
                    }
                });
                sync_point->enable_processing();
                engine->_disk_stat_monitor_thread_callback();
                _exit(3);
            },
            ::testing::ExitedWithCode(DiskHealthCheckWatchdog::kTimeoutExitCode), "");
}

TEST_F(StorageEngineWatchdogDeathTest, ExitsWhenBrokenPathPersistenceHangs) {
    ASSERT_EXIT(
            {
                auto engine = create_engine();
                config::custom_config_dir = _engine_data_path;
                auto* sync_point = SyncPoint::get_instance();
                bool inject_error = true;
                sync_point->set_call_back(
                        "LocalFileSystem::create_file_impl", [&inject_error](auto&& args) {
                            if (inject_error) {
                                inject_error = false;
                                auto* ret = try_any_cast_ret<Status>(args);
                                ret->first = Status::IOError("injected health probe failure");
                                ret->second = true;
                                return;
                            }
                            // The probe already returned EIO. Its error handler is
                            // now creating be_custom.conf.tmp to persist the bad path.
                            constexpr char marker[] = "persisting broken disk path\n";
                            static_cast<void>(write(STDERR_FILENO, marker, sizeof(marker) - 1));
                            while (true) {
                                pause();
                            }
                        });
                sync_point->enable_processing();
                engine->_disk_stat_monitor_thread_callback();
                _exit(3);
            },
            ::testing::ExitedWithCode(DiskHealthCheckWatchdog::kTimeoutExitCode),
            "persisting broken disk path");
}

TEST_F(StorageEngineWatchdogDeathTest, StopKeepsBlockedProbeSupervised) {
    ASSERT_EXIT(
            {
                auto engine = create_engine();
                CountDownLatch probe_entered(1);
                auto* sync_point = SyncPoint::get_instance();
                sync_point->set_call_back("LocalFileSystem::create_file_impl", [&](auto&&) {
                    probe_entered.count_down();
                    engine->_stop_background_threads_latch.wait();
                    // Require evidence that stop() reached its join phase before
                    // the timeout, rather than merely timing out a running probe.
                    constexpr char marker[] = "stop waiting for disk probe\n";
                    static_cast<void>(write(STDERR_FILENO, marker, sizeof(marker) - 1));
                    while (true) {
                        pause();
                    }
                });
                sync_point->enable_processing();
                if (!Thread::create(
                             "StorageEngineTest", "disk_stat_monitor_thread",
                             [&] { engine->_disk_stat_monitor_thread_callback(); },
                             &engine->_disk_stat_monitor_thread)
                             .ok()) {
                    _exit(2);
                }
                probe_entered.wait();
                engine->stop();
                _exit(3);
            },
            ::testing::ExitedWithCode(DiskHealthCheckWatchdog::kTimeoutExitCode),
            "stop waiting for disk probe");
}

TEST_F(StorageEngineWatchdogDeathTest, ExitsWhenMonitorWaitsForStoreLock) {
    ASSERT_EXIT(
            {
                auto engine = create_engine(false);
                std::lock_guard<std::mutex> lock(engine->_store_lock);
                if (!Thread::create(
                             "StorageEngineTest", "disk_stat_monitor_thread",
                             [&] { engine->_start_disk_stat_monitor(); },
                             &engine->_disk_stat_monitor_thread)
                             .ok()) {
                    _exit(2);
                }
                engine->_disk_stat_monitor_thread->join();
                _exit(3);
            },
            ::testing::ExitedWithCode(DiskHealthCheckWatchdog::kTimeoutExitCode), "");
}

TEST_F(StorageEngineWatchdogDeathTest, PreservesHealthyAndIoErrorResults) {
    ASSERT_EXIT(
            {
                auto engine = create_engine(true, std::chrono::seconds(30));
                config::custom_config_dir = _engine_data_path;
                config::max_percentage_of_error_disk = 100;
                auto* data_dir = engine->_store_map.at(_engine_data_path).get();
                engine->_start_disk_stat_monitor();
                if (!data_dir->is_used() || !engine->get_broken_paths().empty()) {
                    _exit(3);
                }
                auto* sync_point = SyncPoint::get_instance();
                bool inject_error = true;
                sync_point->set_call_back(
                        "LocalFileSystem::create_file_impl", [&inject_error](auto&& args) {
                            // Fail the probe only; the subsequent config write must succeed.
                            if (inject_error) {
                                inject_error = false;
                                auto* ret = try_any_cast_ret<Status>(args);
                                ret->first = Status::IOError("injected health probe failure");
                                ret->second = true;
                            }
                        });
                sync_point->enable_processing();
                engine->_start_disk_stat_monitor();
                if (data_dir->is_used() ||
                    !engine->get_broken_paths().contains(_engine_data_path)) {
                    _exit(3);
                }
                sync_point->disable_processing();
                if (config::broken_storage_path != _engine_data_path + ";") {
                    _exit(3);
                }
                std::ifstream persisted_config(_engine_data_path + "/be_custom.conf");
                std::string line;
                bool persisted_path = false;
                while (std::getline(persisted_config, line)) {
                    if (line.find("broken_storage_path") != std::string::npos &&
                        line.find(_engine_data_path + ";") != std::string::npos) {
                        persisted_path = true;
                    }
                }
                if (!persisted_path) {
                    _exit(3);
                }
                engine->stop();
                _exit(0);
            },
            ::testing::ExitedWithCode(0), "");
}

#endif

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
