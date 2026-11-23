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

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <unordered_map>

#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "storage/compaction/cumulative_compaction_policy.h"
#include "storage/data_dir.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_manager.h"
#include "util/threadpool.h"

namespace doris {
using namespace config;

class CompactionTaskTest : public testing::Test {
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

static RowsetSharedPtr create_rowset(Version version, int num_segments, bool overlapping,
                                     int data_size) {
    auto rs_meta = std::make_shared<RowsetMeta>();
    rs_meta->set_rowset_type(BETA_ROWSET); // important
    rs_meta->_rowset_meta_pb.set_start_version(version.first);
    rs_meta->_rowset_meta_pb.set_end_version(version.second);
    rs_meta->set_num_segments(num_segments);
    rs_meta->set_segments_overlap(overlapping ? OVERLAPPING : NONOVERLAPPING);
    rs_meta->set_total_disk_size(data_size);
    RowsetSharedPtr rowset;
    Status st = RowsetFactory::create_rowset(nullptr, "", std::move(rs_meta), &rowset);
    if (!st.ok()) {
        return nullptr;
    }
    return rowset;
}

TEST_F(CompactionTaskTest, TestSubmitCompactionTask) {
    auto st = ThreadPoolBuilder("BaseCompactionTaskThreadPool")
                      .set_min_threads(2)
                      .set_max_threads(2)
                      .build(&_storage_engine->_base_compaction_thread_pool);
    EXPECT_TRUE(st.ok());
    st = ThreadPoolBuilder("CumuCompactionTaskThreadPool")
                 .set_min_threads(2)
                 .set_max_threads(2)
                 .build(&_storage_engine->_cumu_compaction_thread_pool);
    EXPECT_TRUE(st.ok());

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("olap_server::execute_compaction", [](auto&& values) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        bool* pred = try_any_cast<bool*>(values.back());
        *pred = true;
    });

    for (int tablet_cnt = 0; tablet_cnt < 10; ++tablet_cnt) {
        TabletMetaSharedPtr tablet_meta;
        tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                         UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                         TCompressionType::LZ4F));
        TabletSharedPtr tablet(new Tablet(*(_storage_engine.get()), tablet_meta, _data_dir.get(),
                                          CUMULATIVE_SIZE_BASED_POLICY));
        st = tablet->init();
        EXPECT_TRUE(st.ok());

        for (int i = 2; i < 30; ++i) {
            RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
            tablet->_rs_version_map.emplace(rs->version(), rs);
        }
        tablet->_cumulative_point = 2;

        st = _storage_engine->_submit_compaction_task(tablet, CompactionType::CUMULATIVE_COMPACTION,
                                                      false);
        EXPECT_TRUE(st.ok());
    }

    int executing_task_num =
            _storage_engine->_compaction_submit_registry.count_executing_cumu_and_base(
                    _data_dir.get());
    EXPECT_EQ(executing_task_num, 2);
}

TEST_F(CompactionTaskTest, AutomaticCumulativeReachesWorkerWhileBaseIsSaturated) {
    const auto base = CompactionType::BASE_COMPACTION;
    const auto cumulative = CompactionType::CUMULATIVE_COMPACTION;
    const auto old_budget = config::total_permits_for_compaction_score;
    const auto old_rounds = config::cumulative_compaction_rounds_for_each_base_compaction_round;
    const auto old_interval = config::generate_compaction_tasks_interval_ms;
    const bool old_disabled = config::disable_auto_compaction;
    const bool old_memory_pause = config::enable_compaction_pause_on_high_memory;
    Defer restore([&] {
        config::total_permits_for_compaction_score = old_budget;
        config::cumulative_compaction_rounds_for_each_base_compaction_round = old_rounds;
        config::generate_compaction_tasks_interval_ms = old_interval;
        config::disable_auto_compaction = old_disabled;
        config::enable_compaction_pause_on_high_memory = old_memory_pause;
    });
    config::total_permits_for_compaction_score = 4;
    config::cumulative_compaction_rounds_for_each_base_compaction_round = 1;
    config::generate_compaction_tasks_interval_ms = 5;
    config::disable_auto_compaction = false;
    config::enable_compaction_pause_on_high_memory = false;

    _storage_engine->_compaction_admission = std::make_unique<CompactionAdmission>(2);
    ASSERT_TRUE(ThreadPoolBuilder("IsolationBasePool")
                        .set_min_threads(1)
                        .set_max_threads(1)
                        .build(&_storage_engine->_base_compaction_thread_pool)
                        .ok());
    ASSERT_TRUE(ThreadPoolBuilder("IsolationCumulativePool")
                        .set_min_threads(1)
                        .set_max_threads(1)
                        .build(&_storage_engine->_cumu_compaction_thread_pool)
                        .ok());

    const auto make_tablet = [&](int64_t id, int cumulative_point) {
        auto meta = std::make_shared<TabletMeta>(1, 2, id, 15674, 4, 5, TTabletSchema(), 6,
                                                 std::unordered_map<uint32_t, uint32_t> {},
                                                 UniqueId(9, id), TTabletType::TABLET_TYPE_DISK,
                                                 TCompressionType::LZ4F);
        auto tablet = std::make_shared<Tablet>(*_storage_engine, meta, _data_dir.get(),
                                               CUMULATIVE_SIZE_BASED_POLICY);
        EXPECT_TRUE(tablet->init().ok());
        for (int i = 2; i < 30; ++i) {
            auto rowset = create_rowset({i, i}, 1, false, 1024);
            tablet->_rs_version_map.emplace(rowset->version(), rowset);
        }
        tablet->_cumulative_point = cumulative_point;
        return tablet;
    };
    auto slow_base = make_tablet(15673, 25);
    auto next_base = make_tablet(15674, 25);
    auto next_cumu = make_tablet(15675, 2);

    std::promise<void> base_started, release_base, cumulative_started;
    auto base_done = release_base.get_future();
    std::atomic<int> base_runs {0};
    bool base_round_seen = false;
    bool cumulative_scheduled = false;
    auto* sp = SyncPoint::get_instance();
    sp->set_call_back("StorageEngine::_adjust_compaction_thread_num.return_void",
                      [](auto&& values) { *try_any_cast<bool*>(values.back()) = true; });
    sp->set_call_back("olap_server::_generate_compaction_tasks.return_empty", [&](auto&& values) {
        auto type = try_any_cast<CompactionType>(values.front());
        auto* ret = try_any_cast_ret<std::vector<TabletCompactionContext>>(values);
        ret->second = true;
        if (type == base) {
            base_round_seen = true;
            ret->first = {{next_base, 4}};
        } else if (type == cumulative && base_round_seen && !cumulative_scheduled) {
            cumulative_scheduled = true;
            ret->first = {{next_cumu, 4}};
        }
    });
    sp->set_call_back("olap_server::execute_compaction", [&](auto&& values) {
        auto type = try_any_cast<CompactionType>(values.front());
        *try_any_cast<bool*>(values.back()) = true; // The fixture has synthetic rowset metadata.
        if (type == base) {
            if (++base_runs == 1) {
                base_started.set_value();
                base_done.wait();
            }
        } else {
            cumulative_started.set_value();
        }
    });
    sp->enable_processing();
    Defer cleanup([&] {
        release_base.set_value();
        _storage_engine->stop();
        sp->clear_all_call_backs();
        sp->disable_processing();
    });

    ASSERT_TRUE(_storage_engine->_submit_compaction_task(slow_base, base, false).ok());
    ASSERT_EQ(base_started.get_future().wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    ASSERT_TRUE(Thread::create(
                        "StorageEngine", "isolation_test_producer",
                        [&] { _storage_engine->_compaction_tasks_producer_callback(); },
                        &_storage_engine->_compaction_tasks_producer_thread)
                        .ok());
    EXPECT_EQ(cumulative_started.get_future().wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    _storage_engine->_cumu_compaction_thread_pool->wait();
    EXPECT_EQ(_storage_engine->_base_compaction_thread_pool->get_queue_size(), 0);
    EXPECT_EQ(base_runs.load(), 1);
}

TEST_F(CompactionTaskTest, ChangelogCompactionProducerIgnoresCcrSwitch) {
    const bool old_enable_feature_binlog = config::enable_feature_binlog;
    const bool old_disable_auto_compaction = config::disable_auto_compaction;
    const bool old_enable_compaction_pause_on_high_memory =
            config::enable_compaction_pause_on_high_memory;
    Defer restore_config {[&]() {
        config::enable_feature_binlog = old_enable_feature_binlog;
        config::disable_auto_compaction = old_disable_auto_compaction;
        config::enable_compaction_pause_on_high_memory = old_enable_compaction_pause_on_high_memory;
    }};

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    int generation_count = 0;
    sp->set_call_back("StorageEngine::_adjust_compaction_thread_num.return_void",
                      [](auto&& args) { *try_any_cast<bool*>(args.back()) = true; });
    sp->set_call_back("olap_server::_generate_compaction_tasks.return_empty",
                      [&generation_count](auto&& values) {
                          ++generation_count;
                          auto* ret =
                                  try_any_cast_ret<std::vector<TabletCompactionContext>>(values);
                          ret->second = true;
                      });
    Defer clear_sync_points {[&]() {
        sp->clear_all_call_backs();
        sp->disable_processing();
    }};

    config::enable_feature_binlog = false;
    config::disable_auto_compaction = false;
    config::enable_compaction_pause_on_high_memory = false;
    _storage_engine->_stop_background_threads_latch.count_down();
    _storage_engine->_changelog_compaction_tasks_producer_callback();
    EXPECT_EQ(generation_count, 1);

    config::disable_auto_compaction = true;
    _storage_engine->_changelog_compaction_tasks_producer_callback();
    EXPECT_EQ(generation_count, 1);
}

TEST_F(CompactionTaskTest, TestAutoSetCompactionIncreaseTaskNum) {
    auto st = ThreadPoolBuilder("BaseCompactionTaskThreadPool")
                      .set_min_threads(2)
                      .set_max_threads(2)
                      .build(&_storage_engine->_base_compaction_thread_pool);
    EXPECT_TRUE(st.ok());
    st = ThreadPoolBuilder("CumuCompactionTaskThreadPool")
                 .set_min_threads(2)
                 .set_max_threads(2)
                 .build(&_storage_engine->_cumu_compaction_thread_pool);
    EXPECT_TRUE(st.ok());
    config::disable_auto_compaction = false;

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("olap_server::_generate_compaction_tasks.return_empty", [](auto&& values) {
        auto* ret = try_any_cast_ret<std::vector<TabletCompactionContext>>(values);
        ret->second = true;
    });
    sp->set_call_back("StorageEngine::_adjust_compaction_thread_num.return_void",
                      [](auto&& args) { *try_any_cast<bool*>(args.back()) = true; });
    sp->set_call_back("StorageEngine::_compaction_tasks_producer_callback",
                      [](auto&& values) { std::this_thread::sleep_for(std::chrono::seconds(1)); });

    Defer defer {[&]() {
        _storage_engine->_stop_background_threads_latch.count_down();
        sp->clear_all_call_backs();
    }};

    config::generate_compaction_tasks_interval_ms = 1000;
    {
        // queue size 1
        // task num 1->1
        _storage_engine->_cumu_compaction_thread_pool->_total_queued_tasks = 1;
        // compaction tasks producer thread
        st = Thread::create(
                "StorageEngine", "compaction_tasks_producer_thread",
                [this]() { this->_storage_engine->_compaction_tasks_producer_callback(); },
                &_storage_engine->_compaction_tasks_producer_thread);
        EXPECT_TRUE(st.ok());
        _storage_engine->_stop_background_threads_latch.count_down();
        sleep(2);
        EXPECT_EQ(_storage_engine->get_compaction_num_per_round(), 1);
    }
    {
        // queue size 0
        // task num 4->8
        _storage_engine->_cumu_compaction_thread_pool->_total_queued_tasks = 0;
        _storage_engine->_compaction_num_per_round = 4;
        // compaction tasks producer thread
        st = Thread::create(
                "StorageEngine", "compaction_tasks_producer_thread",
                [this]() { this->_storage_engine->_compaction_tasks_producer_callback(); },
                &_storage_engine->_compaction_tasks_producer_thread);
        EXPECT_TRUE(st.ok());
        _storage_engine->_stop_background_threads_latch.count_down();
        sleep(2);
        EXPECT_EQ(_storage_engine->get_compaction_num_per_round(), 8);
    }
    {
        // queue size 0
        // task num 64->64
        _storage_engine->_cumu_compaction_thread_pool->_total_queued_tasks = 0;
        _storage_engine->_compaction_num_per_round = 64;
        // compaction tasks producer thread
        st = Thread::create(
                "StorageEngine", "compaction_tasks_producer_thread",
                [this]() { this->_storage_engine->_compaction_tasks_producer_callback(); },
                &_storage_engine->_compaction_tasks_producer_thread);
        EXPECT_TRUE(st.ok());
        _storage_engine->_stop_background_threads_latch.count_down();
        sleep(2);
        EXPECT_EQ(_storage_engine->get_compaction_num_per_round(), 64);
    }
}

TEST_F(CompactionTaskTest, TestAutoSetCompactionDecreaseTaskNum) {
    auto st = ThreadPoolBuilder("BaseCompactionTaskThreadPool")
                      .set_min_threads(2)
                      .set_max_threads(2)
                      .build(&_storage_engine->_base_compaction_thread_pool);
    EXPECT_TRUE(st.ok());
    st = ThreadPoolBuilder("CumuCompactionTaskThreadPool")
                 .set_min_threads(2)
                 .set_max_threads(2)
                 .build(&_storage_engine->_cumu_compaction_thread_pool);
    EXPECT_TRUE(st.ok());
    config::disable_auto_compaction = false;

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("olap_server::_generate_compaction_tasks.return_empty", [](auto&& values) {
        auto* ret = try_any_cast_ret<std::vector<TabletCompactionContext>>(values);
        ret->second = true;
    });
    sp->set_call_back("StorageEngine::_adjust_compaction_thread_num.return_void",
                      [](auto&& args) { *try_any_cast<bool*>(args.back()) = true; });
    sp->set_call_back("StorageEngine::_compaction_tasks_producer_callback",
                      [](auto&& values) { std::this_thread::sleep_for(std::chrono::seconds(1)); });

    Defer defer {[&]() {
        _storage_engine->_stop_background_threads_latch.count_down();
        sp->clear_all_call_backs();
    }};

    config::generate_compaction_tasks_interval_ms = 1000;
    {
        // queue size 3
        // task num 8->8
        _storage_engine->_compaction_num_per_round = 8;
        _storage_engine->_cumu_compaction_thread_pool->_total_queued_tasks = 3;
        // compaction tasks producer thread
        st = Thread::create(
                "StorageEngine", "compaction_tasks_producer_thread",
                [this]() { this->_storage_engine->_compaction_tasks_producer_callback(); },
                &_storage_engine->_compaction_tasks_producer_thread);
        EXPECT_TRUE(st.ok());
        _storage_engine->_stop_background_threads_latch.count_down();
        sleep(2);
        EXPECT_EQ(_storage_engine->get_compaction_num_per_round(), 8);
    }
    {
        // queue size 5
        // task num 8->4
        _storage_engine->_cumu_compaction_thread_pool->_total_queued_tasks = 5;
        _storage_engine->_compaction_num_per_round = 8;
        // compaction tasks producer thread
        st = Thread::create(
                "StorageEngine", "compaction_tasks_producer_thread",
                [this]() { this->_storage_engine->_compaction_tasks_producer_callback(); },
                &_storage_engine->_compaction_tasks_producer_thread);
        EXPECT_TRUE(st.ok());
        _storage_engine->_stop_background_threads_latch.count_down();
        sleep(2);
        EXPECT_EQ(_storage_engine->get_compaction_num_per_round(), 4);
    }
    {
        // queue size 1
        // task num 1->1
        _storage_engine->_cumu_compaction_thread_pool->_total_queued_tasks = 1;
        _storage_engine->_compaction_num_per_round = 1;
        // compaction tasks producer thread
        st = Thread::create(
                "StorageEngine", "compaction_tasks_producer_thread",
                [this]() { this->_storage_engine->_compaction_tasks_producer_callback(); },
                &_storage_engine->_compaction_tasks_producer_thread);
        EXPECT_TRUE(st.ok());
        _storage_engine->_stop_background_threads_latch.count_down();
        sleep(2);
        EXPECT_EQ(_storage_engine->get_compaction_num_per_round(), 1);
    }
}

} // namespace doris
