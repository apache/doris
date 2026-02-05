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
#include <unistd.h>

#include <chrono>
#include <memory>
#include <thread>

#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/fs/local_file_system.h"
#include "olap/compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/data_dir.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "util/doris_metrics.h"
#include "util/threadpool.h"

namespace doris {
using namespace config;

class CompactionMetricsTest : public testing::Test {
public:
    void SetUp() override {
        _engine_data_path = "./be/test/olap/test_data/converter_test_data/tmp";
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

    void TearDown() override {
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
    rs_meta->set_num_rows(50);
    RowsetSharedPtr rowset;
    Status st = RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset);
    if (!st.ok()) {
        return nullptr;
    }
    return rowset;
}

TEST_F(CompactionMetricsTest, TestCompactionTaskNumWithDiffStatus) {
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
        std::this_thread::sleep_for(std::chrono::seconds(1));
        bool* pred = try_any_cast<bool*>(values.back());
        *pred = true;
    });
    Defer defer {[&]() { sp->clear_call_back("olap_server::execute_compaction"); }};

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
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        EXPECT_EQ(_storage_engine->_cumu_compaction_thread_pool->num_active_threads(),
                  DorisMetrics::instance()->cumulative_compaction_task_running_total->value());
        EXPECT_EQ(_storage_engine->_cumu_compaction_thread_pool->get_queue_size(),
                  DorisMetrics::instance()->cumulative_compaction_task_pending_total->value());
        EXPECT_EQ(_storage_engine->_base_compaction_thread_pool->num_active_threads(),
                  DorisMetrics::instance()->base_compaction_task_running_total->value());
        EXPECT_EQ(_storage_engine->_base_compaction_thread_pool->get_queue_size(),
                  DorisMetrics::instance()->base_compaction_task_pending_total->value());
    }
}

TEST_F(CompactionMetricsTest, TestCompactionProducerSpendTime) {
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
    bool disable_auto_compaction = config::disable_auto_compaction;
    config::disable_auto_compaction = true;

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("StorageEngine::_compaction_tasks_producer_callback",
                      [](auto&& values) { std::this_thread::sleep_for(std::chrono::seconds(1)); });

    Defer defer {[&]() {
        _storage_engine->_stop_background_threads_latch.count_down();
        config::disable_auto_compaction = disable_auto_compaction;
        sp->clear_call_back("StorageEngine::_compaction_tasks_producer_callback");
    }};

    // compaction tasks producer thread
    st = Thread::create(
            "StorageEngine", "compaction_tasks_producer_thread",
            [this]() { this->_storage_engine->_compaction_tasks_producer_callback(); },
            &_storage_engine->_compaction_tasks_producer_thread);
    EXPECT_TRUE(st.ok());
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // the compaction_producer_callback_a_round_time can't get an accurate value
    // just judge it great than 0
    EXPECT_GT(DorisMetrics::instance()->compaction_producer_callback_a_round_time->value(), 0);
}

TEST_F(CompactionMetricsTest, TestCompactionReadWriteThroughput) {
    DorisMetrics::instance()->local_compaction_read_bytes_total->set_value(0);
    DorisMetrics::instance()->local_compaction_read_rows_total->set_value(0);
    DorisMetrics::instance()->local_compaction_write_bytes_total->set_value(0);
    DorisMetrics::instance()->local_compaction_write_rows_total->set_value(0);

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
    sp->set_call_back("compaction::CompactionMixin::build_basic_info", [](auto&& values) {
        auto* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
        pairs->second = true;
    });
    sp->set_call_back("compaction::CompactionMixin::execute_compact_impl", [&](auto&& values) {
        auto* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
        pairs->second = true;
    });
    sp->set_call_back("compaction::CompactionMixin::execute_compact", [&](auto&& values) {
        auto* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
        pairs->second = true;
    });
    sp->set_call_back("cumulative_compaction::CumulativeCompaction::execute_compact",
                      [&](auto&& values) {
                          auto* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
                          pairs->second = true;
                      });
    Defer defer {[&]() {
        sp->clear_call_back("compaction::CompactionMixin::build_basic_info");
        sp->clear_call_back("compaction::CompactionMixin::execute_compact_impl");
        sp->clear_call_back("compaction::CompactionMixin::execute_compact");
        sp->clear_call_back("cumulative_compaction::CumulativeCompaction::execute_compact");
    }};

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

    std::this_thread::sleep_for(std::chrono::seconds(5));

    EXPECT_EQ(DorisMetrics::instance()->local_compaction_read_bytes_total->value(), 1024 * 28);
    EXPECT_EQ(DorisMetrics::instance()->local_compaction_read_rows_total->value(), 50 * 28);
}

} // namespace doris
