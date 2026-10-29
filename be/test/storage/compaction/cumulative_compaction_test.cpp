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

#include "storage/compaction/cumulative_compaction.h"

#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "common/status.h"
#include "cpp/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "storage/compaction/compaction.h"
#include "storage/compaction/cumulative_compaction_policy.h"
#include "storage/data_dir.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_manager.h"
#include "util/threadpool.h"

namespace doris {
using namespace config;

class CumulativeCompactionTest : public testing::Test {
public:
    virtual void SetUp() {}

    virtual void TearDown() {}
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

class TestableCumulativeCompactionMixin : public CompactionMixin {
public:
    TestableCumulativeCompactionMixin(StorageEngine& engine, TabletSharedPtr tablet)
            : CompactionMixin(engine, tablet, "TestableCumulativeCompactionMixin") {}

    void set_input_rowsets(const std::vector<RowsetSharedPtr>& rowsets) {
        _input_rowsets = rowsets;
    }

    Status prepare_compact() override { return Status::OK(); }

    Status execute_compact() override { return Status::OK(); }

    ReaderType compaction_type() const override { return ReaderType::READER_CUMULATIVE_COMPACTION; }

    std::string_view compaction_name() const override { return "testable cumulative compaction"; }

protected:
    Status construct_output_rowset_writer(RowsetWriterContext&) override { return Status::OK(); }

    Status update_delete_bitmap() override { return Status::OK(); }
};

TEST_F(CumulativeCompactionTest, TestConsecutiveVersion) {
    EngineOptions options;
    StorageEngine storage_engine(options);
    //TabletSharedPtr tablet;

    TabletMetaSharedPtr tablet_meta;
    tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                     UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                     TCompressionType::LZ4F));
    TabletSharedPtr tablet(
            new Tablet(storage_engine, tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));

    CumulativeCompaction cumu_compaction(storage_engine, tablet);

    {
        std::vector<RowsetSharedPtr> rowsets;
        for (int i = 2; i < 10; ++i) {
            RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
            rowsets.push_back(rs);
        }
        std::vector<Version> missing_version;
        cumu_compaction.find_longest_consecutive_version(&rowsets, &missing_version);
        EXPECT_EQ(rowsets.size(), 8);
        EXPECT_EQ(rowsets.front()->start_version(), 2);
        EXPECT_EQ(rowsets.front()->end_version(), 2);

        EXPECT_EQ(rowsets.back()->start_version(), 9);
        EXPECT_EQ(rowsets.back()->end_version(), 9);

        EXPECT_EQ(missing_version.size(), 0);
    }

    {
        std::vector<RowsetSharedPtr> rowsets;
        for (int i = 2; i <= 4; ++i) {
            RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
            rowsets.push_back(rs);
        }

        for (int i = 6; i <= 10; ++i) {
            RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
            rowsets.push_back(rs);
        }

        for (int i = 12; i <= 13; ++i) {
            RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
            rowsets.push_back(rs);
        }

        std::vector<Version> missing_version;
        cumu_compaction.find_longest_consecutive_version(&rowsets, &missing_version);

        EXPECT_EQ(rowsets.size(), 5);
        EXPECT_EQ(rowsets.front()->start_version(), 6);
        EXPECT_EQ(rowsets.front()->end_version(), 6);
        EXPECT_EQ(rowsets.back()->start_version(), 10);
        EXPECT_EQ(rowsets.back()->end_version(), 10);

        EXPECT_EQ(missing_version.size(), 4);
        EXPECT_EQ(missing_version[0].first, 4);
        EXPECT_EQ(missing_version[0].second, 4);
        EXPECT_EQ(missing_version[1].first, 6);
        EXPECT_EQ(missing_version[1].second, 6);
        EXPECT_EQ(missing_version[2].first, 10);
        EXPECT_EQ(missing_version[2].second, 10);
        EXPECT_EQ(missing_version[3].first, 12);
        EXPECT_EQ(missing_version[3].second, 12);
    }

    {
        std::vector<RowsetSharedPtr> rowsets;
        for (int i = 2; i <= 2; ++i) {
            RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
            rowsets.push_back(rs);
        }

        for (int i = 4; i <= 4; ++i) {
            RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
            rowsets.push_back(rs);
        }

        std::vector<Version> missing_version;
        cumu_compaction.find_longest_consecutive_version(&rowsets, &missing_version);

        EXPECT_EQ(rowsets.size(), 1);
        EXPECT_EQ(rowsets.front()->start_version(), 2);
        EXPECT_EQ(rowsets.front()->end_version(), 2);
        EXPECT_EQ(rowsets.back()->start_version(), 2);
        EXPECT_EQ(rowsets.back()->end_version(), 2);

        EXPECT_EQ(missing_version.size(), 2);
        EXPECT_EQ(missing_version[0].first, 2);
        EXPECT_EQ(missing_version[0].second, 2);
        EXPECT_EQ(missing_version[1].first, 4);
        EXPECT_EQ(missing_version[1].second, 4);
    }

    {
        std::vector<RowsetSharedPtr> rowsets;
        RowsetSharedPtr rs = create_rowset({2, 3}, 1, false, 1024);
        rowsets.push_back(rs);
        rs = create_rowset({4, 5}, 1, false, 1024);
        rowsets.push_back(rs);

        rs = create_rowset({9, 11}, 1, false, 1024);
        rowsets.push_back(rs);
        rs = create_rowset({12, 13}, 1, false, 1024);
        rowsets.push_back(rs);

        std::vector<Version> missing_version;
        cumu_compaction.find_longest_consecutive_version(&rowsets, &missing_version);

        EXPECT_EQ(rowsets.size(), 2);
        EXPECT_EQ(rowsets.front()->start_version(), 2);
        EXPECT_EQ(rowsets.front()->end_version(), 3);
        EXPECT_EQ(rowsets.back()->start_version(), 4);
        EXPECT_EQ(rowsets.back()->end_version(), 5);

        EXPECT_EQ(missing_version.size(), 2);
        EXPECT_EQ(missing_version[0].first, 4);
        EXPECT_EQ(missing_version[0].second, 5);
        EXPECT_EQ(missing_version[1].first, 9);
        EXPECT_EQ(missing_version[1].second, 11);
    }

    {
        std::vector<RowsetSharedPtr> rowsets;
        for (int i = 2; i <= 2; ++i) {
            RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
            rowsets.push_back(rs);
        }

        std::vector<Version> missing_version;
        cumu_compaction.find_longest_consecutive_version(&rowsets, &missing_version);
        EXPECT_EQ(rowsets.size(), 1);
        EXPECT_EQ(rowsets.front()->start_version(), 2);
        EXPECT_EQ(rowsets.front()->end_version(), 2);

        EXPECT_EQ(rowsets.back()->start_version(), 2);
        EXPECT_EQ(rowsets.back()->end_version(), 2);
        EXPECT_EQ(missing_version.size(), 0);
    }

    {
        std::vector<RowsetSharedPtr> rowsets;
        for (int i = 2; i <= 2; ++i) {
            RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
            rowsets.push_back(rs);
        }

        std::vector<Version> missing_version;
        cumu_compaction.find_longest_consecutive_version(&rowsets, &missing_version);
        EXPECT_EQ(rowsets.size(), 1);
        EXPECT_EQ(rowsets.front()->start_version(), 2);
        EXPECT_EQ(rowsets.front()->end_version(), 2);

        EXPECT_EQ(rowsets.back()->start_version(), 2);
        EXPECT_EQ(rowsets.back()->end_version(), 2);
        EXPECT_EQ(missing_version.size(), 0);
    }

    {
        std::vector<RowsetSharedPtr> rowsets;
        for (int i = 2; i <= 4; ++i) {
            RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
            rowsets.push_back(rs);
        }

        for (int i = 6; i <= 10; ++i) {
            RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
            rowsets.push_back(rs);
        }

        for (int i = 12; i <= 20; ++i) {
            RowsetSharedPtr rs = create_rowset({i, i}, 1, false, 1024);
            rowsets.push_back(rs);
        }

        std::vector<Version> missing_version;
        cumu_compaction.find_longest_consecutive_version(&rowsets, &missing_version);

        EXPECT_EQ(rowsets.size(), 9);
        EXPECT_EQ(rowsets.front()->start_version(), 12);
        EXPECT_EQ(rowsets.front()->end_version(), 12);
        EXPECT_EQ(rowsets.back()->start_version(), 20);
        EXPECT_EQ(rowsets.back()->end_version(), 20);

        EXPECT_EQ(missing_version.size(), 4);
        EXPECT_EQ(missing_version[0].first, 4);
        EXPECT_EQ(missing_version[0].second, 4);
        EXPECT_EQ(missing_version[1].first, 6);
        EXPECT_EQ(missing_version[1].second, 6);
        EXPECT_EQ(missing_version[2].first, 10);
        EXPECT_EQ(missing_version[2].second, 10);
        EXPECT_EQ(missing_version[3].first, 12);
        EXPECT_EQ(missing_version[3].second, 12);
    }
}

TEST_F(CumulativeCompactionTest, TestShouldDelayLargeTask) {
    // Initialize storage engine and thread pool
    EngineOptions options;
    StorageEngine storage_engine(options);
    EXPECT_EQ(ThreadPoolBuilder("CumuCompactionTaskThreadPool")
                      .set_min_threads(1)
                      .set_max_threads(2)
                      .build(&storage_engine._cumu_compaction_thread_pool),
              Status::OK());

    // Configure parameters
    config::large_cumu_compaction_task_min_thread_num = 3;
    config::large_cumu_compaction_task_row_num_threshold = 10;

    // Set thread pool max threads
    EXPECT_EQ(storage_engine._cumu_compaction_thread_pool->set_max_threads(3), Status::OK());

    storage_engine._cumu_compaction_thread_pool_used_threads = 2;
    EXPECT_EQ(storage_engine._should_delay_large_task(), false);

    storage_engine._cumu_compaction_thread_pool_used_threads = 3;
    storage_engine._cumu_compaction_thread_pool_small_tasks_running = 1;
    EXPECT_EQ(storage_engine._should_delay_large_task(), false);

    storage_engine._cumu_compaction_thread_pool_used_threads = 3;
    storage_engine._cumu_compaction_thread_pool_small_tasks_running = 0;
    EXPECT_EQ(storage_engine._should_delay_large_task(), true);
}

TEST_F(CumulativeCompactionTest, TestCalcInputRowsetsRowNumUsesRowCount) {
    EngineOptions options;
    StorageEngine storage_engine(options);

    TabletMetaSharedPtr tablet_meta;
    tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                     UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                     TCompressionType::LZ4F));
    TabletSharedPtr tablet(
            new Tablet(storage_engine, tablet_meta, nullptr, CUMULATIVE_SIZE_BASED_POLICY));

    TestableCumulativeCompactionMixin compaction(storage_engine, tablet);

    std::vector<RowsetSharedPtr> rowsets;
    auto rowset1 = create_rowset({1, 1}, 1, false, 1024);
    ASSERT_NE(rowset1, nullptr);
    rowset1->rowset_meta()->set_num_rows(10);
    rowsets.push_back(rowset1);

    auto rowset2 = create_rowset({2, 2}, 1, false, 2048);
    ASSERT_NE(rowset2, nullptr);
    rowset2->rowset_meta()->set_num_rows(20);
    rowsets.push_back(rowset2);

    auto rowset3 = create_rowset({3, 3}, 1, false, 4096);
    ASSERT_NE(rowset3, nullptr);
    rowset3->rowset_meta()->set_num_rows(30);
    rowsets.push_back(rowset3);

    compaction.set_input_rowsets(rowsets);

    EXPECT_EQ(compaction.calc_input_rowsets_row_num(), 60);
    EXPECT_EQ(compaction.calc_input_rowsets_total_size(), 7168);
}

} // namespace doris
