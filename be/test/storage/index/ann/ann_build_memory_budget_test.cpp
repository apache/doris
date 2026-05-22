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

#include "storage/index/ann/ann_build_memory_budget.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "common/config.h"
#include "io/fs/local_file_system.h"
#include "storage/index/ann/ann_index_writer.h"
#include "storage/index/ann/faiss_ann_index.h"
#include "storage/index/ann/vector_search_utils.h"
#include "storage/index/index_file_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/defer_op.h"

using namespace doris::vector_search_utils;

namespace doris::segment_v2 {

class AnnBuildMemoryBudgetTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_budget = config::ann_index_build_memory_budget_bytes;
        _saved_timeout = config::ann_index_build_memory_wait_timeout_ms;
        _saved_action = config::ann_index_build_on_oom_action;
        AnnBuildMemoryBudget::instance().reset_for_test();
    }
    void TearDown() override {
        AnnBuildMemoryBudget::instance().reset_for_test();
        config::ann_index_build_memory_budget_bytes = _saved_budget;
        config::ann_index_build_memory_wait_timeout_ms = _saved_timeout;
        config::ann_index_build_on_oom_action = _saved_action;
    }

    int64_t _saved_budget = 0;
    int64_t _saved_timeout = 0;
    std::string _saved_action;
};

TEST_F(AnnBuildMemoryBudgetTest, DisabledBudgetGrantsEverything) {
    config::ann_index_build_memory_budget_bytes = 0;
    auto& budget = AnnBuildMemoryBudget::instance();
    EXPECT_TRUE(budget.try_reserve(1LL << 40, /*timeout_ms=*/0));
    EXPECT_EQ(budget.reserved_bytes(), 0); // disabled => no accounting
}

TEST_F(AnnBuildMemoryBudgetTest, ReserveAndReleaseAccounting) {
    config::ann_index_build_memory_budget_bytes = 1024;
    auto& budget = AnnBuildMemoryBudget::instance();
    ASSERT_TRUE(budget.try_reserve(400, 0));
    EXPECT_EQ(budget.reserved_bytes(), 400);
    ASSERT_TRUE(budget.try_reserve(600, 0));
    EXPECT_EQ(budget.reserved_bytes(), 1000);
    budget.release(400);
    EXPECT_EQ(budget.reserved_bytes(), 600);
    budget.release(600);
    EXPECT_EQ(budget.reserved_bytes(), 0);
}

TEST_F(AnnBuildMemoryBudgetTest, TimeoutWhenStarved) {
    config::ann_index_build_memory_budget_bytes = 1024;
    auto& budget = AnnBuildMemoryBudget::instance();
    ASSERT_TRUE(budget.try_reserve(1024, 0));
    EXPECT_FALSE(budget.try_reserve(1, /*timeout_ms=*/10));
    budget.release(1024);
}

TEST_F(AnnBuildMemoryBudgetTest, OversizedSingleBuildIsAllowed) {
    // When nothing else is in flight, a single build larger than the budget
    // must still proceed (otherwise it would self-deadlock). Once it is
    // holding bytes, the next request must respect the budget.
    config::ann_index_build_memory_budget_bytes = 100;
    auto& budget = AnnBuildMemoryBudget::instance();
    ASSERT_TRUE(budget.try_reserve(10000, 0));
    EXPECT_EQ(budget.reserved_bytes(), 10000);
    EXPECT_FALSE(budget.try_reserve(1, 0));
    budget.release(10000);
}

TEST_F(AnnBuildMemoryBudgetTest, WaiterWakesOnRelease) {
    config::ann_index_build_memory_budget_bytes = 1024;
    auto& budget = AnnBuildMemoryBudget::instance();
    ASSERT_TRUE(budget.try_reserve(1024, 0));

    std::atomic<bool> waiter_ok {false};
    std::thread waiter([&]() {
        // Plenty of room to wait; should be unblocked by the release below.
        waiter_ok = budget.try_reserve(500, /*timeout_ms=*/5000);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(waiter_ok.load());
    budget.release(1024);
    waiter.join();
    EXPECT_TRUE(waiter_ok.load());
    budget.release(500);
}

TEST_F(AnnBuildMemoryBudgetTest, ReservationRaiiReleasesOnDestruction) {
    config::ann_index_build_memory_budget_bytes = 1024;
    auto& budget = AnnBuildMemoryBudget::instance();
    {
        auto handle = AnnBuildMemoryReservation::try_acquire(800, 0);
        ASSERT_TRUE(handle.active());
        EXPECT_EQ(handle.bytes(), 800);
        EXPECT_EQ(budget.reserved_bytes(), 800);
    }
    EXPECT_EQ(budget.reserved_bytes(), 0);
}

TEST_F(AnnBuildMemoryBudgetTest, ReservationFailureYieldsInactiveHandle) {
    config::ann_index_build_memory_budget_bytes = 100;
    auto& budget = AnnBuildMemoryBudget::instance();
    ASSERT_TRUE(budget.try_reserve(100, 0));
    auto handle = AnnBuildMemoryReservation::try_acquire(1, /*timeout_ms=*/5);
    EXPECT_FALSE(handle.active());
    EXPECT_EQ(handle.bytes(), 0);
    budget.release(100);
}

TEST_F(AnnBuildMemoryBudgetTest, GrowAllowedForSoleBuildEvenPastBudget) {
    // A build that already holds bytes may keep growing past the budget as long
    // as it is the only build in flight; otherwise it would deadlock on itself.
    config::ann_index_build_memory_budget_bytes = 100;
    auto& budget = AnnBuildMemoryBudget::instance();
    auto handle = AnnBuildMemoryReservation::try_acquire(60, /*timeout_ms=*/0);
    ASSERT_TRUE(handle.active());
    EXPECT_EQ(budget.reserved_bytes(), 60);
    // 60 + 100 = 160 > budget, but sole build => allowed without waiting.
    EXPECT_TRUE(handle.grow(100, /*timeout_ms=*/0));
    EXPECT_EQ(handle.bytes(), 160);
    EXPECT_EQ(budget.reserved_bytes(), 160);
}

TEST_F(AnnBuildMemoryBudgetTest, GrowBlocksWhenOtherBuildInFlight) {
    config::ann_index_build_memory_budget_bytes = 100;
    auto& budget = AnnBuildMemoryBudget::instance();
    auto handle = AnnBuildMemoryReservation::try_acquire(60, /*timeout_ms=*/0);
    ASSERT_TRUE(handle.active());
    // A second, independent build takes 30 (reserved now 90, another build present).
    ASSERT_TRUE(budget.try_reserve(30, /*timeout_ms=*/0));
    // handle grow by 50 -> 90 + 50 = 140 > budget and not sole build => timeout.
    EXPECT_FALSE(handle.grow(50, /*timeout_ms=*/10));
    EXPECT_EQ(handle.bytes(), 60); // unchanged on failure
    EXPECT_EQ(budget.reserved_bytes(), 90);
    budget.release(30);
}

TEST_F(AnnBuildMemoryBudgetTest, GrowWakesWhenOtherBuildReleases) {
    config::ann_index_build_memory_budget_bytes = 100;
    auto& budget = AnnBuildMemoryBudget::instance();
    auto handle = AnnBuildMemoryReservation::try_acquire(40, /*timeout_ms=*/0);
    ASSERT_TRUE(handle.active());
    ASSERT_TRUE(budget.try_reserve(50, /*timeout_ms=*/0)); // other build, reserved=90

    std::atomic<bool> grew {false};
    std::thread waiter([&]() {
        // 90 + 30 = 120 > budget; must wait for the other build to release.
        grew = handle.grow(30, /*timeout_ms=*/5000);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(grew.load());
    budget.release(50);
    waiter.join();
    EXPECT_TRUE(grew.load());
    EXPECT_EQ(handle.bytes(), 70);
}

TEST_F(AnnBuildMemoryBudgetTest, EstimateGrowsWithRowsAndDim) {
    FaissBuildParameter p;
    p.index_type = FaissBuildParameter::IndexType::HNSW;
    p.quantizer = FaissBuildParameter::Quantizer::FLAT;
    p.dim = 128;
    p.max_degree = 32;
    const int64_t small = estimate_ann_build_memory(p, /*expected_rows=*/1000, /*chunk_rows=*/100);
    const int64_t big = estimate_ann_build_memory(p, /*expected_rows=*/100000, /*chunk_rows=*/100);
    EXPECT_GT(big, small);
    EXPECT_GT(small, 0);
}

TEST_F(AnnBuildMemoryBudgetTest, EstimateUnknownRowsFallsBackToChunk) {
    FaissBuildParameter p;
    p.index_type = FaissBuildParameter::IndexType::HNSW;
    p.quantizer = FaissBuildParameter::Quantizer::FLAT;
    p.dim = 64;
    p.max_degree = 16;
    const int64_t unknown = estimate_ann_build_memory(p, /*expected_rows=*/0, /*chunk_rows=*/4096);
    const int64_t one_chunk =
            estimate_ann_build_memory(p, /*expected_rows=*/4096, /*chunk_rows=*/4096);
    EXPECT_EQ(unknown, one_chunk);
}

TEST_F(AnnBuildMemoryBudgetTest, EstimateQuantizersShrinkStore) {
    FaissBuildParameter p;
    p.index_type = FaissBuildParameter::IndexType::HNSW;
    p.dim = 256;
    p.max_degree = 32;

    p.quantizer = FaissBuildParameter::Quantizer::FLAT;
    const int64_t flat = estimate_ann_build_memory(p, 10000, 1000);
    p.quantizer = FaissBuildParameter::Quantizer::SQ8;
    const int64_t sq8 = estimate_ann_build_memory(p, 10000, 1000);
    p.quantizer = FaissBuildParameter::Quantizer::SQ4;
    const int64_t sq4 = estimate_ann_build_memory(p, 10000, 1000);
    p.quantizer = FaissBuildParameter::Quantizer::PQ;
    p.pq_m = 16;
    const int64_t pq = estimate_ann_build_memory(p, 10000, 1000);

    // Each quantization step must reduce the per-row footprint of the store,
    // pulling the overall estimate down.
    EXPECT_GT(flat, sq8);
    EXPECT_GT(sq8, sq4);
    EXPECT_GT(sq4, pq);
}

TEST_F(AnnBuildMemoryBudgetTest, EstimateHandlesZeroDim) {
    FaissBuildParameter p;
    p.dim = 0;
    EXPECT_EQ(estimate_ann_build_memory(p, 1000, 100), 0);
}

// -------------------------------------------------------------------------
// Writer integration: skip / fail / disabled paths.
// -------------------------------------------------------------------------

class TestSkipAwareWriter : public AnnIndexColumnWriter {
public:
    using AnnIndexColumnWriter::AnnIndexColumnWriter;
    bool skip_due_to_oom() const { return _skip_due_to_oom; }
};

class WriterAdmissionTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (ExecEnv::GetInstance()->get_tmp_file_dirs() == nullptr) {
            const std::string tmp_dir = "./ut_dir/tmp_vector_search";
            (void)doris::io::global_local_filesystem()->delete_directory(tmp_dir);
            (void)doris::io::global_local_filesystem()->create_directory(tmp_dir);
            std::vector<doris::StorePath> paths;
            paths.emplace_back(tmp_dir, -1);
            auto tmp_file_dirs = std::make_unique<doris::segment_v2::TmpFileDirs>(paths);
            ASSERT_TRUE(tmp_file_dirs->init().ok());
            ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
        }

        _saved_budget = config::ann_index_build_memory_budget_bytes;
        _saved_timeout = config::ann_index_build_memory_wait_timeout_ms;
        _saved_action = config::ann_index_build_on_oom_action;
        AnnBuildMemoryBudget::instance().reset_for_test();

        _properties["index_type"] = "hnsw";
        _properties["metric_type"] = "l2_distance";
        _properties["dim"] = "16";
        _properties["max_degree"] = "16";
        _tablet_index = std::make_unique<TabletIndex>();
        _tablet_index->_index_type = IndexType::ANN;
        _tablet_index->_properties = _properties;
        _tablet_index->_index_id = 42;
        _tablet_index->_index_name = "test_ann_index";

        _index_file_writer =
                std::make_unique<MockIndexFileWriter>(doris::io::global_local_filesystem());
    }
    void TearDown() override {
        AnnBuildMemoryBudget::instance().reset_for_test();
        config::ann_index_build_memory_budget_bytes = _saved_budget;
        config::ann_index_build_memory_wait_timeout_ms = _saved_timeout;
        config::ann_index_build_on_oom_action = _saved_action;
    }

    int64_t _saved_budget = 0;
    int64_t _saved_timeout = 0;
    std::string _saved_action;
    std::map<std::string, std::string> _properties;
    std::unique_ptr<TabletIndex> _tablet_index;
    std::unique_ptr<MockIndexFileWriter> _index_file_writer;
};

TEST_F(WriterAdmissionTest, SkipModeDeletesIndexAndSwallowsRows) {
    // Tiny budget that the estimator will never satisfy. The writer should
    // enter skip mode, accept add_array_values silently, and ask the index
    // file writer to delete the index entry on finish().
    config::ann_index_build_memory_budget_bytes = 1;
    config::ann_index_build_memory_wait_timeout_ms = 0;
    config::ann_index_build_on_oom_action = "skip";

    auto writer = std::make_unique<TestSkipAwareWriter>(_index_file_writer.get(),
                                                        _tablet_index.get());
    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));
    // IndexFileWriter::delete_index is not mocked; the real method short-circuits
    // to Status::OK when the index id is not registered, which matches the
    // skip-path expectation here.

    ASSERT_TRUE(writer->init().ok());
    EXPECT_TRUE(writer->skip_due_to_oom());

    const size_t dim = 16;
    const size_t num_rows = 2;
    std::vector<float> vectors(num_rows * dim, 1.0f);
    std::vector<size_t> offsets = {0, dim, 2 * dim};
    EXPECT_TRUE(writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                         reinterpret_cast<const uint8_t*>(offsets.data()), num_rows)
                        .ok());

    EXPECT_TRUE(writer->finish().ok());
}

TEST_F(WriterAdmissionTest, FailModeReturnsErrorFromInit) {
    config::ann_index_build_memory_budget_bytes = 1;
    config::ann_index_build_memory_wait_timeout_ms = 0;
    config::ann_index_build_on_oom_action = "fail";

    auto writer = std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(),
                                                         _tablet_index.get());
    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    Status status = writer->init();
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::RUNTIME_ERROR>()) << status.to_string();
}

TEST_F(WriterAdmissionTest, FailModeDoesNotWaitDespiteLargeTimeout) {
    // Regression: "fail" must return immediately, never blocking for the
    // configured wait timeout. We set a large timeout and assert init() fails
    // in well under it.
    config::ann_index_build_memory_budget_bytes = 1;
    config::ann_index_build_memory_wait_timeout_ms = 10000;
    config::ann_index_build_on_oom_action = "fail";
    // Occupy the budget so the writer's reservation cannot fit.
    ASSERT_TRUE(AnnBuildMemoryBudget::instance().try_reserve(1, /*timeout_ms=*/0));

    auto writer = std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(),
                                                         _tablet_index.get());
    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    const auto start = std::chrono::steady_clock::now();
    Status status = writer->init();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - start)
                                 .count();
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::RUNTIME_ERROR>()) << status.to_string();
    EXPECT_LT(elapsed, 2000) << "fail action waited " << elapsed << " ms";
    AnnBuildMemoryBudget::instance().release(1);
}

TEST_F(WriterAdmissionTest, ReservationGrowsWithAccumulatedRows) {
    // With a generous budget the writer must enlarge its reservation as chunks
    // are added, so the global counter tracks real memory rather than just the
    // initial per-chunk floor.
    config::ann_index_build_memory_budget_bytes = 1LL << 30;
    config::ann_index_build_memory_wait_timeout_ms = 1000;
    config::ann_index_build_on_oom_action = "wait";

    auto writer = std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(),
                                                         _tablet_index.get());
    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());
    const int64_t after_init = AnnBuildMemoryBudget::instance().reserved_bytes();
    EXPECT_GT(after_init, 0);

    // BE_TEST chunk_size() is 10, so 30 rows flush three full chunks.
    const size_t dim = 16;
    const size_t num_rows = 30;
    std::vector<float> vectors(num_rows * dim, 0.5f);
    std::vector<size_t> offsets(num_rows + 1);
    for (size_t i = 0; i <= num_rows; ++i) {
        offsets[i] = i * dim;
    }
    ASSERT_TRUE(writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                         reinterpret_cast<const uint8_t*>(offsets.data()), num_rows)
                        .ok());

    const int64_t after_adds = AnnBuildMemoryBudget::instance().reserved_bytes();
    EXPECT_GT(after_adds, after_init)
            << "reservation did not grow: after_init=" << after_init
            << " after_adds=" << after_adds;

    ASSERT_TRUE(writer->finish().ok());
    writer.reset(); // RAII release
    EXPECT_EQ(AnnBuildMemoryBudget::instance().reserved_bytes(), 0);
}

TEST_F(WriterAdmissionTest, DisabledBudgetIsTransparent) {
    config::ann_index_build_memory_budget_bytes = 0;
    auto writer = std::make_unique<TestSkipAwareWriter>(_index_file_writer.get(),
                                                        _tablet_index.get());
    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());
    EXPECT_FALSE(writer->skip_due_to_oom());
    EXPECT_EQ(AnnBuildMemoryBudget::instance().reserved_bytes(), 0);
}

} // namespace doris::segment_v2
