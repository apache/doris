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

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "io/io_common.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

class CompactionSampleInfoTest : public testing::Test {
protected:
    void SetUp() override {
        _engine = std::make_unique<StorageEngine>(EngineOptions {});
        TabletMetaSharedPtr tablet_meta;
        tablet_meta.reset(new TabletMeta(1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                         UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                         TCompressionType::LZ4F));
        _tablet = std::make_shared<Tablet>(*_engine, tablet_meta, nullptr,
                                           CUMULATIVE_SIZE_BASED_POLICY);
    }

    std::unique_ptr<StorageEngine> _engine;
    std::shared_ptr<Tablet> _tablet;
};

// Test that get_sample_infos returns the correct reference for each compaction type
TEST_F(CompactionSampleInfoTest, GetSampleInfosReturnsCorrectReference) {
    auto& cumu_infos = _tablet->get_sample_infos(ReaderType::READER_CUMULATIVE_COMPACTION);
    auto& base_infos = _tablet->get_sample_infos(ReaderType::READER_BASE_COMPACTION);
    auto& full_infos = _tablet->get_sample_infos(ReaderType::READER_FULL_COMPACTION);

    // Verify they are different references
    EXPECT_NE(&cumu_infos, &base_infos);
    EXPECT_NE(&base_infos, &full_infos);
    EXPECT_NE(&cumu_infos, &full_infos);

    // Verify they match the expected member variables
    EXPECT_EQ(&cumu_infos, &_tablet->cumu_sample_infos);
    EXPECT_EQ(&base_infos, &_tablet->base_sample_infos);
    EXPECT_EQ(&full_infos, &_tablet->full_sample_infos);
}

// Test that get_sample_info_lock returns the correct reference for each compaction type
TEST_F(CompactionSampleInfoTest, GetSampleInfoLockReturnsCorrectReference) {
    auto& cumu_lock = _tablet->get_sample_info_lock(ReaderType::READER_CUMULATIVE_COMPACTION);
    auto& base_lock = _tablet->get_sample_info_lock(ReaderType::READER_BASE_COMPACTION);
    auto& full_lock = _tablet->get_sample_info_lock(ReaderType::READER_FULL_COMPACTION);

    // Verify they are different references
    EXPECT_NE(&cumu_lock, &base_lock);
    EXPECT_NE(&base_lock, &full_lock);
    EXPECT_NE(&cumu_lock, &full_lock);

    // Verify they match the expected member variables
    EXPECT_EQ(&cumu_lock, &_tablet->cumu_sample_info_lock);
    EXPECT_EQ(&base_lock, &_tablet->base_sample_info_lock);
    EXPECT_EQ(&full_lock, &_tablet->full_sample_info_lock);
}

// Test that default reader types fall back to base_sample_infos
TEST_F(CompactionSampleInfoTest, DefaultReaderTypeFallsBackToBase) {
    auto& query_infos = _tablet->get_sample_infos(ReaderType::READER_QUERY);
    auto& alter_infos = _tablet->get_sample_infos(ReaderType::READER_ALTER_TABLE);
    auto& cold_infos = _tablet->get_sample_infos(ReaderType::READER_COLD_DATA_COMPACTION);

    // All should fall back to base_sample_infos
    EXPECT_EQ(&query_infos, &_tablet->base_sample_infos);
    EXPECT_EQ(&alter_infos, &_tablet->base_sample_infos);
    EXPECT_EQ(&cold_infos, &_tablet->base_sample_infos);
}

// Test that different compaction types can have different sample_infos sizes
TEST_F(CompactionSampleInfoTest, IndependentSampleInfosSizes) {
    auto& cumu_infos = _tablet->get_sample_infos(ReaderType::READER_CUMULATIVE_COMPACTION);
    auto& base_infos = _tablet->get_sample_infos(ReaderType::READER_BASE_COMPACTION);
    auto& full_infos = _tablet->get_sample_infos(ReaderType::READER_FULL_COMPACTION);

    // Resize each to different sizes
    cumu_infos.resize(3);
    base_infos.resize(5);
    full_infos.resize(7);

    // Verify sizes are independent
    EXPECT_EQ(cumu_infos.size(), 3);
    EXPECT_EQ(base_infos.size(), 5);
    EXPECT_EQ(full_infos.size(), 7);

    // Resize one doesn't affect others
    cumu_infos.resize(10);
    EXPECT_EQ(cumu_infos.size(), 10);
    EXPECT_EQ(base_infos.size(), 5);
    EXPECT_EQ(full_infos.size(), 7);
}

// Test concurrent access to different sample_infos doesn't cause issues
TEST_F(CompactionSampleInfoTest, ConcurrentAccessToDifferentTypes) {
    std::atomic<bool> has_error {false};
    std::atomic<int> completed_threads {0};
    constexpr int kIterations = 1000;

    // Thread simulating cumulative compaction
    auto cumu_thread = [this, &has_error, &completed_threads]() {
        try {
            for (int i = 0; i < kIterations; ++i) {
                auto& lock =
                        _tablet->get_sample_info_lock(ReaderType::READER_CUMULATIVE_COMPACTION);
                auto& infos = _tablet->get_sample_infos(ReaderType::READER_CUMULATIVE_COMPACTION);
                std::unique_lock<std::mutex> guard(lock);
                infos.resize((i % 5) + 1);
                for (size_t j = 0; j < infos.size(); ++j) {
                    infos[j].group_data_size = i;
                    infos[j].bytes = i * 100;
                    infos[j].rows = i * 10;
                }
            }
        } catch (...) {
            has_error = true;
        }
        completed_threads++;
    };

    // Thread simulating base compaction
    auto base_thread = [this, &has_error, &completed_threads]() {
        try {
            for (int i = 0; i < kIterations; ++i) {
                auto& lock = _tablet->get_sample_info_lock(ReaderType::READER_BASE_COMPACTION);
                auto& infos = _tablet->get_sample_infos(ReaderType::READER_BASE_COMPACTION);
                std::unique_lock<std::mutex> guard(lock);
                infos.resize((i % 7) + 1);
                for (size_t j = 0; j < infos.size(); ++j) {
                    infos[j].group_data_size = i * 2;
                    infos[j].bytes = i * 200;
                    infos[j].rows = i * 20;
                }
            }
        } catch (...) {
            has_error = true;
        }
        completed_threads++;
    };

    // Thread simulating full compaction
    auto full_thread = [this, &has_error, &completed_threads]() {
        try {
            for (int i = 0; i < kIterations; ++i) {
                auto& lock = _tablet->get_sample_info_lock(ReaderType::READER_FULL_COMPACTION);
                auto& infos = _tablet->get_sample_infos(ReaderType::READER_FULL_COMPACTION);
                std::unique_lock<std::mutex> guard(lock);
                infos.resize((i % 3) + 1);
                for (size_t j = 0; j < infos.size(); ++j) {
                    infos[j].group_data_size = i * 3;
                    infos[j].bytes = i * 300;
                    infos[j].rows = i * 30;
                }
            }
        } catch (...) {
            has_error = true;
        }
        completed_threads++;
    };

    // Run all threads concurrently
    std::thread t1(cumu_thread);
    std::thread t2(base_thread);
    std::thread t3(full_thread);

    t1.join();
    t2.join();
    t3.join();

    EXPECT_FALSE(has_error);
    EXPECT_EQ(completed_threads, 3);
}

// Test that simulates the race condition scenario that caused the original crash
// This test verifies that concurrent resize and access on different compaction types
// don't interfere with each other
TEST_F(CompactionSampleInfoTest, SimulateOriginalCrashScenario) {
    std::atomic<bool> has_crash {false};
    std::atomic<int> completed_iterations {0};
    constexpr int kIterations = 500;

    // This simulates the scenario where:
    // - Thread A (base compaction) resizes to 5 groups and starts processing
    // - Thread B (cumu compaction) resizes to 3 groups
    // - Thread A tries to access group index 4 - with the fix, this should be safe
    //   because they use different sample_infos

    auto base_compaction_thread = [this, &has_crash, &completed_iterations]() {
        for (int iter = 0; iter < kIterations && !has_crash; ++iter) {
            auto& lock = _tablet->get_sample_info_lock(ReaderType::READER_BASE_COMPACTION);
            auto& infos = _tablet->get_sample_infos(ReaderType::READER_BASE_COMPACTION);

            // Resize to 5 groups
            {
                std::unique_lock<std::mutex> guard(lock);
                infos.resize(5);
            }

            // Access all 5 groups (simulating estimate_batch_size calls)
            for (int group_idx = 0; group_idx < 5; ++group_idx) {
                std::unique_lock<std::mutex> guard(lock);
                // This access should be safe even if cumu_compaction resizes its own infos
                if (static_cast<size_t>(group_idx) >= infos.size()) {
                    has_crash = true;
                    return;
                }
                infos[group_idx].group_data_size = iter;
            }
            completed_iterations++;
        }
    };

    auto cumu_compaction_thread = [this, &has_crash, &completed_iterations]() {
        for (int iter = 0; iter < kIterations && !has_crash; ++iter) {
            auto& lock = _tablet->get_sample_info_lock(ReaderType::READER_CUMULATIVE_COMPACTION);
            auto& infos = _tablet->get_sample_infos(ReaderType::READER_CUMULATIVE_COMPACTION);

            // Resize to 3 groups (smaller than base compaction's 5)
            {
                std::unique_lock<std::mutex> guard(lock);
                infos.resize(3);
            }

            // Access all 3 groups
            for (int group_idx = 0; group_idx < 3; ++group_idx) {
                std::unique_lock<std::mutex> guard(lock);
                if (static_cast<size_t>(group_idx) >= infos.size()) {
                    has_crash = true;
                    return;
                }
                infos[group_idx].group_data_size = iter;
            }
            completed_iterations++;
        }
    };

    auto full_compaction_thread = [this, &has_crash, &completed_iterations]() {
        for (int iter = 0; iter < kIterations && !has_crash; ++iter) {
            auto& lock = _tablet->get_sample_info_lock(ReaderType::READER_FULL_COMPACTION);
            auto& infos = _tablet->get_sample_infos(ReaderType::READER_FULL_COMPACTION);

            // Resize to 2 groups (even smaller)
            {
                std::unique_lock<std::mutex> guard(lock);
                infos.resize(2);
            }

            // Access all 2 groups
            for (int group_idx = 0; group_idx < 2; ++group_idx) {
                std::unique_lock<std::mutex> guard(lock);
                if (static_cast<size_t>(group_idx) >= infos.size()) {
                    has_crash = true;
                    return;
                }
                infos[group_idx].group_data_size = iter;
            }
            completed_iterations++;
        }
    };

    std::thread t1(base_compaction_thread);
    std::thread t2(cumu_compaction_thread);
    std::thread t3(full_compaction_thread);

    t1.join();
    t2.join();
    t3.join();

    EXPECT_FALSE(has_crash) << "Detected out-of-bounds access during concurrent compaction";
    EXPECT_EQ(completed_iterations, kIterations * 3);
}

} // namespace doris
