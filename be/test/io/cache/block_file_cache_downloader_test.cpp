//
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

#include "io/cache/block_file_cache_downloader.h"

#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "cloud/cloud_storage_engine.h"
#include "common/config.h"

namespace doris::io {

class FileCacheBlockDownloaderTest : public testing::Test {
public:
    FileCacheBlockDownloaderTest() : _engine(CloudStorageEngine(EngineOptions {})) {}

    void SetUp() override {
        // Enable file cache for testing
        config::enable_file_cache = true;
        // Set reasonable thread pool size for testing
        config::file_cache_downloader_thread_num_min = 2;
        config::file_cache_downloader_thread_num_max = 4;
    }

    void TearDown() override { config::enable_file_cache = false; }

    // Helper to wait for inflight tasks to complete with timeout
    bool wait_for_task_done(FileCacheBlockDownloader& downloader, int64_t tablet_id,
                            int timeout_seconds = 10) {
        auto start = std::chrono::steady_clock::now();
        while (true) {
            std::vector<int64_t> tablets = {tablet_id};
            std::map<int64_t, bool> done;
            downloader.check_download_task(tablets, &done);

            if (done.contains(tablet_id) && done[tablet_id]) {
                return true;
            }

            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::steady_clock::now() - start)
                                   .count();
            if (elapsed >= timeout_seconds) {
                return false;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    // Helper to wait for multiple tablets
    bool wait_for_tasks_done(FileCacheBlockDownloader& downloader,
                             const std::vector<int64_t>& tablet_ids, int timeout_seconds = 10) {
        auto start = std::chrono::steady_clock::now();
        while (true) {
            std::map<int64_t, bool> done;
            downloader.check_download_task(tablet_ids, &done);

            bool all_done = true;
            for (int64_t tablet_id : tablet_ids) {
                if (!done.contains(tablet_id) || !done[tablet_id]) {
                    all_done = false;
                    break;
                }
            }

            if (all_done) {
                return true;
            }

            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::steady_clock::now() - start)
                                   .count();
            if (elapsed >= timeout_seconds) {
                return false;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

protected:
    CloudStorageEngine _engine;
};

// Test that inflight count is correctly decremented when tablet is not found
// This tests the bug fix where early return did not decrement inflight count
TEST_F(FileCacheBlockDownloaderTest, TestInflightCountDecrementOnTabletNotFound) {
    FileCacheBlockDownloader downloader(_engine);

    // Create a download task with a non-existent tablet
    DownloadTask::FileCacheBlockMetaVec metas;
    auto* meta = metas.Add();
    meta->set_tablet_id(99999999); // Non-existent tablet ID
    meta->set_rowset_id("non_existent_rowset");
    meta->set_segment_id(0);
    meta->set_offset(0);
    meta->set_size(1024);

    DownloadTask task(std::move(metas));

    // Submit the task - this increments inflight count
    downloader.submit_download_task(std::move(task));

    // Wait for the task to be processed with timeout
    bool done = wait_for_task_done(downloader, 99999999);

    // The task should be marked as done because inflight count was decremented
    EXPECT_TRUE(done) << "Inflight count should be decremented even when tablet not found";
}

// Test that inflight count is correctly decremented for multiple metas
// when some fail and some succeed (or all fail)
TEST_F(FileCacheBlockDownloaderTest, TestInflightCountDecrementOnMultipleMetasFail) {
    FileCacheBlockDownloader downloader(_engine);

    // Create a download task with multiple non-existent tablets
    DownloadTask::FileCacheBlockMetaVec metas;

    std::vector<int64_t> tablet_ids;
    // Add multiple metas for different tablets
    for (int i = 0; i < 5; i++) {
        auto* meta = metas.Add();
        int64_t tablet_id = 88888880 + i;
        meta->set_tablet_id(tablet_id); // Non-existent tablet IDs
        meta->set_rowset_id("non_existent_rowset_" + std::to_string(i));
        meta->set_segment_id(0);
        meta->set_offset(0);
        meta->set_size(1024);
        tablet_ids.push_back(tablet_id);
    }

    DownloadTask task(std::move(metas));

    // Submit the task
    downloader.submit_download_task(std::move(task));

    // Wait for all tasks to be processed
    bool all_done = wait_for_tasks_done(downloader, tablet_ids);

    // All tasks should be marked as done
    EXPECT_TRUE(all_done) << "All inflight counts should be decremented for non-existent tablets";
}

// Test that inflight count is correctly decremented for same tablet with multiple blocks
TEST_F(FileCacheBlockDownloaderTest, TestInflightCountDecrementSameTabletMultipleBlocks) {
    FileCacheBlockDownloader downloader(_engine);

    // Create a download task with multiple blocks for the same tablet
    DownloadTask::FileCacheBlockMetaVec metas;

    int64_t tablet_id = 77777777;
    for (int i = 0; i < 3; i++) {
        auto* meta = metas.Add();
        meta->set_tablet_id(tablet_id); // Same tablet
        meta->set_rowset_id("non_existent_rowset");
        meta->set_segment_id(i);
        meta->set_offset(i * 1024);
        meta->set_size(1024);
    }

    DownloadTask task(std::move(metas));

    // Submit the task - this increments inflight count by 3 for the same tablet
    downloader.submit_download_task(std::move(task));

    // Wait for the task to be processed
    bool done = wait_for_task_done(downloader, tablet_id);

    // The task should be marked as done
    // All 3 blocks should have decremented the count, resulting in removal from map
    EXPECT_TRUE(done) << "Inflight count should be zero after all blocks processed for tablet "
                      << tablet_id;
}

// Test check_download_task returns true for tablets that were never submitted
TEST_F(FileCacheBlockDownloaderTest, TestCheckDownloadTaskNonExistentTablet) {
    FileCacheBlockDownloader downloader(_engine);

    // Check a tablet that was never submitted
    std::vector<int64_t> tablets = {11111111};
    std::map<int64_t, bool> done;
    downloader.check_download_task(tablets, &done);

    // Should return true (done) because it's not in inflight map
    ASSERT_TRUE(done.contains(11111111));
    EXPECT_TRUE(done[11111111]) << "Non-existent tablet should be reported as done";
}

// Test that check_download_task correctly reports in-flight status
TEST_F(FileCacheBlockDownloaderTest, TestCheckDownloadTaskInflightStatus) {
    FileCacheBlockDownloader downloader(_engine);

    int64_t tablet_id = 66666666;

    // Initially, should be done (not in inflight)
    {
        std::vector<int64_t> tablets = {tablet_id};
        std::map<int64_t, bool> done;
        downloader.check_download_task(tablets, &done);
        EXPECT_TRUE(done[tablet_id]) << "Initially tablet should not be in inflight";
    }

    // Submit a task
    DownloadTask::FileCacheBlockMetaVec metas;
    auto* meta = metas.Add();
    meta->set_tablet_id(tablet_id);
    meta->set_rowset_id("non_existent_rowset");
    meta->set_segment_id(0);
    meta->set_offset(0);
    meta->set_size(1024);

    DownloadTask task(std::move(metas));
    downloader.submit_download_task(std::move(task));

    // After task completes (should be quick since tablet doesn't exist)
    bool done = wait_for_task_done(downloader, tablet_id);
    EXPECT_TRUE(done) << "Task should complete with decremented inflight count";
}

} // namespace doris::io
