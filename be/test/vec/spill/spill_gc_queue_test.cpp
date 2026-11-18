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

#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "exec/spill/local_spill_data_dir.h"
#include "exec/spill/spill_file_manager.h"
#include "io/fs/local_file_system.h"

namespace doris {

class SpillGcQueueTest : public testing::Test {
protected:
    using Pending = SpillFileManager::PendingSpillDirectory;
    using PendingList = std::list<Pending>;

    Pending _entry(std::string dir) {
        return {.failed_count = 0,
                .dir = dir,
                .query_id = dir.substr(0, dir.find('/')),
                .data_dir = &_store,
                .fs = _fs,
                .charged_bytes = 1,
                .persisted_bytes = 2,
                .file_log_contexts = {std::move(dir)}};
    }

    std::unique_ptr<SpillFileManager> _manager() {
        return std::make_unique<SpillFileManager>(
                std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> {});
    }

    static PendingList _coalesce(PendingList entries) {
        return SpillFileManager::_coalesce_pending_directories(std::move(entries));
    }

    PendingList _parent_batch(bool parent_first) {
        PendingList batch;
        auto parent = _entry("query/run-1");
        parent.failed_count = 7;
        if (parent_first) {
            batch.emplace_back(parent);
        }
        batch.emplace_back(_entry("query/run-1/part"));
        batch.emplace_back(_entry("query/run-10")); // Not under run-1.
        batch.emplace_back(_entry("query/run-1/part/nested"));
        if (!parent_first) {
            batch.emplace_back(std::move(parent));
        }
        batch.emplace_back(_entry("query/run-1")); // Exact duplicate.
        return batch;
    }

    static void _expect_parent_metadata(const PendingList& batch) {
        ASSERT_EQ(batch.size(), 2);
        EXPECT_EQ(batch.front().dir, "query/run-1");
        EXPECT_EQ(batch.front().charged_bytes, 4);
        EXPECT_EQ(batch.front().persisted_bytes, 8);
        EXPECT_EQ(batch.front().failed_count, 7);
        EXPECT_EQ(batch.front().file_log_contexts.size(), 4);
        EXPECT_EQ(batch.back().dir, "query/run-10");
        EXPECT_EQ(batch.back().charged_bytes, 1);
    }

    LocalSpillDataDir _store {"spill_gc_queue_test_store", 0};
    io::FileSystemSPtr _fs = io::global_local_filesystem();
};

TEST_F(SpillGcQueueTest, EnqueueDefersCoalescingUntilGc) {
    auto manager = _manager();
    for (const auto& dir : {"query/run", "query", "query/run"}) {
        manager->queue_spill_directory_deletion(&_store, _fs, dir, 1, 2, "query", {dir});
    }
    // Producer code preserves raw requests, even when a parent or duplicate is queued.
    EXPECT_EQ(manager->pending_delete_dir_count(), 3);
    EXPECT_EQ(manager->_pending_spill_directories.front().dir, "query/run");
    auto batch = _coalesce(std::move(manager->_pending_spill_directories));
    ASSERT_EQ(batch.size(), 1);
    EXPECT_EQ(batch.front().dir, "query");
    EXPECT_EQ(batch.front().charged_bytes, 3);
    EXPECT_EQ(batch.front().persisted_bytes, 6);
    EXPECT_EQ(batch.front().file_log_contexts.size(), 3);
}

TEST_F(SpillGcQueueTest, SiblingBacklogKeepsFifoOrder) {
    PendingList batch;
    constexpr int kFiles = 8192;
    for (int i = kFiles; i > 0; --i) {
        batch.emplace_back(_entry("query/run-" + std::to_string(i)));
    }
    batch = _coalesce(std::move(batch));
    ASSERT_EQ(batch.size(), kFiles);
    int expected = kFiles;
    for (const auto& entry : batch) {
        EXPECT_EQ(entry.dir, "query/run-" + std::to_string(expected--));
        EXPECT_EQ(entry.charged_bytes, 1);
        EXPECT_EQ(entry.persisted_bytes, 2);
    }
}

TEST_F(SpillGcQueueTest, ParentBeforeOrAfterChildrenRetainsAllMetadata) {
    for (bool parent_first : {true, false}) {
        _expect_parent_metadata(_coalesce(_parent_batch(parent_first)));
    }
}

TEST_F(SpillGcQueueTest, DifferentStoresAndFileSystemObjectsStaySeparate) {
    LocalSpillDataDir other_store {"spill_gc_queue_other_store", 0};
    // These have the same filesystem ID; identity must still be the pinned object.
    io::FileSystemSPtr other_fs(new io::LocalFileSystem());
    EXPECT_EQ(other_fs->id(), _fs->id());
    PendingList batch;
    batch.emplace_back(_entry("query"));
    auto other_disk = _entry("query/run");
    other_disk.data_dir = &other_store;
    batch.emplace_back(std::move(other_disk));
    auto other_vault = _entry("query/run");
    other_vault.fs = other_fs;
    batch.emplace_back(std::move(other_vault));
    batch.emplace_back(_entry("query/run"));
    batch = _coalesce(std::move(batch));
    ASSERT_EQ(batch.size(), 3);
    auto it = batch.begin();
    EXPECT_EQ(it->dir, "query");
    EXPECT_EQ(it->charged_bytes, 2);
    EXPECT_EQ((++it)->data_dir, &other_store);
    EXPECT_EQ((++it)->fs, other_fs);
    EXPECT_EQ(it->charged_bytes, 1);
}

TEST_F(SpillGcQueueTest, EmptyAndDuplicateBatches) {
    EXPECT_TRUE(_coalesce({}).empty());
    PendingList batch;
    for (int i = 0; i < 1024; ++i) {
        batch.emplace_back(_entry("query/run"));
    }
    batch = _coalesce(std::move(batch));
    ASSERT_EQ(batch.size(), 1);
    EXPECT_EQ(batch.front().charged_bytes, 1024);
    EXPECT_EQ(batch.front().persisted_bytes, 2048);
    EXPECT_EQ(batch.front().file_log_contexts.size(), 1024);
}

TEST_F(SpillGcQueueTest, ConcurrentProducersPreserveEveryRequest) {
    auto manager = _manager();
    constexpr int kQueries = 4;
    constexpr int kFiles = 2048;
    std::vector<std::thread> producers;
    for (int query = 0; query < kQueries; ++query) {
        producers.emplace_back([&, query]() {
            const auto query_id = "query-" + std::to_string(query);
            for (int file = 0; file < kFiles; ++file) {
                manager->queue_spill_directory_deletion(&_store, _fs,
                                                        query_id + "/run-" + std::to_string(file),
                                                        1, 2, query_id, {"file-context"});
            }
            manager->queue_spill_directory_deletion(&_store, _fs, query_id, 0, 0, query_id);
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }
    EXPECT_EQ(manager->pending_delete_dir_count(), kQueries * (kFiles + 1));
    auto batch = _coalesce(std::move(manager->_pending_spill_directories));
    ASSERT_EQ(batch.size(), kQueries);
    for (const auto& entry : batch) {
        EXPECT_EQ(entry.charged_bytes, kFiles);
        EXPECT_EQ(entry.persisted_bytes, 2 * kFiles);
        EXPECT_EQ(entry.file_log_contexts.size(), kFiles);
    }
}

} // namespace doris
