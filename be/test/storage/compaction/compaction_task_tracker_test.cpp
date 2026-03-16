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

#include "storage/compaction/compaction_task_tracker.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/config.h"

namespace doris {

class CompactionTaskTrackerTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_max_records = config::compaction_profile_max_records;
        config::compaction_profile_max_records = 100;
        CompactionTaskTracker::instance()->clear_for_test();
    }

    void TearDown() override {
        CompactionTaskTracker::instance()->clear_for_test();
        config::compaction_profile_max_records = _saved_max_records;
    }

    CompactionTaskInfo make_info(int64_t id, CompactionTaskStatus status,
                                 TriggerMethod trigger = TriggerMethod::BACKGROUND) {
        CompactionTaskInfo info;
        info.compaction_id = id;
        info.backend_id = 1;
        info.table_id = 100;
        info.partition_id = 200;
        info.tablet_id = 300 + id;
        info.compaction_type = CompactionProfileType::BASE;
        info.status = status;
        info.trigger_method = trigger;
        info.compaction_score = 10;
        info.scheduled_time_ms = 1000000;
        info.input_rowsets_count = 5;
        info.input_row_num = 1000;
        info.input_data_size = 10000;
        info.input_segments_num = 3;
        info.input_version_range = "[0-5]";
        return info;
    }

    CompletionStats make_completion_stats(const std::string& status_msg_for_version = "[0-5]") {
        CompletionStats stats;
        stats.input_version_range = "[0-5]";
        stats.end_time_ms = 2000000;
        stats.merged_rows = 100;
        stats.filtered_rows = 10;
        stats.output_row_num = 890;
        stats.output_data_size = 8000;
        stats.output_segments_num = 1;
        stats.output_version = status_msg_for_version;
        stats.bytes_read_from_local = 5000;
        stats.bytes_read_from_remote = 3000;
        stats.peak_memory_bytes = 1024 * 1024;
        return stats;
    }

    // Helper to find a task by ID in a vector
    const CompactionTaskInfo* find_task(const std::vector<CompactionTaskInfo>& tasks,
                                        int64_t id) {
        for (const auto& t : tasks) {
            if (t.compaction_id == id) return &t;
        }
        return nullptr;
    }

    int32_t _saved_max_records = 0;
};

TEST_F(CompactionTaskTrackerTest, FullLifecycle_PendingToRunningToFinished) {
    auto* tracker = CompactionTaskTracker::instance();
    int64_t id = tracker->next_compaction_id();

    // Register as PENDING
    auto info = make_info(id, CompactionTaskStatus::PENDING);
    tracker->register_task(info);

    auto tasks = tracker->get_all_tasks();
    auto* task = find_task(tasks, id);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->status, CompactionTaskStatus::PENDING);
    EXPECT_EQ(task->tablet_id, 300 + id);
    EXPECT_EQ(task->input_version_range, "[0-5]");

    // Update to RUNNING
    RunningStats rs;
    rs.start_time_ms = 1500000;
    rs.is_vertical = true;
    rs.permits = 42;
    tracker->update_to_running(id, rs);

    tasks = tracker->get_all_tasks();
    task = find_task(tasks, id);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->status, CompactionTaskStatus::RUNNING);
    EXPECT_EQ(task->start_time_ms, 1500000);
    EXPECT_TRUE(task->is_vertical);
    EXPECT_EQ(task->permits, 42);
    // Fields from PENDING should be preserved
    EXPECT_EQ(task->input_version_range, "[0-5]");

    // Complete
    auto cs = make_completion_stats();
    tracker->complete(id, cs);

    tasks = tracker->get_all_tasks();
    task = find_task(tasks, id);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->status, CompactionTaskStatus::FINISHED);
    EXPECT_EQ(task->merged_rows, 100);
    EXPECT_EQ(task->output_row_num, 890);
    EXPECT_EQ(task->peak_memory_bytes, 1024 * 1024);
    EXPECT_EQ(task->tablet_id, 300 + id);
    EXPECT_EQ(task->start_time_ms, 1500000);
}

TEST_F(CompactionTaskTrackerTest, DirectRunning_ManualTrigger) {
    auto* tracker = CompactionTaskTracker::instance();
    int64_t id = tracker->next_compaction_id();

    auto info = make_info(id, CompactionTaskStatus::RUNNING, TriggerMethod::MANUAL);
    info.start_time_ms = 1500000;
    tracker->register_task(info);

    auto tasks = tracker->get_all_tasks();
    auto* task = find_task(tasks, id);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->status, CompactionTaskStatus::RUNNING);
    EXPECT_EQ(task->trigger_method, TriggerMethod::MANUAL);

    tracker->complete(id, make_completion_stats());

    tasks = tracker->get_all_tasks();
    task = find_task(tasks, id);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->status, CompactionTaskStatus::FINISHED);
    EXPECT_EQ(task->trigger_method, TriggerMethod::MANUAL);
}

TEST_F(CompactionTaskTrackerTest, FailPath_StatusMsgPreserved) {
    auto* tracker = CompactionTaskTracker::instance();
    int64_t id = tracker->next_compaction_id();

    auto info = make_info(id, CompactionTaskStatus::RUNNING);
    tracker->register_task(info);

    auto cs = make_completion_stats();
    tracker->fail(id, cs, "compaction failed: tablet corruption detected");

    auto tasks = tracker->get_all_tasks();
    auto* task = find_task(tasks, id);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->status, CompactionTaskStatus::FAILED);
    EXPECT_EQ(task->status_msg, "compaction failed: tablet corruption detected");
    EXPECT_EQ(task->merged_rows, 100); // completion stats still filled
}

TEST_F(CompactionTaskTrackerTest, RemoveTask_CleansUpPending) {
    // Simulates: task registered as PENDING, then prepare fails or early return,
    // so remove_task() is called to clean up.
    auto* tracker = CompactionTaskTracker::instance();
    int64_t id = tracker->next_compaction_id();

    auto info = make_info(id, CompactionTaskStatus::PENDING);
    tracker->register_task(info);

    // Simulate early return (e.g., large task delay, tablet state change)
    tracker->remove_task(id);

    auto tasks = tracker->get_all_tasks();
    EXPECT_EQ(find_task(tasks, id), nullptr);
}

TEST_F(CompactionTaskTrackerTest, RemoveTask_NoOpAfterComplete) {
    // Verifies that remove_task() is safe to call after complete() — it's a no-op.
    // This matches the Defer pattern in _handle_compaction().
    auto* tracker = CompactionTaskTracker::instance();
    int64_t id = tracker->next_compaction_id();

    auto info = make_info(id, CompactionTaskStatus::RUNNING);
    tracker->register_task(info);
    tracker->complete(id, make_completion_stats());

    // This should be a no-op — the task is in _completed_tasks, not _active_tasks
    tracker->remove_task(id);

    auto tasks = tracker->get_all_tasks();
    auto* task = find_task(tasks, id);
    ASSERT_NE(task, nullptr); // Still in completed
    EXPECT_EQ(task->status, CompactionTaskStatus::FINISHED);
}

TEST_F(CompactionTaskTrackerTest, FallbackRecord_NoRegister) {
    // Simulates: entry point forgot to register_task(), but submit_profile_record()
    // calls complete/fail. Tracker creates a degraded record.
    auto* tracker = CompactionTaskTracker::instance();
    int64_t id = tracker->next_compaction_id();

    auto cs = make_completion_stats();
    tracker->complete(id, cs);

    auto tasks = tracker->get_all_tasks();
    auto* task = find_task(tasks, id);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->status, CompactionTaskStatus::FINISHED);
    EXPECT_EQ(task->trigger_method, TriggerMethod::BACKGROUND); // default
    EXPECT_EQ(task->scheduled_time_ms, cs.end_time_ms);         // fallback
    EXPECT_EQ(task->compaction_score, 0);                        // not available
    EXPECT_EQ(task->merged_rows, 100);                           // completion stats present
}

TEST_F(CompactionTaskTrackerTest, FallbackRecord_FailWithStatusMsg) {
    // Simulates: entry point forgot to register, execute_compact fails with a real error.
    auto* tracker = CompactionTaskTracker::instance();
    int64_t id = tracker->next_compaction_id();

    auto cs = make_completion_stats();
    tracker->fail(id, cs, "[INTERNAL_ERROR]compaction failed: disk full");

    auto tasks = tracker->get_all_tasks();
    auto* task = find_task(tasks, id);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->status, CompactionTaskStatus::FAILED);
    EXPECT_EQ(task->status_msg, "[INTERNAL_ERROR]compaction failed: disk full");
}

TEST_F(CompactionTaskTrackerTest, InputVersionRange_BackfillOnComplete) {
    // Verifies: if input_version_range was not set at registration,
    // it gets backfilled from CompletionStats.
    auto* tracker = CompactionTaskTracker::instance();
    int64_t id = tracker->next_compaction_id();

    CompactionTaskInfo info;
    info.compaction_id = id;
    info.status = CompactionTaskStatus::RUNNING;
    // input_version_range is empty
    tracker->register_task(info);

    auto cs = make_completion_stats();
    cs.input_version_range = "[2-10]";
    tracker->complete(id, cs);

    auto tasks = tracker->get_all_tasks();
    auto* task = find_task(tasks, id);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->input_version_range, "[2-10]");
}

TEST_F(CompactionTaskTrackerTest, InputVersionRange_NotOverwrittenIfAlreadySet) {
    // If input_version_range was set at registration, it should NOT be overwritten.
    auto* tracker = CompactionTaskTracker::instance();
    int64_t id = tracker->next_compaction_id();

    CompactionTaskInfo info;
    info.compaction_id = id;
    info.status = CompactionTaskStatus::RUNNING;
    info.input_version_range = "[0-5]";
    tracker->register_task(info);

    auto cs = make_completion_stats();
    cs.input_version_range = "[2-10]"; // different
    tracker->complete(id, cs);

    auto tasks = tracker->get_all_tasks();
    auto* task = find_task(tasks, id);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->input_version_range, "[0-5]"); // original preserved
}

TEST_F(CompactionTaskTrackerTest, TrimCompleted) {
    auto* tracker = CompactionTaskTracker::instance();
    config::compaction_profile_max_records = 10;

    for (int i = 0; i < 20; i++) {
        int64_t id = tracker->next_compaction_id();
        auto info = make_info(id, CompactionTaskStatus::RUNNING);
        tracker->register_task(info);
        tracker->complete(id, make_completion_stats());
    }

    auto tasks = tracker->get_all_tasks();
    int completed = 0;
    for (const auto& t : tasks) {
        if (t.status == CompactionTaskStatus::FINISHED) {
            completed++;
        }
    }
    EXPECT_LE(completed, 10);
}

TEST_F(CompactionTaskTrackerTest, DisableConfig_ClearsCompletedButKeepsActive) {
    auto* tracker = CompactionTaskTracker::instance();

    // Create a completed task
    int64_t id1 = tracker->next_compaction_id();
    auto info1 = make_info(id1, CompactionTaskStatus::RUNNING);
    tracker->register_task(info1);
    tracker->complete(id1, make_completion_stats());

    // Create an active task
    int64_t id2 = tracker->next_compaction_id();
    auto info2 = make_info(id2, CompactionTaskStatus::PENDING);
    tracker->register_task(info2);

    // Disable
    config::compaction_profile_max_records = 0;
    auto completed = tracker->get_completed_tasks();
    EXPECT_TRUE(completed.empty());

    // Active task should still be visible
    auto all = tracker->get_all_tasks();
    EXPECT_NE(find_task(all, id2), nullptr);

    tracker->remove_task(id2);
}

TEST_F(CompactionTaskTrackerTest, ConcurrentSafety) {
    auto* tracker = CompactionTaskTracker::instance();
    constexpr int kThreads = 8;
    constexpr int kOpsPerThread = 50;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; t++) {
        threads.emplace_back([tracker]() {
            for (int i = 0; i < kOpsPerThread; i++) {
                int64_t id = tracker->next_compaction_id();
                CompactionTaskInfo info;
                info.compaction_id = id;
                info.status = CompactionTaskStatus::RUNNING;
                tracker->register_task(info);

                CompletionStats cs;
                cs.end_time_ms = 1000;
                tracker->complete(id, cs);

                tracker->get_all_tasks();
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }

    auto tasks = tracker->get_all_tasks();
    int completed = 0;
    for (const auto& t : tasks) {
        if (t.status == CompactionTaskStatus::FINISHED) completed++;
    }
    EXPECT_GT(completed, 0);
}

TEST_F(CompactionTaskTrackerTest, GetCompletedTasks_FilterByTabletAndTopN) {
    auto* tracker = CompactionTaskTracker::instance();

    for (int i = 0; i < 5; i++) {
        int64_t id = tracker->next_compaction_id();
        CompactionTaskInfo info;
        info.compaction_id = id;
        info.tablet_id = (i < 3) ? 1001 : 1002;
        info.status = CompactionTaskStatus::RUNNING;
        tracker->register_task(info);
        tracker->complete(id, make_completion_stats());
    }

    auto filtered = tracker->get_completed_tasks(1001);
    EXPECT_EQ(filtered.size(), 3);
    for (const auto& t : filtered) {
        EXPECT_EQ(t.tablet_id, 1001);
    }

    auto top2 = tracker->get_completed_tasks(0, 2);
    EXPECT_EQ(top2.size(), 2);
}

TEST_F(CompactionTaskTrackerTest, ToStringFunctions) {
    EXPECT_STREQ(to_string(CompactionTaskStatus::PENDING), "PENDING");
    EXPECT_STREQ(to_string(CompactionTaskStatus::RUNNING), "RUNNING");
    EXPECT_STREQ(to_string(CompactionTaskStatus::FINISHED), "FINISHED");
    EXPECT_STREQ(to_string(CompactionTaskStatus::FAILED), "FAILED");

    EXPECT_STREQ(to_string(CompactionProfileType::BASE), "base");
    EXPECT_STREQ(to_string(CompactionProfileType::CUMULATIVE), "cumulative");
    EXPECT_STREQ(to_string(CompactionProfileType::FULL), "full");
    EXPECT_STREQ(to_string(CompactionProfileType::SINGLE_REPLICA), "single_replica");
    EXPECT_STREQ(to_string(CompactionProfileType::COLD_DATA), "cold_data");
    EXPECT_STREQ(to_string(CompactionProfileType::INDEX_CHANGE), "index_change");

    EXPECT_STREQ(to_string(TriggerMethod::MANUAL), "MANUAL");
    EXPECT_STREQ(to_string(TriggerMethod::BACKGROUND), "BACKGROUND");
}

} // namespace doris
