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

#include "storage/compaction_task_tracker.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <thread>
#include <vector>

#include "common/config.h"

namespace doris {

class CompactionTaskTrackerTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_enable = config::enable_compaction_task_tracker;
        _saved_max_records = config::compaction_task_tracker_max_records;
        config::enable_compaction_task_tracker = true;
        config::compaction_task_tracker_max_records = 10000;
    }

    void TearDown() override {
        // Drain all tasks from the singleton so tests are independent.
        _clear_tracker();
        config::enable_compaction_task_tracker = _saved_enable;
        config::compaction_task_tracker_max_records = _saved_max_records;
    }

    CompactionTaskTracker* tracker() { return CompactionTaskTracker::instance(); }

    // Helper: create a CompactionTaskInfo with sensible defaults for testing.
    CompactionTaskInfo make_info(int64_t compaction_id, int64_t tablet_id = 1001,
                                 CompactionProfileType type = CompactionProfileType::CUMULATIVE,
                                 CompactionTaskStatus status = CompactionTaskStatus::PENDING,
                                 TriggerMethod trigger = TriggerMethod::AUTO) {
        CompactionTaskInfo info;
        info.compaction_id = compaction_id;
        info.backend_id = 10001;
        info.table_id = 2001;
        info.partition_id = 3001;
        info.tablet_id = tablet_id;
        info.compaction_type = type;
        info.status = status;
        info.trigger_method = trigger;
        info.compaction_score = 42;
        info.scheduled_time_ms = 1000000;
        info.input_rowsets_count = 5;
        info.input_row_num = 50000;
        info.input_data_size = 10000000;
        info.input_index_size = 200000;
        info.input_total_size = 10200000;
        info.input_segments_num = 5;
        info.input_version_range = "[0-5]";
        return info;
    }

    // Helper: create a CompletionStats with test data.
    CompletionStats make_completion_stats() {
        CompletionStats stats;
        stats.input_version_range = "[0-5]";
        stats.end_time_ms = 2000000;
        stats.merged_rows = 1000;
        stats.filtered_rows = 50;
        stats.output_rows = 48950;
        stats.output_row_num = 48950;
        stats.output_data_size = 5000000;
        stats.output_index_size = 100000;
        stats.output_total_size = 5100000;
        stats.output_segments_num = 1;
        stats.output_version = "[0-5]";
        stats.merge_latency_ms = 200;
        stats.bytes_read_from_local = 10000000;
        stats.bytes_read_from_remote = 0;
        stats.peak_memory_bytes = 33554432;
        return stats;
    }

    // Helper: find a task by compaction_id in a vector.
    const CompactionTaskInfo* find_task(const std::vector<CompactionTaskInfo>& tasks,
                                        int64_t compaction_id) {
        for (const auto& t : tasks) {
            if (t.compaction_id == compaction_id) {
                return &t;
            }
        }
        return nullptr;
    }

private:
    void _clear_tracker() { tracker()->clear_for_test(); }

    bool _saved_enable;
    int32_t _saved_max_records;
};

// 1. Full lifecycle: PENDING -> RUNNING -> update_progress -> FINISHED
TEST_F(CompactionTaskTrackerTest, FullLifecycle_PendingToRunningToFinished) {
    int64_t id = tracker()->next_compaction_id();
    auto info = make_info(id);
    tracker()->register_task(info);

    // Verify PENDING state.
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->status, CompactionTaskStatus::PENDING);
        EXPECT_EQ(task->tablet_id, 1001);
        EXPECT_EQ(task->compaction_type, CompactionProfileType::CUMULATIVE);
        EXPECT_EQ(task->trigger_method, TriggerMethod::AUTO);
        EXPECT_EQ(task->compaction_score, 42);
        EXPECT_EQ(task->scheduled_time_ms, 1000000);
        EXPECT_EQ(task->start_time_ms, 0);
        EXPECT_EQ(task->end_time_ms, 0);
        EXPECT_EQ(task->input_rowsets_count, 5);
        EXPECT_EQ(task->input_row_num, 50000);
        EXPECT_EQ(task->input_data_size, 10000000);
        EXPECT_EQ(task->is_vertical, false);
        EXPECT_EQ(task->permits, 0);
    }

    // Transition to RUNNING.
    RunningStats rs;
    rs.start_time_ms = 1500000;
    rs.is_vertical = true;
    rs.permits = 10200000;
    tracker()->update_to_running(id, rs);

    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->status, CompactionTaskStatus::RUNNING);
        EXPECT_EQ(task->start_time_ms, 1500000);
        EXPECT_EQ(task->is_vertical, true);
        EXPECT_EQ(task->permits, 10200000);
        // Identity and input stats should be preserved.
        EXPECT_EQ(task->tablet_id, 1001);
        EXPECT_EQ(task->input_rowsets_count, 5);
        // Output not yet available.
        EXPECT_EQ(task->output_row_num, 0);
        EXPECT_EQ(task->end_time_ms, 0);
    }

    // Update progress.
    tracker()->update_progress(id, 4, 2);
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->vertical_total_groups, 4);
        EXPECT_EQ(task->vertical_completed_groups, 2);
    }

    // Complete.
    auto stats = make_completion_stats();
    tracker()->complete(id, stats);

    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->status, CompactionTaskStatus::FINISHED);
        EXPECT_EQ(task->end_time_ms, 2000000);
        EXPECT_EQ(task->merged_rows, 1000);
        EXPECT_EQ(task->filtered_rows, 50);
        EXPECT_EQ(task->output_rows, 48950);
        EXPECT_EQ(task->output_row_num, 48950);
        EXPECT_EQ(task->output_data_size, 5000000);
        EXPECT_EQ(task->output_index_size, 100000);
        EXPECT_EQ(task->output_total_size, 5100000);
        EXPECT_EQ(task->output_segments_num, 1);
        EXPECT_EQ(task->output_version, "[0-5]");
        EXPECT_EQ(task->merge_latency_ms, 200);
        EXPECT_EQ(task->bytes_read_from_local, 10000000);
        EXPECT_EQ(task->bytes_read_from_remote, 0);
        EXPECT_EQ(task->peak_memory_bytes, 33554432);
        // Identity preserved through full lifecycle.
        EXPECT_EQ(task->compaction_id, id);
        EXPECT_EQ(task->tablet_id, 1001);
        EXPECT_EQ(task->trigger_method, TriggerMethod::AUTO);
        // Running stats preserved.
        EXPECT_EQ(task->is_vertical, true);
        EXPECT_EQ(task->permits, 10200000);
        EXPECT_EQ(task->start_time_ms, 1500000);
    }
}

// 2. Direct running path (manual trigger, no PENDING stage).
TEST_F(CompactionTaskTrackerTest, DirectRunning_ManualTrigger) {
    int64_t id = tracker()->next_compaction_id();
    auto info = make_info(id, 2002, CompactionProfileType::BASE, CompactionTaskStatus::RUNNING,
                          TriggerMethod::MANUAL);
    info.start_time_ms = 1000000;
    info.is_vertical = false;
    info.permits = 5000000;
    tracker()->register_task(info);

    // Verify directly RUNNING.
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->status, CompactionTaskStatus::RUNNING);
        EXPECT_EQ(task->trigger_method, TriggerMethod::MANUAL);
        EXPECT_EQ(task->compaction_type, CompactionProfileType::BASE);
        EXPECT_EQ(task->start_time_ms, 1000000);
    }

    // Complete.
    tracker()->complete(id, make_completion_stats());

    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->status, CompactionTaskStatus::FINISHED);
        EXPECT_EQ(task->trigger_method, TriggerMethod::MANUAL);
    }
}

// 3. Fail path: status_msg is preserved.
TEST_F(CompactionTaskTrackerTest, FailPath_StatusMsgPreserved) {
    int64_t id = tracker()->next_compaction_id();
    tracker()->register_task(make_info(id));

    RunningStats rs;
    rs.start_time_ms = 1500000;
    rs.is_vertical = false;
    rs.permits = 1000;
    tracker()->update_to_running(id, rs);

    std::string error_msg = "checksum verification failed";
    auto stats = make_completion_stats();
    tracker()->fail(id, stats, error_msg);

    auto tasks = tracker()->get_all_tasks();
    const auto* task = find_task(tasks, id);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->status, CompactionTaskStatus::FAILED);
    EXPECT_EQ(task->status_msg, "checksum verification failed");
    // Output stats should still be filled even on failure.
    EXPECT_EQ(task->output_row_num, 48950);
    EXPECT_EQ(task->end_time_ms, 2000000);
}

// 4. Prepare failure: register then remove_task cleans up.
TEST_F(CompactionTaskTrackerTest, PrepareFailure_RemoveTask) {
    int64_t id = tracker()->next_compaction_id();
    tracker()->register_task(make_info(id));

    // Verify task exists.
    {
        auto tasks = tracker()->get_all_tasks();
        EXPECT_NE(find_task(tasks, id), nullptr);
    }

    // Remove (simulating prepare failure cleanup).
    tracker()->remove_task(id);

    // Task should be gone.
    {
        auto tasks = tracker()->get_all_tasks();
        EXPECT_EQ(find_task(tasks, id), nullptr);
    }
}

// 5. Thread pool submit failure: register(PENDING) then remove_task.
TEST_F(CompactionTaskTrackerTest, ThreadPoolSubmitFailure_Cleanup) {
    int64_t id = tracker()->next_compaction_id();
    auto info = make_info(id);
    info.status = CompactionTaskStatus::PENDING;
    tracker()->register_task(info);

    // Verify PENDING.
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->status, CompactionTaskStatus::PENDING);
    }

    // Simulate thread pool submit failure -> cleanup.
    tracker()->remove_task(id);

    // No dirty PENDING record should remain.
    {
        auto tasks = tracker()->get_all_tasks();
        EXPECT_EQ(find_task(tasks, id), nullptr);
    }
}

// 6. TRY_LOCK_FAILED via queue path: PENDING -> RUNNING -> remove_task.
TEST_F(CompactionTaskTrackerTest, TryLockFailed_QueuePath) {
    int64_t id = tracker()->next_compaction_id();
    tracker()->register_task(make_info(id));

    RunningStats rs;
    rs.start_time_ms = 1500000;
    rs.is_vertical = false;
    rs.permits = 1000;
    tracker()->update_to_running(id, rs);

    // Verify RUNNING.
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->status, CompactionTaskStatus::RUNNING);
    }

    // execute_compact returns TRY_LOCK_FAILED, submit_profile_record not called.
    // _handle_compaction does idempotent remove_task.
    tracker()->remove_task(id);

    // Task should be cleaned up.
    {
        auto tasks = tracker()->get_all_tasks();
        EXPECT_EQ(find_task(tasks, id), nullptr);
    }
}

// 7. TRY_LOCK_FAILED via direct path: register(RUNNING) -> remove_task.
TEST_F(CompactionTaskTrackerTest, TryLockFailed_DirectPath) {
    int64_t id = tracker()->next_compaction_id();
    auto info = make_info(id, 1001, CompactionProfileType::BASE, CompactionTaskStatus::RUNNING,
                          TriggerMethod::MANUAL);
    info.start_time_ms = 1000000;
    tracker()->register_task(info);

    // Verify RUNNING.
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->status, CompactionTaskStatus::RUNNING);
    }

    // Simulate TRY_LOCK_FAILED: execute returns early, remove_task cleans up.
    tracker()->remove_task(id);

    {
        auto tasks = tracker()->get_all_tasks();
        EXPECT_EQ(find_task(tasks, id), nullptr);
    }
}

// 8. Missed register: complete with unknown ID should not crash and should not create a record.
TEST_F(CompactionTaskTrackerTest, MissedRegister_SilentSkip) {
    int64_t unknown_id = tracker()->next_compaction_id();

    // complete() with an unregistered ID: should be a silent no-op (WARNING log).
    tracker()->complete(unknown_id, make_completion_stats());

    // No record should exist for this ID.
    auto tasks = tracker()->get_all_tasks();
    EXPECT_EQ(find_task(tasks, unknown_id), nullptr);

    // Also test fail() with unknown ID.
    int64_t unknown_id2 = tracker()->next_compaction_id();
    tracker()->fail(unknown_id2, make_completion_stats(), "some error");
    tasks = tracker()->get_all_tasks();
    EXPECT_EQ(find_task(tasks, unknown_id2), nullptr);
}

// 9. remove_task is idempotent: register -> complete -> remove_task should not affect completed.
TEST_F(CompactionTaskTrackerTest, RemoveTask_Idempotent) {
    int64_t id = tracker()->next_compaction_id();
    tracker()->register_task(make_info(id));
    tracker()->complete(id, make_completion_stats());

    // Verify task is FINISHED.
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->status, CompactionTaskStatus::FINISHED);
    }

    // remove_task after complete: should be no-op since task was already
    // extracted from _active_tasks by complete().
    tracker()->remove_task(id);

    // Completed record should still exist.
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->status, CompactionTaskStatus::FINISHED);
    }
}

// 10. Vertical compaction progress tracking.
TEST_F(CompactionTaskTrackerTest, VerticalProgress) {
    int64_t id = tracker()->next_compaction_id();
    tracker()->register_task(make_info(id));

    RunningStats rs;
    rs.start_time_ms = 1500000;
    rs.is_vertical = true;
    rs.permits = 5000000;
    tracker()->update_to_running(id, rs);

    // Initial progress: total_groups=4, completed=0.
    tracker()->update_progress(id, 4, 0);
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->vertical_total_groups, 4);
        EXPECT_EQ(task->vertical_completed_groups, 0);
    }

    // Progress: 2 of 4 groups done.
    tracker()->update_progress(id, 4, 2);
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->vertical_total_groups, 4);
        EXPECT_EQ(task->vertical_completed_groups, 2);
    }

    // All groups done.
    tracker()->update_progress(id, 4, 4);
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->vertical_total_groups, 4);
        EXPECT_EQ(task->vertical_completed_groups, 4);
    }

    // Complete.
    tracker()->complete(id, make_completion_stats());
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* task = find_task(tasks, id);
        ASSERT_NE(task, nullptr);
        EXPECT_EQ(task->status, CompactionTaskStatus::FINISHED);
        EXPECT_EQ(task->vertical_total_groups, 4);
        EXPECT_EQ(task->vertical_completed_groups, 4);
    }
}

// 11. Trim completed records when exceeding max_records.
TEST_F(CompactionTaskTrackerTest, TrimCompleted) {
    config::compaction_task_tracker_max_records = 5;

    // Add 10 completed tasks.
    std::vector<int64_t> ids;
    for (int i = 0; i < 10; i++) {
        int64_t id = tracker()->next_compaction_id();
        ids.push_back(id);
        auto info = make_info(id, 1001 + i);
        tracker()->register_task(info);
        tracker()->complete(id, make_completion_stats());
    }

    // Only the last 5 should remain in completed tasks.
    auto completed = tracker()->get_completed_tasks();
    EXPECT_EQ(completed.size(), 5);

    // The oldest 5 should have been evicted; the newest 5 should remain.
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(find_task(completed, ids[i]), nullptr)
                << "Task " << ids[i] << " (index " << i << ") should have been evicted";
    }
    for (int i = 5; i < 10; i++) {
        EXPECT_NE(find_task(completed, ids[i]), nullptr)
                << "Task " << ids[i] << " (index " << i << ") should be present";
    }

    // get_all_tasks should also show exactly 5 (no active tasks remain).
    auto all = tracker()->get_all_tasks();
    EXPECT_EQ(all.size(), 5);
}

// 12. Disable switch: when disabled, push operations are no-ops, queries return empty.
TEST_F(CompactionTaskTrackerTest, DisableSwitch) {
    config::enable_compaction_task_tracker = false;

    int64_t id = tracker()->next_compaction_id();
    tracker()->register_task(make_info(id));

    // get_all_tasks should be empty because register was a no-op.
    auto tasks = tracker()->get_all_tasks();
    EXPECT_TRUE(tasks.empty());

    // Re-enable: registrations should work again.
    config::enable_compaction_task_tracker = true;

    int64_t id2 = tracker()->next_compaction_id();
    tracker()->register_task(make_info(id2));

    tasks = tracker()->get_all_tasks();
    EXPECT_EQ(tasks.size(), 1);
    EXPECT_NE(find_task(tasks, id2), nullptr);

    // The first ID should still not be present (was registered while disabled).
    EXPECT_EQ(find_task(tasks, id), nullptr);
}

// 13. get_completed_tasks filter tests.
TEST_F(CompactionTaskTrackerTest, GetCompletedTasks_Filters) {
    // Create a diverse set of completed tasks:
    // T1: tablet=1001, CUMULATIVE, FINISHED
    // T2: tablet=1001, BASE, FAILED
    // T3: tablet=2002, CUMULATIVE, FINISHED
    // T4: tablet=2002, FULL, FINISHED
    // T5: tablet=1001, CUMULATIVE, FINISHED

    auto register_and_complete = [&](int64_t tablet_id, CompactionProfileType type,
                                     bool success) -> int64_t {
        int64_t id = tracker()->next_compaction_id();
        auto info = make_info(id, tablet_id, type);
        tracker()->register_task(info);
        if (success) {
            tracker()->complete(id, make_completion_stats());
        } else {
            tracker()->fail(id, make_completion_stats(), "test failure");
        }
        return id;
    };

    int64_t t1 = register_and_complete(1001, CompactionProfileType::CUMULATIVE, true);
    int64_t t2 = register_and_complete(1001, CompactionProfileType::BASE, false);
    int64_t t3 = register_and_complete(2002, CompactionProfileType::CUMULATIVE, true);
    int64_t t4 = register_and_complete(2002, CompactionProfileType::FULL, true);
    int64_t t5 = register_and_complete(1001, CompactionProfileType::CUMULATIVE, true);

    // No filter: should return all 5.
    {
        auto result = tracker()->get_completed_tasks();
        EXPECT_EQ(result.size(), 5);
    }

    // Filter by tablet_id=1001: should return 3 (t1, t2, t5).
    {
        auto result = tracker()->get_completed_tasks(1001);
        EXPECT_EQ(result.size(), 3);
        for (const auto& r : result) {
            EXPECT_EQ(r.tablet_id, 1001);
        }
    }

    // Filter by tablet_id=2002: should return 2 (t3, t4).
    {
        auto result = tracker()->get_completed_tasks(2002);
        EXPECT_EQ(result.size(), 2);
    }

    // top_n=2: should return only 2 (newest first: t5, t4).
    {
        auto result = tracker()->get_completed_tasks(0, 2);
        EXPECT_EQ(result.size(), 2);
        // Newest first (reverse iteration): t5 then t4.
        EXPECT_EQ(result[0].compaction_id, t5);
        EXPECT_EQ(result[1].compaction_id, t4);
    }

    // compact_type="cumulative": should return t1, t3, t5.
    {
        auto result = tracker()->get_completed_tasks(0, 0, "cumulative");
        EXPECT_EQ(result.size(), 3);
        for (const auto& r : result) {
            EXPECT_EQ(r.compaction_type, CompactionProfileType::CUMULATIVE);
        }
    }

    // compact_type="base": should return t2 only.
    {
        auto result = tracker()->get_completed_tasks(0, 0, "base");
        EXPECT_EQ(result.size(), 1);
        EXPECT_EQ(result[0].compaction_id, t2);
    }

    // compact_type="full": should return t4 only.
    {
        auto result = tracker()->get_completed_tasks(0, 0, "full");
        EXPECT_EQ(result.size(), 1);
        EXPECT_EQ(result[0].compaction_id, t4);
    }

    // success_filter=1 (success only): should return t1, t3, t4, t5 (not t2).
    {
        auto result = tracker()->get_completed_tasks(0, 0, "", 1);
        EXPECT_EQ(result.size(), 4);
        for (const auto& r : result) {
            EXPECT_EQ(r.status, CompactionTaskStatus::FINISHED);
        }
    }

    // success_filter=0 (failed only): should return t2 only.
    {
        auto result = tracker()->get_completed_tasks(0, 0, "", 0);
        EXPECT_EQ(result.size(), 1);
        EXPECT_EQ(result[0].compaction_id, t2);
        EXPECT_EQ(result[0].status, CompactionTaskStatus::FAILED);
    }

    // Combined: tablet_id=1001 + compact_type="cumulative" + success_filter=1.
    // Should return t1, t5.
    {
        auto result = tracker()->get_completed_tasks(1001, 0, "cumulative", 1);
        EXPECT_EQ(result.size(), 2);
        for (const auto& r : result) {
            EXPECT_EQ(r.tablet_id, 1001);
            EXPECT_EQ(r.compaction_type, CompactionProfileType::CUMULATIVE);
            EXPECT_EQ(r.status, CompactionTaskStatus::FINISHED);
        }
    }

    // Combined with top_n: tablet_id=1001 + top_n=1.
    {
        auto result = tracker()->get_completed_tasks(1001, 1);
        EXPECT_EQ(result.size(), 1);
        EXPECT_EQ(result[0].compaction_id, t5); // newest matching
    }

    // Suppress unused variable warnings.
    (void)t1;
    (void)t3;
}

// 14. Concurrent safety: multiple threads doing register/update/complete/query simultaneously.
TEST_F(CompactionTaskTrackerTest, ConcurrentSafety) {
    const int num_tasks_per_thread = 50;
    const int num_threads = 8;

    auto worker = [&](int thread_idx) {
        for (int i = 0; i < num_tasks_per_thread; i++) {
            int64_t id = tracker()->next_compaction_id();
            auto info = make_info(id, 1000 + thread_idx);
            tracker()->register_task(info);

            RunningStats rs;
            rs.start_time_ms = 1500000;
            rs.is_vertical = (i % 2 == 0);
            rs.permits = 1000;
            tracker()->update_to_running(id, rs);

            if (rs.is_vertical) {
                tracker()->update_progress(id, 3, 1);
                tracker()->update_progress(id, 3, 3);
            }

            // Interleave queries.
            auto tasks = tracker()->get_all_tasks();
            (void)tasks;

            if (i % 3 == 0) {
                // Simulate failure.
                tracker()->fail(id, make_completion_stats(), "test error");
            } else {
                tracker()->complete(id, make_completion_stats());
            }

            // Idempotent remove_task after complete/fail.
            tracker()->remove_task(id);
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back(worker, t);
    }
    for (auto& t : threads) {
        t.join();
    }

    // All tasks should be completed. No active tasks should remain.
    auto all = tracker()->get_all_tasks();
    int active_count = 0;
    int completed_count = 0;
    for (const auto& task : all) {
        if (task.status == CompactionTaskStatus::PENDING ||
            task.status == CompactionTaskStatus::RUNNING) {
            active_count++;
        } else {
            completed_count++;
        }
    }
    EXPECT_EQ(active_count, 0);
    EXPECT_EQ(completed_count, num_tasks_per_thread * num_threads);
}

// 15. Trigger method distinction: AUTO, MANUAL, LOAD_TRIGGERED are all preserved.
TEST_F(CompactionTaskTrackerTest, TriggerMethodDistinction) {
    int64_t id_auto = tracker()->next_compaction_id();
    auto info_auto = make_info(id_auto, 1001, CompactionProfileType::CUMULATIVE,
                               CompactionTaskStatus::PENDING, TriggerMethod::AUTO);
    tracker()->register_task(info_auto);

    int64_t id_manual = tracker()->next_compaction_id();
    auto info_manual = make_info(id_manual, 1002, CompactionProfileType::BASE,
                                 CompactionTaskStatus::RUNNING, TriggerMethod::MANUAL);
    info_manual.start_time_ms = 1000000;
    tracker()->register_task(info_manual);

    int64_t id_load = tracker()->next_compaction_id();
    auto info_load = make_info(id_load, 1003, CompactionProfileType::CUMULATIVE,
                               CompactionTaskStatus::PENDING, TriggerMethod::LOAD_TRIGGERED);
    tracker()->register_task(info_load);

    // Verify each trigger method is preserved.
    {
        auto tasks = tracker()->get_all_tasks();
        const auto* t_auto = find_task(tasks, id_auto);
        ASSERT_NE(t_auto, nullptr);
        EXPECT_EQ(t_auto->trigger_method, TriggerMethod::AUTO);

        const auto* t_manual = find_task(tasks, id_manual);
        ASSERT_NE(t_manual, nullptr);
        EXPECT_EQ(t_manual->trigger_method, TriggerMethod::MANUAL);

        const auto* t_load = find_task(tasks, id_load);
        ASSERT_NE(t_load, nullptr);
        EXPECT_EQ(t_load->trigger_method, TriggerMethod::LOAD_TRIGGERED);
    }

    // Complete all and verify trigger methods are still preserved in completed records.
    RunningStats rs;
    rs.start_time_ms = 1500000;
    rs.is_vertical = false;
    rs.permits = 1000;
    tracker()->update_to_running(id_auto, rs);
    tracker()->update_to_running(id_load, rs);

    tracker()->complete(id_auto, make_completion_stats());
    tracker()->complete(id_manual, make_completion_stats());
    tracker()->complete(id_load, make_completion_stats());

    {
        auto completed = tracker()->get_completed_tasks();
        EXPECT_EQ(completed.size(), 3);

        const auto* c_auto = find_task(completed, id_auto);
        ASSERT_NE(c_auto, nullptr);
        EXPECT_EQ(c_auto->trigger_method, TriggerMethod::AUTO);

        const auto* c_manual = find_task(completed, id_manual);
        ASSERT_NE(c_manual, nullptr);
        EXPECT_EQ(c_manual->trigger_method, TriggerMethod::MANUAL);

        const auto* c_load = find_task(completed, id_load);
        ASSERT_NE(c_load, nullptr);
        EXPECT_EQ(c_load->trigger_method, TriggerMethod::LOAD_TRIGGERED);
    }
}

} // namespace doris
