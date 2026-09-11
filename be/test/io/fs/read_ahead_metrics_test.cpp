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

#include "io/fs/read_ahead_metrics.h"

#include <gtest/gtest.h>

#include <memory>
#include <thread>

namespace doris::io {

TEST(ReadAheadMetricsTest, ReportsOnlyNewDeltasAndAggregatesReaders) {
    ReadAheadStatistics first;
    ReadAheadStatistics second;
    RuntimeProfile profile("SegmentIterator");

    COUNTER_UPDATE(&first.input_pages, 2);
    COUNTER_UPDATE(&first.input_bytes, 48);
    COUNTER_UPDATE(&first.plan_time, 100);
    COUNTER_UPDATE(&first.column_plan_time, 1000);
    COUNTER_UPDATE(&first.column_init_time, 200);
    COUNTER_UPDATE(&first.window_discard_time, 100);
    COUNTER_UPDATE(&first.current_batch_plan_time, 600);
    COUNTER_UPDATE(&first.window_extend_time, 50);
    COUNTER_UPDATE(&first.candidate_bytes, 60);
    COUNTER_UPDATE(&first.page_cache_hit_bytes, 12);
    COUNTER_UPDATE(&first.query_budget_rejected_batches, 1);
    COUNTER_UPDATE(&first.be_budget_rejected_batches, 2);
    COUNTER_UPDATE(&first.submit_failed_ranges, 3);
    COUNTER_UPDATE(&first.successful_source_bytes, 40);
    COUNTER_UPDATE(&first.fallback_pages, 1);
    COUNTER_UPDATE(&first.fallback_bytes, 8);
    COUNTER_UPDATE(&first.fallback_time, 200);
    first.update_profile(&profile);
    first.update_profile(&profile);
    COUNTER_UPDATE(&first.input_bytes, 12);
    COUNTER_UPDATE(&second.input_bytes, 20);
    COUNTER_UPDATE(&second.column_plan_time, 300);
    first.update_profile(&profile);
    second.update_profile(&profile);

    EXPECT_EQ(profile.get_counter("PageReadAhead")->type(), TUnit::NONE);
    EXPECT_EQ(profile.get_counter("ReadAheadInputPages")->value(), 2);
    EXPECT_EQ(profile.get_counter("ReadAheadInputBytes")->value(), 80);
    EXPECT_EQ(profile.get_counter("ReadAheadInputBytes")->type(), TUnit::BYTES);
    EXPECT_EQ(profile.get_counter("ReadAheadPlanTime")->value(), 100);
    EXPECT_EQ(profile.get_counter("ReadAheadPlanTime")->type(), TUnit::TIME_NS);
    EXPECT_EQ(profile.get_counter("ReadAheadColumnPlanTime")->value(), 1300);
    EXPECT_EQ(profile.get_counter("ReadAheadColumnInitTime")->value(), 200);
    EXPECT_EQ(profile.get_counter("ReadAheadWindowDiscardTime")->value(), 100);
    EXPECT_EQ(profile.get_counter("ReadAheadCurrentBatchPlanTime")->value(), 600);
    EXPECT_EQ(profile.get_counter("ReadAheadWindowExtendTime")->value(), 50);
    EXPECT_EQ(profile.get_counter("ReadAheadCandidateBytes")->value(), 60);
    EXPECT_EQ(profile.get_counter("ReadAheadPageCacheHitBytes")->value(), 12);
    EXPECT_EQ(profile.get_counter("ReadAheadQueryBudgetRejectedBatches")->value(), 1);
    EXPECT_EQ(profile.get_counter("ReadAheadBEBudgetRejectedBatches")->value(), 2);
    EXPECT_EQ(profile.get_counter("ReadAheadSubmitFailedRanges")->value(), 3);
    EXPECT_EQ(profile.get_counter("ReadAheadSuccessfulSourceBytes")->value(), 40);
    EXPECT_EQ(profile.get_counter("ReadAheadFallbackPages")->value(), 1);
    EXPECT_EQ(profile.get_counter("ReadAheadFallbackBytes")->value(), 8);
    EXPECT_EQ(profile.get_counter("ReadAheadFallbackTime")->value(), 200);
    EXPECT_EQ(first.input_bytes.value(), 0);
    EXPECT_EQ(second.input_bytes.value(), 0);
    EXPECT_EQ(first.column_plan_time.value(), 0);
    EXPECT_EQ(first.column_init_time.value(), 0);
    EXPECT_EQ(first.window_discard_time.value(), 0);
    EXPECT_EQ(first.current_batch_plan_time.value(), 0);
    EXPECT_EQ(first.window_extend_time.value(), 0);
    EXPECT_EQ(second.column_plan_time.value(), 0);

    TRuntimeProfileTree tree;
    profile.to_thrift(&tree);
    const auto& children = tree.nodes.at(0).child_counters_map;
    EXPECT_EQ(children.at("ReadAheadPlanning").count("ReadAheadColumnPlanTime"), 1);
    EXPECT_EQ(children.at("ReadAheadColumnPlanTime").count("ReadAheadColumnInitTime"), 1);
    EXPECT_EQ(children.at("ReadAheadColumnPlanTime").count("ReadAheadWindowDiscardTime"), 1);
    EXPECT_EQ(children.at("ReadAheadColumnPlanTime").count("ReadAheadCurrentBatchPlanTime"), 1);
    EXPECT_EQ(children.at("ReadAheadCurrentBatchPlanTime").count("ReadAheadWindowExtendTime"), 1);
}

TEST(ReadAheadMetricsTest, PreservesConcurrentUpdatesDuringReporting) {
    auto statistics = std::make_shared<ReadAheadStatistics>();
    RuntimeProfile profile("SegmentIterator");
    constexpr int64_t updates = 50000;
    std::thread worker([statistics]() {
        for (int64_t index = 0; index < updates; ++index) {
            COUNTER_UPDATE(&statistics->remote_bytes, 1);
        }
    });
    for (int report = 0; report < 100; ++report) {
        statistics->update_profile(&profile);
    }
    worker.join();
    statistics->update_profile(&profile);
    EXPECT_EQ(profile.get_counter("ReadAheadRemoteBytes")->value(), updates);
    EXPECT_EQ(statistics->remote_bytes.value(), 0);
}

TEST(ReadAheadMetricsTest, CountersOutliveReportedProfile) {
    auto statistics = std::make_shared<ReadAheadStatistics>();
    {
        RuntimeProfile profile("ClosedScanner");
        COUNTER_UPDATE(&statistics->completed_ranges, 1);
        statistics->update_profile(&profile);
    }
    std::thread worker([statistics]() { COUNTER_UPDATE(&statistics->completed_ranges, 1); });
    worker.join();
    RuntimeProfile next_report("RemainingSnapshot");
    statistics->update_profile(&next_report);
    EXPECT_EQ(next_report.get_counter("ReadAheadCompletedRanges")->value(), 1);
}

} // namespace doris::io
