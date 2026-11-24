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

#include <array>
#include <cstddef>

namespace doris::io {

namespace {

struct MetricDefinition {
    const char* name;
    const char* parent;
    RuntimeProfile::Counter ReadAheadStatistics::*source;
};

constexpr auto kMetrics = std::to_array<MetricDefinition>({
        {.name = "ReadAheadPlanCalls",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::plan_calls},
        {.name = "ReadAheadPlanTime",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::plan_time},
        {.name = "ReadAheadColumnPlanTime",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::column_plan_time},
        {.name = "ReadAheadColumnInitTime",
         .parent = "ReadAheadColumnPlanTime",
         .source = &ReadAheadStatistics::column_init_time},
        {.name = "ReadAheadPageAdvanceTime",
         .parent = "ReadAheadColumnPlanTime",
         .source = &ReadAheadStatistics::page_advance_time},
        {.name = "ReadAheadWindowDiscardTime",
         .parent = "ReadAheadColumnPlanTime",
         .source = &ReadAheadStatistics::window_discard_time},
        {.name = "ReadAheadCurrentBatchPlanTime",
         .parent = "ReadAheadColumnPlanTime",
         .source = &ReadAheadStatistics::current_batch_plan_time},
        {.name = "ReadAheadWindowExtendTime",
         .parent = "ReadAheadColumnPlanTime",
         .source = &ReadAheadStatistics::window_extend_time},
        {.name = "ReadAheadCandidatePages",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::candidate_pages},
        {.name = "ReadAheadCandidateBytes",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::candidate_bytes},
        {.name = "ReadAheadPageCacheHits",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::page_cache_hits},
        {.name = "ReadAheadPageCacheHitBytes",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::page_cache_hit_bytes},
        {.name = "ReadAheadInputPages",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::input_pages},
        {.name = "ReadAheadInputBytes",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::input_bytes},
        {.name = "ReadAheadCoalescedRanges",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::coalesced_ranges},
        {.name = "ReadAheadCoalescedBytes",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::coalesced_bytes},
        {.name = "ReadAheadBlockFillBytes",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::block_fill_bytes},
        {.name = "ReadAheadPlannedRanges",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::planned_ranges},
        {.name = "ReadAheadPlannedBytes",
         .parent = "ReadAheadPlanning",
         .source = &ReadAheadStatistics::planned_bytes},
        {.name = "ReadAheadSubmittedRanges",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::submitted_ranges},
        {.name = "ReadAheadSubmittedBytes",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::submitted_bytes},
        {.name = "ReadAheadRejectedBatches",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::rejected_batches},
        {.name = "ReadAheadQueryBudgetRejectedBatches",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::query_budget_rejected_batches},
        {.name = "ReadAheadBEBudgetRejectedBatches",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::be_budget_rejected_batches},
        {.name = "ReadAheadSubmitFailedRanges",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::submit_failed_ranges},
        {.name = "ReadAheadCompletedRanges",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::completed_ranges},
        {.name = "ReadAheadFailedRanges",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::failed_ranges},
        {.name = "ReadAheadCancelledRanges",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::cancelled_ranges},
        {.name = "ReadAheadQueueWaitTime",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::queue_wait_time},
        {.name = "ReadAheadReadTime",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::read_time},
        {.name = "ReadAheadSourceBytes",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::source_bytes},
        {.name = "ReadAheadSuccessfulSourceBytes",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::successful_source_bytes},
        {.name = "ReadAheadRemoteBytes",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::remote_bytes},
        {.name = "ReadAheadLocalBytes",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::local_bytes},
        {.name = "ReadAheadRemoteRequests",
         .parent = "ReadAheadIO",
         .source = &ReadAheadStatistics::remote_requests},
        {.name = "ReadAheadReadyPageHits",
         .parent = "ReadAheadConsume",
         .source = &ReadAheadStatistics::ready_page_hits},
        {.name = "ReadAheadWaitPageHits",
         .parent = "ReadAheadConsume",
         .source = &ReadAheadStatistics::wait_page_hits},
        {.name = "ReadAheadWaitTime",
         .parent = "ReadAheadConsume",
         .source = &ReadAheadStatistics::wait_time},
        {.name = "ReadAheadFallbackPages",
         .parent = "ReadAheadConsume",
         .source = &ReadAheadStatistics::fallback_pages},
        {.name = "ReadAheadFallbackBytes",
         .parent = "ReadAheadConsume",
         .source = &ReadAheadStatistics::fallback_bytes},
        {.name = "ReadAheadFallbackTime",
         .parent = "ReadAheadConsume",
         .source = &ReadAheadStatistics::fallback_time},
        {.name = "ReadAheadConsumedPageBytes",
         .parent = "ReadAheadConsume",
         .source = &ReadAheadStatistics::consumed_page_bytes},
        {.name = "ReadAheadConsumedRangeBytes",
         .parent = "ReadAheadConsume",
         .source = &ReadAheadStatistics::consumed_range_bytes},
        {.name = "ReadAheadReleasedPages",
         .parent = "ReadAheadConsume",
         .source = &ReadAheadStatistics::released_pages},
        {.name = "ReadAheadWritebackTime",
         .parent = "ReadAheadWriteback",
         .source = &ReadAheadStatistics::writeback_time},
        {.name = "ReadAheadCompleteBlockSubmitTime",
         .parent = "ReadAheadWritebackTime",
         .source = &ReadAheadStatistics::complete_block_submit_time},
        {.name = "ReadAheadPartialBlockSubmitTime",
         .parent = "ReadAheadWritebackTime",
         .source = &ReadAheadStatistics::partial_block_submit_time},
        {.name = "ReadAheadCompleteBlocksSubmitted",
         .parent = "ReadAheadWriteback",
         .source = &ReadAheadStatistics::complete_blocks_submitted},
        {.name = "ReadAheadCompleteBlockBytes",
         .parent = "ReadAheadWriteback",
         .source = &ReadAheadStatistics::complete_block_bytes},
        {.name = "ReadAheadPartialBlocksQueued",
         .parent = "ReadAheadWriteback",
         .source = &ReadAheadStatistics::partial_blocks_queued},
        {.name = "ReadAheadPartialBlocksMerged",
         .parent = "ReadAheadWriteback",
         .source = &ReadAheadStatistics::partial_blocks_merged},
        {.name = "ReadAheadPartialFragmentBytes",
         .parent = "ReadAheadWriteback",
         .source = &ReadAheadStatistics::partial_fragment_bytes},
        {.name = "ReadAheadWritebackDeduplicatedBlocks",
         .parent = "ReadAheadWriteback",
         .source = &ReadAheadStatistics::writeback_deduplicated_blocks},
        {.name = "ReadAheadWritebackRejectedBlocks",
         .parent = "ReadAheadWriteback",
         .source = &ReadAheadStatistics::writeback_rejected_blocks},
});

} // namespace

ReadAheadProfileReporter::ReadAheadProfileReporter(RuntimeProfile& profile,
                                                   const ReadAheadStatistics& schema) {
    static_assert(kMetrics.size() == kMetricCount);
    ADD_LABEL_COUNTER(&profile, "PageReadAhead");
    for (const char* group :
         {"ReadAheadPlanning", "ReadAheadIO", "ReadAheadConsume", "ReadAheadWriteback"}) {
        ADD_CHILD_COUNTER(&profile, group, TUnit::NONE, "PageReadAhead");
    }
    for (std::size_t i = 0; i < kMetrics.size(); ++i) {
        const auto& metric = kMetrics[i];
        _bindings[i] = {
                .destination = ADD_CHILD_COUNTER(&profile, metric.name,
                                                 (schema.*metric.source).type(), metric.parent),
                .source = metric.source};
    }
}

void ReadAheadProfileReporter::update(ReadAheadStatistics& statistics) const {
    for (const auto& binding : _bindings) {
        auto& source = statistics.*binding.source;
        const int64_t delta = source.value();
        COUNTER_UPDATE(binding.destination, delta);
        // Subtract the snapshot, not the current value: an asynchronous increment of 3 after
        // reading 5 must leave 3 for the next report rather than being reset to zero.
        COUNTER_UPDATE(&source, -delta);
    }
}

void ReadAheadStatistics::update_profile(RuntimeProfile* profile) {
    DORIS_CHECK(profile != nullptr);
    ReadAheadProfileReporter reporter(*profile, *this);
    reporter.update(*this);
}

ReadAheadBvars& read_ahead_bvars() {
    static ReadAheadBvars metrics;
    return metrics;
}

} // namespace doris::io
