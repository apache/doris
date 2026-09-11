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

namespace doris::io {

void ReadAheadStatistics::update_profile(RuntimeProfile* profile) {
    DORIS_CHECK(profile != nullptr);
    ADD_LABEL_COUNTER(profile, "PageReadAhead");
    for (const char* group :
         {"ReadAheadPlanning", "ReadAheadIO", "ReadAheadConsume", "ReadAheadWriteback"}) {
        ADD_CHILD_COUNTER(profile, group, TUnit::NONE, "PageReadAhead");
    }
    const auto update = [&](const char* name, const char* parent,
                            RuntimeProfile::Counter& counter) {
        const int64_t delta = counter.value();
        COUNTER_UPDATE(ADD_CHILD_COUNTER(profile, name, counter.type(), parent), delta);
        // Only the reporting thread drains counters. Subtract this snapshot so concurrent
        // increments remain available for the next report.
        COUNTER_UPDATE(&counter, -delta);
    };
    update("ReadAheadPlanCalls", "ReadAheadPlanning", plan_calls);
    update("ReadAheadPlanTime", "ReadAheadPlanning", plan_time);
    update("ReadAheadColumnPlanTime", "ReadAheadPlanning", column_plan_time);
    update("ReadAheadColumnInitTime", "ReadAheadColumnPlanTime", column_init_time);
    update("ReadAheadWindowDiscardTime", "ReadAheadColumnPlanTime", window_discard_time);
    update("ReadAheadCurrentBatchPlanTime", "ReadAheadColumnPlanTime", current_batch_plan_time);
    update("ReadAheadWindowExtendTime", "ReadAheadCurrentBatchPlanTime", window_extend_time);
    update("ReadAheadCandidatePages", "ReadAheadPlanning", candidate_pages);
    update("ReadAheadCandidateBytes", "ReadAheadPlanning", candidate_bytes);
    update("ReadAheadPageCacheHits", "ReadAheadPlanning", page_cache_hits);
    update("ReadAheadPageCacheHitBytes", "ReadAheadPlanning", page_cache_hit_bytes);
    update("ReadAheadInputPages", "ReadAheadPlanning", input_pages);
    update("ReadAheadInputBytes", "ReadAheadPlanning", input_bytes);
    update("ReadAheadCoalescedRanges", "ReadAheadPlanning", coalesced_ranges);
    update("ReadAheadCoalescedBytes", "ReadAheadPlanning", coalesced_bytes);
    update("ReadAheadBlockFillBytes", "ReadAheadPlanning", block_fill_bytes);
    update("ReadAheadPlannedRanges", "ReadAheadPlanning", planned_ranges);
    update("ReadAheadPlannedBytes", "ReadAheadPlanning", planned_bytes);
    update("ReadAheadSubmittedRanges", "ReadAheadIO", submitted_ranges);
    update("ReadAheadSubmittedBytes", "ReadAheadIO", submitted_bytes);
    update("ReadAheadRejectedBatches", "ReadAheadIO", rejected_batches);
    update("ReadAheadQueryBudgetRejectedBatches", "ReadAheadIO", query_budget_rejected_batches);
    update("ReadAheadBEBudgetRejectedBatches", "ReadAheadIO", be_budget_rejected_batches);
    update("ReadAheadSubmitFailedRanges", "ReadAheadIO", submit_failed_ranges);
    update("ReadAheadCompletedRanges", "ReadAheadIO", completed_ranges);
    update("ReadAheadFailedRanges", "ReadAheadIO", failed_ranges);
    update("ReadAheadCancelledRanges", "ReadAheadIO", cancelled_ranges);
    update("ReadAheadQueueWaitTime", "ReadAheadIO", queue_wait_time);
    update("ReadAheadReadTime", "ReadAheadIO", read_time);
    update("ReadAheadSourceBytes", "ReadAheadIO", source_bytes);
    update("ReadAheadSuccessfulSourceBytes", "ReadAheadIO", successful_source_bytes);
    update("ReadAheadRemoteBytes", "ReadAheadIO", remote_bytes);
    update("ReadAheadLocalBytes", "ReadAheadIO", local_bytes);
    update("ReadAheadRemoteRequests", "ReadAheadIO", remote_requests);
    update("ReadAheadReadyPageHits", "ReadAheadConsume", ready_page_hits);
    update("ReadAheadWaitPageHits", "ReadAheadConsume", wait_page_hits);
    update("ReadAheadWaitTime", "ReadAheadConsume", wait_time);
    update("ReadAheadFallbackPages", "ReadAheadConsume", fallback_pages);
    update("ReadAheadFallbackBytes", "ReadAheadConsume", fallback_bytes);
    update("ReadAheadFallbackTime", "ReadAheadConsume", fallback_time);
    update("ReadAheadConsumedPageBytes", "ReadAheadConsume", consumed_page_bytes);
    update("ReadAheadConsumedRangeBytes", "ReadAheadConsume", consumed_range_bytes);
    update("ReadAheadReleasedPages", "ReadAheadConsume", released_pages);
    update("ReadAheadWritebackTime", "ReadAheadWriteback", writeback_time);
    update("ReadAheadCompleteBlocksSubmitted", "ReadAheadWriteback", complete_blocks_submitted);
    update("ReadAheadCompleteBlockBytes", "ReadAheadWriteback", complete_block_bytes);
    update("ReadAheadPartialBlocksQueued", "ReadAheadWriteback", partial_blocks_queued);
    update("ReadAheadPartialBlocksMerged", "ReadAheadWriteback", partial_blocks_merged);
    update("ReadAheadPartialFragmentBytes", "ReadAheadWriteback", partial_fragment_bytes);
    update("ReadAheadWritebackDeduplicatedBlocks", "ReadAheadWriteback",
           writeback_deduplicated_blocks);
    update("ReadAheadWritebackRejectedBlocks", "ReadAheadWriteback", writeback_rejected_blocks);
}

ReadAheadBvars& read_ahead_bvars() {
    static ReadAheadBvars metrics;
    return metrics;
}

} // namespace doris::io
