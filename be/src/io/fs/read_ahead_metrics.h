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

#pragma once

#include <bvar/reducer.h>

#include <cstdint>

#include "runtime/runtime_profile.h"

namespace doris::io {

/// Shared by the reader and its asynchronous range handles. Counters are owned here rather than
/// by the caller's RuntimeProfile, so outstanding tasks can finish after the scanner is destroyed.
struct ReadAheadStatistics {
    RuntimeProfile::Counter plan_calls {TUnit::UNIT};
    RuntimeProfile::Counter plan_time {TUnit::TIME_NS};
    RuntimeProfile::Counter candidate_pages {TUnit::UNIT};
    RuntimeProfile::Counter candidate_bytes {TUnit::BYTES};
    RuntimeProfile::Counter page_cache_hits {TUnit::UNIT};
    RuntimeProfile::Counter page_cache_hit_bytes {TUnit::BYTES};
    RuntimeProfile::Counter input_pages {TUnit::UNIT};
    RuntimeProfile::Counter input_bytes {TUnit::BYTES};
    RuntimeProfile::Counter coalesced_ranges {TUnit::UNIT};
    RuntimeProfile::Counter coalesced_bytes {TUnit::BYTES};
    RuntimeProfile::Counter block_fill_bytes {TUnit::BYTES};
    RuntimeProfile::Counter planned_ranges {TUnit::UNIT};
    RuntimeProfile::Counter planned_bytes {TUnit::BYTES};
    RuntimeProfile::Counter submitted_ranges {TUnit::UNIT};
    RuntimeProfile::Counter submitted_bytes {TUnit::BYTES};
    RuntimeProfile::Counter rejected_batches {TUnit::UNIT};
    RuntimeProfile::Counter query_budget_rejected_batches {TUnit::UNIT};
    RuntimeProfile::Counter be_budget_rejected_batches {TUnit::UNIT};
    /// Executor rejection, including the rejected part of an otherwise accepted batch.
    RuntimeProfile::Counter submit_failed_ranges {TUnit::UNIT};
    RuntimeProfile::Counter completed_ranges {TUnit::UNIT};
    RuntimeProfile::Counter failed_ranges {TUnit::UNIT};
    RuntimeProfile::Counter cancelled_ranges {TUnit::UNIT};
    RuntimeProfile::Counter queue_wait_time {TUnit::TIME_NS};
    RuntimeProfile::Counter read_time {TUnit::TIME_NS};
    RuntimeProfile::Counter source_bytes {TUnit::BYTES};
    /// Successful full source reads, including buffers cancelled after the read returned.
    RuntimeProfile::Counter successful_source_bytes {TUnit::BYTES};
    RuntimeProfile::Counter remote_bytes {TUnit::BYTES};
    RuntimeProfile::Counter local_bytes {TUnit::BYTES};
    RuntimeProfile::Counter remote_requests {TUnit::UNIT};
    RuntimeProfile::Counter ready_page_hits {TUnit::UNIT};
    RuntimeProfile::Counter wait_page_hits {TUnit::UNIT};
    RuntimeProfile::Counter wait_time {TUnit::TIME_NS};
    /// Synchronous data-page read attempts and requested bytes; a retry counts again.
    RuntimeProfile::Counter fallback_pages {TUnit::UNIT};
    RuntimeProfile::Counter fallback_bytes {TUnit::BYTES};
    RuntimeProfile::Counter fallback_time {TUnit::TIME_NS};
    RuntimeProfile::Counter consumed_page_bytes {TUnit::BYTES};
    RuntimeProfile::Counter consumed_range_bytes {TUnit::BYTES};
    RuntimeProfile::Counter released_pages {TUnit::UNIT};
    RuntimeProfile::Counter writeback_time {TUnit::TIME_NS};
    RuntimeProfile::Counter complete_blocks_submitted {TUnit::UNIT};
    RuntimeProfile::Counter complete_block_bytes {TUnit::BYTES};
    RuntimeProfile::Counter partial_blocks_queued {TUnit::UNIT};
    RuntimeProfile::Counter partial_blocks_merged {TUnit::UNIT};
    RuntimeProfile::Counter partial_fragment_bytes {TUnit::BYTES};
    RuntimeProfile::Counter writeback_deduplicated_blocks {TUnit::UNIT};
    RuntimeProfile::Counter writeback_rejected_blocks {TUnit::UNIT};

    /// Transfer accumulated deltas into PageReadAhead counters on the reporting thread. Repeated
    /// calls add only new work, including when multiple scanners share the destination profile.
    void update_profile(RuntimeProfile* profile);
};

/// BE-wide gauges and cumulative totals, without query/file/cache-path labels. Gauge updates
/// follow the same ownership transitions as the scheduler and hole-fill queue.
struct ReadAheadBvars {
    bvar::Adder<int64_t> resident_bytes {"doris_read_ahead_resident_bytes"};
    bvar::Adder<int64_t> inflight_ranges {"doris_read_ahead_inflight_ranges"};
    bvar::Adder<int64_t> rejected_batches {"doris_read_ahead_rejected_batches"};
    bvar::Adder<int64_t> failed_ranges {"doris_read_ahead_failed_ranges"};
    // Updated on IO completion or page consumption, independently of Profile reporting.
    bvar::Adder<int64_t> source_bytes {"doris_read_ahead_source_bytes"};
    bvar::Adder<int64_t> successful_source_bytes {"doris_read_ahead_successful_source_bytes"};
    bvar::Adder<int64_t> remote_bytes {"doris_read_ahead_remote_bytes"};
    bvar::Adder<int64_t> remote_requests {"doris_read_ahead_remote_requests"};
    bvar::Adder<int64_t> consumed_page_bytes {"doris_read_ahead_consumed_page_bytes"};
    bvar::Adder<int64_t> hole_fill_pending_bytes {"doris_hole_fill_pending_bytes"};
    bvar::Adder<int64_t> hole_fill_active_blocks {"doris_hole_fill_active_blocks"};
    bvar::Adder<int64_t> hole_fill_remote_requests {"doris_hole_fill_remote_requests"};
    bvar::Adder<int64_t> hole_fill_remote_bytes {"doris_hole_fill_remote_bytes"};
    bvar::Adder<int64_t> hole_fill_remote_read_time_ns {"doris_hole_fill_remote_read_time_ns"};
    bvar::Adder<int64_t> hole_fill_failed_blocks {"doris_hole_fill_failed_blocks"};
    bvar::Adder<int64_t> hole_fill_write_submitted_blocks {
            "doris_hole_fill_write_submitted_blocks"};
    bvar::Adder<int64_t> hole_fill_dropped_blocks {"doris_hole_fill_dropped_blocks"};
};

ReadAheadBvars& read_ahead_bvars();

} // namespace doris::io
