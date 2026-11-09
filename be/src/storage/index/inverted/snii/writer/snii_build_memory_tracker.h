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

#include <cstdint>

#include "storage/index/inverted/snii/writer/memory_reporter.h"

namespace doris {
class MemTracker;
} // namespace doris

namespace doris::snii::writer {

// Reports process-wide SNII build memory without charging allocations twice.
// It outlives writers that may release memory during static teardown.
doris::MemTracker* snii_build_mem_tracker();

// Classifies whether a forced spill can reclaim a reporter's memory.
enum class BuildMemoryPopulation {
    // Ingestion writers with reclaimable posting buffers.
    kRegistered,
    // Compaction scratch that cannot be reclaimed by a forced spill.
    kUnregistered,
};

// A reporter holds its changes back from the two process-wide counters until they
// add up to this many bytes. Reservations move several times per term, so writing
// every change would have all flush threads bouncing the same cache lines: a log
// column of mostly unique terms spent over half of its flush CPU there. The step has
// to exceed what one term swings, for example 32 KiB of window scratch plus a ZSTD
// context.
//
// The price is that the counters can lag a reporter by less than one step while its live
// bytes are above zero. They are exact again whenever the live bytes are zero, so a
// finished writer leaves nothing behind even though IndexFileWriter keeps its reporter
// until the rowset writer closes. The reporter itself stays exact, so its budget and
// spill threshold do not change. The forced-spill decision reads the sum, so it can start
// late by the lag of the writers still building: at most 1 GiB with 100 flush threads on
// a 10-index table, against a share of at least 4 * inverted_index_ram_buffer_size. A
// writer below the step could not be a victim anyway, because forced spills only pick
// arenas above snii_forced_spill_min_arena_bytes (default 64 MiB), and the process-level
// backstops read real memory.
inline constexpr int64_t kBuildBytesPublishStep = 1LL << 20;

// Mirrors a reporter's live bytes into the process tracker, to within one publish step
// while they are above zero, and exactly whenever they are zero or the reporter is
// destroyed.
MemoryReporter::ConsumeReleaseFn snii_build_consume_release(BuildMemoryPopulation population);

// Returns reclaimable build bytes from registered writers.
// One atomic avoids inconsistent snapshots across the two populations.
int64_t snii_registered_build_bytes();

} // namespace doris::snii::writer
