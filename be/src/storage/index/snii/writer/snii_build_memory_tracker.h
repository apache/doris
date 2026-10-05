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

#include "storage/index/snii/writer/memory_reporter.h"

namespace doris {
class MemTracker;
} // namespace doris

namespace doris::snii::writer {

// Process-wide OBSERVATION tracker for SNII index-build RAM, the index-build
// analogue of MemTableMemoryLimiter's "AllMemTableMemory" tracker.
//
// WHAT IT IS NOT: it is NOT a MemTrackerLimiter and must never be turned into
// one. Doris charges these bytes AUTOMATICALLY -- the jemalloc allocation hook
// attributes every allocation on a thread to the MemTrackerLimiter attached by
// SCOPED_ATTACH_TASK (which is why the SNII IO pool threads attach at all).
// Enforcement and process accounting therefore already happen without SNII
// doing anything. Charging a MemTrackerLimiter here as well would count the
// same bytes twice.
//
// WHAT IT IS FOR: the hook knows only which THREAD allocated, so SNII's
// index-build memory is invisible as a category -- it disappears into whichever
// task tracker happened to be attached. This labelled MemTracker is a pure
// classified counter (thread-safe, no limit, cannot refuse an allocation) fed
// explicitly by every MemoryReporter, so index-build RAM shows up as its own
// line in the memory picture.
//
// IT IS NOT AN INPUT TO THE DECISION. The GlobalMemoryLimiter reads
// snii_registered_build_bytes() below, never this tracker. That separation is
// what makes the accounting safe under concurrency: charging a reporter touches
// this tracker AND (on the registered path) the counter below, which cannot be
// done atomically as a pair -- so the decision must never combine the two. It
// reads exactly one of them.
//
// Never destroyed: writers release bytes from destructors that can run at any
// point, including static teardown, so the tracker must outlive all of them.
doris::MemTracker* snii_build_mem_tracker();

// Which POPULATION a reporter's bytes belong to. The observation line is the
// same for both -- this only classifies whether a forced spill could ever
// reclaim any of these bytes, which is what the decision layer needs.
enum class BuildMemoryPopulation {
    // Ingestion writers. Their SpimiTermBuffer registers with the
    // GlobalMemoryLimiter and holds a reclaimable posting arena, so asking it
    // to spill actually frees memory.
    kRegistered,
    // Index-merge compaction. It holds Reservation scratch only: it never
    // constructs a SpimiTermBuffer and never registers, so the limiter has no
    // lever over these bytes at all. Its own kHardLimit cap policy bounds them
    // instead -- it refuses allocations, which a spill request cannot do.
    kUnregistered,
};

// A MemoryReporter consume_release callback that mirrors the reporter's live
// bytes into the tracker above. Every production MemoryReporter is built with
// one; off-Doris users (benchmarks, unit tests) pass null and keep only the
// reporter's local atomic. The population must be stated explicitly: guessing
// it wrong either charges unreclaimable bytes to the wrong victims or hides
// reclaimable ones from the decision.
MemoryReporter::ConsumeReleaseFn snii_build_consume_release(BuildMemoryPopulation population);

// Live bytes of the kRegistered population -- the memory a forced spill could
// actually reclaim, and the ONLY build-memory input to the decision layer.
//
// A plain atomic rather than a second MemTracker on purpose: it must not look
// like an independent line in the memory picture that a reader might add to the
// tracker's total. It is a subset of that total, maintained alongside it.
//
// Maintained DIRECTLY rather than derived by subtracting the unregistered
// population from the tracker. Deriving it would mean the decision reads two
// independent atomics that are updated in two separate steps, and a read landing
// between those steps tears: while a compaction grows, its bytes would briefly
// look reclaimable and could trigger a spill no writer needed; while it
// releases, reclaimable would be understated and could even go negative,
// skipping a spill that was needed. No memory order fixes that -- two atomics
// cannot be sampled as one snapshot. Reading a single counter has no such
// window.
int64_t snii_registered_build_bytes();

} // namespace doris::snii::writer
