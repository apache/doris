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

// Mirrors a reporter's live bytes into the process tracker.
MemoryReporter::ConsumeReleaseFn snii_build_consume_release(BuildMemoryPopulation population);

// Returns reclaimable build bytes from registered writers.
// One atomic avoids inconsistent snapshots across the two populations.
int64_t snii_registered_build_bytes();

} // namespace doris::snii::writer
