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

#include "storage/index/snii/writer/snii_build_memory_tracker.h"

#include <atomic>

#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "runtime/memory/mem_tracker.h"

namespace doris {
DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(snii_index_build_mem_consumption, MetricUnit::BYTES, "",
                                   snii_index_build_mem_consumption, Labels({{"type", "load"}}));
} // namespace doris

namespace doris::snii::writer {

doris::MemTracker* snii_build_mem_tracker() {
    // Intentionally leaked (never destroyed): MemoryReporters release their
    // bytes from destructors that may run during static teardown at process
    // exit, and a destroyed tracker there would be a use-after-free. Leaking
    // makes the "tracker outlives every reporter" contract unconditional.
    //
    // The hook metric is registered exactly once, alongside the tracker, so the
    // gauge exists as soon as anything charges it. It is never deregistered for
    // the same reason the tracker is never destroyed.
    static auto* tracker = [] {
        auto* instance = new doris::MemTracker("SniiIndexBuild");
        REGISTER_HOOK_METRIC(snii_index_build_mem_consumption,
                             []() { return snii_build_mem_tracker()->consumption(); });
        return instance;
    }();
    return tracker;
}

namespace {
// The kRegistered subset of the tracker above -- see the header for why this is
// maintained directly instead of being derived by subtraction, and why it is a
// plain atomic rather than a second MemTracker.
std::atomic<int64_t> g_registered_build_bytes {0};
} // namespace

int64_t snii_registered_build_bytes() {
    return g_registered_build_bytes.load(std::memory_order_relaxed);
}

MemoryReporter::ConsumeReleaseFn snii_build_consume_release(BuildMemoryPopulation population) {
    auto* tracker = snii_build_mem_tracker();
    // MemTracker and the subset counter are both thread-safe atomics, which is
    // what MemoryReporter requires of this callback: it is invoked
    // concurrently, from Reservation destructors, and must not throw.
    //
    // The registered path updates two of them, which is NOT atomic as a pair --
    // and does not need to be. The decision layer reads only
    // g_registered_build_bytes; the tracker is observation. Because no consumer
    // combines the two, there is no window in which they can disagree with each
    // other in a way anything can observe.
    const bool registered = population == BuildMemoryPopulation::kRegistered;
    return [tracker, registered](int64_t delta) {
        if (delta >= 0) {
            tracker->consume(delta);
        } else {
            tracker->release(-delta);
        }
        if (registered) {
            g_registered_build_bytes.fetch_add(delta, std::memory_order_relaxed);
        }
    };
}

} // namespace doris::snii::writer
