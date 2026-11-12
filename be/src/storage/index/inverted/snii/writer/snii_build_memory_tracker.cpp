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

#include "storage/index/inverted/snii/writer/snii_build_memory_tracker.h"

#include <atomic>
#include <cstdlib>
#include <memory>
#include <utility>

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

// Holds one reporter's changes until they reach a publish step or its live bytes reach zero.
// Flush any remainder on destruction because IndexFileWriter can retain a reporter after its writer finishes.
class HeldBackBuildBytes {
public:
    HeldBackBuildBytes(doris::MemTracker* tracker, bool registered)
            : tracker_(tracker), registered_(registered) {}
    ~HeldBackBuildBytes() { publish(pending_.exchange(0, std::memory_order_relaxed)); }

    void add(int64_t delta) {
        const int64_t pending = pending_.fetch_add(delta, std::memory_order_relaxed) + delta;
        // acq_rel, so that whoever brings live_ to zero sees every change counted in
        // live_ before it already in pending_ when it exchanges.
        const int64_t live = live_.fetch_add(delta, std::memory_order_acq_rel) + delta;
        // At zero only a non-empty remainder is published: a scratch reservation that is
        // taken and given back per term leaves pending_ at zero too, and must not touch
        // the shared counters.
        if (std::abs(pending) >= kBuildBytesPublishStep || (live == 0 && pending != 0)) {
            publish(pending_.exchange(0, std::memory_order_relaxed));
        }
    }

private:
    // The limiter reads only the registered counter; the tracker provides observations.
    // Their atomic updates do not need to form one transaction.
    void publish(int64_t delta) {
        if (delta >= 0) {
            tracker_->consume(delta);
        } else {
            tracker_->release(-delta);
        }
        if (registered_) {
            g_registered_build_bytes.fetch_add(delta, std::memory_order_relaxed);
        }
    }

    doris::MemTracker* const tracker_;
    const bool registered_;
    std::atomic<int64_t> pending_ {0};
    std::atomic<int64_t> live_ {0};
};
} // namespace

int64_t snii_registered_build_bytes() {
    return g_registered_build_bytes.load(std::memory_order_relaxed);
}

MemoryReporter::ConsumeReleaseFn snii_build_consume_release(BuildMemoryPopulation population) {
    // pending_ is atomic because MemoryReporter requires this callback to be safe
    // to invoke concurrently; it is also invoked from Reservation destructors and
    // must not throw. It is shared because std::function must be copyable: the
    // last copy to go publishes the remainder.
    auto held_back = std::make_shared<HeldBackBuildBytes>(
            snii_build_mem_tracker(), population == BuildMemoryPopulation::kRegistered);
    return [held_back = std::move(held_back)](int64_t delta) { held_back->add(delta); };
}

} // namespace doris::snii::writer
