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

#include <condition_variable>
#include <cstdint>
#include <mutex>

#include "storage/index/ann/faiss_ann_index.h"

namespace doris::segment_v2 {

// Global byte budget shared by every concurrent ANN/vector index build on this
// BE. Acts as admission control: writers reserve their estimated peak before
// train/add and release on finish. A budget of 0 (config default) disables
// admission control and all reservations succeed immediately.
//
// Pairs with ScopedOmpThreadBudget (which caps CPU) to bound HNSW/IVF build
// memory across concurrent loads, compactions, and schema-change builds.
class AnnBuildMemoryBudget {
public:
    static AnnBuildMemoryBudget& instance();

    // Try to reserve `bytes` from the global budget. Blocks up to `timeout_ms`
    // for other builds to release memory. Returns true on success or when the
    // budget is disabled (`ann_index_build_memory_budget_bytes` <= 0). Returns
    // false on timeout. `bytes <= 0` always succeeds without altering counters.
    //
    // `caller_held` is what this same build already holds; it is excluded from
    // the "single build in flight" exemption so a build can keep growing its own
    // reservation without deadlocking against itself (a grow is always allowed
    // when the caller is the only build in flight, i.e. `_reserved == caller_held`).
    bool try_reserve(int64_t bytes, int64_t timeout_ms, int64_t caller_held = 0);

    // Releases previously reserved bytes and notifies any waiters.
    void release(int64_t bytes);

    int64_t reserved_bytes() const;

    // Test hook: reset internal counter. Not for production use.
    void reset_for_test();

private:
    AnnBuildMemoryBudget() = default;

    mutable std::mutex _mu;
    std::condition_variable _cv;
    int64_t _reserved = 0;
};

// RAII handle for a successful AnnBuildMemoryBudget reservation. Releases the
// bytes on destruction or explicit release(). Move-only.
class AnnBuildMemoryReservation {
public:
    AnnBuildMemoryReservation() = default;
    AnnBuildMemoryReservation(const AnnBuildMemoryReservation&) = delete;
    AnnBuildMemoryReservation& operator=(const AnnBuildMemoryReservation&) = delete;
    AnnBuildMemoryReservation(AnnBuildMemoryReservation&& other) noexcept : _bytes(other._bytes) {
        other._bytes = 0;
    }
    AnnBuildMemoryReservation& operator=(AnnBuildMemoryReservation&& other) noexcept {
        if (this != &other) {
            release();
            _bytes = other._bytes;
            other._bytes = 0;
        }
        return *this;
    }
    ~AnnBuildMemoryReservation() { release(); }

    void release() noexcept;
    int64_t bytes() const { return _bytes; }
    bool active() const { return _bytes > 0; }

    // Grow this reservation by `additional_bytes` against the global budget.
    // Used to track real memory as a build accumulates rows past its initial
    // (chunk-sized) admission reservation. Because the build already holds
    // bytes() it is exempt from blocking against itself; it only waits for
    // *other* concurrent builds. Returns true on success (handle grows),
    // false on timeout (handle unchanged). `additional_bytes <= 0` is a no-op
    // that returns true.
    bool grow(int64_t additional_bytes, int64_t timeout_ms);

    // Acquire `bytes` against the global budget. On success the returned
    // reservation owns the bytes; on failure the returned reservation is
    // inactive (active() == false). `bytes <= 0` yields an inactive reservation
    // and is a successful no-op.
    static AnnBuildMemoryReservation try_acquire(int64_t bytes, int64_t timeout_ms);

private:
    explicit AnnBuildMemoryReservation(int64_t bytes) : _bytes(bytes) {}
    int64_t _bytes = 0;
};

// Conservative memory peak estimate for a single ANN index build.
// expected_rows is an upper bound on segment row count (0 = unknown, treated as
// chunk_rows so admission is at least chunk-sized).
//
// Bytes accounted:
//   - input chunk buffer (chunk_rows * dim * sizeof(float))
//   - quantized vector store: FLAT=4*dim, SQ8=dim, SQ4=dim/2, PQ=pq_m bytes/row
//   - HNSW graph: rows * max_degree * 8 bytes * graph_factor
//     OR IVF coarse quantizer + inverted lists overhead
//   - per-thread workspace (omp_threads * 4 MiB)
//
// The model is intentionally conservative: precision is not the goal, the
// goal is to refuse builds that would obviously blow past the budget.
int64_t estimate_ann_build_memory(const FaissBuildParameter& params, int64_t expected_rows,
                                  int64_t chunk_rows);

} // namespace doris::segment_v2
