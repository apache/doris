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

#include "storage/index/ann/ann_build_memory_budget.h"

#include <algorithm>
#include <chrono>

#include "common/config.h"
#include "util/cpu_info.h"

namespace doris::segment_v2 {

namespace {

constexpr int64_t kPerThreadWorkspaceBytes = 4LL << 20; // 4 MiB
constexpr double kHnswGraphFactor = 2.0;
constexpr int kHnswEdgeBytes = 8;

int omp_threads_cap() {
    if (config::omp_threads_limit > 0) {
        return config::omp_threads_limit;
    }
    return std::max(1, CpuInfo::num_cores());
}

int64_t per_row_store_bytes(const FaissBuildParameter& params) {
    const int64_t dim = std::max(0, params.dim);
    switch (params.quantizer) {
    case FaissBuildParameter::Quantizer::FLAT:
        return dim * static_cast<int64_t>(sizeof(float));
    case FaissBuildParameter::Quantizer::SQ8:
        return dim;
    case FaissBuildParameter::Quantizer::SQ4:
        return (dim + 1) / 2;
    case FaissBuildParameter::Quantizer::PQ:
        return std::max(0, params.pq_m);
    }
    return dim * static_cast<int64_t>(sizeof(float));
}

} // namespace

AnnBuildMemoryBudget& AnnBuildMemoryBudget::instance() {
    static AnnBuildMemoryBudget s_instance;
    return s_instance;
}

bool AnnBuildMemoryBudget::try_reserve(int64_t bytes, int64_t timeout_ms, int64_t caller_held) {
    if (bytes <= 0) {
        return true;
    }
    const int64_t budget = config::ann_index_build_memory_budget_bytes;
    if (budget <= 0) {
        // Admission control disabled.
        return true;
    }
    if (caller_held < 0) {
        caller_held = 0;
    }

    std::unique_lock<std::mutex> lock(_mu);
    auto can_fit = [&]() {
        // Re-read budget on each wake: the config is mutable and an operator
        // may raise it while we wait.
        const int64_t current_budget = config::ann_index_build_memory_budget_bytes;
        if (current_budget <= 0) {
            return true;
        }
        // Allow a single oversized build to proceed if it is the only one in
        // flight; otherwise it would deadlock forever against itself. `caller_held`
        // is what this same build already reserved, so a grow only blocks on
        // *other* builds (when _reserved == caller_held nobody else is in flight).
        if (_reserved <= caller_held) {
            return true;
        }
        return _reserved + bytes <= current_budget;
    };

    if (can_fit()) {
        _reserved += bytes;
        return true;
    }
    if (timeout_ms <= 0) {
        return false;
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    if (_cv.wait_until(lock, deadline, can_fit)) {
        _reserved += bytes;
        return true;
    }
    return false;
}

void AnnBuildMemoryBudget::release(int64_t bytes) {
    if (bytes <= 0) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(_mu);
        _reserved -= bytes;
        if (_reserved < 0) {
            _reserved = 0;
        }
    }
    _cv.notify_all();
}

int64_t AnnBuildMemoryBudget::reserved_bytes() const {
    std::lock_guard<std::mutex> lock(_mu);
    return _reserved;
}

void AnnBuildMemoryBudget::reset_for_test() {
    {
        std::lock_guard<std::mutex> lock(_mu);
        _reserved = 0;
    }
    _cv.notify_all();
}

void AnnBuildMemoryReservation::release() noexcept {
    if (_bytes > 0) {
        AnnBuildMemoryBudget::instance().release(_bytes);
        _bytes = 0;
    }
}

bool AnnBuildMemoryReservation::grow(int64_t additional_bytes, int64_t timeout_ms) {
    if (additional_bytes <= 0) {
        return true;
    }
    if (!AnnBuildMemoryBudget::instance().try_reserve(additional_bytes, timeout_ms,
                                                      /*caller_held=*/_bytes)) {
        return false;
    }
    _bytes += additional_bytes;
    return true;
}

AnnBuildMemoryReservation AnnBuildMemoryReservation::try_acquire(int64_t bytes,
                                                                 int64_t timeout_ms) {
    if (bytes <= 0) {
        return AnnBuildMemoryReservation();
    }
    if (!AnnBuildMemoryBudget::instance().try_reserve(bytes, timeout_ms)) {
        return AnnBuildMemoryReservation();
    }
    return AnnBuildMemoryReservation(bytes);
}

int64_t estimate_ann_build_memory(const FaissBuildParameter& params, int64_t expected_rows,
                                  int64_t chunk_rows) {
    const int64_t dim = std::max(0, params.dim);
    if (dim == 0) {
        return 0;
    }
    if (chunk_rows < 1) {
        chunk_rows = 1;
    }
    if (expected_rows <= 0) {
        // Unknown segment size: assume at least one chunk worth so admission
        // covers the input buffer plus a chunk's worth of graph/store.
        expected_rows = chunk_rows;
    }

    const int64_t buffer_bytes = chunk_rows * dim * static_cast<int64_t>(sizeof(float));
    const int64_t store_bytes = expected_rows * per_row_store_bytes(params);

    int64_t structure_bytes = 0;
    switch (params.index_type) {
    case FaissBuildParameter::IndexType::HNSW: {
        const int64_t degree = std::max(1, params.max_degree);
        structure_bytes = static_cast<int64_t>(expected_rows * degree * kHnswEdgeBytes *
                                               kHnswGraphFactor);
        break;
    }
    case FaissBuildParameter::IndexType::IVF:
    case FaissBuildParameter::IndexType::IVF_ON_DISK: {
        // Coarse quantizer centroids (flat) + small per-list overhead.
        const int64_t nlist = std::max(1, params.ivf_nlist);
        const int64_t centroid_bytes = nlist * dim * static_cast<int64_t>(sizeof(float));
        const int64_t list_overhead = nlist * 64; // small per-list bookkeeping
        structure_bytes = centroid_bytes + list_overhead;
        break;
    }
    }

    const int64_t temp_bytes =
            std::max<int64_t>(buffer_bytes, omp_threads_cap() * kPerThreadWorkspaceBytes);

    return buffer_bytes + store_bytes + structure_bytes + temp_bytes;
}

} // namespace doris::segment_v2
