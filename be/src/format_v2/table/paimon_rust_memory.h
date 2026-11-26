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
#include <memory>
#include <string_view>

#include "common/status.h"

struct paimon_resource_context;
struct paimon_error;
namespace arrow {
class RecordBatch;
}
namespace doris {
class MemTrackerLimiter;
class RuntimeProfile;
} // namespace doris

namespace doris::format::paimon {

// Fixed foreign-memory admission credit, not an allocator or RSS meter. Scanners reserve on the
// same query tracker before entering Rust. Each receives at most 1/16 of a limited query and 64 MiB;
// half bounds SDK reservations and half bounds the imported pending Arrow estimate. Those estimates
// may overlap and neither covers all SDK allocations. In particular synchronous next can allocate
// untracked memory before returning; its output is checked immediately, before conversion/retention.
class PaimonRustMemoryBudget {
public:
    PaimonRustMemoryBudget(std::shared_ptr<MemTrackerLimiter> query_tracker,
                           RuntimeProfile* profile);
    // The caller must release pending batches and the SDK pipeline before destroying the budget.
    ~PaimonRustMemoryBudget();
    PaimonRustMemoryBudget(const PaimonRustMemoryBudget&) = delete;
    PaimonRustMemoryBudget& operator=(const PaimonRustMemoryBudget&) = delete;

    Status acquire();
    paimon_resource_context* sdk_context() const { return _sdk_context; }
    void sample_sdk();
    Status sdk_error(paimon_error* error, std::string_view operation);
    Status retain_pending(const arrow::RecordBatch& batch);
    void release_pending();

    int64_t credit_bytes() const { return _credit_bytes; }
    int64_t sdk_limit_bytes() const { return _sdk_limit_bytes; }
    int64_t pending_bytes() const { return _pending_bytes; }
    static int64_t credit_for_query(int64_t query_limit);
    // Union of exposed address ranges, including known parent buffers, children and dictionaries.
    // A C-imported slice may hide its allocation capacity: this is an estimate of retained extents,
    // never logical row size or a claim that each alias/slice owns a separate allocation.
    static int64_t estimate_pending_bytes(const arrow::RecordBatch& batch);

private:
    struct Counters;
    std::unique_ptr<Counters> _counters;
    std::shared_ptr<MemTrackerLimiter> _query_tracker;
    paimon_resource_context* _sdk_context = nullptr;
    int64_t _credit_bytes = 0;
    int64_t _sdk_limit_bytes = 0;
    int64_t _pending_bytes = 0;
};

// Takes ownership of the C error, preserving resource exhaustion as a memory-limit status.
Status paimon_rust_error(paimon_error* error, std::string_view operation);

} // namespace doris::format::paimon
