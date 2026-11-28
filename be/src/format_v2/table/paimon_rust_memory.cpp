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

#include "format_v2/table/paimon_rust_memory.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/record_batch.h"
#include "core/custom_allocator.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_profile.h"

extern "C" {
#include "paimon_rust/paimon.h"
}

namespace doris::format::paimon {
namespace {
using BufferRanges = DorisVector<std::pair<uintptr_t, uintptr_t>>;
// ErrorCode::ResourceExhausted in the pinned 0.5.0 C ABI (not an errno).
constexpr int32_t SDK_RESOURCE_EXHAUSTED = 6;

void collect_paimon_buffers(const arrow::ArrayData& data, BufferRanges* ranges) {
    for (const auto& buffer : data.buffers) {
        if (!buffer) {
            continue; // An absent validity bitmap owns no exposed extent.
        }
        auto root = buffer;
        while (root->parent()) {
            root = root->parent();
        }
        if (root->size() == 0) {
            continue; // An empty slice can still retain a nonempty parent; inspect the root first.
        }
        const auto begin = reinterpret_cast<uintptr_t>(root->data());
        DORIS_CHECK_GE(root->size(), 0);
        DORIS_CHECK_LE(static_cast<uint64_t>(root->size()),
                       std::numeric_limits<uintptr_t>::max() - begin);
        ranges->emplace_back(begin, begin + root->size());
    }
    for (const auto& child : data.child_data) {
        collect_paimon_buffers(*child, ranges);
    }
    if (data.dictionary) {
        collect_paimon_buffers(*data.dictionary, ranges);
    }
}

// Offline readers may omit Profile. Production counters live below the existing reader timer.
void set_counter(RuntimeProfile::Counter* counter, int64_t value) {
    if (counter != nullptr) {
        COUNTER_SET(counter, value);
    }
}
void update_peak(RuntimeProfile::Counter* counter, int64_t value) {
    if (counter != nullptr) {
        COUNTER_SET(counter, std::max(counter->value(), value));
    }
}
} // namespace

struct PaimonRustMemoryBudget::Counters {
    RuntimeProfile::Counter* credit = nullptr;
    RuntimeProfile::Counter* peak_credit = nullptr;
    RuntimeProfile::Counter* sdk_limit = nullptr;
    RuntimeProfile::Counter* arrow_limit = nullptr;
    RuntimeProfile::Counter* sdk_reserved = nullptr;
    RuntimeProfile::Counter* sdk_peak_reserved = nullptr;
    RuntimeProfile::Counter* pending = nullptr;
    RuntimeProfile::Counter* peak_pending = nullptr;
    RuntimeProfile::Counter* rejections = nullptr;
};

PaimonRustMemoryBudget::PaimonRustMemoryBudget(std::shared_ptr<MemTrackerLimiter> query_tracker,
                                               RuntimeProfile* profile)
        : _counters(std::make_unique<Counters>()), _query_tracker(std::move(query_tracker)) {
    DORIS_CHECK(_query_tracker != nullptr);
    if (profile != nullptr) {
        const auto add = [&](const char* name, TUnit::type unit = TUnit::BYTES) {
            return ADD_CHILD_COUNTER(profile, name, unit, "PaimonRustReader");
        };
        _counters->credit = add("ReaderCreditBytes");
        _counters->peak_credit = add("PeakReaderCreditBytes");
        _counters->sdk_limit = add("SdkMemoryLimitBytes");
        _counters->arrow_limit = add("PendingArrowLimitBytes");
        _counters->sdk_reserved = add("SdkReservedBytes");
        _counters->sdk_peak_reserved = add("SdkPeakReservedBytes");
        _counters->pending = add("PendingArrowEstimatedBytes");
        _counters->peak_pending = add("PeakPendingArrowEstimatedBytes");
        _counters->rejections = add("MemoryBudgetRejections", TUnit::UNIT);
        profile->add_info_string(
                "PaimonRustMemoryAccounting",
                "Fixed query admission credit; SDK reservation and unique exposed Arrow buffer "
                "extents have separate half-credit ceilings, may overlap and are not RSS. "
                "C slices can hide allocation capacity; untracked SDK/in-flight allocations "
                "remain. Limits describe the latest split; peaks persist across splits.");
    }
}

int64_t PaimonRustMemoryBudget::credit_for_query(int64_t query_limit) {
    constexpr int64_t MAX_CREDIT = 64L * 1024 * 1024;
    return query_limit < 0 ? MAX_CREDIT : std::min(MAX_CREDIT, query_limit / 16);
}

Status PaimonRustMemoryBudget::acquire() {
    DORIS_CHECK(_sdk_context == nullptr);
    DORIS_CHECK_EQ(_credit_bytes, 0);
    const auto credit = credit_for_query(_query_tracker->limit());
    _sdk_limit_bytes = credit / 2;
    set_counter(_counters->sdk_limit, _sdk_limit_bytes);
    set_counter(_counters->arrow_limit, credit - _sdk_limit_bytes);
    if (!_query_tracker->try_reserve(credit)) {
        if (_counters->rejections != nullptr) {
            COUNTER_UPDATE(_counters->rejections, 1);
        }
        return Status::MemoryLimitExceeded(
                "paimon-rust cannot reserve {} bytes of reader admission credit on query tracker "
                "{}",
                credit, _query_tracker->label());
    }
    _credit_bytes = credit;
    set_counter(_counters->credit, credit);
    update_peak(_counters->peak_credit, credit);
    auto result = paimon_resource_context_create(_sdk_limit_bytes);
    if (result.error != nullptr) {
        return sdk_error(result.error, "create resource context");
    }
    _sdk_context = result.context;
    return Status::OK();
}

void PaimonRustMemoryBudget::sample_sdk() {
    paimon_resource_metrics metrics {};
    auto* error = paimon_resource_context_metrics(_sdk_context, &metrics);
    // Both pointers are owned, valid handles. The C ABI can reject only invalid/null arguments.
    DORIS_CHECK(error == nullptr);
    set_counter(_counters->sdk_reserved, metrics.reserved_memory_bytes);
    update_peak(_counters->sdk_peak_reserved, metrics.peak_reserved_memory_bytes);
}

Status PaimonRustMemoryBudget::sdk_error(paimon_error* error, std::string_view operation) {
    if (error->code == SDK_RESOURCE_EXHAUSTED && _counters->rejections != nullptr) {
        COUNTER_UPDATE(_counters->rejections, 1);
    }
    return paimon_rust_error(error, operation);
}

int64_t PaimonRustMemoryBudget::estimate_pending_bytes(const arrow::RecordBatch& batch) {
    BufferRanges ranges;
    for (const auto& column : batch.columns()) {
        collect_paimon_buffers(*column->data(), &ranges);
    }
    std::ranges::sort(ranges);
    uint64_t bytes = 0;
    uintptr_t covered_end = 0;
    for (const auto& [begin, end] : ranges) {
        if (end > covered_end) {
            bytes += end - std::max(begin, covered_end);
            covered_end = end;
        }
    }
    DORIS_CHECK_LE(bytes, static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    return static_cast<int64_t>(bytes);
}

Status PaimonRustMemoryBudget::retain_pending(const arrow::RecordBatch& batch) {
    DORIS_CHECK_EQ(_pending_bytes, 0);
    const auto estimate = estimate_pending_bytes(batch);
    const auto limit = _credit_bytes - _sdk_limit_bytes;
    if (estimate > limit) {
        if (_counters->rejections != nullptr) {
            COUNTER_UPDATE(_counters->rejections, 1);
        }
        return Status::MemoryLimitExceeded(
                "paimon-rust imported Arrow buffer extent estimate {} exceeds pending limit {} "
                "(reader admission credit {})",
                estimate, limit, _credit_bytes);
    }
    _pending_bytes = estimate;
    set_counter(_counters->pending, estimate);
    update_peak(_counters->peak_pending, estimate);
    return Status::OK();
}

void PaimonRustMemoryBudget::release_pending() {
    _pending_bytes = 0;
    set_counter(_counters->pending, 0);
}

PaimonRustMemoryBudget::~PaimonRustMemoryBudget() {
    release_pending();
    if (_sdk_context != nullptr) {
        sample_sdk();
        paimon_resource_context_free(_sdk_context);
    }
    // This is the same shared query tracker used at admission, even on another close thread.
    _query_tracker->shrink_reserved(_credit_bytes);
    _query_tracker->release(_credit_bytes);
    set_counter(_counters->credit, 0);
}

Status paimon_rust_error(paimon_error* error, std::string_view operation) {
    DORIS_CHECK(error != nullptr);
    const std::unique_ptr<paimon_error, decltype(&paimon_error_free)> owned(error,
                                                                            paimon_error_free);
    const auto code = owned->code;
    std::string message;
    if (owned->message.len > 0) {
        message.assign(reinterpret_cast<const char*>(owned->message.data), owned->message.len);
    }
    if (code == SDK_RESOURCE_EXHAUSTED) {
        return Status::MemoryLimitExceeded("paimon-rust {}: code={}, msg={}", operation, code,
                                           message);
    }
    return Status::InternalError("paimon-rust {}: code={}, msg={}", operation, code, message);
}

} // namespace doris::format::paimon
