// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format_v2/lance/lance_memory.h"

#include <atomic>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_state.h"
#include "runtime/workload_group/workload_group.h"
#include "util/mem_info.h"

namespace doris::format::lance {
namespace {

constexpr int32_t QUERY_LIMIT = 1;
constexpr int32_t WORKLOAD_GROUP_LIMIT = 2;
constexpr int32_t PROCESS_LIMIT = 3;

// WG's check-and-increment is not atomic. Serialize explicit Lance admissions,
// including different accounts in the same group, without tying reservations to TLS.
std::mutex reservation_mutex;

} // namespace

struct LanceMemoryAccount::Context {
    Context(std::shared_ptr<MemTrackerLimiter> tracker_, std::weak_ptr<WorkloadGroup> group_)
            : tracker(std::move(tracker_)), workload_group(std::move(group_)) {}

    static void retain(void* opaque) noexcept {
        static_cast<Context*>(opaque)->references.fetch_add(1, std::memory_order_relaxed);
    }

    static void release(void* opaque) noexcept {
        auto* self = static_cast<Context*>(opaque);
        if (self->references.fetch_sub(1, std::memory_order_acq_rel) == 1) delete self;
    }

    void latch_query_limit() noexcept {
        if (tracker->limit() >= 0 && tracker->consumption() > tracker->limit()) {
            query_limit_reached.store(true, std::memory_order_relaxed);
        }
    }

    static void record(void* opaque, int64_t delta) noexcept {
        auto* self = static_cast<Context*>(opaque);
        // This path runs inside Rust's infallible allocator: no admission failure,
        // Status construction, logging or C++ allocation may escape this callback.
        self->tracker->consume(delta);
        if (delta > 0) {
            if (auto group = self->workload_group.lock()) {
                group->add_wg_refresh_interval_memory_growth(delta);
            }
            self->latch_query_limit();
        }
        // A refresh can already have absorbed growth into the group's snapshot.
        // A later free releases the owner tracker, never the old growth estimate.
    }

    static int32_t reserve(void* opaque, size_t bytes) noexcept {
        if (bytes == 0) return 0;
        if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
            return PROCESS_LIMIT;
        }
        auto* self = static_cast<Context*>(opaque);
        const auto size = static_cast<int64_t>(bytes);
        auto group = self->workload_group.lock();
        bool query_reserved = false;
        auto rollback = [&] {
            // WG refresh can reset growth even while process-pressure logging
            // runs. Keep its conservative estimate until refresh reconciles it;
            // subtracting here could erase allocations from a newer interval.
            if (query_reserved) {
                self->tracker->shrink_reserved(size);
                self->tracker->release(size);
            }
        };
        try {
            std::lock_guard lock(reservation_mutex);
            const auto consumed = self->tracker->consumption();
            if (self->query_limit_reached.load(std::memory_order_relaxed) ||
                (consumed > 0 && size > std::numeric_limits<int64_t>::max() - consumed) ||
                !self->tracker->try_reserve(size)) {
                // Rejected reservations own no bytes. Only actual allocation
                // overage is sticky, so a caller may retry a smaller reservation.
                return QUERY_LIMIT;
            }
            query_reserved = true;
            if (group) {
                if (!group->try_add_wg_refresh_interval_memory_growth(size)) {
                    rollback();
                    return WORKLOAD_GROUP_LIMIT;
                }
            }
            // The arbitrator adds signed counters; reject impossible requests
            // before its addition can overflow, then use its atomic admission.
            const auto process_limit = MemInfo::soft_mem_limit();
            if (size >= process_limit ||
                GlobalMemoryArbitrator::process_memory_usage() >= process_limit - size ||
                !GlobalMemoryArbitrator::try_reserve_process_memory(size)) {
                rollback();
                return PROCESS_LIMIT;
            }
            return 0;
        } catch (...) {
            // Process-pressure diagnostics can allocate. Even that failure must
            // refund query quota and remain an ordinary Rust error.
            rollback();
            return PROCESS_LIMIT;
        }
    }

    static void finish_reserve(void* opaque, size_t bytes, bool allocated) noexcept {
        if (bytes == 0) return;
        auto* self = static_cast<Context*>(opaque);
        const auto size = static_cast<int64_t>(bytes);
        // Rust pairs every successful reserve exactly once, even on allocation
        // failure. Success keeps its precharged live bytes; it must not record them again.
        self->tracker->shrink_reserved(size);
        if (!allocated) {
            self->tracker->release(size);
            // As in admission rollback, WG growth is reconciled by its next
            // refresh, never decremented across unknown interval generations.
        }
        GlobalMemoryArbitrator::shrink_process_reserved(size);
    }

    static int32_t check(void* opaque) noexcept {
        auto* self = static_cast<Context*>(opaque);
        self->latch_query_limit();
        if (self->query_limit_reached.load(std::memory_order_relaxed)) return QUERY_LIMIT;
        if (auto group = self->workload_group.lock()) {
            bool low_watermark = false;
            bool high_watermark = false;
            group->check_mem_used(&low_watermark, &high_watermark);
            if (high_watermark) return WORKLOAD_GROUP_LIMIT;
        }
        // Numeric checks avoid the logging/allocations in the allocator's process
        // checks and do not inspect a worker's unrelated TLS reservation state.
        if (GlobalMemoryArbitrator::process_memory_usage() >= MemInfo::soft_mem_limit() ||
            GlobalMemoryArbitrator::sys_mem_available() <
                    MemInfo::sys_mem_available_warning_water_mark()) {
            return PROCESS_LIMIT;
        }
        return 0;
    }

    std::atomic<size_t> references {1};
    const std::shared_ptr<MemTrackerLimiter> tracker;
    const std::weak_ptr<WorkloadGroup> workload_group;
    std::atomic<bool> query_limit_reached {false};
};

LanceMemoryAccount::LanceMemoryAccount(std::shared_ptr<MemTrackerLimiter> tracker,
                                       std::weak_ptr<WorkloadGroup> workload_group) {
    if (!tracker) throw std::invalid_argument("Lance memory account requires a tracker");
    _context = new Context(std::move(tracker), std::move(workload_group));
}

LanceMemoryAccount::~LanceMemoryAccount() {
    Context::release(_context);
}

LanceHostMemoryAccountOptionsV1 LanceMemoryAccount::options() const noexcept {
    LanceHostMemoryAccountOptionsV1 options {};
    options.abi_version = 1;
    options.struct_size = sizeof(options);
    options.context = _context;
    options.retain_context = Context::retain;
    options.release_context = Context::release;
    options.record = Context::record;
    options.reserve = Context::reserve;
    options.finish_reserve = Context::finish_reserve;
    options.check = Context::check;
    return options;
}

Status install_lance_memory_services() {
    static std::once_flag once;
    static Status status;
    try {
        std::call_once(once, [] {
            LanceMemoryAccount background(MemTrackerLimiter::create_shared(
                    MemTrackerLimiter::Type::GLOBAL, "LanceBackground"));
            LanceMemoryAccount index_cache(MemTrackerLimiter::create_shared(
                    MemTrackerLimiter::Type::CACHE, "LanceIndexCache"));
            LanceMemoryAccount metadata_cache(MemTrackerLimiter::create_shared(
                    MemTrackerLimiter::Type::METADATA, "LanceMetadataCache"));
            LanceHostMemoryServicesOptionsV1 services {};
            services.abi_version = 1;
            services.struct_size = sizeof(services);
            services.background = background.options();
            services.index_cache = index_cache.options();
            services.metadata_cache = metadata_cache.options();
            if (lance_runtime_install_memory_services_v1(&services) != 0) {
                status = Status::InternalError("Could not install Lance memory services");
            }
            // The SDK retains these account contexts, including cached buffers
            // that outlive the host runtime's registration handles.
        });
        return status;
    } catch (const std::exception& error) {
        return Status::InternalError("Could not create Lance memory services: {}", error.what());
    }
}

Status attach_lance_memory_account(::LanceHostExecutor* executor, RuntimeState* state) {
    if (executor == nullptr || state == nullptr) {
        return Status::InvalidArgument(
                "Lance memory account requires an executor and runtime state");
    }
    try {
        std::weak_ptr<WorkloadGroup> group;
        if (state->get_query_ctx() != nullptr) group = state->workload_group();
        LanceMemoryAccount account(state->query_mem_tracker(), std::move(group));
        auto options = account.options();
        if (lance_host_executor_set_memory_v1(executor, &options) != 0) {
            return Status::InternalError("Could not attach Lance query memory account");
        }
        return Status::OK();
    } catch (const std::exception& error) {
        return Status::InternalError("Could not create Lance query memory account: {}",
                                     error.what());
    }
}

} // namespace doris::format::lance
