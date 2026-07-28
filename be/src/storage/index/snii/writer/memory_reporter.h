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

#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <utility>

#include "common/check.h"
#include "common/status.h"

namespace doris::snii::writer {

// Per-WRITER accurate byte counter for build-time RAM (one per SniiCompoundWriter =
// one per segment's inverted index). Legacy modules report resident-byte deltas
// after their allocation; hard-gated modules own a Reservation that atomically
// pre-charges before allocation. current_bytes() is their shared live total.
// consume_release mirrors successful changes into Doris's LOAD MemTracker; it is
// null off-Doris (bench / unit tests), where only the local atomic is updated.
class MemoryReporter {
public:
    // The callback may be invoked concurrently and from Reservation destructors;
    // it must be thread-safe and must not throw. Null off-Doris.
    using ConsumeReleaseFn = std::function<void(int64_t delta)>;

    enum class CapPolicy : uint8_t {
        // Reservations fail before an allocation would cross the cap. Native
        // compaction uses this policy so an over-budget merge can fall back to
        // the raw-column rebuild path without exceeding its bounded workspace.
        kHardLimit,
        // The cap is a spill trigger, not an allocation limit. Ordinary index
        // ingestion uses this policy because persistent vocabulary structures
        // can exceed the reclaimable posting-arena threshold by design.
        kSpillThreshold,
    };

    // Move-only ownership of bytes pre-charged against this reporter. Growing a
    // reservation atomically charges before allocation. Hard-limit reporters
    // reject an over-cap charge without changing state; spill-threshold reporters
    // retain exact accounting above the threshold. Callers must release/shrink the
    // physical buffer before lowering the reservation. A Reservation borrows its
    // reporter, which must outlive it.
    class Reservation {
    public:
        Reservation() = default;
        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;
        Reservation(Reservation&& other) noexcept;
        Reservation& operator=(Reservation&& other) noexcept;
        ~Reservation();

        Status set_bytes(uint64_t target_bytes);
        // Pre-charges an independent allocation while this Reservation keeps
        // covering the old one. After the physical replacement succeeds, move
        // `replacement` back into this Reservation to release the old charge.
        Status prepare_replacement(uint64_t target_bytes, Reservation* replacement) const;
        void reset();
        uint64_t bytes() const { return bytes_; }

    private:
        friend class MemoryReporter;
        explicit Reservation(MemoryReporter* owner) : owner_(owner) {}

        MemoryReporter* owner_ = nullptr;
        uint64_t bytes_ = 0;
    };

    // cap_bytes is the shared gate-2 threshold (0 = unlimited). Hard-limit
    // reporters reject reservations before their covered allocations cross it.
    // Spill-threshold reporters keep exact accounting above it so over_cap() can
    // drive reclaim without turning irreducible vocabulary growth into an import
    // failure.
    explicit MemoryReporter(ConsumeReleaseFn consume_release = nullptr, uint64_t cap_bytes = 0,
                            CapPolicy cap_policy = CapPolicy::kHardLimit)
            : consume_release_(std::move(consume_release)),
              cap_bytes_(cap_bytes),
              cap_policy_(cap_policy) {}

    MemoryReporter(const MemoryReporter&) = delete;
    MemoryReporter& operator=(const MemoryReporter&) = delete;

    Reservation make_reservation() { return Reservation(this); }

    // Observe-only legacy path: delta > 0 grows, delta < 0 shrinks/frees. New
    // hard-gated allocations must use Reservation instead.
    void report(int64_t delta) {
        if (delta == 0) return;
        DCHECK_NE(delta, std::numeric_limits<int64_t>::min());
        int64_t current = current_.load(std::memory_order_relaxed);
        while (true) {
            DCHECK_GE(current, 0);
            if (delta > 0) {
                DCHECK_LE(delta, std::numeric_limits<int64_t>::max() - current);
            } else {
                DCHECK_GE(current, -delta);
            }
            const int64_t desired = current + delta;
            if (current_.compare_exchange_weak(current, desired, std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
                if (consume_release_) consume_release_(delta);
                return;
            }
        }
    }

    int64_t current_bytes() const { return current_.load(std::memory_order_relaxed); }

    // True once all reported/reserved build RAM reaches the shared spill threshold.
    bool over_cap() const {
        const int64_t current = current_bytes();
        DCHECK_GE(current, 0);
        return cap_bytes_ != 0 && static_cast<uint64_t>(current) >= cap_bytes_;
    }
    uint64_t cap_bytes() const { return cap_bytes_; }

private:
    Status try_acquire(uint64_t bytes);
    void release(uint64_t bytes);

    std::atomic<int64_t> current_ {0};
    ConsumeReleaseFn consume_release_;
    uint64_t cap_bytes_ = 0;
    CapPolicy cap_policy_ = CapPolicy::kHardLimit;
};

inline MemoryReporter::Reservation::Reservation(Reservation&& other) noexcept
        : owner_(std::exchange(other.owner_, nullptr)), bytes_(std::exchange(other.bytes_, 0)) {}

inline MemoryReporter::Reservation& MemoryReporter::Reservation::operator=(
        Reservation&& other) noexcept {
    if (this != &other) {
        reset();
        owner_ = std::exchange(other.owner_, nullptr);
        bytes_ = std::exchange(other.bytes_, 0);
    }
    return *this;
}

inline MemoryReporter::Reservation::~Reservation() {
    reset();
}

inline Status MemoryReporter::Reservation::set_bytes(uint64_t target_bytes) {
    DORIS_CHECK(owner_ != nullptr);
    if (target_bytes > bytes_) {
        RETURN_IF_ERROR(owner_->try_acquire(target_bytes - bytes_));
    } else if (target_bytes < bytes_) {
        owner_->release(bytes_ - target_bytes);
    }
    bytes_ = target_bytes;
    return Status::OK();
}

inline Status MemoryReporter::Reservation::prepare_replacement(uint64_t target_bytes,
                                                               Reservation* replacement) const {
    DORIS_CHECK(owner_ != nullptr);
    DORIS_CHECK(replacement != nullptr);
    DORIS_CHECK(replacement->owner_ == nullptr);
    Reservation pending(owner_);
    RETURN_IF_ERROR(pending.set_bytes(target_bytes));
    *replacement = std::move(pending);
    return Status::OK();
}

inline void MemoryReporter::Reservation::reset() {
    if (owner_ != nullptr && bytes_ != 0) {
        owner_->release(bytes_);
        bytes_ = 0;
    }
}

inline Status MemoryReporter::try_acquire(uint64_t bytes) {
    if (bytes == 0) {
        return Status::OK();
    }
    int64_t current = current_.load(std::memory_order_relaxed);
    while (true) {
        DCHECK_GE(current, 0);
        const uint64_t current_bytes = static_cast<uint64_t>(current);
        const bool exceeds_cap = cap_policy_ == CapPolicy::kHardLimit && cap_bytes_ != 0 &&
                                 (current_bytes > cap_bytes_ || bytes > cap_bytes_ - current_bytes);
        const bool exceeds_counter =
                bytes > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - current_bytes;
        if (exceeds_cap || exceeds_counter) {
            return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                    "SNII memory reservation exceeds limit: request={} current={} cap={}", bytes,
                    current_bytes, cap_bytes_);
        }
        const int64_t desired = current + static_cast<int64_t>(bytes);
        if (current_.compare_exchange_weak(current, desired, std::memory_order_relaxed,
                                           std::memory_order_relaxed)) {
            if (consume_release_) {
                consume_release_(static_cast<int64_t>(bytes));
            }
            return Status::OK();
        }
    }
}

inline void MemoryReporter::release(uint64_t bytes) {
    DCHECK_LE(bytes, static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    const int64_t delta = static_cast<int64_t>(bytes);
    const int64_t previous = current_.fetch_sub(delta, std::memory_order_relaxed);
    DCHECK_GE(previous, delta);
    if (consume_release_) {
        consume_release_(-delta);
    }
}

} // namespace doris::snii::writer
