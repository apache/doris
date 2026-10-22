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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <utility>

#include "common/check.h"
#include "common/logging.h"
#include "common/status.h"

namespace doris::snii::writer {

// Build-time byte counter for one logical index writer or merge session. Writers
// sharing a reporter share its posting budget. Legacy modules report resident-byte deltas
// after their allocation; hard-gated modules own a Reservation that atomically
// pre-charges before allocation. current_bytes() is their shared live total.
// consume_release mirrors successful changes into the process-wide SNII
// index-build OBSERVATION tracker; it is null off-Doris (bench / unit tests),
// where only the local atomic is updated.
class MemoryReporter {
public:
    // The callback may be invoked concurrently and from Reservation destructors;
    // it must be thread-safe and must not throw. Null off-Doris.
    //
    // It must NEVER charge a MemTrackerLimiter: Doris's jemalloc allocation hook
    // has already attributed these bytes to the MemTrackerLimiter attached to
    // the allocating thread, so an explicit charge would count them twice. The
    // only valid target is a plain MemTracker used for classified observation
    // (see snii_build_consume_release).
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
        explicit Reservation(MemoryReporter* owner, bool postings = false)
                : owner_(owner), postings_(postings) {}

        MemoryReporter* owner_ = nullptr;
        uint64_t bytes_ = 0;
        bool postings_ = false;
    };

    // cap_bytes is the shared gate-2 threshold (0 = unlimited). Hard-limit
    // reporters reject reservations before their covered allocations cross it.
    // Spill-threshold reporters keep exact accounting above it so over_cap() can
    // drive reclaim without turning irreducible vocabulary growth into an import
    // failure.
    explicit MemoryReporter(ConsumeReleaseFn consume_release = nullptr, uint64_t cap_bytes = 0,
                            CapPolicy cap_policy = CapPolicy::kHardLimit,
                            uint64_t postings_cap_bytes = 32ULL * 1024 * 1024)
            : consume_release_(std::move(consume_release)),
              cap_bytes_(cap_bytes),
              cap_policy_(cap_policy),
              postings_cap_bytes_(postings_cap_bytes) {}

    MemoryReporter(const MemoryReporter&) = delete;
    MemoryReporter& operator=(const MemoryReporter&) = delete;

    // TERMINAL DRAIN. Reservations are RAII and balance themselves, but the
    // legacy report() path is manual, so a caller that dies between report(+X)
    // and report(-X) leaks X. That used to be harmless -- consume_release_ was
    // null in production, so the residue died with this object's atomic. It is
    // not harmless now: the mirrored bytes are a PROCESS-WIDE counter that also
    // feeds the build-RAM decision, so a few MiB of residue per segment would
    // become permanent phantom pressure on a long-lived BE.
    //
    // Warn rather than DCHECK. An unbalanced reporter is worth knowing about,
    // but it is a legitimate end state for this observe-only type -- callers
    // use report() for transient accounting they may abandon on an error path
    // -- so aborting debug builds over it would be wrong. A warning also
    // reaches RELEASE builds, which is where an unnoticed leak would actually
    // accumulate; a DCHECK would not.
    ~MemoryReporter() {
        const int64_t remaining = current_.load(std::memory_order_relaxed);
        if (remaining != 0 && consume_release_) {
            LOG(WARNING) << "SNII MemoryReporter destroyed with " << remaining
                         << " unbalanced bytes; draining them so they do not accumulate in the "
                         << "process-wide index-build counter.";
            consume_release_(-remaining);
        }
    }

    Reservation make_reservation() { return Reservation(this); }

    // One hard workspace limit shared by spill, merge and the final posting
    // encoder. These bytes are also included in current_bytes(), exactly once.
    // Persistent vocabulary and the input posting arena use ordinary reservations.
    Reservation make_postings_reservation() { return Reservation(this, true); }
    uint64_t postings_cap_bytes() const { return postings_cap_bytes_; }
    uint64_t postings_available_bytes() const {
        const uint64_t used = postings_current_bytes();
        uint64_t available = used < postings_cap_bytes_ ? postings_cap_bytes_ - used : 0;
        if (cap_policy_ == CapPolicy::kHardLimit && cap_bytes_ != 0) {
            const auto total = static_cast<uint64_t>(current_bytes());
            available = std::min(available, total < cap_bytes_ ? cap_bytes_ - total : 0);
        }
        return available;
    }
    uint64_t postings_current_bytes() const {
        return postings_current_.load(std::memory_order_relaxed);
    }
    uint64_t postings_peak_bytes() const { return postings_peak_.load(std::memory_order_relaxed); }
    void record_postings_io(uint64_t read_bytes, uint64_t written_bytes) {
        postings_read_.fetch_add(read_bytes, std::memory_order_relaxed);
        postings_written_.fetch_add(written_bytes, std::memory_order_relaxed);
    }
    uint64_t postings_read_bytes() const { return postings_read_.load(std::memory_order_relaxed); }
    uint64_t postings_written_bytes() const {
        return postings_written_.load(std::memory_order_relaxed);
    }

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
    Status try_acquire(uint64_t bytes, bool postings = false);
    void release(uint64_t bytes, bool postings = false);

    std::atomic<int64_t> current_ {0};
    ConsumeReleaseFn consume_release_;
    uint64_t cap_bytes_ = 0;
    CapPolicy cap_policy_ = CapPolicy::kHardLimit;
    const uint64_t postings_cap_bytes_;
    std::atomic<uint64_t> postings_current_ {0};
    std::atomic<uint64_t> postings_peak_ {0};
    std::atomic<uint64_t> postings_read_ {0};
    std::atomic<uint64_t> postings_written_ {0};
};

inline MemoryReporter::Reservation::Reservation(Reservation&& other) noexcept
        : owner_(std::exchange(other.owner_, nullptr)),
          bytes_(std::exchange(other.bytes_, 0)),
          postings_(other.postings_) {}

inline MemoryReporter::Reservation& MemoryReporter::Reservation::operator=(
        Reservation&& other) noexcept {
    if (this != &other) {
        reset();
        owner_ = std::exchange(other.owner_, nullptr);
        bytes_ = std::exchange(other.bytes_, 0);
        postings_ = other.postings_;
    }
    return *this;
}

inline MemoryReporter::Reservation::~Reservation() {
    reset();
}

inline Status MemoryReporter::Reservation::set_bytes(uint64_t target_bytes) {
    DORIS_CHECK(owner_ != nullptr);
    if (target_bytes > bytes_) {
        RETURN_IF_ERROR(owner_->try_acquire(target_bytes - bytes_, postings_));
    } else if (target_bytes < bytes_) {
        owner_->release(bytes_ - target_bytes, postings_);
    }
    bytes_ = target_bytes;
    return Status::OK();
}

inline Status MemoryReporter::Reservation::prepare_replacement(uint64_t target_bytes,
                                                               Reservation* replacement) const {
    DORIS_CHECK(owner_ != nullptr);
    DORIS_CHECK(replacement != nullptr);
    DORIS_CHECK(replacement->owner_ == nullptr);
    Reservation pending(owner_, postings_);
    RETURN_IF_ERROR(pending.set_bytes(target_bytes));
    *replacement = std::move(pending);
    return Status::OK();
}

inline void MemoryReporter::Reservation::reset() {
    if (owner_ != nullptr && bytes_ != 0) {
        owner_->release(bytes_, postings_);
        bytes_ = 0;
    }
}

inline Status MemoryReporter::try_acquire(uint64_t bytes, bool postings) {
    if (bytes == 0) {
        return Status::OK();
    }
    uint64_t workspace = 0;
    if (postings) {
        workspace = postings_current_.load(std::memory_order_relaxed);
        while (true) {
            if (workspace > postings_cap_bytes_ || bytes > postings_cap_bytes_ - workspace) {
                return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                        "SNII posting workspace exceeds limit: request={} current={} cap={} "
                        "(snii_postings_workspace_bytes)",
                        bytes, workspace, postings_cap_bytes_);
            }
            if (postings_current_.compare_exchange_weak(workspace, workspace + bytes,
                                                        std::memory_order_relaxed)) {
                break;
            }
        }
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
            if (postings) {
                postings_current_.fetch_sub(bytes, std::memory_order_relaxed);
            }
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
            if (postings) {
                uint64_t peak = postings_peak_.load(std::memory_order_relaxed);
                while (peak < workspace + bytes &&
                       !postings_peak_.compare_exchange_weak(peak, workspace + bytes,
                                                             std::memory_order_relaxed)) {
                }
            }
            return Status::OK();
        }
    }
}

inline void MemoryReporter::release(uint64_t bytes, bool postings) {
    DCHECK_LE(bytes, static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    const int64_t delta = static_cast<int64_t>(bytes);
    const int64_t previous = current_.fetch_sub(delta, std::memory_order_relaxed);
    DCHECK_GE(previous, delta);
    if (postings) {
        [[maybe_unused]] const uint64_t previous_workspace =
                postings_current_.fetch_sub(bytes, std::memory_order_relaxed);
        DCHECK_GE(previous_workspace, bytes);
    }
    if (consume_release_) {
        consume_release_(-delta);
    }
}

} // namespace doris::snii::writer
