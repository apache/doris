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
#include <memory>
#include <utility>

#include "common/check.h"
#include "common/status.h"

namespace doris::index_query {

// Admission accounting for query-owned buffers. Doris allocator hooks continue
// to charge the task MemTracker; this budget does not charge it a second time.
class MemoryBudget {
private:
    struct State {
        explicit State(uint64_t byte_limit) : limit(byte_limit) {}
        Status acquire(uint64_t bytes) {
            uint64_t previous_used = used.load(std::memory_order_relaxed);
            do {
                if (bytes > limit - previous_used) {
                    return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                            "index query memory budget exceeded: requested {}, used {}, limit {}",
                            bytes, previous_used, limit);
                }
            } while (!used.compare_exchange_weak(previous_used, previous_used + bytes,
                                                 std::memory_order_relaxed));
            const uint64_t total = previous_used + bytes;
            uint64_t previous_peak = peak.load(std::memory_order_relaxed);
            while (previous_peak < total &&
                   !peak.compare_exchange_weak(previous_peak, total, std::memory_order_relaxed)) {
            }
            return Status::OK();
        }

        const uint64_t limit;
        std::atomic<uint64_t> used {0};
        std::atomic<uint64_t> peak {0};
    };

public:
    // Keep this token in the physical owner, before its buffer members, so the
    // charge survives borrowed views and is released after the buffers.
    class Reservation {
    public:
        Reservation() = default;
        ~Reservation() { reset(); }
        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;
        Reservation(Reservation&& other) noexcept
                : state_(std::move(other.state_)), bytes_(std::exchange(other.bytes_, 0)) {}
        Reservation& operator=(Reservation&& other) noexcept {
            if (this != &other) {
                reset();
                state_ = std::move(other.state_);
                bytes_ = std::exchange(other.bytes_, 0);
            }
            return *this;
        }

        uint64_t bytes() const { return bytes_; }
        // Growth fails without changing the current charge; shrinking releases only the difference.
        Status resize(uint64_t bytes) {
            DORIS_CHECK(state_ != nullptr);
            if (bytes > bytes_) {
                RETURN_IF_ERROR(state_->acquire(bytes - bytes_));
            } else {
                state_->used.fetch_sub(bytes_ - bytes, std::memory_order_relaxed);
            }
            bytes_ = bytes;
            return Status::OK();
        }

        // Transfers part of an admitted charge without changing total usage or peak.
        Reservation split(uint64_t bytes) {
            DORIS_CHECK(state_ != nullptr);
            DORIS_CHECK_LE(bytes, bytes_);
            Reservation result;
            result.state_ = state_;
            result.bytes_ = bytes;
            bytes_ -= bytes;
            return result;
        }

        void reset() {
            if (state_) {
                state_->used.fetch_sub(bytes_, std::memory_order_relaxed);
                state_.reset();
            }
            bytes_ = 0;
        }

    private:
        friend class MemoryBudget;
        std::shared_ptr<State> state_;
        uint64_t bytes_ = 0;
    };

    explicit MemoryBudget(uint64_t limit) : state_(std::make_shared<State>(limit)) {}
    MemoryBudget(const MemoryBudget&) = delete;
    MemoryBudget& operator=(const MemoryBudget&) = delete;

    // Requires an empty output token. Failure leaves all reservations unchanged.
    Status reserve(uint64_t bytes, Reservation* out) {
        DORIS_CHECK(out != nullptr);
        DORIS_CHECK(out->state_ == nullptr);
        RETURN_IF_ERROR(state_->acquire(bytes));
        out->state_ = state_;
        out->bytes_ = bytes;
        return Status::OK();
    }

    uint64_t used_bytes() const { return state_->used.load(std::memory_order_relaxed); }
    uint64_t peak_bytes() const { return state_->peak.load(std::memory_order_relaxed); }
    uint64_t limit_bytes() const { return state_->limit; }

private:
    std::shared_ptr<State> state_;
};

} // namespace doris::index_query
