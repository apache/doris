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

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "cpp/obj-client/obj_storage_client.h"

namespace doris::cloud {

// The AIMD policy of the delete rate of one ObjDeleteGroup. It only decides the rate from the
// responses of the group's requests, the pacing is done by the group. It is not thread safe.
// The responses of the requests admitted before the last decrease reflect the rate before it, they
// are ignored.
//   - A throttled response multiplies the rate by `obj_delete_group_rate_decrease_factor`, down to
//     100 keys per second, and increments the decrease count if the rate is actually decreased. The
//     rate is decreased at most once per round trip of the requests and at most once per second,
//     since the object storage may still throttle the requests sent shortly after a decrease.
//   - A succeeded response adds `obj_delete_group_rate_increase_step` to the rate if no request is
//     throttled for 10 seconds.
class ObjDeleteRateController {
public:
    using Clock = std::chrono::steady_clock;

    struct Adjustment {
        double old_rate = 0;
        double new_rate = 0;
        uint64_t rate_decrease_count = 0;
    };

    // `max_rate` is the upper bound of the rate, in keys per second, at least the min rate.
    ObjDeleteRateController(Clock::time_point now, double max_rate);

    // The current rate, in keys per second
    double rate() const { return _rate; }

    // How many times the rate has been decreased, a response only counts for the rate it was
    // sent at.
    uint64_t rate_decrease_count() const { return _rate_decrease_count; }

    // A request admitted when the rate had been decreased `rate_decrease_count_at_admission` times
    // returns `response`, which moves the rate by at most one step. `max_rate` may be
    // changed at runtime. Returns the adjustment if the rate is changed.
    std::optional<Adjustment> on_response(double max_rate,
                                          uint64_t rate_decrease_count_at_admission,
                                          const ObjStorageResponse& response);

private:
    double _rate;
    uint64_t _rate_decrease_count = 0;
    Clock::time_point _next_decrease; // earliest time to decrease the rate again
    // Earliest time to increase the rate, pushed back by each throttling
    Clock::time_point _next_increase;
};

// The object deletions of one storage vault prefix and one recycle task. Object storages throttle
// requests by key prefix partition, so each group adapts its delete rate to the throttling of its
// own requests only (see ObjDeleteRateController). A group limits:
//   - the rate: the requests are admitted evenly at the adaptive rate, a request deleting n keys
//     costs the same keys of the rate. The requests which arrive together spend the send budget,
//     the keys the rate allows in one second, at once, so they are not spread while the object
//     storage is idle;
//   - the concurrency: at most `obj_delete_group_max_inflight_requests` requests are admitted and
//     not finished. Tasks queued in a thread pool have not been admitted yet.
// A group holds no thread, the callers wait in their own threads.
class ObjDeleteGroup : public ObjStorageDeleteGroup,
                       public std::enable_shared_from_this<ObjDeleteGroup> {
public:
    using Clock = ObjDeleteRateController::Clock;
    using DeleteRequestCallback = std::function<ObjStorageResponse()>;

    // A request admitted by the group. It holds one request slot, which is returned on
    // destruction.
    class DeleteRequestPermit {
    public:
        ~DeleteRequestPermit();

        DeleteRequestPermit(const DeleteRequestPermit&) = delete;
        DeleteRequestPermit& operator=(const DeleteRequestPermit&) = delete;

        // Sends `delete_request_callback` once, its response adjusts the rate of the group.
        ObjStorageResponse execute_without_retry(
                const DeleteRequestCallback& delete_request_callback);

        // Returns the backoff before retrying a request which has been retried `retries` times
        // and returns `response`, or nullopt if the request should not be retried: it is not
        // throttled or the retry limit has been reached.
        std::optional<Clock::duration> retry_backoff_if_throttled(
                const ObjStorageResponse& response, int64_t retries) const;

        // Sends `delete_request_callback`. A throttled request is retried in the current thread
        // after a backoff with the permit held, and each retry is admitted by the rate again.
        // Returns the response of the last request.
        ObjStorageResponse execute_with_throttling_retries(
                const DeleteRequestCallback& delete_request_callback);

    private:
        friend class ObjDeleteGroup;
        DeleteRequestPermit(std::shared_ptr<ObjDeleteGroup> group, size_t num_keys,
                            uint64_t rate_decrease_count)
                : _group(std::move(group)),
                  _num_keys_to_delete(num_keys),
                  _rate_decrease_count_at_admission(rate_decrease_count) {}

        const std::shared_ptr<ObjDeleteGroup> _group;
        const size_t _num_keys_to_delete;
        // The decrease count of the rate when the request is admitted, updated by each retry
        uint64_t _rate_decrease_count_at_admission;
    };

    explicit ObjDeleteGroup(std::string name);

    ObjDeleteGroup(const ObjDeleteGroup&) = delete;
    ObjDeleteGroup& operator=(const ObjDeleteGroup&) = delete;

    // Blocks until a request deleting `num_keys` keys is admitted by the concurrency limit and
    // then by the rate.
    std::unique_ptr<DeleteRequestPermit> acquire_delete_permit(size_t num_keys);

    // Acquires a permit for `delete_request_callback`, which deletes `num_keys` keys, and executes
    // it, retrying a throttled request in the caller thread.
    ObjStorageResponse execute(size_t num_keys,
                               const DeleteRequestCallback& delete_request_callback) override;

    const std::string& name() const { return _name; }

private:
    struct SendReservation {
        Clock::time_point send_at;
        uint64_t rate_decrease_count; // the decrease count of the rate when it is admitted
    };

    // Blocks until the rate admits a request deleting `num_keys` keys, returns the send time it is
    // admitted at and the decrease count of the rate then.
    SendReservation wait_for_send_time(size_t num_keys);
    // Requires `_mtx`. Reserves the earliest send time not reserved by another request, the caller
    // sleeps until it without the lock.
    SendReservation reserve_send_time(size_t num_keys);

    // Called by DeleteRequestPermit after each request
    void record_response_and_adjust_rate(uint64_t rate_decrease_count_at_admission, size_t num_keys,
                                         const ObjStorageResponse& response);

    // Called by ~DeleteRequestPermit
    void release_request_slot();

    const std::string _name;

    std::mutex _mtx;
    std::condition_variable _request_slot_available; // an in-flight request finishes
    size_t _occupied_request_slots = 0;
    Clock::time_point _next_send_allowed_at; // earliest send time not reserved by a request
    ObjDeleteRateController _rate_controller;
};

} // namespace doris::cloud
