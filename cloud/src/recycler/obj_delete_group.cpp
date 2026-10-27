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

#include "recycler/obj_delete_group.h"

#include <glog/logging.h>

#include <algorithm>
#include <random>
#include <thread>
#include <utility>

#include "common/bvars.h"
#include "common/config.h"
#include "cpp/sync_point.h"

namespace doris::cloud {
namespace {

using namespace std::chrono;

// The lower bound of the rate, also of `obj_delete_group_max_keys_per_second`
constexpr double kMinRate = 100;
// The min interval between two decreases
constexpr auto kDecreaseInterval = seconds(30);
// The throttling-free interval before each increase
constexpr auto kIncreaseInterval = seconds(60);

// ObjDeleteGroup: the window the send budget of a group is measured over, see `reserve_send_time`
constexpr auto kSendBudgetWindow = seconds(1);
// Max retries of a throttled request, and the base of the retry backoff
constexpr int64_t kMaxThrottlingRetries = 1;
constexpr auto kRetryBackoffBase = milliseconds(1000);

double configured_max_rate() {
    return config::obj_delete_group_max_keys_per_second;
}

size_t configured_max_concurrent_requests() {
    return static_cast<size_t>(config::obj_delete_group_max_inflight_requests);
}

// The interval between the admissions of a request deleting `num_keys` keys and the next request
ObjDeleteGroup::Clock::duration pacing_interval(size_t num_keys, double rate) {
    return duration_cast<ObjDeleteGroup::Clock::duration>(duration<double>(num_keys / rate));
}

// The backoff before the (retries + 1)-th retry: kRetryBackoffBase * 2^retries with a jitter in
// [0.5, 1.5], so the throttled requests of a group do not retry at the same time.
milliseconds calculate_retry_backoff(int64_t retries) {
    int64_t base = kRetryBackoffBase.count() << retries;
    thread_local std::mt19937_64 gen(std::random_device {}());
    return milliseconds(base / 2 + std::uniform_int_distribution<int64_t>(0, base)(gen));
}

} // namespace

ObjDeleteRateController::ObjDeleteRateController(Clock::time_point now, double max_rate)
        : _rate(max_rate), _next_decrease(now), _next_increase(now + kIncreaseInterval) {
    DCHECK_GE(max_rate, kMinRate);
}

std::optional<ObjDeleteRateController::Adjustment> ObjDeleteRateController::on_response(
        double max_rate, uint64_t rate_decrease_count_at_admission,
        const ObjStorageResponse& response) {
    auto now = Clock::now();
    TEST_SYNC_POINT_CALLBACK("ObjDeleteRateController::on_response", &now);
    DCHECK_GE(max_rate, kMinRate);
    const double old_rate = _rate;
    _rate = std::min(_rate, max_rate);
    if (rate_decrease_count_at_admission != _rate_decrease_count) {
        // The request was admitted before the rate was decreased, its response reflects the rate
        // before the decrease.
    } else if (response.throttled) {
        // Multiplicative decrease. The throttling-free period required for increasing starts over.
        _next_increase = now + kIncreaseInterval;
        // At the min rate the rate is not decreased, the responses of the requests admitted before
        // still reflect it.
        if (now >= _next_decrease && _rate > kMinRate) {
            _rate = std::max(kMinRate, _rate * config::obj_delete_group_rate_decrease_factor);
            ++_rate_decrease_count;
            _next_decrease = now + kDecreaseInterval;
        }
    } else if (response.ok() && now >= _next_increase && _rate < max_rate) {
        // Additive increase
        _rate = std::min(max_rate, _rate + config::obj_delete_group_rate_increase_step);
        _next_increase = now + kIncreaseInterval;
    }
    if (_rate == old_rate) {
        return std::nullopt;
    }
    return Adjustment {
            .old_rate = old_rate,
            .new_rate = _rate,
            .rate_decrease_count = _rate_decrease_count,
    };
}

ObjDeleteGroup::DeleteRequestPermit::~DeleteRequestPermit() {
    _group->release_request_slot();
}

ObjStorageResponse ObjDeleteGroup::DeleteRequestPermit::execute_without_retry(
        const DeleteRequestCallback& delete_request_callback) {
    auto delete_response = delete_request_callback();
    _group->record_response_and_adjust_rate(_rate_decrease_count_at_admission, _num_keys_to_delete,
                                            delete_response);
    return delete_response;
}

std::optional<ObjDeleteGroup::Clock::duration>
ObjDeleteGroup::DeleteRequestPermit::retry_backoff_if_throttled(const ObjStorageResponse& response,
                                                                int64_t retries) const {
    if (!response.throttled || retries >= kMaxThrottlingRetries) {
        return std::nullopt;
    }
    auto retry_backoff_duration = calculate_retry_backoff(retries);
    TEST_SYNC_POINT_CALLBACK("ObjDeleteGroup::retry_backoff_if_throttled", &retry_backoff_duration);
    LOG(INFO) << "obj delete group retries a throttled request, group=" << _group->name()
              << " num_keys=" << _num_keys_to_delete << " retries=" << retries + 1
              << " backoff_ms=" << retry_backoff_duration.count()
              << " request_id=" << response.request_id;
    return retry_backoff_duration;
}

ObjStorageResponse ObjDeleteGroup::DeleteRequestPermit::execute_with_throttling_retries(
        const DeleteRequestCallback& delete_request_callback) {
    for (int64_t retries = 0;; ++retries) {
        auto delete_response = execute_without_retry(delete_request_callback);
        auto retry_backoff_duration = retry_backoff_if_throttled(delete_response, retries);
        if (!retry_backoff_duration.has_value()) {
            return delete_response;
        }
        // Keep the concurrency while backing off, the retry only waits for the time, never for
        // another permit.
        std::this_thread::sleep_for(*retry_backoff_duration);
        _rate_decrease_count_at_admission =
                _group->wait_for_send_time(_num_keys_to_delete).rate_decrease_count;
    }
}

ObjDeleteGroup::ObjDeleteGroup(std::string name)
        : _name(std::move(name)), _rate_controller(Clock::now(), configured_max_rate()) {
    g_bvar_obj_delete_group_rate.put({_name}, static_cast<int64_t>(_rate_controller.rate()));
    LOG(INFO) << "obj delete group is created, group=" << _name
              << " rate=" << static_cast<int64_t>(_rate_controller.rate());
}

std::unique_ptr<ObjDeleteGroup::DeleteRequestPermit> ObjDeleteGroup::acquire_delete_permit(
        size_t num_keys) {
    const auto acquire_start = Clock::now();
    std::unique_lock lock(_mtx);
    _request_slot_available.wait(lock, [this] {
        return _occupied_request_slots < configured_max_concurrent_requests();
    });
    ++_occupied_request_slots;
    // Take the concurrency before the rate, so the admission time of a request is close to the
    // time it is sent.
    const auto reservation = reserve_send_time(num_keys);
    lock.unlock();
    std::this_thread::sleep_until(reservation.send_at);
    g_bvar_obj_delete_group_acquire_wait_us.put(
            {_name}, duration_cast<microseconds>(Clock::now() - acquire_start).count());
    return std::unique_ptr<DeleteRequestPermit>(
            new DeleteRequestPermit(shared_from_this(), num_keys, reservation.rate_decrease_count));
}

ObjStorageResponse ObjDeleteGroup::execute(size_t num_keys,
                                           const DeleteRequestCallback& delete_request_callback) {
    auto delete_permit = acquire_delete_permit(num_keys);
    return delete_permit->execute_with_throttling_retries(delete_request_callback);
}

ObjDeleteGroup::SendReservation ObjDeleteGroup::wait_for_send_time(size_t num_keys) {
    std::unique_lock lock(_mtx);
    const auto reservation = reserve_send_time(num_keys);
    lock.unlock();
    std::this_thread::sleep_until(reservation.send_at);
    return reservation;
}

ObjDeleteGroup::SendReservation ObjDeleteGroup::reserve_send_time(size_t num_keys) {
    // The requests are admitted evenly at the rate, in the order they reserve. Each reservation
    // starts where the previous one ends rather than when its caller wakes up, so each caller
    // sleeps once and the wake-up latency does not slow the rate down. A request reserved before
    // a decrease is paced by the old rate, its response is ignored.
    //
    // The requests which arrive together are sent at once as long as the group has not spent its
    // send budget, which is the keys the rate allows in `kSendBudgetWindow`.
    // `_next_send_allowed_at` is ahead of `now` by the keys already reserved and not allowed by the
    // rate yet, so the group may reserve a request whose keys still fit in what the budget leaves,
    // which is the budget minus the keys the request itself takes. Spreading a batch over its
    // intervals instead would only make it take longer while the object storage is idle.
    //
    // The budget is a window rather than a balance: the reservations start from `now` floor the
    // send time, so the group does not accumulate more than the keys of one window while it is
    // idle, and it does not send a burst of the keys of every idle second when the requests come
    // back.
    //
    // The next send time starts from the reservation rather than from the send time: the requests
    // which spend their budget still have to pay for it, so the budget is spent once and the
    // reservations after it are paced again. Starting from the send time would reset the cost of
    // the burst requests on every reservation which is floored by `now`, and the budget would be
    // reusable, letting the group send without limit while the requests are in flight.
    const auto now = Clock::now();
    const auto interval = pacing_interval(num_keys, _rate_controller.rate());
    // The keys of the request are not a part of what it may advance by, the budget must still hold
    // them after it is sent
    const auto head_start = std::max(Clock::duration::zero(), kSendBudgetWindow - interval);
    const auto send_at = std::max(now, _next_send_allowed_at - head_start);
    _next_send_allowed_at = std::max(_next_send_allowed_at, now) + interval;
    return {.send_at = send_at, .rate_decrease_count = _rate_controller.rate_decrease_count()};
}

void ObjDeleteGroup::record_response_and_adjust_rate(uint64_t rate_decrease_count_at_admission,
                                                     size_t num_keys,
                                                     const ObjStorageResponse& response) {
    std::optional<ObjDeleteRateController::Adjustment> adjustment;
    {
        std::lock_guard lock(_mtx);
        adjustment = _rate_controller.on_response(configured_max_rate(),
                                                  rate_decrease_count_at_admission, response);
    }
    if (response.throttled) {
        g_bvar_obj_delete_group_throttled.put({_name}, 1);
    } else if (response.ok()) {
        g_bvar_obj_delete_group_deleted_keys.put({_name}, static_cast<int64_t>(num_keys));
    }
    if (!adjustment.has_value()) {
        return;
    }
    g_bvar_obj_delete_group_rate.put({_name}, static_cast<int64_t>(adjustment->new_rate));
    LOG(INFO) << "obj delete group "
              << (adjustment->new_rate > adjustment->old_rate ? "increases" : "decreases")
              << " rate, group=" << _name << " rate=" << static_cast<int64_t>(adjustment->old_rate)
              << "->" << static_cast<int64_t>(adjustment->new_rate)
              << " rate_decreases=" << adjustment->rate_decrease_count
              << " throttled=" << response.throttled << " request_id=" << response.request_id;
}

void ObjDeleteGroup::release_request_slot() {
    {
        std::lock_guard lock(_mtx);
        DCHECK_GT(_occupied_request_slots, 0) << "group=" << _name;
        --_occupied_request_slots;
    }
    // Wake up all the waiters, `obj_delete_group_max_inflight_requests` may have been raised and
    // admit more than one of them.
    _request_slot_available.notify_all();
}

} // namespace doris::cloud
