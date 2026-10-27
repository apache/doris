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

#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/http/HttpResponse.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>

#include "common/config.h"
#include "common/defer.h"
#include "cpp/obj_retry_strategy.h"
#include "cpp/sync_point.h"

namespace doris::cloud {
namespace {

using namespace std::chrono_literals;
using Clock = ObjDeleteRateController::Clock;

constexpr double kMaxRate = 6400;

ObjStorageResponse succeeded_response() {
    return ObjStorageResponse::OK();
}

ObjStorageResponse throttled_response() {
    return {.status = {TStatusCode::INTERNAL_ERROR, "SlowDown"},
            .http_code = 503,
            .throttled = true};
}

ObjStorageResponse failed_response() {
    return {.status = {TStatusCode::INTERNAL_ERROR, "simulated failure"}, .http_code = 500};
}

template <typename T>
class ScopedConfig {
public:
    ScopedConfig(T& config, T value) : _config(config), _original(config) { _config = value; }
    ~ScopedConfig() { _config = _original; }

private:
    T& _config;
    T _original;
};

std::optional<ObjDeleteRateController::Adjustment> on_response_at(
        ObjDeleteRateController& controller, Clock::time_point now, double max_rate,
        uint64_t rate_decrease_count_at_admission, const ObjStorageResponse& response) {
    auto* sp = SyncPoint::get_instance();
    sp->set_call_back("ObjDeleteRateController::on_response", [now](auto&& args) {
        *try_any_cast<Clock::time_point*>(args[0]) = now;
    });
    sp->enable_processing();
    DORIS_CLOUD_DEFER {
        sp->clear_call_back("ObjDeleteRateController::on_response");
        sp->disable_processing();
    };
    return controller.on_response(max_rate, rate_decrease_count_at_admission, response);
}

TEST(ObjDeleteRateControllerTest, HalvesRateOncePerRoundTrip) {
    ScopedConfig decrease_factor(config::obj_delete_group_rate_decrease_factor, 0.5);
    const auto start = Clock::now();
    ObjDeleteRateController controller(start, 800);
    EXPECT_EQ(controller.rate(), 800);
    EXPECT_EQ(controller.rate_decrease_count(), 0);

    auto adjustment = on_response_at(controller, start, kMaxRate, 0, throttled_response());
    ASSERT_TRUE(adjustment.has_value());
    EXPECT_EQ(adjustment->old_rate, 800);
    EXPECT_EQ(adjustment->new_rate, 400);
    EXPECT_EQ(adjustment->rate_decrease_count, 1);

    // A request admitted before the decrease reflects the old rate, it is ignored.
    EXPECT_FALSE(on_response_at(controller, start + 2s, kMaxRate, 0, throttled_response()).has_value());
    EXPECT_EQ(controller.rate(), 400);

    // A request admitted after the decrease decreases the rate again
    adjustment = on_response_at(controller, start + 2s, kMaxRate, 1, throttled_response());
    ASSERT_TRUE(adjustment.has_value());
    EXPECT_EQ(adjustment->new_rate, 200);
    EXPECT_EQ(controller.rate_decrease_count(), 2);
}

TEST(ObjDeleteRateControllerTest, DecreasesAtMostOncePerSecond) {
    ScopedConfig decrease_factor(config::obj_delete_group_rate_decrease_factor, 0.5);
    ScopedConfig increase_step(config::obj_delete_group_rate_increase_step, 200);
    const auto start = Clock::now();
    ObjDeleteRateController controller(start, 800);
    ASSERT_TRUE(on_response_at(controller, start, kMaxRate, 0, throttled_response()).has_value());
    EXPECT_EQ(controller.rate(), 400);

    // The object storage may still throttle the requests sent shortly after the decrease, they do
    // not decrease the rate again within a second
    EXPECT_FALSE(
            on_response_at(controller, start + 500ms, kMaxRate, 1, throttled_response()).has_value());
    EXPECT_EQ(controller.rate(), 400);
    EXPECT_EQ(controller.rate_decrease_count(), 1);

    // But they push the next increase back by 10 seconds
    EXPECT_FALSE(on_response_at(controller, start + 10s, kMaxRate, 1, succeeded_response()).has_value());
    ASSERT_TRUE(
            on_response_at(controller, start + 10500ms, kMaxRate, 1, succeeded_response()).has_value());
    EXPECT_EQ(controller.rate(), 600);

    // A throttling a second after the decrease decreases the rate again
    auto adjustment = on_response_at(controller, start + 11s, kMaxRate, 1, throttled_response());
    ASSERT_TRUE(adjustment.has_value());
    EXPECT_EQ(adjustment->new_rate, 300);
    EXPECT_EQ(adjustment->rate_decrease_count, 2);
}

TEST(ObjDeleteRateControllerTest, RateDoesNotDecreaseBelowMinRate) {
    ScopedConfig decrease_factor(config::obj_delete_group_rate_decrease_factor, 0.5);
    ScopedConfig increase_step(config::obj_delete_group_rate_increase_step, 200);
    const auto start = Clock::now();
    ObjDeleteRateController controller(start, 150);

    auto adjustment = on_response_at(controller, start, kMaxRate, 0, throttled_response());
    ASSERT_TRUE(adjustment.has_value());
    EXPECT_EQ(adjustment->new_rate, 100);
    EXPECT_EQ(controller.rate(), 100);
    EXPECT_EQ(controller.rate_decrease_count(), 1);

    // At the min rate a throttling does not decrease the rate, nor the decrease count
    EXPECT_FALSE(on_response_at(controller, start + 1s, kMaxRate, 1, throttled_response()).has_value());
    EXPECT_EQ(controller.rate(), 100);
    EXPECT_EQ(controller.rate_decrease_count(), 1);

    // But it still pushes the next increase back by 10 seconds
    EXPECT_FALSE(on_response_at(controller, start + 10s, kMaxRate, 1, succeeded_response()).has_value());
    adjustment = on_response_at(controller, start + 11s, kMaxRate, 1, succeeded_response());
    ASSERT_TRUE(adjustment.has_value());
    EXPECT_EQ(adjustment->new_rate, 300);
}

TEST(ObjDeleteRateControllerTest, RateIsBoundedByConfiguredMaxRate) {
    const auto start = Clock::now();
    ObjDeleteRateController controller(start, 1000);
    EXPECT_FALSE(on_response_at(controller, start + 10s, 1000, 0, succeeded_response()).has_value());
    EXPECT_EQ(controller.rate(), 1000);

    // The max rate may be lowered at runtime.
    auto adjustment = on_response_at(controller, start + 11s, 500, 0, succeeded_response());
    ASSERT_TRUE(adjustment.has_value());
    EXPECT_EQ(adjustment->new_rate, 500);
}

TEST(ObjDeleteRateControllerTest, IncreasesAdditivelyAfterThrottlingFreeInterval) {
    ScopedConfig increase_step(config::obj_delete_group_rate_increase_step, 200);
    const auto start = Clock::now();
    // The rate starts below the max rate, as if the max rate were raised at runtime
    ObjDeleteRateController controller(start, 800);

    // Not within 10 seconds since the construction
    EXPECT_FALSE(on_response_at(controller, start + 9s, kMaxRate, 0, succeeded_response()).has_value());
    // Nor on a failed response
    EXPECT_FALSE(on_response_at(controller, start + 10s, kMaxRate, 0, failed_response()).has_value());
    auto adjustment = on_response_at(controller, start + 10s, kMaxRate, 0, succeeded_response());
    ASSERT_TRUE(adjustment.has_value());
    EXPECT_EQ(adjustment->old_rate, 800);
    EXPECT_EQ(adjustment->new_rate, 1000);

    // At most once per 10 seconds
    EXPECT_FALSE(on_response_at(controller, start + 19s, kMaxRate, 0, succeeded_response()).has_value());
    ASSERT_TRUE(on_response_at(controller, start + 20s, kMaxRate, 0, succeeded_response()).has_value());
    EXPECT_EQ(controller.rate(), 1200);
}

// The initial rate is 800 keys per second, so a request deleting 40 keys is followed by a 50ms
// interval.
constexpr size_t kKeysPer50ms = 40;

// Reserves the send time of a request deleting `num_keys` keys without sleeping until it, so the
// tests check the reserved times exactly instead of measuring the sleeps. The reservations of a
// test are made back to back, far within the 50ms of the shortest interval checked.
Clock::time_point reserve_send_time(ObjDeleteGroup& group, size_t num_keys) {
    std::lock_guard lock(group._mtx);
    return group.reserve_send_time(num_keys).send_at;
}

TEST(ObjDeleteGroupTest, AdmitsRequestsInArrivalOrder) {
    // A request of 800 keys at 800 keys per second takes the whole send budget, so each request
    // is sent one second after the one arriving before it
    ScopedConfig max_rate(config::obj_delete_group_max_keys_per_second, 800);
    auto group = std::make_shared<ObjDeleteGroup>("admission_order_test");
    const auto first = reserve_send_time(*group, 800);
    for (int i = 1; i < 5; ++i) {
        EXPECT_EQ(reserve_send_time(*group, 800) - first, i * 1s);
    }
}

TEST(ObjDeleteGroupTest, SendsRequestsArrivingTogetherAtOnce) {
    ScopedConfig max_rate(config::obj_delete_group_max_keys_per_second, 800);
    auto group = std::make_shared<ObjDeleteGroup>("burst_test");
    // The budget holds the keys of all the 16 requests, none of them waits. Paced at the rate,
    // they would be spread over 15 intervals of 50ms each.
    for (int i = 0; i < 16; ++i) {
        EXPECT_LE(reserve_send_time(*group, kKeysPer50ms), Clock::now());
    }
}

TEST(ObjDeleteGroupTest, PacesTheRequestsBeyondTheSendBudget) {
    ScopedConfig max_rate(config::obj_delete_group_max_keys_per_second, 800);
    // The initial rate is 800 keys per second, so the budget of one second is 800 keys and holds
    // 20 requests of 40 keys each
    auto group = std::make_shared<ObjDeleteGroup>("send_budget_test");
    constexpr int kNumRequestsWithinBudget = 20;
    // The requests within the budget are sent at once
    const auto first = reserve_send_time(*group, kKeysPer50ms);
    for (int i = 1; i < kNumRequestsWithinBudget; ++i) {
        EXPECT_LE(reserve_send_time(*group, kKeysPer50ms), Clock::now());
    }
    // The ones beyond it wait their intervals
    for (int i = 1; i <= 5; ++i) {
        EXPECT_EQ(reserve_send_time(*group, kKeysPer50ms) - first, i * 50ms);
    }
}

TEST(ObjDeleteGroupTest, BudgetIsSpentByTheKeysOfTheRequests) {
    // 100 keys per second: a request of 60 keys takes 600ms of the 1s budget, so a request of
    // 70 keys may only start 300ms before the end of the budget of the first one
    ScopedConfig max_rate(config::obj_delete_group_max_keys_per_second, 100);
    auto group = std::make_shared<ObjDeleteGroup>("send_budget_keys_test");
    const auto first = reserve_send_time(*group, 60);
    EXPECT_EQ(reserve_send_time(*group, 70) - first, 300ms);
}

TEST(ObjDeleteGroupTest, AcquireWaitsForTheReservedSendTime) {
    // 100 keys per second: a request of 100 keys takes the whole budget, so the next request of
    // 10 keys is sent 100ms after it
    ScopedConfig max_rate(config::obj_delete_group_max_keys_per_second, 100);
    auto group = std::make_shared<ObjDeleteGroup>("acquire_wait_test");
    const auto start = Clock::now();
    auto first = group->acquire_delete_permit(100);
    auto second = group->acquire_delete_permit(10);
    // A sleep never ends early, so the lower bound holds on any machine
    EXPECT_GE(Clock::now() - start, 100ms);
}

TEST(ObjDeleteGroupTest, LimitsInflightRequests) {
    ScopedConfig max_inflight_requests(config::obj_delete_group_max_inflight_requests, 2);
    auto group = std::make_shared<ObjDeleteGroup>("inflight_requests_test");
    auto first = group->acquire_delete_permit(1);
    auto second = group->acquire_delete_permit(1);

    std::atomic_bool admitted = false;
    std::thread third_caller([&]() {
        auto third = group->acquire_delete_permit(1);
        admitted = true;
    });
    std::this_thread::sleep_for(200ms);
    EXPECT_FALSE(admitted);

    first.reset();
    third_caller.join();
    EXPECT_TRUE(admitted);
}

TEST(ObjDeleteGroupTest, RetriesThrottledRequestInCallerThread) {
    auto* sp = SyncPoint::get_instance();
    sp->set_call_back("ObjDeleteGroup::retry_backoff_if_throttled", [](auto&& args) {
        *try_any_cast<std::chrono::milliseconds*>(args[0]) = 0ms;
    });
    sp->enable_processing();
    DORIS_CLOUD_DEFER {
        sp->clear_call_back("ObjDeleteGroup::retry_backoff_if_throttled");
        sp->disable_processing();
    };
    auto group = std::make_shared<ObjDeleteGroup>("throttling_retry_test");

    int num_requests = 0;
    auto response = group->execute(
            1, [&]() { return ++num_requests == 1 ? throttled_response() : succeeded_response(); });
    EXPECT_TRUE(response.ok());
    EXPECT_EQ(num_requests, 2);

    num_requests = 0;
    response = group->execute(1, [&]() {
        ++num_requests;
        return throttled_response();
    });
    EXPECT_TRUE(response.throttled);
    EXPECT_EQ(num_requests, 2); // 1 request and 1 retry
}

TEST(S3CustomRetryStrategyTest, RetriesThrottlingUnlessDisabled) {
    auto make_error = [](Aws::Http::HttpResponseCode response_code, const char* exception_name) {
        Aws::Client::AWSError<Aws::Client::CoreErrors> error(Aws::Client::CoreErrors::UNKNOWN,
                                                             exception_name, "test error", true);
        error.SetResponseCode(response_code);
        return error;
    };
    const auto slow_down = make_error(Aws::Http::HttpResponseCode::SERVICE_UNAVAILABLE, "SlowDown");
    const auto service_unavailable =
            make_error(Aws::Http::HttpResponseCode::SERVICE_UNAVAILABLE, "ServiceUnavailable");
    const auto too_many_requests =
            make_error(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS, "TooManyRequests");
    const auto internal_error =
            make_error(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR, "InternalError");

    S3CustomRetryStrategy retry_all(3);
    EXPECT_TRUE(retry_all.ShouldRetry(slow_down, 0));
    EXPECT_TRUE(retry_all.ShouldRetry(too_many_requests, 0));

    // 503 SlowDown and 429 are not retried, other retryable errors are still retried up to the limit
    S3CustomRetryStrategy no_slow_down_retry(3, /*retry_slow_down=*/false);
    EXPECT_FALSE(no_slow_down_retry.ShouldRetry(slow_down, 0));
    EXPECT_FALSE(no_slow_down_retry.ShouldRetry(too_many_requests, 0));
    EXPECT_TRUE(no_slow_down_retry.ShouldRetry(service_unavailable, 0));
    EXPECT_TRUE(no_slow_down_retry.ShouldRetry(internal_error, 0));
    EXPECT_FALSE(no_slow_down_retry.ShouldRetry(internal_error, 3));
}

} // namespace
} // namespace doris::cloud
