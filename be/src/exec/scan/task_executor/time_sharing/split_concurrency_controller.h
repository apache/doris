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
#include <cmath>
#include <cstdint>
#include <stdexcept>

#include "common/factory_creator.h"
#include "common/status.h"

namespace doris::vectorized {

class SplitConcurrencyController {
    ENABLE_FACTORY_CREATOR(SplitConcurrencyController);

public:
    SplitConcurrencyController(int initial_concurrency,
                               std::chrono::nanoseconds adjustment_interval)
            : _adjustment_interval_nanos(adjustment_interval),
              _target_concurrency(initial_concurrency) {}

    void update(uint64_t nanos, double utilization, int current_concurrency) {
        _validate_args(nanos, utilization, current_concurrency);
        _thread_nanos_since_adjust += nanos;
        if (_should_increase_concurrency(current_concurrency, utilization)) {
            _reset_adjust_counter();
            ++_target_concurrency;
        }
    }

    int target_concurrency() const { return _target_concurrency; }

    void split_finished(uint64_t split_nanos, double utilization, int current_concurrency) {
        _validate_args(split_nanos, utilization, current_concurrency);
        if (_should_adjust(split_nanos)) {
            if (utilization > TARGET_UTIL && _target_concurrency > 1) {
                _reset_adjust_counter();
                --_target_concurrency;
            } else if (utilization < TARGET_UTIL && current_concurrency >= _target_concurrency) {
                _reset_adjust_counter();
                ++_target_concurrency;
            }
        }
    }

private:
    static constexpr double TARGET_UTIL = 0.5;
    const std::chrono::nanoseconds _adjustment_interval_nanos;
    int _target_concurrency;
    uint64_t _thread_nanos_since_adjust = 0;

    void _validate_args(uint64_t nanos, double util, int concurrency) const {
        CHECK(std::isfinite(util)) << "Invalid utilization";
        CHECK(util >= 0) << "Negative utilization";
        CHECK(concurrency >= 0) << "Negative concurrency";
    }

    bool _should_increase_concurrency(int curr_concurrency, double util) const {
        return _thread_nanos_since_adjust >= _adjustment_interval_nanos.count() &&
               util < TARGET_UTIL && curr_concurrency >= _target_concurrency;
    }

    bool _should_adjust(uint64_t split_nanos) const {
        return _thread_nanos_since_adjust >= _adjustment_interval_nanos.count() ||
               _thread_nanos_since_adjust >= split_nanos;
    }

    void _reset_adjust_counter() { _thread_nanos_since_adjust = 0; }
};

} // namespace doris::vectorized
