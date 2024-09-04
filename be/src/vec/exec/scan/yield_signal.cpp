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

#include "vec/exec/scan/yield_signal.h"

#include <glog/logging.h>

#include <boost/asio.hpp>
#include <boost/asio/steady_timer.hpp>
#include <iostream>

namespace doris::vectorized {

YieldSignal::YieldSignal() : _yield(false), _running_sequence(0), _termination_started(false) {}

YieldSignal::~YieldSignal() {
    std::unique_lock<std::mutex> lock(_mutex);
    _termination_started = true;
    _yield.store(true);
    if (_yield_timer != nullptr) {
        _yield_timer->cancel();
        _yield_timer.reset();
    }
}

void YieldSignal::set_with_delay(std::chrono::nanoseconds max_run_nanos,
                                 boost::asio::io_context* io_context) {
    std::unique_lock<std::mutex> lock(_mutex);

    // Increment running sequence and create a new timer
    _running_sequence++;
    long expected_running_sequence = _running_sequence;

    _yield_timer = std::make_shared<boost::asio::steady_timer>(*io_context);
    _yield_timer->expires_after(max_run_nanos);

    _yield_timer->async_wait(
            [this, expected_running_sequence](const boost::system::error_code& ec) {
                std::unique_lock<std::mutex> lock(_mutex);
                if (!ec && expected_running_sequence == _running_sequence) {
                    _yield.store(true);
                }
            });
}

void YieldSignal::reset() {
    std::unique_lock<std::mutex> lock(_mutex);
    if (_termination_started) {
        return;
    }
    DCHECK(_yield_timer != nullptr) << "No ongoing yield to reset";

    // Reset yield state and cancel all timers
    _yield.store(false);
    _yield_timer->cancel();
    _yield_timer.reset();
}

bool YieldSignal::is_set() const {
    return _yield.load();
}

void YieldSignal::yield_immediately_for_termination() {
    std::unique_lock<std::mutex> lock(_mutex);
    _termination_started = true;
    _yield.store(true);
}

} // namespace doris::vectorized
