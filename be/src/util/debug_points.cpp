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

#include "util/debug_points.h"

#include "common/logging.h"
#include "util/time.h"

namespace doris {

DebugPoints::DebugPoints() : _debug_points(std::make_shared<const DebugPointMap>()) {}

DebugPoints* DebugPoints::instance() {
    static DebugPoints instance;
    return &instance;
}

bool DebugPoints::is_enable(const std::string& name) {
    if (!config::enable_debug_points) {
        return false;
    }
    auto map_ptr = std::atomic_load_explicit(&_debug_points, std::memory_order_relaxed);
    auto it = map_ptr->find(name);
    if (it == map_ptr->end()) {
        return false;
    }

    auto& debug_point = *(it->second);
    if ((debug_point.expire_ms > 0 && MonotonicMillis() >= debug_point.expire_ms) ||
        (debug_point.execute_limit > 0 &&
         debug_point.execute_num.fetch_add(1, std::memory_order_relaxed) >=
                 debug_point.execute_limit)) {
        remove(name);
        return false;
    }

    return true;
}

void DebugPoints::add(const std::string& name, int64_t execute_limit, int64_t timeout_second) {
    auto debug_point = std::make_shared<DebugPoint>();
    debug_point->execute_limit = execute_limit;
    if (timeout_second > 0) {
        debug_point->expire_ms = MonotonicMillis() + timeout_second * MILLIS_PER_SEC;
    }
    update([&](DebugPointMap& new_points) { new_points[name] = debug_point; });

    LOG(INFO) << "add debug point: name=" << name << ", execute=" << execute_limit
              << ", timeout=" << timeout_second;
}

void DebugPoints::remove(const std::string& name) {
    bool exists = false;
    update([&](DebugPointMap& new_points) { exists = new_points.erase(name) > 0; });

    LOG(INFO) << "remove debug point: name=" << name << ", exists=" << exists;
}

void DebugPoints::update(std::function<void(DebugPointMap&)>&& handler) {
    auto old_points = std::atomic_load_explicit(&_debug_points, std::memory_order_relaxed);
    while (true) {
        auto new_points = std::make_shared<DebugPointMap>(*old_points);
        handler(*new_points);
        if (std::atomic_compare_exchange_strong_explicit(
                    &_debug_points, &old_points,
                    std::static_pointer_cast<const DebugPointMap>(new_points),
                    std::memory_order_relaxed, std::memory_order_relaxed)) {
            break;
        }
    }
}

void DebugPoints::clear() {
    std::atomic_store_explicit(&_debug_points, std::make_shared<const DebugPointMap>(),
                               std::memory_order_relaxed);
    LOG(INFO) << "clear debug points";
}

} // namespace doris
