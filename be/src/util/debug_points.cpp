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
    return get_debug_point(name) != nullptr;
}

std::shared_ptr<DebugPoint> DebugPoints::get_debug_point(const std::string& name) {
    if (!config::enable_debug_points) {
        return nullptr;
    }
    std::shared_ptr<const DebugPointMap> map_ptr;
    {
        std::lock_guard<std::mutex> lock(_debug_points_mutex);
        map_ptr = _debug_points;
    }
    auto it = map_ptr->find(name);
    if (it == map_ptr->end()) {
        return nullptr;
    }

    auto debug_point = it->second;
    if ((debug_point->expire_ms > 0 && MonotonicMillis() >= debug_point->expire_ms) ||
        (debug_point->execute_limit > 0 &&
         debug_point->execute_num.fetch_add(1, std::memory_order_relaxed) >=
                 debug_point->execute_limit)) {
        remove(name);
        return nullptr;
    }

    return debug_point;
}

void DebugPoints::add(const std::string& name, std::shared_ptr<DebugPoint> debug_point) {
    update([&](DebugPointMap& new_points) { new_points[name] = debug_point; });

    std::ostringstream oss;
    oss << "{";
    for (auto [key, value] : debug_point->params) {
        oss << key << " : " << value << ", ";
    }
    oss << "}";

    LOG(INFO) << "add debug point: name=" << name << ", params=" << oss.str();
}

void DebugPoints::remove(const std::string& name) {
    bool exists = false;
    update([&](DebugPointMap& new_points) { exists = new_points.erase(name) > 0; });

    LOG(INFO) << "remove debug point: name=" << name << ", exists=" << exists;
}

void DebugPoints::update(std::function<void(DebugPointMap&)>&& handler) {
    std::lock_guard<std::mutex> lock(_debug_points_mutex);
    auto new_points = std::make_shared<DebugPointMap>(*_debug_points);
    handler(*new_points);
    _debug_points = std::static_pointer_cast<const DebugPointMap>(new_points);
}

void DebugPoints::clear() {
    std::lock_guard<std::mutex> lock(_debug_points_mutex);
    _debug_points = std::make_shared<const DebugPointMap>();
    LOG(INFO) << "clear debug points";
}

} // namespace doris
