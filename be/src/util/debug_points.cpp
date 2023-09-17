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

bool DebugPoints::is_enable(const std::string& name) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _debug_points.find(name);
    if (it == _debug_points.end()) {
        return false;
    }

    auto& debug_point = it->second;
    if ((debug_point.expire_ms > 0 && MonotonicMillis() >= debug_point.expire_ms) ||
        (debug_point.execute_limit > 0 && debug_point.execute_num >= debug_point.execute_limit)) {
        _debug_points.erase(it);
        return false;
    }

    debug_point.execute_num++;

    return true;
}

void DebugPoints::add(const std::string& name, int execute_limit, int64_t timeout_second) {
    DebugPoint debug_point;
    debug_point.execute_limit = execute_limit;
    if (timeout_second > 0) {
        debug_point.expire_ms = MonotonicMillis() + timeout_second * MILLIS_PER_SEC;
    }

    {
        std::lock_guard<std::mutex> lock(_mutex);
        _debug_points[name] = debug_point;
    }
    LOG(INFO) << "add debug point: name=" << name << ", execute=" << execute_limit
              << ", timeout=" << timeout_second;
}

void DebugPoints::remove(const std::string& name) {
    bool exists;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        exists = _debug_points.erase(name) > 0;
    }
    LOG(INFO) << "remove debug point: name=" << name << ", exists=" << exists;
}

void DebugPoints::clear() {
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _debug_points.clear();
    }
    LOG(INFO) << "clear debug points";
}

} // namespace doris
