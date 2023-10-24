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
#include <functional>
#include <map>
#include <memory>
#include <string>

#include "common/compiler_util.h"
#include "common/config.h"

#define DBUG_EXECUTE_IF(debug_point, code)                     \
    if (UNLIKELY(config::enable_debug_points)) {               \
        if (DebugPoints::instance()->is_enable(debug_point)) { \
            code;                                              \
        }                                                      \
    }

namespace doris {

struct DebugPoint {
    std::atomic<int64_t> execute_num {0};
    int64_t execute_limit = -1;
    int64_t expire_ms = -1;
};

class DebugPoints {
public:
    bool is_enable(const std::string& name);
    void add(const std::string& name, int64_t execute_limit, int64_t timeout_second);
    void remove(const std::string& name);
    void clear();

    static DebugPoints* instance();

private:
    DebugPoints();

    using DebugPointMap = std::map<std::string, std::shared_ptr<DebugPoint>>;

    // handler(new_debug_points)
    void update(std::function<void(DebugPointMap&)>&& handler);

private:
    /// TODO: replace atomic_load/store() on shared_ptr (which is deprecated as of C++20) by C++20 std::atomic<std::shared_ptr>.
    /// Clang 15 currently does not support it.
    std::shared_ptr<const DebugPointMap> _debug_points;
};

} // namespace doris
