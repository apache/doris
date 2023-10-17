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
#include <boost/lexical_cast.hpp>
#include <functional>
#include <map>
#include <memory>
#include <type_traits>

#include "common/compiler_util.h"
#include "common/config.h"

#define DBUG_EXECUTE_IF(debug_point_name, code)                               \
    if (UNLIKELY(config::enable_debug_points)) {                              \
        auto dp = DebugPoints::instance()->get_debug_point(debug_point_name); \
        if (dp) {                                                             \
            code;                                                             \
        }                                                                     \
    }

namespace doris {

struct DebugPoint {
    std::atomic<int64_t> execute_num {0};
    int64_t execute_limit = -1;
    int64_t expire_ms = -1;

    std::map<std::string, std::string> params;

    template <typename T = int>
    T param(const std::string& key, T default_value = T()) {
        auto it = params.find(key);
        if (it == params.end()) {
            return default_value;
        }
        if constexpr (std::is_same_v<T, bool>) {
            if (it->second == "true") {
                return true;
            }
            if (it->second == "false") {
                return false;
            }
            return boost::lexical_cast<T>(it->second);
        } else if constexpr (std::is_arithmetic_v<T>) {
            return boost::lexical_cast<T>(it->second);
        } else {
            static_assert(std::is_same_v<T, std::string>);
            return it->second;
        }
    }

    std::string param(const std::string& key, const char* default_value) {
        return param<std::string>(key, std::string(default_value));
    }
};

class DebugPoints {
public:
    bool is_enable(const std::string& name);
    std::shared_ptr<DebugPoint> get_debug_point(const std::string& name);
    void add(const std::string& name, std::shared_ptr<DebugPoint> debug_point);
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
