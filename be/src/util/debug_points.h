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
#include "fmt/format.h"

// more usage can see 'util/debug_points_test.cpp'
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

    template <typename T>
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
        } else if constexpr (std::is_same_v<T, const char*>) {
            return it->second.c_str();
        } else {
            static_assert(std::is_same_v<T, std::string>);
            return it->second;
        }
    }
};

class DebugPoints {
public:
    bool is_enable(const std::string& name);
    std::shared_ptr<DebugPoint> get_debug_point(const std::string& name);
    void remove(const std::string& name);
    void clear();

    // if not enable debug point or its params not contains `key`, then return `default_value`
    // url: /api/debug_point/add/name?k1=v1&k2=v2&...
    template <typename T>
    T get_debug_param_or_default(const std::string& name, const std::string& key,
                                 const T& default_value) {
        auto debug_point = get_debug_point(name);
        return debug_point ? debug_point->param(key, default_value) : default_value;
    }

    // url: /api/debug_point/add/name?value=v
    template <typename T>
    T get_debug_param_or_default(const std::string& name, const T& default_value) {
        return get_debug_param_or_default(name, "value", default_value);
    }

    void add(const std::string& name, std::shared_ptr<DebugPoint> debug_point);

    // more 'add' functions for convenient use
    void add(const std::string& name) { add(name, std::make_shared<DebugPoint>()); }
    void add_with_params(const std::string& name,
                         const std::map<std::string, std::string>& params) {
        add(name, std::shared_ptr<DebugPoint>(new DebugPoint {.params = params}));
    }
    template <typename T>
    void add_with_value(const std::string& name, const T& value) {
        add_with_params(name, {{"value", fmt::format("{}", value)}});
    }

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
