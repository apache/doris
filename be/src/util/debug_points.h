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

#include <any>
#include <atomic>
#include <boost/lexical_cast.hpp>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <thread>
#include <type_traits>

#include "common/compiler_util.h"
#include "common/config.h"
#include "fmt/format.h"

// more usage can see 'util/debug_points_test.cpp'
#define DBUG_EXECUTE_IF(debug_point_name, code)                               \
    if (UNLIKELY(config::enable_debug_points)) {                              \
        auto dp = DebugPoints::instance()->get_debug_point(debug_point_name); \
        if (dp) {                                                             \
            [[maybe_unused]] auto DP_NAME = debug_point_name;                 \
            code;                                                             \
        }                                                                     \
    }

// define some common debug actions
// usage example: DBUG_EXECUTE_IF("xxx", DBUG_BLOCK);

#define DBUG_BLOCK                                                      \
    {                                                                   \
        LOG(INFO) << "start debug block " << DP_NAME;                   \
        while (DebugPoints::instance()->is_enable(DP_NAME)) {           \
            std::this_thread::sleep_for(std::chrono::milliseconds(10)); \
        }                                                               \
        LOG(INFO) << "end debug block " << DP_NAME;                     \
    }

// example of debug point with handler.
//
// demo code:
//
// void demo() {
//     int a = 100;
//     int b = 0;
//
//     DBUG_EXECUTE_IF("my_test", {
//          std::function<void(int, int&)> handler;
//          try {
//              handler = std::any_cast<decltype(handler)>(dp->handler);
//          } catch (const std::bad_any_cast& e)
//              LOG(WARNING) << "cast debug point my_test function failed, err=" << e.what();
//          }
//
//          if (handler) {
//              handler(a, b);
//          }
//     });
// }
//
// test code:
//
// void test {
//    int input_b = 1000;
//    int output_a = 0;
//
//    std::function<void(int, int&)> handler = [=input_b, &output_a](int a, int& b) {
//        output_a = a;
//        b = input_b;
//    }
//
//    DebugPoints.instance()->add_with_handler("my_test", handler);
//
//    ASSERT.EQ(100, output_a);
// }

namespace doris {

struct DebugPoint {
    std::atomic<int64_t> execute_num {0};
    int64_t execute_limit = -1;
    int64_t expire_ms = -1;

    std::map<std::string, std::string> params;

    // Usually `handler` use in be ut, to get local variable of the injected code,
    // or change with different injected handlers.
    // test/util/debug_points_test.cpp#Handler give a example.
    std::any handler;

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

    void add_with_handler(const std::string& name, std::any handler) {
        add(name, std::shared_ptr<DebugPoint>(new DebugPoint {.handler = handler}));
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
