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

#include <cstdint>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "common/status.h"

namespace doris {

class HttpTokenManager {
public:
    static constexpr int64_t kHttpTokenGracePeriodSeconds = 1800;
    static constexpr int64_t kHttpTokenGracePeriodMs = kHttpTokenGracePeriodSeconds * 1000;

    static HttpTokenManager& instance();

    void init(const std::string& token);

    bool verify(std::string_view token) const;

    // Reload http_token from effective config files (be.conf overlaid by be_custom.conf) and
    // atomically rotate runtime token state. Returns Status::OK on success,
    // Status::InvalidArgument when empty, or Status::InternalError on read/parse failure.
    Status rotate(const std::string& base_conf_path = "", const std::string& custom_conf_path = "");

    void set_clock_for_test(std::function<int64_t()> clock_ms);
    void reset_for_test(const std::string& token);

private:
    HttpTokenManager() = default;

    int64_t now_ms() const;
    static bool constant_time_equals(std::string_view a, std::string_view b);

    mutable std::shared_mutex _mtx;
    std::mutex _rotate_mtx;
    bool _initialized = false;
    std::string _current_token;
    std::string _previous_token;
    int64_t _previous_expire_at_ms = 0;
    std::function<int64_t()> _clock_ms;
};

} // namespace doris
