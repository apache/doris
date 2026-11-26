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

#include <gen_cpp/cloud.pb.h>

#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>

namespace doris::cloud {

class HttpTokenManager {
public:
    static constexpr int64_t kHttpTokenGracePeriodSeconds = 1800;
    static constexpr int64_t kHttpTokenGracePeriodMs = kHttpTokenGracePeriodSeconds * 1000;

    static HttpTokenManager& instance();

    void init(const std::string& token);

    bool verify(std::string_view token) const;

    // Reload http_token from effective config files and rotate runtime token state.
    // Returns MetaServiceCode::OK on success, INVALID_ARGUMENT if configured token is empty,
    // or UNDEFINED_ERR if reading/parsing config files fails.
    MetaServiceCode rotate(std::string* err_msg, const std::string& base_conf_path = "",
                           const std::string& custom_conf_path = "");

    void reset_for_test(const std::string& token);

private:
    HttpTokenManager() = default;

    static int64_t now_ms();
    static bool constant_time_equals(std::string_view a, std::string_view b);

    mutable std::shared_mutex mtx_;
    std::mutex rotate_mtx_;
    bool rotated_ = false;
    std::string current_token_;
    std::string previous_token_;
    int64_t previous_expire_at_ms_ = 0;
};

} // namespace doris::cloud
