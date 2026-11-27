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

#include "common/http_token_manager.h"

#include <chrono>
#include <cstddef>

#include "common/config.h"
#include "common/configbase.h"
#include "common/logging.h"
#include "cpp/sync_point.h"

namespace doris::cloud {

HttpTokenManager& HttpTokenManager::instance() {
    static HttpTokenManager mgr;
    return mgr;
}

int64_t HttpTokenManager::now_ms() {
    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now().time_since_epoch())
                          .count();
    TEST_SYNC_POINT_CALLBACK("HttpTokenManager::now_ms", &now);
    return now;
}

bool HttpTokenManager::constant_time_equals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) {
        return false;
    }
    unsigned char diff = 0;
    for (size_t i = 0; i < a.size(); ++i) {
        diff |= static_cast<unsigned char>(a[i]) ^ static_cast<unsigned char>(b[i]);
    }
    return diff == 0;
}

void HttpTokenManager::init(const std::string& token) {
    std::unique_lock lock(mtx_);
    current_token_ = token;
    previous_token_.clear();
    previous_expire_at_ms_ = 0;
    rotated_ = false;
}

void HttpTokenManager::reset_for_test(const std::string& token) {
    init(token);
}

bool HttpTokenManager::verify(std::string_view token) const {
    if (token.empty()) {
        return false;
    }

    std::shared_lock lock(mtx_);
    if (!rotated_) {
        if (config::http_token.empty()) {
            return false;
        }
        return constant_time_equals(token, config::http_token);
    }

    if (!current_token_.empty() && constant_time_equals(token, current_token_)) {
        return true;
    }
    if (!previous_token_.empty() && now_ms() < previous_expire_at_ms_ &&
        constant_time_equals(token, previous_token_)) {
        return true;
    }
    return false;
}

MetaServiceCode HttpTokenManager::rotate(std::string* err_msg, const std::string& base_conf_path,
                                         const std::string& custom_conf_path) {
    std::lock_guard rotate_lock(rotate_mtx_);

    std::string new_token;
    auto read_status = config::read_effective_string_config("http_token", &new_token,
                                                            base_conf_path, custom_conf_path);
    if (read_status == config::ReadConfigStatus::READ_ERROR) {
        if (err_msg != nullptr) {
            *err_msg = "Failed to read configured http_token";
        }
        LOG(WARNING) << "failed to read configured http_token during rotation";
        return MetaServiceCode::UNDEFINED_ERR;
    }
    if (read_status == config::ReadConfigStatus::NOT_FOUND || new_token.empty()) {
        if (err_msg != nullptr) {
            *err_msg = "Configured http_token must not be empty";
        }
        LOG(WARNING) << "configured http_token is empty during rotation";
        return MetaServiceCode::INVALID_ARGUMENT;
    }

    {
        std::unique_lock lock(mtx_);
        const std::string& current = rotated_ ? current_token_ : config::http_token;
        if (new_token == current) {
            if (!rotated_) {
                current_token_ = current;
                rotated_ = true;
            }
            LOG(INFO) << "rotate_http_token succeeded (idempotent, unchanged)";
            return MetaServiceCode::OK;
        }

        previous_token_ = current;
        current_token_ = new_token;
        previous_expire_at_ms_ = now_ms() + kHttpTokenGracePeriodMs;
        rotated_ = true;
    }

    if (config::full_conf_map != nullptr) {
        std::unique_lock conf_lock(*config::get_mutable_string_config_lock());
        (*config::full_conf_map)["http_token"] = new_token;
    }
    LOG(INFO) << "rotate_http_token succeeded";
    return MetaServiceCode::OK;
}

} // namespace doris::cloud
