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

#include "service/http/http_token_manager.h"

#include <chrono>
#include <cstddef>
#include <utility>

#include "common/config.h"
#include "common/logging.h"

namespace doris {

HttpTokenManager& HttpTokenManager::instance() {
    static HttpTokenManager mgr;
    return mgr;
}

int64_t HttpTokenManager::now_ms() const {
    if (_clock_ms) {
        return _clock_ms();
    }
    return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
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
    std::unique_lock lock(_mtx);
    _current_token = token;
    _previous_token.clear();
    _previous_expire_at_ms = 0;
    _initialized = true;
}

void HttpTokenManager::set_clock_for_test(std::function<int64_t()> clock_ms) {
    std::unique_lock lock(_mtx);
    _clock_ms = std::move(clock_ms);
}

void HttpTokenManager::reset_for_test(const std::string& token) {
    std::unique_lock lock(_mtx);
    _clock_ms = nullptr;
    _current_token = token;
    _previous_token.clear();
    _previous_expire_at_ms = 0;
    _initialized = true;
}

bool HttpTokenManager::verify(std::string_view token) const {
    if (token.empty()) {
        return false;
    }

    std::shared_lock lock(_mtx);
    const std::string& current = _initialized ? _current_token : config::http_token;
    if (!current.empty() && constant_time_equals(token, current)) {
        return true;
    }
    if (_initialized && !_previous_token.empty() && now_ms() < _previous_expire_at_ms &&
        constant_time_equals(token, _previous_token)) {
        return true;
    }
    return false;
}

Status HttpTokenManager::rotate(const std::string& base_conf_path,
                                const std::string& custom_conf_path) {
    std::lock_guard rotate_lock(_rotate_mtx);

    std::string new_token;
    bool found = false;
    Status read_st = config::read_effective_string_config("http_token", &new_token, &found,
                                                          base_conf_path, custom_conf_path);
    if (!read_st.ok()) {
        LOG(WARNING) << "failed to read configured http_token during rotation";
        return Status::InternalError("Failed to read configured http_token");
    }
    if (!found || new_token.empty()) {
        LOG(WARNING) << "configured http_token is empty during rotation";
        return Status::InvalidArgument("Configured http_token must not be empty");
    }

    {
        std::unique_lock lock(_mtx);
        const std::string& current = _initialized ? _current_token : config::http_token;
        if (new_token == current) {
            if (!_initialized) {
                _current_token = current;
                _initialized = true;
            }
            LOG(INFO) << "rotate_http_token succeeded (idempotent, unchanged)";
            return Status::OK();
        }

        _previous_token = current;
        _current_token = new_token;
        _previous_expire_at_ms = now_ms() + kHttpTokenGracePeriodMs;
        _initialized = true;
    }

    if (config::full_conf_map != nullptr) {
        std::lock_guard<std::mutex> conf_lock(*config::get_mutable_string_config_lock());
        (*config::full_conf_map)["http_token"] = new_token;
    }
    LOG(INFO) << "rotate_http_token succeeded";
    return Status::OK();
}

} // namespace doris
