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

#include "rate_limiter.h"

#include <butil/strings/string_split.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "common/bvars.h"
#include "common/config.h"
#include "common/configbase.h"

namespace doris::cloud {

std::unordered_map<std::string, int64_t> parse_specific_qps_limit(const std::string& list_str) {
    std::unordered_map<std::string, int64_t> rpc_name_to_max_qps_limit;
    std::vector<std::string> max_qps_limit_list;
    butil::SplitString(list_str, ';', &max_qps_limit_list);
    for (const auto& v : max_qps_limit_list) {
        auto p = v.find(':');
        if (p != std::string::npos && p != (v.size() - 1)) {
            auto rpc_name = v.substr(0, p);
            try {
                int64_t max_qps_limit = std::stoll(v.substr(p + 1));
                if (max_qps_limit > 0) {
                    rpc_name_to_max_qps_limit[rpc_name] = max_qps_limit;
                }
            } catch (...) {
                LOG(WARNING) << "failed to parse max_qps_limit to rpc: " << rpc_name
                             << " config: " << v;
            }
        }
    }
    return rpc_name_to_max_qps_limit;
}

template <typename Callable>
void for_each_rpc_name(google::protobuf::Service* service, Callable cb) {
    auto method_size = service->GetDescriptor()->method_count();
    for (auto i = 0; i < method_size; ++i) {
        std::string rpc_name = service->GetDescriptor()->method(i)->name();
        cb(rpc_name);
    }
}

void RateLimiter::init(google::protobuf::Service* service) {
    auto rpc_name_to_specific_limit = parse_specific_qps_limit(config::specific_max_qps_limit);
    std::unique_lock write_lock(shared_mtx_);
    for_each_rpc_name(service, [&](const std::string& rpc_name) {
        auto it = rpc_name_to_specific_limit.find(rpc_name);
        int64_t max_qps_limit = config::default_max_qps_limit;
        if (it != rpc_name_to_specific_limit.end()) {
            max_qps_limit = it->second;
        }
        limiters_[rpc_name] = std::make_shared<RpcRateLimiter>(rpc_name, max_qps_limit);
    });
    for (const auto& [k, _] : rpc_name_to_specific_limit) {
        rpc_with_specific_limit_.insert(k);
    }
}

std::shared_ptr<RpcRateLimiter> RateLimiter::get_rpc_rate_limiter(const std::string& rpc_name) {
    std::shared_lock read_lock(shared_mtx_);
    auto it = limiters_.find(rpc_name);
    if (it == limiters_.end()) {
        return nullptr;
    }
    return it->second;
}

void RateLimiter::reset_rate_limit(google::protobuf::Service* service, int64_t default_qps_limit,
                                   const std::string& specific_max_qps_limit) {
    // TODO: merge specific_max_qps_limit
    auto specific_limits = parse_specific_qps_limit(specific_max_qps_limit);

    auto reset_specific_limit = [&](const std::string& rpc_name) -> bool {
        if (auto it = specific_limits.find(rpc_name); it != specific_limits.end()) {
            limiters_[rpc_name]->reset_max_qps_limit(it->second);
            return true;
        }
        return false;
    };
    auto reset_default_limit = [&](const std::string& rpc_name) {
        if (rpc_with_specific_limit_.contains(rpc_name)) {
            return;
        }
        limiters_[rpc_name]->reset_max_qps_limit(default_qps_limit);
    };

    std::unique_lock write_lock(shared_mtx_);
    for (const auto& [k, _] : specific_limits) {
        rpc_with_specific_limit_.insert(k);
    }
    if (default_qps_limit < 0) {
        for_each_rpc_name(service, std::move(reset_specific_limit));
        return;
    }
    if (specific_limits.empty()) {
        for_each_rpc_name(service, std::move(reset_default_limit));
        return;
    }
    for_each_rpc_name(service, [&](const std::string& rpc_name) {
        if (reset_specific_limit(rpc_name)) {
            return;
        }
        reset_default_limit(rpc_name);
    });
}

void RateLimiter::for_each_rpc_limiter(
        std::function<void(std::string_view, std::shared_ptr<RpcRateLimiter>)> cb) {
    for (const auto& [rpc_name, rpc_limiter] : limiters_) {
        cb(rpc_name, rpc_limiter);
    }
}

bool RpcRateLimiter::get_qps_token(const std::string& instance_id,
                                   std::function<int()>& get_bvar_qps) {
    if (!config::use_detailed_metrics || instance_id.empty()) {
        return true;
    }
    std::shared_ptr<QpsToken> qps_token = nullptr;
    {
        std::lock_guard<bthread::Mutex> l(mutex_);
        auto it = qps_limiter_.find(instance_id);

        // new instance always can get token
        if (it == qps_limiter_.end()) {
            qps_token = std::make_shared<QpsToken>(max_qps_limit_);
            qps_limiter_[instance_id] = qps_token;
            return true;
        }
        qps_token = it->second;
    }

    return qps_token->get_token(get_bvar_qps);
}

void RpcRateLimiter::reset_max_qps_limit(int64_t max_qps_limit) {
    std::lock_guard<bthread::Mutex> l(mutex_);
    max_qps_limit_ = max_qps_limit;
    for (auto& [_, v] : qps_limiter_) {
        v->reset_max_qps_limit(max_qps_limit);
    }
}

bool RpcRateLimiter::QpsToken::get_token(std::function<int()>& get_bvar_qps) {
    using namespace std::chrono;
    auto now = steady_clock::now();
    std::lock_guard<bthread::Mutex> l(mutex_);
    // Todo: if current_qps_ > max_qps_limit_, always return false until the bvar qps is updated,
    //        maybe need to reduce the bvar's update interval.
    ++access_count_;
    auto duration_s = duration_cast<seconds>(now - last_update_time_).count();
    if (duration_s > config::bvar_qps_update_second ||
        (duration_s != 0 && (access_count_ / duration_s > max_qps_limit_ / 2))) {
        access_count_ = 0;
        last_update_time_ = now;
        current_qps_ = get_bvar_qps();
    }
    return current_qps_ < max_qps_limit_;
}

void RpcRateLimiter::QpsToken::reset_max_qps_limit(int64_t max_qps_limit) {
    std::lock_guard<bthread::Mutex> l(mutex_);
    max_qps_limit_ = max_qps_limit;
}

} // namespace doris::cloud
