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
#include <memory>
#include <mutex>

#include "common/bvars.h"
#include "common/config.h"
#include "common/configbase.h"

namespace doris::cloud {

void RateLimiter::init(google::protobuf::Service* service) {
    std::map<std::string, int64_t> rpc_name_to_max_qps_limit;
    std::vector<std::string> max_qps_limit_list;
    butil::SplitString(config::specific_max_qps_limit, ';', &max_qps_limit_list);
    for (const auto& v : max_qps_limit_list) {
        auto p = v.find(':');
        if (p != std::string::npos && p != (v.size() - 1)) {
            auto rpc_name = v.substr(0, p);
            try {
                int64_t max_qps_limit = std::stoll(v.substr(p + 1));
                if (max_qps_limit > 0) {
                    rpc_name_to_max_qps_limit[rpc_name] = max_qps_limit;
                    LOG(INFO) << "set rpc: " << rpc_name << " max_qps_limit: " << max_qps_limit;
                }
            } catch (...) {
                LOG(WARNING) << "failed to set max_qps_limit to rpc: " << rpc_name
                             << " config: " << v;
            }
        }
    }
    auto method_size = service->GetDescriptor()->method_count();
    for (auto i = 0; i < method_size; ++i) {
        std::string rpc_name = service->GetDescriptor()->method(i)->name();
        int64_t max_qps_limit = config::default_max_qps_limit;

        auto it = rpc_name_to_max_qps_limit.find(rpc_name);
        if (it != rpc_name_to_max_qps_limit.end()) {
            max_qps_limit = it->second;
        }
        limiters_[rpc_name] = std::make_shared<RpcRateLimiter>(rpc_name, max_qps_limit);
    }
}

std::shared_ptr<RpcRateLimiter> RateLimiter::get_rpc_rate_limiter(const std::string& rpc_name) {
    // no need to be locked, because it is only modified during initialization
    auto it = limiters_.find(rpc_name);
    if (it == limiters_.end()) {
        return nullptr;
    }
    return it->second;
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

} // namespace doris::cloud