
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

#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"
#include "meta_service.h"
#include "meta_service_http.h"

namespace doris::cloud {

std::map<std::string, std::function<void()>> suite_map;
std::once_flag register_suites_once;

inline std::default_random_engine make_random_engine() {
    return std::default_random_engine(
            static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
}

static void register_suites() {
    suite_map.emplace("test_txn_lazy_commit", [] {
        auto sp = SyncPoint::get_instance();

        sp->set_call_back("commit_txn_immediately::advance_last_pending_txn_id", [&](auto&& args) {
            std::default_random_engine rng = make_random_engine();
            std::uniform_int_distribution<uint32_t> u(100, 1000);
            uint32_t duration_ms = u(rng);
            LOG(INFO) << "commit_txn_immediately::advance_last_pending_txn_id sleep " << duration_ms
                      << " ms";
            std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
        });

        sp->set_call_back("commit_txn_eventually::txn_lazy_committer_submit", [&](auto&& args) {
            std::default_random_engine rng = make_random_engine();
            std::uniform_int_distribution<uint32_t> u(100, 1000);
            uint32_t duration_ms = u(rng);
            LOG(INFO) << "commit_txn_eventually::txn_lazy_committer_submit sleep " << duration_ms
                      << " ms";
            std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
        });

        sp->set_call_back("commit_txn_eventually::txn_lazy_committer_wait", [&](auto&& args) {
            std::default_random_engine rng = make_random_engine();
            std::uniform_int_distribution<uint32_t> u(100, 1000);
            uint32_t duration_ms = u(rng);
            LOG(INFO) << "commit_txn_eventually::txn_lazy_committer_wait sleep " << duration_ms
                      << " ms";
            std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
        });

        sp->set_call_back("convert_tmp_rowsets::before_commit", [&](auto&& args) {
            std::default_random_engine rng = make_random_engine();
            std::uniform_int_distribution<uint32_t> u(1, 50);
            uint32_t duration_ms = u(rng);
            std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
            LOG(INFO) << "convert_tmp_rowsets::before_commit sleep " << duration_ms << " ms";
            if (duration_ms <= 25) {
                MetaServiceCode* code = try_any_cast<MetaServiceCode*>(args[0]);
                *code = MetaServiceCode::KV_TXN_CONFLICT;
                bool* pred = try_any_cast<bool*>(args.back());
                *pred = true;
                LOG(INFO) << "convert_tmp_rowsets::before_commit random_value=" << duration_ms
                          << " inject kv txn conflict";
            }
        });
    });
}

HttpResponse set_sleep(const std::string& point, const brpc::URI& uri) {
    std::string duration_str(http_query(uri, "duration"));
    int64_t duration = 0;
    try {
        duration = std::stol(duration_str);
    } catch (const std::exception& e) {
        auto msg = fmt::format("invalid duration:{}", duration_str);
        LOG(WARNING) << msg;
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [point, duration](auto&& args) {
        LOG(INFO) << "injection point hit, point=" << point << " sleep ms=" << duration;
        std::this_thread::sleep_for(std::chrono::milliseconds(duration));
    });
    return http_json_reply(MetaServiceCode::OK, "OK");
}

HttpResponse set_return(const std::string& point, const brpc::URI& uri) {
    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [point](auto&& args) {
        try {
            LOG(INFO) << "injection point hit, point=" << point << " return void";
            auto pred = try_any_cast<bool*>(args.back());
            *pred = true;
        } catch (const std::bad_any_cast& e) {
            LOG(ERROR) << "failed to process `return` e:" << e.what();
        }
    });

    return http_json_reply(MetaServiceCode::OK, "OK");
}

HttpResponse handle_set(const brpc::URI& uri) {
    const std::string point(http_query(uri, "name"));
    if (point.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "empty point name");
    }

    const std::string behavior(http_query(uri, "behavior"));
    if (behavior.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "empty behavior");
    }
    if (behavior == "sleep") {
        return set_sleep(point, uri);
    } else if (behavior == "return") {
        return set_return(point, uri);
    }

    return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "unknown behavior: " + behavior);
}

HttpResponse handle_clear(const brpc::URI& uri) {
    const std::string point(http_query(uri, "name"));
    auto* sp = SyncPoint::get_instance();
    LOG(INFO) << "clear injection point : " << (point.empty() ? "(all points)" : point);
    if (point.empty()) {
        // If point name is emtpy, clear all
        sp->clear_all_call_backs();
        return http_json_reply(MetaServiceCode::OK, "OK");
    }
    sp->clear_call_back(point);
    return http_json_reply(MetaServiceCode::OK, "OK");
}

HttpResponse handle_apply_suite(const brpc::URI& uri) {
    const std::string suite(http_query(uri, "name"));
    if (suite.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "empty suite name");
    }

    std::call_once(register_suites_once, register_suites);
    if (auto it = suite_map.find(suite); it != suite_map.end()) {
        it->second(); // set injection callbacks
        return http_json_reply(MetaServiceCode::OK, "OK apply suite " + suite + "\n");
    }

    return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "unknown suite: " + suite + "\n");
}

HttpResponse handle_enable(const brpc::URI& uri) {
    SyncPoint::get_instance()->enable_processing();
    return http_json_reply(MetaServiceCode::OK, "OK");
}

HttpResponse handle_disable(const brpc::URI& uri) {
    SyncPoint::get_instance()->disable_processing();
    return http_json_reply(MetaServiceCode::OK, "OK");
}

//
// enable/disable injection point
// ```
// curl "ms_ip:port/MetaService/http/v1/injection_point?token=greedisgood9999&op=enable"
// curl "ms_ip:port/MetaService/http/v1/injection_point?token=greedisgood9999&op=disable"
// ```

// clear all injection points
// ```
// curl "ms_ip:port/MetaService/http/v1/injection_point?token=greedisgood9999&op=clear"
// ```

// apply/activate specific suite with registered action, see `register_suites()` for more details
// ```
// curl "ms_ip:port/MetaService/http/v1/injection_point?token=greedisgood9999&op=apply_suite&name=${suite_name}"
// ```

// ```
// curl "ms_ip:port/MetaService/http/v1/injection_point?token=greedisgood9999&op=set
//     &name=${injection_point_name}&behavior=sleep&duration=${x_millsec}" # sleep x millisecs

// curl "ms_ip:port/MetaService/http/v1/injection_point?token=greedisgood9999&op=set
//     &name=${injection_point_name}&behavior=return" # return void
// ```

HttpResponse process_injection_point(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    LOG(INFO) << "handle InjectionPointAction uri:" << uri;
    const std::string op(http_query(uri, "op"));

    if (op == "set") {
        return handle_set(uri);
    } else if (op == "clear") {
        return handle_clear(uri);
    } else if (op == "apply_suite") {
        return handle_apply_suite(uri);
    } else if (op == "enable") {
        return handle_enable(uri);
    } else if (op == "disable") {
        return handle_disable(uri);
    }

    return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "unknown op:" + op);
}
} // namespace doris::cloud