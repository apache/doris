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
#include <rapidjson/document.h>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta_service.h"
#include "meta_service_http.h"

namespace doris::cloud {

std::map<std::string, std::function<void()>> suite_map;
std::once_flag register_suites_once;

// define a struct to store value only
struct TypedValue {
    std::variant<int64_t, bool, std::string, double> value;
};

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
    suite_map.emplace("Transaction::commit.enable_inject", []() {
        auto* sp = SyncPoint::get_instance();
        sp->set_call_back("Transaction::commit.inject_random_fault", [](auto&& args) {
            std::mt19937 gen {std::random_device {}()};
            double p {-1.0};
            TEST_INJECTION_POINT_CALLBACK("Transaction::commit.inject_random_fault.set_p", &p);
            if (p < 0 || p > 1.0) {
                p = 0.01; // default injection possibility is 1%
            }
            std::bernoulli_distribution inject_fault {p};
            if (inject_fault(gen)) {
                std::bernoulli_distribution err_type {0.5};
                fdb_error_t inject_err = (err_type(gen) ? /* FDB_ERROR_CODE_TXN_CONFLICT*/ 1020
                                                        : /* FDB_ERROR_CODE_TXN_TOO_OLD */ 1007);
                LOG_WARNING("inject {} err when txn->commit()", inject_err);
                *try_any_cast<fdb_error_t*>(args[0]) = inject_err;
            }
        });
        LOG_INFO("enable Transaction::commit.enable_inject");
    });
}

bool url_decode(const std::string& in, std::string* out) {
    out->clear();
    out->reserve(in.size());

    for (size_t i = 0; i < in.size(); ++i) {
        if (in[i] == '%') {
            if (i + 3 <= in.size()) {
                int value = 0;
                std::istringstream is(in.substr(i + 1, 2));

                if (is >> std::hex >> value) {
                    (*out) += static_cast<char>(value);
                    i += 2;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else if (in[i] == '+') {
            (*out) += ' ';
        } else {
            (*out) += in[i];
        }
    }

    return true;
}

HttpResponse set_value(const std::string& point, const brpc::URI& uri) {
    std::string value_str(http_query(uri, "value"));
    std::string decoded_value;
    if (!url_decode(value_str, &decoded_value)) {
        auto msg = fmt::format("failed to decode value: {}", value_str);
        LOG(WARNING) << msg;
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
    }
    rapidjson::Document doc;
    if (doc.Parse(decoded_value.c_str()).HasParseError()) {
        auto msg = fmt::format("invalid json value: {}", decoded_value);
        LOG(WARNING) << msg;
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    if (!doc.IsArray()) {
        auto msg = "value must be a json array";
        LOG(WARNING) << msg;
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    // use vector to keep order
    std::vector<TypedValue> parsed_values;

    for (const auto& value : doc.GetArray()) {
        TypedValue typed_value;
        try {
            if (value.IsBool()) {
                typed_value.value = value.GetBool();
            } else if (value.IsInt64()) {
                typed_value.value = value.GetInt64();
            } else if (value.IsDouble()) {
                typed_value.value = value.GetDouble();
            } else if (value.IsString()) {
                typed_value.value = value.GetString();
            } else {
                auto msg = "value must be boolean, integer, double or string";
                LOG(WARNING) << msg;
                return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
            }
            parsed_values.push_back(std::move(typed_value));
        } catch (const std::exception& e) {
            auto msg = fmt::format("failed to parse value");
            LOG(WARNING) << msg;
            return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
        }
    }

    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [point, parsed_values = std::move(parsed_values)](auto&& args) {
        LOG(INFO) << "injection point hit, point=" << point;
        for (size_t i = 0; i < parsed_values.size(); i++) {
            const auto& typed_value = parsed_values[i];
            std::visit(
                    [&](const auto& v) {
                        LOG(INFO) << "index=" << i << " value=" << v
                                  << " type=" << typeid(v).name();
                        if constexpr (std::is_same_v<std::decay_t<decltype(v)>, int64_t>) {
                            // process int64_t
                            *try_any_cast<int64_t*>(args[i]) = v;
                        } else if constexpr (std::is_same_v<std::decay_t<decltype(v)>, bool>) {
                            // process bool
                            *try_any_cast<bool*>(args[i]) = v;
                        } else if constexpr (std::is_same_v<std::decay_t<decltype(v)>, double>) {
                            // process double
                            *try_any_cast<double*>(args[i]) = v;
                        } else if constexpr (std::is_same_v<std::decay_t<decltype(v)>,
                                                            std::string>) {
                            // process string
                            *try_any_cast<std::string*>(args[i]) = v;
                        }
                    },
                    typed_value.value);
        }
    });
    return http_json_reply(MetaServiceCode::OK, "OK");
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
    } else if (behavior == "change_args") {
        return set_value(point, uri);
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

// ATTN: change_args use uri encode, see example test_ms_api.groovy
// use inject point in cpp file
// bool testBool = false;
// std::string testString = "world";
// TEST_SYNC_POINT_CALLBACK("resource_manager::set_safe_drop_time", &exceed_time, &testBool, &testString);

// curl http://175.40.101.1:5000/MetaService/http/v1/injection_point?token=greedisgood9999&o
// p=set&name=resource_manager::set_safe_drop_time&behavior=change_args&value=%5B-1%2Ctrue%2C%22hello%22%5D
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