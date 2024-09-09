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

#include "cloud/injection_point_action.h"

#include <glog/logging.h>

#include <chrono>
#include <mutex>

#include "common/status.h"
#include "cpp/sync_point.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/rowset/rowset.h"
#include "util/stack_util.h"

namespace doris {
namespace {

// TODO(cyx): Provide an object pool
// `suite_map` won't be modified after `register_suites`
std::map<std::string, std::function<void()>> suite_map;
std::once_flag register_suites_once;

// only call once
void register_suites() {
    suite_map.emplace("test_compaction", [] {
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("new_cumulative_point", [](auto&& args) {
            auto output_rowset = try_any_cast<Rowset*>(args[0]);
            auto last_cumulative_point = try_any_cast<int64_t>(args[1]);
            auto& [ret_vault, should_ret] = *try_any_cast<std::pair<int64_t, bool>*>(args.back());
            ret_vault = output_rowset->start_version() == last_cumulative_point
                                ? output_rowset->end_version() + 1
                                : last_cumulative_point;
            should_ret = true;
        });
    });
    suite_map.emplace("test_s3_file_writer", [] {
        auto* sp = SyncPoint::get_instance();
        sp->set_call_back("UploadFileBuffer::upload_to_local_file_cache", [](auto&&) {
            std::srand(static_cast<unsigned int>(std::time(nullptr)));
            int random_sleep_time_second = std::rand() % 10 + 1;
            std::this_thread::sleep_for(std::chrono::seconds(random_sleep_time_second));
        });
        sp->set_call_back("UploadFileBuffer::upload_to_local_file_cache_inject", [](auto&& args) {
            auto& [ret_status, should_ret] = *try_any_cast<std::pair<Status, bool>*>(args.back());
            ret_status =
                    Status::IOError<false>("failed to write into file cache due to inject error");
            should_ret = true;
        });
    });
    suite_map.emplace("test_storage_vault", [] {
        auto* sp = SyncPoint::get_instance();
        sp->set_call_back("HdfsFileWriter::append_hdfs_file_delay", [](auto&&) {
            std::srand(static_cast<unsigned int>(std::time(nullptr)));
            int random_sleep_time_second = std::rand() % 10 + 1;
            std::this_thread::sleep_for(std::chrono::seconds(random_sleep_time_second));
        });
        sp->set_call_back("HdfsFileWriter::append_hdfs_file_error", [](auto&& args) {
            auto& [_, should_ret] = *try_any_cast<std::pair<Status, bool>*>(args.back());
            should_ret = true;
        });
        sp->set_call_back("HdfsFileWriter::hdfsFlush", [](auto&& args) {
            auto& [ret_value, should_ret] = *try_any_cast<std::pair<Status, bool>*>(args.back());
            ret_value = Status::InternalError("failed to flush hdfs file");
            should_ret = true;
        });
        sp->set_call_back("HdfsFileWriter::hdfsCloseFile", [](auto&& args) {
            auto& [ret_value, should_ret] = *try_any_cast<std::pair<Status, bool>*>(args.back());
            ret_value = Status::InternalError("failed to flush hdfs file");
            should_ret = true;
        });
        sp->set_call_back("HdfsFileWriter::hdfeSync", [](auto&& args) {
            auto& [ret_value, should_ret] = *try_any_cast<std::pair<Status, bool>*>(args.back());
            ret_value = Status::InternalError("failed to flush hdfs file");
            should_ret = true;
        });
        sp->set_call_back("HdfsFileReader:read_error", [](auto&& args) {
            auto& [ret_status, should_ret] = *try_any_cast<std::pair<Status, bool>*>(args.back());
            ret_status = Status::InternalError("read hdfs error");
            should_ret = true;
        });
    });
    suite_map.emplace("test_cancel_node_channel", [] {
        auto* sp = SyncPoint::get_instance();
        sp->set_call_back("VNodeChannel::try_send_block", [](auto&& args) {
            LOG(INFO) << "injection VNodeChannel::try_send_block";
            auto* arg0 = try_any_cast<Status*>(args[0]);
            *arg0 = Status::InternalError<false>("test_cancel_node_channel injection error");
        });
        sp->set_call_back("VOlapTableSink::close",
                          [](auto&&) { std::this_thread::sleep_for(std::chrono::seconds(5)); });
    });
    suite_map.emplace("test_file_segment_cache_corruption", [] {
        auto* sp = SyncPoint::get_instance();
        sp->set_call_back("Segment::open:corruption", [](auto&& args) {
            LOG(INFO) << "injection Segment::open:corruption";
            auto* arg0 = try_any_cast<Status*>(args[0]);
            *arg0 = Status::Corruption<false>("test_file_segment_cache_corruption injection error");
        });
    });
    suite_map.emplace("test_file_segment_cache_corruption1", [] {
        auto* sp = SyncPoint::get_instance();
        sp->set_call_back("Segment::open:corruption1", [](auto&& args) {
            LOG(INFO) << "injection Segment::open:corruption1";
            auto* arg0 = try_any_cast<Status*>(args[0]);
            *arg0 = Status::Corruption<false>("test_file_segment_cache_corruption injection error");
        });
    });
    // curl be_ip:http_port/api/injection_point/apply_suite?name=test_cloud_meta_mgr_commit_txn'
    suite_map.emplace("test_cloud_meta_mgr_commit_txn", [] {
        auto* sp = SyncPoint::get_instance();
        sp->set_call_back("CloudMetaMgr::commit_txn", [](auto&& args) {
            LOG(INFO) << "injection CloudMetaMgr::commit_txn";
            auto* arg0 = try_any_cast_ret<Status>(args);
            arg0->first = Status::InternalError<false>(
                    "test_file_segment_cache_corruption injection error");
            arg0->second = true;
        });
    });
}

void set_sleep(const std::string& point, HttpRequest* req) {
    int duration = 0;
    auto& duration_str = req->param("duration");
    if (!duration_str.empty()) {
        try {
            duration = std::stoi(duration_str);
        } catch (const std::exception&) {
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                    "invalid duration: " + duration_str);
            return;
        }
    }
    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [point, duration](auto&& args) {
        LOG(INFO) << "injection point hit, point=" << point << " sleep milliseconds=" << duration;
        std::this_thread::sleep_for(std::chrono::milliseconds(duration));
    });
    HttpChannel::send_reply(req, HttpStatus::OK, "OK");
}

void set_return(const std::string& point, HttpRequest* req) {
    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [point](auto&& args) {
        try {
            LOG(INFO) << "injection point hit, point=" << point << " return void";
            auto pred = try_any_cast<bool*>(args.back());
            *pred = true;
        } catch (const std::bad_any_cast&) {
            LOG_EVERY_N(ERROR, 10) << "failed to process `return` callback\n" << get_stack_trace();
        }
    });
    HttpChannel::send_reply(req, HttpStatus::OK, "OK");
}

void set_return_ok(const std::string& point, HttpRequest* req) {
    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [point](auto&& args) {
        try {
            LOG(INFO) << "injection point hit, point=" << point << " return ok";
            auto* pair = try_any_cast_ret<Status>(args);
            pair->first = Status::OK();
            pair->second = true;
        } catch (const std::bad_any_cast&) {
            LOG_EVERY_N(ERROR, 10) << "failed to process `return_ok` callback\n"
                                   << get_stack_trace();
        }
    });
    HttpChannel::send_reply(req, HttpStatus::OK, "OK");
}

void set_return_error(const std::string& point, HttpRequest* req) {
    const std::string CODE_PARAM = "code";
    int code = ErrorCode::INTERNAL_ERROR;
    auto& code_str = req->param(CODE_PARAM);
    if (!code_str.empty()) {
        try {
            code = std::stoi(code_str);
        } catch (const std::exception& e) {
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                    fmt::format("convert topn failed, {}", e.what()));
            return;
        }
    }

    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [code, point](auto&& args) {
        try {
            LOG(INFO) << "injection point hit, point=" << point << " return error code=" << code;
            auto* pair = try_any_cast_ret<Status>(args);
            pair->first = Status::Error<false>(code, "injected error");
            pair->second = true;
        } catch (const std::bad_any_cast&) {
            LOG_EVERY_N(ERROR, 10) << "failed to process `return_error` callback\n"
                                   << get_stack_trace();
        }
    });
    HttpChannel::send_reply(req, HttpStatus::OK, "OK");
}

void handle_set(HttpRequest* req) {
    auto& point = req->param("name");
    if (point.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "empty point name");
        return;
    }
    auto& behavior = req->param("behavior");
    if (behavior.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "empty behavior");
        return;
    }
    if (behavior == "sleep") {
        set_sleep(point, req);
        return;
    } else if (behavior == "return") {
        set_return(point, req);
        return;
    } else if (behavior == "return_ok") {
        set_return_ok(point, req);
        return;
    } else if (behavior == "return_error") {
        set_return_error(point, req);
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "unknown behavior: " + behavior);
}

void handle_clear(HttpRequest* req) {
    const auto& point = req->param("name");
    auto* sp = SyncPoint::get_instance();
    LOG(INFO) << "clear injection point : " << (point.empty() ? "(all points)" : point);
    if (point.empty()) {
        // If point name is emtpy, clear all
        sp->clear_all_call_backs();
        HttpChannel::send_reply(req, HttpStatus::OK, "OK");
        return;
    }

    sp->clear_call_back(point);
    HttpChannel::send_reply(req, HttpStatus::OK, "OK");
}

void handle_apply_suite(HttpRequest* req) {
    auto& suite = req->param("name");
    if (suite.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "empty suite name");
        return;
    }

    std::call_once(register_suites_once, register_suites);
    if (auto it = suite_map.find(suite); it != suite_map.end()) {
        it->second(); // set injection callbacks
        HttpChannel::send_reply(req, HttpStatus::OK, "OK apply suite " + suite + "\n");
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                            "unknown suite: " + suite + "\n");
}

void handle_enable(HttpRequest* req) {
    SyncPoint::get_instance()->enable_processing();
    HttpChannel::send_reply(req, HttpStatus::OK, "OK");
}

void handle_disable(HttpRequest* req) {
    SyncPoint::get_instance()->disable_processing();
    HttpChannel::send_reply(req, HttpStatus::OK, "OK");
}

} // namespace

InjectionPointAction::InjectionPointAction() = default;

//
// enable/disable injection point
// ```
// curl "be_ip:http_port/api/injection_point/enable"
// curl "be_ip:http_port/api/injection_point/disable"
// ```
//
// clear all injection points
// ```
// curl "be_ip:http_port/api/injection_point/clear"
// ```
//
// apply/activate specific suite with registered action, see `register_suites()` for more details
// ```
// curl "be_ip:http_port/api/injection_point/apply_suite?name=${suite_name}"
// ```
//
// set predifined action for specific injection point, supported actions are:
// * sleep: for injection point with callback, accepted param is `duration` in milliseconds
// * return: for injection point without return value (return void)
// * return_ok: for injection point with return value, always return Status::OK
// * return_error: for injection point with return value, accepted param is `code`,
//                 which is an int, valid values can be found in status.h, e.g. -235 or -230,
//                 if `code` is not present return Status::InternalError
// ```
// curl "be_ip:http_port/api/injection_point/set?name=${injection_point_name}&behavior=sleep&duration=${x_millsec}" # sleep x millisecs
// curl "be_ip:http_port/api/injection_point/set?name=${injection_point_name}&behavior=return" # return void
// curl "be_ip:http_port/api/injection_point/set?name=${injection_point_name}&behavior=return_ok" # return ok
// curl "be_ip:http_port/api/injection_point/set?name=${injection_point_name}&behavior=return_error" # internal error
// curl "be_ip:http_port/api/injection_point/set?name=${injection_point_name}&behavior=return_error&code=${code}" # -235
// ```
void InjectionPointAction::handle(HttpRequest* req) {
    LOG(INFO) << "handle InjectionPointAction " << req->debug_string();
    auto& op = req->param("op");
    if (op == "set") {
        handle_set(req);
        return;
    } else if (op == "clear") {
        handle_clear(req);
        return;
    } else if (op == "apply_suite") {
        handle_apply_suite(req);
        return;
    } else if (op == "enable") {
        handle_enable(req);
        return;
    } else if (op == "disable") {
        handle_disable(req);
        return;
    }

    HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "unknown op: " + op);
}

} // namespace doris
