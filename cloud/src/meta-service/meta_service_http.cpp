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

#include "meta_service_http.h"

#include <brpc/controller.h>
#include <brpc/http_status_code.h>
#include <brpc/uri.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <google/protobuf/util/json_util.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <array>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/http_helper.h"
#include "common/logging.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/txn_kv.h"
#include "meta_service.h"
#include "recycler/recycler_service.h"

namespace doris::cloud {

extern void get_kv_range_boundaries_count(std::vector<std::string>& partition_boundaries,
                                          std::unordered_map<std::string, size_t>& partition_count);

template <typename Message>
static google::protobuf::util::Status parse_json_message(const std::string& unresolved_path,
                                                         const std::string& body, Message* req) {
    static_assert(std::is_base_of_v<google::protobuf::Message, Message>);
    auto st = google::protobuf::util::JsonStringToMessage(body, req);
    if (!st.ok()) {
        std::string msg = "failed to strictly parse http request for '" + unresolved_path +
                          "' error: " + st.ToString();
        LOG_WARNING(msg).tag("body", encryt_sk(hide_access_key(body)));

        // ignore unknown fields
        google::protobuf::util::JsonParseOptions json_parse_options;
        json_parse_options.ignore_unknown_fields = true;
        return google::protobuf::util::JsonStringToMessage(body, req, json_parse_options);
    }
    return {};
}

std::tuple<int, std::string_view> convert_ms_code_to_http_code(MetaServiceCode ret) {
    switch (ret) {
    case OK:
        return {200, "OK"};
    case INVALID_ARGUMENT:
    case PROTOBUF_PARSE_ERR:
        return {400, "INVALID_ARGUMENT"};
    case CLUSTER_NOT_FOUND:
    case TABLET_NOT_FOUND:
        return {404, "NOT_FOUND"};
    case ALREADY_EXISTED:
        return {409, "ALREADY_EXISTED"};
    case KV_TXN_CREATE_ERR:
    case KV_TXN_GET_ERR:
    case KV_TXN_COMMIT_ERR:
    case PROTOBUF_SERIALIZE_ERR:
    case TXN_GEN_ID_ERR:
    case TXN_DUPLICATED_REQ:
    case TXN_LABEL_ALREADY_USED:
    case TXN_INVALID_STATUS:
    case TXN_LABEL_NOT_FOUND:
    case TXN_ID_NOT_FOUND:
    case TXN_ALREADY_ABORTED:
    case TXN_ALREADY_VISIBLE:
    case TXN_ALREADY_PRECOMMITED:
    case VERSION_NOT_FOUND:
    case UNDEFINED_ERR:
    default:
        return {500, "INTERNAL_ERROR"};
    }
}

HttpResponse http_json_reply(MetaServiceCode code, const std::string& msg,
                             std::optional<std::string> body) {
    auto [status_code, status_msg] = convert_ms_code_to_http_code(code);
    rapidjson::Document d;
    d.SetObject();
    if (code == MetaServiceCode::OK) {
        d.AddMember("code", "OK", d.GetAllocator());
        d.AddMember("msg", rapidjson::StringRef(msg.data(), msg.size()), d.GetAllocator());
    } else {
        d.AddMember("code", rapidjson::StringRef(status_msg.data(), status_msg.size()),
                    d.GetAllocator());
        d.AddMember("msg", rapidjson::StringRef(msg.data(), msg.size()), d.GetAllocator());
    }

    rapidjson::Document result;
    if (body.has_value()) {
        rapidjson::ParseResult ok = result.Parse(body->c_str());
        if (!ok) {
            LOG_WARNING("JSON parse error")
                    .tag("code", rapidjson::GetParseError_En(ok.Code()))
                    .tag("offset", ok.Offset());
            d.AddMember("code", "INTERNAL_ERROR", d.GetAllocator());
            d.AddMember("msg", "JSON parse error", d.GetAllocator());
        } else {
            d.AddMember("result", result, d.GetAllocator());
        }
    }

    rapidjson::StringBuffer sb;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
    d.Accept(writer);
    return {status_code, msg, sb.GetString()};
}

static std::string format_http_request(brpc::Controller* cntl) {
    const brpc::HttpHeader& request = cntl->http_request();
    auto& unresolved_path = request.unresolved_path();
    auto& uri = request.uri();
    std::stringstream ss;
    ss << "\nuri_path=" << uri.path();
    ss << "\nunresolved_path=" << unresolved_path;
    ss << "\nmethod=" << brpc::HttpMethod2Str(request.method());
    ss << "\nquery strings:";
    for (auto it = uri.QueryBegin(); it != uri.QueryEnd(); ++it) {
        ss << "\n" << it->first << "=" << it->second;
    }
    ss << "\nheaders:";
    for (auto it = request.HeaderBegin(); it != request.HeaderEnd(); ++it) {
        ss << "\n" << it->first << ":" << it->second;
    }
    std::string body = cntl->request_attachment().to_string();
    ss << "\nbody=" << (body.empty() ? "(empty)" : body);
    return ss.str();
}

void MetaServiceImpl::http(::google::protobuf::RpcController* controller,
                           const MetaServiceHttpRequest*, MetaServiceHttpResponse*,
                           ::google::protobuf::Closure* done) {
    auto* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard closure_guard(done);

    LOG(INFO) << "rpc from " << cntl->remote_side()
              << " request: " << cntl->http_request().uri().path();
    std::string http_request = format_http_request(cntl);
    std::string http_request_for_log = encryt_sk(http_request);
    http_request_for_log = hide_ak(http_request_for_log);
    const auto& unresolved_path = cntl->http_request().unresolved_path();
    auto api_path = split_http_api_path(unresolved_path);
    const auto& handlers = get_http_handlers();
    auto it = handlers.find(api_path.route);

    // Auth
    auto token = http_query(cntl->http_request().uri(), "token");
    if (token != config::http_token) {
        std::string body = fmt::format("incorrect token, token={}",
                                       (token.empty() ? std::string_view("(not given)") : token));
        cntl->http_response().set_status_code(403);
        cntl->response_attachment().append(body);
        cntl->response_attachment().append("\n");
        LOG(WARNING) << "failed to handle http from " << cntl->remote_side()
                     << " request: " << http_request_for_log << " msg: " << body;
        return;
    }

    const auto* handler =
            it == handlers.end() ? nullptr : resolve_http_handler(it->second, api_path.version);
    if (handler == nullptr ||
        (it->second.role != HttpRole::META_SERVICE && it->second.role != HttpRole::BOTH)) {
        std::string msg = "http path not found or not allowed";
        cntl->http_response().set_status_code(404);
        cntl->response_attachment().append(msg);
        cntl->response_attachment().append("\n");
        return;
    }

    auto [status_code, msg, body] = (*handler)(this, cntl);
    cntl->http_response().set_status_code(status_code);
    cntl->response_attachment().append(body);
    cntl->response_attachment().append("\n");

    int ret = cntl->http_response().status_code();
    LOG(INFO) << (ret == 200 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
              << cntl->remote_side() << " request=\n"
              << http_request_for_log << "\n ret=" << ret << " msg=" << msg;
}

} // namespace doris::cloud
