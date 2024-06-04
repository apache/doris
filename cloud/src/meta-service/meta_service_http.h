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

#include <brpc/uri.h>
#include <gen_cpp/cloud.pb.h>

#include <optional>
#include <string>
#include <string_view>

#include "common/util.h"

namespace doris::cloud {

struct HttpResponse {
    int status_code;
    std::string msg;
    std::string body;
};

std::tuple<int, std::string_view> convert_ms_code_to_http_code(MetaServiceCode ret);

HttpResponse http_json_reply(MetaServiceCode code, const std::string& msg,
                             std::optional<std::string> body = {});

HttpResponse process_http_get_value(TxnKv* txn_kv, const brpc::URI& uri);

HttpResponse process_http_encode_key(const brpc::URI& uri);

/// Return the query value or an empty string if not exists.
inline static std::string_view http_query(const brpc::URI& uri, const char* name) {
    return uri.GetQuery(name) ? *uri.GetQuery(name) : std::string_view();
}

inline static HttpResponse http_json_reply(const MetaServiceResponseStatus& status,
                                           std::optional<std::string> body = {}) {
    return http_json_reply(status.code(), status.msg(), body);
}

inline static HttpResponse http_json_reply_message(MetaServiceCode code, const std::string& msg,
                                                   const google::protobuf::Message& body) {
    return http_json_reply(code, msg, proto_to_json(body));
}

inline static HttpResponse http_json_reply_message(const MetaServiceResponseStatus& status,
                                                   const google::protobuf::Message& msg) {
    return http_json_reply(status, proto_to_json(msg));
}

inline static HttpResponse http_text_reply(MetaServiceCode code, const std::string& msg,
                                           const std::string& body) {
    auto [status_code, _] = convert_ms_code_to_http_code(code);
    return {status_code, msg, body};
}

inline static HttpResponse http_text_reply(const MetaServiceResponseStatus& status,
                                           const std::string& body) {
    return http_text_reply(status.code(), status.msg(), body);
}

} // namespace doris::cloud
