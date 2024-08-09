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
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <google/protobuf/util/json_util.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <optional>
#include <type_traits>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "meta_service.h"

namespace doris::cloud {

#define PARSE_MESSAGE_OR_RETURN(ctrl, req)                                                      \
    do {                                                                                        \
        std::string body = ctrl->request_attachment().to_string();                              \
        auto& unresolved_path = ctrl->http_request().unresolved_path();                         \
        auto st = parse_json_message(unresolved_path, body, &req);                              \
        if (!st.ok()) {                                                                         \
            std::string msg = "parse http request '" + unresolved_path + "': " + st.ToString(); \
            LOG_WARNING(msg).tag("body", body);                                                 \
            return http_json_reply(MetaServiceCode::PROTOBUF_PARSE_ERR, msg);                   \
        }                                                                                       \
    } while (0)

extern std::string get_instance_id(const std::shared_ptr<ResourceManager>& rc_mgr,
                                   const std::string& cloud_unique_id);

extern int decrypt_instance_info(InstanceInfoPB& instance, const std::string& instance_id,
                                 MetaServiceCode& code, std::string& msg,
                                 std::shared_ptr<Transaction>& txn);

template <typename Message>
static google::protobuf::util::Status parse_json_message(const std::string& unresolved_path,
                                                         const std::string& body, Message* req) {
    static_assert(std::is_base_of_v<google::protobuf::Message, Message>);
    auto st = google::protobuf::util::JsonStringToMessage(body, req);
    if (!st.ok()) {
        std::string msg = "failed to strictly parse http request for '" + unresolved_path +
                          "' error: " + st.ToString();
        LOG_WARNING(msg).tag("body", body);

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
        d.AddMember("msg", "", d.GetAllocator());
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

static std::string format_http_request(const brpc::HttpHeader& request) {
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
    return ss.str();
}

static std::string_view remove_version_prefix(std::string_view path) {
    if (path.size() > 3 && path.substr(0, 3) == "v1/") path.remove_prefix(3);
    return path;
}

static HttpResponse process_alter_cluster(MetaServiceImpl* service, brpc::Controller* ctrl) {
    static std::unordered_map<std::string_view, AlterClusterRequest::Operation> operations {
            {"add_cluster", AlterClusterRequest::ADD_CLUSTER},
            {"drop_cluster", AlterClusterRequest::DROP_CLUSTER},
            {"rename_cluster", AlterClusterRequest::RENAME_CLUSTER},
            {"update_cluster_endpoint", AlterClusterRequest::UPDATE_CLUSTER_ENDPOINT},
            {"update_cluster_mysql_user_name", AlterClusterRequest::UPDATE_CLUSTER_MYSQL_USER_NAME},
            {"add_node", AlterClusterRequest::ADD_NODE},
            {"drop_node", AlterClusterRequest::DROP_NODE},
            {"decommission_node", AlterClusterRequest::DECOMMISSION_NODE},
            {"set_cluster_status", AlterClusterRequest::SET_CLUSTER_STATUS},
            {"notify_decommissioned", AlterClusterRequest::NOTIFY_DECOMMISSIONED},
    };

    auto& path = ctrl->http_request().unresolved_path();
    auto body = ctrl->request_attachment().to_string();
    auto it = operations.find(remove_version_prefix(path));
    if (it == operations.end()) {
        std::string msg = "not supportted alter cluster operation: " + path;
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    AlterClusterRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);

    req.set_op(it->second);
    AlterClusterResponse resp;
    service->alter_cluster(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

static HttpResponse process_get_obj_store_info(MetaServiceImpl* service, brpc::Controller* ctrl) {
    GetObjStoreInfoRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);

    GetObjStoreInfoResponse resp;
    service->get_obj_store_info(ctrl, &req, &resp, nullptr);
    return http_json_reply_message(resp.status(), resp);
}

static HttpResponse process_alter_obj_store_info(MetaServiceImpl* service, brpc::Controller* ctrl) {
    static std::unordered_map<std::string_view, AlterObjStoreInfoRequest::Operation> operations {
            {"add_obj_info", AlterObjStoreInfoRequest::ADD_OBJ_INFO},
            {"legacy_update_ak_sk", AlterObjStoreInfoRequest::LEGACY_UPDATE_AK_SK}};

    auto& path = ctrl->http_request().unresolved_path();
    auto it = operations.find(remove_version_prefix(path));
    if (it == operations.end()) {
        std::string msg = "not supportted alter obj store info operation: " + path;
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    AlterObjStoreInfoRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    req.set_op(it->second);

    AlterObjStoreInfoResponse resp;
    service->alter_obj_store_info(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

static HttpResponse process_alter_storage_vault(MetaServiceImpl* service, brpc::Controller* ctrl) {
    static std::unordered_map<std::string_view, AlterObjStoreInfoRequest::Operation> operations {
            {"drop_s3_vault", AlterObjStoreInfoRequest::DROP_S3_VAULT},
            {"add_s3_vault", AlterObjStoreInfoRequest::ADD_S3_VAULT},
            {"drop_hdfs_vault", AlterObjStoreInfoRequest::DROP_HDFS_INFO},
            {"add_hdfs_vault", AlterObjStoreInfoRequest::ADD_HDFS_INFO}};

    auto& path = ctrl->http_request().unresolved_path();
    auto it = operations.find(remove_version_prefix(path));
    if (it == operations.end()) {
        std::string msg = "not supportted alter storage vault operation: " + path;
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    AlterObjStoreInfoRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    req.set_op(it->second);

    AlterObjStoreInfoResponse resp;
    service->alter_storage_vault(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

static HttpResponse process_update_ak_sk(MetaServiceImpl* service, brpc::Controller* ctrl) {
    UpdateAkSkRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    UpdateAkSkResponse resp;
    service->update_ak_sk(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

static HttpResponse process_create_instance(MetaServiceImpl* service, brpc::Controller* ctrl) {
    CreateInstanceRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    CreateInstanceResponse resp;
    service->create_instance(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

static HttpResponse process_alter_instance(MetaServiceImpl* service, brpc::Controller* ctrl) {
    static std::unordered_map<std::string_view, std::vector<AlterInstanceRequest::Operation>>
            operations {{"rename_instance", {AlterInstanceRequest::RENAME}},
                        {"enable_instance_sse", {AlterInstanceRequest::ENABLE_SSE}},
                        {"disable_instance_sse", {AlterInstanceRequest::DISABLE_SSE}},
                        {"drop_instance", {AlterInstanceRequest::DROP}},
                        {"set_instance_status",
                         {AlterInstanceRequest::SET_NORMAL, AlterInstanceRequest::SET_OVERDUE}}};

    auto& path = ctrl->http_request().unresolved_path();
    auto it = operations.find(remove_version_prefix(path));
    if (it == operations.end()) {
        std::string msg = "not supportted alter instance operation: '" + path +
                          "', remove version prefix=" + std::string(remove_version_prefix(path));
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    AlterInstanceRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    // for unresolved path whose corresponding operation is signal, we need set operation by ourselves.
    if ((it->second).size() == 1) {
        req.set_op((it->second)[0]);
    }
    AlterInstanceResponse resp;
    service->alter_instance(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

static HttpResponse process_abort_txn(MetaServiceImpl* service, brpc::Controller* ctrl) {
    AbortTxnRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    AbortTxnResponse resp;
    service->abort_txn(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

static HttpResponse process_abort_tablet_job(MetaServiceImpl* service, brpc::Controller* ctrl) {
    FinishTabletJobRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    req.set_action(FinishTabletJobRequest::ABORT);
    FinishTabletJobResponse resp;
    service->finish_tablet_job(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

static HttpResponse process_alter_ram_user(MetaServiceImpl* service, brpc::Controller* ctrl) {
    AlterRamUserRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    AlterRamUserResponse resp;
    service->alter_ram_user(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

static HttpResponse process_alter_iam(MetaServiceImpl* service, brpc::Controller* ctrl) {
    AlterIamRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    AlterIamResponse resp;
    service->alter_iam(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

static HttpResponse process_decode_key(MetaServiceImpl*, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    std::string_view key = http_query(uri, "key");
    if (key.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "no key to decode");
    }

    bool unicode = http_query(uri, "unicode") != "false";
    std::string body = prettify_key(key, unicode);
    if (body.empty()) {
        std::string msg = "failed to decode key, key=" + std::string(key);
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
    }
    return http_text_reply(MetaServiceCode::OK, "", body);
}

static HttpResponse process_encode_key(MetaServiceImpl*, brpc::Controller* ctrl) {
    return process_http_encode_key(ctrl->http_request().uri());
}

static HttpResponse process_get_value(MetaServiceImpl* service, brpc::Controller* ctrl) {
    return process_http_get_value(service->txn_kv().get(), ctrl->http_request().uri());
}

static HttpResponse process_get_instance_info(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    std::string_view instance_id = http_query(uri, "instance_id");
    std::string_view cloud_unique_id = http_query(uri, "cloud_unique_id");

    InstanceInfoPB instance;
    auto [code, msg] = service->get_instance_info(std::string(instance_id),
                                                  std::string(cloud_unique_id), &instance);
    return http_json_reply_message(code, msg, instance);
}

static HttpResponse process_get_cluster(MetaServiceImpl* service, brpc::Controller* ctrl) {
    GetClusterRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);

    bool get_all_cluster_info = false;
    // if cluster_id、cluster_name、mysql_user_name all empty, get this instance's all cluster info.
    if (req.cluster_id().empty() && req.cluster_name().empty() && req.mysql_user_name().empty()) {
        get_all_cluster_info = true;
    }

    GetClusterResponse resp;
    service->get_cluster(ctrl, &req, &resp, nullptr);

    if (resp.status().code() == MetaServiceCode::OK) {
        if (get_all_cluster_info) {
            return http_json_reply_message(resp.status(), resp);
        } else {
            // ATTN: only returns the first cluster pb.
            return http_json_reply_message(resp.status(), resp.cluster(0));
        }
    } else {
        return http_json_reply(resp.status());
    }
}

static HttpResponse process_get_tablet_stats(MetaServiceImpl* service, brpc::Controller* ctrl) {
    GetTabletStatsRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    GetTabletStatsResponse resp;
    service->get_tablet_stats(ctrl, &req, &resp, nullptr);

    std::string body;
    if (resp.status().code() == MetaServiceCode::OK) {
        body = resp.DebugString();
    }
    return http_text_reply(resp.status(), body);
}

static HttpResponse process_get_stage(MetaServiceImpl* service, brpc::Controller* ctrl) {
    GetStageRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    GetStageResponse resp;
    service->get_stage(ctrl, &req, &resp, nullptr);
    return http_json_reply_message(resp.status(), resp);
}

static HttpResponse process_get_cluster_status(MetaServiceImpl* service, brpc::Controller* ctrl) {
    GetClusterStatusRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    GetClusterStatusResponse resp;
    service->get_cluster_status(ctrl, &req, &resp, nullptr);
    return http_json_reply_message(resp.status(), resp);
}

static HttpResponse process_unknown(MetaServiceImpl*, brpc::Controller*) {
    // ATTN: To be compatible with cloud manager versions higher than this MS
    return http_json_reply(MetaServiceCode::OK, "");
}

void MetaServiceImpl::http(::google::protobuf::RpcController* controller,
                           const MetaServiceHttpRequest*, MetaServiceHttpResponse*,
                           ::google::protobuf::Closure* done) {
    using HttpHandler = HttpResponse (*)(MetaServiceImpl*, brpc::Controller*);
    static std::unordered_map<std::string_view, HttpHandler> http_handlers {
            // for alter cluster.
            {"add_cluster", process_alter_cluster},
            {"drop_cluster", process_alter_cluster},
            {"rename_cluster", process_alter_cluster},
            {"update_cluster_endpoint", process_alter_cluster},
            {"update_cluster_mysql_user_name", process_alter_cluster},
            {"add_node", process_alter_cluster},
            {"drop_node", process_alter_cluster},
            {"decommission_node", process_alter_cluster},
            {"set_cluster_status", process_alter_cluster},
            {"notify_decommissioned", process_alter_cluster},
            {"v1/add_cluster", process_alter_cluster},
            {"v1/drop_cluster", process_alter_cluster},
            {"v1/rename_cluster", process_alter_cluster},
            {"v1/update_cluster_endpoint", process_alter_cluster},
            {"v1/update_cluster_mysql_user_name", process_alter_cluster},
            {"v1/add_node", process_alter_cluster},
            {"v1/drop_node", process_alter_cluster},
            {"v1/decommission_node", process_alter_cluster},
            {"v1/set_cluster_status", process_alter_cluster},
            // for alter instance
            {"create_instance", process_create_instance},
            {"drop_instance", process_alter_instance},
            {"rename_instance", process_alter_instance},
            {"enable_instance_sse", process_alter_instance},
            {"disable_instance_sse", process_alter_instance},
            {"set_instance_status", process_alter_instance},
            {"v1/create_instance", process_create_instance},
            {"v1/drop_instance", process_alter_instance},
            {"v1/rename_instance", process_alter_instance},
            {"v1/enable_instance_sse", process_alter_instance},
            {"v1/disable_instance_sse", process_alter_instance},
            {"v1/set_instance_status", process_alter_instance},
            // for alter obj store info
            {"add_obj_info", process_alter_obj_store_info},
            {"legacy_update_ak_sk", process_alter_obj_store_info},
            {"update_ak_sk", process_update_ak_sk},
            {"v1/add_obj_info", process_alter_obj_store_info},
            {"v1/legacy_update_ak_sk", process_alter_obj_store_info},
            {"v1/update_ak_sk", process_update_ak_sk},
            {"show_storage_vaults", process_get_obj_store_info},
            {"add_hdfs_vault", process_alter_storage_vault},
            {"add_s3_vault", process_alter_storage_vault},
            {"alter_s3_vault", process_alter_storage_vault},
            {"drop_s3_vault", process_alter_storage_vault},
            {"drop_hdfs_vault", process_alter_storage_vault},
            // for tools
            {"decode_key", process_decode_key},
            {"encode_key", process_encode_key},
            {"get_value", process_get_value},
            {"v1/decode_key", process_decode_key},
            {"v1/encode_key", process_encode_key},
            {"v1/get_value", process_get_value},
            // for get
            {"get_instance", process_get_instance_info},
            {"get_obj_store_info", process_get_obj_store_info},
            {"get_cluster", process_get_cluster},
            {"get_tablet_stats", process_get_tablet_stats},
            {"get_stage", process_get_stage},
            {"get_cluster_status", process_get_cluster_status},
            {"v1/get_instance", process_get_instance_info},
            {"v1/get_obj_store_info", process_get_obj_store_info},
            {"v1/get_cluster", process_get_cluster},
            {"v1/get_tablet_stats", process_get_tablet_stats},
            {"v1/get_stage", process_get_stage},
            {"v1/get_cluster_status", process_get_cluster_status},
            // misc
            {"abort_txn", process_abort_txn},
            {"abort_tablet_job", process_abort_tablet_job},
            {"alter_ram_user", process_alter_ram_user},
            {"alter_iam", process_alter_iam},
            {"v1/abort_txn", process_abort_txn},
            {"v1/abort_tablet_job", process_abort_tablet_job},
            {"v1/alter_ram_user", process_alter_ram_user},
            {"v1/alter_iam", process_alter_iam},
    };

    auto cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard closure_guard(done);

    // Prepare input request info
    LOG(INFO) << "rpc from " << cntl->remote_side()
              << " request: " << cntl->http_request().uri().path();
    std::string http_request = format_http_request(cntl->http_request());

    // Auth
    auto token = http_query(cntl->http_request().uri(), "token");
    if (token != config::http_token) {
        std::string body = fmt::format("incorrect token, token={}",
                                       (token.empty() ? std::string_view("(not given)") : token));
        cntl->http_response().set_status_code(403);
        cntl->response_attachment().append(body);
        cntl->response_attachment().append("\n");
        LOG(WARNING) << "failed to handle http from " << cntl->remote_side()
                     << " request: " << http_request << " msg: " << body;
        return;
    }

    // Process http request
    auto& unresolved_path = cntl->http_request().unresolved_path();
    HttpHandler handler = process_unknown;
    auto it = http_handlers.find(unresolved_path);
    if (it != http_handlers.end()) {
        handler = it->second;
    }

    auto [status_code, msg, body] = handler(this, cntl);
    cntl->http_response().set_status_code(status_code);
    cntl->response_attachment().append(body);
    cntl->response_attachment().append("\n");

    int ret = cntl->http_response().status_code();
    LOG(INFO) << (ret == 200 ? "succ to " : "failed to ") << __PRETTY_FUNCTION__ << " "
              << cntl->remote_side() << " request=\n"
              << http_request << "\n ret=" << ret << " msg=" << msg;
}

} // namespace doris::cloud
