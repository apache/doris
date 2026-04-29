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

#include "http_helper.h"

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

#include <algorithm>
#include <cstdint>
#include <string>
#include <type_traits>

#include "common/metric.h"
#include "cpp/s3_rate_limiter.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/meta_service_http.h"
#include "recycler/recycler.h"
#include "recycler/recycler_service.h"
namespace doris::cloud {

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

#define PARSE_MESSAGE_OR_RETURN(ctrl, req)                                                      \
    do {                                                                                        \
        std::string body = ctrl->request_attachment().to_string();                              \
        auto& unresolved_path = ctrl->http_request().unresolved_path();                         \
        auto st = parse_json_message(unresolved_path, body, &req);                              \
        if (!st.ok()) {                                                                         \
            std::string msg = "parse http request '" + unresolved_path + "': " + st.ToString(); \
            LOG_WARNING(msg).tag("body", encryt_sk(hide_access_key(body)));                     \
            return http_json_reply(MetaServiceCode::PROTOBUF_PARSE_ERR, msg);                   \
        }                                                                                       \
    } while (0)

const std::unordered_map<std::string_view, HttpHandlerInfo>& get_http_handlers() {
    using MS = MetaServiceImpl;
    using RS = RecyclerServiceImpl;

    static const auto handlers = [] {
        return std::unordered_map<std::string_view, HttpHandlerInfo> {
                // MetaService APIs
                {"add_cluster",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_cluster((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"drop_cluster",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_cluster((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"rename_cluster",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_cluster((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"update_cluster_endpoint",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_cluster((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"update_cluster_mysql_user_name",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_cluster((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"add_node",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_cluster((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"drop_node",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_cluster((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"decommission_node",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_cluster((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"set_cluster_status",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_cluster((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"notify_decommissioned",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_cluster((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"alter_vcluster_info",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_cluster((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},

                {"create_instance",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_create_instance((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"drop_instance",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_instance((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"rename_instance",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_instance((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"enable_instance_sse",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_instance((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"disable_instance_sse",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_instance((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"set_instance_status",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_instance((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},

                {"add_obj_info",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_alter_obj_store_info((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"legacy_update_ak_sk",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_alter_obj_store_info((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"update_ak_sk",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_update_ak_sk((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"show_storage_vaults",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_get_obj_store_info((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"add_hdfs_vault",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_alter_storage_vault((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"add_s3_vault",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_alter_storage_vault((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"alter_s3_vault",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_alter_storage_vault((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"drop_s3_vault",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_alter_storage_vault((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"drop_hdfs_vault",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_alter_storage_vault((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"alter_obj_info",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_alter_obj_store_info((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},

                {"decode_key",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_decode_key((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"encode_key",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_encode_key((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"get_value",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_get_value((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"set_value",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_set_value((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"show_meta_ranges",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_show_meta_ranges((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"txn_lazy_commit",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_txn_lazy_commit((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"injection_point",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_injection_point((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"fix_tablet_stats",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_fix_tablet_stats((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"fix_tablet_db_id",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_fix_tablet_db_id((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},

                {"get_instance",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_get_instance_info((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"get_obj_store_info",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_get_obj_store_info((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"get_cluster",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_get_cluster((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"get_tablet_stats",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_get_tablet_stats((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"get_stage",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_get_stage((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"get_cluster_status",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_get_cluster_status((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},

                {"list_snapshot",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_list_snapshot((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"drop_snapshot",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_drop_snapshot((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"set_snapshot_property",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_set_snapshot_property((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"get_snapshot_property",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_get_snapshot_property((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"set_multi_version_status",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_set_multi_version_status((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"compact_snapshot",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_compact_snapshot((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"decouple_instance",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_decouple_instance((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"abort_txn",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_abort_txn((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"abort_tablet_job",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_abort_tablet_job((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"alter_ram_user",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_ram_user((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"alter_iam",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_alter_iam((MS*)s, c); },
                  .role = HttpRole::META_SERVICE}},
                {"adjust_rate_limit",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_adjust_rate_limit((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},
                {"list_rate_limit",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_query_rate_limit((MS*)s, c);
                          },
                  .role = HttpRole::META_SERVICE}},

                // Recycler APIs
                {"recycle_instance",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_recycle_instance((RS*)s, c);
                          },
                  .role = HttpRole::RECYCLER}},
                {"statistics_recycle",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_statistics_recycle((RS*)s, c);
                          },
                  .role = HttpRole::RECYCLER}},
                {"recycle_copy_jobs",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_recycle_copy_jobs((RS*)s, c);
                          },
                  .role = HttpRole::RECYCLER}},
                {"recycle_job_info",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_recycle_job_info((RS*)s, c);
                          },
                  .role = HttpRole::RECYCLER}},
                {"check_instance",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_check_instance((RS*)s, c); },
                  .role = HttpRole::RECYCLER}},
                {"check_job_info",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_check_job_info((RS*)s, c); },
                  .role = HttpRole::RECYCLER}},
                {"check_meta",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_check_meta((RS*)s, c); },
                  .role = HttpRole::RECYCLER}},
                {"adjust_rate_limiter",
                 {.handler =
                          [](void* s, brpc::Controller* c) {
                              return process_adjust_rate_limiter((RS*)s, c);
                          },
                  .role = HttpRole::RECYCLER}},

                // Shared APIs
                {"show_config",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_show_config((MS*)s, c); },
                  .role = HttpRole::BOTH}},
                {"update_config",
                 {.handler = [](void* s,
                                brpc::Controller* c) { return process_update_config((MS*)s, c); },
                  .role = HttpRole::BOTH}},
        };
    }();

    return handlers;
}

HttpResponse process_alter_cluster(MetaServiceImpl* service, brpc::Controller* ctrl) {
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
            {"alter_vcluster_info", AlterClusterRequest::ALTER_VCLUSTER_INFO},
    };

    auto& path = ctrl->http_request().unresolved_path();
    auto body = ctrl->request_attachment().to_string();
    auto it = operations.find(http_api_route(path));
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

HttpResponse process_get_obj_store_info(MetaServiceImpl* service, brpc::Controller* ctrl) {
    GetObjStoreInfoRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);

    GetObjStoreInfoResponse resp;
    service->get_obj_store_info(ctrl, &req, &resp, nullptr);
    return http_json_reply_message(resp.status(), resp);
}

HttpResponse process_alter_obj_store_info(MetaServiceImpl* service, brpc::Controller* ctrl) {
    static std::unordered_map<std::string_view, AlterObjStoreInfoRequest::Operation> operations {
            {"add_obj_info", AlterObjStoreInfoRequest::ADD_OBJ_INFO},
            {"legacy_update_ak_sk", AlterObjStoreInfoRequest::LEGACY_UPDATE_AK_SK},
            {"alter_obj_info", AlterObjStoreInfoRequest::ALTER_OBJ_INFO}};

    auto& path = ctrl->http_request().unresolved_path();
    auto it = operations.find(http_api_route(path));
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

HttpResponse process_alter_storage_vault(MetaServiceImpl* service, brpc::Controller* ctrl) {
    static std::unordered_map<std::string_view, AlterObjStoreInfoRequest::Operation> operations {
            {"drop_s3_vault", AlterObjStoreInfoRequest::DROP_S3_VAULT},
            {"add_s3_vault", AlterObjStoreInfoRequest::ADD_S3_VAULT},
            {"alter_s3_vault", AlterObjStoreInfoRequest::ALTER_S3_VAULT},
            {"drop_hdfs_vault", AlterObjStoreInfoRequest::DROP_HDFS_INFO},
            {"add_hdfs_vault", AlterObjStoreInfoRequest::ADD_HDFS_INFO}};

    auto& path = ctrl->http_request().unresolved_path();
    auto it = operations.find(http_api_route(path));
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

HttpResponse process_update_ak_sk(MetaServiceImpl* service, brpc::Controller* ctrl) {
    UpdateAkSkRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    UpdateAkSkResponse resp;
    service->update_ak_sk(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

HttpResponse process_create_instance(MetaServiceImpl* service, brpc::Controller* ctrl) {
    CreateInstanceRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    CreateInstanceResponse resp;
    service->create_instance(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

HttpResponse process_alter_instance(MetaServiceImpl* service, brpc::Controller* ctrl) {
    static std::unordered_map<std::string_view, std::vector<AlterInstanceRequest::Operation>>
            operations {{"rename_instance", {AlterInstanceRequest::RENAME}},
                        {"enable_instance_sse", {AlterInstanceRequest::ENABLE_SSE}},
                        {"disable_instance_sse", {AlterInstanceRequest::DISABLE_SSE}},
                        {"drop_instance", {AlterInstanceRequest::DROP}},
                        {"set_instance_status",
                         {AlterInstanceRequest::SET_NORMAL, AlterInstanceRequest::SET_OVERDUE}}};

    auto& path = ctrl->http_request().unresolved_path();
    auto it = operations.find(http_api_route(path));
    if (it == operations.end()) {
        std::string msg = "not supportted alter instance operation: '" + path +
                          "', route=" + std::string(http_api_route(path));
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

HttpResponse process_abort_txn(MetaServiceImpl* service, brpc::Controller* ctrl) {
    AbortTxnRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    AbortTxnResponse resp;
    service->abort_txn(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

HttpResponse process_abort_tablet_job(MetaServiceImpl* service, brpc::Controller* ctrl) {
    FinishTabletJobRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    req.set_action(FinishTabletJobRequest::ABORT);
    FinishTabletJobResponse resp;
    service->finish_tablet_job(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

HttpResponse process_alter_ram_user(MetaServiceImpl* service, brpc::Controller* ctrl) {
    AlterRamUserRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    AlterRamUserResponse resp;
    service->alter_ram_user(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

HttpResponse process_alter_iam(MetaServiceImpl* service, brpc::Controller* ctrl) {
    AlterIamRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    AlterIamResponse resp;
    service->alter_iam(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

HttpResponse process_adjust_rate_limit(MetaServiceImpl* service, brpc::Controller* cntl) {
    const auto& uri = cntl->http_request().uri();
    auto qps_limit_str = std::string {http_query(uri, "qps_limit")};
    auto rpc_name = std::string {http_query(uri, "rpc_name")};
    auto instance_id = std::string {http_query(uri, "instance_id")};

    auto process_set_qps_limit = [&](std::function<bool(int64_t)> cb) -> HttpResponse {
        DCHECK(!qps_limit_str.empty());
        int64_t qps_limit = -1;
        try {
            qps_limit = std::stoll(qps_limit_str);
        } catch (const std::exception& ex) {
            return http_json_reply(
                    MetaServiceCode::INVALID_ARGUMENT,
                    fmt::format("param `qps_limit` is not a legal int64 type:{}", ex.what()));
        }
        if (qps_limit < 0) {
            return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                                   "`qps_limit` should not be less than 0");
        }
        if (cb(qps_limit)) {
            return http_json_reply(MetaServiceCode::OK, "sucess to adjust rate limit");
        }
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               fmt::format("failed to adjust rate limit for qps_limit={}, "
                                           "rpc_name={}, instance_id={}, plz ensure correct "
                                           "rpc/instance name",
                                           qps_limit_str, rpc_name, instance_id));
    };

    auto set_global_qps_limit = [process_set_qps_limit, service]() {
        return process_set_qps_limit([service](int64_t qps_limit) {
            return service->rate_limiter()->set_rate_limit(qps_limit);
        });
    };

    auto set_rpc_qps_limit = [&]() {
        return process_set_qps_limit([&](int64_t qps_limit) {
            return service->rate_limiter()->set_rate_limit(qps_limit, rpc_name);
        });
    };

    auto set_instance_qps_limit = [&]() {
        return process_set_qps_limit([&](int64_t qps_limit) {
            return service->rate_limiter()->set_instance_rate_limit(qps_limit, instance_id);
        });
    };

    auto set_instance_rpc_qps_limit = [&]() {
        return process_set_qps_limit([&](int64_t qps_limit) {
            return service->rate_limiter()->set_rate_limit(qps_limit, rpc_name, instance_id);
        });
    };

    auto process_invalid_arguments = [&]() -> HttpResponse {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               fmt::format("invalid argument: qps_limit(required)={}, "
                                           "rpc_name(optional)={}, instance_id(optional)={}",
                                           qps_limit_str, rpc_name, instance_id));
    };

    // We have 3 optional params and 2^3 combination, and 4 of them are illegal.
    // We register callbacks for them in porcessors accordings to the level, represented by 3 bits.
    std::array<std::function<HttpResponse()>, 8> processors;
    std::fill_n(processors.begin(), 8, std::move(process_invalid_arguments));
    processors[0b001] = std::move(set_global_qps_limit);
    processors[0b011] = std::move(set_rpc_qps_limit);
    processors[0b101] = std::move(set_instance_qps_limit);
    processors[0b111] = std::move(set_instance_rpc_qps_limit);

    uint8_t level = (0x01 & !qps_limit_str.empty()) | ((0x01 & !rpc_name.empty()) << 1) |
                    ((0x01 & !instance_id.empty()) << 2);

    DCHECK_LT(level, 8);

    return processors[level]();
}

HttpResponse process_query_rate_limit(MetaServiceImpl* service, brpc::Controller* cntl) {
    auto rate_limiter = service->rate_limiter();
    rapidjson::Document d;
    d.SetObject();
    auto get_qps_limit = [&d](std::string_view rpc_name,
                              std::shared_ptr<RpcRateLimiter> rpc_limiter) {
        rapidjson::Document node;
        node.SetObject();
        rapidjson::Document sub;
        sub.SetObject();
        auto get_qps_token_limit = [&](std::string_view instance_id,
                                       std::shared_ptr<RpcRateLimiter::QpsToken> qps_token) {
            sub.AddMember(rapidjson::StringRef(instance_id.data(), instance_id.size()),
                          qps_token->max_qps_limit(), d.GetAllocator());
        };
        rpc_limiter->for_each_qps_token(std::move(get_qps_token_limit));

        node.AddMember("RPC qps limit", rpc_limiter->max_qps_limit(), d.GetAllocator());
        node.AddMember("instance specific qps limit", sub, d.GetAllocator());
        d.AddMember(rapidjson::StringRef(rpc_name.data(), rpc_name.size()), node, d.GetAllocator());
    };
    rate_limiter->for_each_rpc_limiter(std::move(get_qps_limit));

    rapidjson::StringBuffer sb;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
    d.Accept(writer);
    return http_json_reply(MetaServiceCode::OK, "", sb.GetString());
}

// Recycler HTTP handlers
HttpResponse process_recycle_instance(RecyclerServiceImpl* service, brpc::Controller* cntl) {
    std::string request_body = cntl->request_attachment().to_string();
    RecycleInstanceRequest req;
    auto st = google::protobuf::util::JsonStringToMessage(request_body, &req);
    if (!st.ok()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               "failed to parse RecycleInstanceRequest");
    }
    RecycleInstanceResponse res;
    service->recycle_instance(cntl, &req, &res, nullptr);
    return http_text_reply(res.status(), res.status().msg());
}

HttpResponse process_statistics_recycle(RecyclerServiceImpl* service, brpc::Controller* cntl) {
    std::string request_body = cntl->request_attachment().to_string();
    StatisticsRecycleRequest req;
    auto st = google::protobuf::util::JsonStringToMessage(request_body, &req);
    if (!st.ok()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               "failed to parse StatisticsRecycleRequest");
    }
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;
    service->statistics_recycle(req, code, msg);
    return http_text_reply(code, msg, msg);
}

HttpResponse process_recycle_copy_jobs(RecyclerServiceImpl* service, brpc::Controller* cntl) {
    const auto* instance_id = cntl->http_request().uri().GetQuery("instance_id");
    if (!instance_id || instance_id->empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "no instance id");
    }
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;
    recycle_copy_jobs(service->txn_kv(), *instance_id, code, msg,
                      service->recycler()->thread_pool_group(), service->txn_lazy_committer());
    return http_text_reply(code, msg, msg);
}

HttpResponse process_recycle_job_info(RecyclerServiceImpl* service, brpc::Controller* cntl) {
    const auto* instance_id = cntl->http_request().uri().GetQuery("instance_id");
    if (!instance_id || instance_id->empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "no instance id");
    }
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg, key;
    job_recycle_key({*instance_id}, &key);
    recycle_job_info(service->txn_kv(), *instance_id, key, code, msg);
    return http_text_reply(code, msg, msg);
}

HttpResponse process_check_instance(RecyclerServiceImpl* service, brpc::Controller* cntl) {
    const auto* instance_id = cntl->http_request().uri().GetQuery("instance_id");
    if (!instance_id || instance_id->empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "no instance id");
    }
    if (!service->checker()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "checker not enabled");
    }
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg;
    service->check_instance(*instance_id, code, msg);
    return http_text_reply(code, msg, msg);
}

HttpResponse process_check_job_info(RecyclerServiceImpl* service, brpc::Controller* cntl) {
    const auto* instance_id = cntl->http_request().uri().GetQuery("instance_id");
    if (!instance_id || instance_id->empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "no instance id");
    }
    MetaServiceCode code = MetaServiceCode::OK;
    std::string msg, key;
    job_check_key({*instance_id}, &key);
    recycle_job_info(service->txn_kv(), *instance_id, key, code, msg);
    return http_text_reply(code, msg, msg);
}

HttpResponse process_check_meta(RecyclerServiceImpl* service, brpc::Controller* cntl) {
    const auto& uri = cntl->http_request().uri();
    const auto* instance_id = uri.GetQuery("instance_id");
    const auto* host = uri.GetQuery("host");
    const auto* port = uri.GetQuery("port");
    const auto* user = uri.GetQuery("user");
    const auto* password = uri.GetQuery("password");
    if (!instance_id || instance_id->empty() || !host || host->empty() || !port || port->empty() ||
        !password || !user || user->empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "missing required parameters");
    }
    std::string msg;
    check_meta(service->txn_kv(), *instance_id, *host, *port, *user, *password, msg);
    return http_text_reply(MetaServiceCode::OK, msg, msg);
}

HttpResponse process_adjust_rate_limiter(RecyclerServiceImpl*, brpc::Controller* cntl) {
    const auto& uri = cntl->http_request().uri();
    const auto* type_string = uri.GetQuery("type");
    const auto* speed = uri.GetQuery("speed");
    const auto* burst = uri.GetQuery("burst");
    const auto* limit = uri.GetQuery("limit");
    if (!type_string || type_string->empty() || !speed || !burst || !limit ||
        (*type_string != "get" && *type_string != "put")) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "invalid arguments");
    }
    auto max_speed = speed->empty() ? 0 : std::stoul(*speed);
    auto max_burst = burst->empty() ? 0 : std::stoul(*burst);
    auto max_limit = limit->empty() ? 0 : std::stoul(*limit);
    if (reset_s3_rate_limiter(string_to_s3_rate_limit_type(*type_string), max_speed, max_burst,
                              max_limit) != 0) {
        return http_json_reply(MetaServiceCode::UNDEFINED_ERR, "adjust failed");
    }
    return http_json_reply(MetaServiceCode::OK, "");
}

HttpResponse process_show_config(MetaServiceImpl*, brpc::Controller* cntl) {
    auto& uri = cntl->http_request().uri();
    std::string_view conf_name = http_query(uri, "conf_key");

    if (config::full_conf_map == nullptr) {
        return http_json_reply(MetaServiceCode::UNDEFINED_ERR, "config map not initialized");
    }

    std::string result = config::show_config(std::string(conf_name));
    return http_json_reply(MetaServiceCode::OK, "", result);
}

HttpResponse process_update_config(MetaServiceImpl* service, brpc::Controller* cntl) {
    const auto& uri = cntl->http_request().uri();
    bool persist = (http_query(uri, "persist") == "true");
    auto configs = std::string {http_query(uri, "configs")};
    auto reason = std::string {http_query(uri, "reason")};
    LOG(INFO) << "modify configs for reason=" << reason << ", configs=" << configs
              << ", persist=" << http_query(uri, "persist");

    if (auto [succ, cause] = config::update_config(configs, persist, config::custom_conf_path);
        !succ) {
        LOG(WARNING) << cause;
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, cause);
    }
    return http_json_reply(MetaServiceCode::OK, "");
}

HttpResponse process_decode_key(MetaServiceImpl*, brpc::Controller* ctrl) {
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

HttpResponse process_encode_key(MetaServiceImpl*, brpc::Controller* ctrl) {
    return process_http_encode_key(ctrl->http_request().uri());
}

HttpResponse process_get_value(MetaServiceImpl* service, brpc::Controller* ctrl) {
    return process_http_get_value(service->txn_kv().get(), ctrl->http_request().uri());
}

HttpResponse process_set_value(MetaServiceImpl* service, brpc::Controller* ctrl) {
    return process_http_set_value(service->txn_kv().get(), ctrl);
}

// show all key ranges and their count.
HttpResponse process_show_meta_ranges(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto txn_kv = std::dynamic_pointer_cast<FdbTxnKv>(service->txn_kv());
    if (!txn_kv) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               "this method only support fdb txn kv");
    }

    std::vector<std::string> partition_boundaries;
    TxnErrorCode code = txn_kv->get_partition_boundaries(&partition_boundaries);
    if (code != TxnErrorCode::TXN_OK) {
        auto msg = fmt::format("failed to get boundaries, code={}", code);
        return http_json_reply(MetaServiceCode::UNDEFINED_ERR, msg);
    }
    std::unordered_map<std::string, size_t> partition_count;
    get_kv_range_boundaries_count(partition_boundaries, partition_count);

    // sort ranges by count
    std::vector<std::pair<std::string, size_t>> meta_ranges;
    meta_ranges.reserve(partition_count.size());
    for (auto&& [key, count] : partition_count) {
        meta_ranges.emplace_back(key, count);
    }

    std::sort(meta_ranges.begin(), meta_ranges.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.second > rhs.second; });

    std::string body = fmt::format("total meta ranges: {}\n", partition_boundaries.size());
    for (auto&& [key, count] : meta_ranges) {
        body += fmt::format("{}: {}\n", key, count);
    }
    return http_text_reply(MetaServiceCode::OK, "", body);
}

HttpResponse process_get_instance_info(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    std::string_view instance_id = http_query(uri, "instance_id");
    std::string_view cloud_unique_id = http_query(uri, "cloud_unique_id");

    InstanceInfoPB instance;
    auto [code, msg] = service->get_instance_info(std::string(instance_id),
                                                  std::string(cloud_unique_id), &instance);
    return http_json_reply_message(code, msg, instance);
}

HttpResponse process_get_cluster(MetaServiceImpl* service, brpc::Controller* ctrl) {
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

HttpResponse process_get_tablet_stats(MetaServiceImpl* service, brpc::Controller* ctrl) {
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

HttpResponse process_fix_tablet_stats(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    std::string_view cloud_unique_id = http_query(uri, "cloud_unique_id");
    std::string_view table_id = http_query(uri, "table_id");
    std::string_view tablet_id = http_query(uri, "tablet_id");

    MetaServiceResponseStatus st = service->fix_tablet_stats(
            std::string(cloud_unique_id), std::string(table_id), std::string(tablet_id));
    return http_text_reply(st, st.DebugString());
}

HttpResponse process_fix_tablet_db_id(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    std::string instance_id(http_query(uri, "instance_id"));
    std::string tablet_id_str(http_query(uri, "tablet_id"));
    std::string db_id_str(http_query(uri, "db_id"));

    int64_t tablet_id = 0, db_id = 0;
    try {
        db_id = std::stol(db_id_str);
    } catch (const std::exception& e) {
        auto msg = fmt::format("db_id {} must be a number, meet error={}", db_id_str, e.what());
        LOG(WARNING) << msg;
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    try {
        tablet_id = std::stol(tablet_id_str);
    } catch (const std::exception& e) {
        auto msg = fmt::format("tablet_id {} must be a number, meet error={}", tablet_id_str,
                               e.what());
        LOG(WARNING) << msg;
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    auto [code, msg] = service->fix_tablet_db_id(instance_id, tablet_id, db_id);
    return http_text_reply(code, msg, "");
}

HttpResponse process_get_stage(MetaServiceImpl* service, brpc::Controller* ctrl) {
    GetStageRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    GetStageResponse resp;
    service->get_stage(ctrl, &req, &resp, nullptr);
    return http_json_reply_message(resp.status(), resp);
}

HttpResponse process_get_cluster_status(MetaServiceImpl* service, brpc::Controller* ctrl) {
    GetClusterStatusRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    GetClusterStatusResponse resp;
    service->get_cluster_status(ctrl, &req, &resp, nullptr);
    return http_json_reply_message(resp.status(), resp);
}

HttpResponse process_txn_lazy_commit(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    std::string instance_id(http_query(uri, "instance_id"));
    std::string txn_id_str(http_query(uri, "txn_id"));
    if (instance_id.empty() || txn_id_str.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "instance_id or txn_id is empty");
    }

    int64_t txn_id = 0;
    try {
        txn_id = std::stol(txn_id_str);
    } catch (const std::exception& e) {
        auto msg = fmt::format("txn_id {} must be a number, meet error={}", txn_id_str, e.what());
        LOG(WARNING) << msg;
        return http_json_reply(MetaServiceCode::UNDEFINED_ERR, msg);
    }

    DCHECK_GT(txn_id, 0);

    auto txn_lazy_committer = service->txn_lazy_committer();
    if (!txn_lazy_committer) {
        return http_json_reply(MetaServiceCode::UNDEFINED_ERR, "txn lazy committer is nullptr");
    }

    std::shared_ptr<TxnLazyCommitTask> task = txn_lazy_committer->submit(instance_id, txn_id);
    auto [code, msg] = task->wait();
    return http_json_reply(code, msg);
}

HttpResponse process_list_snapshot(MetaServiceImpl* service, brpc::Controller* ctrl) {
    ListSnapshotRequest req;
    auto& uri = ctrl->http_request().uri();
    std::string instance_id(http_query(uri, "instance_id"));
    if (instance_id.empty()) {
        PARSE_MESSAGE_OR_RETURN(ctrl, req);
    } else {
        req.set_instance_id(instance_id);
    }

    ListSnapshotResponse resp;
    service->list_snapshot(ctrl, &req, &resp, nullptr);
    return http_json_reply_message(resp.status(), resp);
}

HttpResponse process_drop_snapshot(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    std::string instance_id(http_query(uri, "instance_id"));
    std::string snapshot_id(http_query(uri, "snapshot_id"));
    if (instance_id.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "instance_id is empty");
    }
    if (snapshot_id.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "snapshot_id is empty");
    }
    DropSnapshotRequest req;
    req.set_snapshot_id(snapshot_id);
    DropSnapshotResponse resp;
    service->snapshot_manager()->drop_snapshot(instance_id, req, &resp);
    return http_json_reply(resp.status());
}

HttpResponse process_compact_snapshot(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    std::string instance_id(http_query(uri, "instance_id"));
    if (instance_id.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "instance_id is empty");
    }
    CompactSnapshotRequest req;
    req.set_instance_id(instance_id);
    CompactSnapshotResponse resp;
    service->compact_snapshot(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

HttpResponse process_decouple_instance(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    std::string instance_id(http_query(uri, "instance_id"));
    if (instance_id.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "instance_id is empty");
    }
    auto [code, msg] = service->snapshot_manager()->decouple_instance(instance_id);
    return http_json_reply(code, msg);
}

HttpResponse process_set_snapshot_property(MetaServiceImpl* service, brpc::Controller* ctrl) {
    AlterInstanceRequest req;
    PARSE_MESSAGE_OR_RETURN(ctrl, req);
    auto* properties = req.mutable_properties();
    if (properties->contains("status")) {
        std::string status = properties->at("status");
        if (status != "ENABLED" && status != "DISABLED") {
            return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                                   "Invalid value for status property: " + status +
                                           ", expected 'ENABLED' or 'DISABLED' (case insensitive)");
        }
        std::string_view is_enable = (status == "ENABLED") ? "true" : "false";
        const std::string& property_name =
                AlterInstanceRequest::SnapshotProperty_Name(AlterInstanceRequest::ENABLE_SNAPSHOT);
        (*properties)[property_name] = is_enable;
        properties->erase("status");
    }
    if (properties->contains("max_reserved_snapshots")) {
        const std::string& property_name = AlterInstanceRequest::SnapshotProperty_Name(
                AlterInstanceRequest::MAX_RESERVED_SNAPSHOTS);
        (*properties)[property_name] = properties->at("max_reserved_snapshots");
        properties->erase("max_reserved_snapshots");
    }
    if (properties->contains("snapshot_interval_seconds")) {
        const std::string& property_name = AlterInstanceRequest::SnapshotProperty_Name(
                AlterInstanceRequest::SNAPSHOT_INTERVAL_SECONDS);
        (*properties)[property_name] = properties->at("snapshot_interval_seconds");
        properties->erase("snapshot_interval_seconds");
    }
    req.set_op(AlterInstanceRequest::SET_SNAPSHOT_PROPERTY);
    AlterInstanceResponse resp;
    service->alter_instance(ctrl, &req, &resp, nullptr);
    return http_json_reply(resp.status());
}

HttpResponse process_set_multi_version_status(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    std::string instance_id(http_query(uri, "instance_id"));
    std::string cloud_unique_id(http_query(uri, "cloud_unique_id"));
    std::string multi_version_status_str(http_query(uri, "multi_version_status"));

    // Prefer instance_id if provided, fallback to cloud_unique_id
    if (instance_id.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "empty instance id");
    }

    if (multi_version_status_str.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               "multi_version_status is required");
    }

    // Parse multi_version_status from string to enum
    MultiVersionStatus multi_version_status;
    std::string multi_version_status_upper = multi_version_status_str;
    std::ranges::transform(multi_version_status_upper, multi_version_status_upper.begin(),
                           ::toupper);

    if (multi_version_status_upper == "MULTI_VERSION_DISABLED") {
        multi_version_status = MultiVersionStatus::MULTI_VERSION_DISABLED;
    } else if (multi_version_status_upper == "MULTI_VERSION_WRITE_ONLY") {
        multi_version_status = MultiVersionStatus::MULTI_VERSION_WRITE_ONLY;
    } else if (multi_version_status_upper == "MULTI_VERSION_READ_WRITE") {
        multi_version_status = MultiVersionStatus::MULTI_VERSION_READ_WRITE;
    } else if (multi_version_status_upper == "MULTI_VERSION_ENABLED") {
        multi_version_status = MultiVersionStatus::MULTI_VERSION_ENABLED;
    } else {
        return http_json_reply(
                MetaServiceCode::INVALID_ARGUMENT,
                "invalid multi_version_status value. Supported values: MULTI_VERSION_DISABLED, "
                "MULTI_VERSION_WRITE_ONLY, MULTI_VERSION_READ_WRITE, MULTI_VERSION_ENABLED");
    }
    // Call snapshot manager directly
    auto [code, msg] = service->snapshot_manager()->set_multi_version_status(instance_id,
                                                                             multi_version_status);

    return http_json_reply(code, msg);
}

HttpResponse process_get_snapshot_property(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    std::string_view instance_id = http_query(uri, "instance_id");
    std::string_view cloud_unique_id = http_query(uri, "cloud_unique_id");

    if (instance_id.empty() && cloud_unique_id.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               "empty instance_id and cloud_unique_id");
    }

    InstanceInfoPB instance;
    auto [code, msg] = service->get_instance_info(std::string(instance_id),
                                                  std::string(cloud_unique_id), &instance);
    if (code != MetaServiceCode::OK) {
        return http_json_reply(code, msg);
    }

    // Build snapshot properties response
    rapidjson::Document doc;
    doc.SetObject();

    // Snapshot switch status
    std::string_view switch_status;
    switch (instance.snapshot_switch_status()) {
    case SNAPSHOT_SWITCH_DISABLED:
        switch_status = "UNSUPPORTED";
        break;
    case SNAPSHOT_SWITCH_OFF:
        switch_status = "DISABLED";
        break;
    case SNAPSHOT_SWITCH_ON:
        switch_status = "ENABLED";
        break;
    default:
        switch_status = "UNKNOWN";
        break;
    }
    doc.AddMember("status", rapidjson::StringRef(switch_status.data(), switch_status.size()),
                  doc.GetAllocator());

    // Max reserved snapshots
    if (instance.has_max_reserved_snapshot()) {
        doc.AddMember("max_reserved_snapshots", instance.max_reserved_snapshot(),
                      doc.GetAllocator());
    }

    // Snapshot interval seconds
    if (instance.has_snapshot_interval_seconds()) {
        doc.AddMember("snapshot_interval_seconds", instance.snapshot_interval_seconds(),
                      doc.GetAllocator());
    }

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);

    return http_json_reply(MetaServiceCode::OK, "", buffer.GetString());
}

} // namespace doris::cloud
