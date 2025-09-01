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

#include "meta-service/meta_service_http.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/callback.h>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

#include "common/config.h"
#include "common/configbase.h"
#include "common/defer.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "mock_resource_manager.h"
#include "resource-manager/resource_manager.h"

namespace doris::cloud {

extern std::unique_ptr<MetaServiceProxy> get_meta_service(bool mock_resource_mgr);

template <typename Request, typename Response>
using MetaServiceMethod = void (MetaService::*)(google::protobuf::RpcController*, const Request*,
                                                Response*, google::protobuf::Closure*);

template <typename Result>
struct JsonTemplate {
    MetaServiceResponseStatus status;
    std::optional<Result> result;

    static JsonTemplate parse(const std::string& json) {
        static_assert(std::is_base_of_v<::google::protobuf::Message, Result>);

        MetaServiceResponseStatus status;
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;
        auto ss = google::protobuf::util::JsonStringToMessage(json, &status, options);
        EXPECT_TRUE(ss.ok()) << "JSON Parse result: " << ss.ToString() << ", body: " << json;

        rapidjson::Document d;
        rapidjson::ParseResult ps = d.Parse(json.c_str());
        EXPECT_TRUE(ps) << __PRETTY_FUNCTION__
                        << " parse failed: " << rapidjson::GetParseError_En(ps.Code())
                        << ", body: " << json;

        if (!ps.IsError() && d.HasMember("result")) {
            rapidjson::StringBuffer sb;
            rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
            d["result"].Accept(writer);
            std::string content = sb.GetString();
            Result result;
            auto s = google::protobuf::util::JsonStringToMessage(content, &result);
            EXPECT_TRUE(s.ok()) << "JSON Parse result: " << s.ToString()
                                << ", content: " << content;
            return {std::move(status), std::move(result)};
        }
        return {std::move(status), {}};
    }
};

class HttpContext {
public:
    HttpContext(bool mock_resource_mgr = false)
            : meta_service_(get_meta_service(mock_resource_mgr)) {
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
            auto* ret = try_any_cast<int*>(args[0]);
            *ret = 0;
            auto* key = try_any_cast<std::string*>(args[1]);
            *key = "test";
            auto* key_id = try_any_cast<int64_t*>(args[2]);
            *key_id = 1;
        });
        sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
            auto* key = try_any_cast<std::string*>(args[0]);
            *key = "test";
            auto* ret = try_any_cast<int*>(args[1]);
            *ret = 0;
        });
        sp->enable_processing();

        brpc::ServerOptions options;
        server.AddService(meta_service_.get(), brpc::ServiceOwnership::SERVER_DOESNT_OWN_SERVICE);
        if (server.Start("0.0.0.0:0", &options) == -1) {
            perror("Start brpc server");
        }
    }

    ~HttpContext() {
        server.Stop(0);
        server.Join();

        auto sp = SyncPoint::get_instance();
        sp->clear_all_call_backs();
        sp->clear_trace();
        sp->disable_processing();
    }

    template <typename Response>
    std::tuple<int, Response> query(std::string_view resource, std::string_view params,
                                    std::optional<std::string_view> body = {}) {
        butil::EndPoint endpoint = server.listen_address();

        brpc::Channel channel;
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_HTTP;
        EXPECT_EQ(channel.Init(endpoint, &options), 0) << "Fail to initialize channel";

        brpc::Controller ctrl;
        if (params.find("token=") != std::string_view::npos) {
            ctrl.http_request().uri() = fmt::format("0.0.0.0:{}/MetaService/http/{}?{}",
                                                    endpoint.port, resource, params);
        } else {
            ctrl.http_request().uri() =
                    fmt::format("0.0.0.0:{}/MetaService/http/{}?token={}&{}", endpoint.port,
                                resource, config::http_token, params);
        }
        if (body.has_value()) {
            ctrl.http_request().set_method(brpc::HTTP_METHOD_POST);
            ctrl.request_attachment().append(body->data(), body->size());
        }
        channel.CallMethod(nullptr, &ctrl, nullptr, nullptr, nullptr);
        int status_code = ctrl.http_response().status_code();

        std::string response_body = ctrl.response_attachment().to_string();
        if constexpr (std::is_base_of_v<::google::protobuf::Message, Response>) {
            Response resp;
            auto s = google::protobuf::util::JsonStringToMessage(response_body, &resp);
            static_assert(std::is_base_of_v<::google::protobuf::Message, Response>);
            EXPECT_TRUE(s.ok()) << __PRETTY_FUNCTION__ << " Parse JSON: " << s.ToString();
            return {status_code, std::move(resp)};
        } else if constexpr (std::is_same_v<std::string, Response>) {
            return {status_code, std::move(response_body)};
        } else {
            return {status_code, {}};
        }
    }

    template <typename Response>
    std::tuple<int, JsonTemplate<Response>> query_with_result(std::string_view resource,
                                                              std::string_view param) {
        auto [status_code, body] = query<std::string>(resource, param);
        LOG_INFO(__PRETTY_FUNCTION__).tag("body", body);
        return {status_code, JsonTemplate<Response>::parse(body)};
    }

    template <typename Response, typename Request>
    std::tuple<int, Response> forward(std::string_view query, const Request& req) {
        static_assert(std::is_base_of_v<::google::protobuf::Message, Request>);

        butil::EndPoint endpoint = server.listen_address();

        brpc::Channel channel;
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_HTTP;
        EXPECT_EQ(channel.Init(endpoint, &options), 0) << "Fail to initialize channel";

        brpc::Controller ctrl;
        ctrl.http_request().set_method(brpc::HTTP_METHOD_POST);
        ctrl.http_request().uri() = fmt::format(
                "0.0.0.0:{}/MetaService/http/{}{}token={}", endpoint.port, query,
                (query.find('?') != std::string_view::npos) ? "&" : "?", config::http_token);
        ctrl.request_attachment().append(proto_to_json(req));
        LOG_INFO("request attachment").tag("msg", ctrl.request_attachment().to_string());
        channel.CallMethod(nullptr, &ctrl, nullptr, nullptr, nullptr);
        int status_code = ctrl.http_response().status_code();

        std::string response_body = ctrl.response_attachment().to_string();
        if constexpr (std::is_base_of_v<::google::protobuf::Message, Response>) {
            Response resp;
            auto s = google::protobuf::util::JsonStringToMessage(response_body, &resp);
            EXPECT_TRUE(s.ok()) << __PRETTY_FUNCTION__ << " Parse JSON: " << s.ToString()
                                << ", body: " << response_body << ", query: " << query;
            return {status_code, std::move(resp)};
        } else if (std::is_same_v<std::string, Response>) {
            return {status_code, std::move(response_body)};
        } else {
            return {status_code, {}};
        }
    }
    template <typename Response, typename Request>
    std::tuple<int, JsonTemplate<Response>> forward_with_result(std::string_view query,
                                                                const Request& req) {
        auto [status_code, body] = forward<std::string>(query, req);
        LOG_INFO(__PRETTY_FUNCTION__).tag("body", body);
        return {status_code, JsonTemplate<Response>::parse(body)};
    }

    InstanceInfoPB get_instance_info(std::string_view instance_id) {
        InstanceKeyInfo key_info {instance_id};
        std::string key;
        std::string val;
        instance_key(key_info, &key);
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(meta_service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        EXPECT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance;
        instance.ParseFromString(val);
        return instance;
    }

private:
    std::unique_ptr<MetaServiceProxy> meta_service_;
    brpc::Server server;
};

static std::string next_rowset_id() {
    static int cnt = 0;
    return std::to_string(++cnt);
}

static void add_tablet(CreateTabletsRequest& req, int64_t table_id, int64_t index_id,
                       int64_t partition_id, int64_t tablet_id) {
    auto tablet = req.add_tablet_metas();
    tablet->set_table_id(table_id);
    tablet->set_index_id(index_id);
    tablet->set_partition_id(partition_id);
    tablet->set_tablet_id(tablet_id);
    auto schema = tablet->mutable_schema();
    schema->set_schema_version(0);
    auto first_rowset = tablet->add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(next_rowset_id());
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
}

static void create_tablet(MetaService* meta_service, int64_t table_id, int64_t index_id,
                          int64_t partition_id, int64_t tablet_id) {
    brpc::Controller cntl;
    CreateTabletsRequest req;
    CreateTabletsResponse res;
    add_tablet(req, table_id, index_id, partition_id, tablet_id);
    meta_service->create_tablets(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << tablet_id;
}

static void get_tablet_stats(MetaService* meta_service, int64_t table_id, int64_t index_id,
                             int64_t partition_id, int64_t tablet_id, GetTabletStatsResponse& res) {
    brpc::Controller cntl;
    GetTabletStatsRequest req;
    auto idx = req.add_tablet_idx();
    idx->set_table_id(table_id);
    idx->set_index_id(index_id);
    idx->set_partition_id(partition_id);
    idx->set_tablet_id(tablet_id);
    meta_service->get_tablet_stats(&cntl, &req, &res, nullptr);
}

static void begin_txn(MetaService* meta_service, int64_t db_id, const std::string& label,
                      int64_t table_id, int64_t& txn_id) {
    brpc::Controller cntl;
    BeginTxnRequest req;
    BeginTxnResponse res;
    auto txn_info = req.mutable_txn_info();
    txn_info->set_db_id(db_id);
    txn_info->set_label(label);
    txn_info->add_table_ids(table_id);
    txn_info->set_timeout_ms(36000);
    meta_service->begin_txn(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    ASSERT_TRUE(res.has_txn_id()) << label;
    txn_id = res.txn_id();
}

static void commit_txn(MetaService* meta_service, int64_t db_id, int64_t txn_id,
                       const std::string& label) {
    brpc::Controller cntl;
    CommitTxnRequest req;
    CommitTxnResponse res;
    req.set_db_id(db_id);
    req.set_txn_id(txn_id);
    meta_service->commit_txn(&cntl, &req, &res, nullptr);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
}

static doris::RowsetMetaCloudPB create_rowset(int64_t txn_id, int64_t tablet_id,
                                              int64_t version = -1, int num_rows = 100) {
    doris::RowsetMetaCloudPB rowset;
    rowset.set_rowset_id(0); // required
    rowset.set_rowset_id_v2(next_rowset_id());
    rowset.set_tablet_id(tablet_id);
    rowset.set_txn_id(txn_id);
    if (version > 0) {
        rowset.set_start_version(version);
        rowset.set_end_version(version);
    }
    rowset.set_num_segments(1);
    rowset.set_num_rows(num_rows);
    rowset.set_data_disk_size(num_rows * 100);
    rowset.set_index_disk_size(num_rows * 10);
    rowset.set_total_disk_size(num_rows * 110);
    rowset.mutable_tablet_schema()->set_schema_version(0);
    rowset.set_txn_expiration(::time(nullptr)); // Required by DCHECK
    return rowset;
}

static void prepare_rowset(MetaService* meta_service, const doris::RowsetMetaCloudPB& rowset,
                           CreateRowsetResponse& res) {
    brpc::Controller cntl;
    auto arena = res.GetArena();
    auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    req->mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->prepare_rowset(&cntl, req, &res, nullptr);
    if (!arena) delete req;
}

static void commit_rowset(MetaService* meta_service, const doris::RowsetMetaCloudPB& rowset,
                          CreateRowsetResponse& res) {
    brpc::Controller cntl;
    auto arena = res.GetArena();
    auto req = google::protobuf::Arena::CreateMessage<CreateRowsetRequest>(arena);
    req->mutable_rowset_meta()->CopyFrom(rowset);
    meta_service->commit_rowset(&cntl, req, &res, nullptr);
    if (!arena) delete req;
}

static void insert_rowset(MetaService* meta_service, int64_t db_id, const std::string& label,
                          int64_t table_id, int64_t tablet_id) {
    int64_t txn_id = 0;
    ASSERT_NO_FATAL_FAILURE(begin_txn(meta_service, db_id, label, table_id, txn_id));
    CreateRowsetResponse res;
    auto rowset = create_rowset(txn_id, tablet_id);
    prepare_rowset(meta_service, rowset, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    res.Clear();
    ASSERT_NO_FATAL_FAILURE(commit_rowset(meta_service, rowset, res));
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK) << label;
    commit_txn(meta_service, db_id, txn_id, label);
}

/// NOTICE: Not ALL `code`, returned by http server, are supported by `MetaServiceCode`.

TEST(MetaServiceHttpTest, InstanceTest) {
    HttpContext ctx;

    // case: normal create instance
    {
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("create_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // case: request has invalid argument
    {
        CreateInstanceRequest req;
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("create_instance", req);
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // case: rename instance
    {
        AlterInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_name("new_name");
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("rename_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);

        InstanceInfoPB instance = ctx.get_instance_info("test_instance");
        ASSERT_EQ(instance.name(), "new_name");
    }

    // The default instance sse is disabled, to execute enable first.
    // case: enable instance sse
    {
        AlterInstanceRequest req;
        req.set_instance_id("test_instance");
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("enable_instance_sse", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);

        InstanceInfoPB instance = ctx.get_instance_info("test_instance");
        ASSERT_TRUE(instance.sse_enabled());
    }

    // case: disable instance sse
    {
        AlterInstanceRequest req;
        req.set_instance_id("test_instance");
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("disable_instance_sse", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);

        InstanceInfoPB instance = ctx.get_instance_info("test_instance");
        ASSERT_FALSE(instance.sse_enabled());
    }

    // case: get instance
    {
        auto [status_code, resp] =
                ctx.query_with_result<InstanceInfoPB>("get_instance", "instance_id=test_instance");
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status.code(), MetaServiceCode::OK);
        ASSERT_TRUE(resp.result.has_value());
        InstanceInfoPB instance = resp.result.value();
        ASSERT_EQ(instance.instance_id(), "test_instance");
        ASSERT_EQ(instance.status(), InstanceInfoPB::NORMAL);
    }

    // case: set over_due instance
    {
        AlterInstanceRequest req;
        req.set_op(AlterInstanceRequest::SET_OVERDUE);
        req.set_instance_id("test_instance");
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("set_instance_status", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
        InstanceInfoPB instance = ctx.get_instance_info("test_instance");
        ASSERT_EQ(instance.status(), InstanceInfoPB::OVERDUE);
    }

    // case: set_normal instance
    {
        AlterInstanceRequest req;
        req.set_op(AlterInstanceRequest::SET_NORMAL);
        req.set_instance_id("test_instance");
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("set_instance_status", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);

        InstanceInfoPB instance = ctx.get_instance_info("test_instance");
        ASSERT_EQ(instance.status(), InstanceInfoPB::NORMAL);
    }

    // case: get instance by cloud_unique_id
    {
        auto [status_code, resp] = ctx.query_with_result<InstanceInfoPB>(
                "get_instance", "cloud_unique_id=1:test_instance:1");
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status.code(), MetaServiceCode::OK);
        ASSERT_TRUE(resp.result.has_value());
        InstanceInfoPB instance = resp.result.value();
        ASSERT_EQ(instance.instance_id(), "test_instance");
        ASSERT_EQ(instance.status(), InstanceInfoPB::NORMAL);
    }

    // case: normal drop instance
    {
        AlterInstanceRequest req;
        req.set_instance_id("test_instance");
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("drop_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);

        InstanceInfoPB instance = ctx.get_instance_info("test_instance");
        ASSERT_EQ(instance.status(), InstanceInfoPB::DELETED);
    }
}

TEST(MetaServiceHttpTest, InstanceTestWithVersion) {
    HttpContext ctx;

    // case: normal create instance
    {
        CreateInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("v1/create_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // case: request has invalid argument
    {
        CreateInstanceRequest req;
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("v1/create_instance", req);
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // case: rename instance
    {
        AlterInstanceRequest req;
        req.set_instance_id("test_instance");
        req.set_name("new_name");
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("v1/rename_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);

        InstanceInfoPB instance = ctx.get_instance_info("test_instance");
        ASSERT_EQ(instance.name(), "new_name");
    }

    // The default instance sse is disabled, to execute enable first.
    // case: enable instance sse
    {
        AlterInstanceRequest req;
        req.set_instance_id("test_instance");
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("v1/enable_instance_sse", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);

        InstanceInfoPB instance = ctx.get_instance_info("test_instance");
        ASSERT_TRUE(instance.sse_enabled());
    }

    // case: disable instance sse
    {
        AlterInstanceRequest req;
        req.set_instance_id("test_instance");
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("v1/disable_instance_sse", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);

        InstanceInfoPB instance = ctx.get_instance_info("test_instance");
        ASSERT_FALSE(instance.sse_enabled());
    }

    // case: get instance
    {
        auto [status_code, resp] = ctx.query_with_result<InstanceInfoPB>(
                "v1/get_instance", "instance_id=test_instance");
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status.code(), MetaServiceCode::OK);
        ASSERT_TRUE(resp.result.has_value());
        InstanceInfoPB instance = resp.result.value();
        ASSERT_EQ(instance.instance_id(), "test_instance");
        ASSERT_EQ(instance.status(), InstanceInfoPB::NORMAL);
    }

    // case: normal drop instance
    {
        AlterInstanceRequest req;
        req.set_instance_id("test_instance");
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("v1/drop_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);

        InstanceInfoPB instance = ctx.get_instance_info("test_instance");
        ASSERT_EQ(instance.status(), InstanceInfoPB::DELETED);
    }
}

TEST(MetaServiceHttpTest, AlterClusterTest) {
    config::enable_cluster_name_check = true;

    HttpContext ctx;
    {
        CreateInstanceRequest req;
        req.set_instance_id(mock_instance);
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("create_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // case: normal add cluster
    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name("not-support");
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name("中文not-support");
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name("   ");
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(" not_support  ");
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(" not_support");
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // no cluster name
    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id + "1");
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(resp.msg(), "not have cluster name");
    }

    // cluster name ""
    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        req.mutable_cluster()->set_cluster_name("%");
        req.mutable_cluster()->set_cluster_id(mock_cluster_id + "1");
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(resp.msg(),
                  "cluster name not regex with ^[a-zA-Z][a-zA-Z0-9_]*$, please check it");
    }

    config::enable_cluster_name_check = false;
    // cluster name ""
    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        req.mutable_cluster()->set_cluster_name("");
        req.mutable_cluster()->set_cluster_id(mock_cluster_id + "1");
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(resp.msg(), "not have cluster name");
    }

    config::enable_cluster_name_check = true;
    // ok
    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        req.mutable_cluster()->set_cluster_name("aaaa");
        req.mutable_cluster()->set_cluster_id(mock_cluster_id + "1");
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
        ASSERT_EQ(resp.msg(), "");
    }

    // case: request has invalid argument
    {
        AlterClusterRequest req;
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("drop_cluster", req);
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    // add node
    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        auto node = req.mutable_cluster()->add_nodes();
        node->set_ip("127.0.0.1");
        node->set_heartbeat_port(9999);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_node", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // drop node
    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        auto node = req.mutable_cluster()->add_nodes();
        node->set_ip("127.0.0.1");
        node->set_heartbeat_port(9999);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("drop_node", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // rename cluster
    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_cluster_name("rename_cluster_name");
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("rename_cluster", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // alter cluster status
    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_cluster_status(ClusterStatus::SUSPENDED);
        req.set_op(AlterClusterRequest::SET_CLUSTER_STATUS);
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("set_cluster_status", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // update cluster mysql user name
    {
        AlterClusterRequest req;
        req.mutable_cluster()->add_mysql_user_name("test_user");
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("update_cluster_mysql_user_name", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // decommission_node
    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        auto node = req.mutable_cluster()->add_nodes();
        node->set_ip("127.0.0.1");
        node->set_heartbeat_port(9999);
        node->set_cloud_unique_id("cloud_unique_id");
        auto& meta_service = ctx.meta_service_;
        NodeInfoPB npb;
        npb.set_heartbeat_port(9999);
        npb.set_ip("127.0.0.1");
        npb.set_cloud_unique_id("cloud_unique_id");
        meta_service->resource_mgr()->node_info_.insert(
                {"cloud_unique_id", NodeInfo {Role::COMPUTE_NODE, mock_instance,
                                              "rename_cluster_name", mock_cluster_id, npb}});
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("decommission_node", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // notify_decommissioned
    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_cluster_name);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        auto node = req.mutable_cluster()->add_nodes();
        node->set_ip("127.0.0.1");
        node->set_heartbeat_port(9996);
        node->set_cloud_unique_id("cloud_unique_id");
        auto& meta_service = ctx.meta_service_;
        NodeInfoPB npb;
        npb.set_heartbeat_port(9996);
        npb.set_ip("127.0.0.1");
        npb.set_cloud_unique_id("cloud_unique_id");
        meta_service->resource_mgr()->node_info_.insert(
                {"cloud_unique_id", NodeInfo {Role::COMPUTE_NODE, mock_instance,
                                              "rename_cluster_name", mock_cluster_id, npb}});
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("notify_decommissioned", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // update_cluster_endpoint
    {
        AlterClusterRequest req;
        req.mutable_cluster()->add_mysql_user_name("test_user");
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id);
        req.mutable_cluster()->set_public_endpoint("127.0.0.2");
        req.mutable_cluster()->set_private_endpoint("127.0.0.3");
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("update_cluster_endpoint", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceHttpTest, GetClusterTest) {
    HttpContext ctx(true);

    // add cluster first
    InstanceKeyInfo key_info {mock_instance};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    ClusterPB c1;
    c1.set_cluster_name(mock_cluster_name);
    c1.set_cluster_id(mock_cluster_id);
    c1.add_mysql_user_name()->append("m1");
    instance.add_clusters()->CopyFrom(c1);
    ClusterPB c2;
    c2.set_cluster_name(mock_cluster_name + "2");
    c2.set_cluster_id(mock_cluster_id + "2");
    c2.add_mysql_user_name()->append("m2");
    instance.add_clusters()->CopyFrom(c2);
    val = instance.SerializeAsString();

    std::unique_ptr<Transaction> txn;
    std::string get_val;
    ASSERT_EQ(ctx.meta_service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // case: normal get
    {
        GetClusterRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        req.set_cluster_id(mock_cluster_id);
        req.set_cluster_name(mock_cluster_name);
        auto [status_code, resp] = ctx.forward_with_result<ClusterPB>("get_cluster", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status.code(), MetaServiceCode::OK);
        ASSERT_TRUE(resp.result.has_value());
        ASSERT_EQ(resp.result->cluster_id(), mock_cluster_id);
    }

    // case: not found
    {
        GetClusterRequest req;
        req.set_cloud_unique_id("unknown_id");
        req.set_cluster_id("unknown_cluster_id");
        req.set_cluster_name("unknown_cluster_name");
        auto [status_code, resp] = ctx.forward_with_result<ClusterPB>("get_cluster", req);
        ASSERT_EQ(status_code, 404);
    }

    // case: get all clusters
    {
        GetClusterRequest req;
        req.set_cloud_unique_id("test_cloud_unique_id");
        auto [status_code, resp] = ctx.forward_with_result<GetClusterResponse>("get_cluster", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status.code(), MetaServiceCode::OK);
        ASSERT_TRUE(resp.result.has_value());
        ASSERT_EQ(resp.result->cluster_size(), 2);
    }
}

TEST(MetaServiceHttpTest, AbortTxnTest) {
    HttpContext ctx(true);

    // case: abort txn by txn_id
    {
        int64_t db_id = 666;
        int64_t table_id = 12345;
        std::string label = "abort_txn_by_txn_id";
        std::string cloud_unique_id = "test_cloud_unique_id";
        int64_t txn_id = -1;
        // begin txn
        {
            brpc::Controller cntl;
            BeginTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(36000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            ctx.meta_service_->begin_txn(
                    reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res,
                    nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            txn_id = res.txn_id();
        }

        // abort txn by txn_id
        {
            AbortTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_txn_id(txn_id);
            req.set_reason("test");
            auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("abort_txn", req);
            ASSERT_EQ(status_code, 200);
            ASSERT_EQ(resp.code(), MetaServiceCode::OK);
        }
    }
}

TEST(MetaServiceHttpTest, AlterIamTest) {
    HttpContext ctx;

    brpc::Controller cntl;
    auto cloud_unique_id = "test_cloud_unique_id";
    std::string instance_id = "alter_iam_test_instance_id";
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        SyncPoint::get_instance()->clear_all_call_backs();
    };
    sp->set_call_back("get_instance_id", [&](auto&& args) {
        auto* ret = try_any_cast_ret<std::string>(args);
        ret->first = instance_id;
        ret->second = true;
    });
    sp->set_call_back("encrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 0;
        auto* key = try_any_cast<std::string*>(args[1]);
        *key = "test";
        auto* key_id = try_any_cast<int64_t*>(args[2]);
        *key_id = 1;
    });
    sp->set_call_back("decrypt_ak_sk:get_encryption_key", [](auto&& args) {
        auto* key = try_any_cast<std::string*>(args[0]);
        *key = "test";
        auto* ret = try_any_cast<int*>(args[1]);
        *ret = 0;
    });
    sp->enable_processing();

    config::arn_id = "iam_arn";
    config::arn_ak = "iam_ak";
    config::arn_sk = "iam_sk";

    ObjectStoreInfoPB obj;
    obj.set_ak("123");
    obj.set_sk("321");
    obj.set_bucket("456");
    obj.set_prefix("654");
    obj.set_endpoint("789");
    obj.set_region("987");
    obj.set_external_endpoint("888");
    obj.set_provider(ObjectStoreInfoPB::BOS);

    // create instance without ram user
    CreateInstanceRequest create_instance_req;
    create_instance_req.set_instance_id(instance_id);
    create_instance_req.set_user_id("test_user");
    create_instance_req.set_name("test_name");
    create_instance_req.mutable_obj_info()->CopyFrom(obj);
    CreateInstanceResponse create_instance_res;
    ctx.meta_service_->create_instance(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                       &create_instance_req, &create_instance_res, nullptr);
    ASSERT_EQ(create_instance_res.status().code(), MetaServiceCode::OK);

    // get iam and ram user
    GetIamRequest request;
    request.set_cloud_unique_id(cloud_unique_id);
    GetIamResponse response;
    ctx.meta_service_->get_iam(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                               &request, &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(response.has_ram_user(), false);
    ASSERT_EQ(response.iam_user().user_id(), "iam_arn");
    ASSERT_EQ(response.iam_user().ak(), "iam_ak");
    ASSERT_EQ(response.iam_user().sk(), "iam_sk");

    // alter ram user
    RamUserPB ram_user;
    ram_user.set_user_id("test_user_id");
    ram_user.set_ak("test_ak");
    ram_user.set_sk("test_sk");
    AlterRamUserRequest alter_ram_user_request;
    alter_ram_user_request.set_instance_id(instance_id);
    alter_ram_user_request.mutable_ram_user()->CopyFrom(ram_user);
    auto [status_code, resp] =
            ctx.forward<MetaServiceResponseStatus>("alter_ram_user", alter_ram_user_request);
    ASSERT_EQ(status_code, 200);
    ASSERT_EQ(resp.code(), MetaServiceCode::OK);

    // alter iam
    {
        AlterIamRequest alter_iam_request;
        alter_iam_request.set_ak("new_ak");
        alter_iam_request.set_sk("new_sk");
        alter_iam_request.set_account_id("account_id");
        auto [status_code, resp] =
                ctx.forward<MetaServiceResponseStatus>("alter_iam", alter_iam_request);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // get iam and ram user
    ctx.meta_service_->get_iam(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                               &request, &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(response.has_ram_user(), true);
    ASSERT_EQ(response.ram_user().user_id(), "test_user_id");
    ASSERT_EQ(response.ram_user().ak(), "test_ak");
    ASSERT_EQ(response.ram_user().sk(), "test_sk");
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();
}

TEST(MetaServiceHttpTest, AlterObjStoreInfoTest) {
    HttpContext ctx(true);

    {
        // Prepare instance info.
        CreateInstanceRequest req;
        req.set_instance_id(mock_instance);
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("create_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // case: add new obj store info.
    {
        AlterObjStoreInfoRequest req;
        req.set_cloud_unique_id("cloud_unique_id");
        auto* obj = req.mutable_obj();
        obj->set_ak("123_1");
        obj->set_sk("321_2");
        obj->set_bucket("456_3");
        obj->set_prefix("654_4");
        obj->set_endpoint("789_5");
        obj->set_region("987_5");
        obj->set_external_endpoint("888_");
        obj->set_provider(ObjectStoreInfoPB::BOS);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_obj_info", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);

        InstanceInfoPB instance = ctx.get_instance_info(mock_instance);
        ASSERT_EQ(instance.obj_info().size(), 2);
    }
}

TEST(MetaServiceHttpTest, GetObjStoreInfoTest) {
    HttpContext ctx(true);

    {
        // Prepare instance info.
        CreateInstanceRequest req;
        req.set_instance_id(mock_instance);
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("create_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    GetObjStoreInfoRequest req;
    req.set_cloud_unique_id("test_cloud_unique_id");
    auto [status_code, resp] =
            ctx.forward_with_result<GetObjStoreInfoResponse>("get_obj_store_info", req);
    ASSERT_EQ(status_code, 200);
    ASSERT_EQ(resp.status.code(), MetaServiceCode::OK);
    ASSERT_TRUE(resp.result.has_value());
    ASSERT_EQ(resp.result->obj_info_size(), 1);
    ObjectStoreInfoPB info = resp.result.value().obj_info().at(0);
    ASSERT_EQ(info.ak(), "123");
    ASSERT_EQ(info.sk(), "321");
}

TEST(MetaServiceHttpTest, UpdateAkSkTest) {
    HttpContext ctx(true);

    // Prepare instance info.
    {
        CreateInstanceRequest req;
        req.set_instance_id(mock_instance);
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);
        auto* user = req.mutable_ram_user();
        user->set_user_id("user_id");
        user->set_ak("old_ak");
        user->set_sk("old_sk");

        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("create_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // Case update user ak,sk
    {
        UpdateAkSkRequest req;
        req.set_instance_id(mock_instance);
        auto* user = req.mutable_ram_user();
        user->set_ak("ak");
        user->set_user_id("user_id");
        user->set_sk("sk");

        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("update_ak_sk", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }
}

TEST(MetaServiceHttpTest, GetStageTest) {
    HttpContext ctx(true);

    // Prepare instance info.
    {
        CreateInstanceRequest req;
        req.set_instance_id(mock_instance);
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("create_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    // Create a stage
    {
        CreateStageRequest req;
        req.set_cloud_unique_id("test");
        auto* stage = req.mutable_stage();
        stage->set_stage_id("stage_id");
        stage->set_arn("arn");
        stage->set_comment("comment");
        stage->set_name("stage_name");
        stage->add_mysql_user_name("mysql_user_name");
        stage->add_mysql_user_id("mysql_user_id");
        stage->set_type(StagePB::INTERNAL);

        brpc::Controller ctrl;
        CreateStageResponse resp;
        ctx.meta_service_->create_stage(&ctrl, &req, &resp, nullptr);
        ASSERT_EQ(resp.status().code(), MetaServiceCode::OK);
    }

    // Get stage
    {
        GetStageRequest req;
        req.set_stage_name("stage_name");
        req.set_type(StagePB::INTERNAL);
        req.set_cloud_unique_id("test");
        req.set_mysql_user_id("mysql_user_id");
        req.set_mysql_user_name("mysql_user_name");
        auto [status_code, resp] = ctx.forward_with_result<GetStageResponse>("get_stage", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.status.code(), MetaServiceCode::OK);
        ASSERT_TRUE(resp.result.has_value());
        ASSERT_EQ(resp.result->stage_size(), 1);
        auto& stage = resp.result->stage(0);
        ASSERT_EQ(stage.stage_id(), "stage_id");
    }
}

TEST(MetaServiceHttpTest, GetTabletStatsTest) {
    HttpContext ctx(true);
    auto& meta_service = ctx.meta_service_;

    constexpr auto table_id = 10001, index_id = 10002, partition_id = 10003, tablet_id = 10004;
    ASSERT_NO_FATAL_FAILURE(
            create_tablet(meta_service.get(), table_id, index_id, partition_id, tablet_id));
    GetTabletStatsResponse res;
    get_tablet_stats(meta_service.get(), table_id, index_id, partition_id, tablet_id, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(res.tablet_stats_size(), 1);
    EXPECT_EQ(res.tablet_stats(0).data_size(), 0);
    EXPECT_EQ(res.tablet_stats(0).num_rows(), 0);
    EXPECT_EQ(res.tablet_stats(0).num_rowsets(), 1);
    EXPECT_EQ(res.tablet_stats(0).num_segments(), 0);
    EXPECT_EQ(res.tablet_stats(0).index_size(), 0);
    EXPECT_EQ(res.tablet_stats(0).segment_size(), 0);
    {
        GetTabletStatsRequest req;
        auto idx = req.add_tablet_idx();
        idx->set_table_id(table_id);
        idx->set_index_id(index_id);
        idx->set_partition_id(partition_id);
        idx->set_tablet_id(tablet_id);
        auto [status_code, content] = ctx.forward<std::string>("get_tablet_stats", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(content, res.DebugString() + "\n");
    }

    // Insert rowset
    config::split_tablet_stats = false;
    ASSERT_NO_FATAL_FAILURE(
            insert_rowset(meta_service.get(), 10000, "label1", table_id, tablet_id));
    ASSERT_NO_FATAL_FAILURE(
            insert_rowset(meta_service.get(), 10000, "label2", table_id, tablet_id));
    config::split_tablet_stats = true;
    ASSERT_NO_FATAL_FAILURE(
            insert_rowset(meta_service.get(), 10000, "label3", table_id, tablet_id));
    ASSERT_NO_FATAL_FAILURE(
            insert_rowset(meta_service.get(), 10000, "label4", table_id, tablet_id));
    // Check tablet stats kv
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(ctx.meta_service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string data_size_key, data_size_val;
    stats_tablet_data_size_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                               &data_size_key);
    ASSERT_EQ(txn->get(data_size_key, &data_size_val), TxnErrorCode::TXN_OK);
    EXPECT_EQ(*(int64_t*)data_size_val.data(), 22000);
    std::string index_size_key, index_size_val;
    stats_tablet_index_size_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                                &index_size_key);
    ASSERT_EQ(txn->get(index_size_key, &index_size_val), TxnErrorCode::TXN_OK);
    EXPECT_EQ(*(int64_t*)index_size_val.data(), 2000);
    std::string segment_size_key, segment_size_val;
    stats_tablet_segment_size_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                                  &segment_size_key);
    ASSERT_EQ(txn->get(segment_size_key, &segment_size_val), TxnErrorCode::TXN_OK);
    EXPECT_EQ(*(int64_t*)segment_size_val.data(), 20000);
    std::string num_rows_key, num_rows_val;
    stats_tablet_num_rows_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                              &num_rows_key);
    ASSERT_EQ(txn->get(num_rows_key, &num_rows_val), TxnErrorCode::TXN_OK);
    EXPECT_EQ(*(int64_t*)num_rows_val.data(), 200);
    std::string num_rowsets_key, num_rowsets_val;
    stats_tablet_num_rowsets_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                                 &num_rowsets_key);
    ASSERT_EQ(txn->get(num_rowsets_key, &num_rowsets_val), TxnErrorCode::TXN_OK);
    EXPECT_EQ(*(int64_t*)num_rowsets_val.data(), 2);
    std::string num_segs_key, num_segs_val;
    stats_tablet_num_segs_key({mock_instance, table_id, index_id, partition_id, tablet_id},
                              &num_segs_key);
    ASSERT_EQ(txn->get(num_segs_key, &num_segs_val), TxnErrorCode::TXN_OK);
    EXPECT_EQ(*(int64_t*)num_segs_val.data(), 2);
    // Get tablet stats
    res.Clear();
    get_tablet_stats(meta_service.get(), table_id, index_id, partition_id, tablet_id, res);
    ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    ASSERT_EQ(res.tablet_stats_size(), 1);
    EXPECT_EQ(res.tablet_stats(0).data_size(), 44000);
    EXPECT_EQ(res.tablet_stats(0).num_rows(), 400);
    EXPECT_EQ(res.tablet_stats(0).num_rowsets(), 5);
    EXPECT_EQ(res.tablet_stats(0).num_segments(), 4);
    EXPECT_EQ(res.tablet_stats(0).index_size(), 4000);
    EXPECT_EQ(res.tablet_stats(0).segment_size(), 40000);
    {
        GetTabletStatsRequest req;
        auto idx = req.add_tablet_idx();
        idx->set_table_id(table_id);
        idx->set_index_id(index_id);
        idx->set_partition_id(partition_id);
        idx->set_tablet_id(tablet_id);
        auto [status_code, content] = ctx.forward<std::string>("get_tablet_stats", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(content, res.DebugString() + "\n");
    }
}

TEST(MetaServiceHttpTest, ToUnknownUrlTest) {
    HttpContext ctx;
    auto [status_code, content] = ctx.query<std::string>("unkown_resource_xxxxxx", "");
    ASSERT_EQ(status_code, 200);
    ASSERT_EQ(content, "{\n    \"code\": \"OK\",\n    \"msg\": \"\"\n}\n");
}

TEST(MetaServiceHttpTest, UnknownFields) {
    // LOG:
    // parse http request 'get_tablet_stats': INVALID_ARGUMENT:an_unknown_field: Cannot find field. body="{"table_id": 1, "an_unknown_field": "xxxx"}"
    HttpContext ctx;
    std::string body =
            "{\"table_id\": 1, \"an_unknown_field\": \"xxxx\", \"cloud_unique_id\": "
            "\"1:test_instance:1\"}";
    auto [status_code, content] = ctx.query<std::string>("get_tablet_stats", "", body);
    ASSERT_EQ(status_code, 200);
}

TEST(MetaServiceHttpTest, EncodeAndDecodeKey) {
    HttpContext ctx;
    {
        auto [status_code, content] =
                ctx.query<std::string>("encode_key", "key_type=InstanceKey&instance_id=test", "");
        ASSERT_EQ(status_code, 200);
        const char* encode_key_output = R"(
┌───────────────────────── 0. key space: 1
│ ┌─────────────────────── 1. instance
│ │                     ┌─ 2. test
│ │                     │ 
▼ ▼                     ▼ 
0110696e7374616e6365000110746573740001
\x01\x10\x69\x6e\x73\x74\x61\x6e\x63\x65\x00\x01\x10\x74\x65\x73\x74\x00\x01

)";
        content.insert(0, 1, '\n');
        ASSERT_EQ(content, encode_key_output);
    }

    {
        auto [status_code, content] = ctx.query<std::string>(
                "decode_key", "key=0110696e7374616e6365000110746573740001", "");
        ASSERT_EQ(status_code, 200);
        const char* decode_key_output = R"(
┌───────────────────────── 0. key space: 1
│ ┌─────────────────────── 1. instance
│ │                     ┌─ 2. test
│ │                     │ 
▼ ▼                     ▼ 
0110696e7374616e6365000110746573740001

)";
        content.insert(0, 1, '\n');
        ASSERT_EQ(content, decode_key_output);
    }
}

TEST(MetaServiceHttpTest, GetValue) {
    HttpContext ctx(true);

    // Prepare instance info.
    {
        CreateInstanceRequest req;
        req.set_instance_id("get_value_instance_id");
        req.set_user_id("test_user");
        req.set_name("test_name");
        ObjectStoreInfoPB obj;
        obj.set_ak("123");
        obj.set_sk("321");
        obj.set_bucket("456");
        obj.set_prefix("654");
        obj.set_endpoint("789");
        obj.set_region("987");
        obj.set_external_endpoint("888");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("create_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    auto param = "key_type=InstanceKey&instance_id=get_value_instance_id";
    auto [status_code, content] = ctx.query<std::string>("get_value", param, "");
    ASSERT_EQ(status_code, 200);
    auto instance_info = ctx.get_instance_info("get_value_instance_id");
    auto get_value_output = proto_to_json(instance_info);
    get_value_output.push_back('\n');
    ASSERT_EQ(content, get_value_output);
}

TEST(MetaServiceHttpTest, InvalidToken) {
    HttpContext ctx(true);
    auto [status_code, content] = ctx.query<std::string>("get_value", "token=invalid_token", "");
    ASSERT_EQ(status_code, 403);
    const char* invalid_token_output = "incorrect token, token=invalid_token\n";
    ASSERT_EQ(content, invalid_token_output);
}

TEST(MetaServiceHttpTest, TxnLazyCommit) {
    HttpContext ctx;
    {
        auto [status_code, content] =
                ctx.query<std::string>("txn_lazy_commit", "instance_id=test_instance", "");
        std::string msg = "instance_id or txn_id is empty";
        ASSERT_TRUE(content.find(msg) != std::string::npos);
        ASSERT_EQ(status_code, 400);
    }

    {
        auto [status_code, content] = ctx.query<std::string>("txn_lazy_commit", "txn_id=1000", "");
        std::string msg = "instance_id or txn_id is empty";
        ASSERT_TRUE(content.find(msg) != std::string::npos);
        ASSERT_EQ(status_code, 400);
    }

    {
        auto [status_code, content] = ctx.query<std::string>(
                "txn_lazy_commit", "instance_id=test_instance&txn_id=1000", "");

        std::string msg = "failed to get db id, txn_id=1000 err=KeyNotFound";
        ASSERT_TRUE(content.find(msg) != std::string::npos)
                << "msg: " << msg << ", content: " << content << ", status_code: " << status_code;
    }

    {
        auto [status_code, content] = ctx.query<std::string>(
                "txn_lazy_commit", "instance_id=test_instance&txn_id=abc", "");

        std::string msg = "txn_id abc must be a number";
        ASSERT_TRUE(content.find(msg) != std::string::npos);
    }
}

TEST(MetaServiceHttpTest, get_stage_response_sk) {
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
    };

    GetStageResponse res;
    auto* stage = res.add_stage();
    stage->mutable_obj_info()->set_ak("stage-ak");
    stage->mutable_obj_info()->set_sk("stage-sk");
    auto foo = [res](auto args) { (*(try_any_cast<GetStageResponse**>(args[0])))->CopyFrom(res); };
    sp->set_call_back("stage_sk_response", foo);
    sp->set_call_back("stage_sk_response_return",
                      [](auto&& args) { *try_any_cast<bool*>(args.back()) = true; });

    auto rate_limiter = std::make_shared<cloud::RateLimiter>();

    auto ms = std::make_unique<cloud::MetaServiceImpl>(nullptr, nullptr, rate_limiter);

    auto bar = [](auto args) {
        std::cout << *try_any_cast<std::string*>(args[0]);

        EXPECT_TRUE((*try_any_cast<std::string*>(args[0])).find("stage-sk") == std::string::npos);
        EXPECT_TRUE((*try_any_cast<std::string*>(args[0]))
                            .find("md5: f497d053066fa4b7d3b1f6564597d233") != std::string::npos);
    };
    sp->set_call_back("sk_finish_rpc", bar);

    GetStageResponse res1;
    GetStageRequest req1;
    brpc::Controller cntl;
    ms->get_stage(&cntl, &req1, &res1, nullptr);
}

TEST(MetaServiceHttpTest, get_obj_store_info_response_sk) {
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    DORIS_CLOUD_DEFER {
        sp->disable_processing();
    };

    GetObjStoreInfoResponse res;
    auto* obj_info = res.add_obj_info();
    obj_info->set_ak("obj-store-info-ak1");
    obj_info->set_sk("obj-store-info-sk1");
    obj_info = res.add_storage_vault()->mutable_obj_info();
    obj_info->set_ak("obj-store-info-ak2");
    obj_info->set_sk("obj-store-info-sk2");
    auto foo = [res](auto args) {
        (*(try_any_cast<GetObjStoreInfoResponse**>(args[0])))->CopyFrom(res);
    };
    sp->set_call_back("obj-store-info_sk_response", foo);
    sp->set_call_back("obj-store-info_sk_response_return",
                      [](auto&& args) { *try_any_cast<bool*>(args.back()) = true; });

    auto rate_limiter = std::make_shared<cloud::RateLimiter>();

    auto ms = std::make_unique<cloud::MetaServiceImpl>(nullptr, nullptr, rate_limiter);

    auto bar = [](auto args) {
        std::cout << *try_any_cast<std::string*>(args[0]);

        EXPECT_TRUE((*try_any_cast<std::string*>(args[0])).find("obj-store-info-sk1") ==
                    std::string::npos);
        EXPECT_TRUE((*try_any_cast<std::string*>(args[0]))
                            .find("md5: 35d5a637fd9d45a28207a888b751efc4") != std::string::npos);

        EXPECT_TRUE((*try_any_cast<std::string*>(args[0])).find("obj-store-info-sk2") ==
                    std::string::npos);
        EXPECT_TRUE((*try_any_cast<std::string*>(args[0]))
                            .find("md5: 01d7473ae201a2ecdf1f7c064eb81a95") != std::string::npos);
    };
    sp->set_call_back("sk_finish_rpc", bar);

    GetObjStoreInfoResponse res1;
    GetObjStoreInfoRequest req1;
    brpc::Controller cntl;
    ms->get_obj_store_info(&cntl, &req1, &res1, nullptr);
}

TEST(MetaServiceHttpTest, AdjustRateLimit) {
    HttpContext ctx;
    {
        auto [status_code, content] =
                ctx.query<std::string>("adjust_rate_limit", "qps_limit=10000");
        ASSERT_EQ(status_code, 200);
    }
    {
        auto [status_code, content] =
                ctx.query<std::string>("adjust_rate_limit", "qps_limit=10000&rpc_name=get_cluster");
        ASSERT_EQ(status_code, 200);
    }
    {
        auto [status_code, content] = ctx.query<std::string>(
                "adjust_rate_limit",
                "qps_limit=10000&rpc_name=get_cluster&instance_id=test_instance");
        ASSERT_EQ(status_code, 200);
    }
    {
        auto [status_code, content] = ctx.query<std::string>(
                "adjust_rate_limit", "qps_limit=10000&instance_id=test_instance");
        ASSERT_EQ(status_code, 200);
    }
    {
        auto [status_code, content] =
                ctx.query<std::string>("adjust_rate_limit", "qps_limit=invalid");
        ASSERT_EQ(status_code, 400);
        std::string msg = "param `qps_limit` is not a legal int64 type:";
        ASSERT_NE(content.find(msg), std::string::npos);
    }
    {
        auto [status_code, content] = ctx.query<std::string>("adjust_rate_limit", "qps_limit=-1");
        ASSERT_EQ(status_code, 400);
        std::string msg = "qps_limit` should not be less than 0";
        ASSERT_NE(content.find(msg), std::string::npos);
    }
    {
        auto [status_code, content] =
                ctx.query<std::string>("adjust_rate_limit", "rpc_name=get_cluster");
        ASSERT_EQ(status_code, 400);
        std::string msg = "invalid argument:";
        ASSERT_NE(content.find(msg), std::string::npos);
    }
    {
        auto [status_code, content] =
                ctx.query<std::string>("adjust_rate_limit", "instance_id=test_instance");
        ASSERT_EQ(status_code, 400);
        std::string msg = "invalid argument:";
        ASSERT_NE(content.find(msg), std::string::npos);
    }
    {
        auto [status_code, content] = ctx.query<std::string>(
                "adjust_rate_limit", "rpc_name=get_cluster&instance_id=test_instance");
        ASSERT_EQ(status_code, 400);
        std::string msg = "invalid argument:";
        ASSERT_NE(content.find(msg), std::string::npos);
    }
    {
        auto [status_code, content] = ctx.query<std::string>("adjust_rate_limit", "");
        ASSERT_EQ(status_code, 400);
        std::string msg = "invalid argument:";
        ASSERT_NE(content.find(msg), std::string::npos);
    }
    {
        auto [status_code, content] =
                ctx.query<std::string>("adjust_rate_limit", "qps_limit=1000&rpc_name=invalid");
        ASSERT_EQ(status_code, 400);
        std::string msg = "failed to adjust rate limit for qps_limit";
        ASSERT_NE(content.find(msg), std::string::npos);
    }
    {
        auto [status_code, content] =
                ctx.query<std::string>("adjust_rate_limit", "qps_limit=1000&instance_id=invalid");
        ASSERT_EQ(status_code, 200);
    }
    {
        auto [status_code, content] = ctx.query<std::string>(
                "adjust_rate_limit", "qps_limit=1000&rpc_name=get_cluster&instance_id=invalid");
        ASSERT_EQ(status_code, 200);
    }
}

TEST(MetaServiceHttpTest, QueryRateLimit) {
    HttpContext ctx;
    {
        auto [status_code, content] = ctx.query<std::string>("list_rate_limit", "");
        ASSERT_EQ(status_code, 200);
    }
}

TEST(MetaServiceHttpTest, UpdateConfig) {
    HttpContext ctx;
    {
        auto [status_code, content] = ctx.query<std::string>("update_config", "");
        ASSERT_EQ(status_code, 400);
        std::string msg = "query param `config` should not be empty";
        ASSERT_NE(content.find(msg), std::string::npos);
    }
    {
        auto [status_code, content] = ctx.query<std::string>("update_config", "configs=aaa");
        ASSERT_EQ(status_code, 400);
        std::string msg = "config aaa is invalid";
        ASSERT_NE(content.find(msg), std::string::npos);
    }
    {
        auto [status_code, content] = ctx.query<std::string>("update_config", "configs=aaa=bbb");
        ASSERT_EQ(status_code, 400);
        std::string msg = "config field=aaa not exists";
        ASSERT_NE(content.find(msg), std::string::npos);
    }
    {
        auto [status_code, content] =
                ctx.query<std::string>("update_config", "configs=custom_conf_path=./doris_conf");
        ASSERT_EQ(status_code, 400);
        std::string msg = "config field=custom_conf_path is immutable";
        ASSERT_NE(content.find(msg), std::string::npos);
    }
    {
        auto [status_code, content] =
                ctx.query<std::string>("update_config", "configs=recycle_interval_seconds=3599");
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(config::recycle_interval_seconds, 3599);
    }
    {
        auto [status_code, content] = ctx.query<std::string>(
                "update_config", "configs=recycle_interval_seconds=3601,retention_seconds=259201");
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(config::retention_seconds, 259201);
        ASSERT_EQ(config::recycle_interval_seconds, 3601);
    }
    {
        auto [status_code, content] =
                ctx.query<std::string>("update_config", "configs=enable_s3_rate_limiter=true");
        ASSERT_EQ(status_code, 200);
        ASSERT_TRUE(config::enable_s3_rate_limiter);
    }
    {
        auto [status_code, content] =
                ctx.query<std::string>("update_config", "enable_s3_rate_limiter=invalid");
        ASSERT_EQ(status_code, 400);
    }
    {
        auto original_conf_path = config::custom_conf_path;
        config::custom_conf_path = "./doris_cloud_custom.conf";
        {
            auto [status_code, content] = ctx.query<std::string>(
                    "update_config",
                    "configs=recycle_interval_seconds=3659,retention_seconds=259219&persist=true");
            ASSERT_EQ(status_code, 200);
            ASSERT_EQ(config::recycle_interval_seconds, 3659);
            ASSERT_EQ(config::retention_seconds, 259219);
            config::Properties props;
            ASSERT_TRUE(props.load(config::custom_conf_path.c_str(), true));
            {
                bool new_val_set = false;
                int64_t recycle_interval_s = 0;
                ASSERT_TRUE(props.get_or_default("recycle_interval_seconds", nullptr,
                                                 recycle_interval_s, &new_val_set));
                ASSERT_TRUE(new_val_set);
                ASSERT_EQ(recycle_interval_s, 3659);
            }
            {
                bool new_val_set = false;
                int64_t retention_s = 0;
                ASSERT_TRUE(props.get_or_default("retention_seconds", nullptr, retention_s,
                                                 &new_val_set));
                ASSERT_TRUE(new_val_set);
                ASSERT_EQ(retention_s, 259219);
            }
        }
        {
            auto [status_code, content] =
                    ctx.query<std::string>("update_config",
                                           "configs=delete_bitmap_lock_v2_white_list="
                                           "warehouse2;warehouse3&persist=true");

            ASSERT_EQ(status_code, 200);
            ASSERT_EQ(config::delete_bitmap_lock_v2_white_list, "warehouse2;warehouse3");
            auto& meta_service = ctx.meta_service_;
            std::string use_version = "";
            std::string instance_id = "warehouse1";
            meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
            ASSERT_EQ(use_version, "v1");
            instance_id = "warehouse2";
            meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
            ASSERT_EQ(use_version, "v2");
            instance_id = "warehouse3";
            meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
            ASSERT_EQ(use_version, "v2");
            config::Properties props;
            ASSERT_TRUE(props.load(config::custom_conf_path.c_str(), true));
            {
                bool new_val_set = false;
                std::string white_list = "";
                ASSERT_TRUE(props.get_or_default("delete_bitmap_lock_v2_white_list", nullptr,
                                                 white_list, &new_val_set));
                ASSERT_TRUE(new_val_set);
                ASSERT_EQ(white_list, "warehouse2;warehouse3");
                instance_id = "warehouse1";
                meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
                ASSERT_EQ(use_version, "v1");
                instance_id = "warehouse2";
                meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
                ASSERT_EQ(use_version, "v2");
                instance_id = "warehouse3";
                meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
                ASSERT_EQ(use_version, "v2");
            }
        }
        //resend config will rewrite it
        {
            auto [status_code, content] = ctx.query<std::string>(
                    "update_config", "configs=delete_bitmap_lock_v2_white_list=''&persist=true");
            ASSERT_EQ(status_code, 200);
            ASSERT_EQ(config::delete_bitmap_lock_v2_white_list, "''");
            auto& meta_service = ctx.meta_service_;
            std::string use_version = "";
            std::string instance_id = "warehouse1";
            meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
            ASSERT_EQ(use_version, "v1");
            instance_id = "warehouse2";
            meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
            ASSERT_EQ(use_version, "v1");
            instance_id = "warehouse3";
            meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
            ASSERT_EQ(use_version, "v1");
        }
        {
            auto [status_code, content] =
                    ctx.query<std::string>("update_config",
                                           "configs=delete_bitmap_lock_v2_white_list="
                                           "warehouse4;warehouse5&persist=true");
            ASSERT_EQ(status_code, 200);
            ASSERT_EQ(config::delete_bitmap_lock_v2_white_list, "warehouse4;warehouse5");
            auto& meta_service = ctx.meta_service_;
            std::string use_version = "";
            std::string instance_id = "warehouse3";
            meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
            ASSERT_EQ(use_version, "v1");
            instance_id = "warehouse4";
            meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
            ASSERT_EQ(use_version, "v2");
            instance_id = "warehouse5";
            meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
            ASSERT_EQ(use_version, "v2");
            config::Properties props;
            ASSERT_TRUE(props.load(config::custom_conf_path.c_str(), true));
            {
                bool new_val_set = false;
                std::string white_list = "";
                ASSERT_TRUE(props.get_or_default("delete_bitmap_lock_v2_white_list", nullptr,
                                                 white_list, &new_val_set));
                ASSERT_TRUE(new_val_set);
                ASSERT_EQ(white_list, "warehouse4;warehouse5");
                instance_id = "warehouse3";
                meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
                ASSERT_EQ(use_version, "v1");
                instance_id = "warehouse4";
                meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
                ASSERT_EQ(use_version, "v2");
                instance_id = "warehouse5";
                meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
                ASSERT_EQ(use_version, "v2");
            }
        }
        {
            auto [status_code, content] = ctx.query<std::string>(
                    "update_config", "configs=enable_s3_rate_limiter=false&persist=true");
            ASSERT_EQ(status_code, 200);
            ASSERT_EQ(config::recycle_interval_seconds, 3659);
            ASSERT_EQ(config::retention_seconds, 259219);
            config::Properties props;
            ASSERT_TRUE(props.load(config::custom_conf_path.c_str(), true));
            {
                bool new_val_set = false;
                int64_t recycle_interval_s = 0;
                ASSERT_TRUE(props.get_or_default("recycle_interval_seconds", nullptr,
                                                 recycle_interval_s, &new_val_set));
                ASSERT_TRUE(new_val_set);
                ASSERT_EQ(recycle_interval_s, 3659);
            }
            {
                bool new_val_set = false;
                int64_t retention_s = 0;
                ASSERT_TRUE(props.get_or_default("retention_seconds", nullptr, retention_s,
                                                 &new_val_set));
                ASSERT_TRUE(new_val_set);
                ASSERT_EQ(retention_s, 259219);
            }
            {
                bool new_val_set = false;
                bool enable_s3_rate_limiter = true;
                ASSERT_TRUE(props.get_or_default("enable_s3_rate_limiter", nullptr,
                                                 enable_s3_rate_limiter, &new_val_set));
                ASSERT_TRUE(new_val_set);
                ASSERT_FALSE(enable_s3_rate_limiter);
            }
            {
                bool new_val_set = false;
                std::string white_list = "";
                ASSERT_TRUE(props.get_or_default("delete_bitmap_lock_v2_white_list", nullptr,
                                                 white_list, &new_val_set));
                ASSERT_TRUE(new_val_set);
                ASSERT_EQ(white_list, "warehouse4;warehouse5");
                auto& meta_service = ctx.meta_service_;
                std::string use_version = "";
                std::string instance_id = "warehouse3";
                meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
                ASSERT_EQ(use_version, "v1");
                instance_id = "warehouse4";
                meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
                ASSERT_EQ(use_version, "v2");
                instance_id = "warehouse5";
                meta_service->get_delete_bitmap_lock_version(use_version, instance_id);
                ASSERT_EQ(use_version, "v2");
            }
        }
        std::filesystem::remove(config::custom_conf_path);
        config::custom_conf_path = original_conf_path;
    }
}

TEST(HttpEncodeKeyTest, ProcessHttpSetValue) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    // Create and serialize initial RowsetMeta
    RowsetMetaCloudPB initial_rowset_meta;
    initial_rowset_meta.set_rowset_id_v2("12345");
    initial_rowset_meta.set_rowset_id(0);
    initial_rowset_meta.set_tablet_id(67890);
    initial_rowset_meta.set_num_rows(100);
    initial_rowset_meta.set_data_disk_size(1024);
    std::string serialized_initial = initial_rowset_meta.SerializeAsString();

    // Generate proper rowset meta key
    std::string instance_id = "test_instance";
    int64_t tablet_id = 67890;
    int64_t version = 10086;

    // Generate proper rowset meta key
    MetaRowsetKeyInfo key_info {instance_id, tablet_id, version};
    std::string initial_key = meta_rowset_key(key_info);

    // Store initial RowsetMeta in TxnKv
    txn->put(initial_key, serialized_initial);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // Create new RowsetMeta to update
    RowsetMetaCloudPB new_rowset_meta;
    new_rowset_meta.set_rowset_id_v2("12345");
    new_rowset_meta.set_rowset_id(0);
    new_rowset_meta.set_tablet_id(67890);
    new_rowset_meta.set_num_rows(200);        // Updated row count
    new_rowset_meta.set_data_disk_size(2048); // Updated size
    std::string json_value = proto_to_json(new_rowset_meta);

    // Initialize cntl URI with required parameters
    brpc::URI cntl_uri;
    cntl_uri._path = "/meta-service/http/set_value";
    cntl_uri.SetQuery("key_type", "MetaRowsetKey");
    cntl_uri.SetQuery("instance_id", instance_id);
    cntl_uri.SetQuery("tablet_id", std::to_string(tablet_id));
    cntl_uri.SetQuery("version", std::to_string(version));

    brpc::Controller cntl;
    cntl.request_attachment().append(json_value);
    cntl.http_request().uri() = cntl_uri;

    // Test update
    auto response = process_http_set_value(txn_kv.get(), &cntl);
    EXPECT_EQ(response.status_code, 200) << response.msg;
    std::stringstream final_json;
    final_json << "original_value_hex=" << hex(initial_rowset_meta.SerializeAsString()) << "\n"
               << "key_hex=" << hex(initial_key) << "\n"
               << "original_value_json=" << proto_to_json(initial_rowset_meta) << "\n"
               << "changed_value_hex=" << hex(new_rowset_meta.SerializeAsString()) << "\n";
    // std::cout << "xxx " << final_json.str() << std::endl;
    EXPECT_EQ(response.body, final_json.str());

    // Verify update
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string updated_value;
    ASSERT_EQ(txn->get(initial_key, &updated_value), TxnErrorCode::TXN_OK);

    RowsetMetaCloudPB updated_rowset_meta;
    ASSERT_TRUE(updated_rowset_meta.ParseFromString(updated_value));
    EXPECT_EQ(updated_rowset_meta.rowset_id_v2(), "12345");
    EXPECT_EQ(updated_rowset_meta.tablet_id(), 67890);
    EXPECT_EQ(updated_rowset_meta.num_rows(), 200);
    EXPECT_EQ(updated_rowset_meta.data_disk_size(), 2048);
}

TEST(MetaServiceHttpTest, CreateInstanceWithIamRoleTest) {
    HttpContext ctx;

    brpc::Controller cntl;
    std::string instance_id = "iam_role_test_instance_id";

    {
        ObjectStoreInfoPB obj;
        obj.set_endpoint("s3.us-east-1.amazonaws.com");
        obj.set_region("us-east-1");
        obj.set_prefix("/test-prefix");
        obj.set_provider(ObjectStoreInfoPB::S3);

        // create instance without ram user
        CreateInstanceRequest create_instance_req;
        create_instance_req.set_instance_id(instance_id);
        create_instance_req.set_user_id("test_user");
        create_instance_req.set_name("test_name");
        create_instance_req.mutable_obj_info()->CopyFrom(obj);
        CreateInstanceResponse create_instance_res;
        ctx.meta_service_->create_instance(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &create_instance_req,
                &create_instance_res, nullptr);
        LOG(INFO) << create_instance_res.DebugString();
        ASSERT_EQ(create_instance_res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    {
        ObjectStoreInfoPB obj;
        obj.set_endpoint("s3.us-east-1.amazonaws.com");
        obj.set_region("us-east-1");
        obj.set_prefix("/test-prefix");
        obj.set_provider(ObjectStoreInfoPB::S3);

        // create instance without ram user
        CreateInstanceRequest create_instance_req;
        create_instance_req.set_instance_id(instance_id);
        create_instance_req.set_user_id("test_user");
        create_instance_req.set_name("test_name");
        create_instance_req.mutable_obj_info()->CopyFrom(obj);
        CreateInstanceResponse create_instance_res;
        ctx.meta_service_->create_instance(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &create_instance_req,
                &create_instance_res, nullptr);
        LOG(INFO) << create_instance_res.DebugString();
        ASSERT_EQ(create_instance_res.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    }

    {
        ObjectStoreInfoPB obj;
        obj.set_endpoint("s3.us-east-1.amazonaws.com");
        obj.set_region("us-east-1");
        obj.set_bucket("test-bucket");
        obj.set_prefix("test-prefix");
        obj.set_provider(ObjectStoreInfoPB::S3);
        obj.set_role_arn("arn:aws:iam::123456789012:role/test-role");
        obj.set_external_id("test-external-id");
        obj.set_cred_provider_type(CredProviderTypePB::INSTANCE_PROFILE);

        CreateInstanceRequest create_instance_req;
        create_instance_req.set_instance_id(instance_id);
        create_instance_req.set_user_id("test_user");
        create_instance_req.set_name("test_name");
        create_instance_req.mutable_obj_info()->CopyFrom(obj);
        CreateInstanceResponse create_instance_res;
        ctx.meta_service_->create_instance(
                reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &create_instance_req,
                &create_instance_res, nullptr);
        LOG(INFO) << create_instance_res.DebugString();
        ASSERT_EQ(create_instance_res.status().code(), MetaServiceCode::OK);

        InstanceInfoPB instance = ctx.get_instance_info(instance_id);
        LOG(INFO) << instance.DebugString();

        ASSERT_EQ(instance.obj_info().Get(0).endpoint(), "s3.us-east-1.amazonaws.com");
        ASSERT_EQ(instance.obj_info().Get(0).region(), "us-east-1");
        ASSERT_EQ(instance.obj_info().Get(0).bucket(), "test-bucket");
        ASSERT_EQ(instance.obj_info().Get(0).prefix(), "test-prefix");
        ASSERT_EQ(instance.obj_info().Get(0).provider(), ObjectStoreInfoPB::S3);
        ASSERT_EQ(instance.obj_info().Get(0).role_arn(),
                  "arn:aws:iam::123456789012:role/test-role");
        ASSERT_EQ(instance.obj_info().Get(0).external_id(), "test-external-id");
        ASSERT_EQ(instance.obj_info().Get(0).cred_provider_type(),
                  CredProviderTypePB::INSTANCE_PROFILE);
        ASSERT_EQ(instance.obj_info().Get(0).has_ak(), false);
        ASSERT_EQ(instance.obj_info().Get(0).has_sk(), false);
    }
}

static std::tuple<int, MetaServiceResponseStatus> add_cluster(
        HttpContext& ctx, std::string cluster_name, std::string cluster_id,
        const ClusterPB::Type type, const std::vector<std::string>& cluster_names,
        const ClusterPolicy* policy) {
    AlterClusterRequest req;
    req.set_instance_id(mock_instance);
    req.mutable_cluster()->set_type(type);
    req.mutable_cluster()->set_cluster_id(cluster_id);
    req.mutable_cluster()->set_cluster_name(cluster_name);
    if (type == ClusterPB::VIRTUAL) {
        for (auto cg_name : cluster_names) {
            req.mutable_cluster()->add_cluster_names(cg_name);
        }
        if (policy != nullptr) {
            req.mutable_cluster()->mutable_cluster_policy()->CopyFrom(*policy);
        }
    }
    return ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
}

static std::tuple<int, MetaServiceResponseStatus> alter_virtual_cluster(
        HttpContext& ctx, std::string instance_id, std::string cluster_name, std::string cluster_id,
        const std::vector<std::string>& cluster_names, const ClusterPolicy* policy) {
    AlterClusterRequest alter_req;
    alter_req.set_instance_id(instance_id);
    alter_req.mutable_cluster()->set_cluster_id(cluster_id);
    alter_req.mutable_cluster()->set_cluster_name(cluster_name);
    alter_req.mutable_cluster()->set_type(ClusterPB::VIRTUAL);
    for (auto subcg_name : cluster_names) {
        alter_req.mutable_cluster()->add_cluster_names(subcg_name);
    }
    if (policy != nullptr) {
        alter_req.mutable_cluster()->mutable_cluster_policy()->CopyFrom(*policy);
    }
    return ctx.forward<MetaServiceResponseStatus>("alter_vcluster_info", alter_req);
}

static std::tuple<int, JsonTemplate<ClusterPB>> get_cluster_info(HttpContext& ctx,
                                                                 std::string cluster_name,
                                                                 std::string cluster_id) {
    GetClusterRequest req;
    req.set_cloud_unique_id("1:" + mock_instance + ":xxxx");
    req.set_cluster_id(cluster_id);
    req.set_cluster_name(cluster_name);
    return ctx.forward_with_result<ClusterPB>("get_cluster", req);
}

static std::tuple<int, MetaServiceResponseStatus> drop_cluster(HttpContext& ctx,
                                                               std::string cluster_name,
                                                               std::string cluster_id) {
    AlterClusterRequest req_drop;
    req_drop.set_instance_id(mock_instance);
    if ("" != cluster_id) {
        req_drop.mutable_cluster()->set_cluster_id(cluster_id);
    }
    if ("" != cluster_name) {
        req_drop.mutable_cluster()->set_cluster_name(cluster_name);
    }
    return ctx.forward<MetaServiceResponseStatus>("drop_cluster", req_drop);
}

TEST(MetaServiceHttpTest, VirtualClusterTest) {
    config::enable_cluster_name_check = true;

    HttpContext ctx;
    {
        CreateInstanceRequest req;
        req.set_instance_id(mock_instance);
        req.set_user_id("test_user_virtual");
        req.set_name("test_name_virtual");
        ObjectStoreInfoPB obj;
        obj.set_ak("123_virtual");
        obj.set_sk("321_virtual");
        obj.set_bucket("456_virtual");
        obj.set_prefix("654_virtual");
        obj.set_endpoint("789_virtual");
        obj.set_region("987_virtual");
        obj.set_external_endpoint("888_virtual");
        obj.set_provider(ObjectStoreInfoPB::BOS);
        req.mutable_obj_info()->CopyFrom(obj);

        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("create_instance", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
    }

    //case: no type
    {
        std::string mock_vcg_name = "virtual_cluster_name";
        std::string mock_vcg_id = "virtual_cluster_id";
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_vcg_name);
        req.mutable_cluster()->set_cluster_id(mock_vcg_id);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        std::cout << resp.DebugString() << std::endl;
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(resp.msg(), "cluster must have type arg");
    }

    //case: regex not ok
    {
        std::string mock_vcg_name = "*virtual_cluster_name";
        std::string mock_vcg_id = "virtual_cluster_id";
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_vcg_name);
        req.mutable_cluster()->set_cluster_id(mock_vcg_id);
        req.mutable_cluster()->set_type(ClusterPB::VIRTUAL);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        std::cout << resp.DebugString() << std::endl;
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(resp.msg(),
                  "cluster name not regex with ^[a-zA-Z][a-zA-Z0-9_]*$, please check it");
    }

    //case: no instance
    {
        std::string mock_vcg_name = "virtual_cluster_name";
        std::string mock_vcg_id = "virtual_cluster_id";
        AlterClusterRequest req;
        req.mutable_cluster()->set_cluster_name(mock_vcg_name);
        req.mutable_cluster()->set_cluster_id(mock_vcg_id);
        req.mutable_cluster()->set_type(ClusterPB::VIRTUAL);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        std::cout << resp.DebugString() << std::endl;
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(resp.msg(), "invalid request instance_id or cluster not given");
    }

    //case: no cluster name
    {
        std::string mock_vcg_name = "virtual_cluster_name";
        std::string mock_vcg_id = "virtual_cluster_id";
        AlterClusterRequest req;
        req.mutable_cluster()->set_cluster_id(mock_vcg_id);
        req.mutable_cluster()->set_type(ClusterPB::VIRTUAL);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        std::cout << resp.DebugString() << std::endl;
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(resp.msg(), "invalid request instance_id or cluster not given");
    }

    //case: no cluster id
    {
        std::string mock_vcg_name = "virtual_cluster_name";
        std::string mock_vcg_id = "virtual_cluster_id";
        AlterClusterRequest req;
        req.mutable_cluster()->set_cluster_name(mock_vcg_name);
        req.mutable_cluster()->set_type(ClusterPB::VIRTUAL);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        std::cout << resp.DebugString() << std::endl;
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(resp.msg(), "invalid request instance_id or cluster not given");
    }

    //case: use ordinary cluster's args, public_endpoint, mysql_user_name, private_endpoint, cluster_status
    {
        std::string mock_vcg_name = "virtual_cluster_name";
        std::string mock_vcg_id = "virtual_cluster_id";
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_vcg_name);
        req.mutable_cluster()->set_cluster_id(mock_vcg_id);
        req.mutable_cluster()->set_type(ClusterPB::VIRTUAL);
        req.mutable_cluster()->set_public_endpoint("public_endpoint");
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        std::cout << resp.DebugString() << std::endl;
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(resp.msg(), "Inconsistent virtual cluster args");
    }

    //case: use ordinary cluster's args, nodes
    {
        std::string mock_vcg_name = "virtual_cluster_name";
        std::string mock_vcg_id = "virtual_cluster_id";
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_cluster_name(mock_vcg_name);
        req.mutable_cluster()->set_cluster_id(mock_vcg_id);
        req.mutable_cluster()->set_type(ClusterPB::VIRTUAL);
        auto node = req.mutable_cluster()->add_nodes();
        node->set_ip("127.0.0.1");
        node->set_heartbeat_port(9999);
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        std::cout << resp.DebugString() << std::endl;
        ASSERT_EQ(status_code, 400);
        ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(resp.msg(), "Inconsistent virtual cluster args");
    }

    // case: add a empty cluster
    {
        std::string mock_vcg_name = "virtual_cluster_name";
        std::string mock_vcg_id = "virtual_cluster_id";
        auto [status_code, resp] =
                add_cluster(ctx, mock_vcg_name, mock_vcg_id, ClusterPB::VIRTUAL, {}, nullptr);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);

        // test alter, add two subcg to empty cluster
        {
            add_cluster(ctx, "subcg_name1", "subcg_id1", ClusterPB::COMPUTE, {}, nullptr);
            add_cluster(ctx, "subcg_name2", "subcg_id2", ClusterPB::COMPUTE, {}, nullptr);
            ClusterPolicy policy;
            policy.set_active_cluster_name("subcg_name2");
            policy.add_standby_cluster_names("subcg_name1");
            const std::vector<std::string> cluster_names = {"subcg_name2", "subcg_name1"};
            auto [status_code, resp] = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name,
                                                             mock_vcg_id, cluster_names, &policy);
            ASSERT_EQ(status_code, 400);
            ASSERT_EQ(resp.code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(resp.msg(), "plz set cluster policy type, use virtual cluster policy");

            // just has cluster_names
            auto [status_code2, resp2] = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name,
                                                               mock_vcg_id, cluster_names, nullptr);
            ASSERT_EQ(status_code2, 400);
            ASSERT_EQ(resp2.code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(resp2.msg(),
                      "subcgs and policy must be Incoming at the same time or do not transmit at "
                      "the same time");

            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name("subcg_name2");
            policy.add_standby_cluster_names("subcg_name1");
            auto [status_code1, resp1] = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name,
                                                               mock_vcg_id, cluster_names, &policy);
            ASSERT_EQ(status_code1, 200);
            ASSERT_EQ(resp1.code(), MetaServiceCode::OK);
            auto ret = get_cluster_info(ctx, mock_vcg_name, mock_vcg_id);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).status.code(), MetaServiceCode::OK);
            ASSERT_TRUE(std::get<1>(ret).result.has_value());
            for (auto showrt : std::get<1>(ret).result->cluster_names()) {
                ASSERT_TRUE(std::find(cluster_names.begin(), cluster_names.end(), showrt) !=
                            cluster_names.end());
            }
            ASSERT_EQ("subcg_name2",
                      std::get<1>(ret).result->cluster_policy().active_cluster_name());
            ASSERT_EQ("subcg_name1",
                      std::get<1>(ret).result->cluster_policy().standby_cluster_names().at(0));
            // check default value
            ASSERT_EQ(3, std::get<1>(ret).result->cluster_policy().failover_failure_threshold());
            ASSERT_EQ(100,
                      std::get<1>(ret).result->cluster_policy().unhealthy_node_threshold_percent());
        }

        // case: disable alter vcluster status
        AlterClusterRequest req1;
        req1.set_instance_id(mock_instance);
        req1.mutable_cluster()->set_cluster_id(mock_vcg_id);
        req1.mutable_cluster()->set_cluster_status(ClusterStatus::SUSPENDED);
        req1.set_op(AlterClusterRequest::SET_CLUSTER_STATUS);
        auto [status_code1, resp1] =
                ctx.forward<MetaServiceResponseStatus>("set_cluster_status", req1);
        ASSERT_EQ(status_code1, 400);
        ASSERT_EQ(resp1.code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(resp1.msg(), "just support set COMPUTE cluster status");

        // rename vcluster
        AlterClusterRequest req2;
        req2.set_instance_id(mock_instance);
        req2.mutable_cluster()->set_cluster_id(mock_vcg_id);
        req2.mutable_cluster()->set_cluster_name("virtual_cluster_name");
        auto [status_code2, resp2] = ctx.forward<MetaServiceResponseStatus>("rename_cluster", req2);
        ASSERT_EQ(status_code2, 400);
        ASSERT_EQ(resp2.code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_TRUE(
                resp2.msg().starts_with("failed to rename cluster, a cluster with the same name "
                                        "already exists in this instance"));

        req2.mutable_cluster()->set_cluster_name("virtual_cluster_new_name");
        auto [status_code3, resp3] = ctx.forward<MetaServiceResponseStatus>("rename_cluster", req2);
        ASSERT_EQ(status_code3, 200);
        ASSERT_EQ(resp3.code(), MetaServiceCode::OK);
    }

    // case: add non-empty vcg failed
    {
        // has subcgs, not has policy
        std::string mock_vcg_name1 = "virtual_cluster_name_1";
        std::string mock_vcg_id1 = "virtual_cluster_id_1";
        auto ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                               {"not_exist_cluster_1"}, nullptr);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "subcgs and policy must be Incoming at the same time or do not transmit at the "
                  "same time");

        // 2 subcg, and not set active_cluster_name
        ClusterPolicy policy;
        policy.set_type(ClusterPolicy::ActiveStandby);
        policy.set_failover_failure_threshold(1);
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "active_cluster_name must not be empty and must be in cluster_names");

        // 2 subcg, seted active_cluster_name but not set standby_cluster_names
        policy.set_active_cluster_name("not_exist_cluster_1");
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "Inconsistent cluster policy: active_cluster_name must be set if "
                  "standby_cluster_names are present, and vice versa.");

        // 2 subcg, seted active_cluster_name, one standby_cluster_names, active_cluster_name not create before
        policy.add_standby_cluster_names("not_exist_cluster_2");
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "sub cluster not_exist_cluster_1 not been added in instance, plz add it before "
                  "create virtual cluster");

        // 2 subcg, seted active_cluster_name, but not_exist_cluster_3 not in subcgs
        policy.set_active_cluster_name("not_exist_cluster_3");
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "active_cluster_name must not be empty and must be in cluster_names");

        // 2 subcg, seted active_cluster_name, two standby_cluster_names, but subcgs not have not_exist_cluster_3
        policy.set_active_cluster_name("not_exist_cluster_1");
        policy.add_standby_cluster_names("not_exist_cluster_3");
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "standby_cluster_name not_exist_cluster_3 must be in cluster_names");

        // just support 2 subcg
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2", "not_exist_cluster_3"},
                          &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(), "Currently, just support two sub clusters");

        // just support 2 subcg
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"only_one_cluster"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(), "Currently, just support two sub clusters");

        // subcg regex cluster name
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"*_regex_err_cluster", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "cluster name *_regex_err_cluster does not match regex ^[a-zA-Z][a-zA-Z0-9_]*$");

        // subcgs and policy must be Incoming at the same time or do not transmit at the same time
        policy.Clear();
        policy.set_type(ClusterPolicy::ActiveStandby);
        policy.add_standby_cluster_names("not_exist_cluster_2");
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL, {}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "subcgs and policy must be Incoming at the same time or do not transmit at the "
                  "same time");

        // policy has standy but not have active
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "Inconsistent cluster policy: active_cluster_name must be set if "
                  "standby_cluster_names are present, and vice versa.");

        policy.set_active_cluster_name("not_exist_cluster_2");
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(), "active_cluster_name is same of standby_cluster_name");

        policy.set_active_cluster_name("not_exist_cluster_1");
        // failover_failure_threshold 0, failed
        policy.set_failover_failure_threshold(0);
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "failover_failure_threshold must be greater than 0 and less than max(int64)");

        // failover_failure_threshold -1, failed
        policy.set_failover_failure_threshold(-1);
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "failover_failure_threshold must be greater than 0 and less than max(int64)");

        policy.set_failover_failure_threshold(88);
        policy.set_unhealthy_node_threshold_percent(-1);
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "unhealthy_node_threshold_percent must be greater than 0 and less than or equal "
                  "to 100");

        policy.set_unhealthy_node_threshold_percent(0);
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "unhealthy_node_threshold_percent must be greater than 0 and less than or equal "
                  "to 100");

        policy.set_failover_failure_threshold(1);
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {"not_exist_cluster_1", "not_exist_cluster_2"}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "unhealthy_node_threshold_percent must be greater than 0 and less than or equal "
                  "to 100");
    }

    // case: add non-empty vcg succ
    {
        std::string mock_vcg_name1 = "virtual_cluster_name_1";
        std::string mock_vcg_id1 = "virtual_cluster_id_1";
        std::string mock_vcg_name2 = "virtual_cluster_name_2";
        std::string mock_vcg_id2 = "virtual_cluster_id_2";
        std::string mock_exist_cluster_name1 = "exist_cluster_name_1";
        std::string mock_exist_cluster_id1 = "exist_cluster_id_1";
        std::string mock_exist_cluster_name2 = "exist_cluster_name_2";
        std::string mock_exist_cluster_id2 = "exist_cluster_id_2";
        std::string mock_exist_cluster_name3 = "fe_exist_cluster_name_3";
        std::string mock_exist_cluster_id3 = "fe_exist_cluster_id_3";
        // add two exist cluster before
        auto ret = add_cluster(ctx, mock_exist_cluster_name1, mock_exist_cluster_id1,
                               ClusterPB::COMPUTE, {}, nullptr);
        ASSERT_EQ(std::get<0>(ret), 200);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

        ret = add_cluster(ctx, mock_exist_cluster_name2, mock_exist_cluster_id2, ClusterPB::COMPUTE,
                          {}, nullptr);
        ASSERT_EQ(std::get<0>(ret), 200);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

        AlterClusterRequest req_before_fe;
        req_before_fe.set_instance_id(mock_instance);
        req_before_fe.mutable_cluster()->set_type(ClusterPB::SQL);
        req_before_fe.mutable_cluster()->set_cluster_id(mock_exist_cluster_id3);
        req_before_fe.mutable_cluster()->set_cluster_name(mock_exist_cluster_name3);
        auto node = req_before_fe.mutable_cluster()->add_nodes();
        node->set_ip("127.0.0.1");
        node->set_edit_log_port(9990);
        node->set_node_type(NodeInfoPB::FE_MASTER);
        ret = ctx.forward<MetaServiceResponseStatus>("add_cluster", req_before_fe);
        ASSERT_EQ(std::get<1>(ret).msg(), "");
        ASSERT_EQ(std::get<0>(ret), 200);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

        // test normal succ
        ClusterPolicy policy;
        policy.set_type(ClusterPolicy::ActiveStandby);
        policy.set_active_cluster_name(mock_exist_cluster_name2);
        policy.add_standby_cluster_names(mock_exist_cluster_name1);
        ret = add_cluster(ctx, mock_vcg_name1, mock_vcg_id1, ClusterPB::VIRTUAL,
                          {mock_exist_cluster_name1, mock_exist_cluster_name2}, &policy);
        ASSERT_EQ(std::get<1>(ret).msg(), "");
        ASSERT_EQ(std::get<0>(ret), 200);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

        // one cluster just can be added by one vcg
        policy.Clear();
        policy.set_type(ClusterPolicy::ActiveStandby);
        policy.set_active_cluster_name(mock_exist_cluster_name1);
        policy.add_standby_cluster_names(mock_exist_cluster_name2);
        ret = add_cluster(ctx, mock_vcg_name2, mock_vcg_id2, ClusterPB::VIRTUAL,
                          {mock_exist_cluster_name1, mock_exist_cluster_name2}, &policy);
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "sub cluster exist_cluster_name_1 has been add by other "
                  "vcg=virtual_cluster_name_1");

        // case alter cluster
        // no instance_id or cloud_unique_id
        {
            ClusterPolicy policy;
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name(mock_exist_cluster_name1);
            policy.add_standby_cluster_names(mock_exist_cluster_name2);
            auto ret = alter_virtual_cluster(ctx, "", mock_vcg_name2, mock_vcg_id2,
                                             {mock_exist_cluster_name1, mock_exist_cluster_name2},
                                             &policy);
            ASSERT_EQ(std::get<0>(ret), 400);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(std::get<1>(ret).msg(), "invalid request instance_id or cluster not given");
        }

        // no cluster_id
        {
            ClusterPolicy policy;
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name(mock_exist_cluster_name1);
            policy.add_standby_cluster_names(mock_exist_cluster_name2);
            auto ret = alter_virtual_cluster(ctx, mock_instance, "", "",
                                             {mock_exist_cluster_name1, mock_exist_cluster_name2},
                                             &policy);
            ASSERT_EQ(std::get<0>(ret), 400);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(std::get<1>(ret).msg(), "missing cluster_id=");
        }

        {
            // current vcg (active:exist_cluster_name2 Standby:exist_cluster_name1)
            // alter to (active:exist_cluster_name1 Standby:exist_cluster_name_4)
            add_cluster(ctx, "exist_cluster_name_4", "exist_cluster_id_4", ClusterPB::COMPUTE, {},
                        nullptr);
            std::vector<std::string> cluster_names = {mock_exist_cluster_name1,
                                                      "exist_cluster_name_4"};
            auto ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                             cluster_names, nullptr);
            // not change policy, so alter failed
            ASSERT_EQ(std::get<0>(ret), 400);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(std::get<1>(ret).msg(),
                      "active_cluster_name must not be empty and must be in cluster_names");

            // cluster_names and policy modify together, succ
            ClusterPolicy policy;
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name(mock_exist_cluster_name1);
            policy.add_standby_cluster_names("exist_cluster_name_4");
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, &policy);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            // change back to vcg (active:exist_cluster_name2 Standby:exist_cluster_name1)
            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name(mock_exist_cluster_name2);
            policy.add_standby_cluster_names(mock_exist_cluster_name1);
            cluster_names = {mock_exist_cluster_name1, mock_exist_cluster_name2};
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, &policy);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            // current vcg (active:exist_cluster_name2 Standby:exist_cluster_name1)
            // alter to (active:exist_cluster_name_5 Standby:exist_cluster_name_4)
            policy.Clear();
            add_cluster(ctx, "exist_cluster_name_5", "exist_cluster_id_5", ClusterPB::COMPUTE, {},
                        nullptr);
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name("exist_cluster_name_5");
            policy.add_standby_cluster_names("exist_cluster_name_4");
            cluster_names = {"exist_cluster_name_4", "exist_cluster_name_5"};
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, &policy);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            // active A standby B -> active A standby B -> active B standby A
            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name("exist_cluster_name_4");
            policy.add_standby_cluster_names("exist_cluster_name_5");
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, &policy);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            auto show_rest = get_cluster_info(ctx, mock_vcg_name1, mock_vcg_id1);
            ASSERT_EQ(std::get<0>(show_rest), 200);
            ASSERT_EQ(std::get<1>(show_rest).status.code(), MetaServiceCode::OK);
            ASSERT_TRUE(std::get<1>(show_rest).result.has_value());
            for (auto showrt : std::get<1>(show_rest).result->cluster_names()) {
                ASSERT_TRUE(std::find(cluster_names.begin(), cluster_names.end(), showrt) !=
                            cluster_names.end());
            }
            ASSERT_EQ("exist_cluster_name_4",
                      std::get<1>(show_rest).result->cluster_policy().active_cluster_name());
            ASSERT_EQ(
                    "exist_cluster_name_5",
                    std::get<1>(show_rest).result->cluster_policy().standby_cluster_names().at(0));
            // check default value
            ASSERT_EQ(3,
                      std::get<1>(show_rest).result->cluster_policy().failover_failure_threshold());
            ASSERT_EQ(100, std::get<1>(show_rest)
                                   .result->cluster_policy()
                                   .unhealthy_node_threshold_percent());
            ASSERT_EQ(ClusterPolicy::ActiveStandby,
                      std::get<1>(show_rest).result->cluster_policy().type());

            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name("exist_cluster_name_5");
            policy.add_standby_cluster_names("exist_cluster_name_4");
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, &policy);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            // active A standby B -> active B standby C -> active C standby D -> active D standby A
            cluster_names = {mock_exist_cluster_name2, "exist_cluster_name_4"};
            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name(mock_exist_cluster_name2);
            policy.add_standby_cluster_names("exist_cluster_name_4");
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, &policy);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            cluster_names = {"exist_cluster_name_4", "exist_cluster_name_5"};
            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name("exist_cluster_name_4");
            policy.add_standby_cluster_names("exist_cluster_name_5");
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, &policy);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            cluster_names = {"exist_cluster_name_5", "exist_cluster_name_1"};
            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name("exist_cluster_name_5");
            policy.add_standby_cluster_names("exist_cluster_name_1");
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, &policy);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            // failed, not_exist_cluster_name_6
            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name("exist_cluster_name_5");
            policy.add_standby_cluster_names("not_exist_cluster_name_6");
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, &policy);
            ASSERT_EQ(std::get<0>(ret), 400);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(std::get<1>(ret).msg(),
                      "sub cluster not_exist_cluster_name_6 not been added in instance, plz "
                      "add it before create virtual cluster");

            cluster_names = {"exist_cluster_name_5", "not_exist_cluster_name_6"};
            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name("exist_cluster_name_5");
            policy.add_standby_cluster_names("not_exist_cluster_name_6");
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, &policy);
            ASSERT_EQ(std::get<0>(ret), 400);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(std::get<1>(ret).msg(),
                      "sub cluster not_exist_cluster_name_6 not been added in instance, plz "
                      "add it before create virtual cluster");

            show_rest = get_cluster_info(ctx, mock_vcg_name1, mock_vcg_id1);
            ASSERT_EQ(std::get<0>(show_rest), 200);
            ASSERT_EQ(std::get<1>(show_rest).status.code(), MetaServiceCode::OK);
            ASSERT_TRUE(std::get<1>(show_rest).result.has_value());
            cluster_names = {"exist_cluster_name_5", "exist_cluster_name_1"};
            for (auto showrt : std::get<1>(show_rest).result->cluster_names()) {
                ASSERT_TRUE(std::find(cluster_names.begin(), cluster_names.end(), showrt) !=
                            cluster_names.end());
            }
            ASSERT_EQ("exist_cluster_name_5",
                      std::get<1>(show_rest).result->cluster_policy().active_cluster_name());
            ASSERT_EQ(
                    "exist_cluster_name_1",
                    std::get<1>(show_rest).result->cluster_policy().standby_cluster_names().at(0));
            // check default value
            ASSERT_EQ(3,
                      std::get<1>(show_rest).result->cluster_policy().failover_failure_threshold());
            ASSERT_EQ(100, std::get<1>(show_rest)
                                   .result->cluster_policy()
                                   .unhealthy_node_threshold_percent());
            ASSERT_EQ(ClusterPolicy::ActiveStandby,
                      std::get<1>(show_rest).result->cluster_policy().type());

            // just change active,standby, so can not send cluster_names
            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name("exist_cluster_name_1");
            policy.add_standby_cluster_names("exist_cluster_name_5");
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1, {},
                                        &policy);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            // just change cluster_names, so can not send policy, nothing changed
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, nullptr);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            // just alter active_cluster_name, failed
            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_active_cluster_name("exist_cluster_name_5");
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1, {},
                                        &policy);
            ASSERT_EQ(std::get<0>(ret), 400);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(std::get<1>(ret).msg(),
                      "active_cluster_name is same of standby_cluster_name");

            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.add_standby_cluster_names("exist_cluster_name_1");
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1, {},
                                        &policy);
            ASSERT_EQ(std::get<0>(ret), 400);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(std::get<1>(ret).msg(),
                      "active_cluster_name is same of standby_cluster_name");

            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1, {},
                                        &policy);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_failover_failure_threshold(55);
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1, {},
                                        &policy);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            show_rest = get_cluster_info(ctx, mock_vcg_name1, mock_vcg_id1);
            ASSERT_EQ(std::get<0>(show_rest), 200);
            ASSERT_EQ(std::get<1>(show_rest).status.code(), MetaServiceCode::OK);
            // check default value
            ASSERT_EQ(55,
                      std::get<1>(show_rest).result->cluster_policy().failover_failure_threshold());

            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_unhealthy_node_threshold_percent(66);
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1, {},
                                        &policy);
            ASSERT_EQ(std::get<0>(ret), 200);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

            show_rest = get_cluster_info(ctx, mock_vcg_name1, mock_vcg_id1);
            ASSERT_EQ(std::get<0>(show_rest), 200);
            ASSERT_EQ(std::get<1>(show_rest).status.code(), MetaServiceCode::OK);
            // check default value
            ASSERT_EQ(66, std::get<1>(show_rest)
                                  .result->cluster_policy()
                                  .unhealthy_node_threshold_percent());

            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_unhealthy_node_threshold_percent(200);
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1, {},
                                        &policy);
            ASSERT_EQ(std::get<0>(ret), 400);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(std::get<1>(ret).msg(),
                      "unhealthy_node_threshold_percent must be greater than 0 and less than "
                      "or equal to 100");

            policy.Clear();
            policy.set_type(ClusterPolicy::ActiveStandby);
            policy.set_unhealthy_node_threshold_percent(88);
            cluster_names = {"exist_cluster_name_5"};
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, &policy);
            ASSERT_EQ(std::get<0>(ret), 400);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(std::get<1>(ret).msg(), "Currently, just support two sub clusters");

            cluster_names = {"exist_cluster_name_5", "exist_cluster_name_4",
                             mock_exist_cluster_name1};
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1,
                                        cluster_names, &policy);
            ASSERT_EQ(std::get<0>(ret), 400);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(std::get<1>(ret).msg(), "Currently, just support two sub clusters");

            policy.Clear();
            policy.set_active_cluster_name("exist_cluster_name_1");
            policy.add_standby_cluster_names("exist_cluster_name_1");
            ret = alter_virtual_cluster(ctx, mock_instance, mock_vcg_name1, mock_vcg_id1, {},
                                        &policy);
            ASSERT_EQ(std::get<0>(ret), 400);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(std::get<1>(ret).msg(),
                      "active_cluster_name is same of standby_cluster_name");
        }

        {
            // sub cg must be COMPUTE cluster
            AlterClusterRequest req2;
            req2.set_instance_id(mock_instance);
            req2.mutable_cluster()->set_cluster_name("vcg_name_3");
            req2.mutable_cluster()->set_cluster_id("vcg_id_3");
            req2.mutable_cluster()->set_type(ClusterPB::VIRTUAL);
            req2.mutable_cluster()->add_cluster_names("exist_cluster_name_4");
            req2.mutable_cluster()->add_cluster_names(mock_exist_cluster_name3);
            auto* policy2 = req2.mutable_cluster()->mutable_cluster_policy();
            policy2->set_type(ClusterPolicy::ActiveStandby);
            policy2->set_active_cluster_name(mock_exist_cluster_name3);
            policy2->add_standby_cluster_names("exist_cluster_name_4");
            ret = ctx.forward<MetaServiceResponseStatus>("add_cluster", req2);
            ASSERT_EQ(std::get<0>(ret), 400);
            ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
            ASSERT_EQ(std::get<1>(ret).msg(),
                      "sub cluster fe_exist_cluster_name_3 's type must be eq COMPUTE");
        }

        // drop subcg failed, if not drop vcg
        ret = drop_cluster(ctx, "", mock_exist_cluster_id1);
        // ASSERT_EQ(std::get<0>(ret), 409);
        // ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::ALREADY_EXISTED);
        ASSERT_EQ(std::get<1>(ret).msg(),
                  "failed to drop cluster, this cluster owned by virtual "
                  "cluster=virtual_cluster_name_1 if you want drop this cluster, please drop "
                  "virtual "
                  "cluster=virtual_cluster_name_1 firstly");

        // case drop cluster no cluster_id
        ret = drop_cluster(ctx, mock_vcg_name1, "");
        ASSERT_EQ(std::get<0>(ret), 400);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::INVALID_ARGUMENT);
        ASSERT_EQ(std::get<1>(ret).msg(), "missing cluster_id=");

        // case drop cluster
        ret = drop_cluster(ctx, mock_vcg_name1, mock_vcg_id1);
        ASSERT_EQ(std::get<0>(ret), 200);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

        // ATTN: meta_service_http_test.cpp:203 check, parse json err, but I don't want change pb add errcode
        // drop_cluster(ctx, mock_vcg_name1, mock_vcg_id1);

        // after drop vcg, cg can be drop
        ret = drop_cluster(ctx, "", mock_exist_cluster_id1);
        ASSERT_EQ(std::get<0>(ret), 200);
        ASSERT_EQ(std::get<1>(ret).code(), MetaServiceCode::OK);

        // ret = drop_cluster(ctx, "", "not_exist_cg_id_1");
        // ATTN: meta_service_http_test.cpp:203 check, parse json err, but I don't want change pb add errcode
        // std::tuple<int, Response> doris::cloud::HttpContext::forward(std::string_view, const Request &) [Response = doris::cloud::MetaServiceResponseStatus, Request = doris::cloud::AlterClusterRequest] Parse JSON: INVALID_ARGUMENT:(code): invalid value "NOT_FOUND" for type type.googleapis.com/doris.cloud.MetaServiceCode, body: {
        // "code": "NOT_FOUND",
        // "msg": "failed to find cluster to drop, instance_id=test_instance cluster_id=not_exist_cg_id_1 cluster_name="
        // }

    } // namespace doris::cloud
}
} // namespace doris::cloud
