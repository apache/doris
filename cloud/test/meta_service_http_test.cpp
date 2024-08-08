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
#include <optional>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_service.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"
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

    {
        AlterClusterRequest req;
        req.set_instance_id(mock_instance);
        req.mutable_cluster()->set_type(ClusterPB::COMPUTE);
        req.mutable_cluster()->set_cluster_id(mock_cluster_id + "1");
        auto [status_code, resp] = ctx.forward<MetaServiceResponseStatus>("add_cluster", req);
        ASSERT_EQ(status_code, 200);
        ASSERT_EQ(resp.code(), MetaServiceCode::OK);
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
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
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
    EXPECT_EQ(*(int64_t*)data_size_val.data(), 20000);
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
    EXPECT_EQ(res.tablet_stats(0).data_size(), 40000);
    EXPECT_EQ(res.tablet_stats(0).num_rows(), 400);
    EXPECT_EQ(res.tablet_stats(0).num_rowsets(), 5);
    EXPECT_EQ(res.tablet_stats(0).num_segments(), 4);
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

} // namespace doris::cloud
