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

#include "service/http/action/stream_load.h"

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Status_types.h>
#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <initializer_list>
#include <memory>
#include <string_view>
#include <utility>

#include "cloud/config.h"
#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "event2/buffer.h"
#include "event2/event.h"
#include "event2/http.h"
#include "event2/http_struct.h"
#include "evhttp.h"
#include "load/group_commit/wal/wal_manager.h"
#include "load/stream_load/new_load_stream_mgr.h"
#include "load/stream_load/stream_load_context.h"
#include "load/stream_load/stream_load_executor.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "service/http/action/http_stream.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_common.h"
#include "service/http/http_handler.h"
#include "service/http/http_handler_with_auth.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/utils.h"
#include "util/defer_op.h"

namespace doris {

class StreamLoadTest : public testing::Test {
public:
    StreamLoadTest() = default;
    virtual ~StreamLoadTest() = default;
    void SetUp() override {}
    void TearDown() override { ExecEnv::GetInstance()->clear_wal_mgr(); }
};

void http_request_done_cb(struct evhttp_request* req, void* arg) {
    event_base_loopbreak((struct event_base*)arg);
}

extern TStreamLoadPutResult k_stream_load_put_result;

TEST_F(StreamLoadTest, CloudMow2PCRequiresFECapabilityInBothHandlers) {
    auto* env = ExecEnv::GetInstance();
    const auto old_deploy_mode = config::deploy_mode;
    auto* old_cluster = env->cluster_info();
    auto old_stream_mgr = std::move(env->_new_load_stream_mgr);
    auto old_executor = std::move(env->_stream_load_executor);
    const auto old_put_result = k_stream_load_put_result;
    ClusterInfo cluster;
    auto* sync = SyncPoint::get_instance();
    Defer restore {[&] {
        sync->disable_processing();
        k_stream_load_put_result = old_put_result;
        env->set_stream_load_executor(std::move(old_executor));
        env->set_new_load_stream_mgr(std::move(old_stream_mgr));
        env->set_cluster_info(old_cluster);
        config::deploy_mode = old_deploy_mode;
    }};
    config::deploy_mode = "cloud";
    env->set_cluster_info(&cluster);
    env->set_new_load_stream_mgr(NewLoadStreamMgr::create_unique());
    env->set_stream_load_executor(std::make_unique<StreamLoadExecutor>(env));

    SyncPoint::CallbackGuard put_guard;
    sync->set_call_back(
            "HttpStreamAction::process_put.before_rpc",
            [](auto&& args) {
                *try_any_cast<TStreamLoadPutResult*>(args[0]) = k_stream_load_put_result;
                *try_any_cast_ret<Status>(args) = {Status::OK(), true};
            },
            &put_guard);
    sync->enable_processing();

    StreamLoadAction stream_load(env);
    HttpStreamAction http_stream(env);
    std::unique_ptr<evhttp_request, decltype(&evhttp_request_free)> ev_request(
            evhttp_request_new(nullptr, nullptr), evhttp_request_free);
    HttpRequest request(ev_request.get());

    for (int capability : {-1, 0, 1}) {
        SCOPED_TRACE(capability);
        k_stream_load_put_result = TStreamLoadPutResult();
        k_stream_load_put_result.status.__set_status_code(TStatusCode::OK);
        TPipelineFragmentParams plan;
        plan.__set_is_mow_table(true);
        k_stream_load_put_result.__set_pipeline_params(plan);
        if (capability != -1) {
            k_stream_load_put_result.__set_supports_cloud_mow_2pc(capability == 1);
        }

        for (bool use_http_stream : {false, true}) {
            SCOPED_TRACE(use_http_stream);
            auto ctx = std::make_shared<StreamLoadContext>(env);
            ctx->two_phase_commit = true;
            ctx->format = TFileFormatType::FORMAT_CSV_PLAIN;
            auto status = use_http_stream ? http_stream.process_put(&request, ctx)
                                          : stream_load._process_put(&request, ctx);
            env->new_load_stream_mgr()->remove(ctx->id);
            const auto& sink = ctx->put_result.pipeline_params.fragment.output_sink.olap_table_sink;
            if (capability == 1) {
                EXPECT_TRUE(status.ok()) << status;
                EXPECT_TRUE(sink.__isset.is_cloud_mow_2pc && sink.is_cloud_mow_2pc);
                EXPECT_EQ(ctx->load_status_future.wait_for(std::chrono::seconds(0)),
                          std::future_status::ready);
            } else {
                EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
                EXPECT_NE(status.msg().find("upgrade FE"), std::string_view::npos);
                EXPECT_FALSE(sink.__isset.is_cloud_mow_2pc);
                EXPECT_EQ(ctx->load_status_future.wait_for(std::chrono::seconds(0)),
                          std::future_status::timeout);
            }
        }
    }
}

TEST_F(StreamLoadTest, TestHeader) {
    // 1G
    auto wal_mgr = WalManager::create_unique(ExecEnv::GetInstance(), config::group_commit_wal_path);
    static_cast<void>(wal_mgr->_wal_dirs_info->add("test_path_1", 1000, 0, 0));
    static_cast<void>(wal_mgr->_wal_dirs_info->add("test_path_2", 10000, 0, 0));
    static_cast<void>(wal_mgr->_wal_dirs_info->add("test_path_3", 100000, 0, 0));
    ExecEnv::GetInstance()->set_wal_mgr(std::move(wal_mgr));
    // 1. empty info
    {
        auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        EXPECT_EQ(req.header(HttpHeaders::CONTENT_LENGTH).empty(), true);
        evhttp_request_free(evhttp_req);
    }

    // 2. chunked stream load whih group commit
    {
        //struct event_base* base = nullptr;
        char uri[] = "Http://127.0.0.1/test.txt";
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        evhttp_req->type = EVHTTP_REQ_GET;
        evhttp_req->uri = uri;
        evhttp_req->uri_elems = evhttp_uri_parse(uri);
        evhttp_add_header(evhttp_req->input_headers, HTTP_GROUP_COMMIT.c_str(), "true");
        HttpRequest req(evhttp_req);
        req.init_from_evhttp();
        EXPECT_EQ(req.header(HttpHeaders::CONTENT_LENGTH).empty(), true);
        evhttp_uri_free(evhttp_req->uri_elems);
        evhttp_req->uri = nullptr;
        evhttp_req->uri_elems = nullptr;
        evhttp_request_free(evhttp_req);
    }

    // 3. small stream load whih group commit
    {
        char uri[] = "Http://127.0.0.1/test.txt";
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        evhttp_req->type = EVHTTP_REQ_GET;
        evhttp_req->uri = uri;
        evhttp_req->uri_elems = evhttp_uri_parse(uri);
        evhttp_add_header(evhttp_req->input_headers, HTTP_GROUP_COMMIT.c_str(), "true");
        evhttp_add_header(evhttp_req->input_headers, HttpHeaders::CONTENT_LENGTH, "20000");
        HttpRequest req(evhttp_req);
        req.init_from_evhttp();
        EXPECT_EQ(req.header(HttpHeaders::CONTENT_LENGTH), "20000");
        EXPECT_EQ(load_size_smaller_than_wal_limit(20000), true);
        evhttp_uri_free(evhttp_req->uri_elems);
        evhttp_req->uri = nullptr;
        evhttp_req->uri_elems = nullptr;
        evhttp_request_free(evhttp_req);
    }

    // 4. large stream load whih group commit
    {
        char uri[] = "Http://127.0.0.1/test.txt";
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        evhttp_req->type = EVHTTP_REQ_GET;
        evhttp_req->uri = uri;
        evhttp_req->uri_elems = evhttp_uri_parse(uri);
        evhttp_add_header(evhttp_req->input_headers, HTTP_GROUP_COMMIT.c_str(), "true");
        evhttp_add_header(evhttp_req->input_headers, HttpHeaders::CONTENT_LENGTH, "1073741824");
        HttpRequest req(evhttp_req);
        req.init_from_evhttp();
        EXPECT_EQ(req.header(HttpHeaders::CONTENT_LENGTH), "1073741824");
        EXPECT_EQ(load_size_smaller_than_wal_limit(1073741824), false);
        evhttp_uri_free(evhttp_req->uri_elems);
        evhttp_req->uri = nullptr;
        evhttp_req->uri_elems = nullptr;
        evhttp_request_free(evhttp_req);
    }
}

TEST_F(StreamLoadTest, JsonBodySizeLimitPlusOneErrorIncludesExactBytes) {
    const auto original_json_max_mb = config::streaming_load_json_max_mb;
    Defer restore_json_max_mb {
            [original_json_max_mb] { config::streaming_load_json_max_mb = original_json_max_mb; }};
    config::streaming_load_json_max_mb = 100;

    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest req(evhttp_req);
    req.set_header(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    req.set_header(HTTP_FORMAT_KEY, "json");
    req.set_header(HttpHeaders::CONTENT_LENGTH, "104857601");

    StreamLoadAction action(nullptr);
    auto ctx = std::make_shared<StreamLoadContext>(nullptr);
    auto status = action._on_header(&req, ctx);

    EXPECT_TRUE(status.is<ErrorCode::EXCEEDED_LIMIT>());
    EXPECT_EQ(status.to_string_no_stack(),
              "[E-217]json body size 104857601 bytes (100.00 MiB) exceeds the limit of 104857600 "
              "bytes (100 MiB) set by BE's conf streaming_load_json_max_mb. Increase it if you "
              "are sure this load is reasonable");
    evhttp_request_free(evhttp_req);
}

TEST_F(StreamLoadTest, CsvBodySizeLimitPlusOneErrorIncludesExactBytes) {
    const auto original_max_mb = config::streaming_load_max_mb;
    Defer restore_max_mb {[original_max_mb] { config::streaming_load_max_mb = original_max_mb; }};
    config::streaming_load_max_mb = 100;

    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest req(evhttp_req);
    req.set_header(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    req.set_header(HTTP_FORMAT_KEY, "csv");
    req.set_header(HttpHeaders::CONTENT_LENGTH, "104857601");

    StreamLoadAction action(nullptr);
    auto ctx = std::make_shared<StreamLoadContext>(nullptr);
    auto status = action._on_header(&req, ctx);

    EXPECT_TRUE(status.is<ErrorCode::EXCEEDED_LIMIT>());
    EXPECT_EQ(status.to_string_no_stack(),
              "[E-217]body size 104857601 bytes (100.00 MiB) exceeds the limit of 104857600 bytes "
              "(100 MiB) set by BE's conf streaming_load_max_mb. Increase it if you are sure this "
              "load is reasonable");
    evhttp_request_free(evhttp_req);
}

TEST_F(StreamLoadTest, HttpStreamBodySizeLimitPlusOneErrorIncludesExactBytes) {
    const auto original_max_mb = config::streaming_load_max_mb;
    Defer restore_max_mb {[original_max_mb] { config::streaming_load_max_mb = original_max_mb; }};
    config::streaming_load_max_mb = 100;

    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest req(evhttp_req);
    req.set_header(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    req.set_header(HttpHeaders::CONTENT_LENGTH, "104857601");

    HttpStreamAction action(nullptr);
    auto ctx = std::make_shared<StreamLoadContext>(nullptr);
    auto status = action._on_header(&req, ctx);

    EXPECT_TRUE(status.is<ErrorCode::EXCEEDED_LIMIT>());
    EXPECT_EQ(status.to_string_no_stack(),
              "[E-217]body size 104857601 bytes (100.00 MiB) exceeds the limit of 104857600 bytes "
              "(100 MiB) set by BE config `streaming_load_max_mb`. Increase it if you are sure "
              "this load is reasonable");
    evhttp_request_free(evhttp_req);
}

// A malformed Content-Length used to escape as an exception from the libevent header
// callback and take the whole BE process down.
TEST_F(StreamLoadTest, HttpStreamMalformedContentLengthIsRejected) {
    for (const auto* content_length : {"not_a_number", "9223372036854775808"}) {
        auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        req.set_header(HTTP_GROUP_COMMIT, "async_mode");
        req.set_header(HttpHeaders::CONTENT_LENGTH, content_length);

        HttpStreamAction action(nullptr);
        auto ctx = std::make_shared<StreamLoadContext>(nullptr);
        auto status = action._handle_group_commit(&req, ctx);

        EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << content_length;
        evhttp_request_free(evhttp_req);
    }
}

TEST_F(StreamLoadTest, HttpStreamMalformedWalIdIsRejected) {
    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest req(evhttp_req);
    req.set_header(HTTP_WAL_ID_KY, "not_a_number");

    HttpStreamAction action(nullptr);
    auto ctx = std::make_shared<StreamLoadContext>(nullptr);
    req.set_handler(&action);
    req.set_handler_ctx(ctx);

    action.on_chunk_data(&req);

    EXPECT_TRUE(ctx->status.is<ErrorCode::INVALID_ARGUMENT>());
    // ctx carries no ExecEnv, so detach it before ~HttpRequest calls free_handler_ctx()
    req.set_handler_ctx(nullptr);
    evhttp_request_free(evhttp_req);
}
} // namespace doris
