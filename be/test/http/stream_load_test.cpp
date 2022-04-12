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

#include "http/action/stream_load.h"

#include <event2/http.h>
#include <event2/http_struct.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "exec/schema_scanner/schema_helper.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/thread_resource_mgr.h"
#include "util/brpc_client_cache.h"
#include "util/cpu_info.h"

struct mg_connection;

namespace doris {

std::string k_response_str;

// Send Unauthorized status with basic challenge
void HttpChannel::send_basic_challenge(HttpRequest* req, const std::string& realm) {}

void HttpChannel::send_error(HttpRequest* request, HttpStatus status) {}

void HttpChannel::send_reply(HttpRequest* request, HttpStatus status) {}

void HttpChannel::send_reply(HttpRequest* request, HttpStatus status, const std::string& content) {
    k_response_str = content;
}

void HttpChannel::send_file(HttpRequest* request, int fd, size_t off, size_t size) {}

extern TLoadTxnBeginResult k_stream_load_begin_result;
extern TLoadTxnCommitResult k_stream_load_commit_result;
extern TLoadTxnRollbackResult k_stream_load_rollback_result;
extern TStreamLoadPutResult k_stream_load_put_result;
extern Status k_stream_load_plan_status;

class StreamLoadActionTest : public testing::Test {
public:
    StreamLoadActionTest() {}
    virtual ~StreamLoadActionTest() {}
    void SetUp() override {
        k_stream_load_begin_result = TLoadTxnBeginResult();
        k_stream_load_commit_result = TLoadTxnCommitResult();
        k_stream_load_rollback_result = TLoadTxnRollbackResult();
        k_stream_load_put_result = TStreamLoadPutResult();
        k_stream_load_plan_status = Status::OK();
        k_response_str = "";
        config::streaming_load_max_mb = 1;

        _env._thread_mgr = new ThreadResourceMgr();
        _env._master_info = new TMasterInfo();
        _env._load_stream_mgr = new LoadStreamMgr();
        _env._internal_client_cache = new BrpcClientCache<PBackendService_Stub>();
        _env._function_client_cache = new BrpcClientCache<PFunctionService_Stub>();
        _env._stream_load_executor = new StreamLoadExecutor(&_env);

        _evhttp_req = evhttp_request_new(nullptr, nullptr);
    }
    void TearDown() override {
        delete _env._internal_client_cache;
        _env._internal_client_cache = nullptr;
        delete _env._function_client_cache;
        _env._function_client_cache = nullptr;
        delete _env._load_stream_mgr;
        _env._load_stream_mgr = nullptr;
        delete _env._master_info;
        _env._master_info = nullptr;
        delete _env._thread_mgr;
        _env._thread_mgr = nullptr;
        delete _env._stream_load_executor;
        _env._stream_load_executor = nullptr;

        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }
    }

private:
    ExecEnv _env;
    evhttp_request* _evhttp_req = nullptr;
};

TEST_F(StreamLoadActionTest, no_auth) {
    StreamLoadAction action(&_env);

    HttpRequest request(_evhttp_req);
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    EXPECT_STREQ("Fail", doc["Status"].GetString());
}

TEST_F(StreamLoadActionTest, normal) {
    StreamLoadAction action(&_env);

    HttpRequest request(_evhttp_req);

    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    request._ev_req = &ev_req;

    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "0");
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    EXPECT_STREQ("Success", doc["Status"].GetString());
}

TEST_F(StreamLoadActionTest, put_fail) {
    StreamLoadAction action(&_env);

    HttpRequest request(_evhttp_req);

    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    request._ev_req = &ev_req;

    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
    Status status = Status::InternalError("TestFail");
    status.to_thrift(&k_stream_load_put_result.status);
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    EXPECT_STREQ("Fail", doc["Status"].GetString());
}

TEST_F(StreamLoadActionTest, commit_fail) {
    StreamLoadAction action(&_env);

    HttpRequest request(_evhttp_req);
    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    request._ev_req = &ev_req;
    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
    Status status = Status::InternalError("TestFail");
    status.to_thrift(&k_stream_load_commit_result.status);
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    EXPECT_STREQ("Fail", doc["Status"].GetString());
}

TEST_F(StreamLoadActionTest, begin_fail) {
    StreamLoadAction action(&_env);

    HttpRequest request(_evhttp_req);
    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    request._ev_req = &ev_req;
    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
    Status status = Status::InternalError("TestFail");
    status.to_thrift(&k_stream_load_begin_result.status);
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    EXPECT_STREQ("Fail", doc["Status"].GetString());
}

#if 0
TEST_F(StreamLoadActionTest, receive_failed) {
    StreamLoadAction action(&_env);

    HttpRequest request(_evhttp_req);
    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::TRANSFER_ENCODING, "chunked");
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    EXPECT_STREQ("Fail", doc["Status"].GetString());
}
#endif

TEST_F(StreamLoadActionTest, plan_fail) {
    StreamLoadAction action(&_env);

    HttpRequest request(_evhttp_req);
    struct evhttp_request ev_req;
    ev_req.remote_host = nullptr;
    request._ev_req = &ev_req;
    request._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
    request._headers.emplace(HttpHeaders::CONTENT_LENGTH, "16");
    k_stream_load_plan_status = Status::InternalError("TestFail");
    request.set_handler(&action);
    action.on_header(&request);
    action.handle(&request);

    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    EXPECT_STREQ("Fail", doc["Status"].GetString());
}

} // namespace doris
