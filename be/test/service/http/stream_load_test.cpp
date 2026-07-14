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
#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "event2/buffer.h"
#include "event2/event.h"
#include "event2/http.h"
#include "event2/http_struct.h"
#include "evhttp.h"
#include "load/group_commit/wal/wal_manager.h"
#include "runtime/exec_env.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_common.h"
#include "service/http/http_handler.h"
#include "service/http/http_handler_with_auth.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/utils.h"

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

TEST_F(StreamLoadTest, TestSetStreamLoadComputeGroup) {
    HttpRequest req(nullptr);
    req.set_header(HTTP_COMPUTE_GROUP, "compute_group_1");
    req.set_header(HTTP_CLOUD_CLUSTER, "cloud_cluster_1");
    TStreamLoadPutRequest request;

    set_stream_load_compute_group(req, request);

    EXPECT_TRUE(request.__isset.cloud_cluster);
    EXPECT_EQ(request.cloud_cluster, "compute_group_1");

    HttpRequest legacy_req(nullptr);
    legacy_req.set_header(HTTP_CLOUD_CLUSTER, "cloud_cluster_1");
    TStreamLoadPutRequest legacy_request;

    set_stream_load_compute_group(legacy_req, legacy_request);

    EXPECT_TRUE(legacy_request.__isset.cloud_cluster);
    EXPECT_EQ(legacy_request.cloud_cluster, "cloud_cluster_1");
}
} // namespace doris
