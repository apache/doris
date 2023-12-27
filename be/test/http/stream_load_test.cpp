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

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "event2/buffer.h"
#include "event2/event.h"
#include "event2/http.h"
#include "event2/http_struct.h"
#include "evhttp.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_common.h"
#include "http/http_handler.h"
#include "http/http_handler_with_auth.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "olap/wal_manager.h"
#include "runtime/exec_env.h"

namespace doris {

class StreamLoadTest : public testing::Test {
public:
    StreamLoadTest() = default;
    virtual ~StreamLoadTest() = default;
    void SetUp() override {}
    void TearDown() override {}
};

void http_request_done_cb(struct evhttp_request* req, void* arg) {
    event_base_loopbreak((struct event_base*)arg);
}

TEST_F(StreamLoadTest, TestHeader) {
    // 1G
    auto wal_mgr = WalManager::create_shared(ExecEnv::GetInstance(), config::group_commit_wal_path);
    static_cast<void>(wal_mgr->_wal_dirs_info->add("test_path_1", 1000, 0, 0));
    static_cast<void>(wal_mgr->_wal_dirs_info->add("test_path_2", 10000, 0, 0));
    static_cast<void>(wal_mgr->_wal_dirs_info->add("test_path_3", 100000, 0, 0));
    ExecEnv::GetInstance()->set_wal_mgr(wal_mgr);
    // 1. empty info
    {
        auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        EXPECT_EQ(load_size_smaller_than_wal_limit(&req), false);
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
        EXPECT_EQ(load_size_smaller_than_wal_limit(&req), false);
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
        EXPECT_EQ(load_size_smaller_than_wal_limit(&req), true);
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
        EXPECT_EQ(load_size_smaller_than_wal_limit(&req), false);
        evhttp_uri_free(evhttp_req->uri_elems);
        evhttp_req->uri = nullptr;
        evhttp_req->uri_elems = nullptr;
        evhttp_request_free(evhttp_req);
    }
}
} // namespace doris