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

#include "http/http_client.h"

#include <gtest/gtest.h>

#include "boost/algorithm/string.hpp"
#include "common/logging.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_request.h"

namespace doris {

class HttpClientTestSimpleGetHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string user;
        std::string passwd;
        if (!parse_basic_auth(*req, &user, &passwd) || user != "test1") {
            HttpChannel::send_basic_challenge(req, "abc");
            return;
        }
        req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");
        if (req->method() == HttpMethod::HEAD) {
            req->add_output_header(HttpHeaders::CONTENT_LENGTH, std::to_string(5).c_str());
            HttpChannel::send_reply(req);
        } else {
            std::string response = "test1";
            HttpChannel::send_reply(req, response);
        }
    }
};

class HttpClientTestSimplePostHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string user;
        std::string passwd;
        if (!parse_basic_auth(*req, &user, &passwd) || user != "test1") {
            HttpChannel::send_basic_challenge(req, "abc");
            return;
        }
        if (req->method() == HttpMethod::POST) {
            std::string post_body = req->get_request_body();
            if (!post_body.empty()) {
                HttpChannel::send_reply(req, post_body);
            } else {
                HttpChannel::send_reply(req, "empty");
            }
        }
    }
};

static HttpClientTestSimpleGetHandler s_simple_get_handler = HttpClientTestSimpleGetHandler();
static HttpClientTestSimplePostHandler s_simple_post_handler = HttpClientTestSimplePostHandler();
static EvHttpServer* s_server = nullptr;
static int real_port = 0;
static std::string hostname = "";

class HttpClientTest : public testing::Test {
public:
    HttpClientTest() {}
    ~HttpClientTest() override {}

    static void SetUpTestCase() {
        s_server = new EvHttpServer(0);
        s_server->register_handler(GET, "/simple_get", &s_simple_get_handler);
        s_server->register_handler(HEAD, "/simple_get", &s_simple_get_handler);
        s_server->register_handler(POST, "/simple_post", &s_simple_post_handler);
        s_server->start();
        real_port = s_server->get_real_port();
        EXPECT_NE(0, real_port);
        hostname = "http://127.0.0.1:" + std::to_string(real_port);
    }

    static void TearDownTestCase() { delete s_server; }
};

TEST_F(HttpClientTest, get_normal) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_get");
    EXPECT_TRUE(st.ok());
    client.set_method(GET);
    client.set_basic_auth("test1", "");
    std::string response;
    st = client.execute(&response);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("test1", response.c_str());

    // for head
    st = client.init(hostname + "/simple_get");
    EXPECT_TRUE(st.ok());
    client.set_method(HEAD);
    client.set_basic_auth("test1", "");
    st = client.execute();
    EXPECT_TRUE(st.ok());
    uint64_t len = 0;
    st = client.get_content_length(&len);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(5, len);
}

TEST_F(HttpClientTest, download) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_get");
    EXPECT_TRUE(st.ok());
    client.set_basic_auth("test1", "");
    std::string local_file = ".http_client_test.dat";
    st = client.download(local_file);
    EXPECT_TRUE(st.ok());
    char buf[50];
    auto fp = fopen(local_file.c_str(), "r");
    auto size = fread(buf, 1, 50, fp);
    buf[size] = 0;
    EXPECT_STREQ("test1", buf);
    unlink(local_file.c_str());
}

TEST_F(HttpClientTest, get_failed) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_get");
    EXPECT_TRUE(st.ok());
    client.set_method(GET);
    client.set_basic_auth("test1", "");
    std::string response;
    st = client.execute(&response);
    EXPECT_FALSE(!st.ok());
}

TEST_F(HttpClientTest, post_normal) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_post");
    EXPECT_TRUE(st.ok());
    client.set_method(POST);
    client.set_basic_auth("test1", "");
    std::string response;
    std::string request_body = "simple post body query";
    st = client.execute_post_request(request_body, &response);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(response.length(), request_body.length());
    EXPECT_STREQ(response.c_str(), request_body.c_str());
}

TEST_F(HttpClientTest, post_failed) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_pos");
    EXPECT_TRUE(st.ok());
    client.set_method(POST);
    client.set_basic_auth("test1", "");
    std::string response;
    std::string request_body = "simple post body query";
    st = client.execute_post_request(request_body, &response);
    EXPECT_FALSE(st.ok());
    std::string not_found = "404";
    EXPECT_TRUE(boost::algorithm::contains(st.get_error_msg(), not_found));
}

} // namespace doris
