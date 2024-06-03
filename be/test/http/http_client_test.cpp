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

#include <fcntl.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/algorithm/string/predicate.hpp>

#include "gtest/gtest_pred_impl.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "util/md5.h"

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
        bool is_acquire_md5 = !req->param("acquire_md5").empty();
        if (req->method() == HttpMethod::HEAD) {
            req->add_output_header(HttpHeaders::CONTENT_LENGTH, std::to_string(5).c_str());
            if (is_acquire_md5) {
                Md5Digest md5;
                md5.update("md5sum", 6);
                md5.digest();
                req->add_output_header(HttpHeaders::CONTENT_MD5, md5.hex().c_str());
            }
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

class HttpNotFoundHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "file not exist.");
    }
};

class HttpDownloadFileHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        do_file_response("/proc/self/exe", req, nullptr, true);
    }
};

static EvHttpServer* s_server = nullptr;
static int real_port = 0;
static std::string hostname = "";

static HttpClientTestSimpleGetHandler s_simple_get_handler;
static HttpClientTestSimplePostHandler s_simple_post_handler;
static HttpNotFoundHandler s_not_found_handler;
static HttpDownloadFileHandler s_download_file_handler;

class HttpClientTest : public testing::Test {
public:
    HttpClientTest() {}
    ~HttpClientTest() override {}

    static void SetUpTestCase() {
        s_server = new EvHttpServer(0);
        s_server->register_handler(GET, "/simple_get", &s_simple_get_handler);
        s_server->register_handler(HEAD, "/simple_get", &s_simple_get_handler);
        s_server->register_handler(POST, "/simple_post", &s_simple_post_handler);
        s_server->register_handler(GET, "/not_found", &s_not_found_handler);
        s_server->register_handler(HEAD, "/download_file", &s_download_file_handler);
        static_cast<void>(s_server->start());
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
    std::string response;
    st = client.execute(&response);
    EXPECT_FALSE(st.ok());
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
    EXPECT_TRUE(boost::algorithm::contains(st.to_string(), not_found));
}

TEST_F(HttpClientTest, not_found) {
    HttpClient client;
    std::string url = hostname + "/not_found";
    constexpr uint64_t kMaxTimeoutMs = 1000;

    auto get_cb = [&url](HttpClient* client) {
        std::string resp;
        RETURN_IF_ERROR(client->init(url));
        client->set_timeout_ms(kMaxTimeoutMs);
        return client->execute(&resp);
    };

    auto status = HttpClient::execute_with_retry(3, 1, get_cb);
    // libcurl is configured by CURLOPT_FAILONERROR
    EXPECT_FALSE(status.ok());
}

TEST_F(HttpClientTest, header_content_md5) {
    std::string url = hostname + "/simple_get";

    {
        // without md5
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(HEAD);
        client.set_basic_auth("test1", "");
        st = client.execute();
        EXPECT_TRUE(st.ok());
        uint64_t len = 0;
        st = client.get_content_length(&len);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(5, len);
        std::string md5;
        st = client.get_content_md5(&md5);
        EXPECT_TRUE(st.ok());
        EXPECT_TRUE(md5.empty());
    }

    {
        // with md5
        HttpClient client;
        auto st = client.init(url + "?acquire_md5=true");
        EXPECT_TRUE(st.ok());
        client.set_method(HEAD);
        client.set_basic_auth("test1", "");
        st = client.execute();
        EXPECT_TRUE(st.ok());
        uint64_t len = 0;
        st = client.get_content_length(&len);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(5, len);
        std::string md5_value;
        st = client.get_content_md5(&md5_value);
        EXPECT_TRUE(st.ok());

        Md5Digest md5;
        md5.update("md5sum", 6);
        md5.digest();
        EXPECT_EQ(md5_value, md5.hex());
    }
}

TEST_F(HttpClientTest, download_file_md5) {
    std::string url = hostname + "/download_file";
    HttpClient client;
    auto st = client.init(url);
    EXPECT_TRUE(st.ok());
    client.set_method(HEAD);
    client.set_basic_auth("test1", "");
    st = client.execute();
    EXPECT_TRUE(st.ok());

    std::string md5_value;
    st = client.get_content_md5(&md5_value);
    EXPECT_TRUE(st.ok());

    int fd = open("/proc/self/exe", O_RDONLY);
    ASSERT_TRUE(fd >= 0);
    struct stat stat;
    ASSERT_TRUE(fstat(fd, &stat) >= 0);

    int64_t file_size = stat.st_size;
    Md5Digest md5;
    void* buf = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    md5.update(buf, file_size);
    md5.digest();
    munmap(buf, file_size);

    EXPECT_EQ(md5_value, md5.hex());
    close(fd);
}

} // namespace doris
