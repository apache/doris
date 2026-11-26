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

#include "service/http/action/rotate_credential_action.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_client.h"
#include "service/http/http_headers.h"
#include "service/http/http_token_manager.h"
#include "util/defer_op.h"

namespace doris {

namespace {

void write_text_file(const std::filesystem::path& path, const std::string& content) {
    std::ofstream out(path, std::ios::trunc);
    ASSERT_TRUE(out.is_open()) << path;
    out << content;
    out.close();
}

struct HttpReply {
    long status_code = 0;
    std::string body;
};

HttpReply send_http_request(int port, HttpMethod method, const std::string& path_and_query,
                            const std::string& auth_header = "") {
    const std::string url = "http://127.0.0.1:" + std::to_string(port) + path_and_query;
    HttpClient client;
    EXPECT_TRUE(client.init(url, false).ok());
    client.set_method(method);
    client.set_timeout_ms(5000);
    if (!auth_header.empty()) {
        client.set_header(HttpHeaders::AUTHORIZATION, auth_header);
    }
    std::string resp;
    Status st = (method == HttpMethod::POST) ? client.execute_post_request("", &resp)
                                             : client.execute(&resp);
    EXPECT_TRUE(st.ok()) << st.to_string();
    return {.status_code = client.get_http_status(), .body = std::move(resp)};
}

} // namespace

TEST(RotateCredentialActionTest, RotateHttpTokenLifecycleAuthAndFailures) {
    auto temp_dir = std::filesystem::temp_directory_path() /
                    ("be_rotate_http_token_" + std::to_string(::getpid()));
    std::filesystem::create_directories(temp_dir);
    auto base_conf = temp_dir / "be.conf";
    auto custom_conf = temp_dir / "be_custom.conf";

    std::string old_base_path = config::get_base_conf_path();
    std::string old_custom_path = config::get_custom_conf_path();
    std::string old_token = config::http_token;
    bool old_all_auth = config::enable_all_http_auth;
    int32_t old_be_port = config::be_port;

    Defer restore {[&]() {
        config::set_base_conf_path(old_base_path);
        config::set_custom_conf_path(old_custom_path);
        config::http_token = old_token;
        config::enable_all_http_auth = old_all_auth;
        config::be_port = old_be_port;
        HttpTokenManager::instance().reset_for_test(old_token);
        std::error_code ec;
        std::filesystem::remove_all(temp_dir, ec);
    }};

    write_text_file(base_conf, "http_token = token-A\n");
    config::set_base_conf_path(base_conf.string());
    config::set_custom_conf_path(custom_conf.string());
    config::http_token = "token-A";
    HttpTokenManager::instance().reset_for_test("token-A");

    std::atomic<int64_t> fake_now_ms {1000000};
    HttpTokenManager::instance().set_clock_for_test([&]() { return fake_now_ms.load(); });

    RotateHttpTokenAction rotate_token_action;
    EvHttpServer server {0};
    ASSERT_TRUE(server.register_handler(HttpMethod::POST, "/api/rotate_http_token",
                                        &rotate_token_action));
    server.start();
    int port = server.get_real_port();
    ASSERT_GT(port, 0);

    // Prepare B in be_custom.conf (overrides be.conf) + unrelated config change.
    write_text_file(base_conf, "http_token = token-base\nbe_port = 19999\n");
    write_text_file(custom_conf, "http_token = token-B\n");

    // 1. Missing/empty/wrong token rejected with HTTP 403 NOT_AUTHORIZED even when
    //    enable_all_http_auth=false and Basic Auth header is present.
    config::enable_all_http_auth = false;
    {
        auto reply = send_http_request(port, HttpMethod::POST, "/api/rotate_http_token",
                                       "Basic cm9vdDo=");
        ASSERT_EQ(reply.status_code, 403) << reply.body;
        rapidjson::Document d;
        ASSERT_FALSE(d.Parse(reply.body.c_str()).HasParseError());
        EXPECT_STREQ(d["status"].GetString(), "NOT_AUTHORIZED");
        EXPECT_STREQ(d["msg"].GetString(), "incorrect token");
    }
    // New token on disk cannot authenticate before rotate.
    {
        auto reply =
                send_http_request(port, HttpMethod::POST, "/api/rotate_http_token?token=token-B");
        ASSERT_EQ(reply.status_code, 403);
    }

    // 2. GET must not trigger rotation.
    {
        auto reply =
                send_http_request(port, HttpMethod::GET, "/api/rotate_http_token?token=token-A");
        EXPECT_NE(reply.status_code, 200);
        EXPECT_TRUE(HttpTokenManager::instance().verify("token-A"));
        EXPECT_FALSE(HttpTokenManager::instance().verify("token-B"));
    }

    // 3. POST without body rotates A -> B with custom override; unrelated config untouched.
    {
        auto reply =
                send_http_request(port, HttpMethod::POST, "/api/rotate_http_token?token=token-A");
        ASSERT_EQ(reply.status_code, 200) << reply.body;
        rapidjson::Document d;
        ASSERT_FALSE(d.Parse(reply.body.c_str()).HasParseError());
        EXPECT_STREQ(d["status"].GetString(), "OK");
        EXPECT_STREQ(d["msg"].GetString(), "OK");
        EXPECT_EQ(config::be_port, old_be_port);
        EXPECT_TRUE(HttpTokenManager::instance().verify("token-A"));
        EXPECT_TRUE(HttpTokenManager::instance().verify("token-B"));
        EXPECT_FALSE(HttpTokenManager::instance().verify("token-base"));
    }

    // 4. Idempotent retry at T + 1000s must not refresh expiry or clear previous token A.
    fake_now_ms.store(1000000 + 1000 * 1000);
    {
        auto reply =
                send_http_request(port, HttpMethod::POST, "/api/rotate_http_token?token=token-B");
        ASSERT_EQ(reply.status_code, 200) << reply.body;
        EXPECT_TRUE(HttpTokenManager::instance().verify("token-A"));
    }

    // 5. Grace period boundary (1800s).
    fake_now_ms.store(1000000 + HttpTokenManager::kHttpTokenGracePeriodMs - 1);
    EXPECT_TRUE(HttpTokenManager::instance().verify("token-A"));
    fake_now_ms.store(1000000 + HttpTokenManager::kHttpTokenGracePeriodMs);
    EXPECT_FALSE(HttpTokenManager::instance().verify("token-A"));
    EXPECT_TRUE(HttpTokenManager::instance().verify("token-B"));
    {
        auto reply =
                send_http_request(port, HttpMethod::POST, "/api/rotate_http_token?token=token-A");
        ASSERT_EQ(reply.status_code, 403);
    }

    // 6. Empty value -> HTTP 400 INVALID_ARGUMENT, state unchanged.
    write_text_file(custom_conf, "http_token = \n");
    {
        auto reply =
                send_http_request(port, HttpMethod::POST, "/api/rotate_http_token?token=token-B");
        ASSERT_EQ(reply.status_code, 400) << reply.body;
        rapidjson::Document d;
        ASSERT_FALSE(d.Parse(reply.body.c_str()).HasParseError());
        EXPECT_STREQ(d["status"].GetString(), "INVALID_ARGUMENT");
        EXPECT_STREQ(d["msg"].GetString(), "Configured http_token must not be empty");
        EXPECT_TRUE(HttpTokenManager::instance().verify("token-B"));
    }

    // 7. Corrupted config -> HTTP 500 INTERNAL_ERROR, state unchanged.
    write_text_file(custom_conf, "broken_line_without_equals\n");
    {
        auto reply =
                send_http_request(port, HttpMethod::POST, "/api/rotate_http_token?token=token-B");
        ASSERT_EQ(reply.status_code, 500) << reply.body;
        rapidjson::Document d;
        ASSERT_FALSE(d.Parse(reply.body.c_str()).HasParseError());
        EXPECT_STREQ(d["status"].GetString(), "INTERNAL_ERROR");
        EXPECT_STREQ(d["msg"].GetString(), "Failed to read configured http_token");
        EXPECT_TRUE(HttpTokenManager::instance().verify("token-B"));
    }

    // 8. Concurrent rotation is serialized.
    write_text_file(custom_conf, "http_token = token-C\n");
    std::vector<std::thread> threads;
    std::atomic<int> ok_count {0};
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&]() {
            auto reply = send_http_request(port, HttpMethod::POST,
                                           "/api/rotate_http_token?token=token-B");
            if (reply.status_code == 200) {
                ok_count.fetch_add(1);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(ok_count.load(), 8);
    EXPECT_TRUE(HttpTokenManager::instance().verify("token-C"));
    EXPECT_TRUE(HttpTokenManager::instance().verify("token-B"));
}

TEST(RotateCredentialActionTest, RotateCloudUniqueIdConcurrencyMaskingAndConfigGuard) {
    auto temp_dir = std::filesystem::temp_directory_path() /
                    ("be_rotate_cloud_id_" + std::to_string(::getpid()));
    std::filesystem::create_directories(temp_dir);
    auto base_conf = temp_dir / "be.conf";
    auto custom_conf = temp_dir / "be_custom.conf";

    std::string old_base_path = config::get_base_conf_path();
    std::string old_custom_path = config::get_custom_conf_path();
    std::string old_token = config::http_token;
    std::string old_cloud_id = config::cloud_unique_id;

    Defer restore {[&]() {
        config::set_base_conf_path(old_base_path);
        config::set_custom_conf_path(old_custom_path);
        config::http_token = old_token;
        config::cloud_unique_id = old_cloud_id;
        config::reset_cloud_unique_id_override_for_test();
        HttpTokenManager::instance().reset_for_test(old_token);
        std::error_code ec;
        std::filesystem::remove_all(temp_dir, ec);
    }};

    write_text_file(base_conf, "http_token = token-A\ncloud_unique_id = 1:inst1:be-A\n");
    config::set_base_conf_path(base_conf.string());
    config::set_custom_conf_path(custom_conf.string());
    config::http_token = "token-A";
    config::cloud_unique_id = "1:inst1:be-A";
    config::reset_cloud_unique_id_override_for_test();
    HttpTokenManager::instance().reset_for_test("token-A");

    RotateCloudUniqueIdAction rotate_id_action;
    EvHttpServer server {0};
    ASSERT_TRUE(server.register_handler(HttpMethod::POST, "/api/rotate_cloud_unique_id",
                                        &rotate_id_action));
    server.start();
    int port = server.get_real_port();
    ASSERT_GT(port, 0);

    // 1. Unauthorized rejected with HTTP 403.
    {
        auto reply = send_http_request(port, HttpMethod::POST,
                                       "/api/rotate_cloud_unique_id?token=wrong");
        ASSERT_EQ(reply.status_code, 403);
    }

    // 2. Custom override rotation succeeds.
    write_text_file(custom_conf, "cloud_unique_id = 1:inst1:be-B\n");
    {
        auto reply = send_http_request(port, HttpMethod::POST,
                                       "/api/rotate_cloud_unique_id?token=token-A");
        ASSERT_EQ(reply.status_code, 200) << reply.body;
        rapidjson::Document d;
        ASSERT_FALSE(d.Parse(reply.body.c_str()).HasParseError());
        EXPECT_STREQ(d["status"].GetString(), "OK");
        EXPECT_STREQ(d["msg"].GetString(), "OK");
        EXPECT_EQ(config::get_cloud_unique_id(), "1:inst1:be-B");
        EXPECT_TRUE(config::is_cloud_unique_id_rotated());
    }

    // 3. Idempotent retry succeeds.
    {
        auto reply = send_http_request(port, HttpMethod::POST,
                                       "/api/rotate_cloud_unique_id?token=token-A");
        ASSERT_EQ(reply.status_code, 200);
        EXPECT_EQ(config::get_cloud_unique_id(), "1:inst1:be-B");
    }

    // 4. Empty value -> HTTP 400 INVALID_ARGUMENT, runtime ID unchanged.
    write_text_file(custom_conf, "cloud_unique_id = \n");
    {
        auto reply = send_http_request(port, HttpMethod::POST,
                                       "/api/rotate_cloud_unique_id?token=token-A");
        ASSERT_EQ(reply.status_code, 400) << reply.body;
        rapidjson::Document d;
        ASSERT_FALSE(d.Parse(reply.body.c_str()).HasParseError());
        EXPECT_STREQ(d["status"].GetString(), "INVALID_ARGUMENT");
        EXPECT_STREQ(d["msg"].GetString(), "Configured cloud_unique_id must not be empty");
        EXPECT_EQ(config::get_cloud_unique_id(), "1:inst1:be-B");
    }

    // 5. Missing base config -> HTTP 500 INTERNAL_ERROR, runtime ID unchanged.
    std::filesystem::remove(custom_conf);
    std::filesystem::remove(base_conf);
    {
        auto reply = send_http_request(port, HttpMethod::POST,
                                       "/api/rotate_cloud_unique_id?token=token-A");
        ASSERT_EQ(reply.status_code, 500) << reply.body;
        rapidjson::Document d;
        ASSERT_FALSE(d.Parse(reply.body.c_str()).HasParseError());
        EXPECT_STREQ(d["status"].GetString(), "INTERNAL_ERROR");
        EXPECT_STREQ(d["msg"].GetString(), "Failed to read configured cloud_unique_id");
        EXPECT_EQ(config::get_cloud_unique_id(), "1:inst1:be-B");
    }

    // 6. Existing update_config path cannot bypass rotation.
    EXPECT_FALSE(config::set_config("http_token", "bypassed", false).ok());
    EXPECT_FALSE(config::set_config("cloud_unique_id", "bypassed", false).ok());

    // 7. get_config_info masks http_token and cloud_unique_id when non-empty.
    if (config::full_conf_map != nullptr) {
        {
            std::lock_guard<std::mutex> lock(*config::get_mutable_string_config_lock());
            (*config::full_conf_map)["http_token"] = "token-A";
            (*config::full_conf_map)["cloud_unique_id"] = "1:inst1:be-B";
        }
        auto infos = config::get_config_info();
        bool saw_token = false;
        bool saw_cloud_id = false;
        for (const auto& row : infos) {
            if (row[0] == "http_token") {
                saw_token = true;
                EXPECT_EQ(row[2], "******");
            }
            if (row[0] == "cloud_unique_id") {
                saw_cloud_id = true;
                EXPECT_EQ(row[2], "******");
            }
        }
        EXPECT_TRUE(saw_token);
        EXPECT_TRUE(saw_cloud_id);
    }

    // 8. Concurrent reads and rotations have no data race and always observe valid snapshots.
    std::atomic<bool> stop {false};
    std::thread reader([&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            std::string id = config::get_cloud_unique_id();
            EXPECT_FALSE(id.empty());
        }
    });
    for (int i = 0; i < 20; ++i) {
        config::set_cloud_unique_id("1:inst1:be-rot-" + std::to_string(i), true);
    }
    stop.store(true, std::memory_order_relaxed);
    reader.join();
    EXPECT_EQ(config::get_cloud_unique_id(), "1:inst1:be-rot-19");
}

} // namespace doris
