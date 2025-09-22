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

#include "http/action/file_cache_action.h"

#include <event2/event.h>
#include <event2/http.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>

#include "common/config.h"
#include "http/http_request.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "runtime/exec_env.h"

namespace doris {

class FileCacheActionTest : public testing::Test {
public:
    FileCacheActionTest() : _action(nullptr) {}
    ~FileCacheActionTest() override = default;

    static void SetUpTestSuite() {
        if (doris::ExecEnv::GetInstance()->file_cache_factory() == nullptr) {
            _suite_factory = std::make_unique<doris::io::FileCacheFactory>();
            doris::ExecEnv::GetInstance()->_file_cache_factory = _suite_factory.get();
        }

        if (!doris::ExecEnv::GetInstance()->_file_cache_open_fd_cache) {
            doris::ExecEnv::GetInstance()->_file_cache_open_fd_cache =
                    std::make_unique<doris::io::FDCache>();
        }
    }
    static void TearDownTestSuite() {
        if (doris::ExecEnv::GetInstance()->file_cache_factory() != nullptr) {
            doris::io::FileCacheFactory::instance()->clear_file_caches(true);
        }
        _suite_factory.reset(nullptr);
        doris::ExecEnv::GetInstance()->_file_cache_open_fd_cache.reset(nullptr);
    }

    void SetUp() override {
        _event_base = event_base_new();
        _evhttp_req = evhttp_request_new(nullptr, nullptr);
        _action = std::make_unique<FileCacheAction>(nullptr);

        ASSERT_NE(doris::ExecEnv::GetInstance()->file_cache_factory(), nullptr);

        _base_path = "/tmp/file_cache_action_ut_" + std::to_string(getpid());
        (void)std::filesystem::create_directories(_base_path);
        doris::io::FileCacheSettings settings;
        settings.storage = "memory";
        settings.capacity = 1024 * 1024;          // 1MB
        settings.max_file_block_size = 64 * 1024; // 64KB
        auto cache = std::make_unique<doris::io::BlockFileCache>(_base_path, settings);
        ASSERT_TRUE(cache->initialize());
        for (int i = 0; i < 1000; ++i) {
            if (cache->get_async_open_success()) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        auto* raw = cache.get();
        auto* factory = doris::io::FileCacheFactory::instance();
        factory->_caches.emplace_back(std::move(cache));
        factory->_path_to_cache[_base_path] = raw;
    }

    void TearDown() override {
        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }

        if (_event_base != nullptr) {
            event_base_free(_event_base);
        }

        if (doris::ExecEnv::GetInstance()->file_cache_factory() != nullptr) {
            doris::io::FileCacheFactory::instance()->clear_file_caches(true);
        }
    }

protected:
    evhttp_request* _evhttp_req = nullptr;
    event_base* _event_base = nullptr;
    std::unique_ptr<FileCacheAction> _action;
    std::string _base_path;

    inline static std::unique_ptr<doris::io::FileCacheFactory> _suite_factory;
};

TEST_F(FileCacheActionTest, list_base_paths) {
    HttpRequest req(_evhttp_req);
    std::string json_metrics;

    req._params["op"] = "list_base_paths";

    Status status = _action->_handle_header(&req, &json_metrics);
    EXPECT_TRUE(status.ok());

    std::string expected = "[\"" + _base_path + "\"]";
    EXPECT_EQ(json_metrics, expected);
}

TEST_F(FileCacheActionTest, check_consistency_missing_param) {
    HttpRequest req(_evhttp_req);
    std::string json_metrics;

    req._params["op"] = "check_consistency";

    Status status = _action->_handle_header(&req, &json_metrics);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.to_string(), "[INVALID_ARGUMENT]missing parameter: base_path is required");
}

TEST_F(FileCacheActionTest, check_consistency_not_found) {
    HttpRequest req(_evhttp_req);
    std::string json_metrics;

    req._params["op"] = "check_consistency";
    req._params["base_path"] = "/tmp/file_cache_action_ut_not_exist";

    Status status = _action->_handle_header(&req, &json_metrics);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.to_string(),
              "[INVALID_ARGUMENT]file cache not found for base_path: "
              "/tmp/file_cache_action_ut_not_exist");
}

TEST_F(FileCacheActionTest, check_consistency_ok) {
    HttpRequest req(_evhttp_req);
    std::string json_metrics;

    req._params["op"] = "check_consistency";
    req._params["base_path"] = _base_path;

    Status status = _action->_handle_header(&req, &json_metrics);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(json_metrics, "null");
}

} // namespace doris