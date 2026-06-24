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

#include "service/http/action/file_cache_action.h"

#include <event2/event.h>
#include <event2/http.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string_view>
#include <thread>

#include "common/config.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "runtime/exec_env.h"
#include "service/http/http_request.h"
#include "util/slice.h"

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
        reset_file_cache_factory();

        _base_path = "/tmp/file_cache_action_ut_" + std::to_string(getpid());
        (void)std::filesystem::create_directories(_base_path);
        doris::io::FileCacheSettings settings;
        settings.storage = "memory";
        settings.capacity = 1024 * 1024;          // 1MB
        settings.max_file_block_size = 64 * 1024; // 64KB
        settings.query_queue_size = 1024 * 1024;
        settings.query_queue_elements = 16;
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
        _cache = raw;
    }

    void TearDown() override {
        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }

        if (_event_base != nullptr) {
            event_base_free(_event_base);
        }

        if (doris::ExecEnv::GetInstance()->file_cache_factory() != nullptr) {
            reset_file_cache_factory();
        }
    }

protected:
    evhttp_request* _evhttp_req = nullptr;
    event_base* _event_base = nullptr;
    std::unique_ptr<FileCacheAction> _action;
    std::string _base_path;
    doris::io::BlockFileCache* _cache = nullptr;

    inline static std::unique_ptr<doris::io::FileCacheFactory> _suite_factory;

    void reset_file_cache_factory() {
        auto* factory = doris::io::FileCacheFactory::instance();
        factory->clear_file_caches(true);
        factory->_caches.clear();
        factory->_path_to_cache.clear();
        factory->_capacity = 0;
        _cache = nullptr;
    }

    void cache_file_block(const std::string& file_path) {
        auto hash = doris::io::BlockFileCache::hash(file_path);
        doris::io::CacheContext context;
        doris::io::ReadStatistics stats;
        context.cache_type = doris::io::FileCacheType::NORMAL;
        context.stats = &stats;

        auto holder = _cache->get_or_set(hash, 0, 4, context);
        ASSERT_EQ(holder.file_blocks.size(), 1);
        auto& block = holder.file_blocks.front();
        ASSERT_EQ(block->get_or_set_downloader(), doris::io::FileBlock::get_caller_id());
        std::string data = "data";
        ASSERT_TRUE(block->append(Slice(data.data(), data.size())).ok());
        ASSERT_TRUE(block->finalize().ok());
        ASSERT_EQ(_cache->_cur_cache_size, 4);
    }
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

TEST_F(FileCacheActionTest, clear_defaults_to_async) {
    cache_file_block("clear_defaults_to_async.dat");
    HttpRequest req(_evhttp_req);
    std::string json_metrics;

    req._params["op"] = "clear";

    Status status = _action->_handle_header(&req, &json_metrics);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(json_metrics.empty());
    EXPECT_EQ(_cache->_cur_cache_size, 0);
}

TEST_F(FileCacheActionTest, clear_sync_true_returns_sync_summary_json) {
    cache_file_block("clear_sync_true_returns_sync_summary_json.dat");
    HttpRequest req(_evhttp_req);
    std::string json_metrics;

    req._params["op"] = "clear";
    req._params["sync"] = "true";

    Status status = _action->_handle_header(&req, &json_metrics);

    EXPECT_TRUE(status.ok());
    EXPECT_NE(json_metrics.find("\"status\":\"OK\""), std::string::npos);
    EXPECT_NE(json_metrics.find("finish clear_file_cache_sync"), std::string::npos);
    EXPECT_NE(json_metrics.find("sync_remove=1"), std::string::npos);
    EXPECT_EQ(_cache->_cur_cache_size, 0);
}

TEST_F(FileCacheActionTest, clear_value_uses_async_remove) {
    constexpr std::string_view file_path = "clear_value_uses_async_remove.dat";
    cache_file_block(std::string(file_path));
    auto hash = doris::io::BlockFileCache::hash(std::string(file_path));
    HttpRequest req(_evhttp_req);
    std::string json_metrics;

    req._params["op"] = "clear";
    req._params["value"] = std::string(file_path);

    Status status = _action->_handle_header(&req, &json_metrics);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(json_metrics.empty());
    EXPECT_EQ(_cache->_cur_cache_size, 0);
    EXPECT_TRUE(_cache->get_blocks_by_key(hash).empty());
}

} // namespace doris
