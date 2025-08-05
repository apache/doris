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

#include "cpp/util.h"

#include <chrono>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/simple_thread_pool.h"
#include "common/string_util.h"
#include "cpp/aws_common.h"
#include "cpp/sync_point.h"
#include "gtest/gtest.h"
#include "recycler/recycler.h"
#include "recycler/sync_executor.h"

using namespace doris::cloud;

int main(int argc, char** argv) {
    const auto* conf_file = "doris_cloud.conf";
    if (!config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!::init_glog("util")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(StringUtilTest, test_string_strip) {
    // clang-format off
    //                       str         expect          to_drop
    std::vector<std::tuple<std::string, std::string, std::string>> leading_inputs {
        {""       , ""      , ""    },
        {""       , ""      , "/"   },
        {"/"      , ""      , "/"   },
        {"\t////" , ""      , "/ \t"},
        {"/a///"  , "a///"  , "/"   },
        {"/a/b/c/", "a/b/c/", "/"   },
        {"a/b/c/" , "a/b/c/", "/"   },
        {"a/b/c/" , "/b/c/" , "a"   },
    };
    int idx = 0;
    for (auto&& i : leading_inputs) {
        doris::cloud::strip_leading(std::get<0>(i), std::get<2>(i));
        EXPECT_EQ(std::get<0>(i), std::get<1>(i)) << " index=" << idx;
        ++idx;
    }

    idx = 0;
    std::vector<std::tuple<std::string, std::string, std::string>> trailing_inputs {
        {""       , ""      , ""    },
        {"/"      , ""      , "/"   },
        {"////\t" , ""      , "/ \t"},
        {"/a///"  , "/a"    ,  "/"  },
        {"/a/b/c/", "/a/b/c", "/"   },
        {"a/b/c/" , "a/b/c" , "/"   },
        {"a/b/c"  , "a/b/c" , "/"   },
        {"a/b/c"  , "a/b/"  , "c"   },
    };
    for (auto&& i : trailing_inputs) {
        doris::cloud::strip_trailing(std::get<0>(i), std::get<2>(i));
        EXPECT_EQ(std::get<0>(i), std::get<1>(i)) << " index=" << idx;
        ++idx;
    }

    idx = 0;
    std::vector<std::tuple<std::string, std::string>> trim_inputs {
        {""              , ""     },
        {""              , ""     },
        {"/"             , ""     },
        {"\t////"        , ""     },
        {"/a ///"        , "a"   },
        {"/a/b/c/"       , "a/b/c"},
        {"a/b/c/"        , "a/b/c"},
        {"a/b/c"         , "a/b/c"},
        {"\t/bbc///"     , "bbc"  },
        {"ab c"          , "ab c" },
        {"\t  /a/b/c \t/", "a/b/c"},
    };
    for (auto&& i : trim_inputs) {
        doris::cloud::trim(std::get<0>(i));
        EXPECT_EQ(std::get<0>(i), std::get<1>(i)) << " index=" << idx;
        ++idx;
    }

    // clang-format on
}

template <typename... Func>
auto task_wrapper(Func... funcs) -> std::function<int()> {
    return [funcs...]() {
        return [](std::initializer_list<int> numbers) {
            int i = 0;
            for (int num : numbers) {
                if (num != 0) {
                    i = num;
                }
            }
            return i;
        }({funcs()...});
    };
}

TEST(UtilTest, stage_wrapper) {
    std::function<int()> func1 = []() { return 0; };
    std::function<int()> func2 = []() { return -1; };
    std::function<int()> func3 = []() { return 0; };
    auto f = task_wrapper(func1, func2, func3);
    ASSERT_EQ(-1, f());

    f = task_wrapper(func1, func3);
    ASSERT_EQ(0, f());
}

TEST(UtilTest, delay) {
    auto s3_producer_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    s3_producer_pool->start();
    // test normal execute
    {
        SyncExecutor<int> sync_executor(s3_producer_pool, "normal test",
                                        [](int k) { return k == -1; });
        auto f1 = []() { return -1; };
        auto f2 = []() {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            return 1;
        };
        sync_executor.add(f2);
        sync_executor.add(f2);
        sync_executor.add(f1);
        bool finished = true;
        std::vector<int> res = sync_executor.when_all(&finished);
        ASSERT_EQ(finished, false);
        ASSERT_EQ(3, res.size());
    }
    // test normal execute
    {
        SyncExecutor<std::string_view> sync_executor(
                s3_producer_pool, "normal test",
                [](const std::string_view k) { return k.empty(); });
        auto f1 = []() { return ""; };
        auto f2 = []() {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            return "fake";
        };
        sync_executor.add(f2);
        sync_executor.add(f2);
        sync_executor.add(f1);
        bool finished = true;
        auto res = sync_executor.when_all(&finished);
        ASSERT_EQ(finished, false);
        ASSERT_EQ(3, res.size());
    }
}

TEST(UtilTest, normal) {
    auto s3_producer_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    s3_producer_pool->start();
    // test normal execute
    {
        SyncExecutor<int> sync_executor(s3_producer_pool, "normal test",
                                        [](int k) { return k == -1; });
        auto f1 = []() { return 1; };
        sync_executor.add(f1);
        sync_executor.add(f1);
        sync_executor.add(f1);
        bool finished = true;
        std::vector<int> res = sync_executor.when_all(&finished);
        ASSERT_EQ(3, res.size());
        ASSERT_EQ(finished, true);
        std::for_each(res.begin(), res.end(), [](auto&& n) { ASSERT_EQ(1, n); });
    }
    // test when error happen
    {
        SyncExecutor<int> sync_executor(s3_producer_pool, "normal test",
                                        [](int k) { return k == -1; });
        auto f1 = []() { return 1; };
        sync_executor._stop_token = true;
        sync_executor.add(f1);
        sync_executor.add(f1);
        sync_executor.add(f1);
        bool finished = true;
        std::vector<int> res = sync_executor.when_all(&finished);
        ASSERT_EQ(finished, false);
        ASSERT_EQ(0, res.size());
    }
    {
        SyncExecutor<int> sync_executor(s3_producer_pool, "normal test",
                                        [](int k) { return k == -1; });
        auto f1 = []() { return 1; };
        auto cancel = []() { return -1; };
        sync_executor.add(f1);
        sync_executor.add(f1);
        sync_executor.add(f1);
        sync_executor.add(cancel);
        bool finished = true;
        std::vector<int> res = sync_executor.when_all(&finished);
        ASSERT_EQ(finished, false);
    }
    // test string_view
    {
        SyncExecutor<std::string_view> sync_executor(
                s3_producer_pool, "normal test",
                [](const std::string_view k) { return k.empty(); });
        std::string s = "Hello World";
        auto f1 = [&s]() { return std::string_view(s); };
        sync_executor.add(f1);
        sync_executor.add(f1);
        sync_executor.add(f1);
        bool finished = true;
        std::vector<std::string_view> res = sync_executor.when_all(&finished);
        ASSERT_EQ(3, res.size());
        std::for_each(res.begin(), res.end(), [&s](auto&& n) { ASSERT_EQ(s, n); });
    }
}

TEST(UtilTest, test_add_after_when_all) {
    auto f = []() {
        auto pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
        pool->start();
        SyncExecutor<int> sync_executor(pool, "test add after when all: inside",
                                        [](int k) { return k != 0; });
        auto f1 = []() { return 0; };
        sync_executor.add(f1);
        bool finished = true;
        std::vector<int> res = sync_executor.when_all(&finished);
        sync_executor.add(f1);
        res = sync_executor.when_all(&finished);
        EXPECT_EQ(1, res.size());
        EXPECT_EQ(finished, true);
        std::for_each(res.begin(), res.end(), [](auto&& n) { EXPECT_EQ(0, n); });
        return 0;
    };

    auto s3_producer_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    s3_producer_pool->start();
    SyncExecutor<int> s3_sync_executor(s3_producer_pool, "test add after when all: outside",
                                       [](int k) { return k != 0; });
    s3_sync_executor.add(f);
    bool finished = true;
    std::vector<int> res = s3_sync_executor.when_all(&finished);
    EXPECT_EQ(1, res.size());
    EXPECT_EQ(finished, true);
    std::for_each(res.begin(), res.end(), [](auto&& n) { EXPECT_EQ(0, n); });
}

TEST(UtilTest, exception) {
    auto s3_producer_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    s3_producer_pool->start();
    {
        SyncExecutor<int> sync_executor(s3_producer_pool, "exception test",
                                        [](int k) { return k != 0; });
        auto f = []() {
            throw(std::runtime_error("test exception"));
            return 1;
        };
        sync_executor.add(f);
        bool finished = true;
        std::vector<int> res = sync_executor.when_all(&finished);
        EXPECT_EQ(0, res.size());
        EXPECT_EQ(finished, false);
        std::for_each(res.begin(), res.end(), [](auto&& n) { EXPECT_EQ(1, n); });
    }
}

TEST(UtilTest, test_sync_executor) {
    auto f = []() {
        sleep(1);
        auto pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
        pool->start();
        SyncExecutor<int> sync_executor(pool, "test sync executor: inside",
                                        [](int k) { return k != 0; });
        auto f1 = []() { return 0; };
        sync_executor.add(f1);
        bool finished = true;
        std::vector<int> res = sync_executor.when_all(&finished);
        sync_executor.add(f1);
        res = sync_executor.when_all(&finished);
        EXPECT_EQ(1, res.size());
        EXPECT_EQ(finished, true);
        std::for_each(res.begin(), res.end(), [](auto&& n) { EXPECT_EQ(0, n); });
        return 0;
    };
    std::mutex go_mutex;

    auto* sp = doris::SyncPoint::get_instance();
    sp->set_call_back("SyncExecutor::when_all.set_wait_time", [&](auto&& args) {
        std::unique_lock<std::mutex> _lock(go_mutex);
        [[maybe_unused]] auto max_wait_time = *doris::try_any_cast<size_t*>(args[0]);
        max_wait_time = 100;
    });

    auto s3_producer_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    s3_producer_pool->start();
    SyncExecutor<int> s3_sync_executor(s3_producer_pool, "test sync executor: outside",
                                       [](int k) { return k != 0; });
    s3_sync_executor.add(f);
    bool finished = true;
    std::vector<int> res = s3_sync_executor.when_all(&finished);
    EXPECT_EQ(1, res.size());
    EXPECT_EQ(finished, true);
    std::for_each(res.begin(), res.end(), [](auto&& n) { EXPECT_EQ(0, n); });
}

TEST(UtilTest, test_split) {
    auto path = doris::get_valid_ca_cert_path(doris::cloud::split(config::ca_cert_file_paths, ';'));
    LOG(INFO) << "config:" << config::ca_cert_file_paths << " path:" << path;
    ASSERT_FALSE(path.empty());
}

TEST(UtilTest, test_normalize_http_uri) {
    // ===== Basic functionality with HTTPS protocol =====
    EXPECT_EQ(doris::normalize_http_uri("https://example.com/path"), "https://example.com/path");
    EXPECT_EQ(doris::normalize_http_uri("https://example.com//path"), "https://example.com/path");
    EXPECT_EQ(doris::normalize_http_uri("https://example.com///path"), "https://example.com/path");

    // ===== Basic functionality with HTTP protocol =====
    EXPECT_EQ(doris::normalize_http_uri("http://example.com/path"), "http://example.com/path");
    EXPECT_EQ(doris::normalize_http_uri("http://example.com//path"), "http://example.com/path");
    EXPECT_EQ(doris::normalize_http_uri("http://example.com///path"), "http://example.com/path");

    // ===== Multiple consecutive slashes in different positions =====
    EXPECT_EQ(doris::normalize_http_uri("https://host.com//bucket//prefix"),
              "https://host.com/bucket/prefix");
    EXPECT_EQ(doris::normalize_http_uri("https://host.com///bucket///prefix///"),
              "https://host.com/bucket/prefix/");
    EXPECT_EQ(doris::normalize_http_uri("https://host.com////bucket////prefix////file"),
              "https://host.com/bucket/prefix/file");

    // ===== Azure blob storage specific URLs =====
    EXPECT_EQ(doris::normalize_http_uri("https://account.blob.core.windows.net//container"),
              "https://account.blob.core.windows.net/container");
    EXPECT_EQ(
            doris::normalize_http_uri("https://account.blob.core.windows.net///container//prefix"),
            "https://account.blob.core.windows.net/container/prefix");
    EXPECT_EQ(doris::normalize_http_uri(
                      "https://account.blob.core.windows.net////container///prefix///file.txt"),
              "https://account.blob.core.windows.net/container/prefix/file.txt");

    // ===== URLs without protocol =====
    EXPECT_EQ(doris::normalize_http_uri("example.com//path"), "example.com/path");
    EXPECT_EQ(doris::normalize_http_uri("host.com///bucket//prefix"), "host.com/bucket/prefix");
    EXPECT_EQ(doris::normalize_http_uri("//path//to//file"), "/path/to/file");

    // ===== Edge cases =====
    // Empty string
    EXPECT_EQ(doris::normalize_http_uri(""), "");

    // Only protocol
    EXPECT_EQ(doris::normalize_http_uri("https://"), "https://");
    EXPECT_EQ(doris::normalize_http_uri("http://"), "http://");

    // Only slashes
    EXPECT_EQ(doris::normalize_http_uri("//"), "/");
    EXPECT_EQ(doris::normalize_http_uri("///"), "/");
    EXPECT_EQ(doris::normalize_http_uri("////"), "/");

    // Single character paths
    EXPECT_EQ(doris::normalize_http_uri("https://a"), "https://a");
    EXPECT_EQ(doris::normalize_http_uri("https://a/"), "https://a/");
    EXPECT_EQ(doris::normalize_http_uri("https://a//"), "https://a/");

    // ===== Protocol preservation =====
    // Ensure protocol :// is never modified
    EXPECT_EQ(doris::normalize_http_uri("https://example.com"), "https://example.com");
    EXPECT_EQ(doris::normalize_http_uri("http://example.com"), "http://example.com");

    // Even with extra slashes after protocol
    EXPECT_EQ(doris::normalize_http_uri("https:///example.com"), "https://example.com");
    EXPECT_EQ(doris::normalize_http_uri("http:///example.com"), "http://example.com");

    // Mixed case protocol (though unusual)
    EXPECT_EQ(doris::normalize_http_uri("HTTP://example.com//path"), "HTTP://example.com/path");
    EXPECT_EQ(doris::normalize_http_uri("HTTPS://example.com//path"), "HTTPS://example.com/path");

    // ===== Trailing slashes =====
    EXPECT_EQ(doris::normalize_http_uri("https://example.com/path/"), "https://example.com/path/");
    EXPECT_EQ(doris::normalize_http_uri("https://example.com/path//"), "https://example.com/path/");
    EXPECT_EQ(doris::normalize_http_uri("https://example.com/path///"),
              "https://example.com/path/");
    EXPECT_EQ(doris::normalize_http_uri("https://example.com/path////"),
              "https://example.com/path/");

    // ===== Complex real-world scenarios =====
    // Simulating common configuration mistakes
    EXPECT_EQ(doris::normalize_http_uri("https://endpoint.com///bucket//prefix//file.txt"),
              "https://endpoint.com/bucket/prefix/file.txt");

    // User configured endpoint with trailing slash + bucket with leading slash
    EXPECT_EQ(doris::normalize_http_uri("https://endpoint.com///bucket"),
              "https://endpoint.com/bucket");

    // Multiple slashes everywhere
    EXPECT_EQ(
            doris::normalize_http_uri("https://host.com////bucket////prefix////subfolder////file"),
            "https://host.com/bucket/prefix/subfolder/file");

    // ===== Special characters in path =====
    EXPECT_EQ(
            doris::normalize_http_uri("https://example.com//path-with-dash//file_with_underscore"),
            "https://example.com/path-with-dash/file_with_underscore");
    EXPECT_EQ(doris::normalize_http_uri("https://example.com//path.with.dots//file@special"),
              "https://example.com/path.with.dots/file@special");
    EXPECT_EQ(doris::normalize_http_uri("https://example.com//bucket123//prefix456//file789"),
              "https://example.com/bucket123/prefix456/file789");

    // ===== URLs with query parameters and fragments =====
    EXPECT_EQ(doris::normalize_http_uri("https://example.com//path?query=value"),
              "https://example.com/path?query=value");
    EXPECT_EQ(doris::normalize_http_uri("https://example.com//path#fragment"),
              "https://example.com/path#fragment");
    EXPECT_EQ(doris::normalize_http_uri("https://example.com//path?query=value#fragment"),
              "https://example.com/path?query=value#fragment");
}

TEST(UtilTest, test_long_normalize_http_uri) {
    std::string longPath = "https://example.com";
    for (int i = 0; i < 100; i++) {
        longPath += "//segment" + std::to_string(i);
    }

    std::string expected = "https://example.com";
    for (int i = 0; i < 100; i++) {
        expected += "/segment" + std::to_string(i);
    }

    EXPECT_EQ(doris::normalize_http_uri(longPath), expected);
}