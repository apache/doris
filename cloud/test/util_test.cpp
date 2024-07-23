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

#include "recycler/util.h"

#include <chrono>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/simple_thread_pool.h"
#include "common/string_util.h"
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
