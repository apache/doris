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

#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "util/logging.h"
#include "util/lru_cache.hpp"

namespace doris {

class LruCacheTest : public testing::Test {};

struct Foo {
    Foo(int num_param) : num(num_param) {}
    int num;
};

TEST_F(LruCacheTest, NormalTest) {
    LruCache<int, std::shared_ptr<Foo>> cache(10);

    for (int i = 0; i < 10; ++i) {
        cache.put(i, std::shared_ptr<Foo>(new Foo(i * 10)));
    }
    ASSERT_EQ(10, cache.size());

    std::shared_ptr<Foo> ptr;
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(cache.get(i, &ptr));
        ASSERT_EQ(i * 10, ptr->num);
    }
}

TEST_F(LruCacheTest, IteratorTest) {
    LruCache<int, std::shared_ptr<Foo>> cache(10);

    int keys = 0;
    int values = 0;
    for (int i = 0; i < 10; ++i) {
        keys += i;
        values += i * 10;
        cache.put(i, std::shared_ptr<Foo>(new Foo(i * 10)));
    }
    ASSERT_EQ(10, cache.size());

    int key_sum = 0;
    int value_sum = 0;
    for (auto& it : cache) {
        key_sum += it.first;
        value_sum += it.second->num;
    }
    ASSERT_EQ(keys, key_sum);
    ASSERT_EQ(values, value_sum);

    std::shared_ptr<Foo> ptr;
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(cache.get(i, &ptr));
        ASSERT_EQ(i * 10, ptr->num);
    }
}

TEST_F(LruCacheTest, OverSize) {
    LruCache<int, std::shared_ptr<Foo>> cache(10);

    for (int i = 0; i < 110; ++i) {
        cache.put(i, std::shared_ptr<Foo>(new Foo(i * 10)));
    }
    ASSERT_EQ(10, cache.size());

    std::shared_ptr<Foo> ptr;
    for (int i = 100; i < 110; ++i) {
        ASSERT_TRUE(cache.get(i, &ptr));
        ASSERT_EQ(i * 10, ptr->num);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
