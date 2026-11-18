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

#include "testutil/scoped_file_cache.h"

#include <gtest/gtest.h>

#include "util/defer_op.h"

namespace doris::test {

TEST(ScopedFileCacheTest, InitializesWithoutAnExistingCacheAndRestoresState) {
    auto* env = ExecEnv::GetInstance();
    auto* original_factory = env->file_cache_factory();
    bool original_enable_file_cache = config::enable_file_cache;
    io::FileCacheFactory empty_factory;
    Defer restore {[&] {
        env->set_file_cache_factory(original_factory);
        config::enable_file_cache = original_enable_file_cache;
    }};

    // A standalone run has no factory; another fixture can leave an empty factory instead.
    for (auto* previous_factory : {static_cast<io::FileCacheFactory*>(nullptr), &empty_factory}) {
        for (bool enabled : {false, true}) {
            env->set_file_cache_factory(previous_factory);
            config::enable_file_cache = enabled;
            {
                ScopedFileCache cache;
                auto st = cache.init();
                ASSERT_TRUE(st.ok()) << st;
                EXPECT_TRUE(config::enable_file_cache);
                auto* factory = env->file_cache_factory();
                ASSERT_NE(factory, nullptr);
                ASSERT_EQ(factory->get_cache_instance_size(), 1);
                EXPECT_NE(factory->get_by_path(io::BlockFileCache::hash("test.dat")), nullptr);
            }
            EXPECT_EQ(env->file_cache_factory(), previous_factory);
            EXPECT_EQ(config::enable_file_cache, enabled);
            EXPECT_EQ(empty_factory.get_cache_instance_size(), 0);
        }
    }
}

TEST(ScopedFileCacheTest, PreservesAnExistingPopulatedCache) {
    ScopedFileCache previous_cache;
    auto st = previous_cache.init();
    ASSERT_TRUE(st.ok()) << st;
    auto* previous_factory = ExecEnv::GetInstance()->file_cache_factory();
    auto key = io::BlockFileCache::hash("test.dat");
    auto* previous_instance = previous_factory->get_by_path(key);

    {
        ScopedFileCache cache;
        st = cache.init();
        ASSERT_TRUE(st.ok()) << st;
        auto* factory = ExecEnv::GetInstance()->file_cache_factory();
        EXPECT_EQ(factory, previous_factory);
        EXPECT_EQ(factory->get_by_path(key), previous_instance);
    }

    EXPECT_EQ(ExecEnv::GetInstance()->file_cache_factory(), previous_factory);
    EXPECT_EQ(previous_factory->get_by_path(key), previous_instance);
    EXPECT_TRUE(config::enable_file_cache);
}

} // namespace doris::test
