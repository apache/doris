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

#include "format/format_common.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace doris {

TEST(KVCacheTest, DifferentKeysOnSameShardLoadConcurrently) {
    ShardedKVCache cache(1);
    std::promise<void> first_entered;
    std::promise<void> release_first;
    std::promise<void> second_entered;
    auto first_entered_future = first_entered.get_future();
    auto release_first_future = release_first.get_future().share();
    auto second_entered_future = second_entered.get_future();

    std::thread first([&]() {
        cache.get<int>("first", [&]() -> int* {
            first_entered.set_value();
            release_first_future.wait();
            return new int(1);
        });
    });
    first_entered_future.wait();

    std::thread second([&]() {
        cache.get<int>("second", [&]() -> int* {
            second_entered.set_value();
            return new int(2);
        });
    });

    bool second_loaded_before_first =
            second_entered_future.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    release_first.set_value();
    first.join();
    second.join();

    EXPECT_TRUE(second_loaded_before_first);
}

TEST(KVCacheTest, SameKeyLoadsOnceForConcurrentCallers) {
    constexpr int num_callers = 8;
    ShardedKVCache cache(1);
    std::atomic<int> factory_calls {0};
    std::promise<void> factory_entered;
    std::promise<void> release_factory;
    auto factory_entered_future = factory_entered.get_future();
    auto release_factory_future = release_factory.get_future().share();
    std::vector<int*> values(num_callers);
    std::vector<std::thread> callers;

    for (int i = 0; i < num_callers; ++i) {
        callers.emplace_back([&, i]() {
            values[i] = cache.get<int>("shared", [&]() -> int* {
                if (++factory_calls == 1) {
                    factory_entered.set_value();
                }
                release_factory_future.wait();
                return new int(7);
            });
        });
    }

    factory_entered_future.wait();
    release_factory.set_value();
    for (auto& caller : callers) {
        caller.join();
    }

    EXPECT_EQ(factory_calls.load(), 1);
    ASSERT_NE(values.front(), nullptr);
    for (int* value : values) {
        EXPECT_EQ(value, values.front());
    }
}

TEST(KVCacheTest, FailedFactoryDoesNotPoisonEntry) {
    ShardedKVCache cache(1);
    std::atomic<int> factory_calls {0};
    std::promise<void> failed_factory_entered;
    std::promise<void> release_failed_factory;
    auto failed_factory_entered_future = failed_factory_entered.get_future();
    auto release_failed_factory_future = release_failed_factory.get_future().share();
    bool first_cache_hit = true;
    bool second_cache_hit = true;
    int* second_value = nullptr;

    std::thread first([&]() {
        EXPECT_EQ(cache.get<int>(
                          "retry",
                          [&]() -> int* {
                              if (++factory_calls == 1) {
                                  failed_factory_entered.set_value();
                              }
                              release_failed_factory_future.wait();
                              return nullptr;
                          },
                          &first_cache_hit),
                  nullptr);
    });
    failed_factory_entered_future.wait();

    std::thread second([&]() {
        second_value = cache.get<int>(
                "retry",
                [&]() -> int* {
                    ++factory_calls;
                    return new int(9);
                },
                &second_cache_hit);
    });

    release_failed_factory.set_value();
    first.join();
    second.join();

    EXPECT_EQ(factory_calls.load(), 2);
    EXPECT_FALSE(first_cache_hit);
    EXPECT_FALSE(second_cache_hit);
    ASSERT_NE(second_value, nullptr);
    EXPECT_EQ(*second_value, 9);
}

TEST(KVCacheTest, SameStringKeyIsIsolatedByValueType) {
    struct FirstValue {
        int value = 1;
    };
    struct SecondValue {
        int value = 2;
    };

    ShardedKVCache cache(1);
    auto* first =
            cache.get<FirstValue>("same-key", []() -> FirstValue* { return new FirstValue(); });
    auto* second =
            cache.get<SecondValue>("same-key", []() -> SecondValue* { return new SecondValue(); });

    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(first->value, 1);
    EXPECT_EQ(second->value, 2);
    EXPECT_EQ(cache.get<FirstValue>("same-key",
                                    []() -> FirstValue* {
                                        ADD_FAILURE() << "cached FirstValue was rebuilt";
                                        return nullptr;
                                    }),
              first);
}

} // namespace doris
