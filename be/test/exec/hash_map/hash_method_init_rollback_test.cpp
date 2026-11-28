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

#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <new>
#include <utility>
#include <variant>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exec/common/agg_utils.h"
#include "exec/common/hash_table/hash_map_context.h"
#include "exec/common/hash_table/hash_map_util.h"
#include "exec/pipeline/dependency.h"

namespace doris {
namespace {

using Map = AggData<UInt32>;
using MethodBase = MethodBaseInner<Map>;
using Method = MethodOneNumber<UInt32, Map>;
using InitHook = MethodBase::HashTableInitForTest;

class ScopedHashTableInit {
public:
    explicit ScopedHashTableInit(InitHook hook)
            : _hook(std::move(hook)), _previous(MethodBase::_hash_table_init_for_test) {
        MethodBase::_hash_table_init_for_test = &_hook;
    }
    ~ScopedHashTableInit() { MethodBase::_hash_table_init_for_test = _previous; }
    ScopedHashTableInit(const ScopedHashTableInit&) = delete;
    ScopedHashTableInit& operator=(const ScopedHashTableInit&) = delete;

private:
    InitHook _hook;
    const InitHook* _previous;
};

enum class FailureSite { MapObject, ControlBlock };

struct TaggedBadAlloc : std::bad_alloc {
    TaggedBadAlloc(FailureSite site_, const void* marker_) : site(site_), marker(marker_) {}
    FailureSite site;
    const void* marker;
};

struct AllocationStats {
    int hook_hits = 0;
    int maps_created = 0;
    int control_block_attempts = 0;
    int maps_deleted = 0;
};

template <typename T>
struct FailingControlBlockAllocator {
    using value_type = T;
    explicit FailingControlBlockAllocator(AllocationStats& stats_) noexcept : stats(&stats_) {}
    template <typename U>
    FailingControlBlockAllocator(const FailingControlBlockAllocator<U>& other) noexcept
            : stats(other.stats) {}
    T* allocate(size_t) {
        ++stats->control_block_attempts;
        throw TaggedBadAlloc(FailureSite::ControlBlock, stats);
    }
    void deallocate(T* ptr, size_t count) noexcept { std::allocator<T> {}.deallocate(ptr, count); }
    AllocationStats* stats;
};

void destroy_shared_state_after_failure(FailureSite site) {
    std::set_terminate([] {
        try {
            std::rethrow_exception(std::current_exception());
        } catch (const std::bad_variant_access&) {
            std::fprintf(stderr, "AggSharedState destructor: terminate(bad_variant_access)\n");
            std::_Exit(86);
        } catch (...) {
            std::_Exit(87);
        }
    });
    bool correct = false;
    {
        AllocationStats stats;
        AggSharedState state;
        // The destructor checks emptiness before visiting the variant; no expression is evaluated.
        state.probe_expr_ctxs.emplace_back();
        const std::vector<DataTypePtr> types {std::make_shared<DataTypeInt32>()};
        bool caught_original = false;
        {
            ScopedHashTableInit hook([&](std::shared_ptr<Map>& table, Map* (*allocate_map)()) {
                ++stats.hook_hits;
                if (site == FailureSite::MapObject) {
                    // Fail this constructor's object allocation, without replacing an allocator.
                    throw TaggedBadAlloc(site, &stats);
                }
                const auto deleter = [&stats](Map* ptr) noexcept {
                    ++stats.maps_deleted;
                    delete ptr;
                };
                const FailingControlBlockAllocator<Map> allocator(stats);
                Map* raw = allocate_map();
                ++stats.maps_created;
                // reset owns raw even when allocating its control block throws.
                table.reset(raw, deleter, allocator);
            });
            try {
                const Status status = init_hash_method(state.agg_data.get(), types, true);
                std::fprintf(stderr, "unexpected status: code=%d\n", status.code());
            } catch (const TaggedBadAlloc& error) {
                caught_original = error.site == site && error.marker == &stats;
            }
        }
        const auto& variant = state.agg_data->method_variant;
        const bool valueless = variant.valueless_by_exception();
        const bool monostate = std::holds_alternative<std::monostate>(variant);
        const int expected_maps = site == FailureSite::ControlBlock ? 1 : 0;
        correct = caught_original && !valueless && monostate && stats.hook_hits == 1 &&
                  stats.maps_created == expected_maps &&
                  stats.control_block_attempts == expected_maps &&
                  stats.maps_deleted == expected_maps;
        std::fprintf(stderr,
                     "site=%d original=%d valueless=%d monostate=%d hits=%d created=%d "
                     "control_attempts=%d deleted=%d\n",
                     static_cast<int>(site), caught_original, valueless, monostate, stats.hook_hits,
                     stats.maps_created, stats.control_block_attempts, stats.maps_deleted);
        // Always run the real destructor, even if a state or allocation assertion failed.
    }
    std::fprintf(stderr, "AggSharedState destructor completed\n");
    std::_Exit(correct ? 0 : 88);
}

TEST(HashMethodInitRollbackDeathTest, HashMapAllocationFailure) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(destroy_shared_state_after_failure(FailureSite::MapObject),
                testing::ExitedWithCode(0),
                "original=1 valueless=0 monostate=1 hits=1 created=0 control_attempts=0 deleted=0");
}

TEST(HashMethodInitRollbackDeathTest, ControlBlockAllocationFailure) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(destroy_shared_state_after_failure(FailureSite::ControlBlock),
                testing::ExitedWithCode(0),
                "original=1 valueless=0 monostate=1 hits=1 created=1 control_attempts=1 deleted=1");
}

TEST(HashMethodInitRollbackTest, NormalAggregationInitialization) {
    AggregatedDataVariants data;
    const std::vector<DataTypePtr> types {std::make_shared<DataTypeInt32>()};
    ASSERT_TRUE(init_hash_method(&data, types, true).ok());
    ASSERT_TRUE(std::holds_alternative<Method>(data.method_variant));
    EXPECT_NE(std::get<Method>(data.method_variant).hash_table, nullptr);

    AggregatedDataVariants without_key;
    ASSERT_TRUE(init_hash_method(&without_key, {}, true).ok());
    EXPECT_TRUE(std::holds_alternative<std::monostate>(without_key.method_variant));
}

TEST(HashMethodInitRollbackTest, DorisExceptionKeepsStatus) {
    AggregatedDataVariants data;
    const std::vector<DataTypePtr> types {std::make_shared<DataTypeInt32>()};
    Exception source(ErrorCode::INTERNAL_ERROR, "hash table initialization failure");
    const Status expected = source.to_status();
    Status actual;
    {
        ScopedHashTableInit hook(
                [&](std::shared_ptr<Map>&, Map* (*)()) { throw std::move(source); });
        actual = init_hash_method(&data, types, true);
    }
    EXPECT_EQ(actual.code(), expected.code());
    EXPECT_EQ(actual.msg(), expected.msg());
    EXPECT_FALSE(data.method_variant.valueless_by_exception());
    EXPECT_TRUE(std::holds_alternative<std::monostate>(data.method_variant));
}

TEST(HashMethodInitRollbackTest, NonStandardExceptionIsPreserved) {
    struct Marker {
        const void* value;
    };
    AggregatedDataVariants data;
    const std::vector<DataTypePtr> types {std::make_shared<DataTypeInt32>()};
    const Marker source {&data};
    bool caught_original = false;
    {
        ScopedHashTableInit hook([&](std::shared_ptr<Map>&, Map* (*)()) { throw source; });
        try {
            const Status status = init_hash_method(&data, types, true);
            FAIL() << "Expected marker exception, got " << status;
        } catch (const Marker& error) {
            caught_original = error.value == source.value;
        }
    }
    EXPECT_TRUE(caught_original);
    EXPECT_FALSE(data.method_variant.valueless_by_exception());
    EXPECT_TRUE(std::holds_alternative<std::monostate>(data.method_variant));
}

} // namespace
} // namespace doris
