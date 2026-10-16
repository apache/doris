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

// ============================================================
// Benchmark: FixedContainer vs DynamicContainer (from hybrid_set.h)
//
// Measures find() performance for different container sizes (1-8)
// and element types (int32_t, int64_t, std::string).
// ============================================================

#pragma once

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "exprs/create_predicate_function.h"
#include "exprs/expr_zonemap_filter.h"
#include "exprs/hybrid_set.h"
#include "exprs/vliteral.h"

namespace doris {

// ============================================================
// Test data generators
// ============================================================

template <typename T>
static std::vector<T> generate_values(size_t n);

template <>
std::vector<int32_t> generate_values<int32_t>(size_t n) {
    std::vector<int32_t> vals;
    vals.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        vals.push_back(static_cast<int32_t>(i * 7 + 13));
    }
    return vals;
}

template <>
std::vector<int64_t> generate_values<int64_t>(size_t n) {
    std::vector<int64_t> vals;
    vals.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        vals.push_back(static_cast<int64_t>(i * 7 + 13));
    }
    return vals;
}

template <>
std::vector<std::string> generate_values<std::string>(size_t n) {
    std::vector<std::string> vals;
    vals.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        vals.push_back("key_" + std::to_string(i * 7 + 13));
    }
    return vals;
}

// Number of find() calls per iteration to amortize loop overhead.
static constexpr size_t FIND_ITERS = 10000;

enum class StringSetWorkload { SHORT, LONG, EMBEDDED_NUL };
enum class StringSetLookup { HIT, MISS };

template <StringSetWorkload workload>
static std::vector<std::string> generate_string_set_values(size_t n, bool misses) {
    std::vector<std::string> values;
    values.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        const auto key = std::to_string(i);
        if constexpr (workload == StringSetWorkload::SHORT) {
            values.emplace_back((misses ? "miss_" : "hit_") + key);
        } else if constexpr (workload == StringSetWorkload::LONG) {
            std::string value(192, static_cast<char>('a' + i % 26));
            value.append(misses ? "_miss_" : "_hit_");
            value.append(key);
            values.emplace_back(std::move(value));
        } else {
            std::string value = misses ? "miss" : "hit";
            value.push_back('\0');
            value.append("binary_");
            value.append(key);
            value.push_back('\0');
            value.append(32, static_cast<char>('a' + i % 26));
            values.emplace_back(std::move(value));
        }
    }
    return values;
}

template <StringSetWorkload workload, StringSetLookup lookup>
static void BM_StringSet_StringRefFind(benchmark::State& state) {
    const auto set_size = static_cast<size_t>(state.range(0));
    const auto hit_values = generate_string_set_values<workload>(set_size, false);
    const auto miss_values = generate_string_set_values<workload>(set_size, true);

    StringSet<> set(false);
    std::vector<StringRef> hit_refs;
    std::vector<StringRef> miss_refs;
    hit_refs.reserve(set_size);
    miss_refs.reserve(set_size);
    for (size_t i = 0; i < set_size; ++i) {
        StringRef value(hit_values[i]);
        set.insert(&value);
        hit_refs.emplace_back(hit_values[i]);
        miss_refs.emplace_back(miss_values[i]);
    }

    const auto& lookup_refs = lookup == StringSetLookup::HIT ? hit_refs : miss_refs;
    const size_t lookup_iters = std::max(FIND_ITERS, set_size);
    const size_t expected_found = lookup == StringSetLookup::HIT ? lookup_iters : 0;
    for (auto _ : state) {
        size_t found = 0;
        for (size_t i = 0; i < lookup_iters; ++i) {
            const auto index = i % set_size;
            found += set.find(&lookup_refs[index]);
        }
        benchmark::DoNotOptimize(found);
        if (found != expected_found) {
            state.SkipWithError("StringSet lookup returned an unexpected result");
            break;
        }
    }
    state.SetItemsProcessed(state.iterations() * lookup_iters);
}

static void BM_StringSet_StringRefFindShortHit(benchmark::State& state) {
    BM_StringSet_StringRefFind<StringSetWorkload::SHORT, StringSetLookup::HIT>(state);
}

static void BM_StringSet_StringRefFindShortMiss(benchmark::State& state) {
    BM_StringSet_StringRefFind<StringSetWorkload::SHORT, StringSetLookup::MISS>(state);
}

static void BM_StringSet_StringRefFindLongHit(benchmark::State& state) {
    BM_StringSet_StringRefFind<StringSetWorkload::LONG, StringSetLookup::HIT>(state);
}

static void BM_StringSet_StringRefFindLongMiss(benchmark::State& state) {
    BM_StringSet_StringRefFind<StringSetWorkload::LONG, StringSetLookup::MISS>(state);
}

static void BM_StringSet_StringRefFindEmbeddedNulHit(benchmark::State& state) {
    BM_StringSet_StringRefFind<StringSetWorkload::EMBEDDED_NUL, StringSetLookup::HIT>(state);
}

static void BM_StringSet_StringRefFindEmbeddedNulMiss(benchmark::State& state) {
    BM_StringSet_StringRefFind<StringSetWorkload::EMBEDDED_NUL, StringSetLookup::MISS>(state);
}

#define REGISTER_STRING_SET_LOOKUP(NAME) \
    BENCHMARK(NAME)->Arg(64)->Arg(1024)->Arg(40960)->Unit(benchmark::kMicrosecond)

REGISTER_STRING_SET_LOOKUP(BM_StringSet_StringRefFindShortHit);
REGISTER_STRING_SET_LOOKUP(BM_StringSet_StringRefFindShortMiss);
REGISTER_STRING_SET_LOOKUP(BM_StringSet_StringRefFindLongHit);
REGISTER_STRING_SET_LOOKUP(BM_StringSet_StringRefFindLongMiss);
REGISTER_STRING_SET_LOOKUP(BM_StringSet_StringRefFindEmbeddedNulHit);
REGISTER_STRING_SET_LOOKUP(BM_StringSet_StringRefFindEmbeddedNulMiss);

// ============================================================
// FixedContainer benchmark: insert N values, then find them
// ============================================================

template <typename T, size_t N>
static void BM_FixedContainer_Find(benchmark::State& state) {
    auto values = generate_values<T>(N);

    // Also prepare a "miss" value for interleaving hit/miss lookups.
    auto miss_values = generate_values<T>(N + 4);

    for (auto _ : state) {
        FixedContainer<T, N> container;
        for (size_t i = 0; i < N; ++i) {
            container.insert(values[i]);
        }

        int64_t found = 0;
        for (size_t iter = 0; iter < FIND_ITERS; ++iter) {
            // Hit: search for existing value
            found += container.find(values[iter % N]);
            // Miss: search for non-existing value
            found += container.find(miss_values[N + (iter % 4)]);
        }
        benchmark::DoNotOptimize(found);
    }
}

// ============================================================
// DynamicContainer benchmark: insert N values, then find them
// ============================================================

template <typename T, size_t N>
static void BM_DynamicContainer_Find(benchmark::State& state) {
    auto values = generate_values<T>(N);
    auto miss_values = generate_values<T>(N + 4);

    for (auto _ : state) {
        DynamicContainer<T> container;
        for (size_t i = 0; i < N; ++i) {
            container.insert(values[i]);
        }

        int64_t found = 0;
        for (size_t iter = 0; iter < FIND_ITERS; ++iter) {
            found += container.find(values[iter % N]);
            found += container.find(miss_values[N + (iter % 4)]);
        }
        benchmark::DoNotOptimize(found);
    }
}

// ============================================================
// Register benchmarks for int32_t
// ============================================================

#define REGISTER_FIXED_INT32(N)                     \
    BENCHMARK(BM_FixedContainer_Find<int32_t, N>)   \
            ->Name("Fixed_Int32_N" #N)              \
            ->Unit(benchmark::kMicrosecond);        \
    BENCHMARK(BM_DynamicContainer_Find<int32_t, N>) \
            ->Name("Dynamic_Int32_N" #N)            \
            ->Unit(benchmark::kMicrosecond);

REGISTER_FIXED_INT32(1)
REGISTER_FIXED_INT32(2)
REGISTER_FIXED_INT32(3)
REGISTER_FIXED_INT32(4)
REGISTER_FIXED_INT32(5)
REGISTER_FIXED_INT32(6)
REGISTER_FIXED_INT32(7)
REGISTER_FIXED_INT32(8)

// ============================================================
// Register benchmarks for int64_t
// ============================================================

#define REGISTER_FIXED_INT64(N)                     \
    BENCHMARK(BM_FixedContainer_Find<int64_t, N>)   \
            ->Name("Fixed_Int64_N" #N)              \
            ->Unit(benchmark::kMicrosecond);        \
    BENCHMARK(BM_DynamicContainer_Find<int64_t, N>) \
            ->Name("Dynamic_Int64_N" #N)            \
            ->Unit(benchmark::kMicrosecond);

REGISTER_FIXED_INT64(1)
REGISTER_FIXED_INT64(2)
REGISTER_FIXED_INT64(3)
REGISTER_FIXED_INT64(4)
REGISTER_FIXED_INT64(5)
REGISTER_FIXED_INT64(6)
REGISTER_FIXED_INT64(7)
REGISTER_FIXED_INT64(8)

// ============================================================
// Register benchmarks for std::string
// ============================================================

#define REGISTER_FIXED_STRING(N)                        \
    BENCHMARK(BM_FixedContainer_Find<std::string, N>)   \
            ->Name("Fixed_String_N" #N)                 \
            ->Unit(benchmark::kMicrosecond);            \
    BENCHMARK(BM_DynamicContainer_Find<std::string, N>) \
            ->Name("Dynamic_String_N" #N)               \
            ->Unit(benchmark::kMicrosecond);

REGISTER_FIXED_STRING(1)
REGISTER_FIXED_STRING(2)
REGISTER_FIXED_STRING(3)
REGISTER_FIXED_STRING(4)
REGISTER_FIXED_STRING(5)
REGISTER_FIXED_STRING(6)
REGISTER_FIXED_STRING(7)
REGISTER_FIXED_STRING(8)

// ============================================================
// Metadata-pruning snapshot benchmark
// ============================================================

struct LegacyInZonemapSnapshot {
    bool contains_null = false;
    std::vector<Field> values;
    Field min_value;
    Field max_value;
};

static void materialize_hybrid_set_legacy(HybridSetBase& set, const DataTypePtr& data_type,
                                          LegacyInZonemapSnapshot* result) {
    DORIS_CHECK(result != nullptr);
    DORIS_CHECK(data_type != nullptr);
    const auto value_type = remove_nullable(data_type);
    DORIS_CHECK(value_type != nullptr);

    result->contains_null = set.contain_null();
    result->values.clear();
    result->min_value = Field();
    result->max_value = Field();

    auto* iterator = set.begin();
    while (iterator->has_next()) {
        const void* value = iterator->get_value();
        if (value != nullptr) {
            TExprNode literal_node = expr_zonemap::create_texpr_node_from_hybrid_set_value(
                    value, value_type->get_primitive_type(), value_type->get_precision(),
                    value_type->get_scale());
            auto literal = VLiteral::create_shared(literal_node);
            Field field;
            literal->get_column_ptr()->get(0, field);
            result->values.emplace_back(std::move(field));
        }
        iterator->next();
    }

    if (!result->values.empty()) {
        const auto minmax = std::ranges::minmax_element(
                result->values, [](const Field& lhs, const Field& rhs) { return lhs < rhs; });
        result->min_value = *minmax.min;
        result->max_value = *minmax.max;
    }
}

static void BM_HybridSet_GetMinMaxInt32(benchmark::State& state) {
    HybridSet<TYPE_INT> set(false);
    const auto values = generate_values<int32_t>(state.range(0));
    for (const auto value : values) {
        set.insert(&value);
    }

    for (auto _ : state) {
        Field min_value;
        Field max_value;
        bool contains_nan = false;
        set.get_min_max(min_value, max_value, contains_nan);
        benchmark::DoNotOptimize(min_value);
        benchmark::DoNotOptimize(max_value);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_HybridSet_GetMinMaxInt32)
        ->Arg(64)
        ->Arg(65)
        ->Arg(1024)
        ->Arg(40960)
        ->Unit(benchmark::kMicrosecond);

static void BM_HybridSet_LegacyMaterializeInt32(benchmark::State& state) {
    HybridSet<TYPE_INT> set(false);
    const auto values = generate_values<int32_t>(state.range(0));
    for (const auto value : values) {
        set.insert(&value);
    }
    const auto data_type = std::make_shared<DataTypeInt32>();

    for (auto _ : state) {
        LegacyInZonemapSnapshot materialized;
        materialize_hybrid_set_legacy(set, data_type, &materialized);
        benchmark::DoNotOptimize(materialized.values);
        benchmark::DoNotOptimize(materialized.min_value);
        benchmark::DoNotOptimize(materialized.max_value);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_HybridSet_LegacyMaterializeInt32)
        ->Arg(64)
        ->Arg(65)
        ->Arg(1024)
        ->Arg(40960)
        ->Unit(benchmark::kMicrosecond);

static void BM_StringSet_GetMinMaxLong(benchmark::State& state) {
    StringSet<> set(false);
    const auto values = generate_string_set_values<StringSetWorkload::LONG>(
            static_cast<size_t>(state.range(0)), false);
    for (const auto& value : values) {
        StringRef string_ref(value);
        set.insert(&string_ref);
    }

    for (auto _ : state) {
        Field min_value;
        Field max_value;
        bool contains_nan = false;
        set.get_min_max(min_value, max_value, contains_nan);
        benchmark::DoNotOptimize(min_value);
        benchmark::DoNotOptimize(max_value);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_StringSet_GetMinMaxLong)
        ->Arg(65)
        ->Arg(1024)
        ->Arg(40960)
        ->Unit(benchmark::kMicrosecond);

static void BM_StringSet_LegacyMaterializeLong(benchmark::State& state) {
    StringSet<> set(false);
    const auto values = generate_string_set_values<StringSetWorkload::LONG>(
            static_cast<size_t>(state.range(0)), false);
    for (const auto& value : values) {
        StringRef string_ref(value);
        set.insert(&string_ref);
    }
    const auto data_type = std::make_shared<DataTypeString>();

    for (auto _ : state) {
        LegacyInZonemapSnapshot materialized;
        materialize_hybrid_set_legacy(set, data_type, &materialized);
        benchmark::DoNotOptimize(materialized.values);
        benchmark::DoNotOptimize(materialized.min_value);
        benchmark::DoNotOptimize(materialized.max_value);
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_StringSet_LegacyMaterializeLong)
        ->Arg(65)
        ->Arg(1024)
        ->Arg(40960)
        ->Unit(benchmark::kMicrosecond);

// ============================================================
// Typed range lookup benchmark
// ============================================================

enum class InZonemapRangeLookup { POINT_HIT, FULL_MISS };

template <InZonemapRangeLookup lookup>
static void BM_HybridSet_ContainsAnyInRangeInt32(benchmark::State& state) {
    const auto set_size = static_cast<size_t>(state.range(0));
    HybridSet<TYPE_INT> set(false);
    const auto values = generate_values<int32_t>(set_size);
    for (const auto value : values) {
        set.insert(&value);
    }

    const int32_t middle_value = values[set_size / 2];
    const auto min_value = Field::create_field<TYPE_INT>(
            lookup == InZonemapRangeLookup::POINT_HIT ? middle_value : middle_value + 1);
    const auto max_value = Field::create_field<TYPE_INT>(
            lookup == InZonemapRangeLookup::POINT_HIT ? middle_value : middle_value + 6);
    constexpr bool expected = lookup == InZonemapRangeLookup::POINT_HIT;
    DORIS_CHECK_EQ(set.contains_any_in_range(min_value, max_value), expected);

    for (auto _ : state) {
        bool result = set.contains_any_in_range(min_value, max_value);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}

static void BM_HybridSet_ContainsAnyInRangeInt32PointHit(benchmark::State& state) {
    BM_HybridSet_ContainsAnyInRangeInt32<InZonemapRangeLookup::POINT_HIT>(state);
}

static void BM_HybridSet_ContainsAnyInRangeInt32FullMiss(benchmark::State& state) {
    BM_HybridSet_ContainsAnyInRangeInt32<InZonemapRangeLookup::FULL_MISS>(state);
}

// kInZoneMapPointCheckThreshold gates on the total set size: a fixed 10,000-element set takes the
// range-only path for every candidate below 10,000. Use each candidate as the set size to measure
// the exact-lookup cost admitted at that boundary; 10,000 is an upper control rather than a
// threshold candidate. FULL_MISS forces traversal of the whole set, while POINT_HIT measures the
// early-exit case.
#define REGISTER_IN_ZONEMAP_RANGE_LOOKUP(NAME) \
    BENCHMARK(NAME)                            \
            ->Arg(64)                          \
            ->Arg(128)                         \
            ->Arg(256)                         \
            ->Arg(512)                         \
            ->Arg(1024)                        \
            ->Arg(2048)                        \
            ->Arg(4096)                        \
            ->Arg(8192)                        \
            ->Arg(10000)                       \
            ->Unit(benchmark::kNanosecond)

REGISTER_IN_ZONEMAP_RANGE_LOOKUP(BM_HybridSet_ContainsAnyInRangeInt32PointHit);
REGISTER_IN_ZONEMAP_RANGE_LOOKUP(BM_HybridSet_ContainsAnyInRangeInt32FullMiss);

// Measure only the native HybridSet traversal used by Bloom pruning. The lightweight fingerprint
// predicate keeps storage Bloom-filter implementation costs outside this microbenchmark; both the
// predicate and its captured fingerprint are constructed outside the timed loop.

static uint64_t raw_bloom_fingerprint(const char* data, size_t size) {
    uint64_t fingerprint = 14695981039346656037ULL;
    for (size_t i = 0; i < size; ++i) {
        fingerprint ^= static_cast<uint8_t>(data[i]);
        fingerprint *= 1099511628211ULL;
    }
    return fingerprint;
}

enum class RawBloomProbeLookup { FULL_MISS, HIT };

template <RawBloomProbeLookup lookup>
static void BM_HybridSet_AnyMatchRawInt32(benchmark::State& state) {
    const auto set_size = static_cast<size_t>(state.range(0));
    std::shared_ptr<HybridSetBase> set(create_set(TYPE_INT, false));
    const auto values = generate_values<int32_t>(set_size);
    for (const auto value : values) {
        set->insert(&value);
    }

    const int32_t target =
            lookup == RawBloomProbeLookup::HIT ? values[set_size / 2] : values.back() + 1;
    const auto target_fingerprint =
            raw_bloom_fingerprint(reinterpret_cast<const char*>(&target), sizeof(target));
    const auto predicate = [target_fingerprint](const char* data, size_t size) {
        return raw_bloom_fingerprint(data, size) == target_fingerprint;
    };
    constexpr bool expected = lookup == RawBloomProbeLookup::HIT;
    DORIS_CHECK_EQ(set->any_match_raw(TYPE_INT, predicate), expected);

    for (auto _ : state) {
        bool found = set->any_match_raw(TYPE_INT, predicate);
        benchmark::DoNotOptimize(found);
    }
    state.SetItemsProcessed(state.iterations());
}

static void BM_HybridSet_AnyMatchRawInt32FullMiss(benchmark::State& state) {
    BM_HybridSet_AnyMatchRawInt32<RawBloomProbeLookup::FULL_MISS>(state);
}

static void BM_HybridSet_AnyMatchRawInt32Hit(benchmark::State& state) {
    BM_HybridSet_AnyMatchRawInt32<RawBloomProbeLookup::HIT>(state);
}

template <StringSetWorkload workload, RawBloomProbeLookup lookup>
static void BM_StringSet_AnyMatchRaw(benchmark::State& state) {
    const auto set_size = static_cast<size_t>(state.range(0));
    std::shared_ptr<HybridSetBase> set(create_set(TYPE_STRING, false));
    const auto values = generate_string_set_values<workload>(set_size, false);
    const auto misses = generate_string_set_values<workload>(set_size, true);
    for (const auto& value : values) {
        StringRef string_ref(value);
        set->insert(&string_ref);
    }

    const auto& target =
            lookup == RawBloomProbeLookup::HIT ? values[set_size / 2] : misses[set_size / 2];
    const auto target_fingerprint = raw_bloom_fingerprint(target.data(), target.size());
    const auto predicate = [target_fingerprint](const char* data, size_t size) {
        return raw_bloom_fingerprint(data, size) == target_fingerprint;
    };
    constexpr bool expected = lookup == RawBloomProbeLookup::HIT;
    DORIS_CHECK_EQ(set->any_match_raw(TYPE_STRING, predicate), expected);

    for (auto _ : state) {
        bool found = set->any_match_raw(TYPE_STRING, predicate);
        benchmark::DoNotOptimize(found);
    }
    state.SetItemsProcessed(state.iterations());
}

static void BM_StringSet_AnyMatchRawLongFullMiss(benchmark::State& state) {
    BM_StringSet_AnyMatchRaw<StringSetWorkload::LONG, RawBloomProbeLookup::FULL_MISS>(state);
}

static void BM_StringSet_AnyMatchRawLongHit(benchmark::State& state) {
    BM_StringSet_AnyMatchRaw<StringSetWorkload::LONG, RawBloomProbeLookup::HIT>(state);
}

static void BM_StringSet_AnyMatchRawEmbeddedNulFullMiss(benchmark::State& state) {
    BM_StringSet_AnyMatchRaw<StringSetWorkload::EMBEDDED_NUL, RawBloomProbeLookup::FULL_MISS>(
            state);
}

static void BM_StringSet_AnyMatchRawEmbeddedNulHit(benchmark::State& state) {
    BM_StringSet_AnyMatchRaw<StringSetWorkload::EMBEDDED_NUL, RawBloomProbeLookup::HIT>(state);
}

#define REGISTER_RAW_BLOOM_PROBE(NAME) \
    BENCHMARK(NAME)->Arg(64)->Arg(1024)->Arg(40960)->Unit(benchmark::kMicrosecond)

REGISTER_RAW_BLOOM_PROBE(BM_HybridSet_AnyMatchRawInt32FullMiss);
REGISTER_RAW_BLOOM_PROBE(BM_HybridSet_AnyMatchRawInt32Hit);
REGISTER_RAW_BLOOM_PROBE(BM_StringSet_AnyMatchRawLongFullMiss);
REGISTER_RAW_BLOOM_PROBE(BM_StringSet_AnyMatchRawLongHit);
REGISTER_RAW_BLOOM_PROBE(BM_StringSet_AnyMatchRawEmbeddedNulFullMiss);
REGISTER_RAW_BLOOM_PROBE(BM_StringSet_AnyMatchRawEmbeddedNulHit);

#undef REGISTER_FIXED_INT32
#undef REGISTER_FIXED_INT64
#undef REGISTER_FIXED_STRING
#undef REGISTER_STRING_SET_LOOKUP
#undef REGISTER_IN_ZONEMAP_RANGE_LOOKUP
#undef REGISTER_RAW_BLOOM_PROBE

} // namespace doris
