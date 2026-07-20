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

#pragma once

#include <benchmark/benchmark.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/compiler_util.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "exec/common/set_utils.h"

namespace doris {

namespace {

enum class SetHashTableInlineOperation {
    BUILD_COLD,
    BUILD_ALL_HIT,
    PROBE_ALL_HIT,
    PROBE_MISS_KEYS,
    PROBE_MIXED_KEYS_50_PERCENT,
};

enum class SetHashTableExecutionMode {
    ROW,
    // String sub-table groups are prepared outside the timed region.
    BATCH_CORE,
    // String sub-table grouping and hash-table operations are both timed.
    BATCH_WITH_GROUPING,
};

template <typename Method>
class SetHashTableMethodAdapterBase : public Method {
public:
    using Method::Method;

protected:
    template <typename State>
    auto ALWAYS_INLINE find_impl(State& state, size_t i) {
        this->template prefetch<true>(i);
        return state.find_key_with_hash(*this->hash_table, i, this->keys[i], this->hash_values[i]);
    }

    template <typename State, typename F, typename FF>
    auto ALWAYS_INLINE lazy_emplace_impl(State& state, size_t i, F&& creator,
                                         FF&& creator_for_null_key) {
        this->template prefetch<false>(i);
        return state.lazy_emplace_key(*this->hash_table, i, this->keys[i], this->hash_values[i],
                                      creator, creator_for_null_key);
    }
};

template <typename Method>
class SetHashTableNoInlineMethod : public SetHashTableMethodAdapterBase<Method> {
public:
    using SetHashTableMethodAdapterBase<Method>::SetHashTableMethodAdapterBase;

    template <typename State>
    auto NO_INLINE find(State& state, size_t i) {
        return this->find_impl(state, i);
    }

    template <typename State, typename F, typename FF>
    auto NO_INLINE lazy_emplace(State& state, size_t i, F&& creator, FF&& creator_for_null_key) {
        return this->lazy_emplace_impl(state, i, std::forward<F>(creator),
                                       std::forward<FF>(creator_for_null_key));
    }
};

template <typename Method>
class SetHashTableAlwaysInlineMethod : public SetHashTableMethodAdapterBase<Method> {
public:
    using SetHashTableMethodAdapterBase<Method>::SetHashTableMethodAdapterBase;

    template <typename State>
    auto ALWAYS_INLINE find(State& state, size_t i) {
        return this->find_impl(state, i);
    }

    template <typename State, typename F, typename FF>
    auto ALWAYS_INLINE lazy_emplace(State& state, size_t i, F&& creator,
                                    FF&& creator_for_null_key) {
        return this->lazy_emplace_impl(state, i, std::forward<F>(creator),
                                       std::forward<FF>(creator_for_null_key));
    }
};

template <typename Key>
struct SetHashTableInlineKeyStorage {
    std::vector<Key> keys;

    SetHashTableInlineKeyStorage(size_t rows, uint64_t seed) {
        keys.reserve(rows);
        for (size_t i = 0; i < rows; ++i) {
            const uint64_t value = seed + i;
            if constexpr (std::is_same_v<Key, UInt64>) {
                keys.emplace_back(value);
            } else if constexpr (std::is_same_v<Key, UInt104>) {
                keys.emplace_back(UInt104 {.a = static_cast<UInt8>(value),
                                           .b = static_cast<UInt32>(value >> 8),
                                           .c = value * 0x9e3779b97f4a7c15ULL});
            } else {
                static_assert(std::is_same_v<Key, UInt64> || std::is_same_v<Key, UInt104>);
            }
        }
    }
};

template <>
struct SetHashTableInlineKeyStorage<StringRef> {
    static constexpr size_t STRING_SIZE = 16;

    std::vector<std::array<char, STRING_SIZE>> bytes;
    std::vector<StringRef> keys;

    SetHashTableInlineKeyStorage(size_t rows, uint64_t seed) : bytes(rows) {
        keys.reserve(rows);
        for (size_t i = 0; i < rows; ++i) {
            const uint64_t value = seed + i;
            const uint64_t mixed = value * 0x9e3779b97f4a7c15ULL;
            std::memcpy(bytes[i].data(), &value, sizeof(value));
            std::memcpy(bytes[i].data() + sizeof(value), &mixed, sizeof(mixed));
            keys.emplace_back(bytes[i].data(), STRING_SIZE);
        }
    }
};

template <typename Method, bool nullable>
class SetHashTableInlineFixture {
public:
    using Key = typename Method::Key;
    using State = typename Method::State;

    explicit SetHashTableInlineFixture(size_t rows)
            : _rows(rows),
              _hit_storage(rows, 1),
              _miss_storage(rows, rows * 4 + 1),
              _method(create_method()) {
        _mixed_keys.reserve(rows);
        for (size_t i = 0; i < rows; ++i) {
            _mixed_keys.emplace_back(i % 2 == 0 ? _hit_storage.keys[i] : _miss_storage.keys[i]);
        }

        if constexpr (nullable) {
            auto nested_column = ColumnInt64::create();
            auto null_map_column = ColumnUInt8::create();
            auto& nested_data = nested_column->get_data();
            auto& null_map = null_map_column->get_data();
            nested_data.reserve(rows);
            null_map.reserve(rows);
            for (size_t i = 0; i < rows; ++i) {
                nested_data.push_back(static_cast<UInt64>(i + 1));
                null_map.push_back(i % 100 == 0);
            }
            _nullable_column =
                    ColumnNullable::create(std::move(nested_column), std::move(null_map_column));
            _state_columns.push_back(_nullable_column.get());
        }
        _state.emplace(_state_columns);
        select_keys(_hit_storage.keys);
    }

    void prepare_empty_table() {
        _method->hash_table->clear_and_shrink();
        _arena.clear();
        _method->hash_table->reserve(_rows);
        select_keys(_hit_storage.keys);
    }

    void prepare_filled_table(const std::vector<Key>& probe_keys) {
        prepare_empty_table();
        fill_table();
        select_keys(probe_keys);
    }

    const std::vector<Key>& hit_keys() const { return _hit_storage.keys; }
    const std::vector<Key>& miss_keys() const { return _miss_storage.keys; }
    const std::vector<Key>& mixed_keys() const { return _mixed_keys; }

    void prepare_batch_groups() {
        if constexpr (Method::is_string_hash_map()) {
            _method->_sub_table_groups.build(_method->keys, static_cast<uint32_t>(_rows));
        }
    }

    void build_rows() {
        auto creator = [&](const auto& ctor, auto& key, auto& origin) {
            Method::try_presis_key(key, origin, _arena);
            ctor(key, RowRefWithFlag {_current_row});
        };
        auto null_creator = [&](auto& mapped) { mapped = RowRefWithFlag {_current_row}; };
        for (size_t i = 0; i < _rows; ++i) {
            _current_row = i;
            auto* mapped = _method->lazy_emplace(*_state, i, creator, null_creator);
            benchmark::DoNotOptimize(mapped);
        }
    }

    void probe_rows() {
        for (size_t i = 0; i < _rows; ++i) {
            auto result = _method->find(*_state, i);
            bool is_found = result.is_found();
            benchmark::DoNotOptimize(is_found);
            if (is_found) {
                benchmark::DoNotOptimize(result.get_mapped().row_num);
            }
        }
    }

    void build_rows_batch() {
        auto creator = [&](const auto& ctor, auto& key, auto& origin) {
            Method::try_presis_key(key, origin, _arena);
            ctor(key, RowRefWithFlag {_current_row});
        };
        auto null_creator = [&](auto& mapped) { mapped = RowRefWithFlag {_current_row}; };
        lazy_emplace_batch(
                *_method, *_state, static_cast<uint32_t>(_rows), creator, null_creator,
                [&](uint32_t row) { _current_row = row; },
                [](uint32_t, auto& mapped) {
                    auto* mapped_ptr = &mapped;
                    benchmark::DoNotOptimize(mapped_ptr);
                });
    }

    void probe_rows_batch() {
        find_batch(*_method, *_state, static_cast<uint32_t>(_rows), [](uint32_t, auto& result) {
            bool is_found = result.is_found();
            benchmark::DoNotOptimize(is_found);
            if (is_found) {
                benchmark::DoNotOptimize(result.get_mapped().row_num);
            }
        });
    }

private:
    static std::unique_ptr<Method> create_method() {
        if constexpr (std::is_constructible_v<Method, Sizes>) {
            return std::make_unique<Method>(Sizes {sizeof(Key)});
        } else {
            return std::make_unique<Method>();
        }
    }

    void select_keys(const std::vector<Key>& keys) {
        _active_keys = keys;
        _method->keys = _active_keys.data();
        _method->hash_values.resize(_rows);
        for (size_t i = 0; i < _rows; ++i) {
            _method->hash_values[i] = _method->hash_table->hash(_active_keys[i]);
        }
    }

    void fill_table() {
        auto creator = [&](const auto& ctor, auto& key, auto& origin) {
            Method::try_presis_key(key, origin, _arena);
            ctor(key, RowRefWithFlag {_current_row});
        };
        auto null_creator = [&](auto& mapped) { mapped = RowRefWithFlag {_current_row}; };
        for (size_t i = 0; i < _rows; ++i) {
            _current_row = i;
            auto* mapped = _method->lazy_emplace(*_state, i, creator, null_creator);
            benchmark::DoNotOptimize(mapped);
        }
    }

    size_t _rows;
    SetHashTableInlineKeyStorage<Key> _hit_storage;
    SetHashTableInlineKeyStorage<Key> _miss_storage;
    std::vector<Key> _mixed_keys;
    std::vector<Key> _active_keys;
    Arena _arena;
    std::unique_ptr<Method> _method;
    ColumnPtr _nullable_column;
    ColumnRawPtrs _state_columns;
    std::optional<State> _state;
    size_t _current_row = 0;
};

template <typename Method, bool nullable, SetHashTableInlineOperation operation,
          SetHashTableExecutionMode execution_mode = SetHashTableExecutionMode::ROW>
void run_set_hash_table_inline(benchmark::State& state) {
    const auto rows = static_cast<size_t>(state.range(0));
    SetHashTableInlineFixture<Method, nullable> fixture(rows);

    if constexpr (operation == SetHashTableInlineOperation::BUILD_ALL_HIT ||
                  operation == SetHashTableInlineOperation::PROBE_ALL_HIT) {
        fixture.prepare_filled_table(fixture.hit_keys());
    } else if constexpr (operation == SetHashTableInlineOperation::PROBE_MISS_KEYS) {
        fixture.prepare_filled_table(fixture.miss_keys());
    } else if constexpr (operation == SetHashTableInlineOperation::PROBE_MIXED_KEYS_50_PERCENT) {
        fixture.prepare_filled_table(fixture.mixed_keys());
    }

    if constexpr (execution_mode == SetHashTableExecutionMode::BATCH_CORE) {
        fixture.prepare_batch_groups();
    }

    for (auto _ : state) {
        if constexpr (operation == SetHashTableInlineOperation::BUILD_COLD) {
            state.PauseTiming();
            fixture.prepare_empty_table();
            state.ResumeTiming();
        }

        if constexpr (execution_mode == SetHashTableExecutionMode::BATCH_WITH_GROUPING) {
            fixture.prepare_batch_groups();
        }

        if constexpr (operation == SetHashTableInlineOperation::BUILD_COLD ||
                      operation == SetHashTableInlineOperation::BUILD_ALL_HIT) {
            if constexpr (execution_mode == SetHashTableExecutionMode::ROW) {
                fixture.build_rows();
            } else {
                fixture.build_rows_batch();
            }
        } else {
            if constexpr (execution_mode == SetHashTableExecutionMode::ROW) {
                fixture.probe_rows();
            } else {
                fixture.probe_rows_batch();
            }
        }
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows));
}

using SetHashTableInlineUInt64 = SetPrimaryTypeHashTableContext<UInt64>;
using SetHashTableInlineUInt104 = SetFixedKeyHashTableContext<UInt104>;
using SetHashTableInlineNullableUInt64 = SetPrimaryTypeHashTableContextNullable<UInt64>;
using SetHashTableInlineString = SetMethodOneString;

#define REGISTER_SET_HASH_TABLE_INLINE_BENCHMARK(MODE, ADAPTER, TYPE_NAME, METHOD, NULLABLE, \
                                                 OPERATION)                                  \
    static void BM_SetHashTable##MODE##_##TYPE_NAME##_##OPERATION(benchmark::State& state) { \
        run_set_hash_table_inline<ADAPTER<METHOD>, NULLABLE,                                 \
                                  SetHashTableInlineOperation::OPERATION>(state);            \
    }                                                                                        \
    BENCHMARK(BM_SetHashTable##MODE##_##TYPE_NAME##_##OPERATION)->Arg(4096)->Arg(65536)

#define REGISTER_SET_HASH_TABLE_INLINE_MODE(MODE, ADAPTER, TYPE_NAME, METHOD, NULLABLE)  \
    REGISTER_SET_HASH_TABLE_INLINE_BENCHMARK(MODE, ADAPTER, TYPE_NAME, METHOD, NULLABLE, \
                                             BUILD_COLD);                                \
    REGISTER_SET_HASH_TABLE_INLINE_BENCHMARK(MODE, ADAPTER, TYPE_NAME, METHOD, NULLABLE, \
                                             BUILD_ALL_HIT);                             \
    REGISTER_SET_HASH_TABLE_INLINE_BENCHMARK(MODE, ADAPTER, TYPE_NAME, METHOD, NULLABLE, \
                                             PROBE_ALL_HIT);                             \
    REGISTER_SET_HASH_TABLE_INLINE_BENCHMARK(MODE, ADAPTER, TYPE_NAME, METHOD, NULLABLE, \
                                             PROBE_MISS_KEYS);                           \
    REGISTER_SET_HASH_TABLE_INLINE_BENCHMARK(MODE, ADAPTER, TYPE_NAME, METHOD, NULLABLE, \
                                             PROBE_MIXED_KEYS_50_PERCENT)

#define REGISTER_SET_HASH_TABLE_BATCH_BENCHMARK(MODE, EXECUTION_MODE, TYPE_NAME, METHOD, NULLABLE, \
                                                OPERATION)                                         \
    static void BM_SetHashTable##MODE##_##TYPE_NAME##_##OPERATION(benchmark::State& state) {       \
        run_set_hash_table_inline<METHOD, NULLABLE, SetHashTableInlineOperation::OPERATION,        \
                                  SetHashTableExecutionMode::EXECUTION_MODE>(state);               \
    }                                                                                              \
    BENCHMARK(BM_SetHashTable##MODE##_##TYPE_NAME##_##OPERATION)->Arg(4096)->Arg(65536)

#define REGISTER_SET_HASH_TABLE_BATCH_MODE(MODE, EXECUTION_MODE, TYPE_NAME, METHOD, NULLABLE)  \
    REGISTER_SET_HASH_TABLE_BATCH_BENCHMARK(MODE, EXECUTION_MODE, TYPE_NAME, METHOD, NULLABLE, \
                                            BUILD_COLD);                                       \
    REGISTER_SET_HASH_TABLE_BATCH_BENCHMARK(MODE, EXECUTION_MODE, TYPE_NAME, METHOD, NULLABLE, \
                                            BUILD_ALL_HIT);                                    \
    REGISTER_SET_HASH_TABLE_BATCH_BENCHMARK(MODE, EXECUTION_MODE, TYPE_NAME, METHOD, NULLABLE, \
                                            PROBE_ALL_HIT);                                    \
    REGISTER_SET_HASH_TABLE_BATCH_BENCHMARK(MODE, EXECUTION_MODE, TYPE_NAME, METHOD, NULLABLE, \
                                            PROBE_MISS_KEYS);                                  \
    REGISTER_SET_HASH_TABLE_BATCH_BENCHMARK(MODE, EXECUTION_MODE, TYPE_NAME, METHOD, NULLABLE, \
                                            PROBE_MIXED_KEYS_50_PERCENT)

#define REGISTER_SET_HASH_TABLE_INLINE_TYPE(TYPE_NAME, METHOD, NULLABLE)                         \
    REGISTER_SET_HASH_TABLE_INLINE_MODE(NoInline, SetHashTableNoInlineMethod, TYPE_NAME, METHOD, \
                                        NULLABLE);                                               \
    REGISTER_SET_HASH_TABLE_INLINE_MODE(AlwaysInline, SetHashTableAlwaysInlineMethod, TYPE_NAME, \
                                        METHOD, NULLABLE);                                       \
    REGISTER_SET_HASH_TABLE_BATCH_MODE(BatchCore, BATCH_CORE, TYPE_NAME, METHOD, NULLABLE)

REGISTER_SET_HASH_TABLE_INLINE_TYPE(UInt64, SetHashTableInlineUInt64, false);
REGISTER_SET_HASH_TABLE_INLINE_TYPE(UInt104, SetHashTableInlineUInt104, false);
REGISTER_SET_HASH_TABLE_INLINE_TYPE(NullableUInt64_1PctNull, SetHashTableInlineNullableUInt64,
                                    true);
REGISTER_SET_HASH_TABLE_INLINE_TYPE(StringRef, SetHashTableInlineString, false);
REGISTER_SET_HASH_TABLE_BATCH_MODE(BatchWithGrouping, BATCH_WITH_GROUPING, StringRef,
                                   SetHashTableInlineString, false);

#undef REGISTER_SET_HASH_TABLE_INLINE_TYPE
#undef REGISTER_SET_HASH_TABLE_BATCH_MODE
#undef REGISTER_SET_HASH_TABLE_BATCH_BENCHMARK
#undef REGISTER_SET_HASH_TABLE_INLINE_MODE
#undef REGISTER_SET_HASH_TABLE_INLINE_BENCHMARK

} // namespace

} // namespace doris
