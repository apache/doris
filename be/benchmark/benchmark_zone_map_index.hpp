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
// Benchmark: TypedZoneMapIndexWriter::add_values
//
// Measures CPU cost of feeding values into the per-page zone-map
// builder for a few representative primitive types and call sizes.
// ============================================================

#pragma once

#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "core/data_type/data_type_factory.hpp"
#include "core/string_ref.h"
#include "storage/field.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2 {

namespace bench_zone_map {

constexpr size_t kTotalRows = 1 << 16;     // 65536 rows fed per iteration
constexpr size_t kStoragePageSize = 65536; // STORAGE_PAGE_SIZE_DEFAULT_VALUE

inline std::vector<int32_t> gen_int32(size_t n) {
    std::mt19937 rng(0xC0FFEE);
    std::uniform_int_distribution<int32_t> d(-1'000'000, 1'000'000);
    std::vector<int32_t> v(n);
    for (auto& x : v) x = d(rng);
    return v;
}
inline std::vector<int64_t> gen_int64(size_t n) {
    std::mt19937_64 rng(0xC0FFEE);
    std::uniform_int_distribution<int64_t> d(-1'000'000'000LL, 1'000'000'000LL);
    std::vector<int64_t> v(n);
    for (auto& x : v) x = d(rng);
    return v;
}
inline std::vector<double> gen_double(size_t n) {
    std::mt19937 rng(0xC0FFEE);
    std::uniform_real_distribution<double> d(-1e6, 1e6);
    std::vector<double> v(n);
    for (auto& x : v) x = d(rng);
    return v;
}
// Build a contiguous string buffer + Slice array (ValType for string is StringRef/Slice).
struct StringBatch {
    std::vector<std::string> data;
    std::vector<Slice> slices;
};
inline StringBatch gen_strings(size_t n, size_t avg_len = 16) {
    StringBatch b;
    b.data.reserve(n);
    b.slices.reserve(n);
    std::mt19937 rng(0xC0FFEE);
    std::uniform_int_distribution<int> ch('a', 'z');
    for (size_t i = 0; i < n; ++i) {
        std::string s(avg_len, 'a');
        for (auto& c : s) c = static_cast<char>(ch(rng));
        b.data.emplace_back(std::move(s));
    }
    for (auto& s : b.data) b.slices.emplace_back(s.data(), s.size());
    return b;
}

inline TabletColumnPtr make_column(FieldType ft, int32_t length, int32_t index_length) {
    auto c = std::make_shared<TabletColumn>();
    c->_unique_id = 0;
    c->_col_name = "c";
    c->_type = ft;
    c->_is_key = true;
    c->_is_nullable = false;
    c->_length = length;
    c->_index_length = index_length;
    return c;
}

template <PrimitiveType PType>
std::unique_ptr<ZoneMapIndexWriter> make_writer() {
    TabletColumnPtr col;
    DataTypePtr dtype;
    if constexpr (PType == TYPE_INT) {
        col = make_column(FieldType::OLAP_FIELD_TYPE_INT, 4, 4);
        dtype = DataTypeFactory::instance().create_data_type(TYPE_INT, false);
    } else if constexpr (PType == TYPE_BIGINT) {
        col = make_column(FieldType::OLAP_FIELD_TYPE_BIGINT, 8, 8);
        dtype = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, false);
    } else if constexpr (PType == TYPE_DOUBLE) {
        col = make_column(FieldType::OLAP_FIELD_TYPE_DOUBLE, 8, 8);
        dtype = DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, false);
    } else if constexpr (PType == TYPE_VARCHAR) {
        col = make_column(FieldType::OLAP_FIELD_TYPE_VARCHAR, 64, 1);
        dtype = DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, false, 0, 0, 64);
    }
    std::unique_ptr<StorageField> field(StorageFieldFactory::create(*col));
    std::unique_ptr<ZoneMapIndexWriter> w;
    (void)ZoneMapIndexWriter::create(dtype, field.get(), w);
    return w;
}

template <PrimitiveType PType, typename Vec>
void run(benchmark::State& state, const Vec& values) {
    const size_t batch = static_cast<size_t>(state.range(0));
    const size_t total = values.size();
    for (auto _ : state) {
        auto w = make_writer<PType>();
        size_t off = 0;
        while (off < total) {
            size_t n = std::min(batch, total - off);
            w->add_values(reinterpret_cast<const void*>(&values[off]), n);
            off += n;
        }
        (void)w->flush();
        benchmark::DoNotOptimize(w);
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(total));
}

// Simulates the ScalarColumnWriter call pattern in compaction:
//   - merge iterator hands `block_rows`-row blocks to ColumnWriter::append
//   - column_writer chunks each block by page remaining capacity and calls
//     add_values() per chunk
//   - when a page is full, finish_current_page() calls flush() on the zone
//     map builder, then a new page begins
//
// Compaction batch_size is computed dynamically as
// `block_mem_limit / group_data_size` clamped to [32, 4064]
// (be/src/storage/merger.cpp:458). For wide rows / variant data it routinely
// drops to the low end (32 - 256), which is the case the flame graph exposes.
template <PrimitiveType PType, typename Vec>
void run_column_writer_like(benchmark::State& state, const Vec& values, size_t elem_size) {
    const size_t block_rows = static_cast<size_t>(state.range(0));
    const size_t page_capacity = kStoragePageSize / elem_size; // e.g. 16384 for int32
    const size_t total = values.size();
    for (auto _ : state) {
        auto w = make_writer<PType>();
        size_t off = 0;
        size_t page_used = 0;
        while (off < total) {
            size_t block_left = std::min(block_rows, total - off);
            while (block_left > 0) {
                size_t n = std::min(block_left, page_capacity - page_used);
                w->add_values(reinterpret_cast<const void*>(&values[off]), n);
                off += n;
                block_left -= n;
                page_used += n;
                if (page_used == page_capacity) {
                    (void)w->flush();
                    page_used = 0;
                }
            }
        }
        if (page_used) (void)w->flush();
        benchmark::DoNotOptimize(w);
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(total));
}

static void BM_ZoneMap_Int32(benchmark::State& state) {
    static auto vals = gen_int32(kTotalRows);
    run<TYPE_INT>(state, vals);
}
static void BM_ZoneMap_Int64(benchmark::State& state) {
    static auto vals = gen_int64(kTotalRows);
    run<TYPE_BIGINT>(state, vals);
}
static void BM_ZoneMap_Double(benchmark::State& state) {
    static auto vals = gen_double(kTotalRows);
    run<TYPE_DOUBLE>(state, vals);
}
static void BM_ZoneMap_String(benchmark::State& state) {
    static auto batch = gen_strings(kTotalRows, 16);
    run<TYPE_VARCHAR>(state, batch.slices);
}

BENCHMARK(BM_ZoneMap_Int32)->Arg(1)->Arg(64)->Arg(1024);
BENCHMARK(BM_ZoneMap_Int64)->Arg(1)->Arg(64)->Arg(1024);
BENCHMARK(BM_ZoneMap_Double)->Arg(1)->Arg(64)->Arg(1024);
BENCHMARK(BM_ZoneMap_String)->Arg(1)->Arg(64)->Arg(1024);

// Realistic compaction-shaped: 1024-row blocks + page-driven flush().
static void BM_ZoneMap_ColWriter_Int32(benchmark::State& state) {
    static auto vals = gen_int32(kTotalRows);
    run_column_writer_like<TYPE_INT>(state, vals, sizeof(int32_t));
}
static void BM_ZoneMap_ColWriter_Int64(benchmark::State& state) {
    static auto vals = gen_int64(kTotalRows);
    run_column_writer_like<TYPE_BIGINT>(state, vals, sizeof(int64_t));
}
static void BM_ZoneMap_ColWriter_Double(benchmark::State& state) {
    static auto vals = gen_double(kTotalRows);
    run_column_writer_like<TYPE_DOUBLE>(state, vals, sizeof(double));
}
static void BM_ZoneMap_ColWriter_String(benchmark::State& state) {
    static auto batch = gen_strings(kTotalRows, 16);
    // For strings the page packs (size+payload); use ~32B avg per element.
    run_column_writer_like<TYPE_VARCHAR>(state, batch.slices, 32);
}
BENCHMARK(BM_ZoneMap_ColWriter_Int32)
        ->Arg(1)
        ->Arg(4)
        ->Arg(16)
        ->Arg(64)
        ->Arg(256)
        ->Arg(1024)
        ->Arg(4096);
BENCHMARK(BM_ZoneMap_ColWriter_Int64)
        ->Arg(1)
        ->Arg(4)
        ->Arg(16)
        ->Arg(64)
        ->Arg(256)
        ->Arg(1024)
        ->Arg(4096);
BENCHMARK(BM_ZoneMap_ColWriter_Double)
        ->Arg(1)
        ->Arg(4)
        ->Arg(16)
        ->Arg(64)
        ->Arg(256)
        ->Arg(1024)
        ->Arg(4096);
BENCHMARK(BM_ZoneMap_ColWriter_String)
        ->Arg(1)
        ->Arg(4)
        ->Arg(16)
        ->Arg(64)
        ->Arg(256)
        ->Arg(1024)
        ->Arg(4096);

} // namespace bench_zone_map
} // namespace doris::segment_v2
