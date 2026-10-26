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

// Memtable benchmarks.
//
// Running them:
//
//   BUILD_TYPE=Release ./build.sh --benchmark -j 80
//   cd <doris home>            # the binary needs ./conf/be.conf relative to the cwd
//   export LD_LIBRARY_PATH="$JAVA_HOME/lib/server:$DORIS_HOME/be/output/lib:$LD_LIBRARY_PATH"
//   ./be/output/lib/benchmark_test --benchmark_filter=BM_MemTable
//
// Comparing two implementations (for example this branch against the commit it forked from):
// build each tree with the same command and run the binaries interleaved, so machine drift hits
// both sides equally. `BM_MemTableScenario` prints the memtable's own phase timers as counters
// (sort_ms / agg_ms / put_ms), so a difference can be attributed to a phase without
// re-instrumenting the benchmark.
//
// A tree that predates MemTable::create() (the by-table-model refactor) can still run the exact
// same workload: build it with
//
//   EXTRA_CXX_FLAGS=-DMEMTABLE_BENCH_BASELINE_CTOR BUILD_TYPE=Release ./build.sh --benchmark
//
// which swaps the construction seam in create_memtable() for the single-class constructor. A tree
// that predates this file entirely also needs `#include "benchmark_memtable.hpp"` added to
// benchmark_main.cpp.

#pragma once

#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_number.h"
#include "load/delta_writer/delta_writer_context.h"
#include "load/memtable/memtable.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

// ---------------------------------------------------------------------------------------------
// Row-index representation microbenchmarks: the cost of tracking one row as a compact position
// versus the RowInBlock reference array the memtable used before. Both sides allocate the row
// storage once per batch, which is what the pre-change memtable already did through
// make_row_batch(); allocating each row separately would compare the compact permutation against
// a strawman rather than against the implementation it replaces.
// ---------------------------------------------------------------------------------------------

static void BM_DuplicateKeyRowIndexSharedPtr(benchmark::State& state) {
    const auto rows = static_cast<size_t>(state.range(0));
    for (auto _ : state) {
        auto batch = std::allocate_shared<RowInBlock[]>(CustomStdAllocator<RowInBlock>(), rows);
        for (size_t i = 0; i < rows; ++i) {
            batch[i] = RowInBlock(i);
        }
        DorisVector<std::shared_ptr<RowInBlock>> row_index;
        row_index.reserve(rows);
        for (size_t i = 0; i < rows; ++i) {
            row_index.emplace_back(batch, &batch[i]);
        }
        benchmark::DoNotOptimize(row_index);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows));
    state.counters["estimated_bytes_per_row"] =
            benchmark::Counter(sizeof(std::shared_ptr<RowInBlock>) + sizeof(RowInBlock));
}

static void BM_DuplicateKeyRowIndexCompact(benchmark::State& state) {
    const auto rows = static_cast<size_t>(state.range(0));
    for (auto _ : state) {
        DorisVector<uint32_t> row_index;
        row_index.resize(rows);
        std::iota(row_index.begin(), row_index.end(), uint32_t {0});
        benchmark::DoNotOptimize(row_index);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows));
    state.counters["estimated_bytes_per_row"] = benchmark::Counter(sizeof(uint32_t));
}

BENCHMARK(BM_DuplicateKeyRowIndexSharedPtr)
        ->Arg(4096)
        ->Arg(65536)
        ->Arg(1048576)
        ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_DuplicateKeyRowIndexCompact)
        ->Arg(4096)
        ->Arg(65536)
        ->Arg(1048576)
        ->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------------------------
// End-to-end memtable benchmarks: insert batches, then sort and (for aggregating models) merge
// equal keys in to_block(). This is the path a load actually pays for, so a structural change to
// the memtable is judged here rather than on the row-index microbenchmark above.
// ---------------------------------------------------------------------------------------------

namespace memtable_bench {

constexpr int64_t kBenchmarkTabletId = 1;

// One workload shape plus the memtable operations the benchmark drives. Every field maps to a
// code path that a memtable change can plausibly move, so a scenario is added whenever a change
// touches a path that the existing scenarios do not reach.
struct Scenario {
    const char* name;
    KeysType keys_type;
    // Rows handed to a single insert() call.
    size_t batch_rows;
    // insert() calls per to_block(). More than one drives the incremental sort over
    // [_last_sorted_pos, size) and the amortized growth of the row-tracking vectors.
    size_t num_batches;
    // Number of distinct key tuples in the whole workload. Equal to the row count means no
    // merging at all (the to_block() fast path); 1 means every row merges into one group.
    int32_t key_cardinality;
    int num_key_columns;
    int num_value_columns;
    // Drive the row-binlog LSN sidecar, which DUP_KEYS keeps as a separate permutation-indexed
    // vector and the aggregating models merge through RowInBlock.
    bool need_lsn;
    // Call shrink_memtable_by_agg() between batches: the is_final=false aggregation path, which
    // repacks the surviving rows into a fresh batch.
    bool shrink_between_batches;
    // UNIQUE_KEYS merge-on-write with cluster keys: an extra re-sort round in _after_aggregate().
    bool unique_cluster_keys;
};

// One prepared workload: schema, descriptor table and the input block that every iteration
// inserts. Building it is excluded from the measured region because a load builds it once.
struct Workload {
    Scenario scenario;
    std::shared_ptr<TabletSchema> schema;
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    TupleDescriptor* tuple_desc = nullptr;
    std::shared_ptr<ResourceContext> resource_ctx;
    // batch_rows * num_batches rows; run_batches() inserts it in slices.
    Block input;
};

inline std::string column_name(bool is_key, int index, int count) {
    // Keep the historical single-column names so numbers stay comparable with earlier runs.
    if (count == 1) {
        return is_key ? "k" : "v";
    }
    return (is_key ? "k" : "v") + std::to_string(index);
}

inline TTypeDesc int_type_desc() {
    TTypeNode node;
    node.type = TTypeNodeType::SCALAR;
    node.__isset.scalar_type = true;
    node.scalar_type.type = TPrimitiveType::INT;
    TTypeDesc desc;
    desc.types.push_back(node);
    return desc;
}

// Descriptor table of the benchmark schema. It is built directly rather than through the test
// descriptor helpers so the benchmark target stays independent of test-only utilities.
inline TDescriptorTable make_descriptor_table(const Scenario& scenario) {
    const int num_columns = scenario.num_key_columns + scenario.num_value_columns;
    TDescriptorTable desc_tbl;
    TTupleDescriptor tuple_desc;
    tuple_desc.id = 0;
    tuple_desc.byteSize = 0;
    tuple_desc.numNullBytes = 0;
    tuple_desc.numNullSlots = num_columns;
    desc_tbl.tupleDescriptors.push_back(tuple_desc);

    for (int32_t i = 0; i < num_columns; ++i) {
        const bool is_key = i < scenario.num_key_columns;
        const auto name = column_name(is_key, is_key ? i : i - scenario.num_key_columns,
                                      is_key ? scenario.num_key_columns
                                             : scenario.num_value_columns);
        TSlotDescriptor slot;
        slot.id = i;
        slot.parent = 0;
        slot.slotType = int_type_desc();
        slot.columnPos = i;
        slot.colName = name;
        slot.slotIdx = i;
        slot.isMaterialized = true;
        slot.byteOffset = i * 4;
        slot.nullIndicatorByte = 0;
        slot.nullIndicatorBit = -1;
        desc_tbl.slotDescriptors.push_back(slot);
    }
    desc_tbl.__isset.slotDescriptors = true;
    return desc_tbl;
}

inline std::shared_ptr<TabletSchema> make_schema(const Scenario& scenario) {
    auto schema = std::make_shared<TabletSchema>();
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(scenario.keys_type);
    schema_pb.set_num_short_key_columns(scenario.num_key_columns);
    schema_pb.set_num_rows_per_row_block(1024);
    schema_pb.set_next_column_unique_id(scenario.num_key_columns + scenario.num_value_columns);
    if (scenario.unique_cluster_keys) {
        // Column unique ids are assigned in column order below, so the value columns are exactly
        // [num_key_columns, num_columns). Set through the PB because init_from_pb() is the only
        // place that reads cluster keys, and it must run before the columns are appended.
        for (int c = scenario.num_key_columns;
             c < scenario.num_key_columns + scenario.num_value_columns; ++c) {
            schema_pb.add_cluster_key_uids(c);
        }
    }
    schema->init_from_pb(schema_pb);

    uint32_t uid = 0;
    for (int c = 0; c < scenario.num_key_columns; ++c, ++uid) {
        TabletColumn key;
        key.set_unique_id(uid);
        key.set_name(column_name(true, c, scenario.num_key_columns));
        key.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        key.set_is_key(true);
        key.set_is_nullable(false);
        key.set_length(4);
        key.set_index_length(4);
        schema->append_column(key);
    }
    for (int c = 0; c < scenario.num_value_columns; ++c, ++uid) {
        TabletColumn value;
        value.set_unique_id(uid);
        value.set_name(column_name(false, c, scenario.num_value_columns));
        value.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        value.set_is_key(false);
        value.set_is_nullable(false);
        value.set_length(4);
        value.set_index_length(4);
        if (scenario.keys_type == KeysType::AGG_KEYS) {
            value.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM);
        }
        schema->append_column(value);
    }
    return schema;
}

inline std::unique_ptr<Workload> make_workload(const Scenario& scenario) {
    auto workload = std::make_unique<Workload>();
    workload->scenario = scenario;
    workload->schema = make_schema(scenario);
    workload->resource_ctx = ResourceContext::create_shared();
    workload->resource_ctx->memory_context()->set_mem_tracker(
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::LOAD, "MemTableBenchmark"));

    DescriptorTbl* desc_tbl = nullptr;
    auto status = DescriptorTbl::create(&workload->pool, make_descriptor_table(scenario), &desc_tbl);
    DORIS_CHECK(status.ok());
    workload->desc_tbl = desc_tbl;
    workload->tuple_desc = desc_tbl->get_tuple_descriptor(0);

    for (const auto* slot : workload->tuple_desc->slots()) {
        workload->input.insert(ColumnWithTypeAndName(slot->get_empty_mutable_column(), slot->type(),
                                                     slot->col_name()));
    }
    const auto total_rows = scenario.batch_rows * scenario.num_batches;
    auto columns_guard = workload->input.mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (size_t i = 0; i < total_rows; ++i) {
        // A bounded key domain makes the aggregating models merge rows, which is the interesting
        // part of their to_block(); DUP_KEYS keeps every row regardless of the key. Every key
        // column derives from the same residue, so the number of distinct key tuples stays
        // key_cardinality no matter how many key columns the scenario has.
        const auto key = static_cast<int32_t>(i % static_cast<size_t>(scenario.key_cardinality));
        for (int c = 0; c < scenario.num_key_columns; ++c) {
            int32_t value = key * (c + 1);
            columns[c]->insert_data(reinterpret_cast<const char*>(&value), 0);
        }
        for (int c = 0; c < scenario.num_value_columns; ++c) {
            int32_t value = static_cast<int32_t>(i) + c;
            columns[scenario.num_key_columns + c]->insert_data(
                    reinterpret_cast<const char*>(&value), 0);
        }
    }
    return workload;
}

// The single construction seam of the comparison. Two trees can only be compared if they build
// the memtable through this function, so a tree that predates MemTable::create() selects the
// single-class constructor at compile time instead of patching the source.
inline std::shared_ptr<MemTable> create_memtable(const Workload& workload) {
    const auto& scenario = workload.scenario;
#ifdef MEMTABLE_BENCH_BASELINE_CTOR
    return std::make_shared<MemTable>(kBenchmarkTabletId, workload.schema,
                                      &workload.tuple_desc->slots(), workload.tuple_desc,
                                      scenario.keys_type == KeysType::UNIQUE_KEYS, nullptr,
                                      workload.resource_ctx, scenario.need_lsn);
#else
    return MemTable::create(kBenchmarkTabletId, workload.schema, &workload.tuple_desc->slots(),
                            workload.tuple_desc, scenario.keys_type == KeysType::UNIQUE_KEYS,
                            nullptr, workload.resource_ctx, scenario.need_lsn);
#endif
}

// Drives the scenario's insert() calls, including the optional inter-batch shrink.
inline void run_batches(MemTable& memtable, const Workload& workload) {
    const auto& scenario = workload.scenario;
    TabletAddRowsPayload rows;
    rows.row_idxs.reserve(scenario.batch_rows);
    if (scenario.need_lsn) {
        rows.allocated_lsns.reserve(scenario.batch_rows);
    }
    for (size_t batch = 0; batch < scenario.num_batches; ++batch) {
        rows.row_idxs.clear();
        rows.allocated_lsns.clear();
        const auto begin = static_cast<uint32_t>(batch * scenario.batch_rows);
        for (uint32_t i = 0; i < scenario.batch_rows; ++i) {
            rows.row_idxs.push_back(begin + i);
            if (scenario.need_lsn) {
                // Monotonic LSNs, as a row binlog would assign them.
                rows.allocated_lsns.push_back(static_cast<int64_t>(begin + i) + 1);
            }
        }
        auto status = memtable.insert(&workload.input, rows);
        DORIS_CHECK(status.ok());
        if (scenario.shrink_between_batches && batch + 1 < scenario.num_batches) {
            memtable.shrink_memtable_by_agg();
        }
    }
}

inline void run_scenario(benchmark::State& state, const Scenario& scenario) {
    auto workload = make_workload(scenario);
    double sort_ns = 0;
    double agg_ns = 0;
    double put_ns = 0;
    for (auto _ : state) {
        auto memtable = create_memtable(*workload);
        run_batches(*memtable, *workload);
        std::unique_ptr<Block> output;
        auto status = memtable->to_block(&output);
        DORIS_CHECK(status.ok());
        // The memtable already times its own phases; surface them so a difference can be
        // attributed without re-instrumenting the benchmark.
        const auto& stat = memtable->stat();
        sort_ns += static_cast<double>(stat.sort_ns);
        agg_ns += static_cast<double>(stat.agg_ns);
        put_ns += static_cast<double>(stat.put_into_output_ns);
        benchmark::DoNotOptimize(output);
        benchmark::ClobberMemory();
    }
    const auto iterations = static_cast<double>(state.iterations());
    const auto total_rows = static_cast<int64_t>(scenario.batch_rows * scenario.num_batches);
    state.SetItemsProcessed(state.iterations() * total_rows);
    state.counters["rows"] = benchmark::Counter(static_cast<double>(total_rows));
    state.counters["sort_ms"] = benchmark::Counter(sort_ns / 1e6 / iterations);
    state.counters["agg_ms"] = benchmark::Counter(agg_ns / 1e6 / iterations);
    state.counters["put_ms"] = benchmark::Counter(put_ns / 1e6 / iterations);
}

// The scenario table. Each entry exists because a memtable change can move that path:
//   *_single_batch      headline number, one 65536-row batch
//   *_multi_batch_1k    incremental sort over [_last_sorted_pos, size) and amortized growth of the
//                       row-tracking vectors (the real load shape: many small batches)
//   *_multi_batch_lsn   row-binlog LSN sidecar: DUP_KEYS keeps a separate vector and applies the
//                       sorted permutation to it, the aggregating models merge it through
//                       RowInBlock
//   *_multi_batch_shrink  shrink_memtable_by_agg(): the is_final=false path, which repacks the
//                       surviving rows into a fresh batch
//   *_no_dup_keys       same_keys_num == 0, so to_block() skips aggregation entirely
//   *_all_dup_keys      one group, maximum merging
//   *_three_key_columns multiple pdqsort passes, one per key column, plus the Tie bookkeeping
//   agg_four_value_columns  four aggregate functions per row
//   dup_zero_key_columns    the no-sort fast path in DupKeysMemTable::_to_block()
//   unique_cluster_keys     the extra cluster-key re-sort round in _after_aggregate()
inline const std::vector<Scenario>& scenarios() {
    static const std::vector<Scenario> kScenarios = {
            // name                          keys_type                batch  batches card   nk nv lsn   shrink cluster
            {"dup_single_batch",             KeysType::DUP_KEYS,      65536, 1,      8192,  1, 1, false, false, false},
            {"unique_single_batch",          KeysType::UNIQUE_KEYS,   65536, 1,      8192,  1, 1, false, false, false},
            {"agg_single_batch",             KeysType::AGG_KEYS,      65536, 1,      8192,  1, 1, false, false, false},
            {"dup_multi_batch_1k",           KeysType::DUP_KEYS,      1024,  64,     8192,  1, 1, false, false, false},
            {"unique_multi_batch_1k",        KeysType::UNIQUE_KEYS,   1024,  64,     8192,  1, 1, false, false, false},
            {"agg_multi_batch_1k",           KeysType::AGG_KEYS,      1024,  64,     8192,  1, 1, false, false, false},
            {"dup_multi_batch_lsn",          KeysType::DUP_KEYS,      1024,  64,     8192,  1, 1, true,  false, false},
            {"unique_multi_batch_lsn",       KeysType::UNIQUE_KEYS,   1024,  64,     8192,  1, 1, true,  false, false},
            {"agg_multi_batch_shrink",       KeysType::AGG_KEYS,      4096,  16,     8192,  1, 1, false, true,  false},
            {"unique_multi_batch_shrink",    KeysType::UNIQUE_KEYS,   4096,  16,     8192,  1, 1, false, true,  false},
            {"dup_no_dup_keys",              KeysType::DUP_KEYS,      65536, 1,      65536, 1, 1, false, false, false},
            {"agg_no_dup_keys",              KeysType::AGG_KEYS,      65536, 1,      65536, 1, 1, false, false, false},
            {"agg_all_dup_keys",             KeysType::AGG_KEYS,      65536, 1,      1,     1, 1, false, false, false},
            {"unique_all_dup_keys",          KeysType::UNIQUE_KEYS,   65536, 1,      1,     1, 1, false, false, false},
            {"dup_three_key_columns",        KeysType::DUP_KEYS,      65536, 1,      8192,  3, 1, false, false, false},
            {"agg_three_key_columns",        KeysType::AGG_KEYS,      65536, 1,      8192,  3, 1, false, false, false},
            {"agg_four_value_columns",       KeysType::AGG_KEYS,      65536, 1,      8192,  1, 4, false, false, false},
            {"dup_zero_key_columns",         KeysType::DUP_KEYS,      65536, 1,      8192,  0, 2, false, false, false},
            {"unique_cluster_keys",          KeysType::UNIQUE_KEYS,   65536, 1,      8192,  1, 2, false, false, true},
    };
    return kScenarios;
}

} // namespace memtable_bench

// Headline benchmark, kept at the historical shape so numbers stay comparable across runs.
static void BM_MemTableInsertToBlock(benchmark::State& state) {
    memtable_bench::Scenario scenario {};
    scenario.name = "insert_to_block";
    scenario.keys_type = static_cast<KeysType>(state.range(0));
    scenario.batch_rows = static_cast<size_t>(state.range(1));
    scenario.num_batches = 1;
    scenario.key_cardinality = static_cast<int32_t>(state.range(2));
    scenario.num_key_columns = 1;
    scenario.num_value_columns = 1;
    memtable_bench::run_scenario(state, scenario);
}

BENCHMARK(BM_MemTableInsertToBlock)
        ->Args({KeysType::DUP_KEYS, 65536, 8192})
        ->Args({KeysType::UNIQUE_KEYS, 65536, 8192})
        ->Args({KeysType::AGG_KEYS, 65536, 8192})
        ->Unit(benchmark::kMillisecond);

// Every scenario in the table above, selected by index.
static void BM_MemTableScenario(benchmark::State& state) {
    const auto& scenario = memtable_bench::scenarios()[static_cast<size_t>(state.range(0))];
    state.SetLabel(scenario.name);
    memtable_bench::run_scenario(state, scenario);
}

BENCHMARK(BM_MemTableScenario)
        ->DenseRange(0, static_cast<int64_t>(memtable_bench::scenarios().size()) - 1)
        ->Unit(benchmark::kMillisecond);

} // namespace doris
