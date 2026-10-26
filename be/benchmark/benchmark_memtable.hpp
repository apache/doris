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
// End-to-end memtable benchmark: insert batches, then sort and (for aggregating models) merge
// equal keys in to_block(). This is the path a load actually pays for, so a structural change to
// the memtable is judged here rather than on the row-index microbenchmark above.
// ---------------------------------------------------------------------------------------------

namespace memtable_bench {

// One prepared workload: schema, descriptor table and the input block that every iteration
// inserts. Building it is excluded from the measured region because a load builds it once.
struct Workload {
    KeysType keys_type = KeysType::DUP_KEYS;
    std::shared_ptr<TabletSchema> schema;
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    TupleDescriptor* tuple_desc = nullptr;
    std::shared_ptr<ResourceContext> resource_ctx;
    Block input;
    TabletAddRowsPayload rows;
};

inline TTypeDesc int_type_desc() {
    TTypeNode node;
    node.type = TTypeNodeType::SCALAR;
    node.__isset.scalar_type = true;
    node.scalar_type.type = TPrimitiveType::INT;
    TTypeDesc desc;
    desc.types.push_back(node);
    return desc;
}

// Descriptor table of the benchmark schema: a non-nullable INT key and INT value. It is built
// directly rather than through the test descriptor helpers so the benchmark target stays
// independent of test-only utilities.
inline TDescriptorTable make_descriptor_table() {
    TDescriptorTable desc_tbl;
    TTupleDescriptor tuple_desc;
    tuple_desc.id = 0;
    tuple_desc.byteSize = 0;
    tuple_desc.numNullBytes = 0;
    tuple_desc.numNullSlots = 2;
    desc_tbl.tupleDescriptors.push_back(tuple_desc);

    const char* names[] = {"k", "v"};
    for (int32_t i = 0; i < 2; ++i) {
        TSlotDescriptor slot;
        slot.id = i;
        slot.parent = 0;
        slot.slotType = int_type_desc();
        slot.columnPos = i;
        slot.colName = names[i];
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

inline std::shared_ptr<TabletSchema> make_schema(KeysType keys_type) {
    auto schema = std::make_shared<TabletSchema>();
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(keys_type);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(1024);
    schema_pb.set_next_column_unique_id(3);
    schema->init_from_pb(schema_pb);

    TabletColumn key;
    key.set_unique_id(0);
    key.set_name("k");
    key.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    key.set_is_key(true);
    key.set_is_nullable(false);
    key.set_length(4);
    key.set_index_length(4);
    schema->append_column(key);

    TabletColumn value;
    value.set_unique_id(1);
    value.set_name("v");
    value.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    value.set_is_key(false);
    value.set_is_nullable(false);
    value.set_length(4);
    value.set_index_length(4);
    if (keys_type == KeysType::AGG_KEYS) {
        value.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM);
    }
    schema->append_column(value);
    return schema;
}

inline std::unique_ptr<Workload> make_workload(KeysType keys_type, size_t num_rows,
                                               int32_t key_cardinality) {
    auto workload = std::make_unique<Workload>();
    workload->keys_type = keys_type;
    workload->schema = make_schema(keys_type);
    workload->resource_ctx = ResourceContext::create_shared();
    workload->resource_ctx->memory_context()->set_mem_tracker(
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::LOAD, "MemTableBenchmark"));

    DescriptorTbl* desc_tbl = nullptr;
    auto status = DescriptorTbl::create(&workload->pool, make_descriptor_table(), &desc_tbl);
    DORIS_CHECK(status.ok());
    workload->desc_tbl = desc_tbl;
    workload->tuple_desc = desc_tbl->get_tuple_descriptor(0);

    for (const auto* slot : workload->tuple_desc->slots()) {
        workload->input.insert(ColumnWithTypeAndName(slot->get_empty_mutable_column(), slot->type(),
                                                     slot->col_name()));
    }
    auto columns_guard = workload->input.mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    workload->rows.row_idxs.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        // A bounded key domain makes aggregating models merge rows, which is the interesting
        // part of their to_block(); DUP_KEYS keeps every row regardless of the key.
        int32_t key = static_cast<int32_t>(i % static_cast<size_t>(key_cardinality));
        int32_t value = static_cast<int32_t>(i);
        columns[0]->insert_data(reinterpret_cast<const char*>(&key), 0);
        columns[1]->insert_data(reinterpret_cast<const char*>(&value), 0);
        workload->rows.row_idxs.push_back(static_cast<uint32_t>(i));
    }
    return workload;
}

// Single construction seam. A baseline that predates MemTable::create only has to replace this
// line with the model's constructor to run the identical workload.
inline std::shared_ptr<MemTable> create_memtable(const Workload& workload) {
    return MemTable::create(1, workload.schema, &workload.tuple_desc->slots(), workload.tuple_desc,
                            workload.keys_type == KeysType::UNIQUE_KEYS, nullptr,
                            workload.resource_ctx, false);
}

} // namespace memtable_bench

static void BM_MemTableInsertToBlock(benchmark::State& state) {
    const auto keys_type = static_cast<KeysType>(state.range(0));
    const auto num_rows = static_cast<size_t>(state.range(1));
    const auto key_cardinality = static_cast<int32_t>(state.range(2));
    auto workload = memtable_bench::make_workload(keys_type, num_rows, key_cardinality);

    for (auto _ : state) {
        auto memtable = memtable_bench::create_memtable(*workload);
        auto status = memtable->insert(&workload->input, workload->rows);
        DORIS_CHECK(status.ok());
        std::unique_ptr<Block> output;
        status = memtable->to_block(&output);
        DORIS_CHECK(status.ok());
        benchmark::DoNotOptimize(output);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(num_rows));
    state.counters["rows"] = benchmark::Counter(static_cast<double>(num_rows));
}

BENCHMARK(BM_MemTableInsertToBlock)
        ->Args({KeysType::DUP_KEYS, 65536, 8192})
        ->Args({KeysType::UNIQUE_KEYS, 65536, 8192})
        ->Args({KeysType::AGG_KEYS, 65536, 8192})
        ->Unit(benchmark::kMillisecond);

} // namespace doris
