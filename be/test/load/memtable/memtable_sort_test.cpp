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

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "load/delta_writer/delta_writer_context.h"
#include "load/memtable/memtable.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

// Schema used by every case: k1 INT (key), k2 VARCHAR (key, nullable), v INT.
// Two key columns are needed so the equal-range refinement between key columns
// is exercised, and k2 is nullable so the ColumnNullable sort path is covered.
// AGG_KEYS additionally gets bm BITMAP BITMAP_UNION. Its aggregate state owns
// heap memory, unlike SUM over an int, so releasing a state twice is an actual
// double free there and the shrink-round cases below can catch it.
bool has_bitmap_col(KeysType keys_type) {
    return keys_type == KeysType::AGG_KEYS;
}

TabletSchemaSPtr create_schema(KeysType keys_type) {
    TabletSchemaPB pb;
    pb.set_keys_type(keys_type);

    auto add = [&](const std::string& name, const std::string& type, bool is_key, bool nullable,
                   int32_t length, const std::string& agg) {
        ColumnPB* c = pb.add_column();
        c->set_unique_id(pb.column_size());
        c->set_name(name);
        c->set_type(type);
        c->set_is_key(is_key);
        c->set_is_nullable(nullable);
        c->set_length(length);
        c->set_aggregation(agg);
        c->set_is_bf_column(false);
    };
    add("k1", "INT", true, false, 4, "NONE");
    add("k2", "VARCHAR", true, true, 20, "NONE");
    // value aggregation only matters for AGG_KEYS; UNIQUE_KEYS always replaces
    add("v", "INT", false, false, 4, keys_type == KeysType::AGG_KEYS ? "SUM" : "REPLACE");
    if (has_bitmap_col(keys_type)) {
        add("bm", "BITMAP", false, false, 16, "BITMAP_UNION");
    }

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(pb);
    return schema;
}

TDescriptorTable create_descriptor_table(KeysType keys_type) {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_INT)
                                   .nullable(false)
                                   .column_name("k1")
                                   .column_pos(0)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .string_type(20)
                                   .nullable(true)
                                   .column_name("k2")
                                   .column_pos(1)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_INT)
                                   .nullable(false)
                                   .column_name("v")
                                   .column_pos(2)
                                   .build());
    if (has_bitmap_col(keys_type)) {
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_BITMAP)
                                       .nullable(false)
                                       .column_name("bm")
                                       .column_pos(3)
                                       .build());
    }
    tuple_builder.build(&dtb);
    return dtb.desc_tbl();
}

struct Row {
    int32_t k1;
    const char* k2; // nullptr means SQL NULL
    int32_t v;
};

} // namespace

class MemTableSortTest : public testing::Test {
protected:
    // Feeds `rows` through a MemTable in `batches` insert() calls and hands back
    // the flushed block in `out`. Going through to_block() means the real _sort()
    // runs. Fatal assertions abort this helper, so callers wrap it in
    // ASSERT_NO_FATAL_FAILURE rather than reading a half-built block.
    void run(KeysType keys_type, const std::vector<Row>& rows, size_t batches,
             std::unique_ptr<Block>* out, bool shrink_between_batches = false) {
        TabletSchemaSPtr schema = create_schema(keys_type);
        TDescriptorTable tdesc = create_descriptor_table(keys_type);
        ObjectPool pool;
        DescriptorTbl* desc_tbl = nullptr;
        ASSERT_TRUE(DescriptorTbl::create(&pool, tdesc, &desc_tbl).ok());
        TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
        ASSERT_NE(nullptr, tuple_desc);
        auto resource_ctx = ResourceContext::create_shared();
        // MemTable dereferences this tracker in its constructor, and a freshly
        // created context has none; production installs one the same way.
        resource_ctx->memory_context()->set_mem_tracker(MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::LOAD, "MemTableSortTest"));

        MemTable mem_table(10000, schema, &tuple_desc->slots(), tuple_desc,
                           false /*enable_unique_key_mow*/, nullptr /*partial_update_info*/,
                           resource_ctx, false /*need_row_binlog_lsn*/);

        const size_t per_batch = (rows.size() + batches - 1) / batches;
        for (size_t begin = 0; begin < rows.size(); begin += per_batch) {
            const size_t end = std::min(begin + per_batch, rows.size());
            Block block;
            for (const auto* slot : tuple_desc->slots()) {
                block.insert(ColumnWithTypeAndName(slot->get_empty_mutable_column(), slot->type(),
                                                   slot->col_name()));
            }
            auto columns = std::move(block).mutate_columns();
            for (size_t i = begin; i < end; ++i) {
                columns[0]->insert_data(reinterpret_cast<const char*>(&rows[i].k1),
                                        sizeof(rows[i].k1));
                if (rows[i].k2 == nullptr) {
                    columns[1]->insert_default();
                } else {
                    columns[1]->insert_data(rows[i].k2, strlen(rows[i].k2));
                }
                columns[2]->insert_data(reinterpret_cast<const char*>(&rows[i].v),
                                        sizeof(rows[i].v));
                if (has_bitmap_col(keys_type)) {
                    // Enough values to push BitmapValue past its inline SINGLE
                    // representation into a heap-backed roaring bitmap, so that
                    // releasing the aggregate state twice is a real double free.
                    BitmapValue bitmap;
                    for (uint64_t b = 0; b < 128; ++b) {
                        bitmap.add(static_cast<uint64_t>(rows[i].v) * 100000 + b * 977);
                    }
                    assert_cast<ColumnBitmap*>(columns[3].get())->insert_value(std::move(bitmap));
                }
            }
            block.set_columns(std::move(columns));

            TabletAddRowsPayload payload;
            for (uint32_t i = 0; i < end - begin; ++i) {
                payload.row_idxs.push_back(i);
            }
            Status st = mem_table.insert(&block, payload);
            ASSERT_TRUE(st.ok()) << st;
            if (shrink_between_batches && end < rows.size()) {
                // Runs a non-final aggregate: rows that survive keep their
                // aggregate state and get aggregated into again next round.
                // Deliberately skipped for the last batch, so the duplicates it
                // introduces are still there for the final aggregate in
                // to_block() to fold into those surviving rows.
                mem_table.shrink_memtable_by_agg();
            }
        }

        Status st = mem_table.to_block(out);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_NE(nullptr, *out);
    }

    static std::string k2_of(const Block& b, size_t row) {
        StringRef ref = b.get_by_position(1).column->get_data_at(row);
        return ref.data == nullptr ? std::string("<null>") : ref.to_string();
    }
    static int32_t int_of(const Block& b, size_t pos, size_t row) {
        ColumnPtr col = b.get_by_position(pos).column;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(col.get())) {
            col = nullable->get_nested_column_ptr();
        }
        return static_cast<int32_t>(col->get_int(row));
    }
    static int32_t k1_of(const Block& b, size_t row) { return int_of(b, 0, row); }
    static int32_t v_of(const Block& b, size_t row) { return int_of(b, 2, row); }
};

// Keys are ordered by (k1, k2); the second key column must only be used to
// refine rows that tie on the first one.
TEST_F(MemTableSortTest, DupKeysOrdersByAllKeyColumns) {
    std::vector<Row> rows = {{2, "b", 20}, {1, "b", 11}, {2, "a", 21}, {1, "a", 10}};
    std::unique_ptr<Block> out;
    ASSERT_NO_FATAL_FAILURE(run(KeysType::DUP_KEYS, rows, 1, &out));
    ASSERT_EQ(4, out->rows());
    EXPECT_EQ(1, k1_of(*out, 0));
    EXPECT_EQ("a", k2_of(*out, 0));
    EXPECT_EQ(1, k1_of(*out, 1));
    EXPECT_EQ("b", k2_of(*out, 1));
    EXPECT_EQ(2, k1_of(*out, 2));
    EXPECT_EQ("a", k2_of(*out, 2));
    EXPECT_EQ(2, k1_of(*out, 3));
    EXPECT_EQ("b", k2_of(*out, 3));
}

// The permutation is applied to _row_in_blocks in place by following its
// cycles, so cover a permutation that is one long cycle rather than the short
// swaps the other cases happen to produce. Keys [5,1,2,3,4] sort to sources
// [1,2,3,4,0], i.e. a single 5-cycle.
TEST_F(MemTableSortTest, SingleCyclePermutation) {
    std::vector<Row> rows = {{5, "a", 50}, {1, "a", 10}, {2, "a", 20}, {3, "a", 30}, {4, "a", 40}};
    std::unique_ptr<Block> out;
    ASSERT_NO_FATAL_FAILURE(run(KeysType::DUP_KEYS, rows, 1, &out));
    ASSERT_EQ(5, out->rows());
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_EQ(static_cast<int32_t>(i + 1), k1_of(*out, i)) << "row " << i;
        EXPECT_EQ(static_cast<int32_t>((i + 1) * 10), v_of(*out, i)) << "row " << i;
    }
}

// Rows sharing the whole key are stabilised on descending row position for
// DUP_KEYS, i.e. reverse insertion order. Nothing depends on that direction in
// principle, but a number of regression cases record it, so pin it down.
TEST_F(MemTableSortTest, DupKeysReversesEqualKeys) {
    std::vector<Row> rows = {{1, "a", 100}, {1, "a", 101}, {1, "a", 102}};
    std::unique_ptr<Block> out;
    ASSERT_NO_FATAL_FAILURE(run(KeysType::DUP_KEYS, rows, 1, &out));
    ASSERT_EQ(3, out->rows());
    EXPECT_EQ(102, v_of(*out, 0));
    EXPECT_EQ(101, v_of(*out, 1));
    EXPECT_EQ(100, v_of(*out, 2));
}

// NULL sorts before any value, matching the nan_direction_hint = -1 the previous
// comparator used.
TEST_F(MemTableSortTest, NullKeySortsFirst) {
    std::vector<Row> rows = {{1, "b", 2}, {1, nullptr, 1}, {1, "a", 3}};
    std::unique_ptr<Block> out;
    ASSERT_NO_FATAL_FAILURE(run(KeysType::DUP_KEYS, rows, 1, &out));
    ASSERT_EQ(3, out->rows());
    EXPECT_EQ("<null>", k2_of(*out, 0));
    EXPECT_EQ("a", k2_of(*out, 1));
    EXPECT_EQ("b", k2_of(*out, 2));
}

// Splitting the same rows across several insert() calls must not change the
// result: _sort() maps a sorted row position back to its RowInBlock through the
// base of the appended range, which only holds if row positions stay contiguous
// across insert() calls. The equal keys make that mapping observable.
TEST_F(MemTableSortTest, ResultIsIndependentOfBatching) {
    std::vector<Row> rows = {{3, "c", 1}, {1, "a", 2}, {2, "b", 3}, {1, "b", 4},
                             {3, "a", 5}, {2, "a", 6}, {1, "a", 7}, {2, "a", 8}};
    std::unique_ptr<Block> one;
    std::unique_ptr<Block> many;
    ASSERT_NO_FATAL_FAILURE(run(KeysType::DUP_KEYS, rows, 1, &one));
    ASSERT_NO_FATAL_FAILURE(run(KeysType::DUP_KEYS, rows, 3, &many));
    ASSERT_EQ(one->rows(), many->rows());
    for (size_t i = 0; i < one->rows(); ++i) {
        EXPECT_EQ(k1_of(*one, i), k1_of(*many, i)) << "row " << i;
        EXPECT_EQ(k2_of(*one, i), k2_of(*many, i)) << "row " << i;
        EXPECT_EQ(v_of(*one, i), v_of(*many, i)) << "row " << i;
    }
}

// For UNIQUE_KEYS the last inserted row must win, which relies on equal keys
// being ordered ascending by row position before aggregation runs.
TEST_F(MemTableSortTest, UniqueKeysLastWriterWins) {
    std::vector<Row> rows = {{1, "a", 10}, {2, "b", 20}, {1, "a", 11}, {1, "a", 12}};
    std::unique_ptr<Block> out;
    ASSERT_NO_FATAL_FAILURE(run(KeysType::UNIQUE_KEYS, rows, 1, &out));
    ASSERT_EQ(2, out->rows());
    EXPECT_EQ(1, k1_of(*out, 0));
    EXPECT_EQ(12, v_of(*out, 0)) << "the last inserted value must survive";
    EXPECT_EQ(2, k1_of(*out, 1));
    EXPECT_EQ(20, v_of(*out, 1));
}

// shrink_memtable_by_agg() aggregates without finalising, so a surviving row
// carries its aggregate state into the next round and to_block() finalises it.
// Interleaving that with more inserts must not change the result -- this covers
// the state handoff between rounds, and the release of those states, which is
// where holding rows by value differs most from holding them behind a pointer.
TEST_F(MemTableSortTest, AggKeysSurviveShrinkRounds) {
    std::vector<Row> rows = {{1, "a", 1},  {2, "b", 10}, {1, "a", 2},  {3, "c", 100},
                             {2, "b", 20}, {1, "a", 4},  {2, "b", 30}, {1, "a", 8}};
    std::unique_ptr<Block> plain;
    std::unique_ptr<Block> shrunk;
    ASSERT_NO_FATAL_FAILURE(run(KeysType::AGG_KEYS, rows, 4, &plain));
    ASSERT_NO_FATAL_FAILURE(run(KeysType::AGG_KEYS, rows, 4, &shrunk, true));
    ASSERT_EQ(3, shrunk->rows());
    ASSERT_EQ(plain->rows(), shrunk->rows());
    for (size_t i = 0; i < plain->rows(); ++i) {
        EXPECT_EQ(k1_of(*plain, i), k1_of(*shrunk, i)) << "row " << i;
        EXPECT_EQ(v_of(*plain, i), v_of(*shrunk, i)) << "row " << i;
    }
    EXPECT_EQ(15, v_of(*shrunk, 0)); // 1 + 2 + 4 + 8
    EXPECT_EQ(60, v_of(*shrunk, 1)); // 10 + 20 + 30
    EXPECT_EQ(100, v_of(*shrunk, 2));
}

// Same handoff for UNIQUE_KEYS, where a round must keep the newest row rather
// than accumulate.
TEST_F(MemTableSortTest, UniqueKeysSurviveShrinkRounds) {
    std::vector<Row> rows = {{1, "a", 10}, {2, "b", 20}, {1, "a", 11},
                             {2, "b", 21}, {1, "a", 12}, {3, "c", 30}};
    std::unique_ptr<Block> out;
    ASSERT_NO_FATAL_FAILURE(run(KeysType::UNIQUE_KEYS, rows, 3, &out, true));
    ASSERT_EQ(3, out->rows());
    EXPECT_EQ(12, v_of(*out, 0));
    EXPECT_EQ(21, v_of(*out, 1));
    EXPECT_EQ(30, v_of(*out, 2));
}

// AGG_KEYS with SUM: every duplicate must be folded into the group exactly once.
TEST_F(MemTableSortTest, AggKeysSumsDuplicates) {
    std::vector<Row> rows = {{1, "a", 1}, {2, "b", 100}, {1, "a", 2}, {1, "a", 4}, {2, "b", 200}};
    std::unique_ptr<Block> out;
    ASSERT_NO_FATAL_FAILURE(run(KeysType::AGG_KEYS, rows, 1, &out));
    ASSERT_EQ(2, out->rows());
    EXPECT_EQ(1, k1_of(*out, 0));
    EXPECT_EQ(7, v_of(*out, 0));
    EXPECT_EQ(2, k1_of(*out, 1));
    EXPECT_EQ(300, v_of(*out, 1));
}

} // namespace doris
