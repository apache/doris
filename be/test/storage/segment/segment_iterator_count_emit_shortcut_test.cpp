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

// G03 count-emission shortcut white-box tests. Given a post-apply _row_bitmap
// of cardinality N (the exact count G02 fabricated), the shortcut must emit
// EXACTLY N rows shaped as NOT-NULL defaults across VStatisticsIterator-sized
// batches, then EOF -- byte-for-byte what today's per-rowid batch loop emits
// for a count-fastpath scan, minus the per-rowid work. The engage decision
// must admit only the provably emission-only configuration and refuse on any
// deviation (falling through to today's loop). Uses the established
// `#define private public` convention of segment_iterator_limit_opt_test.cpp
// over a bare SegmentIterator (no real segment needed: the shortcut never
// touches segment data).
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "storage/index/index_query_context.h"
#include "storage/olap_common.h"
#include "storage/segment/count_on_index_fastpath.h"
#include "storage/tablet/tablet_schema.h"

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#include "storage/segment/segment_iterator.h"
#undef private
#undef protected
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris::segment_v2 {

namespace {

constexpr uint64_t kBatchRows = SegmentIterator::kCountEmitBatchRows; // 65535

void reset_emit_counters() {
    internal::count_emit_test_counters() = internal::CountEmitTestCounters {};
}

uint64_t emit_hits() {
    return internal::count_emit_test_counters().count_emit_shortcut_hits;
}

uint64_t emit_batches() {
    return internal::count_emit_test_counters().count_emit_shortcut_batches;
}

// DUP-key schema shaped like a COUNT_ON_INDEX scan: an INT key plus the
// (nullable STRING) match column. Both columns end up defaults-filled by
// today's path once the index consumed the MATCH predicate.
TabletSchemaSPtr make_tablet_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* key_col = schema_pb.add_column();
    key_col->set_unique_id(0);
    key_col->set_name("k1");
    key_col->set_type("INT");
    key_col->set_is_key(true);
    key_col->set_is_nullable(true);
    auto* match_col = schema_pb.add_column();
    match_col->set_unique_id(1);
    match_col->set_name("content");
    match_col->set_type("STRING");
    match_col->set_is_key(false);
    match_col->set_is_nullable(true);
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    return tablet_schema;
}

SchemaSPtr make_read_schema(const TabletSchemaSPtr& tablet_schema) {
    std::vector<ColumnId> read_column_ids(tablet_schema->num_columns());
    for (uint32_t cid = 0; cid < read_column_ids.size(); ++cid) {
        read_column_ids[cid] = cid;
    }
    return std::make_shared<Schema>(tablet_schema->columns(), read_column_ids);
}

Block make_block_for(const Schema& schema) {
    Block block;
    for (size_t i = 0; i < schema.num_column_ids(); ++i) {
        const auto* col_desc = schema.column(schema.column_id(i));
        auto data_type = Schema::get_data_type_ptr(*col_desc);
        block.insert(
                ColumnWithTypeAndName(data_type->create_column(), data_type, col_desc->name()));
    }
    return block;
}

struct Fixture {
    TabletSchemaSPtr tablet_schema;
    SchemaSPtr read_schema;
    std::unique_ptr<SegmentIterator> iter;
    OlapReaderStatistics stats;

    Fixture() {
        tablet_schema = make_tablet_schema();
        read_schema = make_read_schema(tablet_schema);
        iter = std::make_unique<SegmentIterator>(nullptr, read_schema);
        iter->_opts.tablet_schema = tablet_schema;
        iter->_opts.push_down_agg_type_opt = TPushAggOp::COUNT_ON_INDEX;
        iter->_opts.stats = &stats;
        // State _lazy_init/_vec_init_lazy_materialization would have produced
        // for a count-fastpath scan: no predicate columns, no lazy
        // materialization, index fully answered every column's conditions.
        iter->_is_pred_column.resize(read_schema->columns().size(), false);
        iter->_lazy_materialization_read = false;
        iter->_storage_name_and_type.resize(read_schema->columns().size());
        for (size_t i = 0; i < read_schema->num_column_ids(); ++i) {
            ColumnId cid = read_schema->column_id(i);
            iter->_storage_name_and_type[cid] =
                    std::make_pair(read_schema->column(cid)->name(),
                                   Schema::get_data_type_ptr(*read_schema->column(cid)));
            iter->_need_read_data_indices[cid] = false;
        }
    }

    // Simulates the reader having fabricated a count bitmap (G02 hit).
    void set_hit() { iter->_count_fastpath_hit = true; }

    // Simulates the _lazy_init engage step for a fabricated bitmap of
    // cardinality `count`.
    void engage_with_count(uint64_t count) {
        iter->_count_emit_shortcut = true;
        iter->_count_emit_rows_remaining = count;
    }

    // Drives _emit_count_shortcut_batch until EOF; returns per-batch rows.
    std::vector<size_t> drain(Block* block) {
        std::vector<size_t> batch_rows;
        while (true) {
            Status st = iter->_emit_count_shortcut_batch(block);
            if (st.is<ErrorCode::END_OF_FILE>()) {
                EXPECT_EQ(block->rows(), 0U) << "EOF must deliver an empty block";
                break;
            }
            EXPECT_TRUE(st.ok()) << st.to_string();
            batch_rows.push_back(block->rows());
        }
        return batch_rows;
    }
};

uint64_t total_rows(const std::vector<size_t>& batches) {
    uint64_t total = 0;
    for (size_t rows : batches) {
        total += rows;
    }
    return total;
}

} // namespace

// The emitted total must EQUAL the fabricated count for counts spanning
// several batches plus a tail: this is the count-equality contract with
// today's per-rowid loop (which emits exactly |_row_bitmap| rows).
TEST(CountEmitShortcut, EmitsExactCountAcrossBatchesThenEof) {
    reset_emit_counters();
    Fixture fx;
    const uint64_t count = 3 * kBatchRows + 3395; // 200000
    fx.engage_with_count(count);

    Block block = make_block_for(*fx.read_schema);
    const auto batches = fx.drain(&block);

    ASSERT_EQ(batches.size(), 4U);
    EXPECT_EQ(batches[0], kBatchRows);
    EXPECT_EQ(batches[1], kBatchRows);
    EXPECT_EQ(batches[2], kBatchRows);
    EXPECT_EQ(batches[3], 3395U);
    EXPECT_EQ(total_rows(batches), count);
    EXPECT_EQ(fx.stats.raw_rows_read, static_cast<int64_t>(count));
    EXPECT_EQ(emit_batches(), 4U);

    // A second call after EOF stays EOF (the scanner may re-poll).
    Status st = fx.iter->_emit_count_shortcut_batch(&block);
    EXPECT_TRUE(st.is<ErrorCode::END_OF_FILE>());
    EXPECT_EQ(block.rows(), 0U);
}

// Batch-boundary counts: exact batch multiples and off-by-one neighbours all
// sum to the requested count with ceil(count / batch) batches.
TEST(CountEmitShortcut, BatchBoundaryCountsAreExact) {
    for (const uint64_t count : {uint64_t(1), kBatchRows - 1, kBatchRows, kBatchRows + 1}) {
        reset_emit_counters();
        Fixture fx;
        fx.engage_with_count(count);
        Block block = make_block_for(*fx.read_schema);
        const auto batches = fx.drain(&block);
        EXPECT_EQ(total_rows(batches), count) << "count: " << count;
        const uint64_t expected_batches = (count + kBatchRows - 1) / kBatchRows;
        EXPECT_EQ(batches.size(), expected_batches) << "count: " << count;
        EXPECT_EQ(emit_batches(), expected_batches) << "count: " << count;
        for (size_t rows : batches) {
            EXPECT_LE(rows, kBatchRows);
        }
    }
}

// Empty result (df == 0, e.g. the term missed the dict): immediate EOF with
// zero emitted rows -- identical to today's empty-bitmap first batch.
TEST(CountEmitShortcut, EmptyResultIsImmediateEof) {
    reset_emit_counters();
    Fixture fx;
    fx.engage_with_count(0);
    Block block = make_block_for(*fx.read_schema);
    Status st = fx.iter->_emit_count_shortcut_batch(&block);
    EXPECT_TRUE(st.is<ErrorCode::END_OF_FILE>());
    EXPECT_EQ(block.rows(), 0U);
    EXPECT_EQ(fx.stats.raw_rows_read, 0);
    EXPECT_EQ(emit_batches(), 0U);
}

// Nullable columns must be filled with NOT-NULL defaults exactly like the
// _no_need_read_key_data/_prune_column split fill. A raw
// ColumnNullable::insert_many_defaults would insert NULLs, and count(col)
// (planned under COUNT_ON_INDEX for count(match_col)) counts non-null values:
// NULL-filled rows would collapse the count to zero.
TEST(CountEmitShortcut, NullableColumnsEmitNotNullDefaults) {
    Fixture fx;
    fx.engage_with_count(1000);
    Block block = make_block_for(*fx.read_schema);
    ASSERT_TRUE(fx.iter->_emit_count_shortcut_batch(&block).ok());
    ASSERT_EQ(block.rows(), 1000U);

    // INT key column: not-null zeros.
    const auto* key_col = assert_cast<const ColumnNullable*>(block.get_by_position(0).column.get());
    EXPECT_FALSE(key_col->has_null());
    const auto& key_data =
            assert_cast<const ColumnVector<TYPE_INT>&>(key_col->get_nested_column()).get_data();
    for (size_t j = 0; j < key_data.size(); ++j) {
        ASSERT_EQ(key_data[j], 0) << "row " << j;
    }

    // STRING match column: not-null empty strings with valid offsets.
    const auto* match_col =
            assert_cast<const ColumnNullable*>(block.get_by_position(1).column.get());
    EXPECT_FALSE(match_col->has_null());
    const auto& match_data = assert_cast<const ColumnString&>(match_col->get_nested_column());
    ASSERT_EQ(match_data.size(), 1000U);
    for (size_t j = 0; j < match_data.size(); ++j) {
        ASSERT_EQ(match_data.get_data_at(j).size, 0U) << "row " << j;
    }
}

// The shortcut consumes only the CARDINALITY of the post-apply bitmap; the id
// positions are irrelevant. Pins the null-bearing G02 shape (null-disjoint
// fabrication produces a SHIFTED range like [100, 150), not [0, 50)).
TEST(CountEmitShortcut, NullDisjointFabricatedShapeCountsByCardinality) {
    Fixture fx;
    fx.iter->_row_bitmap = roaring::Roaring {};
    fx.iter->_row_bitmap.addRange(100, 150);
    // The exact engage math from _lazy_init.
    fx.engage_with_count(fx.iter->_row_bitmap.cardinality());
    Block block = make_block_for(*fx.read_schema);
    const auto batches = fx.drain(&block);
    EXPECT_EQ(total_rows(batches), 50U);
}

// Segment iterators keep independent countdowns: a multi-segment scan is the
// sum of per-segment counts, each emitted by its own iterator.
TEST(CountEmitShortcut, MultipleIteratorsEmitIndependently) {
    reset_emit_counters();
    Fixture fx1;
    Fixture fx2;
    fx1.engage_with_count(kBatchRows + 5000); // 2 batches
    fx2.engage_with_count(3);                 // 1 batch
    Block block1 = make_block_for(*fx1.read_schema);
    Block block2 = make_block_for(*fx2.read_schema);

    const auto batches1 = fx1.drain(&block1);
    const auto batches2 = fx2.drain(&block2);
    EXPECT_EQ(total_rows(batches1), kBatchRows + 5000);
    EXPECT_EQ(total_rows(batches2), 3U);
    EXPECT_EQ(batches1.size(), 2U);
    EXPECT_EQ(batches2.size(), 1U);
    EXPECT_EQ(emit_batches(), 3U);
}

// G02->G03 handshake teardown: the reply flag is captured into the iterator
// and BOTH context flags are cleared so no later read_from_index call can
// observe or forge them.
TEST(CountEmitShortcut, CaptureHitRecordsReplyAndResetsHandshake) {
    Fixture fx;
    fx.iter->_index_query_context = std::make_shared<IndexQueryContext>();
    fx.iter->_index_query_context->count_on_index_fastpath = true;
    fx.iter->_index_query_context->count_on_index_fastpath_hit = true;

    fx.iter->_capture_count_fastpath_hit();
    EXPECT_TRUE(fx.iter->_count_fastpath_hit);
    EXPECT_FALSE(fx.iter->_index_query_context->count_on_index_fastpath);
    EXPECT_FALSE(fx.iter->_index_query_context->count_on_index_fastpath_hit);

    // No reply (row-accurate decode / cache hit): captured false.
    fx.iter->_index_query_context->count_on_index_fastpath = true;
    fx.iter->_capture_count_fastpath_hit();
    EXPECT_FALSE(fx.iter->_count_fastpath_hit);
    EXPECT_FALSE(fx.iter->_index_query_context->count_on_index_fastpath);

    // Null context (index machinery never initialized): no crash, no hit.
    fx.iter->_index_query_context = nullptr;
    fx.iter->_capture_count_fastpath_hit();
    EXPECT_FALSE(fx.iter->_count_fastpath_hit);
}

// Engage admits the one emission-only configuration and counts a seam hit.
TEST(CountEmitShortcut, EngageAdmitsCleanCountFastpathState) {
    reset_emit_counters();
    Fixture fx;
    fx.set_hit();
    Block block = make_block_for(*fx.read_schema);
    EXPECT_TRUE(fx.iter->_should_engage_count_emit_shortcut(&block));
    EXPECT_EQ(emit_hits(), 1U);
}

// Engage refusals: every deviation keeps today's per-rowid loop. The seam
// stays untouched on refusals.
TEST(CountEmitShortcut, EngageRefusalTruthTable) {
    reset_emit_counters();
    // Without the reader hit (row-accurate bitmap): refuse.
    {
        Fixture fx;
        Block block = make_block_for(*fx.read_schema);
        EXPECT_FALSE(fx.iter->_should_engage_count_emit_shortcut(&block));
    }
    // A surviving evaluation stage (e.g. index-eval downgrade kept the expr).
    {
        Fixture fx;
        fx.set_hit();
        fx.iter->_is_need_expr_eval = true;
        Block block = make_block_for(*fx.read_schema);
        EXPECT_FALSE(fx.iter->_should_engage_count_emit_shortcut(&block));
    }
    // A pushed-down read limit must keep the limit-aware loop.
    {
        Fixture fx;
        fx.set_hit();
        fx.iter->_opts.read_limit = 10;
        Block block = make_block_for(*fx.read_schema);
        EXPECT_FALSE(fx.iter->_should_engage_count_emit_shortcut(&block));
    }
    // Condition-cache writes are only produced by the real batch loop.
    {
        Fixture fx;
        fx.set_hit();
        fx.iter->_opts.condition_cache_digest = 7;
        Block block = make_block_for(*fx.read_schema);
        EXPECT_FALSE(fx.iter->_should_engage_count_emit_shortcut(&block));
    }
    // Rowid consumers need real row ids.
    {
        Fixture fx;
        fx.set_hit();
        fx.iter->_opts.record_rowids = true;
        Block block = make_block_for(*fx.read_schema);
        EXPECT_FALSE(fx.iter->_should_engage_count_emit_shortcut(&block));
    }
    // Block shape != read schema (e.g. delete-condition column beyond block).
    {
        Fixture fx;
        fx.set_hit();
        Block block = make_block_for(*fx.read_schema);
        block.erase(1);
        EXPECT_FALSE(fx.iter->_should_engage_count_emit_shortcut(&block));
    }
    // A column today's path would REALLY read (index did not answer its
    // conditions): values would come from column data, not defaults.
    {
        Fixture fx;
        fx.set_hit();
        fx.iter->_need_read_data_indices.erase(1);
        Block block = make_block_for(*fx.read_schema);
        EXPECT_FALSE(fx.iter->_should_engage_count_emit_shortcut(&block));
    }
    // A storage->schema cast column: today's path emits CAST(file-type
    // default), which the shortcut does not reproduce. Simulated by giving
    // the INT key column the STRING column's storage type.
    {
        Fixture fx;
        fx.set_hit();
        fx.iter->_storage_name_and_type[0].second =
                Schema::get_data_type_ptr(*fx.read_schema->column(1));
        Block block = make_block_for(*fx.read_schema);
        EXPECT_FALSE(fx.iter->_should_engage_count_emit_shortcut(&block));
    }
    EXPECT_EQ(emit_hits(), 0U);
}

} // namespace doris::segment_v2
