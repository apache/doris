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

#include <memory>

#include "common/status.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_state.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/mock/mock_fn_call.h"
#include "testutil/mock/mock_literal_expr.h"
#include "testutil/mock/mock_slot_ref.h"

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"
#undef private
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris::segment_v2 {

namespace {

class MockZoneMapExprColumnIterator : public ColumnIterator {
public:
    struct Entry {
        ZoneMap zone_map;
        RowRange row_range;
    };

    explicit MockZoneMapExprColumnIterator(std::vector<Entry> entries, int64_t pass_all_count = 0,
                                           int64_t missing_index_count = 0)
            : _entries(std::move(entries)),
              _pass_all_count(pass_all_count),
              _missing_index_count(missing_index_count) {}

    Status seek_to_ordinal(ordinal_t ord) override { return Status::OK(); }
    ordinal_t get_current_ordinal() const override { return 0; }

    Status get_row_ranges_by_zone_map_expr(const ZoneMapMatchFunc& match_func,
                                           RowRanges* row_ranges, int64_t* pass_all_count,
                                           int64_t* missing_index_count) override {
        ++calls;
        row_ranges->clear();
        for (const auto& entry : _entries) {
            if (match_func(entry.zone_map)) {
                row_ranges->add(entry.row_range);
            }
        }
        if (pass_all_count != nullptr) {
            *pass_all_count = _pass_all_count;
        }
        if (missing_index_count != nullptr) {
            *missing_index_count = _missing_index_count;
        }
        return Status::OK();
    }

    int calls = 0;

private:
    std::vector<Entry> _entries;
    int64_t _pass_all_count;
    int64_t _missing_index_count;
};

TabletSchemaSPtr make_tablet_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* col = schema_pb.add_column();
    col->set_unique_id(0);
    col->set_name("k0");
    col->set_type("INT");
    col->set_is_key(true);
    col->set_is_nullable(false);
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    return tablet_schema;
}

std::shared_ptr<Segment> make_stub_segment(uint32_t num_rows,
                                           const TabletSchemaSPtr& tablet_schema) {
    auto seg = std::make_shared<Segment>(0, RowsetId(), tablet_schema, InvertedIndexFileInfo());
    seg->_num_rows = num_rows;
    return seg;
}

} // namespace

class SegmentIteratorExprZoneMapTest : public testing::Test {
protected:
    void SetUp() override {
        _tablet_schema = make_tablet_schema();
        _segment = make_stub_segment(100, _tablet_schema);
        _read_schema = std::make_shared<Schema>(_tablet_schema);
        _iter = std::make_unique<SegmentIterator>(_segment, _read_schema);

        TQueryOptions query_options;
        query_options.__set_enable_fallback_on_missing_inverted_index(true);
        _runtime_state.set_query_options(query_options);

        _iter->_opts.runtime_state = &_runtime_state;
        _iter->_opts.stats = &_stats;
        _iter->_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    }

    static ZoneMap make_zone_map(int32_t min_value, int32_t max_value, bool has_null = false,
                                 bool has_not_null = true, bool pass_all = false) {
        ZoneMap zone_map;
        zone_map.min_value = Field::create_field<TYPE_INT>(min_value);
        zone_map.max_value = Field::create_field<TYPE_INT>(max_value);
        zone_map.has_null = has_null;
        zone_map.has_not_null = has_not_null;
        zone_map.pass_all = pass_all;
        return zone_map;
    }

    static VExprContextSPtr make_binary_ctx(
            TExprOpcode::type opcode, int32_t literal_value,
            const DataTypePtr& literal_type = std::make_shared<DataTypeInt32>()) {
        auto slot_ref = std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt32>());
        slot_ref->_slot_id = 0;
        slot_ref->_column_id = 0;
        slot_ref->set_expr_name("k0");

        auto fn = MockFnCall::create(opcode == TExprOpcode::GT ? "gt" : "eq");
        fn->add_child(slot_ref);
        if (literal_type->get_primitive_type() == PrimitiveType::TYPE_STRING) {
            fn->add_child(std::make_shared<MockLiteral>(
                    ColumnHelper::create_column_with_name<DataTypeString>(
                            {std::to_string(literal_value)})));
        } else {
            fn->add_child(std::make_shared<MockLiteral>(
                    ColumnHelper::create_column_with_name<DataTypeInt32>({literal_value})));
        }
        fn->_node_type = TExprNodeType::BINARY_PRED;
        fn->_opcode = opcode;

        auto ctx = VExprContext::create_shared(fn);
        ctx->_prepared = true;
        ctx->_opened = true;
        return ctx;
    }

    std::shared_ptr<Segment> _segment;
    TabletSchemaSPtr _tablet_schema;
    SchemaSPtr _read_schema;
    std::unique_ptr<SegmentIterator> _iter;
    RuntimeState _runtime_state;
    OlapReaderStatistics _stats;
};

TEST_F(SegmentIteratorExprZoneMapTest, prunes_ranges_and_updates_stats) {
    auto* mock_iter = new MockZoneMapExprColumnIterator(
            {{make_zone_map(0, 9), RowRange(0, 10)}, {make_zone_map(20, 29), RowRange(10, 20)}}, 1,
            2);
    _iter->_column_iterators.resize(1);
    _iter->_column_iterators[0].reset(mock_iter);
    _iter->_zone_map_expr_ctxs = {make_binary_ctx(TExprOpcode::GT, 15)};
    _iter->_zone_map_expr_cids = {0};
    _iter->_storage_cid_to_slot_index[0] = 0;

    RowRanges condition_row_ranges = RowRanges::create_single(20);
    Status st = _iter->_get_row_ranges_from_conditions(&condition_row_ranges);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(10, condition_row_ranges.count());
    EXPECT_EQ(10, condition_row_ranges.from());
    EXPECT_EQ(20, condition_row_ranges.to());
    EXPECT_EQ(1, mock_iter->calls);
    EXPECT_EQ(2, _stats.zone_map_expr_evaluated);
    EXPECT_EQ(1, _stats.zone_map_expr_skipped_pages);
    EXPECT_EQ(0, _stats.zone_map_expr_unsupported);
    EXPECT_EQ(1, _stats.zone_map_expr_missing_stats);
    EXPECT_EQ(2, _stats.zone_map_expr_missing_index);
}

TEST_F(SegmentIteratorExprZoneMapTest, missing_slot_mapping_is_unsupported) {
    auto* mock_iter = new MockZoneMapExprColumnIterator(
            {{make_zone_map(0, 9), RowRange(0, 10)}, {make_zone_map(20, 29), RowRange(10, 20)}});
    _iter->_column_iterators.resize(1);
    _iter->_column_iterators[0].reset(mock_iter);
    _iter->_zone_map_expr_ctxs = {make_binary_ctx(TExprOpcode::GT, 15)};
    _iter->_zone_map_expr_cids = {0};

    RowRanges condition_row_ranges = RowRanges::create_single(20);
    Status st = _iter->_get_row_ranges_from_conditions(&condition_row_ranges);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(20, condition_row_ranges.count());
    EXPECT_EQ(0, mock_iter->calls);
    EXPECT_EQ(1, _stats.zone_map_expr_unsupported);
    EXPECT_EQ(0, _stats.zone_map_expr_evaluated);
}

TEST_F(SegmentIteratorExprZoneMapTest, type_mismatch_is_conservative) {
    auto* mock_iter = new MockZoneMapExprColumnIterator(
            {{make_zone_map(0, 9), RowRange(0, 10)}, {make_zone_map(20, 29), RowRange(10, 20)}});
    _iter->_column_iterators.resize(1);
    _iter->_column_iterators[0].reset(mock_iter);
    _iter->_zone_map_expr_ctxs = {
            make_binary_ctx(TExprOpcode::GT, 15, std::make_shared<DataTypeString>())};
    _iter->_zone_map_expr_cids = {0};
    _iter->_storage_cid_to_slot_index[0] = 0;

    RowRanges condition_row_ranges = RowRanges::create_single(20);
    Status st = _iter->_get_row_ranges_from_conditions(&condition_row_ranges);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(20, condition_row_ranges.count());
    EXPECT_EQ(1, mock_iter->calls);
    EXPECT_EQ(2, _stats.zone_map_expr_evaluated);
    EXPECT_EQ(0, _stats.zone_map_expr_unsupported);
    EXPECT_EQ(2, _stats.zone_map_expr_type_mismatch);
    EXPECT_EQ(0, _stats.zone_map_expr_skipped_pages);
}

TEST_F(SegmentIteratorExprZoneMapTest, refresh_rebuilds_unread_ranges_for_late_runtime_filter) {
    auto* mock_iter = new MockZoneMapExprColumnIterator(
            {{make_zone_map(0, 4), RowRange(0, 5)}, {make_zone_map(7, 9), RowRange(5, 10)}});
    _iter->_column_iterators.resize(1);
    _iter->_column_iterators[0].reset(mock_iter);
    _iter->_row_bitmap.addRange(0, 10);
    roaring::Roaring unread_row_bitmap;
    unread_row_bitmap.addRange(3, 10);
    _iter->_reset_range_iter(unread_row_bitmap);
    _iter->_lazy_inited = true;
    _iter->_opts.condition_cache_digest = 123;
    _iter->_opts.late_arrival_rf_container = std::make_shared<LateArrivalRFContainer>();
    _iter->_opts.late_arrival_rf_container->zone_map_expr_ctxs = {
            make_binary_ctx(TExprOpcode::GT, 6)};
    _iter->_opts.late_arrival_rf_container->version = 1;

    Status st = _iter->refresh_for_late_arrival_runtime_filter();
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(0, _iter->_opts.condition_cache_digest);
    EXPECT_EQ(1, mock_iter->calls);
    EXPECT_EQ(5, _iter->_row_bitmap.cardinality());
    for (rowid_t rowid = 0; rowid < 10; ++rowid) {
        EXPECT_EQ(rowid >= 5, _iter->_row_bitmap.contains(rowid));
    }
}

TEST_F(SegmentIteratorExprZoneMapTest, refresh_before_lazy_init_only_caches_late_rf_exprs) {
    _iter->_opts.condition_cache_digest = 123;
    _iter->_opts.late_arrival_rf_container = std::make_shared<LateArrivalRFContainer>();
    _iter->_opts.late_arrival_rf_container->zone_map_expr_ctxs = {
            make_binary_ctx(TExprOpcode::GT, 6)};
    _iter->_opts.late_arrival_rf_container->version = 1;

    Status st = _iter->refresh_for_late_arrival_runtime_filter();
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(_iter->_lazy_inited);
    EXPECT_EQ(1, _iter->_zone_map_expr_ctxs.size());
    EXPECT_EQ(1, _iter->_zone_map_expr_cids.size());
    EXPECT_EQ(0, _iter->_opts.condition_cache_digest);
    EXPECT_EQ(1, _iter->_applied_late_arrival_rf_version);
}

TEST_F(SegmentIteratorExprZoneMapTest, refresh_after_all_unread_rows_consumed_skips_re_evaluation) {
    auto* mock_iter = new MockZoneMapExprColumnIterator(
            {{make_zone_map(0, 4), RowRange(0, 5)}, {make_zone_map(7, 9), RowRange(5, 10)}});
    _iter->_column_iterators.resize(1);
    _iter->_column_iterators[0].reset(mock_iter);
    _iter->_row_bitmap.addRange(0, 10);
    _iter->_reset_range_iter(roaring::Roaring());
    _iter->_lazy_inited = true;
    _iter->_opts.condition_cache_digest = 123;
    _iter->_opts.late_arrival_rf_container = std::make_shared<LateArrivalRFContainer>();
    _iter->_opts.late_arrival_rf_container->zone_map_expr_ctxs = {
            make_binary_ctx(TExprOpcode::GT, 6)};
    _iter->_opts.late_arrival_rf_container->version = 1;

    Status st = _iter->refresh_for_late_arrival_runtime_filter();
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(0, mock_iter->calls);
    EXPECT_TRUE(_iter->_row_bitmap.isEmpty());
    EXPECT_EQ(0, _iter->_opts.condition_cache_digest);
}

} // namespace doris::segment_v2
