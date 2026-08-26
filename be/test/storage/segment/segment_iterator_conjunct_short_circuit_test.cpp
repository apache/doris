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

// White-box tests for progressive intersection and short-circuit of pushed-down
// conjuncts in SegmentIterator::_apply_index_expr. Once an earlier conjunct's
// index result empties _row_bitmap, the remaining (potentially expensive, e.g.
// MATCH_PHRASE_PREFIX) conjuncts must not be evaluated against the index at
// all; skipped conjuncts stay pushed down so the row-level path keeps their
// semantics over the (now empty) candidate set. Uses the established
// `#define private public` convention of segment_iterator_limit_opt_test.cpp.
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type_number.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_state.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"
#undef private
#undef protected
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris::segment_v2 {

namespace {

// A test VExpr that counts evaluations and registers a fixed-cardinality index
// result bitmap, mimicking how real match exprs publish results.
class BitmapEvalExpr : public VExpr {
public:
    explicit BitmapEvalExpr(std::vector<uint32_t> rows) : _rows(std::move(rows)) {
        _data_type = std::make_shared<DataTypeUInt8>();
    }

    const std::string& expr_name() const override {
        static const std::string kName = "BitmapEvalExpr";
        return kName;
    }

    Status execute(VExprContext*, Block*, int*) const override { return Status::OK(); }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        return Status::OK();
    }

    Status evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) override {
        ++_eval_count;
        auto data = std::make_shared<roaring::Roaring>();
        for (uint32_t row : _rows) {
            data->add(row);
        }
        InvertedIndexResultBitmap result(std::move(data), std::make_shared<roaring::Roaring>());
        context->get_index_context()->set_index_result_for_expr(this, std::move(result));
        return Status::OK();
    }

    int eval_count() const { return _eval_count; }

private:
    std::vector<uint32_t> _rows;
    int _eval_count = 0;
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

// Minimal Segment stub: _apply_index_expr only consults num_rows().
std::shared_ptr<Segment> make_stub_segment(uint32_t num_rows,
                                           const TabletSchemaSPtr& tablet_schema) {
    auto seg = std::make_shared<Segment>(0, RowsetId(), tablet_schema, InvertedIndexFileInfo());
    seg->_num_rows = num_rows;
    return seg;
}

// VExprContext with a BitmapEvalExpr root and an index context to publish into.
VExprContextSPtr make_bitmap_ctx(const std::shared_ptr<BitmapEvalExpr>& expr) {
    auto ctx = std::make_shared<VExprContext>(expr);
    std::vector<std::unique_ptr<IndexIterator>> index_iters;
    std::vector<IndexFieldNameAndTypePair> storage_types;
    std::unordered_map<ColumnId, std::unordered_map<const VExpr*, bool>> status_map;
    ColumnIteratorOptions column_iter_opts;
    auto index_ctx = std::make_shared<IndexExecContext>(index_iters, storage_types, status_map,
                                                        nullptr, nullptr, column_iter_opts);
    ctx->set_index_context(index_ctx);
    return ctx;
}

} // namespace

class SegmentIteratorConjunctShortCircuitTest : public testing::Test {
protected:
    void SetUp() override {
        _tablet_schema = make_tablet_schema();
        _segment = make_stub_segment(100, _tablet_schema);
        _read_schema = std::make_shared<ReadSchema>(_tablet_schema->columns());
        _iter = std::make_unique<SegmentIterator>(_segment, _read_schema);

        TQueryOptions query_options;
        query_options.__set_enable_fallback_on_missing_inverted_index(true);
        _runtime_state.set_query_options(query_options);

        _iter->_opts.runtime_state = &_runtime_state;
        _iter->_opts.stats = &_stats;
    }

    std::shared_ptr<Segment> _segment;
    std::shared_ptr<TabletSchema> _tablet_schema;
    ReadSchemaSPtr _read_schema;
    std::unique_ptr<SegmentIterator> _iter;
    RuntimeState _runtime_state;
    OlapReaderStatistics _stats;
};

// Once an earlier conjunct's index result empties the row bitmap, remaining
// conjuncts must not be evaluated against the index. With zero surviving
// rows every remaining conjunct is trivially satisfied, so the whole
// pushed-down list is consumed -- mirroring the column-predicate path and
// keeping both the "all conditions consumed" contract (debug point
// segment_iterator.apply_inverted_index) and the condition-cache digest
// intact.
TEST_F(SegmentIteratorConjunctShortCircuitTest, empty_bitmap_short_circuits_remaining) {
    _iter->_row_bitmap.addRange(0, 100);

    auto empty_expr = std::make_shared<BitmapEvalExpr>(std::vector<uint32_t> {});
    auto expensive_expr = std::make_shared<BitmapEvalExpr>(std::vector<uint32_t> {0, 1, 2, 3, 4});
    auto empty_ctx = make_bitmap_ctx(empty_expr);
    auto expensive_ctx = make_bitmap_ctx(expensive_expr);
    _iter->_common_expr_ctxs_push_down = {empty_ctx, expensive_ctx};

    ASSERT_TRUE(_iter->_apply_index_expr().ok());

    // The empty result is intersected as soon as it is produced ...
    EXPECT_TRUE(_iter->_row_bitmap.isEmpty());
    // ... so the expensive conjunct is never evaluated against the index ...
    EXPECT_EQ(empty_expr->eval_count(), 1);
    EXPECT_EQ(expensive_expr->eval_count(), 0);
    // ... and the whole list is consumed: zero surviving rows satisfy every
    // remaining conjunct, exactly like the column-predicate short circuit.
    EXPECT_TRUE(_iter->_common_expr_ctxs_push_down.empty());
    // The skip is visible in reader statistics (profile:
    // InvertedIndexConjunctsShortCircuited).
    EXPECT_EQ(_stats.inverted_index_conjuncts_short_circuited, 1);
}

// A conjunct whose index result does not empty the bitmap must not stop
// evaluation of the following conjuncts; consumed results are intersected
// progressively.
TEST_F(SegmentIteratorConjunctShortCircuitTest, progressive_intersection_keeps_going) {
    _iter->_row_bitmap.addRange(0, 100);

    auto first_expr = std::make_shared<BitmapEvalExpr>(std::vector<uint32_t> {1, 2, 3, 50, 60});
    auto second_expr = std::make_shared<BitmapEvalExpr>(std::vector<uint32_t> {2, 3, 4});
    auto first_ctx = make_bitmap_ctx(first_expr);
    auto second_ctx = make_bitmap_ctx(second_expr);
    _iter->_common_expr_ctxs_push_down = {first_ctx, second_ctx};

    ASSERT_TRUE(_iter->_apply_index_expr().ok());

    EXPECT_EQ(first_expr->eval_count(), 1);
    EXPECT_EQ(second_expr->eval_count(), 1);
    // {1,2,3,50,60} intersected with {2,3,4} is {2,3}.
    EXPECT_EQ(_iter->_row_bitmap.cardinality(), 2);
    EXPECT_TRUE(_iter->_row_bitmap.contains(2));
    EXPECT_TRUE(_iter->_row_bitmap.contains(3));
    // Both conjuncts were fully consumed by the index; nothing was skipped.
    EXPECT_TRUE(_iter->_common_expr_ctxs_push_down.empty());
    EXPECT_EQ(_stats.inverted_index_conjuncts_short_circuited, 0);
}

} // namespace doris::segment_v2
