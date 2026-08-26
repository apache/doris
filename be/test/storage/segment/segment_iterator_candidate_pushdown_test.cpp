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

// White-box tests for the candidate-pushdown handshake in
// SegmentIterator::_get_row_ranges_by_column_conditions: the engage decision
// must be refreshed after earlier index conjuncts shrink the row bitmap (a
// 50% entry bitmap cut to 5% by an indexed predicate must publish
// candidate_rows for the later expression conjuncts), must reject non-finite
// config values outright, and must always reset on exit. Uses the established
// `#define private public` convention of segment_iterator_limit_opt_test.cpp.
#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/data_type/data_type_number.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_iterator.h"
#include "storage/index/index_query_context.h"
#include "storage/olap_common.h"
#include "storage/predicate/column_predicate.h"
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

// Records the candidate_rows pointer the SegmentIterator exposes at the moment
// expression conjuncts are index-evaluated.
class CapturingExpr : public VExpr {
public:
    explicit CapturingExpr(SegmentIterator* iter) : _iter(iter) {
        _data_type = std::make_shared<DataTypeUInt8>();
    }

    const std::string& expr_name() const override {
        static const std::string kName = "CapturingExpr";
        return kName;
    }

    Status execute(VExprContext*, Block*, int*) const override { return Status::OK(); }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        return Status::OK();
    }

    Status evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) override {
        _captured = true;
        _captured_candidate = _iter->_index_query_context != nullptr
                                      ? _iter->_index_query_context->candidate_rows
                                      : nullptr;
        return Status::OK();
    }

    bool captured() const { return _captured; }
    const roaring::Roaring* captured_candidate() const { return _captured_candidate; }

private:
    SegmentIterator* _iter;
    bool _captured = false;
    const roaring::Roaring* _captured_candidate = nullptr;
};

// An indexed predicate stub that shrinks the row bitmap to a fixed set,
// standing in for a selective indexed equality applied before the expression
// conjuncts (modeled on MockNestedPredicate of accept_null_predicate_test).
class ShrinkingPredicate : public ColumnPredicate {
public:
    ShrinkingPredicate(uint32_t column_id, std::shared_ptr<roaring::Roaring> result_bitmap)
            : ColumnPredicate(column_id, "mock_col", PrimitiveType::TYPE_INT, false),
              _result_bitmap(std::move(result_bitmap)) {}

    PredicateType type() const override { return PredicateType::EQ; }

    Status evaluate(const IndexFieldNameAndTypePair& name_with_type, IndexIterator* iterator,
                    uint32_t num_rows, roaring::Roaring* bitmap) const override {
        *bitmap = *_result_bitmap;
        return Status::OK();
    }

    std::shared_ptr<ColumnPredicate> clone(uint32_t col_id) const override {
        return std::make_shared<ShrinkingPredicate>(col_id, _result_bitmap);
    }

private:
    uint16_t _evaluate_inner(const IColumn& column, uint16_t* sel, uint16_t size) const override {
        return size;
    }

    std::shared_ptr<roaring::Roaring> _result_bitmap;
};

// Minimal IndexIterator so has_index_in_iterators()/_check_apply_by_inverted_index pass.
class StubIndexIterator : public IndexIterator {
public:
    IndexReaderPtr get_reader(IndexReaderType) const override { return nullptr; }
    Status read_from_index(const IndexParam&) override { return Status::OK(); }
    Status read_null_bitmap(InvertedIndexQueryCacheHandle*) override { return Status::OK(); }
    Result<bool> has_null() override { return false; }
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

VExprContextSPtr make_capturing_ctx(const std::shared_ptr<CapturingExpr>& expr) {
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

class SegmentIteratorCandidatePushdownTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_ratio = config::inverted_index_candidate_pushdown_ratio;
        _tablet_schema = make_tablet_schema();
        _segment = make_stub_segment(100, _tablet_schema);
        _read_schema = std::make_shared<ReadSchema>(_tablet_schema->columns());
        _iter = std::make_unique<SegmentIterator>(_segment, _read_schema);

        TQueryOptions query_options;
        query_options.__set_enable_inverted_index_query(true);
        query_options.__set_enable_fallback_on_missing_inverted_index(true);
        _runtime_state.set_query_options(query_options);

        _iter->_opts.runtime_state = &_runtime_state;
        _iter->_opts.stats = &_stats;
        _iter->_opts.tablet_schema = _tablet_schema;
        _iter->_index_query_context = std::make_shared<IndexQueryContext>();
        _iter->_index_query_context->stats = &_stats;
        _iter->_column_states.resize(_read_schema->num_read_columns());
        _iter->_storage_name_and_type.resize(_read_schema->num_read_columns());

        _expr = std::make_shared<CapturingExpr>(_iter.get());
        _iter->_common_expr_ctxs_push_down = {make_capturing_ctx(_expr)};
    }

    void TearDown() override { config::inverted_index_candidate_pushdown_ratio = _saved_ratio; }

    void add_shrinking_predicate(std::initializer_list<uint32_t> rows) {
        auto result = std::make_shared<roaring::Roaring>();
        for (uint32_t row : rows) {
            result->add(row);
        }
        _iter->_index_iterators.resize(1);
        _iter->_index_iterators[0] = std::make_unique<StubIndexIterator>();
        _iter->_col_predicates.emplace_back(std::make_shared<ShrinkingPredicate>(0, result));
    }

    double _saved_ratio = 0;
    std::shared_ptr<Segment> _segment;
    std::shared_ptr<TabletSchema> _tablet_schema;
    ReadSchemaSPtr _read_schema;
    std::unique_ptr<SegmentIterator> _iter;
    RuntimeState _runtime_state;
    OlapReaderStatistics _stats;
    std::shared_ptr<CapturingExpr> _expr;
};

// Entry bitmap below the threshold: candidate_rows is published for the
// expression conjuncts and reset on exit.
TEST_F(SegmentIteratorCandidatePushdownTest, engages_below_threshold_and_resets) {
    config::inverted_index_candidate_pushdown_ratio = 0.3;
    _iter->_row_bitmap.addRange(0, 5); // 5% of 100 rows

    ASSERT_TRUE(_iter->_get_row_ranges_by_column_conditions().ok());

    ASSERT_TRUE(_expr->captured());
    EXPECT_EQ(_expr->captured_candidate(), &_iter->_row_bitmap);
    EXPECT_EQ(_iter->_index_query_context->candidate_rows, nullptr);
}

// CIR-style threshold crossing: the entry bitmap (50%) is above the threshold,
// then an indexed predicate shrinks it to 5%. The handshake must be refreshed
// at that conjunct boundary so the later expression conjuncts still get the
// candidate restriction.
TEST_F(SegmentIteratorCandidatePushdownTest, refreshes_after_index_conjuncts_shrink_bitmap) {
    config::inverted_index_candidate_pushdown_ratio = 0.3;
    _iter->_row_bitmap.addRange(0, 50); // 50% of 100 rows: no engage at entry
    add_shrinking_predicate({0, 1, 2, 3, 4});

    ASSERT_TRUE(_iter->_get_row_ranges_by_column_conditions().ok());

    ASSERT_TRUE(_expr->captured());
    EXPECT_EQ(_expr->captured_candidate(), &_iter->_row_bitmap);
    EXPECT_EQ(_iter->_index_query_context->candidate_rows, nullptr);
}

// A non-finite configured ratio must never engage the pushdown (the multiply
// and integer conversion would otherwise be undefined behavior).
TEST_F(SegmentIteratorCandidatePushdownTest, non_finite_ratio_never_engages) {
    config::inverted_index_candidate_pushdown_ratio = std::numeric_limits<double>::infinity();
    _iter->_row_bitmap.add(0); // 1 row, far below any finite threshold

    ASSERT_TRUE(_iter->_get_row_ranges_by_column_conditions().ok());

    ASSERT_TRUE(_expr->captured());
    EXPECT_EQ(_expr->captured_candidate(), nullptr);
    EXPECT_EQ(_iter->_index_query_context->candidate_rows, nullptr);
}

} // namespace doris::segment_v2
