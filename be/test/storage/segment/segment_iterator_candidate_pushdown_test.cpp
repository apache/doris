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
#include <optional>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/data_type/data_type_number.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/virtual_slot_ref.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_iterator.h"
#include "storage/index/index_query_context.h"
#include "storage/olap_common.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/column_predicate.h"
#include "storage/segment/column_reader.h"
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
        if (_captured_candidate != nullptr) {
            _captured_candidate_copy = *_captured_candidate;
        }
        return Status::OK();
    }

    bool captured() const { return _captured; }
    const roaring::Roaring* captured_candidate() const { return _captured_candidate; }
    const std::optional<roaring::Roaring>& captured_candidate_copy() const {
        return _captured_candidate_copy;
    }

private:
    SegmentIterator* _iter;
    bool _captured = false;
    const roaring::Roaring* _captured_candidate = nullptr;
    std::optional<roaring::Roaring> _captured_candidate_copy;
};

// Produces a deterministic inverted-index result while modeling the contract
// of a candidate-consuming leaf: only its TRUE bitmap is candidate-restricted;
// its NULL bitmap remains segment-wide so compound SQL three-valued logic can
// distinguish FALSE from UNKNOWN.
class CandidateRestrictedBitmapExpr : public VExpr {
public:
    CandidateRestrictedBitmapExpr(SegmentIterator* iter, std::initializer_list<uint32_t> true_rows,
                                  std::initializer_list<uint32_t> null_rows)
            : _iter(iter) {
        _data_type = make_nullable(std::make_shared<DataTypeUInt8>());
        for (uint32_t row : true_rows) {
            _true_rows.add(row);
        }
        for (uint32_t row : null_rows) {
            _null_rows.add(row);
        }
    }

    const std::string& expr_name() const override {
        static const std::string kName = "CandidateRestrictedBitmapExpr";
        return kName;
    }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::NotSupported("bitmap-only test expression");
    }

    Status evaluate_inverted_index(VExprContext* context, uint32_t) override {
        _evaluated = true;
        auto data = std::make_shared<roaring::Roaring>(_true_rows);
        if (_iter->_index_query_context->candidate_rows != nullptr) {
            *data &= *_iter->_index_query_context->candidate_rows;
        }
        context->get_index_context()->set_index_result_for_expr(
                this, InvertedIndexResultBitmap(std::move(data),
                                                std::make_shared<roaring::Roaring>(_null_rows)));
        return Status::OK();
    }

    bool evaluated() const { return _evaluated; }

private:
    SegmentIterator* _iter;
    roaring::Roaring _true_rows;
    roaring::Roaring _null_rows;
    bool _evaluated = false;
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

class RangePruningColumnIterator : public ColumnIterator {
public:
    explicit RangePruningColumnIterator(RowRanges row_ranges)
            : _row_ranges(std::move(row_ranges)) {}

    Status seek_to_ordinal(ordinal_t ord) override { return Status::OK(); }
    ordinal_t get_current_ordinal() const override { return 0; }

    Status get_row_ranges_by_zone_map(
            const AndBlockColumnPredicate* col_predicates,
            const std::vector<std::shared_ptr<const ColumnPredicate>>* delete_predicates,
            RowRanges* row_ranges) override {
        *row_ranges = _row_ranges;
        return Status::OK();
    }

private:
    RowRanges _row_ranges;
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

TExprNode make_compound_node(TExprOpcode::type opcode, int num_children) {
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::COMPOUND_PRED);
    node.__set_opcode(opcode);
    node.__set_num_children(num_children);
    node.__set_is_nullable(true);
    return node;
}

VExprContextSPtr make_virtual_slot_ctx(const VExprSPtr& virtual_expr) {
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::VIRTUAL_SLOT_REF);
    node.__set_num_children(0);
    node.__set_is_nullable(true);
    node.__set_label("virtual_compound");
    TSlotRef slot_ref;
    slot_ref.__set_slot_id(-1);
    slot_ref.__set_tuple_id(-1);
    node.__set_slot_ref(slot_ref);

    auto root = VirtualSlotRef::create_shared(node);
    root->set_virtual_column_expr(virtual_expr);
    static const std::string kColumnName = "virtual_compound";
    root->set_column_name(&kColumnName);
    root->set_column_data_type(make_nullable(std::make_shared<DataTypeUInt8>()));

    auto ctx = std::make_shared<VExprContext>(root);
    std::vector<std::unique_ptr<IndexIterator>> index_iters;
    std::vector<IndexFieldNameAndTypePair> storage_types;
    std::unordered_map<ColumnId, std::unordered_map<const VExpr*, bool>> status_map;
    ColumnIteratorOptions column_iter_opts;
    ctx->set_index_context(std::make_shared<IndexExecContext>(
            index_iters, storage_types, status_map, nullptr, nullptr, column_iter_opts));
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

    void add_range_pruning_condition(rowid_t from, rowid_t to) {
        _iter->_column_iterators[0] =
                std::make_unique<RangePruningColumnIterator>(RowRanges::create_single(from, to));
        auto result = std::make_shared<roaring::Roaring>();
        auto predicate = std::make_shared<ShrinkingPredicate>(0, std::move(result));
        auto block_predicate = AndBlockColumnPredicate::create_shared();
        block_predicate->add_column_predicate(
                SingleColumnBlockPredicate::create_unique(std::move(predicate)));
        _iter->_opts.col_id_to_predicates.emplace(0, std::move(block_predicate));
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

TEST_F(SegmentIteratorCandidatePushdownTest, external_row_ranges_engage_candidate_before_expr) {
    config::inverted_index_candidate_pushdown_ratio = 0.3;
    _iter->_row_bitmap.addRange(0, 100); // full segment: no engage without the split
    _iter->_opts.row_ranges = RowRanges::create_single(0, 5);

    ASSERT_TRUE(_iter->_get_row_ranges_by_column_conditions().ok());

    ASSERT_TRUE(_expr->captured());
    ASSERT_TRUE(_expr->captured_candidate_copy().has_value());
    EXPECT_EQ(_expr->captured_candidate_copy()->cardinality(), 5);
    EXPECT_TRUE(_expr->captured_candidate_copy()->contains(0));
    EXPECT_FALSE(_expr->captured_candidate_copy()->contains(5));
    EXPECT_EQ(_iter->_index_query_context->candidate_rows, nullptr);
}

TEST_F(SegmentIteratorCandidatePushdownTest, delete_bitmap_engages_candidate_before_expr) {
    config::inverted_index_candidate_pushdown_ratio = 0.3;
    _iter->_row_bitmap.addRange(0, 100); // full segment: no engage without deletes
    auto deleted_rows = std::make_shared<roaring::Roaring>();
    deleted_rows->addRange(5, 100);
    _iter->_opts.delete_bitmap.emplace(_iter->segment_id(), std::move(deleted_rows));

    ASSERT_TRUE(_iter->_get_row_ranges_by_column_conditions().ok());

    ASSERT_TRUE(_expr->captured());
    ASSERT_TRUE(_expr->captured_candidate_copy().has_value());
    EXPECT_EQ(_expr->captured_candidate_copy()->cardinality(), 5);
    EXPECT_TRUE(_expr->captured_candidate_copy()->contains(4));
    EXPECT_FALSE(_expr->captured_candidate_copy()->contains(5));
    EXPECT_EQ(_iter->_index_query_context->candidate_rows, nullptr);
}

TEST_F(SegmentIteratorCandidatePushdownTest, condition_ranges_engage_candidate_before_expr) {
    config::inverted_index_candidate_pushdown_ratio = 0.3;
    _iter->_row_bitmap.addRange(0, 100); // full segment: no engage without range pruning
    add_range_pruning_condition(0, 5);

    ASSERT_TRUE(_iter->_get_row_ranges_by_column_conditions().ok());

    ASSERT_TRUE(_expr->captured());
    ASSERT_TRUE(_expr->captured_candidate_copy().has_value());
    EXPECT_EQ(_expr->captured_candidate_copy()->cardinality(), 5);
    EXPECT_TRUE(_expr->captured_candidate_copy()->contains(0));
    EXPECT_FALSE(_expr->captured_candidate_copy()->contains(5));
    EXPECT_EQ(_iter->_index_query_context->candidate_rows, nullptr);
}

// Three-valued compound shortcuts (VCompoundPred) treat an empty TRUE bitmap
// as a whole-segment fact; under a candidate restriction that inference is
// wrong for candidate rows (NOT(A AND B) with nullable A can turn FALSE into
// NULL and drop rows whose SQL result is TRUE). Compound roots must therefore
// be evaluated without the candidate -- in the conjunct loop and the
// virtual-column projection loop alike -- while simple roots keep it.
TEST_F(SegmentIteratorCandidatePushdownTest, compound_root_evaluates_without_candidate) {
    config::inverted_index_candidate_pushdown_ratio = 0.3;
    _iter->_row_bitmap.addRange(0, 5); // 5% of 100 rows: candidate engages

    auto compound_expr = std::make_shared<CapturingExpr>(_iter.get());
    compound_expr->set_node_type(TExprNodeType::COMPOUND_PRED);
    _iter->_common_expr_ctxs_push_down.push_back(make_capturing_ctx(compound_expr));

    auto vcol_compound_expr = std::make_shared<CapturingExpr>(_iter.get());
    vcol_compound_expr->set_node_type(TExprNodeType::COMPOUND_PRED);
    _iter->_virtual_column_exprs[0] = make_capturing_ctx(vcol_compound_expr);

    ASSERT_TRUE(_iter->_get_row_ranges_by_column_conditions().ok());

    ASSERT_TRUE(_expr->captured());
    EXPECT_EQ(_expr->captured_candidate(), &_iter->_row_bitmap)
            << "a simple root stays candidate-restricted";
    ASSERT_TRUE(compound_expr->captured());
    EXPECT_EQ(compound_expr->captured_candidate(), nullptr)
            << "a candidate-restricted TRUE bitmap must not feed three-valued "
               "compound shortcuts";
    ASSERT_TRUE(vcol_compound_expr->captured());
    EXPECT_EQ(vcol_compound_expr->captured_candidate(), nullptr)
            << "the virtual-column projection loop must suppress the candidate "
               "for compound roots too";
    EXPECT_EQ(_iter->_index_query_context->candidate_rows, nullptr);
}

TEST_F(SegmentIteratorCandidatePushdownTest,
       virtual_slot_wrapped_compound_preserves_three_valued_logic) {
    config::inverted_index_candidate_pushdown_ratio = 0.3;
    _iter->_row_bitmap.add(0); // 1% of 100 rows: candidate engages

    // At row 0, A is NULL and B is FALSE, so SQL requires
    // NOT(A AND B) = NOT(FALSE) = TRUE. A also has a TRUE row outside the
    // candidate so a candidate-restricted evaluation makes its TRUE bitmap
    // empty and exposes VCompoundPred's invalid early exit.
    auto nullable_a = std::make_shared<CandidateRestrictedBitmapExpr>(
            _iter.get(), std::initializer_list<uint32_t> {50}, std::initializer_list<uint32_t> {0});
    auto false_b = std::make_shared<CandidateRestrictedBitmapExpr>(
            _iter.get(), std::initializer_list<uint32_t> {}, std::initializer_list<uint32_t> {});
    auto and_expr = VCompoundPred::create_shared(make_compound_node(TExprOpcode::COMPOUND_AND, 2));
    and_expr->add_child(nullable_a);
    and_expr->add_child(false_b);
    auto not_expr = VCompoundPred::create_shared(make_compound_node(TExprOpcode::COMPOUND_NOT, 1));
    not_expr->add_child(and_expr);
    auto ctx = make_virtual_slot_ctx(not_expr);
    _iter->_common_expr_ctxs_push_down = {ctx};

    ASSERT_TRUE(_iter->_get_row_ranges_by_column_conditions().ok());

    const auto* result = ctx->get_index_context()->get_index_result_for_expr(not_expr.get());
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(false_b->evaluated()) << "AND must evaluate FALSE B after nullable A";
    EXPECT_TRUE(result->get_data_bitmap()->contains(0))
            << "NOT(NULL AND FALSE) must preserve candidate row 0";
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
