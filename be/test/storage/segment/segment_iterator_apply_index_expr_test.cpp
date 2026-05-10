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
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_state.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"
#include "storage/tablet/tablet_schema.h"

// Use #define private public to access private members for testing
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

// A test VExpr that returns a configurable Status from evaluate_inverted_index.
class MockEvalExpr : public VExpr {
public:
    MockEvalExpr() { _data_type = std::make_shared<DataTypeUInt8>(); }

    void set_evaluate_status(Status st) { _eval_status = std::move(st); }

    const std::string& expr_name() const override {
        static const std::string kName = "MockEvalExpr";
        return kName;
    }

    Status execute(VExprContext*, Block*, int*) const override { return Status::OK(); }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        return Status::OK();
    }

    Status evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) override {
        return _eval_status;
    }

private:
    Status _eval_status = Status::OK();
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

// Helper to create a minimal Segment with a given num_rows.
std::shared_ptr<Segment> make_stub_segment(uint32_t num_rows,
                                           const TabletSchemaSPtr& tablet_schema) {
    auto seg = std::make_shared<Segment>(0, RowsetId(), tablet_schema, InvertedIndexFileInfo());
    seg->_num_rows = num_rows;
    return seg;
}

// Helper to create a VExprContext with a MockEvalExpr root.
VExprContextSPtr make_mock_ctx(Status eval_status, bool with_index_context = true) {
    auto expr = std::make_shared<MockEvalExpr>();
    expr->set_evaluate_status(std::move(eval_status));
    auto ctx = std::make_shared<VExprContext>(expr);
    if (with_index_context) {
        std::vector<ColumnId> col_ids;
        std::vector<std::unique_ptr<IndexIterator>> index_iters;
        std::vector<IndexFieldNameAndTypePair> storage_types;
        std::unordered_map<ColumnId, std::unordered_map<const VExpr*, bool>> status_map;
        ColumnIteratorOptions column_iter_opts;
        auto index_ctx =
                std::make_shared<IndexExecContext>(col_ids, index_iters, storage_types, status_map,
                                                   nullptr, nullptr, column_iter_opts);
        ctx->set_index_context(index_ctx);
    }
    return ctx;
}

} // namespace

class SegmentIteratorApplyIndexExprTest : public testing::Test {
protected:
    void SetUp() override {
        _tablet_schema = make_tablet_schema();
        _segment = make_stub_segment(100, _tablet_schema);

        _read_schema = std::make_shared<Schema>(_tablet_schema);
        _iter = std::make_unique<SegmentIterator>(_segment, _read_schema);

        // Set up RuntimeState with fallback enabled so _downgrade_without_index works
        TQueryOptions query_options;
        query_options.__set_enable_fallback_on_missing_inverted_index(true);
        _runtime_state.set_query_options(query_options);

        _iter->_opts.runtime_state = &_runtime_state;
        _iter->_opts.stats = &_stats;
    }

    std::shared_ptr<Segment> _segment;
    std::shared_ptr<TabletSchema> _tablet_schema;
    SchemaSPtr _read_schema;
    std::unique_ptr<SegmentIterator> _iter;
    RuntimeState _runtime_state;
    OlapReaderStatistics _stats;
};

// When evaluate_inverted_index returns OK, _apply_index_expr should succeed.
TEST_F(SegmentIteratorApplyIndexExprTest, virtual_column_evaluate_ok) {
    _iter->_virtual_column_exprs[0] = make_mock_ctx(Status::OK());
    EXPECT_TRUE(_iter->_apply_index_expr().ok());
}

// When the index context is null, the expr should be skipped (continue).
TEST_F(SegmentIteratorApplyIndexExprTest, virtual_column_null_index_context_skipped) {
    _iter->_virtual_column_exprs[0] = make_mock_ctx(Status::OK(), /*with_index_context=*/false);
    EXPECT_TRUE(_iter->_apply_index_expr().ok());
}

// When evaluate_inverted_index returns INVERTED_INDEX_BYPASS (a downgrade error),
// _apply_index_expr should continue and return OK.
TEST_F(SegmentIteratorApplyIndexExprTest, virtual_column_downgrade_bypass_continues) {
    _iter->_virtual_column_exprs[0] =
            make_mock_ctx(Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>("bypass"));
    Status st = _iter->_apply_index_expr();
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(_stats.inverted_index_downgrade_count, 1);
}

// When evaluate_inverted_index returns INVERTED_INDEX_FILE_NOT_FOUND with fallback enabled,
// _apply_index_expr should downgrade and continue.
TEST_F(SegmentIteratorApplyIndexExprTest, virtual_column_downgrade_file_not_found_continues) {
    _iter->_virtual_column_exprs[0] =
            make_mock_ctx(Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>("not found"));
    Status st = _iter->_apply_index_expr();
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(_stats.inverted_index_downgrade_count, 1);
}

// When evaluate_inverted_index returns INVERTED_INDEX_EVALUATE_SKIPPED,
// _apply_index_expr should downgrade and continue.
TEST_F(SegmentIteratorApplyIndexExprTest, virtual_column_downgrade_evaluate_skipped_continues) {
    _iter->_virtual_column_exprs[0] =
            make_mock_ctx(Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>("skipped"));
    Status st = _iter->_apply_index_expr();
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(_stats.inverted_index_downgrade_count, 1);
}

// When evaluate_inverted_index returns INVERTED_INDEX_FILE_CORRUPTED,
// _apply_index_expr should downgrade and continue.
TEST_F(SegmentIteratorApplyIndexExprTest, virtual_column_downgrade_file_corrupted_continues) {
    _iter->_virtual_column_exprs[0] =
            make_mock_ctx(Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>("corrupted"));
    Status st = _iter->_apply_index_expr();
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(_stats.inverted_index_downgrade_count, 1);
}

// When evaluate_inverted_index returns NOT_IMPLEMENTED_ERROR,
// _apply_index_expr should continue and return OK.
TEST_F(SegmentIteratorApplyIndexExprTest, virtual_column_not_implemented_continues) {
    _iter->_virtual_column_exprs[0] =
            make_mock_ctx(Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>("not impl"));
    Status st = _iter->_apply_index_expr();
    EXPECT_TRUE(st.ok()) << st.to_string();
    // NOT_IMPLEMENTED_ERROR does not go through _downgrade_without_index, so count stays 0
    EXPECT_EQ(_stats.inverted_index_downgrade_count, 0);
}

// When evaluate_inverted_index returns an unhandled error (e.g., INTERNAL_ERROR),
// _apply_index_expr should propagate the error.
TEST_F(SegmentIteratorApplyIndexExprTest, virtual_column_unhandled_error_propagated) {
    _iter->_virtual_column_exprs[0] =
            make_mock_ctx(Status::Error<ErrorCode::INTERNAL_ERROR>("internal error"));
    Status st = _iter->_apply_index_expr();
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), ErrorCode::INTERNAL_ERROR);
}

// Multiple virtual column exprs: one OK and one downgrade error should both continue.
TEST_F(SegmentIteratorApplyIndexExprTest, multiple_virtual_columns_mixed_results) {
    _iter->_virtual_column_exprs[0] = make_mock_ctx(Status::OK());
    _iter->_virtual_column_exprs[1] =
            make_mock_ctx(Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>("bypass"));
    _iter->_virtual_column_exprs[2] = make_mock_ctx(Status::OK());
    Status st = _iter->_apply_index_expr();
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(_stats.inverted_index_downgrade_count, 1);
}

// Multiple virtual column exprs: second one returns unhandled error, should stop and propagate.
TEST_F(SegmentIteratorApplyIndexExprTest, multiple_virtual_columns_error_stops_iteration) {
    _iter->_virtual_column_exprs[0] = make_mock_ctx(Status::OK());
    _iter->_virtual_column_exprs[1] =
            make_mock_ctx(Status::Error<ErrorCode::INTERNAL_ERROR>("fail"));
    _iter->_virtual_column_exprs[2] = make_mock_ctx(Status::OK());
    Status st = _iter->_apply_index_expr();
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), ErrorCode::INTERNAL_ERROR);
}

// Empty virtual_column_exprs should just succeed.
TEST_F(SegmentIteratorApplyIndexExprTest, empty_virtual_columns_ok) {
    EXPECT_TRUE(_iter->_apply_index_expr().ok());
}

} // namespace doris::segment_v2
