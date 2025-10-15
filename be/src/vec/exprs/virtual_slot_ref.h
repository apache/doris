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

#include "vec/exprs/vexpr.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class VirtualSlotRef MOCK_REMOVE(final) : public VExpr {
    ENABLE_FACTORY_CREATOR(VirtualSlotRef);

public:
    VirtualSlotRef(const TExprNode& node);
    VirtualSlotRef(const SlotDescriptor* desc);

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    Status execute(VExprContext* context, Block* block, int* result_column_id) override;
    const std::string& expr_name() const override;
    std::string expr_label() override;
    std::string debug_string() const override;
    bool is_constant() const override { return false; }
    int column_id() const { return _column_id; }
    int slot_id() const { return _slot_id; }
    bool equals(const VExpr& other) override;
    size_t estimate_memory(const size_t rows) override { return 0; }
    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }
    std::shared_ptr<VExpr> get_virtual_column_expr() const { return _virtual_column_expr; }

    Status evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) override {
        return _virtual_column_expr->evaluate_inverted_index(context, segment_num_rows);
    }

    /*
    @brief SQL expression tree patterns for ANN range search optimization.
    
    Pattern 1 (should not happen):
    SELECT * FROM tbl WHERE distance_function(columnA, ArrayLiteral) > 100
    VirtualSlotRef
    |
    BINARY_PRED
    |---------------------------------------|
    |                                       |
    FUNCTION_CALL(l2_distance_approximate)  IntLiteral
    |
    |-----------------------|
    |                       |
    SlotRef                 ArrayLiteral

    Pattern 2 (optimizable case):
    SELECT distance_function(columnA, ArrayLiteral) AS dis FROM tbl WHERE dis > 100
    BINARY_PRED
    |
    |---------------------------------------|
    |                                       |
    VIRTUAL_SLOT_REF                        IntLiteral
    |
    FUNCTION_CALL(l2_distance_approximate)
    |
    |-----------------------|
    |                       |
    SlotRef                 ArrayLiteral
    */

    /**
     * @brief Evaluates ANN range search using index-based optimization.
     * 
     * This method implements the core logic for ANN range search optimization.
     * Instead of computing distances for all rows and then filtering, it uses
     * the ANN index to efficiently find only the rows within the specified range.
     * 
     * The method:
     * 1. Extracts query parameters from the range search runtime info
     * 2. Calls the ANN index to perform range search
     * 3. Updates the row bitmap with matching results
     * 4. Collects performance statistics
     * 
     * @param range_search_runtime Runtime info containing query vector, radius, and metrics
     * @param cid_to_index_iterators Vector of index iterators for each column
     * @param idx_to_cid Mapping from index position to column ID
     * @param column_iterators Vector of column iterators for data access
     * @param row_bitmap Output bitmap updated with matching row IDs
     * @param ann_index_stats Statistics collector for performance monitoring
     * @return Status indicating success or failure of the search operation
     */
    Status evaluate_ann_range_search(
            const segment_v2::AnnRangeSearchRuntime& range_search_runtime,
            const std::vector<std::unique_ptr<segment_v2::IndexIterator>>& cid_to_index_iterators,
            const std::vector<ColumnId>& idx_to_cid,
            const std::vector<std::unique_ptr<segment_v2::ColumnIterator>>& column_iterators,
            roaring::Roaring& row_bitmap, segment_v2::AnnIndexStats& ann_index_stats) override;

#ifdef BE_TEST
    // Test-only setter methods for unit testing
    void set_column_id(int column_id) { _column_id = column_id; }
    void set_column_name(const std::string* column_name) { _column_name = column_name; }
    void set_column_data_type(DataTypePtr column_data_type) {
        _column_data_type = std::move(column_data_type);
    }
    void set_virtual_column_expr(std::shared_ptr<VExpr> virtual_column_expr) {
        _virtual_column_expr = virtual_column_expr;
    }
#endif

private:
    int _column_id;                              ///< Column ID in the table schema
    int _slot_id;                                ///< Slot ID in the expression context
    const std::string* _column_name;             ///< Column name for debugging/logging
    const std::string _column_label;             ///< Column label for display purposes
    std::shared_ptr<VExpr> _virtual_column_expr; ///< Underlying virtual expression
    DataTypePtr _column_data_type;               ///< Data type of the column
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized