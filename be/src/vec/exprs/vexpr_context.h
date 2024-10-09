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

#include <glog/logging.h>

#include <memory>
#include <utility>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "runtime/types.h"
#include "udf/udf.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris::vectorized {

class InvertedIndexContext {
public:
    InvertedIndexContext(
            const std::vector<ColumnId>& col_ids,
            const std::vector<std::unique_ptr<segment_v2::InvertedIndexIterator>>&
                    inverted_index_iterators,
            const std::vector<vectorized::IndexFieldNameAndTypePair>& storage_name_and_type_vec,
            std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>>&
                    common_expr_inverted_index_status)
            : _col_ids(col_ids),
              _inverted_index_iterators(inverted_index_iterators),
              _storage_name_and_type(storage_name_and_type_vec),
              _expr_inverted_index_status(common_expr_inverted_index_status) {}

    segment_v2::InvertedIndexIterator* get_inverted_index_iterator_by_column_id(
            int column_index) const {
        if (column_index < 0 || column_index >= _col_ids.size()) {
            return nullptr;
        }
        const auto& column_id = _col_ids[column_index];
        if (column_id >= _inverted_index_iterators.size()) {
            return nullptr;
        }
        if (!_inverted_index_iterators[column_id]) {
            return nullptr;
        }
        return _inverted_index_iterators[column_id].get();
    }

    const vectorized::IndexFieldNameAndTypePair* get_storage_name_and_type_by_column_id(
            int column_index) const {
        if (column_index < 0 || column_index >= _col_ids.size()) {
            return nullptr;
        }
        const auto& column_id = _col_ids[column_index];
        if (column_id >= _storage_name_and_type.size()) {
            return nullptr;
        }
        return &_storage_name_and_type[column_id];
    }

    bool has_inverted_index_result_for_expr(const vectorized::VExpr* expr) const {
        return _inverted_index_result_bitmap.contains(expr);
    }

    void set_inverted_index_result_for_expr(const vectorized::VExpr* expr,
                                            segment_v2::InvertedIndexResultBitmap bitmap) {
        _inverted_index_result_bitmap[expr] = std::move(bitmap);
    }

    std::unordered_map<const vectorized::VExpr*, segment_v2::InvertedIndexResultBitmap>&
    get_inverted_index_result_bitmap() {
        return _inverted_index_result_bitmap;
    }

    std::unordered_map<const vectorized::VExpr*, ColumnPtr>& get_inverted_index_result_column() {
        return _inverted_index_result_column;
    }

    const segment_v2::InvertedIndexResultBitmap* get_inverted_index_result_for_expr(
            const vectorized::VExpr* expr) {
        auto iter = _inverted_index_result_bitmap.find(expr);
        if (iter == _inverted_index_result_bitmap.end()) {
            return nullptr;
        }
        return &iter->second;
    }

    void set_inverted_index_result_column_for_expr(const vectorized::VExpr* expr,
                                                   ColumnPtr column) {
        _inverted_index_result_column[expr] = std::move(column);
    }

    void set_true_for_inverted_index_status(const vectorized::VExpr* expr, int column_index) {
        if (column_index < 0 || column_index >= _col_ids.size()) {
            return;
        }
        const auto& column_id = _col_ids[column_index];
        if (_expr_inverted_index_status.contains(column_id)) {
            if (_expr_inverted_index_status[column_id].contains(expr)) {
                _expr_inverted_index_status[column_id][expr] = true;
            }
        }
    }

private:
    // A reference to a vector of column IDs for the current expression's output columns.
    const std::vector<ColumnId>& _col_ids;

    // A reference to a vector of unique pointers to inverted index iterators.
    const std::vector<std::unique_ptr<segment_v2::InvertedIndexIterator>>&
            _inverted_index_iterators;

    // A reference to a vector of storage name and type pairs related to schema.
    const std::vector<vectorized::IndexFieldNameAndTypePair>& _storage_name_and_type;

    // A map of expressions to their corresponding inverted index result bitmaps.
    std::unordered_map<const vectorized::VExpr*, segment_v2::InvertedIndexResultBitmap>
            _inverted_index_result_bitmap;

    // A map of expressions to their corresponding result columns.
    std::unordered_map<const vectorized::VExpr*, ColumnPtr> _inverted_index_result_column;

    // A reference to a map of common expressions to their inverted index evaluation status.
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>>&
            _expr_inverted_index_status;
};

class VExprContext {
    ENABLE_FACTORY_CREATOR(VExprContext);

public:
    VExprContext(VExprSPtr expr) : _root(std::move(expr)) {}
    ~VExprContext();
    [[nodiscard]] Status prepare(RuntimeState* state, const RowDescriptor& row_desc);
    [[nodiscard]] Status open(RuntimeState* state);
    [[nodiscard]] Status clone(RuntimeState* state, VExprContextSPtr& new_ctx);
    [[nodiscard]] Status execute(Block* block, int* result_column_id);

    VExprSPtr root() { return _root; }
    void set_root(const VExprSPtr& expr) { _root = expr; }
    void set_inverted_index_context(std::shared_ptr<InvertedIndexContext> inverted_index_context) {
        _inverted_index_context = std::move(inverted_index_context);
    }

    std::shared_ptr<InvertedIndexContext> get_inverted_index_context() const {
        return _inverted_index_context;
    }

    /// Creates a FunctionContext, and returns the index that's passed to fn_context() to
    /// retrieve the created context. Exprs that need a FunctionContext should call this in
    /// Prepare() and save the returned index. 'varargs_buffer_size', if specified, is the
    /// size of the varargs buffer in the created FunctionContext (see udf-internal.h).
    int register_function_context(RuntimeState* state, const doris::TypeDescriptor& return_type,
                                  const std::vector<doris::TypeDescriptor>& arg_types);

    /// Retrieves a registered FunctionContext. 'i' is the index returned by the call to
    /// register_function_context(). This should only be called by VExprs.
    FunctionContext* fn_context(int i) {
        if (i < 0 || i >= _fn_contexts.size()) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "fn_context index invalid, index={}, _fn_contexts.size()={}", i,
                            _fn_contexts.size());
        }
        return _fn_contexts[i].get();
    }

    // execute expr with inverted index which column a, b has inverted indexes
    //  but some situation although column b has indexes, but apply index is not useful, we should
    //  skip this expr, just do not apply index anymore.

    [[nodiscard]] Status evaluate_inverted_index(uint32_t segment_num_rows);

    bool all_expr_inverted_index_evaluated();

    [[nodiscard]] static Status filter_block(VExprContext* vexpr_ctx, Block* block,
                                             int column_to_keep);

    [[nodiscard]] static Status filter_block(const VExprContextSPtrs& expr_contexts, Block* block,
                                             int column_to_keep);

    [[nodiscard]] static Status execute_conjuncts(const VExprContextSPtrs& ctxs,
                                                  const std::vector<IColumn::Filter*>* filters,
                                                  bool accept_null, Block* block,
                                                  IColumn::Filter* result_filter,
                                                  bool* can_filter_all);

    [[nodiscard]] static Status execute_conjuncts(const VExprContextSPtrs& conjuncts, Block* block,
                                                  ColumnUInt8& null_map,
                                                  IColumn::Filter& result_filter);

    static Status execute_conjuncts(const VExprContextSPtrs& ctxs,
                                    const std::vector<IColumn::Filter*>* filters, Block* block,
                                    IColumn::Filter* result_filter, bool* can_filter_all);

    [[nodiscard]] static Status execute_conjuncts_and_filter_block(
            const VExprContextSPtrs& ctxs, Block* block, std::vector<uint32_t>& columns_to_filter,
            int column_to_keep);

    static Status execute_conjuncts_and_filter_block(const VExprContextSPtrs& ctxs, Block* block,
                                                     std::vector<uint32_t>& columns_to_filter,
                                                     int column_to_keep, IColumn::Filter& filter);

    [[nodiscard]] static Status get_output_block_after_execute_exprs(const VExprContextSPtrs&,
                                                                     const Block&, Block*,
                                                                     bool do_projection = false);

    int get_last_result_column_id() const {
        DCHECK(_last_result_column_id != -1);
        return _last_result_column_id;
    }

    FunctionContext::FunctionStateScope get_function_state_scope() const {
        return _is_clone ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
    }

    void clone_fn_contexts(VExprContext* other);

    bool force_materialize_slot() const { return _force_materialize_slot; }

    void set_force_materialize_slot() { _force_materialize_slot = true; }

    VExprContext& operator=(const VExprContext& other) {
        if (this == &other) {
            return *this;
        }

        _root = other._root;
        _is_clone = other._is_clone;
        _prepared = other._prepared;
        _opened = other._opened;

        for (const auto& fn : other._fn_contexts) {
            _fn_contexts.emplace_back(fn->clone());
        }

        _last_result_column_id = other._last_result_column_id;
        _depth_num = other._depth_num;
        return *this;
    }

    VExprContext& operator=(VExprContext&& other) {
        _root = other._root;
        other._root = nullptr;
        _is_clone = other._is_clone;
        _prepared = other._prepared;
        _opened = other._opened;
        _fn_contexts = std::move(other._fn_contexts);
        _last_result_column_id = other._last_result_column_id;
        _depth_num = other._depth_num;
        return *this;
    }

private:
    // Close method is called in vexpr context dector, not need call expicility
    void close();

    friend class VExpr;

    /// The expr tree this context is for.
    VExprSPtr _root;

    /// True if this context came from a Clone() call. Used to manage FunctionStateScope.
    bool _is_clone = false;

    /// Variables keeping track of current state.
    bool _prepared = false;
    bool _opened = false;

    /// FunctionContexts for each registered expression. The FunctionContexts are created
    /// and owned by this VExprContext.
    std::vector<std::unique_ptr<FunctionContext>> _fn_contexts;

    int _last_result_column_id = -1;

    /// The depth of expression-tree.
    int _depth_num = 0;

    // This flag only works on VSlotRef.
    // Force to materialize even if the slot need_materialize is false, we just ignore need_materialize flag
    bool _force_materialize_slot = false;

    std::shared_ptr<InvertedIndexContext> _inverted_index_context;
};
} // namespace doris::vectorized
