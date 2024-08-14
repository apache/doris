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
    void set_inverted_index_iterators(
            const std::unordered_map<std::string, segment_v2::InvertedIndexIterator*>& iterators) {
        _inverted_index_iterators_by_col_name = iterators;
    }

    void set_storage_name_and_type(
            const std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>&
                    storage_name_and_type) {
        _storage_name_and_type_by_col_name = storage_name_and_type;
    }

    segment_v2::InvertedIndexIterator* get_inverted_index_iterators_by_column_name(
            std::string column_name) {
        return _inverted_index_iterators_by_col_name[column_name];
    }

    vectorized::IndexFieldNameAndTypePair get_storage_name_and_type_by_column_name(
            std::string column_name) {
        return _storage_name_and_type_by_col_name[column_name];
    }

    bool has_inverted_index_result_for_expr(const vectorized::VExpr* expr) const {
        return _inverted_index_result_bitmap.find(expr) != _inverted_index_result_bitmap.end();
    }

    void set_inverted_index_result_for_expr(const vectorized::VExpr* expr,
                                            segment_v2::InvertedIndexResultBitmap bitmap) {
        _inverted_index_result_bitmap[expr] = std::move(bitmap);
    }

    segment_v2::InvertedIndexResultBitmap get_or_set_inverted_index_result_for_expr(
            const vectorized::VExpr* expr) {
        auto iter = _inverted_index_result_bitmap.find(expr);
        if (iter == _inverted_index_result_bitmap.end()) {
            _inverted_index_result_bitmap[expr] = segment_v2::InvertedIndexResultBitmap();
            return _inverted_index_result_bitmap[expr];
        }
        return iter->second;
    }

    segment_v2::InvertedIndexResultBitmap get_inverted_index_result_for_expr(
            const vectorized::VExpr* expr) {
        auto iter = _inverted_index_result_bitmap.find(expr);
        if (iter == _inverted_index_result_bitmap.end()) {
            return {};
        }
        return iter->second;
    }

    void set_inverted_index_expr_status(
            const std::unordered_map<std::string,
                                     std::unordered_map<const vectorized::VExpr*, bool>>& status) {
        _expr_inverted_index_status = status;
    }

    segment_v2::InvertedIndexResultBitmap get_inverted_index_result_for_root() {
        auto iter = _inverted_index_result_bitmap.find(_root.get());
        if (iter == _inverted_index_result_bitmap.end()) {
            return {};
        }
        return iter->second;
    }

    std::unordered_map<std::string, std::unordered_map<const vectorized::VExpr*, bool>>
    get_expr_inverted_index_status() {
        return _expr_inverted_index_status;
    }

    void set_true_for_inverted_index_status(const vectorized::VExpr* expr,
                                            const std::string& column_name) {
        if (_expr_inverted_index_status.contains(column_name)) {
            if (_expr_inverted_index_status[column_name].contains(expr)) {
                _expr_inverted_index_status[column_name][expr] = true;
            }
        }
    }

    std::unordered_map<const vectorized::VExpr*, segment_v2::InvertedIndexResultBitmap>
    get_inverted_index_result_bitmap() {
        return _inverted_index_result_bitmap;
    }

    std::unordered_map<const vectorized::VExpr*, ColumnPtr> get_inverted_index_result_column() {
        return _inverted_index_result_column;
    }

    void set_inverted_index_result_column_for_expr(const vectorized::VExpr* expr,
                                                   ColumnPtr column) {
        _inverted_index_result_column[expr] = std::move(column);
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

    [[nodiscard]] static Status evaluate_inverted_index(const VExprContextSPtrs& conjuncts,
                                                        uint32_t segment_num_rows);

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

    // result for inverted index expr evaluated
    // [expr address] -> [rowid_list]
    std::unordered_map<const vectorized::VExpr*, segment_v2::InvertedIndexResultBitmap>
            _inverted_index_result_bitmap;
    std::unordered_map<const vectorized::VExpr*, ColumnPtr> _inverted_index_result_column;
    std::unordered_map<std::string, segment_v2::InvertedIndexIterator*>
            _inverted_index_iterators_by_col_name;
    // storage type schema related to _schema, since column in segment may be different with type in _schema
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>
            _storage_name_and_type_by_col_name;
    std::unordered_map<std::string, std::unordered_map<const vectorized::VExpr*, bool>>
            _expr_inverted_index_status;
};
} // namespace doris::vectorized
