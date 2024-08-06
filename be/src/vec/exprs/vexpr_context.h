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
            const std::vector<std::unique_ptr<segment_v2::InvertedIndexIterator>>& iterators) {
        _inverted_index_iterators.clear();
        _inverted_index_iterators.reserve(iterators.size());

        for (const auto& iterator : iterators) {
            _inverted_index_iterators.push_back(iterator.get());
        }
    }

    void set_storage_name_and_type(
            const std::vector<vectorized::IndexFieldNameAndTypePair>& storage_name_and_type) {
        _storage_name_and_type = storage_name_and_type;
    }

    segment_v2::InvertedIndexIterator* get_inverted_index_iterators_by_column_id(
            ColumnId column_id) const {
        if (column_id >= _inverted_index_iterators.size()) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "_inverted_index_iterators column_id invalid, column_id={}, "
                            "_inverted_index_iterators.size()={}",
                            column_id, _inverted_index_iterators.size());
        }
        return _inverted_index_iterators[column_id];
    }

    vectorized::IndexFieldNameAndTypePair get_storage_name_and_type_by_column_id(
            ColumnId column_id) const {
        if (column_id >= _storage_name_and_type.size()) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "_storage_name_and_type column_id invalid, column_id={}, "
                            "_storage_name_and_type.size()={}",
                            column_id, _storage_name_and_type.size());
        }
        return _storage_name_and_type[column_id];
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
    /**
     * @param colid_to_inverted_index_iter contains all column id to inverted index iterator mapping from segmentIterator
     * @param num_rows number of rows in one segment.
     * @param bitmap roaring bitmap to store the result. 0 is present filed by index.
     * @return status not ok means execute failed.
     */
    [[nodiscard]] Status eval_inverted_index(
            const std::unordered_map<ColumnId, std::pair<vectorized::IndexFieldNameAndTypePair,
                                                         segment_v2::InvertedIndexIterator*>>&
                    colid_to_inverted_index_iter,
            uint32_t num_rows, roaring::Roaring* bitmap);

    [[nodiscard]] static Status evaluate_inverted_index(const VExprContextSPtrs& conjuncts,
                                                        uint32_t segment_num_rows);

    [[nodiscard]] Status evaluate_inverted_index(uint32_t segment_num_rows);

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
    std::vector<segment_v2::InvertedIndexIterator*> _inverted_index_iterators;
    // storage type schema related to _schema, since column in segment may be different with type in _schema
    std::vector<vectorized::IndexFieldNameAndTypePair> _storage_name_and_type;
};
} // namespace doris::vectorized
