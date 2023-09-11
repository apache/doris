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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "udf/udf.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/functions/function.h"

namespace doris {
class BitmapFilterFuncBase;
class BloomFilterFuncBase;
class HybridSetBase;
class ObjectPool;
class RowDescriptor;
class RuntimeState;

namespace vectorized {

#define RETURN_IF_ERROR_OR_PREPARED(stmt) \
    if (_prepared) {                      \
        return Status::OK();              \
    } else {                              \
        _prepared = true;                 \
        RETURN_IF_ERROR(stmt);            \
    }

// VExpr should be used as shared pointer because it will be passed between classes
// like runtime filter to scan node, or from scannode to scanner. We could not make sure
// the relatioinship between threads and classes.
class VExpr {
public:
    // resize inserted param column to make sure column size equal to block.rows()
    // and return param column index
    static size_t insert_param(Block* block, ColumnWithTypeAndName&& elem, size_t size) {
        // usually elem.column always is const column, so we just clone it.
        elem.column = elem.column->clone_resized(size);
        block->insert(std::move(elem));
        return block->columns() - 1;
    }

    VExpr(const TExprNode& node);
    VExpr(const VExpr& vexpr);
    VExpr(const TypeDescriptor& type, bool is_slotref, bool is_nullable);
    // only used for test
    VExpr() = default;
    virtual ~VExpr() = default;

    virtual const std::string& expr_name() const = 0;

    /// Initializes this expr instance for execution. This does not include initializing
    /// state in the VExprContext; 'context' should only be used to register a
    /// FunctionContext via RegisterFunctionContext().
    ///
    /// Subclasses overriding this function should call VExpr::Prepare() to recursively call
    /// Prepare() on the expr tree
    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                           VExprContext* context);

    /// Initializes 'context' for execution. If scope if FRAGMENT_LOCAL, both fragment- and
    /// thread-local state should be initialized. Otherwise, if scope is THREAD_LOCAL, only
    /// thread-local state should be initialized.
    //
    /// Subclasses overriding this function should call VExpr::Open() to recursively call
    /// Open() on the expr tree
    virtual Status open(RuntimeState* state, VExprContext* context,
                        FunctionContext::FunctionStateScope scope);

    virtual Status execute(VExprContext* context, Block* block, int* result_column_id) = 0;

    /// Subclasses overriding this function should call VExpr::Close().
    //
    /// If scope if FRAGMENT_LOCAL, both fragment- and thread-local state should be torn
    /// down. Otherwise, if scope is THREAD_LOCAL, only thread-local state should be torn
    /// down.
    virtual void close(VExprContext* context, FunctionContext::FunctionStateScope scope);

    DataTypePtr& data_type() { return _data_type; }

    TypeDescriptor type() { return _type; }

    bool is_slot_ref() const { return _node_type == TExprNodeType::SLOT_REF; }

    TExprNodeType::type node_type() const { return _node_type; }

    TExprOpcode::type op() const { return _opcode; }

    void add_child(const VExprSPtr& expr) { _children.push_back(expr); }
    VExprSPtr get_child(int i) const { return _children[i]; }
    int get_num_children() const { return _children.size(); }

    static Status create_expr_tree(const TExpr& texpr, VExprContextSPtr& ctx);

    static Status create_expr_trees(const std::vector<TExpr>& texprs, VExprContextSPtrs& ctxs);

    static Status prepare(const VExprContextSPtrs& ctxs, RuntimeState* state,
                          const RowDescriptor& row_desc);

    static Status open(const VExprContextSPtrs& ctxs, RuntimeState* state);

    static Status clone_if_not_exists(const VExprContextSPtrs& ctxs, RuntimeState* state,
                                      VExprContextSPtrs& new_ctxs);

    bool is_nullable() const { return _data_type->is_nullable(); }

    PrimitiveType result_type() const { return _type.type; }

    static Status create_expr(const TExprNode& expr_node, VExprSPtr& expr);

    static Status create_tree_from_thrift(const std::vector<TExprNode>& nodes, int* node_idx,
                                          VExprSPtr& root_expr, VExprContextSPtr& ctx);
    virtual const VExprSPtrs& children() const { return _children; }
    void set_children(const VExprSPtrs& children) { _children = children; }
    void set_children(VExprSPtrs&& children) { _children = std::move(children); }
    virtual std::string debug_string() const;
    static std::string debug_string(const VExprSPtrs& exprs);
    static std::string debug_string(const VExprContextSPtrs& ctxs);

    bool is_and_expr() const { return _fn.name.function_name == "and"; }

    virtual bool is_compound_predicate() const { return false; }

    const TFunction& fn() const { return _fn; }

    /// Returns true if expr doesn't contain slotrefs, i.e., can be evaluated
    /// with get_value(NULL). The default implementation returns true if all of
    /// the children are constant.
    virtual bool is_constant() const;

    /// If this expr is constant, evaluates the expr with no input row argument and returns
    /// the output. Returns nullptr if the argument is not constant. The returned ColumnPtr is
    /// owned by this expr. This should only be called after Open() has been called on this
    /// expr.
    Status get_const_col(VExprContext* context, std::shared_ptr<ColumnPtrWrapper>* column_wrapper);

    int fn_context_index() const { return _fn_context_index; }

    static const VExprSPtr expr_without_cast(const VExprSPtr& expr) {
        if (expr->node_type() == TExprNodeType::CAST_EXPR) {
            return expr_without_cast(expr->_children[0]);
        }
        return expr;
    }

    // If this expr is a RuntimeFilterWrapper, this method will return an underlying rf expression
    virtual const VExprSPtr get_impl() const { return {}; }

    // If this expr is a BloomPredicate, this method will return a BloomFilterFunc
    virtual std::shared_ptr<BloomFilterFuncBase> get_bloom_filter_func() const {
        LOG(FATAL) << "Method 'get_bloom_filter_func()' is not supported in expression: "
                   << this->debug_string();
        return nullptr;
    }

    virtual std::shared_ptr<HybridSetBase> get_set_func() const { return nullptr; }

    // If this expr is a BitmapPredicate, this method will return a BitmapFilterFunc
    virtual std::shared_ptr<BitmapFilterFuncBase> get_bitmap_filter_func() const {
        LOG(FATAL) << "Method 'get_bitmap_filter_func()' is not supported in expression: "
                   << this->debug_string();
        return nullptr;
    }

protected:
    /// Simple debug string that provides no expr subclass-specific information
    std::string debug_string(const std::string& expr_name) const {
        std::stringstream out;
        out << expr_name << "(" << VExpr::debug_string() << ")";
        return out.str();
    }

    std::string get_child_names() {
        std::string res;
        for (auto child : _children) {
            if (!res.empty()) {
                res += ", ";
            }
            res += child->expr_name();
        }
        return res;
    }

    Status check_constant(const Block& block, ColumnNumbers arguments) const;

    /// Helper function that calls ctx->register(), sets fn_context_index_, and returns the
    /// registered FunctionContext
    void register_function_context(RuntimeState* state, VExprContext* context);

    /// Helper function to initialize function context, called in `open` phase of VExpr:
    /// 1. Set constant columns result of function arguments.
    /// 2. Call function's prepare() to initialize function state, fragment-local or
    /// thread-local according the input `FunctionStateScope` argument.
    Status init_function_context(VExprContext* context, FunctionContext::FunctionStateScope scope,
                                 const FunctionBasePtr& function) const;

    /// Helper function to close function context, fragment-local or thread-local according
    /// the input `FunctionStateScope` argument. Called in `close` phase of VExpr.
    void close_function_context(VExprContext* context, FunctionContext::FunctionStateScope scope,
                                const FunctionBasePtr& function) const;

    TExprNodeType::type _node_type;
    // Used to check what opcode
    TExprOpcode::type _opcode;
    TypeDescriptor _type;
    DataTypePtr _data_type;
    VExprSPtrs _children;
    TFunction _fn;

    /// Index to pass to ExprContext::fn_context() to retrieve this expr's FunctionContext.
    /// Set in RegisterFunctionContext(). -1 if this expr does not need a FunctionContext and
    /// doesn't call RegisterFunctionContext().
    int _fn_context_index;

    // If this expr is constant, this will store and cache the value generated by
    // get_const_col()
    std::shared_ptr<ColumnPtrWrapper> _constant_col;
    bool _prepared;
};

} // namespace vectorized
} // namespace doris
