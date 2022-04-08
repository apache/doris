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

#include <memory>
#include <vector>

#include "common/status.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/types.h"
#include "udf/udf_internal.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/functions/function.h"

namespace doris {
namespace vectorized {

class VExpr {
public:
    VExpr(const TExprNode& node);
    VExpr(const TypeDescriptor& type, bool is_slotref, bool is_nullable);
    // only used for test
    VExpr() {}
    virtual ~VExpr() = default;

    virtual VExpr* clone(ObjectPool* pool) const = 0;

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
    /// Subclasses overriding this function should call Expr::Open() to recursively call
    /// Open() on the expr tree
    virtual Status open(RuntimeState* state, VExprContext* context,
                        FunctionContext::FunctionStateScope scope);

    virtual Status execute(VExprContext* context, vectorized::Block* block,
                           int* result_column_id) = 0;

    /// Subclasses overriding this function should call VExpr::Close().
    //
    /// If scope if FRAGMENT_LOCAL, both fragment- and thread-local state should be torn
    /// down. Otherwise, if scope is THREAD_LOCAL, only thread-local state should be torn
    /// down.
    virtual void close(RuntimeState* state, VExprContext* context,
                       FunctionContext::FunctionStateScope scope);

    DataTypePtr& data_type() { return _data_type; }

    TypeDescriptor type() { return _type; }

    bool is_slot_ref() const { return _node_type == TExprNodeType::SLOT_REF; }

    TExprNodeType::type node_type() const { return _node_type; }

    void add_child(VExpr* expr) { _children.push_back(expr); }

    static Status create_expr_tree(ObjectPool* pool, const TExpr& texpr, VExprContext** ctx);

    static Status create_expr_trees(ObjectPool* pool, const std::vector<TExpr>& texprs,
                                    std::vector<VExprContext*>* ctxs);

    static Status prepare(const std::vector<VExprContext*>& ctxs, RuntimeState* state,
                          const RowDescriptor& row_desc,
                          const std::shared_ptr<MemTracker>& tracker);

    static Status open(const std::vector<VExprContext*>& ctxs, RuntimeState* state);

    static Status clone_if_not_exists(const std::vector<VExprContext*>& ctxs, RuntimeState* state,
                                      std::vector<VExprContext*>* new_ctxs);

    static void close(const std::vector<VExprContext*>& ctxs, RuntimeState* state);

    bool is_nullable() const { return _data_type->is_nullable(); }

    PrimitiveType result_type() const { return _type.type; }

    static Status create_expr(ObjectPool* pool, const TExprNode& texpr_node, VExpr** expr);

    static Status create_tree_from_thrift(ObjectPool* pool, const std::vector<TExprNode>& nodes,
                                          VExpr* parent, int* node_idx, VExpr** root_expr,
                                          VExprContext** ctx);
    const std::vector<VExpr*>& children() const { return _children; }
    void set_children(std::vector<VExpr*> children) { _children = children; }
    virtual std::string debug_string() const;
    static std::string debug_string(const std::vector<VExpr*>& exprs);
    static std::string debug_string(const std::vector<VExprContext*>& ctxs);

    bool is_and_expr() { return _fn.name.function_name == "and"; }

    const TFunction& fn() const { return _fn; }

    /// Returns true if expr doesn't contain slotrefs, i.e., can be evaluated
    /// with get_value(NULL). The default implementation returns true if all of
    /// the children are constant.
    virtual bool is_constant() const;

    /// If this expr is constant, evaluates the expr with no input row argument and returns
    /// the output. Returns nullptr if the argument is not constant. The returned ColumnPtr is
    /// owned by this expr. This should only be called after Open() has been called on this
    /// expr.
    virtual ColumnPtrWrapper* get_const_col(VExprContext* context);

protected:
    /// Simple debug string that provides no expr subclass-specific information
    std::string debug_string(const std::string& expr_name) const {
        std::stringstream out;
        out << expr_name << "(" << VExpr::debug_string() << ")";
        return out.str();
    }

    /// Helper function that calls ctx->register(), sets fn_context_index_, and returns the
    /// registered FunctionContext
    void register_function_context(doris::RuntimeState* state, VExprContext* context);

    /// Helper function to initialize function context, called in `open` phase of VExpr:
    /// 1. Set constant columns result of function arguments.
    /// 2. Call function's prepare() to initialize function state, fragment-local or
    /// thread-local according the input `FunctionStateScope` argument.
    Status init_function_context(VExprContext* context, FunctionContext::FunctionStateScope scope,
                                 const FunctionBasePtr& function);

    /// Helper function to close function context, fragment-local or thread-local according
    /// the input `FunctionStateScope` argument. Called in `close` phase of VExpr.
    void close_function_context(VExprContext* context, FunctionContext::FunctionStateScope scope,
                                const FunctionBasePtr& function);

    TExprNodeType::type _node_type;
    TypeDescriptor _type;
    DataTypePtr _data_type;
    std::vector<VExpr*> _children;
    TFunction _fn;

    /// Index to pass to ExprContext::fn_context() to retrieve this expr's FunctionContext.
    /// Set in RegisterFunctionContext(). -1 if this expr does not need a FunctionContext and
    /// doesn't call RegisterFunctionContext().
    int _fn_context_index;

    // If this expr is constant, this will store and cache the value generated by
    // get_const_col()
    std::shared_ptr<ColumnPtrWrapper> _constant_col;
};

} // namespace vectorized
} // namespace doris
