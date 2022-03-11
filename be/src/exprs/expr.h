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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_EXPR_H
#define DORIS_BE_SRC_QUERY_EXPRS_EXPR_H

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "exprs/expr_value.h"
#include "gen_cpp/Opcodes_types.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/descriptors.h"
#include "runtime/string_value.h"
#include "runtime/string_value.hpp"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "runtime/types.h"
#include "udf/udf.h"

#undef USING_DORIS_UDF
#define USING_DORIS_UDF using namespace doris_udf

USING_DORIS_UDF;

namespace doris {

class Expr;
class ExprContext;
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class TColumnValue;
class TExpr;
class TExprNode;
class TupleIsNullPredicate;
class VectorizedRowBatch;
class Literal;
class MemTracker;
struct UserFunctionCacheEntry;

// This is the superclass of all expr evaluation nodes.
class Expr {
public:
    // typedef for compute functions.
    typedef void* (*ComputeFn)(Expr*, TupleRow*);

    // typdef for vectorize compute functions.
    typedef bool (*VectorComputeFn)(Expr*, VectorizedRowBatch*);

    // Empty virtual destructor
    virtual ~Expr();

    Expr(const Expr& expr);

    virtual Expr* clone(ObjectPool* pool) const = 0;

    // evaluate expr and return pointer to result. The result is
    // valid as long as 'row' doesn't change.
    // TODO: stop having the result cached in this Expr object
    void* get_value(TupleRow* row) { return nullptr; }

    // Vectorize Evalute expr and return result column index.
    // Result cached in batch and valid as long as batch.
    bool evaluate(VectorizedRowBatch* batch);

    bool is_null_scalar_function(std::string& str) {
        // name and function_name both are required
        if (_fn.name.function_name.compare("is_null_pred") == 0) {
            str.assign("null");
            return true;
        } else if (_fn.name.function_name.compare("is_not_null_pred") == 0) {
            str.assign("not null");
            return true;
        } else {
            return false;
        }
    }
    /// Virtual compute functions for each *Val type. Each Expr subclass should implement
    /// the functions for the return type(s) it supports. For example, a boolean function
    /// will only implement GetBooleanVal(). Some Exprs, like Literal, have many possible
    /// return types and will implement multiple Get*Val() functions.
    virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow*);
    virtual TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*);
    virtual SmallIntVal get_small_int_val(ExprContext* context, TupleRow*);
    virtual IntVal get_int_val(ExprContext* context, TupleRow*);
    virtual BigIntVal get_big_int_val(ExprContext* context, TupleRow*);
    virtual LargeIntVal get_large_int_val(ExprContext* context, TupleRow*);
    virtual FloatVal get_float_val(ExprContext* context, TupleRow*);
    virtual DoubleVal get_double_val(ExprContext* context, TupleRow*);
    virtual StringVal get_string_val(ExprContext* context, TupleRow*);
    // TODO(zc)
    // virtual ArrayVal GetArrayVal(ExprContext* context, TupleRow*);
    virtual DateTimeVal get_datetime_val(ExprContext* context, TupleRow*);
    virtual DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow*);
    virtual CollectionVal get_array_val(ExprContext* context, TupleRow*);

    // Get the number of digits after the decimal that should be displayed for this
    // value. Returns -1 if no scale has been specified (currently the scale is only set for
    // doubles set by RoundUpTo). get_value() must have already been called.
    // TODO: this will be unnecessary once we support the DECIMAL(precision, scale) type
    int output_scale() const { return _output_scale; }
    int output_column() const { return _output_column; }

    void add_child(Expr* expr) { _children.push_back(expr); }
    Expr* get_child(int i) const { return _children[i]; }
    int get_num_children() const { return _children.size(); }

    const TypeDescriptor& type() const { return _type; }
    const std::vector<Expr*>& children() const { return _children; }

    TExprOpcode::type op() const { return _opcode; }

    TExprNodeType::type node_type() const { return _node_type; }

    const TFunction& fn() const { return _fn; }

    bool is_slotref() const { return _is_slotref; }

    /// Returns true if this expr uses a FunctionContext to track its runtime state.
    /// Overridden by exprs which use FunctionContext.
    virtual bool has_fn_ctx() const { return false; }

    /// Returns an error status if the function context associated with the
    /// expr has an error set.
    Status get_fn_context_error(ExprContext* ctx);

    static TExprNodeType::type type_without_cast(const Expr* expr);

    static const Expr* expr_without_cast(const Expr* expr);

    // Returns true if expr doesn't contain slotrefs, ie, can be evaluated
    // with get_value(nullptr). The default implementation returns true if all of
    // the children are constant.
    virtual bool is_constant() const;

    // Returns true ifi expr support vectorized process
    // The default implementation returns true if all the children was supported
    virtual bool is_vectorized() const;

    // Returns true if expr bound
    virtual bool is_bound(std::vector<TupleId>* tuple_ids) const;

    // Returns the slots that are referenced by this expr tree in 'slot_ids'.
    // Returns the number of slots added to the vector
    virtual int get_slot_ids(std::vector<SlotId>* slot_ids) const;

    /// Create expression tree from the list of nodes contained in texpr within 'pool'.
    /// Returns the root of expression tree in 'expr' and the corresponding ExprContext in
    /// 'ctx'.
    static Status create_expr_tree(ObjectPool* pool, const TExpr& texpr, ExprContext** ctx);

    /// Creates vector of ExprContexts containing exprs from the given vector of
    /// TExprs within 'pool'.  Returns an error if any of the individual conversions caused
    /// an error, otherwise OK.
    static Status create_expr_trees(ObjectPool* pool, const std::vector<TExpr>& texprs,
                                    std::vector<ExprContext*>* ctxs);

    /// Create a new ScalarExpr based on thrift Expr 'texpr'. The newly created ScalarExpr
    /// is stored in ObjectPool 'pool' and returned in 'expr' on success. 'row_desc' is the
    /// tuple row descriptor of the input tuple row. On failure, 'expr' is set to nullptr and
    /// the expr tree (if created) will be closed. Error status will be returned too.
    static Status create(const TExpr& texpr, const RowDescriptor& row_desc, RuntimeState* state,
                         ObjectPool* pool, Expr** expr, const std::shared_ptr<MemTracker>& tracker);

    /// Create a new ScalarExpr based on thrift Expr 'texpr'. The newly created ScalarExpr
    /// is stored in ObjectPool 'state->obj_pool()' and returned in 'expr'. 'row_desc' is
    /// the tuple row descriptor of the input tuple row. Returns error status on failure.
    static Status create(const TExpr& texpr, const RowDescriptor& row_desc, RuntimeState* state,
                         Expr** expr, const std::shared_ptr<MemTracker>& tracker);

    /// Convenience functions creating multiple ScalarExpr.
    static Status create(const std::vector<TExpr>& texprs, const RowDescriptor& row_desc,
                         RuntimeState* state, ObjectPool* pool, std::vector<Expr*>* exprs,
                         const std::shared_ptr<MemTracker>& tracker);

    /// Convenience functions creating multiple ScalarExpr.
    static Status create(const std::vector<TExpr>& texprs, const RowDescriptor& row_desc,
                         RuntimeState* state, std::vector<Expr*>* exprs,
                         const std::shared_ptr<MemTracker>& tracker);

    /// Convenience function for preparing multiple expr trees.
    /// Allocations from 'ctxs' will be counted against 'tracker'.
    static Status prepare(const std::vector<ExprContext*>& ctxs, RuntimeState* state,
                          const RowDescriptor& row_desc,
                          const std::shared_ptr<MemTracker>& tracker);

    /// Convenience function for opening multiple expr trees.
    static Status open(const std::vector<ExprContext*>& ctxs, RuntimeState* state);

    /// Clones each ExprContext for multiple expr trees. 'new_ctxs' must be non-nullptr.
    /// Idempotent: if '*new_ctxs' is empty, a clone of each context in 'ctxs' will be added
    /// to it, and if non-empty, it is assumed CloneIfNotExists() was already called and the
    /// call is a no-op. The new ExprContexts are created in state->obj_pool().
    static Status clone_if_not_exists(const std::vector<ExprContext*>& ctxs, RuntimeState* state,
                                      std::vector<ExprContext*>* new_ctxs);

    /// Convenience function for closing multiple expr trees.
    static void close(const std::vector<ExprContext*>& ctxs, RuntimeState* state);

    /// Convenience functions for closing a list of ScalarExpr.
    static void close(const std::vector<Expr*>& exprs);

    // Computes a memory efficient layout for storing the results of evaluating 'exprs'
    // Returns the number of bytes necessary to store all the results and offsets
    // where the result for each expr should be stored.
    // Variable length types are guaranteed to be at the end and 'var_result_begin'
    // will be set the beginning byte offset where variable length results begin.
    // 'var_result_begin' will be set to -1 if there are no variable len types.
    static int compute_results_layout(const std::vector<Expr*>& exprs, std::vector<int>* offsets,
                                      int* var_result_begin);
    static int compute_results_layout(const std::vector<ExprContext*>& ctxs,
                                      std::vector<int>* offsets, int* var_result_begin);

    /// If this expr is constant, evaluates the expr with no input row argument and returns
    /// the output. Returns nullptr if the argument is not constant. The returned AnyVal* is
    /// owned by this expr. This should only be called after Open() has been called on this
    /// expr.
    virtual AnyVal* get_const_val(ExprContext* context);

    /// Assigns indices into the FunctionContext vector 'fn_ctxs_' in an evaluator to
    /// nodes which need FunctionContext in the tree. 'next_fn_ctx_idx' is the index
    /// of the next available entry in the vector. It's updated as this function is
    /// called recursively down the tree.
    void assign_fn_ctx_idx(int* next_fn_ctx_idx);

    virtual std::string debug_string() const;
    static std::string debug_string(const std::vector<Expr*>& exprs);
    static std::string debug_string(const std::vector<ExprContext*>& ctxs);

    // Prefix of Expr::GetConstant() symbols, regardless of template specialization
    static const char* _s_get_constant_symbol_prefix;

    /// The builtin functions are not called from anywhere in the code and the
    /// symbols are therefore not included in the binary. We call these functions
    /// by using dlsym. The compiler must think this function is callable to
    /// not strip these symbols.
    static void init_builtins_dummy();

    // Any additions to this enum must be reflected in both GetConstant() and
    // GetIrConstant().
    enum ExprConstant {
        RETURN_TYPE_SIZE, // int
        ARG_TYPE_SIZE     // int[]
    };

    static Expr* copy(ObjectPool* pool, Expr* old_expr);

protected:
    friend class AggFnEvaluator;
    friend class AnaFnEvaluator;
    friend class TopNNode;
    friend class AnalyticEvalNode;
    friend class ComputeFunctions;
    friend class MathFunctions;
    friend class StringFunctions;
    friend class TimestampFunctions;
    friend class ConditionalFunctions;
    friend class UtilityFunctions;
    friend class CaseExpr;
    friend class InPredicate;
    friend class InfoFunc;
    friend class FunctionCall;
    friend class HashJoinNode;
    friend class ExecNode;
    friend class OlapScanNode;
    friend class SetVar;
    friend class NativeUdfExpr;
    friend class JsonFunctions;
    friend class Literal;
    friend class ExprContext;
    friend class CompoundPredicate;
    friend class ScalarFnCall;
    friend class HllHashFunction;

    /// Constructs an Expr tree from the thrift Expr 'texpr'. 'root' is the root of the
    /// Expr tree created from texpr.nodes[0] by the caller (either ScalarExpr or AggFn).
    /// The newly created Expr nodes are added to 'pool'. Returns error status on failure.
    static Status create_tree(const TExpr& texpr, ObjectPool* pool, Expr* root);

    int fn_ctx_idx() const { return _fn_ctx_idx; }

    Expr(const TypeDescriptor& type);
    Expr(const TypeDescriptor& type, bool is_slotref);
    Expr(const TExprNode& node);
    Expr(const TExprNode& node, bool is_slotref);

    /// Initializes this expr instance for execution. This does not include initializing
    /// state in the ExprContext; 'context' should only be used to register a
    /// FunctionContext via RegisterFunctionContext(). Any IR functions must be generated
    /// here.
    ///
    /// Subclasses overriding this function should call Expr::Prepare() to recursively call
    /// Prepare() on the expr tree.
    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                           ExprContext* context);

    /// Initializes 'context' for execution. If scope if FRAGMENT_LOCAL, both fragment- and
    /// thread-local state should be initialized. Otherwise, if scope is THREAD_LOCAL, only
    /// thread-local state should be initialized.
    //
    /// Subclasses overriding this function should call Expr::Open() to recursively call
    /// Open() on the expr tree.
    Status open(RuntimeState* state, ExprContext* context) {
        return open(state, context, FunctionContext::FRAGMENT_LOCAL);
    }

    virtual Status open(RuntimeState* state, ExprContext* context,
                        FunctionContext::FunctionStateScope scope);

    /// Subclasses overriding this function should call Expr::Close().
    //
    /// If scope if FRAGMENT_LOCAL, both fragment- and thread-local state should be torn
    /// down. Otherwise, if scope is THREAD_LOCAL, only thread-local state should be torn
    /// down.
    void close(RuntimeState* state, ExprContext* context) {
        close(state, context, FunctionContext::FRAGMENT_LOCAL);
    }

    virtual void close(RuntimeState* state, ExprContext* context,
                       FunctionContext::FunctionStateScope scope);

    /// Releases cache entries to LibCache in all nodes of the Expr tree.
    virtual void close();

    /// Helper function that calls ctx->Register(), sets fn_context_index_, and returns the
    /// registered FunctionContext.
    FunctionContext* register_function_context(ExprContext* ctx, RuntimeState* state,
                                               int varargs_buffer_size);

    /// Cache entry for the library implementing this function.
    UserFunctionCacheEntry* _cache_entry = nullptr;

    // function opcode

    TExprNodeType::type _node_type;

    // Used to check what opcode
    TExprOpcode::type _opcode;

    // recognize if this node is a slotref in order to speed up get_value()
    const bool _is_slotref;

    // analysis is done, types are fixed at this point
    TypeDescriptor _type;
    std::vector<Expr*> _children;
    int _output_scale;
    int _output_column;

    /// Function description.
    TFunction _fn;

    /// Index to pass to ExprContext::fn_context() to retrieve this expr's FunctionContext.
    /// Set in RegisterFunctionContext(). -1 if this expr does not need a FunctionContext and
    /// doesn't call RegisterFunctionContext().
    int _fn_context_index;

    // If this expr is constant, this will store and cache the value generated by
    // get_const_val().
    std::shared_ptr<AnyVal> _constant_val;

    // function to evaluate vectorize expr; typically set in prepare()
    VectorComputeFn _vector_compute_fn;

    /// Simple debug string that provides no expr subclass-specific information
    std::string debug_string(const std::string& expr_name) const {
        std::stringstream out;
        out << expr_name << "(" << Expr::debug_string() << ")";
        return out.str();
    }

private:
    friend class ExprTest;
    friend class QueryJitter;

    // Create a new Expr based on texpr_node.node_type within 'pool'.
    static Status create_expr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr);

    // Create a new Expr based on texpr_node.node_type within 'pool'.
    static Status create_expr(ObjectPool* pool, const Expr* old_expr, Expr** new_expr);

    /// Creates an expr tree for the node rooted at 'node_idx' via depth-first traversal.
    /// parameters
    ///   nodes: vector of thrift expression nodes to be translated
    ///   parent: parent of node at node_idx (or nullptr for node_idx == 0)
    ///   node_idx:
    ///     in: root of TExprNode tree
    ///     out: next node in 'nodes' that isn't part of tree
    ///   root_expr: out: root of constructed expr tree
    ///   ctx: out: context of constructed expr tree
    /// return
    ///   status.ok() if successful
    ///   !status.ok() if tree is inconsistent or corrupt
    static Status create_tree_from_thrift(ObjectPool* pool, const std::vector<TExprNode>& nodes,
                                          Expr* parent, int* node_idx, Expr** root_expr,
                                          ExprContext** ctx);

    /// Static wrappers around the virtual Get*Val() functions. Calls the appropriate
    /// Get*Val() function on expr, passing it the context and row arguments.
    //
    /// These are used to call Get*Val() functions from generated functions, since I don't
    /// know how to call virtual functions directly. GetStaticGetValWrapper() returns the
    /// IR function of the appropriate wrapper function.
    static BooleanVal get_boolean_val(Expr* expr, ExprContext* context, TupleRow* row);
    static TinyIntVal get_tiny_int_val(Expr* expr, ExprContext* context, TupleRow* row);
    static SmallIntVal get_small_int_val(Expr* expr, ExprContext* context, TupleRow* row);
    static IntVal get_int_val(Expr* expr, ExprContext* context, TupleRow* row);
    static BigIntVal get_big_int_val(Expr* expr, ExprContext* context, TupleRow* row);
    static LargeIntVal get_large_int_val(Expr* expr, ExprContext* context, TupleRow* row);
    static FloatVal get_float_val(Expr* expr, ExprContext* context, TupleRow* row);
    static DoubleVal get_double_val(Expr* expr, ExprContext* context, TupleRow* row);
    static StringVal get_string_val(Expr* expr, ExprContext* context, TupleRow* row);
    static DateTimeVal get_datetime_val(Expr* expr, ExprContext* context, TupleRow* row);
    static CollectionVal get_array_val(Expr* expr, ExprContext* context, TupleRow* row);
    static DecimalV2Val get_decimalv2_val(Expr* expr, ExprContext* context, TupleRow* row);

    /// Creates an expression tree rooted at 'root' via depth-first traversal.
    /// Called recursively to create children expr trees for sub-expressions.
    ///
    /// parameters:
    ///   nodes: vector of thrift expression nodes to be unpacked.
    ///          It is essentially an Expr tree encoded in a depth-first manner.
    ///   pool: Object pool in which Expr created from nodes are stored.
    ///   root: root of the new tree. Created and initialized by the caller.
    ///   child_node_idx: index into 'nodes' to be unpacked. It's the root of the next child
    ///                   child Expr tree to be added to 'root'. Updated as 'nodes' are
    ///                   consumed to construct the tree.
    /// return
    ///   status.ok() if successful
    ///   !status.ok() if tree is inconsistent or corrupt
    static Status create_tree_internal(const std::vector<TExprNode>& nodes, ObjectPool* pool,
                                       Expr* parent, int* child_node_idx);

    /// 'fn_ctx_idx_' is the index into the FunctionContext vector in ScalarExprEvaluator
    /// for storing FunctionContext needed to evaluate this ScalarExprNode. It's -1 if this
    /// ScalarExpr doesn't need a FunctionContext. The FunctionContext is managed by the
    /// evaluator and initialized by calling ScalarExpr::OpenEvaluator().
    int _fn_ctx_idx = -1;

    /// [fn_ctx_idx_start_, fn_ctx_idx_end_) defines the range in FunctionContext vector
    /// in ScalarExpeEvaluator for the expression subtree rooted at this ScalarExpr node.
    int _fn_ctx_idx_start = 0;
    int _fn_ctx_idx_end = 0;
};

inline bool Expr::evaluate(VectorizedRowBatch* batch) {
    DCHECK(_type.type != INVALID_TYPE);

    if (_is_slotref) {
        // return SlotRef::vector_compute_fn(this, batch);
        return false;
    } else {
        return _vector_compute_fn(this, batch);
    }
}

} // namespace doris

#endif
