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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_EXPR_CONTEXT_H
#define DORIS_BE_SRC_QUERY_EXPRS_EXPR_CONTEXT_H

#include <memory>

#include "common/status.h"
#include "exprs/expr.h"
#include "exprs/expr_value.h"
#include "exprs/slot_ref.h"
#include "udf/udf.h"
#include "udf/udf_internal.h" // for CollectionVal

#undef USING_DORIS_UDF
#define USING_DORIS_UDF using namespace doris_udf

USING_DORIS_UDF;

namespace doris {

class Expr;
class MemPool;
class MemTracker;
class RuntimeState;
class RowDescriptor;
class TColumnValue;
class TupleRow;

/// An ExprContext contains the state for the execution of a tree of Exprs, in particular
/// the FunctionContexts necessary for the expr tree. This allows for multi-threaded
/// expression evaluation, as a given tree can be evaluated using multiple ExprContexts
/// concurrently. A single ExprContext is not thread-safe.
class ExprContext {
public:
    ExprContext(Expr* root);
    ~ExprContext();

    /// Prepare expr tree for evaluation.
    /// Allocations from this context will be counted against 'tracker'.
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   const std::shared_ptr<MemTracker>& tracker);

    /// Must be called after calling Prepare(). Does not need to be called on clones.
    /// Idempotent (this allows exprs to be opened multiple times in subplans without
    /// reinitializing function state).
    Status open(RuntimeState* state);

    //TODO chenhao
    static Status open(std::vector<ExprContext*> input_evals, RuntimeState* state);

    /// Creates a copy of this ExprContext. Open() must be called first. The copy contains
    /// clones of each FunctionContext, which share the fragment-local state of the
    /// originals but have their own MemPool and thread-local state. Clone() should be used
    /// to create an ExprContext for each execution thread that needs to evaluate
    /// 'root'. Note that clones are already opened. '*new_context' must be initialized by
    /// the caller to nullptr.
    Status clone(RuntimeState* state, ExprContext** new_context);

    Status clone(RuntimeState* state, ExprContext** new_ctx, Expr* root);

    /// Closes all FunctionContexts. Must be called on every ExprContext, including clones.
    void close(RuntimeState* state);

    /// Calls the appropriate Get*Val() function on this context's expr tree and stores the
    /// result in result_.
    void* get_value(TupleRow* row);

    /// Convenience functions: print value into 'str' or 'stream'.  nullptr turns into "NULL".
    void print_value(TupleRow* row, std::string* str);
    void print_value(void* value, std::string* str);
    void print_value(void* value, std::stringstream* stream);
    void print_value(TupleRow* row, std::stringstream* stream);

    /// Creates a FunctionContext, and returns the index that's passed to fn_context() to
    /// retrieve the created context. Exprs that need a FunctionContext should call this in
    /// Prepare() and save the returned index. 'varargs_buffer_size', if specified, is the
    /// size of the varargs buffer in the created FunctionContext (see udf-internal.h).
    int register_func(RuntimeState* state, const FunctionContext::TypeDesc& return_type,
                      const std::vector<FunctionContext::TypeDesc>& arg_types,
                      int varargs_buffer_size);

    /// Retrieves a registered FunctionContext. 'i' is the index returned by the call to
    /// register_func(). This should only be called by Exprs.
    FunctionContext* fn_context(int i) {
        DCHECK_GE(i, 0);
        DCHECK_LT(i, _fn_contexts.size());
        return _fn_contexts[i];
    }

    Expr* root() { return _root; }

    bool closed() { return _closed; }

    bool is_nullable();

    /// Calls Get*Val on _root
    BooleanVal get_boolean_val(TupleRow* row);
    TinyIntVal get_tiny_int_val(TupleRow* row);
    SmallIntVal get_small_int_val(TupleRow* row);
    IntVal get_int_val(TupleRow* row);
    BigIntVal get_big_int_val(TupleRow* row);
    FloatVal get_float_val(TupleRow* row);
    DoubleVal get_double_val(TupleRow* row);
    StringVal get_string_val(TupleRow* row);
    // TODO(zc):
    // ArrayVal GetArrayVal(TupleRow* row);
    DateTimeVal get_datetime_val(TupleRow* row);
    DecimalV2Val get_decimalv2_val(TupleRow* row);

    /// Frees all local allocations made by fn_contexts_. This can be called when result
    /// data from this context is no longer needed.
    void free_local_allocations();
    static void free_local_allocations(const std::vector<ExprContext*>& ctxs);
    static void free_local_allocations(const std::vector<FunctionContext*>& ctxs);

    bool opened() { return _opened; }

    /// If 'expr' is constant, evaluates it with no input row argument and returns the
    /// result in 'const_val'. Sets 'const_val' to nullptr if the argument is not constant.
    /// The returned AnyVal and associated varlen data is owned by this evaluator. This
    /// should only be called after Open() has been called on this expr. Returns an error
    /// if there was an error evaluating the expression or if memory could not be allocated
    /// for the expression result.
    Status get_const_value(RuntimeState* state, Expr& expr, AnyVal** const_val);

    /// Returns an error status if there was any error in evaluating the expression
    /// or its sub-expressions. 'start_idx' and 'end_idx' correspond to the range
    /// within the vector of FunctionContext for the sub-expressions of interest.
    /// The default parameters correspond to the entire expr 'root_'.
    Status get_error(int start_idx, int end_idx) const;

    std::string get_error_msg() const;

    // when you reused this expr context, you maybe need clear the error status and message.
    void clear_error_msg();

private:
    friend class Expr;
    friend class ScalarFnCall;
    friend class RPCFnCall;
    friend class InPredicate;
    friend class RuntimePredicateWrapper;
    friend class BloomFilterPredicate;
    friend class OlapScanNode;
    friend class EsScanNode;
    friend class EsPredicate;

    /// FunctionContexts for each registered expression. The FunctionContexts are created
    /// and owned by this ExprContext.
    std::vector<FunctionContext*> _fn_contexts;

    /// Pool backing fn_contexts_. Counts against the runtime state's UDF mem tracker.
    std::unique_ptr<MemPool> _pool;

    /// The expr tree this context is for.
    Expr* _root;

    /// Stores the result of the root expr. This is used in interpreted code when we need a
    /// void*.
    ExprValue _result;

    /// True if this context came from a Clone() call. Used to manage FunctionStateScope.
    bool _is_clone;

    /// Variables keeping track of current state.
    bool _prepared;
    bool _opened;
    bool _closed;

    /// Calls the appropriate Get*Val() function on 'e' and stores the result in result_.
    /// This is used by Exprs to call GetValue() on a child expr, rather than root_.
    void* get_value(Expr* e, TupleRow* row);
};

inline void* ExprContext::get_value(TupleRow* row) {
    if (_root->is_slotref()) {
        return SlotRef::get_value(_root, row);
    }
    return get_value(_root, row);
}

} // namespace doris

#endif
