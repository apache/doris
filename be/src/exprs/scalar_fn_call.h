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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/scalar-fn-call.h
// and modified by Doris

#pragma once

#include <string>

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "udf/udf.h"

namespace doris {

class TExprNode;

/// Expr for evaluating a pre-compiled native or LLVM IR function that uses the UDF
/// interface (i.e. a scalar function). This class overrides GetCodegendComputeFn() to
/// return a function that calls any child exprs and passes the results as arguments to the
/// specified scalar function. If codegen is enabled, ScalarFnCall's Get*Val() compute
/// functions are wrappers around this codegen'd function.
//
/// If codegen is disabled, some native functions can be called without codegen, depending
/// on the native function's signature. However, since we can't write static code to call
/// every possible function signature, codegen may be required to generate the call to the
/// function even if codegen is disabled. Codegen will also be used for IR UDFs (note that
/// there is no way to specify both a native and IR library for a single UDF).
//
/// TODO:
/// - Fix error reporting, e.g. reporting leaks
/// - Testing
///    - Test cancellation
///    - Type descs in UDA test harness
///    - Allow more functions to be nullptr in UDA test harness
class ScalarFnCall : public Expr {
public:
    virtual std::string debug_string() const override;
    virtual ~ScalarFnCall();
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new ScalarFnCall(*this));
    }

    // TODO: just for table function.
    // It is not good to expose this field to public.
    // We should refactor it after implementing real table functions.
    int get_fn_context_index() const { return _fn_context_index; }

protected:
    friend class Expr;

    ScalarFnCall(const TExprNode& node);
    virtual Status prepare(RuntimeState* state, const RowDescriptor& desc,
                           ExprContext* context) override;
    virtual Status open(RuntimeState* state, ExprContext* context,
                        FunctionContext::FunctionStateScope scope) override;
    virtual void close(RuntimeState* state, ExprContext* context,
                       FunctionContext::FunctionStateScope scope) override;

    virtual bool is_constant() const override;

    virtual doris_udf::BooleanVal get_boolean_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::FloatVal get_float_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::DoubleVal get_double_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::StringVal get_string_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::DateTimeVal get_datetime_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::DateV2Val get_datev2_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::DateTimeV2Val get_datetimev2_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow*) override;
    virtual CollectionVal get_array_val(ExprContext* context, TupleRow*) override;

    virtual Decimal32Val get_decimal32_val(ExprContext* context, TupleRow*) override;
    virtual Decimal64Val get_decimal64_val(ExprContext* context, TupleRow*) override;
    virtual Decimal128Val get_decimal128_val(ExprContext* context, TupleRow*) override;

private:
    /// If this function has var args, children()[_vararg_start_idx] is the first vararg
    /// argument.
    /// If this function does not have varargs, it is set to -1.
    int _vararg_start_idx;

    /// Function pointer to the JIT'd function produced by GetCodegendComputeFn().
    /// Has signature *Val (ExprContext*, TupleRow*), and calls the scalar
    /// function with signature like *Val (FunctionContext*, const *Val& arg1, ...)
    void* _scalar_fn_wrapper;

    /// The UDF's prepare function, if specified. This is initialized in Prepare() and
    /// called in Open() (since we may have needed to codegen the function if it's from an
    /// IR module).
    UdfPrepare _prepare_fn;

    /// THe UDF's close function, if specified. This is initialized in Prepare() and called
    /// in Close().
    UdfClose _close_fn;

    /// If running with codegen disabled, _scalar_fn will be a pointer to the non-JIT'd
    /// scalar function.
    void* _scalar_fn;

    /// Returns the number of non-vararg arguments
    int num_fixed_args() const {
        return _vararg_start_idx >= 0 ? _vararg_start_idx : _children.size();
    }

    /// Loads the native or IR function 'symbol' from HDFS and puts the result in *fn.
    /// If the function is loaded from an IR module, it cannot be called until the module
    /// has been JIT'd (i.e. after Prepare() has completed).
    Status get_function(RuntimeState* state, const std::string& symbol, void** fn);

    /// Evaluates the children exprs and stores the results in input_vals. Used in the
    /// interpreted path.
    void evaluate_children(ExprContext* context, TupleRow* row,
                           std::vector<doris_udf::AnyVal*>* input_vals);

    /// Function to call _scalar_fn. Used in the interpreted path.
    template <typename RETURN_TYPE>
    RETURN_TYPE interpret_eval(ExprContext* context, TupleRow* row);
};

} // namespace doris
