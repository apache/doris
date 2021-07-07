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

#include "exprs/scalar_fn_call.h"

#include <vector>

#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"
#include "runtime/user_function_cache.h"
#include "udf/udf_internal.h"
#include "util/debug_util.h"
#include "util/symbols_util.h"

namespace doris {

ScalarFnCall::ScalarFnCall(const TExprNode& node)
        : Expr(node),
          _vararg_start_idx(node.__isset.vararg_start_idx ? node.vararg_start_idx : -1),
          _scalar_fn_wrapper(NULL),
          _prepare_fn(NULL),
          _close_fn(NULL),
          _scalar_fn(NULL) {
    DCHECK_NE(_fn.binary_type, TFunctionBinaryType::HIVE);
}

ScalarFnCall::~ScalarFnCall() {}

Status ScalarFnCall::prepare(RuntimeState* state, const RowDescriptor& desc, ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, desc, context));
    if (_fn.scalar_fn.symbol.empty()) {
        // This path is intended to only be used during development to test FE
        // code before the BE has implemented the function.
        // Having the failure in the BE (rather than during analysis) allows for
        // better FE testing.
        DCHECK_EQ(_fn.binary_type, TFunctionBinaryType::BUILTIN);
        std::stringstream ss;
        ss << "Function " << _fn.name.function_name << " is not implemented.";
        return Status::InternalError(ss.str());
    }

    FunctionContext::TypeDesc return_type = AnyValUtil::column_type_to_type_desc(_type);
    std::vector<FunctionContext::TypeDesc> arg_types;
    bool char_arg = false;
    for (int i = 0; i < _children.size(); ++i) {
        arg_types.push_back(AnyValUtil::column_type_to_type_desc(_children[i]->_type));
        char_arg = char_arg || (_children[i]->_type.type == TYPE_CHAR);
    }

    // Compute buffer size for varargs
    int varargs_buffer_size = 0;
    if (_vararg_start_idx != -1) {
        DCHECK_GT(get_num_children(), _vararg_start_idx);
        for (int i = _vararg_start_idx; i < get_num_children(); ++i) {
            varargs_buffer_size += AnyValUtil::any_val_size(_children[i]->type());
        }
    }

    _fn_context_index = context->register_func(state, return_type, arg_types, varargs_buffer_size);
    // _scalar_fn = OpcodeRegistry::instance()->get_function_ptr(_opcode);
    Status status = Status::OK();
    if (_scalar_fn == NULL) {
        if (SymbolsUtil::is_mangled(_fn.scalar_fn.symbol)) {
            status = UserFunctionCache::instance()->get_function_ptr(
                    _fn.id, _fn.scalar_fn.symbol, _fn.hdfs_location, _fn.checksum, &_scalar_fn,
                    &_cache_entry);
        } else {
            std::vector<TypeDescriptor> arg_types;
            for (auto& t_type : _fn.arg_types) {
                arg_types.push_back(TypeDescriptor::from_thrift(t_type));
            }
            // ColumnType ret_type(INVALID_TYPE);
            // ret_type = ColumnType(thrift_to_type(_fn.ret_type));
            std::string symbol = SymbolsUtil::mangle_user_function(_fn.scalar_fn.symbol, arg_types,
                                                                   _fn.has_var_args, NULL);
            status = UserFunctionCache::instance()->get_function_ptr(
                    _fn.id, symbol, _fn.hdfs_location, _fn.checksum, &_scalar_fn, &_cache_entry);
        }
    }
#if 0
    // If the codegen object hasn't been created yet and we're calling a builtin or native
    // UDF with <= 8 non-variadic arguments, we can use the interpreted path and call the
    // builtin without codegen. This saves us the overhead of creating the codegen object
    // when it's not necessary (i.e., in plan fragments with no codegen-enabled operators).
    // In addition, we can never codegen char arguments.
    // TODO: codegen for char arguments
    if (char_arg || (!state->codegen_created() && num_fixed_args() <= 8 &&
                     (_fn.binary_type == TFunctionBinaryType::BUILTIN ||
                      _fn.binary_type == TFunctionBinaryType::NATIVE))) {
        // Builtins with char arguments must still have <= 8 arguments.
        // TODO: delete when we have codegen for char arguments
        if (char_arg) {
            DCHECK(num_fixed_args() <= 8 && _fn.binary_type == TFunctionBinaryType::BUILTIN);
        }
        Status status = UserFunctionCache::instance()->GetSoFunctionPtr(
            _fn.hdfs_location, _fn.scalar_fn.symbol, &_scalar_fn, &cache_entry_);
        if (!status.ok()) {
            if (_fn.binary_type == TFunctionBinaryType::BUILTIN) {
                // Builtins symbols should exist unless there is a version mismatch.
                status.SetErrorMsg(ErrorMsg(TErrorCode::MISSING_BUILTIN,
                                            _fn.name.function_name, _fn.scalar_fn.symbol));
                return status;
            } else {
                DCHECK_EQ(_fn.binary_type, TFunctionBinaryType::NATIVE);
                return Status::InternalError(strings::Substitute("Problem loading UDF '$0':\n$1",
                                         _fn.name.function_name, status.GetDetail()));
                return status;
            }
        }
    } else {
        // If we got here, either codegen is enabled or we need codegen to run this function.
        LlvmCodeGen* codegen;
        RETURN_IF_ERROR(state->GetCodegen(&codegen));

        if (_fn.binary_type == TFunctionBinaryType::IR) {
            std::string local_path;
            RETURN_IF_ERROR(UserFunctionCache::instance()->GetLocalLibPath(
                    _fn.hdfs_location, UserFunctionCache::TYPE_IR, &local_path));
            // Link the UDF module into this query's main module (essentially copy the UDF
            // module into the main module) so the UDF's functions are available in the main
            // module.
            RETURN_IF_ERROR(codegen->LinkModule(local_path));
        }

        Function* ir_udf_wrapper;
        RETURN_IF_ERROR(GetCodegendComputeFn(state, &ir_udf_wrapper));
        // TODO: don't do this for child exprs
        codegen->AddFunctionToJit(ir_udf_wrapper, &_scalar_fn_wrapper);
    }
#endif
    if (_fn.scalar_fn.__isset.prepare_fn_symbol) {
        RETURN_IF_ERROR(get_function(state, _fn.scalar_fn.prepare_fn_symbol,
                                     reinterpret_cast<void**>(&_prepare_fn)));
    }
    if (_fn.scalar_fn.__isset.close_fn_symbol) {
        RETURN_IF_ERROR(get_function(state, _fn.scalar_fn.close_fn_symbol,
                                     reinterpret_cast<void**>(&_close_fn)));
    }

    return status;
}

Status ScalarFnCall::open(RuntimeState* state, ExprContext* ctx,
                          FunctionContext::FunctionStateScope scope) {
    // Opens and inits children
    RETURN_IF_ERROR(Expr::open(state, ctx, scope));
    FunctionContext* fn_ctx = ctx->fn_context(_fn_context_index);
    if (_scalar_fn != NULL) {
        // We're in the interpreted path (i.e. no JIT). Populate our FunctionContext's
        // staging_input_vals, which will be reused across calls to _scalar_fn.
        DCHECK(_scalar_fn_wrapper == NULL);
        ObjectPool* obj_pool = state->obj_pool();
        std::vector<AnyVal*>* input_vals = fn_ctx->impl()->staging_input_vals();
        for (int i = 0; i < num_fixed_args(); ++i) {
            input_vals->push_back(create_any_val(obj_pool, _children[i]->type()));
        }
    }

    // Only evaluate constant arguments once per fragment
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        std::vector<AnyVal*> constant_args;
        for (int i = 0; i < _children.size(); ++i) {
            constant_args.push_back(_children[i]->get_const_val(ctx));
            // Check if any errors were set during the get_const_val() call
            Status child_status = _children[i]->get_fn_context_error(ctx);
            if (!child_status.ok()) {
                return child_status;
            }
        }
        fn_ctx->impl()->set_constant_args(constant_args);
    }

    if (_prepare_fn != NULL) {
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            _prepare_fn(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
            if (fn_ctx->has_error()) {
                return Status::InternalError(fn_ctx->error_msg());
            }
        }
        _prepare_fn(fn_ctx, FunctionContext::THREAD_LOCAL);
        if (fn_ctx->has_error()) {
            return Status::InternalError(fn_ctx->error_msg());
        }
    }

    // If we're calling MathFunctions::RoundUpTo(), we need to set _output_scale, which
    // determines how many decimal places are printed.
    // TODO: revisit this. We should be able to do this if the scale argument is
    // non-constant.
    if (_fn.name.function_name == "round" && _type.type == TYPE_DOUBLE) {
        DCHECK_EQ(_children.size(), 2);
        if (_children[1]->is_constant()) {
            IntVal scale_arg = _children[1]->get_int_val(ctx, NULL);
            _output_scale = scale_arg.val;
        }
    }

    return Status::OK();
}

void ScalarFnCall::close(RuntimeState* state, ExprContext* context,
                         FunctionContext::FunctionStateScope scope) {
    if (_fn_context_index != -1 && _close_fn != NULL) {
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
        _close_fn(fn_ctx, FunctionContext::THREAD_LOCAL);
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            _close_fn(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
        }
    }
    Expr::close(state, context, scope);
}

bool ScalarFnCall::is_constant() const {
    if (_fn.name.function_name == "rand") {
        return false;
    }
    return Expr::is_constant();
}

Status ScalarFnCall::get_function(RuntimeState* state, const std::string& symbol, void** fn) {
    if (_fn.binary_type == TFunctionBinaryType::NATIVE ||
        _fn.binary_type == TFunctionBinaryType::BUILTIN ||
        _fn.binary_type == TFunctionBinaryType::HIVE) {
        return UserFunctionCache::instance()->get_function_ptr(_fn.id, symbol, _fn.hdfs_location,
                                                               _fn.checksum, fn, &_cache_entry);
    } else {
#if 0
        DCHECK_EQ(_fn.binary_type, TFunctionBinaryType::IR);
        LlvmCodeGen* codegen;
        RETURN_IF_ERROR(state->GetCodegen(&codegen));
        Function* ir_fn = codegen->module()->getFunction(symbol);
        if (ir_fn == NULL) {
            std::stringstream ss;
            ss << "Unable to locate function " << symbol
                << " from LLVM module " << _fn.hdfs_location;
            return Status::InternalError(ss.str());
        }
        codegen->AddFunctionToJit(ir_fn, fn);
        return Status::OK()();
#endif
    }
    return Status::OK();
}

void ScalarFnCall::evaluate_children(ExprContext* context, TupleRow* row,
                                     std::vector<AnyVal*>* input_vals) {
    DCHECK_EQ(input_vals->size(), num_fixed_args());
    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
    uint8_t* varargs_buffer = fn_ctx->impl()->varargs_buffer();
    for (int i = 0; i < _children.size(); ++i) {
        void* src_slot = context->get_value(_children[i], row);
        AnyVal* dst_val = NULL;
        if (_vararg_start_idx == -1 || i < _vararg_start_idx) {
            dst_val = (*input_vals)[i];
        } else {
            dst_val = reinterpret_cast<AnyVal*>(varargs_buffer);
            varargs_buffer += AnyValUtil::any_val_size(_children[i]->type());
        }
        AnyValUtil::set_any_val(src_slot, _children[i]->type(), dst_val);
    }
}

template <typename RETURN_TYPE>
RETURN_TYPE ScalarFnCall::interpret_eval(ExprContext* context, TupleRow* row) {
    DCHECK(_scalar_fn != NULL);
    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
    std::vector<AnyVal*>* input_vals = fn_ctx->impl()->staging_input_vals();

    evaluate_children(context, row, input_vals);

    if (_vararg_start_idx == -1) {
        switch (_children.size()) {
        case 0:
            typedef RETURN_TYPE (*ScalarFn0)(FunctionContext*);
            return reinterpret_cast<ScalarFn0>(_scalar_fn)(fn_ctx);
        case 1:
            typedef RETURN_TYPE (*ScalarFn1)(FunctionContext*, const AnyVal& a1);
            return reinterpret_cast<ScalarFn1>(_scalar_fn)(fn_ctx, *(*input_vals)[0]);
        case 2:
            typedef RETURN_TYPE (*ScalarFn2)(FunctionContext*, const AnyVal& a1, const AnyVal& a2);
            return reinterpret_cast<ScalarFn2>(_scalar_fn)(fn_ctx, *(*input_vals)[0],
                                                           *(*input_vals)[1]);
        case 3:
            typedef RETURN_TYPE (*ScalarFn3)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             const AnyVal& a3);
            return reinterpret_cast<ScalarFn3>(_scalar_fn)(fn_ctx, *(*input_vals)[0],
                                                           *(*input_vals)[1], *(*input_vals)[2]);
        case 4:
            typedef RETURN_TYPE (*ScalarFn4)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             const AnyVal& a3, const AnyVal& a4);
            return reinterpret_cast<ScalarFn4>(_scalar_fn)(fn_ctx, *(*input_vals)[0],
                                                           *(*input_vals)[1], *(*input_vals)[2],
                                                           *(*input_vals)[3]);
        case 5:
            typedef RETURN_TYPE (*ScalarFn5)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             const AnyVal& a3, const AnyVal& a4, const AnyVal& a5);
            return reinterpret_cast<ScalarFn5>(_scalar_fn)(fn_ctx, *(*input_vals)[0],
                                                           *(*input_vals)[1], *(*input_vals)[2],
                                                           *(*input_vals)[3], *(*input_vals)[4]);
        case 6:
            typedef RETURN_TYPE (*ScalarFn6)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                                             const AnyVal& a6);
            return reinterpret_cast<ScalarFn6>(_scalar_fn)(
                    fn_ctx, *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2],
                    *(*input_vals)[3], *(*input_vals)[4], *(*input_vals)[5]);
        case 7:
            typedef RETURN_TYPE (*ScalarFn7)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                                             const AnyVal& a6, const AnyVal& a7);
            return reinterpret_cast<ScalarFn7>(_scalar_fn)(
                    fn_ctx, *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2],
                    *(*input_vals)[3], *(*input_vals)[4], *(*input_vals)[5], *(*input_vals)[6]);
        case 8:
            typedef RETURN_TYPE (*ScalarFn8)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                                             const AnyVal& a6, const AnyVal& a7, const AnyVal& a8);
            return reinterpret_cast<ScalarFn8>(_scalar_fn)(
                    fn_ctx, *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2],
                    *(*input_vals)[3], *(*input_vals)[4], *(*input_vals)[5], *(*input_vals)[6],
                    *(*input_vals)[7]);
        default:
            DCHECK(false) << "Interpreted path not implemented. We should have "
                          << "codegen'd the wrapper";
        }
    } else {
        int num_varargs = _children.size() - num_fixed_args();
        const AnyVal* varargs = reinterpret_cast<AnyVal*>(fn_ctx->impl()->varargs_buffer());
        switch (num_fixed_args()) {
        case 0:
            typedef RETURN_TYPE (*VarargFn0)(FunctionContext*, int num_varargs,
                                             const AnyVal* varargs);
            return reinterpret_cast<VarargFn0>(_scalar_fn)(fn_ctx, num_varargs, varargs);
        case 1:
            typedef RETURN_TYPE (*VarargFn1)(FunctionContext*, const AnyVal& a1, int num_varargs,
                                             const AnyVal* varargs);
            return reinterpret_cast<VarargFn1>(_scalar_fn)(fn_ctx, *(*input_vals)[0], num_varargs,
                                                           varargs);
        case 2:
            typedef RETURN_TYPE (*VarargFn2)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             int num_varargs, const AnyVal* varargs);
            return reinterpret_cast<VarargFn2>(_scalar_fn)(fn_ctx, *(*input_vals)[0],
                                                           *(*input_vals)[1], num_varargs, varargs);
        case 3:
            typedef RETURN_TYPE (*VarargFn3)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             const AnyVal& a3, int num_varargs,
                                             const AnyVal* varargs);
            return reinterpret_cast<VarargFn3>(_scalar_fn)(fn_ctx, *(*input_vals)[0],
                                                           *(*input_vals)[1], *(*input_vals)[2],
                                                           num_varargs, varargs);
        case 4:
            typedef RETURN_TYPE (*VarargFn4)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             const AnyVal& a3, const AnyVal& a4, int num_varargs,
                                             const AnyVal* varargs);
            return reinterpret_cast<VarargFn4>(_scalar_fn)(fn_ctx, *(*input_vals)[0],
                                                           *(*input_vals)[1], *(*input_vals)[2],
                                                           *(*input_vals)[3], num_varargs, varargs);
        case 5:
            typedef RETURN_TYPE (*VarargFn5)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                                             int num_varargs, const AnyVal* varargs);
            return reinterpret_cast<VarargFn5>(_scalar_fn)(
                    fn_ctx, *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2],
                    *(*input_vals)[3], *(*input_vals)[4], num_varargs, varargs);
        case 6:
            typedef RETURN_TYPE (*VarargFn6)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                                             const AnyVal& a6, int num_varargs,
                                             const AnyVal* varargs);
            return reinterpret_cast<VarargFn6>(_scalar_fn)(
                    fn_ctx, *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2],
                    *(*input_vals)[3], *(*input_vals)[4], *(*input_vals)[5], num_varargs, varargs);
        case 7:
            typedef RETURN_TYPE (*VarargFn7)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                                             const AnyVal& a6, const AnyVal& a7, int num_varargs,
                                             const AnyVal* varargs);
            return reinterpret_cast<VarargFn7>(_scalar_fn)(
                    fn_ctx, *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2],
                    *(*input_vals)[3], *(*input_vals)[4], *(*input_vals)[5], *(*input_vals)[6],
                    num_varargs, varargs);
        case 8:
            typedef RETURN_TYPE (*VarargFn8)(FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                                             const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                                             const AnyVal& a6, const AnyVal& a7, const AnyVal& a8,
                                             int num_varargs, const AnyVal* varargs);
            return reinterpret_cast<VarargFn8>(_scalar_fn)(
                    fn_ctx, *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2],
                    *(*input_vals)[3], *(*input_vals)[4], *(*input_vals)[5], *(*input_vals)[6],
                    *(*input_vals)[7], num_varargs, varargs);
        default:
            DCHECK(false) << "Interpreted path not implemented. We should have "
                          << "codegen'd the wrapper";
        }
    }
    return RETURN_TYPE::null();
}

typedef BooleanVal (*BooleanWrapper)(ExprContext*, TupleRow*);
typedef TinyIntVal (*TinyIntWrapper)(ExprContext*, TupleRow*);
typedef SmallIntVal (*SmallIntWrapper)(ExprContext*, TupleRow*);
typedef IntVal (*IntWrapper)(ExprContext*, TupleRow*);
typedef BigIntVal (*BigIntWrapper)(ExprContext*, TupleRow*);
typedef LargeIntVal (*LargeIntWrapper)(ExprContext*, TupleRow*);
typedef FloatVal (*FloatWrapper)(ExprContext*, TupleRow*);
typedef DoubleVal (*DoubleWrapper)(ExprContext*, TupleRow*);
typedef StringVal (*StringWrapper)(ExprContext*, TupleRow*);
typedef DateTimeVal (*DatetimeWrapper)(ExprContext*, TupleRow*);
typedef DecimalV2Val (*DecimalV2Wrapper)(ExprContext*, TupleRow*);

// TODO: macroify this?
BooleanVal ScalarFnCall::get_boolean_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_BOOLEAN);
    DCHECK(context != NULL);
    if (_scalar_fn_wrapper == NULL) {
        return interpret_eval<BooleanVal>(context, row);
    }
    BooleanWrapper fn = reinterpret_cast<BooleanWrapper>(_scalar_fn_wrapper);
    return fn(context, row);
}

TinyIntVal ScalarFnCall::get_tiny_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_TINYINT);
    DCHECK(context != NULL);
    if (_scalar_fn_wrapper == NULL) {
        return interpret_eval<TinyIntVal>(context, row);
    }
    TinyIntWrapper fn = reinterpret_cast<TinyIntWrapper>(_scalar_fn_wrapper);
    return fn(context, row);
}

SmallIntVal ScalarFnCall::get_small_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_SMALLINT);
    DCHECK(context != NULL);
    if (_scalar_fn_wrapper == NULL) {
        return interpret_eval<SmallIntVal>(context, row);
    }
    SmallIntWrapper fn = reinterpret_cast<SmallIntWrapper>(_scalar_fn_wrapper);
    return fn(context, row);
}

IntVal ScalarFnCall::get_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_INT);
    DCHECK(context != NULL);
    if (_scalar_fn_wrapper == NULL) {
        return interpret_eval<IntVal>(context, row);
    }
    IntWrapper fn = reinterpret_cast<IntWrapper>(_scalar_fn_wrapper);
    return fn(context, row);
}

BigIntVal ScalarFnCall::get_big_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_BIGINT);
    DCHECK(context != NULL);
    if (_scalar_fn_wrapper == NULL) {
        return interpret_eval<BigIntVal>(context, row);
    }
    BigIntWrapper fn = reinterpret_cast<BigIntWrapper>(_scalar_fn_wrapper);
    return fn(context, row);
}

LargeIntVal ScalarFnCall::get_large_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_LARGEINT);
    DCHECK(context != NULL);
    if (_scalar_fn_wrapper == NULL) {
        return interpret_eval<LargeIntVal>(context, row);
    }
    LargeIntWrapper fn = reinterpret_cast<LargeIntWrapper>(_scalar_fn_wrapper);
    return fn(context, row);
}

FloatVal ScalarFnCall::get_float_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_FLOAT);
    DCHECK(context != NULL);
    if (_scalar_fn_wrapper == NULL) {
        return interpret_eval<FloatVal>(context, row);
    }
    FloatWrapper fn = reinterpret_cast<FloatWrapper>(_scalar_fn_wrapper);
    return fn(context, row);
}

DoubleVal ScalarFnCall::get_double_val(ExprContext* context, TupleRow* row) {
    DCHECK(_type.type == TYPE_DOUBLE || _type.type == TYPE_TIME);
    DCHECK(context != NULL);
    if (_scalar_fn_wrapper == NULL) {
        return interpret_eval<DoubleVal>(context, row);
    }

    DoubleWrapper fn = reinterpret_cast<DoubleWrapper>(_scalar_fn_wrapper);
    return fn(context, row);
}

StringVal ScalarFnCall::get_string_val(ExprContext* context, TupleRow* row) {
    DCHECK(_type.is_string_type());
    DCHECK(context != NULL);
    if (_scalar_fn_wrapper == NULL) {
        return interpret_eval<StringVal>(context, row);
    }
    StringWrapper fn = reinterpret_cast<StringWrapper>(_scalar_fn_wrapper);
    return fn(context, row);
}

DateTimeVal ScalarFnCall::get_datetime_val(ExprContext* context, TupleRow* row) {
    DCHECK(_type.is_date_type());
    DCHECK(context != NULL);
    if (_scalar_fn_wrapper == NULL) {
        return interpret_eval<DateTimeVal>(context, row);
    }
    DatetimeWrapper fn = reinterpret_cast<DatetimeWrapper>(_scalar_fn_wrapper);
    return fn(context, row);
}

DecimalV2Val ScalarFnCall::get_decimalv2_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_DECIMALV2);
    DCHECK(context != NULL);
    if (_scalar_fn_wrapper == NULL) {
        return interpret_eval<DecimalV2Val>(context, row);
    }
    DecimalV2Wrapper fn = reinterpret_cast<DecimalV2Wrapper>(_scalar_fn_wrapper);
    return fn(context, row);
}

std::string ScalarFnCall::debug_string() const {
    std::stringstream out;
    out << "ScalarFnCall(udf_type=" << _fn.binary_type << " location=" << _fn.hdfs_location
        << " symbol_name=" << _fn.scalar_fn.symbol << Expr::debug_string() << ")";
    return out.str();
}
} // namespace doris
