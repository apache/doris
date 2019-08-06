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
//#include <llvm/IR/Attributes.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>

#include "codegen/codegen_anyval.h"
#include "codegen/llvm_codegen.h"
#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "runtime/user_function_cache.h"
#include "runtime/runtime_state.h"
#include "udf/udf_internal.h"
#include "util/debug_util.h"
#include "util/symbols_util.h"

using llvm::Value;
using llvm::Function;
using llvm::FunctionType;
using llvm::BasicBlock;
using llvm::Type;
namespace doris {

ScalarFnCall::ScalarFnCall(const TExprNode& node) : 
        Expr(node),
        _vararg_start_idx(node.__isset.vararg_start_idx ?  node.vararg_start_idx : -1),
        _scalar_fn_wrapper(NULL),
        _prepare_fn(NULL),
        _close_fn(NULL),
        _scalar_fn(NULL) {
    DCHECK_NE(_fn.binary_type, TFunctionBinaryType::HIVE);
}

ScalarFnCall::~ScalarFnCall() {
}

Status ScalarFnCall::prepare(
        RuntimeState* state, const RowDescriptor& desc,
        ExprContext* context) {
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

    _fn_context_index = context->register_func(
            state, return_type, arg_types, varargs_buffer_size);
    // _scalar_fn = OpcodeRegistry::instance()->get_function_ptr(_opcode);
    Status status = Status::OK();
    if (_scalar_fn == NULL) {
        if (SymbolsUtil::is_mangled(_fn.scalar_fn.symbol)) {
            status = UserFunctionCache::instance()->get_function_ptr(
                _fn.id, _fn.scalar_fn.symbol, _fn.hdfs_location, _fn.checksum, &_scalar_fn, &_cache_entry);
        } else {
            std::vector<TypeDescriptor> arg_types;
            for (auto& t_type : _fn.arg_types) {
                arg_types.push_back(TypeDescriptor::from_thrift(t_type));
            }
            // ColumnType ret_type(INVALID_TYPE);
            // ret_type = ColumnType(thrift_to_type(_fn.ret_type));
            std::string symbol = SymbolsUtil::mangle_user_function(
                _fn.scalar_fn.symbol, arg_types, _fn.has_var_args, NULL);
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
                return Status::InternalError(Substitute("Problem loading UDF '$0':\n$1",
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

Status ScalarFnCall::open(
        RuntimeState* state, ExprContext* ctx, FunctionContext::FunctionStateScope scope) {
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

void ScalarFnCall::close(
        RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
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

// Dynamically loads the pre-compiled UDF and codegens a function that calls each child's
// codegen'd function, then passes those values to the UDF and returns the result.
// Example generated IR for a UDF with signature
//    create function Udf(double, int...) returns double
//    select Udf(1.0, 2, 3, 4, 5)
// define { i8, double } @UdfWrapper(i8* %context, %"class.impala::TupleRow"* %row) {
// entry:
//   %arg_val = call { i8, double }
//      @ExprWrapper(i8* %context, %"class.impala::TupleRow"* %row)
//   %arg_ptr = alloca { i8, double }
//   store { i8, double } %arg_val, { i8, double }* %arg_ptr
//   %arg_val1 = call i64 @ExprWrapper1(i8* %context, %"class.impala::TupleRow"* %row)
//   store i64 %arg_val1, i64* inttoptr (i64 89111072 to i64*)
//   %arg_val2 = call i64 @ExprWrapper2(i8* %context, %"class.impala::TupleRow"* %row)
//   store i64 %arg_val2, i64* inttoptr (i64 89111080 to i64*)
//   %arg_val3 = call i64 @ExprWrapper3(i8* %context, %"class.impala::TupleRow"* %row)
//   store i64 %arg_val3, i64* inttoptr (i64 89111088 to i64*)
//   %arg_val4 = call i64 @ExprWrapper4(i8* %context, %"class.impala::TupleRow"* %row)
//   store i64 %arg_val4, i64* inttoptr (i64 89111096 to i64*)
//   %result = call { i8, double }
//      @_Z14VarSumMultiplyPN10impala_udf15FunctionContextERKNS_9DoubleValEiPKNS_6IntValE(
//        %"class.doris_udf::FunctionContext"* inttoptr
//            (i64 37522464 to %"class.doris_udf::FunctionContext"*),
//        {i8, double }* %arg_ptr,
//        i32 4,
//        i64* inttoptr (i64 89111072 to i64*))
//   ret { i8, double } %result
Status ScalarFnCall::get_codegend_compute_fn(RuntimeState* state, Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }
    for (int i = 0; i < get_num_children(); ++i) {
        if (_children[i]->type().type == TYPE_CHAR) {
            *fn = NULL;
            return Status::InternalError("ScalarFnCall Codegen not supported for CHAR");
        }
    }

    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));

    Function* udf = NULL;
    RETURN_IF_ERROR(get_udf(state, &udf));

    // Create wrapper that computes args and calls UDF
    std::stringstream fn_name;
    fn_name << udf->getName().str() << "_wrapper";

    Value* args[2];
    *fn = create_ir_function_prototype(codegen, fn_name.str(), &args);
    Value* expr_ctx = args[0];
    Value* row = args[1];
    BasicBlock* block = BasicBlock::Create(codegen->context(), "entry", *fn);
    LlvmCodeGen::LlvmBuilder builder(block);

    // Populate UDF arguments
    std::vector<Value*> udf_args;

    // First argument is always FunctionContext*.
    // Index into our registered offset in the ExprContext.
    Value* expr_ctx_gep = builder.CreateStructGEP(expr_ctx, 1, "expr_ctx_gep");
    Value* fn_ctxs_base = builder.CreateLoad(expr_ctx_gep, "fn_ctxs_base");
    // Use GEP to add our index to the base pointer
    Value* fn_ctx_ptr =
        builder.CreateConstGEP1_32(fn_ctxs_base, _fn_context_index, "fn_ctx_ptr");
    Value* fn_ctx = builder.CreateLoad(fn_ctx_ptr, "fn_ctx");
    udf_args.push_back(fn_ctx);

    // Get IR i8* pointer to varargs buffer from FunctionContext* argument
    // (if there are varargs)
    Value* varargs_buffer = NULL;
    if (_vararg_start_idx != -1) {
        // FunctionContextImpl is first field of FunctionContext
        // fn_ctx_impl_ptr has type FunctionContextImpl**
        Value* fn_ctx_impl_ptr = builder.CreateStructGEP(fn_ctx, 0, "fn_ctx_impl_ptr");
        Value* fn_ctx_impl = builder.CreateLoad(fn_ctx_impl_ptr, "fn_ctx_impl");
        // varargs_buffer is first field of FunctionContextImpl
        // varargs_buffer_ptr has type i8**
        Value* varargs_buffer_ptr =
            builder.CreateStructGEP(fn_ctx_impl, 0, "varargs_buffer");
        varargs_buffer = builder.CreateLoad(varargs_buffer_ptr);
    }
    // Tracks where to write the next vararg to
    int varargs_buffer_offset = 0;

    // Call children to populate remaining arguments
    for (int i = 0; i < get_num_children(); ++i) {
        Function* child_fn = NULL;
        std::vector<Value*> child_fn_args;
        if (state->codegen_level() > 0) {
            // Set 'child_fn' to the codegen'd function, sets child_fn = NULL if codegen fails
            _children[i]->get_codegend_compute_fn(state, &child_fn);
        }
        if (child_fn == NULL) {
            // Set 'child_fn' to the interpreted function
            child_fn = get_static_get_val_wrapper(_children[i]->type(), codegen);
            // First argument to interpreted function is _children[i]
            Type* expr_ptr_type = codegen->get_ptr_type(Expr::_s_llvm_class_name);
            child_fn_args.push_back(codegen->cast_ptr_to_llvm_ptr(expr_ptr_type, _children[i]));
        }
        child_fn_args.push_back(expr_ctx);
        child_fn_args.push_back(row);

        // Call 'child_fn', adding the result to either 'udf_args' or 'varargs_buffer'
        DCHECK(child_fn != NULL);
        Type* arg_type = CodegenAnyVal::get_unlowered_type(codegen, _children[i]->type());
        Value* arg_val_ptr = NULL;
        if (_vararg_start_idx == -1 || i < _vararg_start_idx) {
            // Either no varargs or arguments before varargs begin. Allocate space to store
            // 'child_fn's result so we can pass the pointer to the UDF.
            arg_val_ptr = codegen->create_entry_block_alloca(builder, arg_type, "arg_val_ptr");

            if (_children[i]->type().type == TYPE_DECIMAL) {
                // UDFs may manipulate DecimalVal arguments via SIMD instructions such as 'movaps'
                // that require 16-byte memory alignment. LLVM uses 8-byte alignment by default,
                // so explicitly set the alignment for DecimalVals.
                llvm::cast<llvm::AllocaInst>(arg_val_ptr)->setAlignment(16);
            }
            udf_args.push_back(arg_val_ptr);
        } else {
            // Store the result of 'child_fn' in varargs_buffer + varargs_buffer_offset
            arg_val_ptr =
                builder.CreateConstGEP1_32(varargs_buffer, varargs_buffer_offset, "arg_val_ptr");
            varargs_buffer_offset += AnyValUtil::any_val_size(_children[i]->type());
            // Cast arg_val_ptr from i8* to AnyVal pointer type
            arg_val_ptr =
                builder.CreateBitCast(arg_val_ptr, arg_type->getPointerTo(), "arg_val_ptr");
        }
        DCHECK_EQ(arg_val_ptr->getType(), arg_type->getPointerTo());
        // The result of the call must be stored in a lowered AnyVal
        Value* lowered_arg_val_ptr = builder.CreateBitCast(
            arg_val_ptr, CodegenAnyVal::get_lowered_ptr_type(codegen, _children[i]->type()),
            "lowered_arg_val_ptr");
        CodegenAnyVal::create_call(
            codegen, &builder, child_fn, child_fn_args, "arg_val", lowered_arg_val_ptr);
    }

    if (_vararg_start_idx != -1) {
        // We've added the FunctionContext argument plus any non-variadic arguments
        DCHECK_EQ(udf_args.size(), _vararg_start_idx + 1);
        DCHECK_GE(get_num_children(), 1);
        // Add the number of varargs
        udf_args.push_back(codegen->get_int_constant(
                TYPE_INT, get_num_children() - _vararg_start_idx));
        // Add all the accumulated vararg inputs as one input argument.
        llvm::PointerType* vararg_type = codegen->get_ptr_type(
            CodegenAnyVal::get_unlowered_type(codegen, _children.back()->type()));
        udf_args.push_back(builder.CreateBitCast(varargs_buffer, vararg_type, "varargs"));
    }

    // Call UDF
    Value* result_val =
        CodegenAnyVal::create_call(codegen, &builder, udf, udf_args, "result", NULL);
    builder.CreateRet(result_val);

    *fn = codegen->finalize_function(*fn);
    DCHECK(*fn != NULL);
    _ir_compute_fn = *fn;
    return Status::OK();
}

Status ScalarFnCall::get_udf(RuntimeState* state, Function** udf) {
    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));

    // from_utc_timestamp and to_utc_timestamp have inline ASM that cannot be JIT'd.
    // DatetimeFunctions::AddSub() contains a try/catch which doesn't work in JIT'd
    // code.  Always use the statically compiled versions of these functions so the
    // xcompiled versions are not included in the final module to be JIT'd.
    // TODO: fix this
    bool broken_builtin = _fn.name.function_name == "from_utc_timestamp" ||
        _fn.name.function_name == "to_utc_timestamp" ||
        _fn.scalar_fn.symbol.find("add_sub") != std::string::npos;
    if (_fn.binary_type == TFunctionBinaryType::NATIVE 
            || (_fn.binary_type == TFunctionBinaryType::BUILTIN 
                && (!(state->codegen_level() > 0) || broken_builtin))) {
        // In this path, we are code that has been statically compiled to assembly.
        // This can either be a UDF implemented in a .so or a builtin using the UDF
        // interface with the code in impalad.
        void* fn_ptr = NULL;
        Status status = UserFunctionCache::instance()->get_function_ptr(
            _fn.id, _fn.scalar_fn.symbol, _fn.hdfs_location, _fn.checksum, &fn_ptr, &_cache_entry);
        if (!status.ok() && _fn.binary_type == TFunctionBinaryType::BUILTIN) {
            // Builtins symbols should exist unless there is a version mismatch.
            // TODO(zc )
            // status.add_detail(ErrorMsg(TErrorCode::MISSING_BUILTIN,
                                      // _fn.name.function_name, _fn.scalar_fn.symbol).msg());
        }
        RETURN_IF_ERROR(status);
        DCHECK(fn_ptr != NULL);

        // Convert UDF function pointer to Function*
        // First generate the FunctionType* corresponding to the UDF.
        Type* return_type = CodegenAnyVal::get_lowered_type(codegen, type());
        std::vector<Type*> arg_types;

        if (type().type == TYPE_DECIMAL || type().type == TYPE_DECIMALV2) {
            // Per the x64 ABI, DecimalVals are returned via a DecmialVal* output argument
            return_type = codegen->void_type();
            arg_types.push_back(
                codegen->get_ptr_type(CodegenAnyVal::get_unlowered_type(codegen, type())));
        }

        arg_types.push_back(codegen->get_ptr_type("class.doris_udf::FunctionContext"));
        for (int i = 0; i < num_fixed_args(); ++i) {
            Type* arg_type = codegen->get_ptr_type(
                CodegenAnyVal::get_unlowered_type(codegen, _children[i]->type()));
            arg_types.push_back(arg_type);
        }

        if (_vararg_start_idx >= 0) {
            Type* vararg_type = CodegenAnyVal::get_unlowered_ptr_type(
                codegen, _children[_vararg_start_idx]->type());
            arg_types.push_back(codegen->get_type(TYPE_INT));
            arg_types.push_back(vararg_type);
        }
        FunctionType* udf_type = FunctionType::get(return_type, arg_types, false);

        // Create a Function* with the generated type. This is only a function
        // declaration, not a definition, since we do not create any basic blocks or
        // instructions in it.
        *udf = Function::Create(
            udf_type, llvm::GlobalValue::ExternalLinkage,
            _fn.scalar_fn.symbol, codegen->module());

        // Associate the dynamically loaded function pointer with the Function* we
        // defined. This tells LLVM where the compiled function definition is located in
        // memory.
        codegen->execution_engine()->addGlobalMapping(*udf, fn_ptr);
    } else if (_fn.binary_type == TFunctionBinaryType::BUILTIN) {
        // In this path, we're running a builtin with the UDF interface. The IR is
        // in the llvm module.
        DCHECK(state->codegen_level() > 0);
        // TODO(zc)
        std::string symbol = _fn.scalar_fn.symbol;
#if 0  
        *udf = codegen->module()->getFunction(_fn.scalar_fn.symbol);
        if (*udf == NULL) {
            // Builtins symbols should exist unless there is a version mismatch.
            std::stringstream ss;
            ss << "Builtin '" << _fn.name.function_name << "' with symbol '"
                << _fn.scalar_fn.symbol << "' does not exist. "
                << "Verify that all your impalads are the same version.";
            return Status::InternalError(ss.str());
        }
#else 
        if (!SymbolsUtil::is_mangled(symbol)) {
            std::vector<TypeDescriptor> arg_types;
            for (auto& t_type : _fn.arg_types) {
                arg_types.push_back(TypeDescriptor::from_thrift(t_type));
            }
            // ColumnType ret_type(INVALID_TYPE);
            // ret_type = ColumnType(thrift_to_type(_fn.ret_type));
            symbol = SymbolsUtil::mangle_user_function(symbol, arg_types, _fn.has_var_args, NULL);
        }
#endif
        *udf = codegen->module()->getFunction(symbol);
        if (*udf == NULL) {
            // Builtins symbols should exist unless there is a version mismatch.
            std::stringstream ss;
            ss << "Builtin '" << _fn.name.function_name << "' with symbol '"
                << symbol << "' does not exist. "
                << "Verify that all your impalads are the same version.";
            return Status::InternalError(ss.str());
        }
        // Builtin functions may use Expr::GetConstant(). Clone the function in case we need
        // to use it again, and rename it to something more manageable than the mangled name.
        std::string demangled_name = SymbolsUtil::demangle_no_args((*udf)->getName().str());
        *udf = codegen->clone_function(*udf);
        (*udf)->setName(demangled_name);
        inline_constants(codegen, *udf);
        *udf = codegen->finalize_function(*udf);
        DCHECK(*udf != NULL);
    } else {
        // We're running an IR UDF.
        DCHECK_EQ(_fn.binary_type, TFunctionBinaryType::IR);
        *udf = codegen->module()->getFunction(_fn.scalar_fn.symbol);
        if (*udf == NULL) {
            std::stringstream ss;
            ss << "Unable to locate function " << _fn.scalar_fn.symbol
                << " from LLVM module " << _fn.hdfs_location;
            return Status::InternalError(ss.str());
        }
        *udf = codegen->finalize_function(*udf);
        if (*udf == NULL) {
            return Status::InternalError("udf verify failed");
            // TODO(zc)
            // TErrorCode::UDF_VERIFY_FAILED, _fn.scalar_fn.symbol, _fn.hdfs_location);
        }
    }
    return Status::OK();
}

Status ScalarFnCall::get_function(RuntimeState* state, const std::string& symbol, void** fn) {
    if (_fn.binary_type == TFunctionBinaryType::NATIVE 
            || _fn.binary_type == TFunctionBinaryType::BUILTIN) {
        return UserFunctionCache::instance()->get_function_ptr(
            _fn.id, symbol, _fn.hdfs_location, _fn.checksum, fn, &_cache_entry);
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

void ScalarFnCall::evaluate_children(
        ExprContext* context, TupleRow* row, std::vector<AnyVal*>* input_vals) {
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

template<typename RETURN_TYPE>
RETURN_TYPE ScalarFnCall::interpret_eval(ExprContext* context, TupleRow* row) {
    DCHECK(_scalar_fn != NULL);
    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
    std::vector<AnyVal*>* input_vals = fn_ctx->impl()->staging_input_vals();
    
    evaluate_children(context, row, input_vals);

    if (_vararg_start_idx == -1) {
        switch (_children.size()) {
        case 0:
            typedef RETURN_TYPE(*ScalarFn0)(FunctionContext*);
            return reinterpret_cast<ScalarFn0>(_scalar_fn)(fn_ctx);
        case 1:
            typedef RETURN_TYPE(*ScalarFn1)(FunctionContext*, const AnyVal& a1);
            return reinterpret_cast<ScalarFn1>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0]);
        case 2:
            typedef RETURN_TYPE(*ScalarFn2)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2);
            return reinterpret_cast<ScalarFn2>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1]);
        case 3:
            typedef RETURN_TYPE(*ScalarFn3)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2, const AnyVal& a3);
            return reinterpret_cast<ScalarFn3>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1], *(*input_vals)[2]);
        case 4:
            typedef RETURN_TYPE(*ScalarFn4)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2, 
                const AnyVal& a3, const AnyVal& a4);
            return reinterpret_cast<ScalarFn4>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1], 
                *(*input_vals)[2], *(*input_vals)[3]);
        case 5:
            typedef RETURN_TYPE(*ScalarFn5)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2, 
                const AnyVal& a3, const AnyVal& a4, const AnyVal& a5);
            return reinterpret_cast<ScalarFn5>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1], 
                *(*input_vals)[2], *(*input_vals)[3], *(*input_vals)[4]);
        case 6:
            typedef RETURN_TYPE(*ScalarFn6)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2, 
                const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                const AnyVal& a6);
            return reinterpret_cast<ScalarFn6>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1],
                *(*input_vals)[2], *(*input_vals)[3], *(*input_vals)[4], 
                *(*input_vals)[5]);
        case 7:
            typedef RETURN_TYPE(*ScalarFn7)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2, 
                const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                const AnyVal& a6, const AnyVal& a7);
            return reinterpret_cast<ScalarFn7>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1],
                *(*input_vals)[2], *(*input_vals)[3], *(*input_vals)[4], 
                *(*input_vals)[5], *(*input_vals)[6]);
        case 8:
            typedef RETURN_TYPE(*ScalarFn8)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2, 
                const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                const AnyVal& a6, const AnyVal& a7, const AnyVal& a8);
            return reinterpret_cast<ScalarFn8>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1],
                *(*input_vals)[2], *(*input_vals)[3], *(*input_vals)[4], 
                *(*input_vals)[5], *(*input_vals)[6], *(*input_vals)[7]);
        default:
            DCHECK(false) << "Interpreted path not implemented. We should have "
                << "codegen'd the wrapper";
        }
    } else {
        int num_varargs = _children.size() - num_fixed_args();
        const AnyVal* varargs = reinterpret_cast<AnyVal*>(fn_ctx->impl()->varargs_buffer());
        switch (num_fixed_args()) {
        case 0:
            typedef RETURN_TYPE(*VarargFn0)(
                FunctionContext*, int num_varargs, const AnyVal* varargs);
            return reinterpret_cast<VarargFn0>(_scalar_fn)(fn_ctx, num_varargs, varargs);
        case 1:
            typedef RETURN_TYPE(*VarargFn1)(
                FunctionContext*, const AnyVal& a1, int num_varargs, const AnyVal* varargs);
            return reinterpret_cast<VarargFn1>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], num_varargs, varargs);
        case 2:
            typedef RETURN_TYPE(*VarargFn2)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2, 
                int num_varargs, const AnyVal* varargs);
            return reinterpret_cast<VarargFn2>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1], 
                num_varargs, varargs);
        case 3:
            typedef RETURN_TYPE(*VarargFn3)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2, 
                const AnyVal& a3, int num_varargs, const AnyVal* varargs);
            return reinterpret_cast<VarargFn3>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1], 
                *(*input_vals)[2], num_varargs, varargs);
        case 4:
            typedef RETURN_TYPE(*VarargFn4)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2, 
                const AnyVal& a3, const AnyVal& a4, int num_varargs,
                const AnyVal* varargs);
            return reinterpret_cast<VarargFn4>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1], 
                *(*input_vals)[2], *(*input_vals)[3], num_varargs,
                varargs);
        case 5:
            typedef RETURN_TYPE(*VarargFn5)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2, 
                const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                int num_varargs, const AnyVal* varargs);
            return reinterpret_cast<VarargFn5>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1],
                *(*input_vals)[2], *(*input_vals)[3], *(*input_vals)[4],
                num_varargs, varargs);
        case 6:
            typedef RETURN_TYPE(*VarargFn6)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                const AnyVal& a6, int num_varargs, const AnyVal* varargs);
            return reinterpret_cast<VarargFn6>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1],
                *(*input_vals)[2], *(*input_vals)[3], *(*input_vals)[4],
                *(*input_vals)[5], num_varargs, varargs);
        case 7:
            typedef RETURN_TYPE(*VarargFn7)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                const AnyVal& a6, const AnyVal& a7, int num_varargs, 
                const AnyVal* varargs);
            return reinterpret_cast<VarargFn7>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1],
                *(*input_vals)[2], *(*input_vals)[3], *(*input_vals)[4],
                *(*input_vals)[5], *(*input_vals)[6], num_varargs, 
                varargs);
        case 8:
            typedef RETURN_TYPE(*VarargFn8)(
                FunctionContext*, const AnyVal& a1, const AnyVal& a2,
                const AnyVal& a3, const AnyVal& a4, const AnyVal& a5,
                const AnyVal& a6, const AnyVal& a7, const AnyVal& a8, 
                int num_varargs, const AnyVal* varargs);
            return reinterpret_cast<VarargFn8>(_scalar_fn)(
                fn_ctx, *(*input_vals)[0], *(*input_vals)[1],
                *(*input_vals)[2], *(*input_vals)[3], *(*input_vals)[4],
                *(*input_vals)[5], *(*input_vals)[6], *(*input_vals)[7],
                num_varargs, varargs);
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
typedef DecimalVal (*DecimalWrapper)(ExprContext*, TupleRow*);
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

DecimalVal ScalarFnCall::get_decimal_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_DECIMAL);
    DCHECK(context != NULL);
    if (_scalar_fn_wrapper == NULL) {
        return interpret_eval<DecimalVal>(context, row);
    }
    DecimalWrapper fn = reinterpret_cast<DecimalWrapper>(_scalar_fn_wrapper);
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
    out << "ScalarFnCall(udf_type=" << _fn.binary_type
        << " location=" << _fn.hdfs_location
        << " symbol_name=" << _fn.scalar_fn.symbol << Expr::debug_string() << ")";
    return out.str();
}
}
