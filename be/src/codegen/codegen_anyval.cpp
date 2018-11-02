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

#include "codegen/codegen_anyval.h"

#include "runtime/multi_precision.h"

using llvm::Function;
using llvm::Type;
using llvm::Value;
using llvm::ConstantInt;
using llvm::Constant;

namespace doris {

const char* CodegenAnyVal::_s_llvm_booleanval_name   = "struct.doris_udf::BooleanVal";
const char* CodegenAnyVal::_s_llvm_tinyintval_name   = "struct.doris_udf::TinyIntVal";
const char* CodegenAnyVal::_s_llvm_smallintval_name  = "struct.doris_udf::SmallIntVal";
const char* CodegenAnyVal::_s_llvm_intval_name       = "struct.doris_udf::IntVal";
const char* CodegenAnyVal::_s_llvm_bigintval_name    = "struct.doris_udf::BigIntVal";
const char* CodegenAnyVal::_s_llvm_largeintval_name  = "struct.doris_udf::LargeIntVal";
const char* CodegenAnyVal::_s_llvm_floatval_name     = "struct.doris_udf::FloatVal";
const char* CodegenAnyVal::_s_llvm_doubleval_name    = "struct.doris_udf::DoubleVal";
const char* CodegenAnyVal::_s_llvm_stringval_name    = "struct.doris_udf::StringVal";
const char* CodegenAnyVal::_s_llvm_datetimeval_name = "struct.doris_udf::DateTimeVal";
const char* CodegenAnyVal::_s_llvm_decimalval_name   = "struct.doris_udf::DecimalVal";

Type* CodegenAnyVal::get_lowered_type(LlvmCodeGen* cg, const TypeDescriptor& type) {
    switch (type.type) {
    case TYPE_BOOLEAN: // i16
        return cg->smallint_type();
    case TYPE_TINYINT: // i16
        return cg->smallint_type();
    case TYPE_SMALLINT: // i32
        return cg->int_type();
    case TYPE_INT: // i64
        return cg->bigint_type();
    case TYPE_BIGINT: // { i8, i64 }
        return llvm::StructType::get(cg->tinyint_type(), cg->bigint_type(), NULL);
    case TYPE_LARGEINT: // %"struct.doris_udf::LargeIntVal" (isn't lowered)
        // = { {i8}, [15 x i8], i128 }
        return cg->get_type(_s_llvm_largeintval_name);
    case TYPE_FLOAT: // i64
        return cg->bigint_type();
    case TYPE_DOUBLE: // { i8, double }
        return llvm::StructType::get(cg->tinyint_type(), cg->double_type(), NULL);
    case TYPE_VARCHAR: // { i64, i8* }
    case TYPE_CHAR:
    case TYPE_HLL:
        return llvm::StructType::get(cg->bigint_type(), cg->ptr_type(), NULL);
    case TYPE_DATE:
    case TYPE_DATETIME: // %"struct.doris_udf::DateTimeVal" (isn't lowered) 
        // = { {i8}, i64, i32 }
        return cg->get_type(_s_llvm_datetimeval_name);
    case TYPE_DECIMAL: // %"struct.doris_udf::DecimalVal" (isn't lowered)
        // = { {i8}, i8, i8, i8, [9 x i32] }
        return cg->get_type(_s_llvm_decimalval_name);
    default:
        DCHECK(false) << "Unsupported type: " << type;
        return NULL;
    }
}

Type* CodegenAnyVal::get_lowered_ptr_type(LlvmCodeGen* cg, const TypeDescriptor& type) {
    return get_lowered_type(cg, type)->getPointerTo();
}

Type* CodegenAnyVal::get_unlowered_type(LlvmCodeGen* cg, const TypeDescriptor& type) {
    Type* result = NULL;
    switch (type.type) {
    case TYPE_BOOLEAN:
        result = cg->get_type(_s_llvm_booleanval_name);
        break;
    case TYPE_TINYINT:
        result = cg->get_type(_s_llvm_tinyintval_name);
        break;
    case TYPE_SMALLINT:
        result = cg->get_type(_s_llvm_smallintval_name);
        break;
    case TYPE_INT:
        result = cg->get_type(_s_llvm_intval_name);
        break;
    case TYPE_BIGINT:
        result = cg->get_type(_s_llvm_bigintval_name);
        break;
    case TYPE_LARGEINT:
        result = cg->get_type(_s_llvm_largeintval_name);
        break;
    case TYPE_FLOAT:
        result = cg->get_type(_s_llvm_floatval_name);
        break;
    case TYPE_DOUBLE:
        result = cg->get_type(_s_llvm_doubleval_name);
        break;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
        result = cg->get_type(_s_llvm_stringval_name);
        break;
    case TYPE_DATE:
    case TYPE_DATETIME:
        result = cg->get_type(_s_llvm_datetimeval_name);
        break;
    case TYPE_DECIMAL:
        result = cg->get_type(_s_llvm_decimalval_name);
        break;
    default:
        DCHECK(false) << "Unsupported type: " << type;
        return NULL;
    }
    DCHECK(result != NULL) << type;
    return result;
}

Type* CodegenAnyVal::get_unlowered_ptr_type(LlvmCodeGen* cg, const TypeDescriptor& type) {
    return get_unlowered_type(cg, type)->getPointerTo();
}

Value* CodegenAnyVal::create_call(
        LlvmCodeGen* cg, LlvmCodeGen::LlvmBuilder* builder, llvm::Function* fn,
        llvm::ArrayRef<Value*> args, const char* name, Value* result_ptr) {
    if (fn->getReturnType()->isVoidTy()) {
        // Void return type indicates that this function returns a DecimalVal via the first
        // argument (which should be a DecimalVal*).
        llvm::Function::arg_iterator ret_arg = fn->arg_begin();
        DCHECK(ret_arg->getType()->isPointerTy());
        Type* ret_type = ret_arg->getType()->getPointerElementType();

        // We need to pass a DecimalVal pointer to 'fn' that will be populated with the result
        // value. Use 'result_ptr' if specified, otherwise alloca one.
        Value* ret_ptr = (result_ptr == NULL) ?
            cg->create_entry_block_alloca(*builder, ret_type, name) : result_ptr;
        std::vector<Value*> new_args = args.vec();
        new_args.insert(new_args.begin(), ret_ptr);
        builder->CreateCall(fn, new_args);

        // If 'result_ptr' was specified, we're done. Otherwise load and return the result.
        if (result_ptr != NULL) {
            return NULL;
        }
        return builder->CreateLoad(ret_ptr, name);
    } else {
        // Function returns *Val normally (note that it could still be returning a DecimalVal,
        // since we generate non-complaint functions)
        Value* ret = builder->CreateCall(fn, args, name);
        if (result_ptr == NULL) {
            return ret;
        }
        builder->CreateStore(ret, result_ptr);
        return NULL;
    }
}

CodegenAnyVal CodegenAnyVal::create_call_wrapped(
        LlvmCodeGen* cg, LlvmCodeGen::LlvmBuilder* builder, const TypeDescriptor& type,
        llvm::Function* fn, llvm::ArrayRef<Value*> args, 
        const char* name, Value* result_ptr) {
    Value* v = create_call(cg, builder, fn, args, name, result_ptr);
    return CodegenAnyVal(cg, builder, type, v, name);
}

CodegenAnyVal::CodegenAnyVal(
        LlvmCodeGen* codegen, LlvmCodeGen::LlvmBuilder* builder,
        const TypeDescriptor& type, Value* value, const char* name) : 
            _type(type),
            _value(value),
            _name(name),
            _codegen(codegen),
            _builder(builder) {
    Type* value_type = get_lowered_type(codegen, type);
    if (_value == NULL) {
        // No Value* was specified, so allocate one on the stack and load it.
        Value* ptr = _codegen->create_entry_block_alloca(*builder, value_type, "");
        _value = _builder->CreateLoad(ptr, _name);
    }
    DCHECK_EQ(_value->getType(), value_type);
}

Value* CodegenAnyVal::get_is_null(const char* name) {
    switch (_type.type) {
    case TYPE_BIGINT:
    case TYPE_DOUBLE: {
        // Lowered type is of form { i8, * }. Get the i8 value.
        Value* is_null_i8 = _builder->CreateExtractValue(_value, 0);
        DCHECK(is_null_i8->getType() == _codegen->tinyint_type());
        return _builder->CreateTrunc(is_null_i8, _codegen->boolean_type(), name);
    }
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_LARGEINT:
    case TYPE_DECIMAL: {
        // Lowered type is of the form { {i8}, ... }
        uint32_t idxs[] = {0, 0};
        Value* is_null_i8 = _builder->CreateExtractValue(_value, idxs);
        DCHECK(is_null_i8->getType() == _codegen->tinyint_type());
        return _builder->CreateTrunc(is_null_i8, _codegen->boolean_type(), name);
    }
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_CHAR: {
        // Lowered type is of form { i64, *}. Get the first byte of the i64 value.
        Value* v = _builder->CreateExtractValue(_value, 0);
        DCHECK(v->getType() == _codegen->bigint_type());
        return _builder->CreateTrunc(v, _codegen->boolean_type(), name);
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_FLOAT:
        // Lowered type is an integer. Get the first byte.
        return _builder->CreateTrunc(_value, _codegen->boolean_type(), name);
    default:
        DCHECK(false);
        return NULL;
    }
}

void CodegenAnyVal::set_is_null(Value* is_null) {
    switch (_type.type) {
    case TYPE_BIGINT:
    case TYPE_DOUBLE: {
        // Lowered type is of form { i8, * }. Set the i8 value to 'is_null'.
        Value* is_null_ext =
            _builder->CreateZExt(is_null, _codegen->tinyint_type(), "is_null_ext");
        _value = _builder->CreateInsertValue(_value, is_null_ext, 0, _name);
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_LARGEINT:
    case TYPE_DECIMAL: {
        // Lowered type is of form { {i8}, [15 x i8], {i128} }. Set the i8 value to
        // 'is_null'.
        Value* is_null_ext =
            _builder->CreateZExt(is_null, _codegen->tinyint_type(), "is_null_ext");
        // Index into the {i8} struct as well as the outer struct.
        uint32_t idxs[] = {0, 0};
        _value = _builder->CreateInsertValue(_value, is_null_ext, idxs, _name);
        break;
    }
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_CHAR: {
        // Lowered type is of the form { i64, * }. Set the first byte of the i64 value to
        // 'is_null'
        Value* v = _builder->CreateExtractValue(_value, 0);
        v = _builder->CreateAnd(v, -0x100LL, "masked");
        Value* is_null_ext = _builder->CreateZExt(is_null, v->getType(), "is_null_ext");
        v = _builder->CreateOr(v, is_null_ext);
        _value = _builder->CreateInsertValue(_value, v, 0, _name);
        break;
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_FLOAT: {
        // Lowered type is an integer. Set the first byte to 'is_null'.
        _value = _builder->CreateAnd(_value, -0x100LL, "masked");
        Value* is_null_ext = _builder->CreateZExt(is_null, _value->getType(), "is_null_ext");
        _value = _builder->CreateOr(_value, is_null_ext, _name);
        break;
    }
    default:
        DCHECK(false) << "NYI: " << _type.debug_string();
    }
}

Value* CodegenAnyVal::get_val(const char* name) {
    DCHECK(_type.type != TYPE_VARCHAR) << "Use get_ptr and get_len for Varchar";
    DCHECK(_type.type != TYPE_HLL) << "Use get_ptr and get_len for Hll";
    DCHECK(_type.type != TYPE_CHAR) << "Use get_ptr and get_len for Char";
    switch (_type.type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT: {
        // Lowered type is an integer. Get the high bytes.
        int num_bits = _type.get_byte_size() * 8;
        Value* val = get_high_bits(num_bits, _value, name);
        if (_type.type == TYPE_BOOLEAN) {
            // Return booleans as i1 (vs. i8)
            val = _builder->CreateTrunc(val, _builder->getInt1Ty(), name);
        }
        return val;
    }
    case TYPE_FLOAT: {
        // Same as above, but we must cast the value to a float.
        Value* val = get_high_bits(32, _value);
        return _builder->CreateBitCast(val, _codegen->float_type());
    }
    case TYPE_BIGINT:
    case TYPE_DOUBLE:
        // Lowered type is of form { i8, * }. Get the second value.
        return _builder->CreateExtractValue(_value, 1, name);
    case TYPE_LARGEINT:
        // Lowered type is of form { i8, [], * }. Get the third value.
        return _builder->CreateExtractValue(_value, 2, name);
    case TYPE_DATE:
    case TYPE_DATETIME:
        /// TYPE_DATETIME/DateTimeVal: { {i8}, i64, i32 } Not Lowered
        return _builder->CreateExtractValue(_value, 1, name);
    default:
        DCHECK(false) << "Unsupported type: " << _type;
        return NULL;
    }
}

void CodegenAnyVal::set_val(Value* val) {
    DCHECK(_type.type != TYPE_VARCHAR) << "Use set_ptr and set_len for StringVals";
    DCHECK(_type.type != TYPE_HLL) << "Use set_ptr and set_len for StringVals";
    DCHECK(_type.type != TYPE_CHAR) << "Use set_ptr and set_len for StringVals";
    switch (_type.type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT: {
        // Lowered type is an integer. Set the high bytes to 'val'.
        int num_bits = _type.get_byte_size() * 8;
        _value = set_high_bits(num_bits, val, _value, _name);
        break;
    }
    case TYPE_FLOAT:
        // Same as above, but we must cast 'val' to an integer type.
        val = _builder->CreateBitCast(val, _codegen->int_type());
        _value = set_high_bits(32, val, _value, _name);
        break;
    case TYPE_BIGINT:
    case TYPE_DOUBLE:
        // Lowered type is of form { i8, * }. Set the second value to 'val'.
        _value = _builder->CreateInsertValue(_value, val, 1, _name);
        break;
    case TYPE_LARGEINT:
        // Lowered type is of form { i8, [], * }. Set the third value to 'val'.
        _value = _builder->CreateInsertValue(_value, val, 2, _name);
        break;
    case TYPE_DATE:
    case TYPE_DATETIME:
        /// TYPE_DATETIME/DateTimeVal: { {i8}, i64, i32 } Not Lowered
        _value = _builder->CreateInsertValue(_value, val, 1, _name);
        break;
    default:
        DCHECK(false) << "Unsupported type: " << _type;
    }
}

void CodegenAnyVal::set_val(bool val) {
    DCHECK_EQ(_type.type, TYPE_BOOLEAN);
    set_val(_builder->getInt1(val));
}

void CodegenAnyVal::set_val(int8_t val) {
    DCHECK_EQ(_type.type, TYPE_TINYINT);
    set_val(_builder->getInt8(val));
}

void CodegenAnyVal::set_val(int16_t val) {
    DCHECK_EQ(_type.type, TYPE_SMALLINT);
    set_val(_builder->getInt16(val));
}

void CodegenAnyVal::set_val(int32_t val) {
    DCHECK(_type.type == TYPE_INT);
    set_val(_builder->getInt32(val));
}

void CodegenAnyVal::set_val(int64_t val) {
    DCHECK(_type.type == TYPE_BIGINT);
    set_val(_builder->getInt64(val));
}

void CodegenAnyVal::set_val(__int128 val) {
    DCHECK_EQ(_type.type, TYPE_LARGEINT);
    // TODO: is there a better way to do this?
    // Set high bits
    Value* ir_val = llvm::ConstantInt::get(_codegen->i128_type(), high_bits(val));
    ir_val = _builder->CreateShl(ir_val, 64, "tmp");
    // Set low bits
    ir_val = _builder->CreateOr(ir_val, low_bits(val), "tmp");
    set_val(ir_val);
}

void CodegenAnyVal::set_val(float val) {
    DCHECK_EQ(_type.type, TYPE_FLOAT);
    set_val(llvm::ConstantFP::get(_builder->getFloatTy(), val));
}

void CodegenAnyVal::set_val(double val) {
    DCHECK_EQ(_type.type, TYPE_DOUBLE);
    set_val(llvm::ConstantFP::get(_builder->getDoubleTy(), val));
}

Value* CodegenAnyVal::get_ptr() {
    // Set the second pointer value to 'ptr'.
    DCHECK(_type.is_string_type());
    return _builder->CreateExtractValue(_value, 1, _name);
}

Value* CodegenAnyVal::get_len() {
    // Get the high bytes of the first value.
    DCHECK(_type.is_string_type());
    Value* v = _builder->CreateExtractValue(_value, 0);
    return get_high_bits(32, v);
}

void CodegenAnyVal::set_ptr(Value* ptr) {
    // Set the second pointer value to 'ptr'.
    DCHECK(_type.is_string_type());
    _value = _builder->CreateInsertValue(_value, ptr, 1, _name);
}

void CodegenAnyVal::set_len(Value* len) {
    // Set the high bytes of the first value to 'len'.
    DCHECK(_type.is_string_type());
    Value* v = _builder->CreateExtractValue(_value, 0);
    v = set_high_bits(32, len, v);
    _value = _builder->CreateInsertValue(_value, v, 0, _name);
}

Value* CodegenAnyVal::get_unlowered_ptr() {
    Value* value_ptr = _codegen->create_entry_block_alloca(*_builder, _value->getType(), "");
    _builder->CreateStore(_value, value_ptr);
    return _builder->CreateBitCast(value_ptr, get_unlowered_ptr_type(_codegen, _type));
}

void CodegenAnyVal::set_from_raw_ptr(Value* raw_ptr) {
    Value* val_ptr =
        _builder->CreateBitCast(raw_ptr, _codegen->get_ptr_type(_type), "val_ptr");
    Value* val = _builder->CreateLoad(val_ptr);
    set_from_raw_value(val);
}

void CodegenAnyVal::set_from_raw_value(Value* raw_val) {
    DCHECK_EQ(raw_val->getType(), _codegen->get_type(_type))
        << std::endl << LlvmCodeGen::print(raw_val)
        << std::endl << _type << " => " << LlvmCodeGen::print(_codegen->get_type(_type));
    switch (_type.type) {
    case TYPE_VARCHAR:
    case TYPE_CHAR: 
    case TYPE_HLL: {
        // Convert StringValue to StringVal
        set_ptr(_builder->CreateExtractValue(raw_val, 0, "ptr"));
        set_len(_builder->CreateExtractValue(raw_val, 1, "len"));
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        Function* fn = _codegen->get_function(IRFunction::TO_DATETIME_VAL);
        Value* val_ptr = _builder->CreateAlloca(get_lowered_type(_codegen, _type), 0, "val_ptr");
        _builder->CreateCall2(fn, _codegen->get_ptr_to(_builder, raw_val), val_ptr);
        _value = _builder->CreateLoad(val_ptr);
        break;
    }
    case TYPE_DECIMAL: {
        Function* fn = _codegen->get_function(IRFunction::TO_DECIMAL_VAL);
        Value* val_ptr = _builder->CreateAlloca(get_lowered_type(_codegen, _type), 0, "val_ptr");
        _builder->CreateCall2(fn, _codegen->get_ptr_to(_builder, raw_val), val_ptr);
        _value = _builder->CreateLoad(val_ptr);
        break;
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        // raw_val is a native type
        set_val(raw_val);
        break;
    default:
        DCHECK(false) << "NYI: " << _type;
        break;
    }
}

Value* CodegenAnyVal::to_native_value() {
    Type* raw_type = _codegen->get_type(_type);
    Value* raw_val = llvm::Constant::getNullValue(raw_type);
    switch (_type.type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR: 
    case TYPE_HLL: {
        // Convert StringVal to StringValue
        raw_val = _builder->CreateInsertValue(raw_val, get_ptr(), 0);
        raw_val = _builder->CreateInsertValue(raw_val, get_len(), 1);
        break;
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        // raw_val is a native type
        raw_val = get_val();
        break;
    case TYPE_DATE:
    case TYPE_DATETIME: {
        Function* func = _codegen->get_function(IRFunction::FROM_DATETIME_VAL);
        Value* raw_val_ptr = _codegen->create_entry_block_alloca(
            *_builder, _codegen->get_type(TYPE_DECIMAL), "raw_val_ptr");
        _builder->CreateCall2(func, raw_val_ptr, _value);
        raw_val = _builder->CreateLoad(raw_val_ptr, "result");
        break;
    }
    case TYPE_DECIMAL: {
        Function* func = _codegen->get_function(IRFunction::FROM_DECIMAL_VAL);
        Value* raw_val_ptr = _codegen->create_entry_block_alloca(
            *_builder, _codegen->get_type(TYPE_DECIMAL), "raw_val_ptr");
        _builder->CreateCall2(func, raw_val_ptr, _value);
        raw_val = _builder->CreateLoad(raw_val_ptr, "result");
        break;
    }
    default:
        DCHECK(false) << "NYI: " << _type;
        break;
    }
    return raw_val;
}

Value* CodegenAnyVal::to_native_ptr(Value* native_ptr) {
    Value* v = to_native_value();
    if (native_ptr == NULL) {
        native_ptr = _codegen->create_entry_block_alloca(*_builder, v->getType(), "");
    }
    _builder->CreateStore(v, native_ptr);
    return native_ptr;
}

Value* CodegenAnyVal::eq(CodegenAnyVal* other) {
    DCHECK_EQ(_type, other->_type);
    switch (_type.type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
        return _builder->CreateICmpEQ(get_val(), other->get_val(), "eq");
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        return _builder->CreateFCmpUEQ(get_val(), other->get_val(), "eq");
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL: {
        llvm::Function* eq_fn = _codegen->get_function(IRFunction::CODEGEN_ANYVAL_STRING_VAL_EQ);
        return _builder->CreateCall2(
            eq_fn, get_unlowered_ptr(), other->get_unlowered_ptr(), "eq");
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        llvm::Function* eq_fn = _codegen->get_function(
            IRFunction::CODEGEN_ANYVAL_DATETIME_VAL_EQ);
        return _builder->CreateCall2(
            eq_fn, get_unlowered_ptr(), other->get_unlowered_ptr(), "eq");
    }
    case TYPE_DECIMAL: {
        llvm::Function* eq_fn = _codegen->get_function(IRFunction::CODEGEN_ANYVAL_DECIMAL_VAL_EQ);
        return _builder->CreateCall2(
            eq_fn, get_unlowered_ptr(), other->get_unlowered_ptr(), "eq");
    }
    default:
        DCHECK(false) << "NYI: " << _type;
        return NULL;
    }
}

Value* CodegenAnyVal::eq_to_native_ptr(Value* native_ptr) {
    Value* val = NULL;
    if (!_type.is_string_type()) {
        val = _builder->CreateLoad(native_ptr);
    }
    switch (_type.type) {
    case TYPE_NULL:
        return _codegen->false_value();
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
        return _builder->CreateICmpEQ(get_val(), val, "cmp_raw");
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        return _builder->CreateFCmpUEQ(get_val(), val, "cmp_raw");
    case TYPE_CHAR:
    case TYPE_VARCHAR: 
    case TYPE_HLL: {
        llvm::Function* eq_fn = _codegen->get_function(
            IRFunction::CODEGEN_ANYVAL_STRING_VALUE_EQ);
        return _builder->CreateCall2(eq_fn, get_unlowered_ptr(), native_ptr, "cmp_raw");
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        llvm::Function* eq_fn = _codegen->get_function(
            IRFunction::CODEGEN_ANYVAL_DATETIME_VALUE_EQ);
        return _builder->CreateCall2(eq_fn, get_unlowered_ptr(), native_ptr, "cmp_raw");
    }
    case TYPE_DECIMAL: {
        llvm::Function* eq_fn = _codegen->get_function(
            IRFunction::CODEGEN_ANYVAL_DECIMAL_VALUE_EQ);
        return _builder->CreateCall2(eq_fn, get_unlowered_ptr(), native_ptr, "cmp_raw");
    }

    default:
        DCHECK(false) << "NYI: " << _type;
        return NULL;
    }
}

Value* CodegenAnyVal::compare(CodegenAnyVal* other, const char* name) {
    DCHECK_EQ(_type, other->_type);
    Value* v1 = to_native_ptr();
    Value* void_v1 = _builder->CreateBitCast(v1, _codegen->ptr_type());
    Value* v2 = other->to_native_ptr();
    Value* void_v2 = _builder->CreateBitCast(v2, _codegen->ptr_type());
    Value* type_ptr = _codegen->get_ptr_to(_builder, _type.to_ir(_codegen), "type");
    llvm::Function* compare_fn = _codegen->get_function(IRFunction::RAW_VALUE_COMPARE);
    Value* args[] = { void_v1, void_v2, type_ptr };
    return _builder->CreateCall(compare_fn, args, name);
}

Value* CodegenAnyVal::get_high_bits(int num_bits, Value* v, const char* name) {
    DCHECK_EQ(v->getType()->getIntegerBitWidth(), num_bits * 2);
    Value* shifted = _builder->CreateAShr(v, num_bits);
    return _builder->CreateTrunc(
        shifted, llvm::IntegerType::get(_codegen->context(), num_bits));
}

// Example output: (num_bits = 8)
// %1 = zext i1 %src to i16
// %2 = shl i16 %1, 8
// %3 = and i16 %dst1 255 ; clear the top half of dst
// %dst2 = or i16 %3, %2  ; set the top of half of dst to src
Value* CodegenAnyVal::set_high_bits(
        int num_bits, Value* src, Value* dst, const char* name) {
    DCHECK_LE(src->getType()->getIntegerBitWidth(), num_bits);
    DCHECK_EQ(dst->getType()->getIntegerBitWidth(), num_bits * 2);
    Value* extended_src = _builder->CreateZExt(
        src, llvm::IntegerType::get(_codegen->context(), num_bits * 2));
    Value* shifted_src = _builder->CreateShl(extended_src, num_bits);
    Value* masked_dst = _builder->CreateAnd(dst, (1LL << num_bits) - 1);
    return _builder->CreateOr(masked_dst, shifted_src, name);
}

Value* CodegenAnyVal::get_null_val(LlvmCodeGen* codegen, const TypeDescriptor& type) {
    Type* val_type = get_lowered_type(codegen, type);
    return get_null_val(codegen, val_type);
}

Value* CodegenAnyVal::get_null_val(LlvmCodeGen* codegen, Type* val_type) {
    if (val_type->isStructTy()) {
        llvm::StructType* struct_type = llvm::cast<llvm::StructType>(val_type);
        std::vector<Constant*> elements;
        if (struct_type->getElementType(0)->isStructTy()) {
            // Return the struct { {1}, 0, 0 } (the 'is_null' byte, i.e. the first value's first
            // byte, is set to 1, the other bytes don't matter)
            llvm::StructType* anyval_struct_type = llvm::cast<llvm::StructType>(
                struct_type->getElementType(0));
            Type* is_null_type = anyval_struct_type->getElementType(0);
            llvm::Constant* null_anyval = llvm::ConstantStruct::get(
                anyval_struct_type, llvm::ConstantInt::get(is_null_type, 1));
            elements.push_back(null_anyval);
        } else {
            Type* type1 = struct_type->getElementType(0);
            elements.push_back(llvm::ConstantInt::get(type1, 1));
        }
        for (int i = 1; i < struct_type->getNumElements(); ++i) {
            Type* ele_type = struct_type->getElementType(i);
            elements.push_back(llvm::Constant::getNullValue(ele_type));
        }
        return llvm::ConstantStruct::get(struct_type, elements);
    }
    // Return the int 1 ('is_null' byte is 1, other bytes don't matter)
    DCHECK(val_type->isIntegerTy());
    return llvm::ConstantInt::get(val_type, 1);
}

CodegenAnyVal CodegenAnyVal::get_non_null_val(
        LlvmCodeGen* codegen, LlvmCodeGen::LlvmBuilder* builder,
        const TypeDescriptor& type, const char* name) {
    Type* val_type = get_lowered_type(codegen, type);
    // All zeros => 'is_null' = false
    Value* value = llvm::Constant::getNullValue(val_type);
    return CodegenAnyVal(codegen, builder, type, value, name);
}

}

