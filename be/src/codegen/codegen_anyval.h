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

#ifndef IMPALA_CODEGEN_CODEGEN_ANYVAL_H
#define IMPALA_CODEGEN_CODEGEN_ANYVAL_H

#include "codegen/llvm_codegen.h"

namespace llvm {
class Type;
class Value;
}

namespace doris {

/// Class for handling AnyVal subclasses during codegen. Codegen functions should use this
/// wrapper instead of creating or manipulating *Val values directly in most cases. This is
/// because the struct types must be lowered to integer types in many cases in order to
/// conform to the standard calling convention (e.g., { i8, i32 } => i64). This class wraps
/// the lowered types for each *Val struct.
//
/// This class conceptually represents a single *Val that is mutated, but operates by
/// generating IR instructions involving _value (each of which generates a new Value*,
/// since IR uses SSA), and then setting _value to the most recent Value* generated. The
/// generated instructions perform the integer manipulation equivalent to setting the
/// fields of the original struct type.
//
/// Lowered types:
/// TYPE_BOOLEAN/BooleanVal: i16
/// TYPE_TINYINT/TinyIntVal: i16
/// TYPE_SMALLINT/SmallIntVal: i32
/// TYPE_INT/INTVal: i64
/// TYPE_BIGINT/BigIntVal: { i8, i64 }
/// TYPE_LARGEINT/LargeIntVal: { {i8}, [15 x i8], i128 } Not Lowered
/// TYPE_FLOAT/FloatVal: i64
/// TYPE_DOUBLE/DoubleVal: { i8, double }
/// TYPE_STRING/StringVal: { i64, i8* }
/// TYPE_DATETIME/DateTimeVal: { {i8}, i64, i32 } Not Lowered
/// TYPE_DECIMAL/DecimalVal: { {i8}, i8, i8, i8, [9 x i32] } Not Lowered
//
/// TODO:
/// - unit tests
class CodegenAnyVal {
public:
    static const char* _s_llvm_booleanval_name;
    static const char* _s_llvm_tinyintval_name;
    static const char* _s_llvm_smallintval_name;
    static const char* _s_llvm_intval_name;
    static const char* _s_llvm_bigintval_name;
    static const char* _s_llvm_largeintval_name;
    static const char* _s_llvm_floatval_name;
    static const char* _s_llvm_doubleval_name;
    static const char* _s_llvm_stringval_name;
    static const char* _s_llvm_datetimeval_name;
    static const char* _s_llvm_decimalval_name;

    /// Creates a call to 'fn', which should return a (lowered) *Val, and returns the result.
    /// This abstracts over the x64 calling convention, in particular for functions returning
    /// a DecimalVal, which pass the return value as an output argument.
    //
    /// If 'result_ptr' is non-NULL, it should be a pointer to the lowered return type of
    /// 'fn' (e.g. if 'fn' returns a BooleanVal, 'result_ptr' should have type i16*). The
    /// result of calling 'fn' will be stored in 'result_ptr' and this function will return
    /// NULL. If 'result_ptr' is NULL, this function will return the result (note that the
    /// result will not be a pointer in this case).
    //
    /// 'name' optionally specifies the name of the returned value.
    static llvm::Value* create_call(
            LlvmCodeGen* cg, LlvmCodeGen::LlvmBuilder* builder,
            llvm::Function* fn, llvm::ArrayRef<llvm::Value*> args, 
            const char* name,
            llvm::Value* result_ptr);

    static llvm::Value* create_call(
            LlvmCodeGen* cg, LlvmCodeGen::LlvmBuilder* builder,
            llvm::Function* fn, llvm::ArrayRef<llvm::Value*> args, 
            const char* name) {
        return create_call(cg, builder, fn, args, name, NULL);
    }

    /// Same as above but wraps the result in a CodegenAnyVal.
    static CodegenAnyVal create_call_wrapped(LlvmCodeGen* cg,
            LlvmCodeGen::LlvmBuilder* builder, const TypeDescriptor& type, llvm::Function* fn,
            llvm::ArrayRef<llvm::Value*> args, const char* name,
            llvm::Value* result_ptr);

    /// Same as above but wraps the result in a CodegenAnyVal.
    static CodegenAnyVal create_call_wrapped(LlvmCodeGen* cg,
            LlvmCodeGen::LlvmBuilder* builder, const TypeDescriptor& type, llvm::Function* fn,
            llvm::ArrayRef<llvm::Value*> args, const char* name) {
        return create_call_wrapped(cg, builder, type, fn, args, name, NULL);
    }

    /// Returns the lowered AnyVal type associated with 'type'.
    /// E.g.: TYPE_BOOLEAN (which corresponds to a BooleanVal) => i16
    static llvm::Type* get_lowered_type(LlvmCodeGen* cg, const TypeDescriptor& type);

    /// Returns the lowered AnyVal pointer type associated with 'type'.
    /// E.g.: TYPE_BOOLEAN => i16*
    static llvm::Type* get_lowered_ptr_type(LlvmCodeGen* cg, const TypeDescriptor& type);

    /// Returns the unlowered AnyVal type associated with 'type'.
    /// E.g.: TYPE_BOOLEAN => %"struct.impala_udf::BooleanVal"
    static llvm::Type* get_unlowered_type(LlvmCodeGen* cg, const TypeDescriptor& type);

    /// Returns the unlowered AnyVal pointer type associated with 'type'.
    /// E.g.: TYPE_BOOLEAN => %"struct.impala_udf::BooleanVal"*
    static llvm::Type* get_unlowered_ptr_type(LlvmCodeGen* cg, const TypeDescriptor& type);

    /// Return the constant type-lowered value corresponding to a null *Val.
    /// E.g.: passing TYPE_DOUBLE (corresponding to the lowered DoubleVal { i8, double })
    /// returns the constant struct { 1, 0.0 }
    static llvm::Value* get_null_val(LlvmCodeGen* codegen, const TypeDescriptor& type);

    /// Return the constant type-lowered value corresponding to a null *Val.
    /// 'val_type' must be a lowered type (i.e. one of the types returned by GetType)
    static llvm::Value* get_null_val(LlvmCodeGen* codegen, llvm::Type* val_type);

    /// Return the constant type-lowered value corresponding to a non-null *Val.
    /// E.g.: TYPE_DOUBLE (lowered DoubleVal: { i8, double }) => { 0, 0 }
    /// This returns a CodegenAnyVal, rather than the unwrapped Value*, because the actual
    /// value still needs to be set.
    static CodegenAnyVal get_non_null_val(
        LlvmCodeGen* codegen, LlvmCodeGen::LlvmBuilder* builder, 
        const TypeDescriptor& type, const char* name);

    static CodegenAnyVal get_non_null_val(
            LlvmCodeGen* codegen, LlvmCodeGen::LlvmBuilder* builder, 
            const TypeDescriptor& type) {
        return get_non_null_val(codegen, builder, type, "");
    }

    /// Creates a wrapper around a lowered *Val value.
    //
    /// Instructions for manipulating the value are generated using 'builder'. The insert
    /// point of 'builder' is not modified by this class, and it is safe to call
    /// 'builder'.SetInsertPoint() after passing 'builder' to this class.
    //
    /// 'type' identified the type of wrapped value (e.g., TYPE_INT corresponds to IntVal,
    /// which is lowered to i64).
    //
    /// If 'value' is NULL, a new value of the lowered type is alloca'd. Otherwise 'value'
    /// must be of the correct lowered type.
    //
    /// If 'name' is specified, it will be used when generated instructions that set value.
    CodegenAnyVal(LlvmCodeGen* codegen, LlvmCodeGen::LlvmBuilder* builder,
                  const TypeDescriptor& type, llvm::Value* value = NULL, const char* name = "");

    ~CodegenAnyVal() { }

    /// Returns the current type-lowered value.
    llvm::Value* value() { return _value; }

    /// Gets the 'is_null' field of the *Val.
    llvm::Value* get_is_null(const char* name = "is_null");

    /// Get the 'val' field of the *Val. Do not call if this represents a StringVal or
    /// TimestampVal. If this represents a DecimalVal, returns 'val4', 'val8', or 'val16'
    /// depending on the precision of 'type_'.  The returned value will have variable name
    /// 'name'.
    llvm::Value* get_val(const char* name = "val");

    /// Sets the 'is_null' field of the *Val.
    void set_is_null(llvm::Value* is_null);

    /// Sets the 'val' field of the *Val. Do not call if this represents a StringVal or
    /// TimestampVal.
    void set_val(llvm::Value* val);

    /// Sets the 'val' field of the *Val. The *Val must correspond to the argument type.
    void set_val(bool val);
    void set_val(int8_t val);
    void set_val(int16_t val);
    void set_val(int32_t val);
    void set_val(int64_t val);
    void set_val(__int128 val);
    void set_val(float val);
    void set_val(double val);

    /// Getters for StringVals.
    llvm::Value* get_ptr();
    llvm::Value *get_len();

    /// Setters for StringVals.
    void set_ptr(llvm::Value* ptr);
    void set_len(llvm::Value* len);

    /// Allocas and stores this value in an unlowered pointer, and returns the pointer. This
    /// *Val should be non-null.
    llvm::Value* get_unlowered_ptr();

    /// Set this *Val's value based on 'raw_val'. 'raw_val' should be a native type,
    /// StringValue, or DateTimeValue.
    void set_from_raw_value(llvm::Value* raw_val);

    /// Set this *Val's value based on void* 'raw_ptr'. 'raw_ptr' should be a pointer to a
    /// native type, StringValue, or TimestampValue (i.e. the value returned by an
    /// interpreted compute fn).
    void set_from_raw_ptr(llvm::Value* raw_ptr);

    /// Converts this *Val's value to a native type, StringValue, TimestampValue, etc.
    /// This should only be used if this *Val is not null.
    llvm::Value* to_native_value();

    /// Sets 'native_ptr' to this *Val's value. If non-NULL, 'native_ptr' should be a
    /// pointer to a native type, StringValue, TimestampValue, etc. If NULL, a pointer is
    /// alloca'd. In either case the pointer is returned. This should only be used if this
    /// *Val is not null.
    llvm::Value* to_native_ptr(llvm::Value* native_ptr = NULL);

    /// Returns the i1 result of this == other. this and other must be non-null.
    llvm::Value* eq(CodegenAnyVal* other);

    /// Compares this *Val to the value of 'native_ptr'. 'native_ptr' should be a pointer to
    /// a native type, StringValue, or TimestampValue. This *Val should match 'native_ptr's
    /// type (e.g. if this is an IntVal, 'native_ptr' should have type i32*). Returns the i1
    /// result of the equality comparison.
    llvm::Value* eq_to_native_ptr(llvm::Value* native_ptr);

    /// Returns the i32 result of comparing this value to 'other' (similar to
    /// RawValue::Compare()). This and 'other' must be non-null. Return value is < 0 if
    /// this < 'other', 0 if this == 'other', > 0 if this > 'other'.
    llvm::Value* compare(CodegenAnyVal* other, const char* name);
    llvm::Value* compare(CodegenAnyVal* other) { 
        return compare(other, "result");
    }

    /// Ctor for created an uninitialized CodegenAnYVal that can be assigned to later.
    CodegenAnyVal() : 
        _type(INVALID_TYPE), _value(NULL), _name(NULL), _codegen(NULL), _builder(NULL) { 
    }

private:
    TypeDescriptor _type;
    llvm::Value* _value;
    const char* _name;

    LlvmCodeGen* _codegen;
    LlvmCodeGen::LlvmBuilder* _builder;

    /// Helper function for getting the top (most significant) half of 'v'.
    /// 'v' should have width = 'num_bits' * 2 and be an integer type.
    llvm::Value* get_high_bits(int num_bits, llvm::Value* v, const char* name);

    llvm::Value* get_high_bits(int num_bits, llvm::Value* v) {
        return get_high_bits(num_bits, v, "");
    }


    /// Helper function for setting the top (most significant) half of a 'dst' to 'src'.
    /// 'src' must have width <= 'num_bits' and 'dst' must have width = 'num_bits' * 2.
    /// Both 'dst' and 'src' should be integer types.
    llvm::Value* set_high_bits(
        int num_bits, llvm::Value* src, llvm::Value* dst, const char* name);

    llvm::Value* set_high_bits(
            int num_bits, llvm::Value* src, llvm::Value* dst) {
        return set_high_bits(num_bits, src, dst, "");
    }
};

}

#endif

