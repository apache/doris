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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/Native.h
// and modified by Doris

#ifdef DORIS_ENABLE_JIT
#pragma once

#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_const.h"

#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wunused-parameter"

#    include <llvm/IR/IRBuilder.h>

#    pragma GCC diagnostic pop


namespace doris::vectorized {
namespace ErrorCodes {
    extern const int NOT_IMPLEMENTED;
}

static inline bool type_is_signed(const IDataType & type) {
    WhichDataType data_type(type);
    return data_type.is_native_int() || data_type.is_float() || data_type.is_enum();
}

static inline llvm::Type * to_native_type(llvm::IRBuilderBase& builder, const IDataType & type) {
    WhichDataType data_type(type);

    if (data_type.is_nullable()) {
        const auto & data_type_nullable = static_cast<const DataTypeNullable&>(type);
        auto * wrapped = to_native_type(builder, *data_type_nullable.get_nested_type());
        auto * is_null_type = builder.getInt1Ty();
        return wrapped ? llvm::StructType::get(wrapped, is_null_type) : nullptr;
    }

    /// LLVM doesn't have unsigned types, it has unsigned instructions.
    if (data_type.is_int8() || data_type.is_uint8())
        return builder.getInt8Ty();
    else if (data_type.is_int16() || data_type.is_uint16() || data_type.is_date())
        return builder.getInt16Ty();
    else if (data_type.is_int32() || data_type.is_uint32() || data_type.is_date_time())
        return builder.getInt32Ty();
    else if (data_type.is_int64() || data_type.is_uint64())
        return builder.getInt64Ty();
    else if (data_type.is_float32())
        return builder.getFloatTy();
    else if (data_type.is_float64())
        return builder.getDoubleTy();
    else if (data_type.is_enum8())
        return builder.getInt8Ty();
    else if (data_type.is_enum16())
        return builder.getInt16Ty();

    return nullptr;
}

template <typename ToType>
static inline llvm::Type * to_native_type(llvm::IRBuilderBase& builder) {
    if constexpr (std::is_same_v<ToType, Int8> || std::is_same_v<ToType, UInt8>)
        return builder.getInt8Ty();
    else if constexpr (std::is_same_v<ToType, Int16> || std::is_same_v<ToType, UInt16>)
        return builder.getInt16Ty();
    else if constexpr (std::is_same_v<ToType, Int32> || std::is_same_v<ToType, UInt32>)
        return builder.getInt32Ty();
    else if constexpr (std::is_same_v<ToType, Int64> || std::is_same_v<ToType, UInt64>)
        return builder.getInt64Ty();
    else if constexpr (std::is_same_v<ToType, Float32>)
        return builder.getFloatTy();
    else if constexpr (std::is_same_v<ToType, Float64>)
        return builder.getDoubleTy();

    return nullptr;
}

template <typename Type>
static constexpr inline bool can_be_native_type() {
    if constexpr (std::is_same_v<Type, Int8> || std::is_same_v<Type, UInt8>)
        return true;
    else if constexpr (std::is_same_v<Type, Int16> || std::is_same_v<Type, UInt16>)
        return true;
    else if constexpr (std::is_same_v<Type, Int32> || std::is_same_v<Type, UInt32>)
        return true;
    else if constexpr (std::is_same_v<Type, Int64> || std::is_same_v<Type, UInt64>)
        return true;
    else if constexpr (std::is_same_v<Type, Float32>)
        return true;
    else if constexpr (std::is_same_v<Type, Float64>)
        return true;

    return false;
}

static inline bool can_be_native_type(const IDataType & type) {
    WhichDataType data_type(type);

    if (data_type.is_nullable()) {
        const auto & data_type_nullable = static_cast<const DataTypeNullable&>(type);
        return can_be_native_type(*data_type_nullable.get_nested_type());
    }

    return data_type.is_native_int() || data_type.is_native_uint() || data_type.is_float() || data_type.is_date() || data_type.is_enum();
}

static inline llvm::Type * to_native_type(llvm::IRBuilderBase& builder, const DataTypePtr & type) {
    return to_native_type(builder, *type);
}

static inline llvm::Value* native_bool_cast(llvm::IRBuilder<>& b, const DataTypePtr & from_type, llvm::Value* value) {
    if (from_type->is_nullable()) {
        auto * inner = native_bool_cast(b, remove_nullable(from_type), b.CreateExtractValue(value, {0}));
        return b.CreateAnd(b.CreateNot(b.CreateExtractValue(value, {1})), inner);
    }
    auto * zero = llvm::Constant::getNullValue(value->getType());

    if (value->getType()->isIntegerTy())
        return b.CreateICmpNE(value, zero);
    if (value->getType()->isFloatingPointTy())
        return b.CreateFCmpONE(value, zero); /// QNaN is false

    return nullptr;
}

static inline llvm::Value* native_cast(llvm::IRBuilder<>& b, const DataTypePtr & from, llvm::Value* value, llvm::Type * to_type) {
    auto * from_type = value->getType();

    if (from_type == to_type)
        return value;
    else if (from_type->isIntegerTy() && to_type->isFloatingPointTy())
        return type_is_signed(*from) ? b.CreateSIToFP(value, to_type) : b.CreateUIToFP(value, to_type);
    else if (from_type->isFloatingPointTy() && to_type->isIntegerTy())
        return type_is_signed(*from) ? b.CreateFPToSI(value, to_type) : b.CreateFPToUI(value, to_type);
    else if (from_type->isIntegerTy() && to_type->isIntegerTy())
        return b.CreateIntCast(value, to_type, type_is_signed(*from));
    else if (from_type->isFloatingPointTy() && to_type->isFloatingPointTy())
        return b.CreateFPCast(value, to_type);

    return nullptr;
}

template <typename FromType>
static inline llvm::Value* native_cast(llvm::IRBuilder<>& b, llvm::Value* value, llvm::Type * to_type) {
    auto * from_type = value->getType();

    static constexpr bool from_type_is_signed = std::numeric_limits<FromType>::is_signed;

    if (from_type == to_type)
        return value;
    else if (from_type->isIntegerTy() && to_type->isFloatingPointTy())
        return from_type_is_signed ? b.CreateSIToFP(value, to_type) : b.CreateUIToFP(value, to_type);
    else if (from_type->isFloatingPointTy() && to_type->isIntegerTy())
        return from_type_is_signed ? b.CreateFPToSI(value, to_type) : b.CreateFPToUI(value, to_type);
    else if (from_type->isIntegerTy() && to_type->isIntegerTy())
        return b.CreateIntCast(value, to_type, from_type_is_signed);
    else if (from_type->isFloatingPointTy() && to_type->isFloatingPointTy())
        return b.CreateFPCast(value, to_type);

    return nullptr;
}

static inline llvm::Value* native_cast(llvm::IRBuilder<>& b, const DataTypePtr & from, llvm::Value* value, const DataTypePtr & to) {
    auto * n_to = to_native_type(b, to);

    if (value->getType() == n_to) {
        return value;
    }
    else if (from->is_nullable() && to->is_nullable()) {
        auto * inner = native_cast(b, remove_nullable(from), b.CreateExtractValue(value, {0}), to);
        return b.CreateInsertValue(inner, b.CreateExtractValue(value, {1}), {1});
    }
    else if (from->is_nullable()) {
        return native_cast(b, remove_nullable(from), b.CreateExtractValue(value, {0}), to);
    }
    else if (to->is_nullable()) {
        auto * inner = native_cast(b, from, value, remove_nullable(to));
        return b.CreateInsertValue(llvm::Constant::getNullValue(n_to), inner, {0});
    }

    return native_cast(b, from, value, n_to);
}

static inline std::pair<llvm::Value *, llvm::Value *> native_cast_to_common(llvm::IRBuilder<>& b, const DataTypePtr & lhs_type, llvm::Value* lhs, const DataTypePtr & rhs_type, llvm::Value* rhs) {
    llvm::Type * common;

    bool lhs_is_signed = type_is_signed(*lhs_type);
    bool rhs_is_signed = type_is_signed(*rhs_type);

    if (lhs->getType()->isIntegerTy() && rhs->getType()->isIntegerTy()) {
        /// if one integer has a sign bit, make sure the other does as well. llvm generates optimal code
        /// (e.g. uses overflow flag on x86) for (word size + 1)-bit integer operations.

        size_t lhs_bit_width = lhs->getType()->getIntegerBitWidth() + (!lhs_is_signed && rhs_is_signed);
        size_t rhs_bit_width = rhs->getType()->getIntegerBitWidth() + (!rhs_is_signed && lhs_is_signed);

        size_t max_bit_width = std::max(lhs_bit_width, rhs_bit_width);
        common = b.getIntNTy(max_bit_width);
    }
    else {
        /// TODO: Check
        /// (double, float) or (double, int_N where N <= double's mantissa width) -> double
        common = b.getDoubleTy();
    }

    auto * cast_lhs_to_common = native_cast(b, lhs_type, lhs, common);
    auto * cast_rhs_to_common = native_cast(b, rhs_type, rhs, common);

    return std::make_pair(cast_lhs_to_common, cast_rhs_to_common);
}

static inline llvm::Constant * get_column_native_value(llvm::IRBuilderBase& builder, const DataTypePtr & column_type, const IColumn & column, size_t index) {
    if (const auto * constant = typeid_cast<const ColumnConst *>(&column)) {
        return get_column_native_value(builder, column_type, constant->get_data_column(), 0);
    }

    WhichDataType column_data_type(column_type);

    auto * type = to_native_type(builder, column_type);

    if (!type || column.size() <= index)
        return nullptr;

    if (column_data_type.is_nullable()) {
        const auto & nullable_data_type = assert_cast<const DataTypeNullable &>(*column_type);
        const auto & nullable_column = assert_cast<const ColumnNullable &>(column);

        auto * value = get_column_native_value(builder, nullable_data_type.get_nested_type(), nullable_column.get_nested_column(), index);
        auto * is_null = llvm::ConstantInt::get(type->getContainedType(1), nullable_column.is_null_at(index));

        return value ? llvm::ConstantStruct::get(static_cast<llvm::StructType *>(type), value, is_null) : nullptr;
    }
    else if (column_data_type.is_float32()) {
        return llvm::ConstantFP::get(type, assert_cast<const ColumnVector<Float32> &>(column).get_element(index));
    }
    else if (column_data_type.is_float64()) {
        return llvm::ConstantFP::get(type, assert_cast<const ColumnVector<Float64> &>(column).get_element(index));
    }
    else if (column_data_type.is_native_uint() || column_data_type.is_date() || column_data_type.is_date_time()) {
        return llvm::ConstantInt::get(type, column.get_uint(index));
    }
    else if (column_data_type.is_native_int() || column_data_type.is_enum()) {
        return llvm::ConstantInt::get(type, column.get_int(index));
    }

    return nullptr;
}

}
#endif
