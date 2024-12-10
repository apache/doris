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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.cpp
// and modified by Doris

#pragma once
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/core/field.h"

namespace doris::vectorized {
// Compares ORDER BY column values at given rows to find the boundaries of frame:
// [compared] with [reference] +/- offset. Return value is -1/0/+1, like in
// sorting predicates -- -1 means [compared] is less than [reference] +/- offset.
template <typename ColumnType>
static int compareValuesWithOffset(const vectorized::IColumn* _compared_column, size_t compared_row,
                                   const vectorized::IColumn* _reference_column,
                                   size_t reference_row, const vectorized::Field& _offset,
                                   bool offset_is_preceding) {
    // Casting the columns to the known type here makes it faster, probably
    // because the getData call can be devirtualized.
    const auto* compared_column = assert_cast<const ColumnType*>(_compared_column);
    const auto* reference_column = assert_cast<const ColumnType*>(_reference_column);

    using ValueType = typename ColumnType::value_type;
    // Note that the storage type of offset returned by get<> is different, so
    // we need to specify the type explicitly.
    const auto offset = static_cast<ValueType>(_offset.get<ValueType>());
    DCHECK(offset >= 0);

    const auto compared_value_data = compared_column->get_data_at(compared_row);
    DCHECK(compared_value_data.size == sizeof(ValueType));
    auto compared_value = unaligned_load<ValueType>(compared_value_data.data);

    const auto reference_value_data = reference_column->get_data_at(reference_row);
    DCHECK(reference_value_data.size == sizeof(ValueType));
    auto reference_value = unaligned_load<ValueType>(reference_value_data.data);

    bool is_overflow;
    if (offset_is_preceding) {
        is_overflow = common::sub_overflow(reference_value, offset, reference_value);
    } else {
        is_overflow = common::add_overflow(reference_value, offset, reference_value);
    }

    if (is_overflow) {
        if (offset_is_preceding) {
            // Overflow to the negative, [compared] must be greater.
            // We know that because offset is >= 0.
            return 1;
        } else {
            // Overflow to the positive, [compared] must be less.
            return -1;
        }
    } else {
        // No overflow, compare normally.
        return compared_value < reference_value ? -1 : compared_value == reference_value ? 0 : 1;
    }
}

// A specialization of compareValuesWithOffset for floats.
template <typename ColumnType>
static int compareValuesWithOffsetFloat(const vectorized::IColumn* _compared_column,
                                        size_t compared_row,
                                        const vectorized::IColumn* _reference_column,
                                        size_t reference_row, const vectorized::Field& _offset,
                                        bool offset_is_preceding) {
    // Casting the columns to the known type here makes it faster, probably
    // because the getData call can be devirtualized.
    using ValueType = typename ColumnType::value_type;
    const auto* compared_column = assert_cast<const ColumnType*>(_compared_column);
    const auto* reference_column = assert_cast<const ColumnType*>(_reference_column);
    const auto offset = _offset.get<ValueType>();
    DCHECK(offset >= 0);

    const auto compared_value_data = compared_column->get_data_at(compared_row);
    DCHECK(compared_value_data.size == sizeof(ValueType));
    auto compared_value = unaligned_load<ValueType>(compared_value_data.data);

    const auto reference_value_data = reference_column->get_data_at(reference_row);
    DCHECK(reference_value_data.size == sizeof(ValueType));
    auto reference_value = unaligned_load<ValueType>(reference_value_data.data);

    /// Floats overflow to Inf and the comparison will work normally, so we don't have to do anything.
    if (offset_is_preceding) {
        reference_value -= static_cast<ValueType>(offset);
    } else {
        reference_value += static_cast<ValueType>(offset);
    }

    const auto result =
            compared_value < reference_value ? -1 : (compared_value == reference_value ? 0 : 1);

    return result;
}

// Helper macros to dispatch on type of the ORDER BY column
#define APPLY_FOR_ONE_NEST_TYPE(FUNCTION, TYPE)                            \
    else if (typeid_cast<const TYPE*>(nest_compared_column.get())) {       \
        /* clang-tidy you're dumb, I can't put FUNCTION in braces here. */ \
        nest_compare_function = FUNCTION<TYPE>; /* NOLINT */               \
    }

#define APPLY_FOR_NEST_TYPES(FUNCTION)                                                             \
    if (false) /* NOLINT */                                                                        \
    {                                                                                              \
        /* Do nothing, a starter condition. */                                                     \
    }                                                                                              \
    APPLY_FOR_ONE_NEST_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::UInt8>)                 \
    APPLY_FOR_ONE_NEST_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::UInt16>)                \
    APPLY_FOR_ONE_NEST_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::UInt32>)                \
    APPLY_FOR_ONE_NEST_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::UInt64>)                \
    APPLY_FOR_ONE_NEST_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::Int8>)                  \
    APPLY_FOR_ONE_NEST_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::Int16>)                 \
    APPLY_FOR_ONE_NEST_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::Int32>)                 \
    APPLY_FOR_ONE_NEST_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::Int64>)                 \
    APPLY_FOR_ONE_NEST_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::Int128>)                \
    APPLY_FOR_ONE_NEST_TYPE(FUNCTION##Float, vectorized::ColumnVector<vectorized::Float32>)        \
    APPLY_FOR_ONE_NEST_TYPE(FUNCTION##Float, vectorized::ColumnVector<vectorized::Float64>)        \
    else {                                                                                         \
        throw Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,                                   \
                        "The RANGE OFFSET frame for '{}' ORDER BY nest column is not implemented", \
                        demangle(typeid(nest_compared_column).name()));                            \
    }

// A specialization of compareValuesWithOffset for nullable.
template <typename ColumnType>
static int compareValuesWithOffsetNullable(const vectorized::IColumn* _compared_column,
                                           size_t compared_row,
                                           const vectorized::IColumn* _reference_column,
                                           size_t reference_row, const vectorized::Field& _offset,
                                           bool offset_is_preceding) {
    const auto* compared_column = assert_cast<const ColumnType*>(_compared_column);
    const auto* reference_column = assert_cast<const ColumnType*>(_reference_column);

    if (compared_column->is_null_at(compared_row) && !reference_column->is_null_at(reference_row)) {
        return -1;
    } else if (compared_column->is_null_at(compared_row) &&
               reference_column->is_null_at(reference_row)) {
        return 0;
    } else if (!compared_column->is_null_at(compared_row) &&
               reference_column->is_null_at(reference_row)) {
        return 1;
    }

    vectorized::ColumnPtr nest_compared_column = compared_column->get_nested_column_ptr();
    vectorized::ColumnPtr nest_reference_column = reference_column->get_nested_column_ptr();

    std::function<int(const vectorized::IColumn* compared_column, size_t compared_row,
                      const vectorized::IColumn* reference_column, size_t reference_row,
                      const vectorized::Field& offset, bool offset_is_preceding)>
            nest_compare_function;
    APPLY_FOR_NEST_TYPES(compareValuesWithOffset)
    return nest_compare_function(nest_compared_column.get(), compared_row,
                                 nest_reference_column.get(), reference_row, _offset,
                                 offset_is_preceding);
}

// Helper macros to dispatch on type of the ORDER BY column
#define APPLY_FOR_ONE_TYPE(FUNCTION, TYPE)                                    \
    else if (typeid_cast<const TYPE*>(column)) {                              \
        /* clang-tidy you're dumb, I can't put FUNCTION in braces here. */    \
        compare_values_with_offset_func = FUNCTION<TYPE>; /* NOLINT */ \
    }

#define APPLY_FOR_TYPES(FUNCTION)                                                             \
    if (false) /* NOLINT */                                                                   \
    {                                                                                         \
        /* Do nothing, a starter condition. */                                                \
    }                                                                                         \
    APPLY_FOR_ONE_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::UInt8>)                 \
    APPLY_FOR_ONE_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::UInt16>)                \
    APPLY_FOR_ONE_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::UInt32>)                \
    APPLY_FOR_ONE_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::UInt64>)                \
    APPLY_FOR_ONE_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::Int8>)                  \
    APPLY_FOR_ONE_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::Int16>)                 \
    APPLY_FOR_ONE_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::Int32>)                 \
    APPLY_FOR_ONE_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::Int64>)                 \
    APPLY_FOR_ONE_TYPE(FUNCTION, vectorized::ColumnVector<vectorized::Int128>)                \
    APPLY_FOR_ONE_TYPE(FUNCTION##Float, vectorized::ColumnVector<vectorized::Float32>)        \
    APPLY_FOR_ONE_TYPE(FUNCTION##Float, vectorized::ColumnVector<vectorized::Float64>)        \
    APPLY_FOR_ONE_TYPE(FUNCTION##Nullable, vectorized::ColumnNullable)                        \
    else {                                                                                    \
        throw Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,                              \
                        "The RANGE OFFSET frame for '{}' ORDER BY column is not implemented", \
                        demangle(typeid(*column).name()));                                    \
    }



} // namespace doris::vectorized