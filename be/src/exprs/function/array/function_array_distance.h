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

#include <faiss/impl/platform_macros.h>
#include <faiss/utils/distances.h>
#include <gen_cpp/Types_types.h>

#include <optional>

#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "exec/common/util.hpp"
#include "exprs/function/array/function_array_utils.h"
#include "exprs/function/function.h"

namespace doris {

class L1Distance {
public:
    static constexpr auto name = "l1_distance";
    static float distance(const float* x, const float* y, size_t d) {
        return faiss::fvec_L1(x, y, d);
    }
};

class L2Distance {
public:
    static constexpr auto name = "l2_distance";
    static float distance(const float* x, const float* y, size_t d) {
        return std::sqrt(faiss::fvec_L2sqr(x, y, d));
    }
};

class InnerProduct {
public:
    static constexpr auto name = "inner_product";
    static float distance(const float* x, const float* y, size_t d) {
        return faiss::fvec_inner_product(x, y, d);
    }
};

class CosineDistance {
public:
    static constexpr auto name = "cosine_distance";
    static float distance(const float* x, const float* y, size_t d);
};

class CosineSimilarity {
public:
    static constexpr auto name = "cosine_similarity";
    static float distance(const float* x, const float* y, size_t d);
};

class L2DistanceApproximate : public L2Distance {
public:
    static constexpr auto name = "l2_distance_approximate";
};

class InnerProductApproximate : public InnerProduct {
public:
    static constexpr auto name = "inner_product_approximate";
};

template <typename DistanceImpl>
class FunctionArrayDistance : public IFunction {
public:
    using DataType = PrimitiveTypeTraits<TYPE_FLOAT>::DataType;
    using ColumnType = PrimitiveTypeTraits<TYPE_FLOAT>::ColumnType;

    static constexpr auto name = DistanceImpl::name;
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionArrayDistance<DistanceImpl>>(); }
    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() != 2) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Invalid number of arguments");
        }

        // primitive_type of Nullable is its nested type.
        if (arguments[0]->get_primitive_type() != TYPE_ARRAY ||
            arguments[1]->get_primitive_type() != TYPE_ARRAY) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Arguments for function {} must be arrays", get_name());
        }

        return std::make_shared<DataTypeFloat32>();
    }

    // All array distance functions has always not nullable return type.
    // We want to make sure throw exception if input columns contain NULL.
    bool use_default_implementation_for_nulls() const override { return false; }

    // Extract the ColumnArray from a column, unwrapping Nullable if present.
    // Validates that no NULL values exist.
    static const ColumnArray* _extract_array_column(const IColumn* col, const char* arg_name,
                                                    const String& func_name) {
        if (col->is_nullable()) {
            if (col->has_null()) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "{} for function {} cannot be null", arg_name, func_name);
            }
            auto nullable = assert_cast<const ColumnNullable*>(col);
            return assert_cast<const ColumnArray*>(nullable->get_nested_column_ptr().get());
        }
        return assert_cast<const ColumnArray*>(col);
    }

    // Extract the ColumnFloat32 data from an array column, unwrapping Nullable if present.
    // Validates that no NULL elements exist within the array.
    static const ColumnFloat32* _extract_float_data(const ColumnArray* arr, const char* arg_name,
                                                    const String& func_name) {
        if (arr->get_data_ptr()->is_nullable()) {
            if (arr->get_data_ptr()->has_null()) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "{} for function {} cannot have null", arg_name, func_name);
            }
            auto nullable = assert_cast<const ColumnNullable*>(arr->get_data_ptr().get());
            return assert_cast<const ColumnFloat32*>(nullable->get_nested_column_ptr().get());
        }
        return assert_cast<const ColumnFloat32*>(arr->get_data_ptr().get());
    }

    // Holds the extracted float data pointer and dimension for a const array argument,
    // avoiding repeated per-row extraction.
    struct ConstArrayInfo {
        const float* data = nullptr;
        ssize_t dim = 0;
    };

    // Try to extract const array info from a column. If the column is ColumnConst,
    // extract the float data pointer and dimension once; otherwise return nullopt.
    std::optional<ConstArrayInfo> _try_extract_const(const ColumnPtr& col,
                                                     const char* arg_name) const {
        if (!is_column_const(*col)) {
            return std::nullopt;
        }
        auto const_col = assert_cast<const ColumnConst*>(col.get());
        const IColumn* inner = const_col->get_data_column_ptr().get();
        const ColumnArray* arr = _extract_array_column(inner, arg_name, get_name());
        const ColumnFloat32* float_col = _extract_float_data(arr, arg_name, get_name());
        ssize_t dim = static_cast<ssize_t>(float_col->size());
        return ConstArrayInfo {float_col->get_data().data(), dim};
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& arg1 = block.get_by_position(arguments[0]);
        const auto& arg2 = block.get_by_position(arguments[1]);

        // Try to handle const columns without expanding them.
        auto const_info1 = _try_extract_const(arg1.column, "First argument");
        auto const_info2 = _try_extract_const(arg2.column, "Second argument");

        // For non-const columns, expand and extract normally.
        ColumnPtr materialized_col1, materialized_col2;
        const ColumnArray* arr1 = nullptr;
        const ColumnArray* arr2 = nullptr;
        const ColumnFloat32* float1 = nullptr;
        const ColumnFloat32* float2 = nullptr;
        const ColumnOffset64* offset1 = nullptr;
        const ColumnOffset64* offset2 = nullptr;
        const IColumn::Offsets64* offsets_data1 = nullptr;
        const IColumn::Offsets64* offsets_data2 = nullptr;
        const float* float_data1 = nullptr;
        const float* float_data2 = nullptr;

        if (!const_info1) {
            materialized_col1 = arg1.column->convert_to_full_column_if_const();
            arr1 = _extract_array_column(materialized_col1.get(), "First argument", get_name());
            float1 = _extract_float_data(arr1, "First argument", get_name());
            offset1 = assert_cast<const ColumnArray::ColumnOffsets*>(arr1->get_offsets_ptr().get());
            offsets_data1 = &offset1->get_data();
            float_data1 = float1->get_data().data();
        }

        if (!const_info2) {
            materialized_col2 = arg2.column->convert_to_full_column_if_const();
            arr2 = _extract_array_column(materialized_col2.get(), "Second argument", get_name());
            float2 = _extract_float_data(arr2, "Second argument", get_name());
            offset2 = assert_cast<const ColumnArray::ColumnOffsets*>(arr2->get_offsets_ptr().get());
            offsets_data2 = &offset2->get_data();
            float_data2 = float2->get_data().data();
        }

        // prepare return data
        auto dst = ColumnType::create(input_rows_count);
        auto& dst_data = dst->get_data();

        for (size_t row = 0; row < input_rows_count; ++row) {
            const float* data_ptr1;
            const float* data_ptr2;
            ssize_t size1, size2;
            const auto idx = static_cast<ssize_t>(row);

            if (const_info1) {
                data_ptr1 = const_info1->data;
                size1 = const_info1->dim;
            } else {
                // -1 is valid for PaddedPODArray-backed offsets.
                const auto prev_offset1 = (*offsets_data1)[idx - 1];
                size1 = (*offsets_data1)[idx] - prev_offset1;
                data_ptr1 = float_data1 + prev_offset1;
            }

            if (const_info2) {
                data_ptr2 = const_info2->data;
                size2 = const_info2->dim;
            } else {
                const auto prev_offset2 = (*offsets_data2)[idx - 1];
                size2 = (*offsets_data2)[idx] - prev_offset2;
                data_ptr2 = float_data2 + prev_offset2;
            }

            if (size1 != size2) [[unlikely]] {
                return Status::InvalidArgument(
                        "function {} have different input element sizes of array: {} and {}",
                        get_name(), size1, size2);
            }
            dst_data[row] = DistanceImpl::distance(data_ptr1, data_ptr2, size1);
        }

        block.replace_by_position(result, std::move(dst));
        return Status::OK();
    }
};

} // namespace doris
