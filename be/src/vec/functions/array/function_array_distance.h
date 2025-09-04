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

#include "common/exception.h"
#include "common/status.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

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

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& arg1 = block.get_by_position(arguments[0]);
        const auto& arg2 = block.get_by_position(arguments[1]);

        auto col1 = arg1.column->convert_to_full_column_if_const();
        auto col2 = arg2.column->convert_to_full_column_if_const();
        if (col1->size() != col2->size()) {
            return Status::RuntimeError(
                    fmt::format("function {} have different input array sizes: {} and {}",
                                get_name(), col1->size(), col2->size()));
        }

        const ColumnArray* arr1 = nullptr;
        const ColumnArray* arr2 = nullptr;

        if (col1->is_nullable()) {
            if (col1->has_null()) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "First argument for function {} cannot be null", get_name());
            }
            auto nullable1 = assert_cast<const ColumnNullable*>(col1.get());
            arr1 = assert_cast<const ColumnArray*>(nullable1->get_nested_column_ptr().get());
        } else {
            arr1 = assert_cast<const ColumnArray*>(col1.get());
        }

        if (col2->is_nullable()) {
            if (col2->has_null()) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "Second argument for function {} cannot be null",
                                       get_name());
            }
            auto nullable2 = assert_cast<const ColumnNullable*>(col2.get());
            arr2 = assert_cast<const ColumnArray*>(nullable2->get_nested_column_ptr().get());
        } else {
            arr2 = assert_cast<const ColumnArray*>(col2.get());
        }

        const ColumnFloat32* float1 = nullptr;
        const ColumnFloat32* float2 = nullptr;
        if (arr1->get_data_ptr()->is_nullable()) {
            if (arr1->get_data_ptr()->has_null()) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "First argument for function {} cannot have null",
                                       get_name());
            }
            auto nullable1 = assert_cast<const ColumnNullable*>(arr1->get_data_ptr().get());
            float1 = assert_cast<const ColumnFloat32*>(nullable1->get_nested_column_ptr().get());
        } else {
            float1 = assert_cast<const ColumnFloat32*>(arr1->get_data_ptr().get());
        }

        if (arr2->get_data_ptr()->is_nullable()) {
            if (arr2->get_data_ptr()->has_null()) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "Second argument for function {} cannot have null",
                                       get_name());
            }
            auto nullable2 = assert_cast<const ColumnNullable*>(arr2->get_data_ptr().get());
            float2 = assert_cast<const ColumnFloat32*>(nullable2->get_nested_column_ptr().get());
        } else {
            float2 = assert_cast<const ColumnFloat32*>(arr2->get_data_ptr().get());
        }

        const ColumnOffset64* offset1 =
                assert_cast<const ColumnArray::ColumnOffsets*>(arr1->get_offsets_ptr().get());
        const ColumnOffset64* offset2 =
                assert_cast<const ColumnArray::ColumnOffsets*>(arr2->get_offsets_ptr().get());
        // prepare return data
        auto dst = ColumnType::create(input_rows_count);
        auto& dst_data = dst->get_data();

        size_t elemt_cnt = offset1->size();
        for (ssize_t row = 0; row < elemt_cnt; ++row) {
            // Calculate actual array sizes for current row.
            // For nullable arrays, we cannot compare absolute offset values directly because:
            // 1. When a row is null, its offset might equal the previous offset (no elements added)
            // 2. Or it might include the array size even if the row is null (implementation dependent)
            // Therefore, we must calculate the actual array size as: offsets[row] - offsets[row-1]
            ssize_t size1 = offset1->get_data()[row] - offset1->get_data()[row - 1];
            ssize_t size2 = offset2->get_data()[row] - offset2->get_data()[row - 1];

            if (size1 != size2) [[unlikely]] {
                return Status::InvalidArgument(
                        "function {} have different input element sizes of array: {} and {}",
                        get_name(), size1, size2);
            }
            dst_data[row] = DistanceImpl::distance(
                    float1->get_data().data() + offset1->get_data()[row - 1],
                    float2->get_data().data() + offset2->get_data()[row - 1], size1);
        }

        block.replace_by_position(result, std::move(dst));
        return Status::OK();
    }
};

} // namespace doris::vectorized
