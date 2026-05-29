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

#ifndef DISABLE_ANN
#include <faiss/impl/platform_macros.h>
#include <faiss/utils/distances.h>
#else
// When FAISS is disabled (e.g. macOS local UT builds), provide inline stubs
// for the FAISS functions used by L1/L2/InnerProduct distance classes and
// no-op definitions for the float-control pragmas.
#include <cmath>
#include <cstddef>
#define FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
#define FAISS_PRAGMA_IMPRECISE_FUNCTION_END
namespace faiss {
inline float fvec_L1(const float* x, const float* y, std::size_t d) {
    float s = 0;
    for (std::size_t i = 0; i < d; ++i) {
        s += std::abs(x[i] - y[i]);
    }
    return s;
}
inline float fvec_L2sqr(const float* x, const float* y, std::size_t d) {
    float s = 0;
    for (std::size_t i = 0; i < d; ++i) {
        float diff = x[i] - y[i];
        s += diff * diff;
    }
    return s;
}
inline float fvec_inner_product(const float* x, const float* y, std::size_t d) {
    float s = 0;
    for (std::size_t i = 0; i < d; ++i) {
        s += x[i] * y[i];
    }
    return s;
}
} // namespace faiss
#endif

#include <gen_cpp/Types_types.h>

#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_array_view.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "exec/common/util.hpp"
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

    // Validate that neither outer column nor inner array elements contain NULL.
    // Distance functions always throw on NULL input.
    static void _validate_no_nulls(const ColumnPtr& col, const char* arg_name,
                                   const String& func_name) {
        const IColumn* raw = col.get();

        // Unwrap const
        if (is_column_const(*raw)) {
            raw = assert_cast<const ColumnConst*>(raw)->get_data_column_ptr().get();
        }

        // Check outer nullable
        if (raw->is_nullable()) {
            if (raw->has_null()) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "{} for function {} cannot be null", arg_name, func_name);
            }
            raw = assert_cast<const ColumnNullable*>(raw)->get_nested_column_ptr().get();
        }

        // Check inner nullable (array elements)
        const auto& array_col = assert_cast<const ColumnArray&>(*raw);
        if (array_col.get_data_ptr()->is_nullable() && array_col.get_data_ptr()->has_null()) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "{} for function {} cannot have null", arg_name, func_name);
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& col1 = block.get_by_position(arguments[0]).column;
        const auto& col2 = block.get_by_position(arguments[1]).column;

        // Validate no NULLs (distance functions always throw on NULL input)
        _validate_no_nulls(col1, "First argument", get_name());
        _validate_no_nulls(col2, "Second argument", get_name());

        // Create views — handles Const/Nullable unwrapping automatically
        auto view1 = ColumnArrayView<TYPE_FLOAT>::create(col1);
        auto view2 = ColumnArrayView<TYPE_FLOAT>::create(col2);

        auto dst = ColumnType::create(input_rows_count);
        auto& dst_data = dst->get_data();

        for (size_t row = 0; row < input_rows_count; ++row) {
            auto a1 = view1[row];
            auto a2 = view2[row];
            const float* p1 = a1.get_data();
            const float* p2 = a2.get_data();
            auto dim1 = a1.size();
            auto dim2 = a2.size();

            if (dim1 != dim2) [[unlikely]] {
                return Status::InvalidArgument(
                        "function {} have different input element sizes of array: {} and {}",
                        get_name(), dim1, dim2);
            }
            dst_data[row] = DistanceImpl::distance(p1, p2, dim1);
        }

        block.replace_by_position(result, std::move(dst));
        return Status::OK();
    }
};

} // namespace doris
