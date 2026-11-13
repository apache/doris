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

#include <cstring>

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

        return make_nullable(std::make_shared<DataTypeFloat32>());
    }

    // Handle outer NULLs explicitly so their nested array payload is never inspected.
    bool use_default_implementation_for_nulls() const override { return false; }

    static Status _validate_array_elements(const ColumnArrayView<TYPE_FLOAT>& view, size_t num_rows,
                                           const char* arg_name) {
        for (size_t row = 0; row < num_rows; ++row) {
            if (view.is_null_at(row)) {
                // The nested payload of a NULL data row is not a vector.
                continue;
            }
            const auto array = view[row];
            if (array.size() == 0) {
                continue;
            }
            if (std::memchr(array.get_null_map_data(), 1, array.size()) != nullptr) {
                return Status::InvalidArgument("{} for function {} cannot have null", arg_name,
                                               name);
            }
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& col1 = block.get_by_position(arguments[0]).column;
        const auto& col2 = block.get_by_position(arguments[1]).column;

        auto view1 = ColumnArrayView<TYPE_FLOAT>::create(col1);
        auto view2 = ColumnArrayView<TYPE_FLOAT>::create(col2);
        RETURN_IF_ERROR(_validate_array_elements(view1, view1.is_const ? 1 : input_rows_count,
                                                 "First argument"));
        RETURN_IF_ERROR(_validate_array_elements(view2, view2.is_const ? 1 : input_rows_count,
                                                 "Second argument"));

        auto dst = ColumnType::create(input_rows_count);
        auto& dst_data = dst->get_data();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        for (size_t row = 0; row < input_rows_count; ++row) {
            // Missing vectors have no distance, including NULL/NULL and []/[].
            if (view1.is_null_at(row) || view2.is_null_at(row) || view1[row].size() == 0 ||
                view2[row].size() == 0) {
                null_map->get_data()[row] = 1;
                dst_data[row] = 0;
                continue;
            }
            auto a1 = view1[row];
            auto a2 = view2[row];
            const auto dim1 = a1.size();
            const auto dim2 = a2.size();
            if (dim1 != dim2) [[unlikely]] {
                return Status::InvalidArgument(
                        "function {} have different input element sizes of array: {} and {}",
                        get_name(), dim1, dim2);
            }
            dst_data[row] = DistanceImpl::distance(a1.get_data(), a2.get_data(), dim1);
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(dst), std::move(null_map)));
        return Status::OK();
    }
};

} // namespace doris
