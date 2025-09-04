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

#include <gen_cpp/Types_types.h>

#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/columns_number.h"
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
    static float distance(const float* x, const float* y, size_t d);
};

class L2Distance {
public:
    static constexpr auto name = "l2_distance";
    static float distance(const float* x, const float* y, size_t d);
};

class InnerProduct {
public:
    static constexpr auto name = "inner_product";
    static float distance(const float* x, const float* y, size_t d);
};

class CosineDistance {
public:
    static constexpr auto name = "cosine_distance";

    static float distance(const float* x, const float* y, size_t d);
};

template <typename DistanceImpl>
class FunctionArrayDistance : public IFunction {
public:
    using ColumnType = ColumnFloat32;

    static constexpr auto name = DistanceImpl::name;
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionArrayDistance<DistanceImpl>>(); }
    bool is_variadic() const override { return false; }
    size_t get_number_of_arguments() const override { return 2; }
    bool use_default_implementation_for_nulls() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeFloat32>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto& arg1 = block.get_by_position(arguments[0]);
        const auto& arg2 = block.get_by_position(arguments[1]);
        if (!_check_input_type(arg1.type) || !_check_input_type(arg2.type)) {
            return Status::RuntimeError(fmt::format("unsupported types for function {}({}, {})",
                                                    get_name(), arg1.type->get_name(),
                                                    arg2.type->get_name()));
        }

        auto col1 = arg1.column->convert_to_full_column_if_const();
        auto col2 = arg2.column->convert_to_full_column_if_const();
        if (col1->size() != col2->size()) {
            return Status::RuntimeError(
                    fmt::format("function {} have different input array sizes: {} and {}",
                                get_name(), col1->size(), col2->size()));
        }

        ColumnArrayExecutionData arr1;
        ColumnArrayExecutionData arr2;
        if (!extract_column_array_info(*col1, arr1) || !extract_column_array_info(*col2, arr2)) {
            return Status::RuntimeError(fmt::format("unsupported types for function {}({}, {})",
                                                    get_name(), arg1.type->get_name(),
                                                    arg2.type->get_name()));
        }

        // prepare return data
        auto dst = ColumnType::create(input_rows_count);
        auto& dst_data = dst->get_data();

        const auto& offsets1 = *arr1.offsets_ptr;
        const auto& offsets2 = *arr2.offsets_ptr;
        const auto& nested_col1 = assert_cast<const ColumnType*>(arr1.nested_col.get());
        const auto& nested_col2 = assert_cast<const ColumnType*>(arr2.nested_col.get());
        for (ssize_t row = 0; row < offsets1.size(); ++row) {
            // Calculate actual array sizes for current row.
            // For nullable arrays, we cannot compare absolute offset values directly because:
            // 1. When a row is null, its offset might equal the previous offset (no elements added)
            // 2. Or it might include the array size even if the row is null (implementation dependent)
            // Therefore, we must calculate the actual array size as: offsets[row] - offsets[row-1]
            ssize_t size1 = offsets1[row] - offsets1[row - 1];
            ssize_t size2 = offsets2[row] - offsets2[row - 1];

            if (size1 != size2) [[unlikely]] {
                return Status::InvalidArgument(
                        "function {} have different input element sizes of array: {} and {}",
                        get_name(), size1, size2);
            }

            dst_data[row] = DistanceImpl::distance(
                    nested_col1->get_data().data() + offsets1[row - 1],
                    nested_col2->get_data().data() + offsets1[row - 1], size1);
        }
        block.replace_by_position(result, std::move(dst));
        return Status::OK();
    }

private:
    bool _check_input_type(const DataTypePtr& type) const {
        auto array_type = remove_nullable(type);
        if (!is_array(array_type)) {
            return false;
        }
        auto nested_type =
                remove_nullable(assert_cast<const DataTypeArray&>(*array_type).get_nested_type());
        return WhichDataType(nested_type).is_float32();
    }
};

} // namespace doris::vectorized
