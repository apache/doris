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

#include <stddef.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "udf/udf.h"
#include "vec/columns/column_array.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class FunctionBM25 : public IFunction {
public:
    static constexpr auto name = "bm25";
    static FunctionPtr create() { return std::make_shared<FunctionBM25>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    bool is_variadic() const override { return false; }
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeFloat32>();
    }

    /**
    *
    * BM25 is a special function. It must be pushed down to the storage layer for computation.
    * When users call the BM25() function in Doris, the optimizer will rewrite it to
    * BM25(__virtual_proj_col). Here, __virtual_proj_col is a generated column from the searched
    * table. Through __virtual_proj_col, the execution engine retrieves the scores of text similarity
    * from the storage layer.
    */
    void check_number_of_arguments(size_t number_of_arguments) const override {
        if (number_of_arguments != 1) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                "Number of arguments for function {} doesn't match: passed {}, should be {}.",
                       get_name(), number_of_arguments, get_number_of_arguments());
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 1);

        auto res_col = ColumnFloat32::create(input_rows_count);
        auto& res_data = static_cast<ColumnFloat32&>(*res_col).get_data();
        auto nullable_v_proj_col = check_and_get_column<ColumnNullable>(
            block.get_by_position(arguments[0]).column.get());

        if (UNLIKELY(!nullable_v_proj_col)) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                "The virtual projection column type for function {} doesn't match: "
                "passed {}, should be Nullable(Float32).",
           get_name(), block.get_by_position(arguments[0]).column->get_name());
        }

        auto v_proj_col = check_and_get_column<ColumnFloat32>(
            nullable_v_proj_col->get_nested_column());

        if (UNLIKELY(!v_proj_col)) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                "The virtual projection column type for function {} doesn't match: "
                "passed {}, should be Nullable(Float32).",
           get_name(), block.get_by_position(arguments[0]).column->get_name());
        }

        for (int i = 0; i < input_rows_count; i++) {
            if (nullable_v_proj_col->is_null_at(i)) {
                res_data[i] = 0.0f;
            } else {
                res_data[i] = v_proj_col->get_element(i);
            }
        }

        block.replace_by_position(result, std::move(res_col));

        return Status::OK();
    }
};

} // namespace doris::vectorized
