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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/lambda_function/lambda_function.h"
#include "vec/exprs/lambda_function/lambda_function_factory.h"
#include "vec/exprs/vexpr.h"
#include "vec/utils/util.hpp"

namespace doris {
namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class ArrayMapFunction : public LambdaFunction {
    ENABLE_FACTORY_CREATOR(ArrayMapFunction);

public:
    ~ArrayMapFunction() override = default;

    static constexpr auto name = "array_map";

    static LambdaFunctionPtr create() { return std::make_shared<ArrayMapFunction>(); }

    std::string get_name() const override { return name; }

    doris::Status execute(VExprContext* context, doris::vectorized::Block* block,
                          int* result_column_id, const DataTypePtr& result_type,
                          const VExprSPtrs& children) override {
        ///* array_map(lambda,arg1,arg2,.....) *///

        //1. child[1:end]->execute(src_block)
        doris::vectorized::ColumnNumbers arguments(children.size() - 1);
        for (int i = 1; i < children.size(); ++i) {
            int column_id = -1;
            RETURN_IF_ERROR(children[i]->execute(context, block, &column_id));
            arguments[i - 1] = column_id;
        }

        // used for save column array outside null map
        auto outside_null_map =
                ColumnUInt8::create(block->get_by_position(arguments[0])
                                            .column->convert_to_full_column_if_const()
                                            ->size(),
                                    0);
        // offset column
        MutableColumnPtr array_column_offset;
        int nested_array_column_rows = 0;
        ColumnPtr first_array_offsets = nullptr;

        //2. get the result column from executed expr, and the needed is nested column of array
        Block lambda_block;
        for (int i = 0; i < arguments.size(); ++i) {
            const auto& array_column_type_name = block->get_by_position(arguments[i]);
            auto column_array = array_column_type_name.column->convert_to_full_column_if_const();
            auto type_array = array_column_type_name.type;

            if (type_array->is_nullable()) {
                // get the nullmap of nullable column
                const auto& column_array_nullmap =
                        assert_cast<const ColumnNullable&>(*column_array).get_null_map_column();

                // get the array column from nullable column
                column_array = assert_cast<const ColumnNullable*>(column_array.get())
                                       ->get_nested_column_ptr();

                // get the nested type from nullable type
                type_array = assert_cast<const DataTypeNullable*>(array_column_type_name.type.get())
                                     ->get_nested_type();

                // need to union nullmap from all columns
                VectorizedUtils::update_null_map(outside_null_map->get_data(),
                                                 column_array_nullmap.get_data());
            }

            // here is the array column
            const ColumnArray& col_array = assert_cast<const ColumnArray&>(*column_array);
            const auto& col_type = assert_cast<const DataTypeArray&>(*type_array);

            if (i == 0) {
                nested_array_column_rows = col_array.get_data_ptr()->size();
                first_array_offsets = col_array.get_offsets_ptr();
                auto& off_data = assert_cast<const ColumnArray::ColumnOffsets&>(
                        col_array.get_offsets_column());
                array_column_offset = off_data.clone_resized(col_array.get_offsets_column().size());
            } else {
                // select array_map((x,y)->x+y,c_array1,[0,1,2,3]) from array_test2;
                // c_array1: [0,1,2,3,4,5,6,7,8,9]
                auto& array_offsets =
                        assert_cast<const ColumnArray::ColumnOffsets&>(*first_array_offsets)
                                .get_data();
                if (nested_array_column_rows != col_array.get_data_ptr()->size() ||
                    (array_offsets.size() > 0 &&
                     memcmp(array_offsets.data(), col_array.get_offsets().data(),
                            sizeof(array_offsets[0]) * array_offsets.size()) != 0)) {
                    return Status::InternalError(
                            "in array map function, the input column size "
                            "are "
                            "not equal completely, nested column data rows 1st size is {}, {}th "
                            "size is {}.",
                            nested_array_column_rows, i + 1, col_array.get_data_ptr()->size());
                }
            }

            // insert the data column to the new block
            ColumnWithTypeAndName data_column {col_array.get_data_ptr(), col_type.get_nested_type(),
                                               "R" + array_column_type_name.name};
            lambda_block.insert(std::move(data_column));
        }

        //3. child[0]->execute(new_block)
        RETURN_IF_ERROR(children[0]->execute(context, &lambda_block, result_column_id));

        auto res_col = lambda_block.get_by_position(*result_column_id)
                               .column->convert_to_full_column_if_const();
        auto res_type = lambda_block.get_by_position(*result_column_id).type;
        auto res_name = lambda_block.get_by_position(*result_column_id).name;

        //4. get the result column after execution, reassemble it into a new array column, and return.
        ColumnWithTypeAndName result_arr;
        if (result_type->is_nullable()) {
            if (res_type->is_nullable()) {
                result_arr = {ColumnNullable::create(
                                      ColumnArray::create(res_col, std::move(array_column_offset)),
                                      std::move(outside_null_map)),
                              result_type, res_name};
            } else {
                // deal with eg: select array_map(x -> x is null, [null, 1, 2]);
                // need to create the nested column null map for column array
                auto nested_null_map = ColumnUInt8::create(res_col->size(), 0);
                result_arr = {
                        ColumnNullable::create(
                                ColumnArray::create(
                                        ColumnNullable::create(res_col, std::move(nested_null_map)),
                                        std::move(array_column_offset)),
                                std::move(outside_null_map)),
                        result_type, res_name};
            }
        } else {
            if (res_type->is_nullable()) {
                result_arr = {ColumnArray::create(res_col, std::move(array_column_offset)),
                              result_type, res_name};
            } else {
                auto nested_null_map = ColumnUInt8::create(res_col->size(), 0);
                result_arr = {ColumnArray::create(
                                      ColumnNullable::create(res_col, std::move(nested_null_map)),
                                      std::move(array_column_offset)),
                              result_type, res_name};
            }
        }
        block->insert(std::move(result_arr));
        *result_column_id = block->columns() - 1;
        return Status::OK();
    }
};

void register_function_array_map(doris::vectorized::LambdaFunctionFactory& factory) {
    factory.register_function<ArrayMapFunction>();
}
} // namespace doris::vectorized
