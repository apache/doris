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

#include <fmt/core.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/lambda_function/lambda_function.h"
#include "vec/exprs/lambda_function/lambda_function_factory.h"
#include "vec/exprs/vcolumn_ref.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class VExprContext;

// extend a block with all required parameters
struct ReduceLambdaArgs {
    // the lambda function need the column ids of all the slots
    std::vector<int> output_slot_ref_indexs;
    // which line is extended to the original block
    int64_t current_row_idx = 0;
    // the size of the current array
    int64_t cur_size = 0;
    // offset of column array
    const ColumnArray::Offsets64* offsets_ptr = nullptr;
};

class ArrayReduceFunction : public LambdaFunction {
    ENABLE_FACTORY_CREATOR(ArrayReduceFunction);

public:
    ~ArrayReduceFunction() override = default;

    static constexpr auto name = "array_reduce";

    static LambdaFunctionPtr create() { return std::make_shared<ArrayReduceFunction>(); }

    std::string get_name() const override { return name; }

    doris::Status execute(VExprContext* context, doris::vectorized::Block* block,
                          int* result_column_id, const DataTypePtr& result_type,
                          const VExprSPtrs& children) override {
        ReduceLambdaArgs args;
        std::vector<int>& output_slot_ref_indexs = args.output_slot_ref_indexs;
        // collect used slot ref in lambda function body
        _collect_slot_ref_column_id(children[0], output_slot_ref_indexs);

        int gap = 0;
        if (!args.output_slot_ref_indexs.empty()) {
            auto max_id = std::max_element(args.output_slot_ref_indexs.begin(),
                                           args.output_slot_ref_indexs.end());
            gap = *max_id + 1;
            _set_column_ref_column_id(children[0], gap);
        }

        std::vector<std::string> names(gap);
        DataTypes data_types(gap);

        for (int i = 0; i < gap; ++i) {
            if (_contains_column_id(output_slot_ref_indexs, i)) {
                names[i] = block->get_by_position(i).name;
                data_types[i] = block->get_by_position(i).type;
            } else {
                // padding some mock data
                names[i] = "temp";
                data_types[i] = std::make_shared<DataTypeUInt8>();
            }
        }

        // children size = 3, lambda_fn, array, array(initial_value)
        doris::vectorized::ColumnNumbers arguments(children.size() - 1);
        for (int i = 1; i < children.size(); ++i) {
            int column_id = -1;
            RETURN_IF_ERROR(children[i]->execute(context, block, &column_id));
            arguments[i - 1] = column_id;
        }
        /*
			arguments[0]: array
			arguments[1]: array(initial_value)
		*/

        //2. get the result column from executed expr, and the needed is nested column of array
        std::vector<ColumnPtr> lambda_expr_arguments_cols(arguments.size());

        // arguments.size = 2, first is array
        for (int i = 0; i < arguments.size(); ++i) {
            const auto& array_column_type_name = block->get_by_position(arguments[i]);
            auto column_array = array_column_type_name.column->convert_to_full_column_if_const();
            auto type_array = array_column_type_name.type;
            if (type_array->is_nullable()) {
                // get the array column from nullable column
                column_array = assert_cast<const ColumnNullable*>(column_array.get())
                                       ->get_nested_column_ptr();

                // get the nested type from nullable type
                type_array = assert_cast<const DataTypeNullable*>(array_column_type_name.type.get())
                                     ->get_nested_type();
            }

            // here is the array column
            const auto& col_array = assert_cast<const ColumnArray&>(*column_array);
            const auto& col_type = assert_cast<const DataTypeArray&>(*type_array);

            if (i == 0) {
                args.offsets_ptr = &col_array.get_offsets();
            }
            lambda_expr_arguments_cols[i] = column_array;
            names.push_back("R" + array_column_type_name.name);
            data_types.push_back(col_type.get_nested_type());
        }

        ColumnPtr result_col = nullptr;
        DataTypePtr res_type;
        std::string res_name;

        // cur_size equals num of elems in an array
        args.cur_size = (*args.offsets_ptr)[args.current_row_idx] -
                        (*args.offsets_ptr)[args.current_row_idx - 1];

        while (args.current_row_idx < block->rows()) {
            Block final_block;
            for (int i = 0; i < gap; i++) {
                ColumnWithTypeAndName data_column;
                if (_contains_column_id(output_slot_ref_indexs, i) || i >= gap) {
                    data_column = ColumnWithTypeAndName(data_types[i], names[i]);
                } else {
                    data_column = ColumnWithTypeAndName(
                            data_types[i]->create_column_const_with_default_value(0), data_types[i],
                            names[i]);
                }
                final_block.insert(std::move(data_column));
            } // final_block columns num = gap

            auto array_size = (int)args.cur_size;
            for (int i = 0; i < array_size; i++) {
                ColumnWithTypeAndName data_column;
                data_column =
                        ColumnWithTypeAndName(result_type, fmt::format("reduce temp col {}", i));
                final_block.insert(std::move(data_column));
            } // final_block columns num = gap + array_size

            // append initial_value column into final_block
            {
                auto data_column = ColumnWithTypeAndName(result_type, "reduce initial column");
                final_block.insert(std::move(data_column));
            }

            MutableColumns columns = final_block.mutate_columns();
            // start generating final_block arguments -> (gap..gap + array)
            const auto& col_array = assert_cast<const ColumnArray&>(*lambda_expr_arguments_cols[0]);
            for (int i = 0; i < array_size; i++) {
                Field field;
                col_array.get(args.current_row_idx, field);
                const auto& array = field.get<Array>();
                DCHECK_EQ(array.size(), array_size);
                columns[gap + i]->insert(array[i]);
            }

            _add_initial_value_column_in_reduce(columns, block, args, gap + array_size,
                                                arguments[1]);
            final_block.set_columns(std::move(columns));

            // run lambda function
            // final_block layout is [gap, array_size, initial]
            DataTypes lambda_types;
            lambda_types.resize(2);

            for (int i = 0; i < array_size; i++) {
                Block lambda_block;
                // initial value must push to the first place
                lambda_types[0] = final_block.get_by_position(gap + array_size).type;
                lambda_types[1] = final_block.get_by_position(gap + i).type;

                for (int j = 0; j < 2; j++) {
                    auto data_column = ColumnWithTypeAndName(lambda_types[j],
                                                             fmt::format("lambda temp col {}", j));
                    lambda_block.insert(std::move(data_column));
                }
                Columns cols;
                cols.push_back(final_block.get_columns()[gap + array_size]);
                cols.push_back(final_block.get_columns()[gap + i]);

                lambda_block.set_columns(cols);
                RETURN_IF_ERROR(children[0]->execute(context, &lambda_block, result_column_id));
                auto res_col = lambda_block.get_by_position(*result_column_id).column;

                res_type = lambda_block.get_by_position(*result_column_id).type;
                res_name = lambda_block.get_by_position(*result_column_id).name;

                final_block.get_by_position(gap + array_size) = ColumnWithTypeAndName(
                        _cast_to_type(res_col, result_type), result_type, "lambda tmp result");
            }

            auto res_col = final_block.get_by_position(gap + array_size)
                                   .column->convert_to_full_column_if_const();
            DCHECK_EQ(res_col->size(), 1);
            // one row end
            if (!result_col) {
                result_col = std::move(res_col);
            } else {
                MutableColumnPtr column = (*std::move(result_col)).mutate();
                column->insert_range_from(*res_col, 0, res_col->size());
            }

            args.current_row_idx += 1;
            args.cur_size = (*args.offsets_ptr)[args.current_row_idx] -
                            (*args.offsets_ptr)[args.current_row_idx - 1];
        }

        // 4. get the result column after execution, reassemble it into a new array column, and return.
        ColumnWithTypeAndName final_res_col = {result_col, result_type, res_name};
        block->insert(std::move(final_res_col));
        *result_column_id = block->columns() - 1;

        return Status::OK();
    }

private:
    void _add_initial_value_column_in_reduce(std::vector<MutableColumnPtr>& columns, Block* block,
                                             ReduceLambdaArgs& args, int res_col_id,
                                             int src_col_id) {
        const auto& array_col_type_name = block->get_by_position(src_col_id);
        auto src_column = array_col_type_name.column
                                  ->convert_to_full_column_if_const(); // column of array(len = 1)
        if (array_col_type_name.type->is_nullable()) {
            src_column =
                    assert_cast<const ColumnNullable*>(src_column.get())->get_nested_column_ptr();
        }
        // get element at row index
        Field fld;
        const auto& col_arr = assert_cast<const ColumnArray&>(*src_column);
        col_arr.get(args.current_row_idx, fld);
        const auto& arr = fld.get<Array>();
        DCHECK_EQ(arr.size(), 1);
        columns[res_col_id]->insert(arr[0]);
    }

    static ColumnPtr _cast_to_type(ColumnPtr from, DataTypePtr totype) {
        auto to = totype->create_column();
        Field fld;
        from->get(0, fld);
        to->insert(fld);
        return to;
    }
};

void register_function_array_reduce(LambdaFunctionFactory& factory) {
    factory.register_function<ArrayReduceFunction>();
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
