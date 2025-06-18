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

#include <vec/data_types/data_type_number.h>
#include <vec/exprs/vcolumn_ref.h>
#include <vec/exprs/vslot_ref.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/lambda_function/lambda_function.h"
#include "vec/exprs/lambda_function/lambda_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class VExprContext;

// extend a block with all required parameters
struct LambdaArgs {
    // the lambda function need the column ids of all the slots
    std::vector<int> output_slot_ref_indexs;
    // which line is extended to the original block
    int64_t current_row_idx = 0;
    // when a block is filled, the array may be truncated, recording where it was truncated
    int64_t current_offset_in_array = 0;
    // the beginning position of the array
    size_t array_start = 0;
    // the size of the array
    int64_t cur_size = 0;
    // offset of column array
    const ColumnArray::Offsets64* offsets_ptr = nullptr;
    // expend data of repeat times
    int current_repeat_times = 0;
    // whether the current row of the original block has been extended
    bool current_row_eos = false;
};

class ArrayMapFunction : public LambdaFunction {
    ENABLE_FACTORY_CREATOR(ArrayMapFunction);

public:
    ~ArrayMapFunction() override = default;

    static constexpr auto name = "array_map";

    static LambdaFunctionPtr create() { return std::make_shared<ArrayMapFunction>(); }

    std::string get_name() const override { return name; }

    Status execute(VExprContext* context, vectorized::Block* block, int* result_column_id,
                   const DataTypePtr& result_type, const VExprSPtrs& children) override {
        LambdaArgs args_info;
        // collect used slot ref in lambda function body
        std::vector<int>& output_slot_ref_indexs = args_info.output_slot_ref_indexs;
        _collect_slot_ref_column_id(children[0], output_slot_ref_indexs);

        int gap = 0;
        if (!output_slot_ref_indexs.empty()) {
            auto max_id =
                    std::max_element(output_slot_ref_indexs.begin(), output_slot_ref_indexs.end());
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
                // padding some mock data to hold the position, like call block#rows function need
                names[i] = "temp";
                data_types[i] = std::make_shared<DataTypeUInt8>();
            }
        }

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
        size_t nested_array_column_rows = 0;
        ColumnPtr first_array_offsets = nullptr;
        //2. get the result column from executed expr, and the needed is nested column of array
        std::vector<ColumnPtr> lambda_datas(arguments.size());

        for (int i = 0; i < arguments.size(); ++i) {
            const auto& array_column_type_name = block->get_by_position(arguments[i]);
            auto column_array = array_column_type_name.column->convert_to_full_column_if_const();
            auto type_array = array_column_type_name.type;
            if (type_array->is_nullable()) {
                // get the nullmap of nullable column
                // hold the null column instead of a reference 'cause `column_array` will be assigned and freed below.
                auto column_array_nullmap =
                        assert_cast<const ColumnNullable&>(*column_array).get_null_map_column_ptr();

                // get the array column from nullable column
                column_array = assert_cast<const ColumnNullable*>(column_array.get())
                                       ->get_nested_column_ptr();

                // get the nested type from nullable type
                type_array = assert_cast<const DataTypeNullable*>(array_column_type_name.type.get())
                                     ->get_nested_type();

                // need to union nullmap from all columns
                VectorizedUtils::update_null_map(
                        outside_null_map->get_data(),
                        assert_cast<const ColumnUInt8&>(*column_array_nullmap).get_data());
            }

            // here is the array column
            const auto& col_array = assert_cast<const ColumnArray&>(*column_array);
            const auto& col_type = assert_cast<const DataTypeArray&>(*type_array);

            if (i == 0) {
                nested_array_column_rows = col_array.get_data_ptr()->size();
                first_array_offsets = col_array.get_offsets_ptr();
                const auto& off_data = assert_cast<const ColumnArray::ColumnOffsets&>(
                        col_array.get_offsets_column());
                array_column_offset = off_data.clone_resized(col_array.get_offsets_column().size());
                args_info.offsets_ptr = &col_array.get_offsets();
            } else {
                // select array_map((x,y)->x+y,c_array1,[0,1,2,3]) from array_test2;
                // c_array1: [0,1,2,3,4,5,6,7,8,9]
                const auto& array_offsets =
                        assert_cast<const ColumnArray::ColumnOffsets&>(*first_array_offsets)
                                .get_data();
                if (nested_array_column_rows != col_array.get_data_ptr()->size() ||
                    (!array_offsets.empty() &&
                     memcmp(array_offsets.data(), col_array.get_offsets().data(),
                            sizeof(array_offsets[0]) * array_offsets.size()) != 0)) {
                    return Status::InvalidArgument(
                            "in array map function, the input column size "
                            "are "
                            "not equal completely, nested column data rows 1st size is {}, {}th "
                            "size is {}.",
                            nested_array_column_rows, i + 1, col_array.get_data_ptr()->size());
                }
            }
            lambda_datas[i] = col_array.get_data_ptr();
            names.push_back("R" + array_column_type_name.name);
            data_types.push_back(col_type.get_nested_type());
        }

        ColumnWithTypeAndName result_arr;
        // if column_array is NULL, we know the array_data_column will not write any data,
        // so the column is empty. eg : (x) -> concat('|',x + "1"). if still execute the lambda function, will cause the bolck rows are not equal
        // the x column is empty, but "|" is const literal, size of column is 1, so the block rows is 1, but the x column is empty, will be coredump.
        if (std::any_of(lambda_datas.begin(), lambda_datas.end(),
                        [](const auto& v) { return v->empty(); })) {
            DataTypePtr nested_type;
            bool is_nullable = result_type->is_nullable();
            if (is_nullable) {
                nested_type =
                        assert_cast<const DataTypeNullable*>(result_type.get())->get_nested_type();
            } else {
                nested_type = result_type;
            }
            auto empty_nested_column = assert_cast<const DataTypeArray*>(nested_type.get())
                                               ->get_nested_type()
                                               ->create_column();
            auto result_array_column = ColumnArray::create(std::move(empty_nested_column),
                                                           std::move(array_column_offset));

            if (is_nullable) {
                result_arr = {ColumnNullable::create(std::move(result_array_column),
                                                     std::move(outside_null_map)),
                              result_type, "Result"};
            } else {
                result_arr = {std::move(result_array_column), result_type, "Result"};
            }

            block->insert(result_arr);
            *result_column_id = block->columns() - 1;
            return Status::OK();
        }

        MutableColumnPtr result_col = nullptr;
        DataTypePtr res_type;
        std::string res_name;

        //process first row
        args_info.array_start = (*args_info.offsets_ptr)[args_info.current_row_idx - 1];
        args_info.cur_size =
                (*args_info.offsets_ptr)[args_info.current_row_idx] - args_info.array_start;

        // lambda block to exectute the lambda, and reuse the memory
        Block lambda_block;
        auto column_size = names.size();
        MutableColumns columns(column_size);
        do {
            bool mem_reuse = lambda_block.mem_reuse();
            for (int i = 0; i < column_size; i++) {
                if (mem_reuse) {
                    columns[i] = lambda_block.get_by_position(i).column->assume_mutable();
                } else {
                    if (_contains_column_id(output_slot_ref_indexs, i) || i >= gap) {
                        // TODO: maybe could create const column, so not insert_many_from when extand data
                        // but now here handle batch_size of array nested data every time, so maybe have different rows
                        columns[i] = data_types[i]->create_column();
                    } else {
                        columns[i] = data_types[i]
                                             ->create_column_const_with_default_value(0)
                                             ->assume_mutable();
                    }
                }
            }
            // batch_size of array nested data every time inorder to avoid memory overflow
            while (columns[gap]->size() < batch_size) {
                long max_step = batch_size - columns[gap]->size();
                long current_step = std::min(
                        max_step, (long)(args_info.cur_size - args_info.current_offset_in_array));
                size_t pos = args_info.array_start + args_info.current_offset_in_array;
                for (int i = 0; i < arguments.size() && current_step > 0; ++i) {
                    columns[gap + i]->insert_range_from(*lambda_datas[i], pos, current_step);
                }
                args_info.current_offset_in_array += current_step;
                args_info.current_repeat_times += current_step;
                if (args_info.current_offset_in_array >= args_info.cur_size) {
                    args_info.current_row_eos = true;
                }
                _extend_data(columns, block, args_info.current_repeat_times, gap,
                             args_info.current_row_idx, output_slot_ref_indexs);
                args_info.current_repeat_times = 0;
                if (args_info.current_row_eos) {
                    //current row is end of array, move to next row
                    args_info.current_row_idx++;
                    args_info.current_offset_in_array = 0;
                    if (args_info.current_row_idx >= block->rows()) {
                        break;
                    }
                    args_info.current_row_eos = false;
                    args_info.array_start = (*args_info.offsets_ptr)[args_info.current_row_idx - 1];
                    args_info.cur_size = (*args_info.offsets_ptr)[args_info.current_row_idx] -
                                         args_info.array_start;
                }
            }

            if (!mem_reuse) {
                for (int i = 0; i < column_size; ++i) {
                    lambda_block.insert(vectorized::ColumnWithTypeAndName(std::move(columns[i]),
                                                                          data_types[i], names[i]));
                }
            }
            //3. child[0]->execute(new_block)
            RETURN_IF_ERROR(children[0]->execute(context, &lambda_block, result_column_id));

            auto res_col = lambda_block.get_by_position(*result_column_id)
                                   .column->convert_to_full_column_if_const();
            res_type = lambda_block.get_by_position(*result_column_id).type;
            res_name = lambda_block.get_by_position(*result_column_id).name;
            if (!result_col) {
                result_col = res_col->clone_empty();
            }
            result_col->insert_range_from(*res_col, 0, res_col->size());
            lambda_block.clear_column_data(column_size);
        } while (args_info.current_row_idx < block->rows());

        //4. get the result column after execution, reassemble it into a new array column, and return.
        if (result_type->is_nullable()) {
            if (res_type->is_nullable()) {
                result_arr = {
                        ColumnNullable::create(ColumnArray::create(std::move(result_col),
                                                                   std::move(array_column_offset)),
                                               std::move(outside_null_map)),
                        result_type, res_name};
            } else {
                // deal with eg: select array_map(x -> x is null, [null, 1, 2]);
                // need to create the nested column null map for column array
                auto nested_null_map = ColumnUInt8::create(result_col->size(), 0);
                result_arr = {ColumnNullable::create(
                                      ColumnArray::create(
                                              ColumnNullable::create(std::move(result_col),
                                                                     std::move(nested_null_map)),
                                              std::move(array_column_offset)),
                                      std::move(outside_null_map)),
                              result_type, res_name};
            }
        } else {
            if (res_type->is_nullable()) {
                result_arr = {
                        ColumnArray::create(std::move(result_col), std::move(array_column_offset)),
                        result_type, res_name};
            } else {
                auto nested_null_map = ColumnUInt8::create(result_col->size(), 0);
                result_arr = {
                        ColumnArray::create(ColumnNullable::create(std::move(result_col),
                                                                   std::move(nested_null_map)),
                                            std::move(array_column_offset)),
                        result_type, res_name};
            }
        }
        block->insert(std::move(result_arr));
        *result_column_id = block->columns() - 1;

        return Status::OK();
    }

private:
    bool _contains_column_id(const std::vector<int>& output_slot_ref_indexs, int id) {
        const auto it = std::find(output_slot_ref_indexs.begin(), output_slot_ref_indexs.end(), id);
        return it != output_slot_ref_indexs.end();
    }

    void _set_column_ref_column_id(VExprSPtr expr, int gap) {
        for (const auto& child : expr->children()) {
            if (child->is_column_ref()) {
                auto* ref = static_cast<VColumnRef*>(child.get());
                ref->set_gap(gap);
            } else {
                _set_column_ref_column_id(child, gap);
            }
        }
    }

    void _collect_slot_ref_column_id(VExprSPtr expr, std::vector<int>& output_slot_ref_indexs) {
        for (const auto& child : expr->children()) {
            if (child->is_slot_ref()) {
                const auto* ref = static_cast<VSlotRef*>(child.get());
                output_slot_ref_indexs.push_back(ref->column_id());
            } else {
                _collect_slot_ref_column_id(child, output_slot_ref_indexs);
            }
        }
    }

    void _extend_data(std::vector<MutableColumnPtr>& columns, Block* block,
                      int current_repeat_times, int size, int64_t current_row_idx,
                      const std::vector<int>& output_slot_ref_indexs) {
        if (!current_repeat_times || !size) {
            return;
        }
        for (int i = 0; i < size; i++) {
            if (_contains_column_id(output_slot_ref_indexs, i)) {
                auto src_column =
                        block->get_by_position(i).column->convert_to_full_column_if_const();
                columns[i]->insert_many_from(*src_column, current_row_idx, current_repeat_times);
            } else {
                // must be column const
                DCHECK(is_column_const(*columns[i]));
                columns[i]->resize(columns[i]->size() + current_repeat_times);
            }
        }
    }
};

void register_function_array_map(doris::vectorized::LambdaFunctionFactory& factory) {
    factory.register_function<ArrayMapFunction>();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
