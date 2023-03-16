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

#include "exprs/rpc_fn_comm.h"
namespace doris::vectorized {
void convert_nullable_col_to_pvalue(const vectorized::ColumnPtr& column,
                                    const vectorized::DataTypePtr& data_type,
                                    const vectorized::ColumnUInt8& null_col, PValues* arg,
                                    int start, int end) {
    int row_count = end - start;
    if (column->has_null(row_count)) {
        auto* null_map = arg->mutable_null_map();
        null_map->Reserve(row_count);
        const auto* col = vectorized::check_and_get_column<vectorized::ColumnUInt8>(null_col);
        auto& data = col->get_data();
        null_map->Add(data.begin() + start, data.begin() + end);
        convert_col_to_pvalue<true>(column, data_type, arg, start, end);
    } else {
        convert_col_to_pvalue<false>(column, data_type, arg, start, end);
    }
}

void convert_block_to_proto(vectorized::Block& block, const vectorized::ColumnNumbers& arguments,
                            size_t input_rows_count, PFunctionCallRequest* request) {
    size_t row_count = std::min(block.rows(), input_rows_count);
    for (size_t col_idx : arguments) {
        PValues* arg = request->add_args();
        vectorized::ColumnWithTypeAndName& column = block.get_by_position(col_idx);
        arg->set_has_null(column.column->has_null(row_count));
        auto col = column.column->convert_to_full_column_if_const();
        if (auto* nullable =
                    vectorized::check_and_get_column<const vectorized::ColumnNullable>(*col)) {
            auto data_col = nullable->get_nested_column_ptr();
            auto& null_col = nullable->get_null_map_column();
            auto data_type =
                    std::reinterpret_pointer_cast<const vectorized::DataTypeNullable>(column.type);
            convert_nullable_col_to_pvalue(data_col->convert_to_full_column_if_const(),
                                           data_type->get_nested_type(), null_col, arg, 0,
                                           row_count);
        } else {
            convert_col_to_pvalue<false>(col, column.type, arg, 0, row_count);
        }
    }
}

void convert_to_block(vectorized::Block& block, const PValues& result, size_t pos) {
    auto data_type = block.get_data_type(pos);
    if (data_type->is_nullable()) {
        auto null_type =
                std::reinterpret_pointer_cast<const vectorized::DataTypeNullable>(data_type);
        auto data_col = null_type->get_nested_type()->create_column();
        convert_to_column<true>(data_col, result);
        auto null_col = vectorized::ColumnUInt8::create(data_col->size(), 0);
        auto& null_map_data = null_col->get_data();
        null_col->reserve(data_col->size());
        null_col->resize(data_col->size());
        if (result.has_null()) {
            for (int i = 0; i < data_col->size(); ++i) {
                null_map_data[i] = result.null_map(i);
            }
        } else {
            for (int i = 0; i < data_col->size(); ++i) {
                null_map_data[i] = false;
            }
        }
        block.replace_by_position(
                pos, vectorized::ColumnNullable::create(std::move(data_col), std::move(null_col)));
    } else {
        auto column = data_type->create_column();
        convert_to_column<false>(column, result);
        block.replace_by_position(pos, std::move(column));
    }
}

} // namespace doris::vectorized