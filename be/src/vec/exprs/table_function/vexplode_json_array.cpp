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

#include "vec/exprs/table_function/vexplode_json_array.h"

#include <glog/logging.h>
#include <inttypes.h>
#include <rapidjson/rapidjson.h>
#include <stdio.h>

#include <algorithm>
#include <limits>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
template <typename DataImpl>
VExplodeJsonArrayTableFunction<DataImpl>::VExplodeJsonArrayTableFunction() : TableFunction() {
    _fn_name = "vexplode_json_array";
}

template <typename DataImpl>
Status VExplodeJsonArrayTableFunction<DataImpl>::process_init(Block* block, RuntimeState* state) {
    CHECK(_expr_context->root()->children().size() == 1)
            << _expr_context->root()->children().size();

    int text_column_idx = -1;
    RETURN_IF_ERROR(_expr_context->root()->children()[0]->execute(_expr_context.get(), block,
                                                                  &text_column_idx));
    _text_column = block->get_by_position(text_column_idx).column;
    return Status::OK();
}

template <typename DataImpl>
void VExplodeJsonArrayTableFunction<DataImpl>::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);

    StringRef text = _text_column->get_data_at(row_idx);
    if (text.data != nullptr) {
        rapidjson::Document document;
        document.Parse(text.data, text.size);
        if (!document.HasParseError() && document.IsArray() && document.GetArray().Size()) {
            _cur_size = _parsed_data.set_output(document, document.GetArray().Size());
        }
    }
}

template <typename DataImpl>
void VExplodeJsonArrayTableFunction<DataImpl>::process_close() {
    _text_column = nullptr;
    _parsed_data.reset();
}

template <typename DataImpl>
void VExplodeJsonArrayTableFunction<DataImpl>::get_same_many_values(MutableColumnPtr& column,
                                                                    int length) {
    if (current_empty()) {
        column->insert_many_defaults(length);
    } else {
        _insert_same_many_values_into_column(column, length);
    }
}

template <typename DataImpl>
int VExplodeJsonArrayTableFunction<DataImpl>::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        _insert_values_into_column(column, max_step);
    }
    forward(max_step);
    return max_step;
}

template <typename DataImpl>
void VExplodeJsonArrayTableFunction<DataImpl>::_insert_same_many_values_into_column(
        MutableColumnPtr& column, int length) {
    if (_is_nullable) {
        auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
        auto nested_column = nullable_column->get_nested_column_ptr();

        _parsed_data.insert_many_same_value_from_parsed_data(nested_column, _cur_offset, length);

        auto* nullmap_column =
                assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
        size_t old_size = nullmap_column->size();
        nullmap_column->resize(old_size + length);
        memset(nullmap_column->get_data().data() + old_size,
               *(_parsed_data.get_null_flag_address(_cur_offset)), length * sizeof(UInt8));
    } else {
        _parsed_data.insert_many_same_value_from_parsed_data(column, _cur_offset, length);
    }
}

template <typename DataImpl>
void VExplodeJsonArrayTableFunction<DataImpl>::_insert_values_into_column(MutableColumnPtr& column,
                                                                          int max_step) {
    if (_is_nullable) {
        auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
        auto nested_column = nullable_column->get_nested_column_ptr();

        _parsed_data.insert_result_from_parsed_data(nested_column, _cur_offset, max_step);

        auto* nullmap_column =
                assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
        size_t old_size = nullmap_column->size();
        nullmap_column->resize(old_size + max_step);
        memcpy(nullmap_column->get_data().data() + old_size,
               _parsed_data.get_null_flag_address(_cur_offset), max_step * sizeof(UInt8));
    } else {
        _parsed_data.insert_result_from_parsed_data(column, _cur_offset, max_step);
    }
}

template class VExplodeJsonArrayTableFunction<ParsedDataInt>;
template class VExplodeJsonArrayTableFunction<ParsedDataDouble>;
template class VExplodeJsonArrayTableFunction<ParsedDataString>;
template class VExplodeJsonArrayTableFunction<ParsedDataJSON>;

} // namespace doris::vectorized