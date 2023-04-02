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

#include "vec/columns/column.h"
#include "vec/exprs/table_function/table_function.h"

namespace doris::vectorized {

class VExplodeNumbersTableFunction : public TableFunction {
public:
    VExplodeNumbersTableFunction();
    ~VExplodeNumbersTableFunction() override = default;

    Status process_init(Block* block) override;
    Status process_row(size_t row_idx) override;
    Status process_close() override;
    void get_value(MutableColumnPtr& column) override;
    int get_value(MutableColumnPtr& column, int max_step) override {
        if (_is_const) {
            max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
            if (_is_nullable) {
                static_cast<ColumnInt32*>(
                        static_cast<ColumnNullable*>(column.get())->get_nested_column_ptr().get())
                        ->insert_many_from(*_elements_column, _cur_offset, max_step);
                static_cast<ColumnUInt8*>(
                        static_cast<ColumnNullable*>(column.get())->get_null_map_column_ptr().get())
                        ->insert_many_defaults(max_step);
            } else {
                static_cast<ColumnInt32*>(column.get())
                        ->insert_many_from(*_elements_column, _cur_offset, max_step);
            }

            forward(max_step);
            return max_step;
        }

        return TableFunction::get_value(column, max_step);
    }

private:
    ColumnPtr _value_column;
    ColumnPtr _elements_column = ColumnInt32::create();
};

} // namespace doris::vectorized
