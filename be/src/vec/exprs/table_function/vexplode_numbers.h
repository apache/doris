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

#include <algorithm>

#include "common/status.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/table_function/table_function.h"

namespace doris::vectorized {
class Block;
} // namespace doris::vectorized

namespace doris::vectorized {

class VExplodeNumbersTableFunction : public TableFunction {
    ENABLE_FACTORY_CREATOR(VExplodeNumbersTableFunction);

public:
    VExplodeNumbersTableFunction();
    ~VExplodeNumbersTableFunction() override = default;

    Status process_init(Block* block, RuntimeState* state) override;
    void process_row(size_t row_idx) override;
    void process_close() override;
    void get_value(MutableColumnPtr& column) override;
    int get_value(MutableColumnPtr& column, int max_step) override {
        max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
        if (_is_const) {
            if (_is_nullable) {
                static_cast<ColumnInt32*>(
                        static_cast<ColumnNullable*>(column.get())->get_nested_column_ptr().get())
                        ->insert_range_from(*_elements_column, _cur_offset, max_step);
                static_cast<ColumnUInt8*>(
                        static_cast<ColumnNullable*>(column.get())->get_null_map_column_ptr().get())
                        ->insert_many_defaults(max_step);
            } else {
                static_cast<ColumnInt32*>(column.get())
                        ->insert_range_from(*_elements_column, _cur_offset, max_step);
            }
        } else {
            // should dispose the empty status, forward one step
            if (current_empty()) {
                column->insert_default();
                max_step = 1;
            } else {
                ColumnInt32* target = nullptr;
                if (_is_nullable) {
                    target = assert_cast<ColumnInt32*>(assert_cast<ColumnNullable*>(column.get())
                                                               ->get_nested_column_ptr()
                                                               .get());
                    assert_cast<ColumnUInt8*>(assert_cast<ColumnNullable*>(column.get())
                                                      ->get_null_map_column_ptr()
                                                      .get())
                            ->insert_many_defaults(max_step);
                } else {
                    target = assert_cast<ColumnInt32*>(column.get());
                }
                auto origin_size = target->size();
                target->resize(origin_size + max_step);
                std::iota(target->get_data().data() + origin_size,
                          target->get_data().data() + origin_size + max_step, _cur_offset);
            }
        }
        forward(max_step);
        return max_step;
    }

private:
    ColumnPtr _value_column;
    ColumnPtr _elements_column = ColumnInt32::create();
};

} // namespace doris::vectorized
