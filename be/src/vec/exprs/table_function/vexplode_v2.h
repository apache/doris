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

#include <cstddef>

#include "common/status.h"
#include "vec/columns/column_struct.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/functions/array/function_array_utils.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class Block;
} // namespace doris::vectorized

namespace doris::vectorized {

class VExplodeV2TableFunction : public TableFunction {
    ENABLE_FACTORY_CREATOR(VExplodeV2TableFunction);

public:
    VExplodeV2TableFunction();

    ~VExplodeV2TableFunction() override = default;

    Status process_init(Block* block, RuntimeState* state) override;
    void process_row(size_t row_idx) override;
    void process_close() override;
    void get_same_many_values(MutableColumnPtr& column, int length) override;
    int get_value(MutableColumnPtr& column, int max_step) override;

    void set_generate_row_index(bool generate_row_index) {
        _generate_row_index = generate_row_index;
    }

private:
    Status _process_init_variant(Block* block, int value_column_idx, int children_column_idx);
    std::vector<ColumnPtr> _array_columns;
    size_t _row_idx {0};
    ColumnArrayExecutionDatas _multi_detail;
    std::vector<size_t> _array_offsets;

    // `posexplode` & `posexplode_outer`
    bool _generate_row_index {false};
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
