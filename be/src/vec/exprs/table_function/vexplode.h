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

#include "exprs/table_function/table_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/string_ref.h"

namespace doris::vectorized {

class VExplodeTableFunction : public TableFunction {
public:
    VExplodeTableFunction();

    virtual ~VExplodeTableFunction() = default;

    virtual Status process_init(vectorized::Block* block) override;
    virtual Status process_row(size_t row_idx) override;
    virtual Status process_close() override;
    virtual Status reset() override;
    virtual Status get_value(void** output) override;
    virtual Status get_value_length(int64_t* length) override;

private:
    const UInt8* _array_null_map;
    const ColumnArray* _array_column;
    size_t _pos;
};

} // namespace doris::vectorized
