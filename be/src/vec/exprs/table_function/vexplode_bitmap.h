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

#include "util/bitmap_value.h"
#include "vec/columns/column.h"
#include "vec/exprs/table_function/table_function.h"

namespace doris::vectorized {

class VExplodeBitmapTableFunction final : public TableFunction {
public:
    VExplodeBitmapTableFunction();
    ~VExplodeBitmapTableFunction() override = default;

    Status reset() override;
    Status get_value(void** output) override;
    Status forward(bool* eos) override;

    Status process_init(vectorized::Block* block) override;
    Status process_row(size_t row_idx) override;
    Status process_close() override;
    Status get_value_length(int64_t* length) override;

private:
    void _reset_iterator();
    // Not own object, just a reference
    const BitmapValue* _cur_bitmap = nullptr;
    // iterator of _cur_bitmap
    std::unique_ptr<BitmapValueIterator> _cur_iter = nullptr;
    // current value read from bitmap, it will be referenced by
    // table function scan node.
    uint64_t _cur_value = 0;
    ColumnPtr _value_column;
};

} // namespace doris::vectorized
