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
#include "core/data_type/data_type.h"
#include "exprs/table_function/table_function.h"

namespace doris {
class Block;

// json_each('{"a":"foo","b":123}') →
// | key | value        |
// | a   | "foo" (JSON) |
// | b   | 123   (JSON) |
//
// json_each_text('{"a":"foo","b":123}') →
// | key | value   |
// | a   | foo     |  ← string unquoted
// | b   | 123     |  ← number as text
//
// TEXT_MODE=false → json_each  (value column type: JSONB binary)
// TEXT_MODE=true  → json_each_text (value column type: plain STRING)
template <bool TEXT_MODE>
class VJsonEachTableFunction : public TableFunction {
    ENABLE_FACTORY_CREATOR(VJsonEachTableFunction);

public:
    VJsonEachTableFunction();

    ~VJsonEachTableFunction() override = default;

    Status process_init(Block* block, RuntimeState* state) override;
    void process_row(size_t row_idx) override;
    void process_close() override;
    void get_same_many_values(MutableColumnPtr& column, int length) override;
    int get_value(MutableColumnPtr& column, int max_step) override;

#ifdef BE_TEST
    const ColumnPtr& test_json_column() const { return _json_column; }
    const MutableColumnPtr& test_kv_pairs_first() const { return _kv_pairs.first; }
    const MutableColumnPtr& test_kv_pairs_second() const { return _kv_pairs.second; }
#endif

private:
    ColumnPtr _json_column;
    // _kv_pairs.first  : ColumnNullable<ColumnString>  key (always plain text)
    // _kv_pairs.second : ColumnNullable<ColumnString>  value (JSONB bytes or plain text)
    std::pair<MutableColumnPtr, MutableColumnPtr> _kv_pairs;
};

using VJsonEachTableFn = VJsonEachTableFunction<false>;
using VJsonEachTextTableFn = VJsonEachTableFunction<true>;

} // namespace doris
