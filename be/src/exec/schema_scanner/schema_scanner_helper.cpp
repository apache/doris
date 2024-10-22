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

#include "exec/schema_scanner/schema_scanner_helper.h"

#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

void SchemaScannerHelper::insert_string_value(int col_index, std::string str_val,
                                              vectorized::Block* block) {
    vectorized::MutableColumnPtr mutable_col_ptr;
    mutable_col_ptr = std::move(*block->get_by_position(col_index).column).assume_mutable();
    auto* nullable_column = assert_cast<vectorized::ColumnNullable*>(mutable_col_ptr.get());
    vectorized::IColumn* col_ptr = &nullable_column->get_nested_column();
    assert_cast<vectorized::ColumnString*>(col_ptr)->insert_data(str_val.data(), str_val.size());
    nullable_column->get_null_map_data().emplace_back(0);
}

void SchemaScannerHelper::insert_datetime_value(int col_index, const std::vector<void*>& datas,
                                                vectorized::Block* block) {
    vectorized::MutableColumnPtr mutable_col_ptr;
    mutable_col_ptr = std::move(*block->get_by_position(col_index).column).assume_mutable();
    auto* nullable_column = assert_cast<vectorized::ColumnNullable*>(mutable_col_ptr.get());
    vectorized::IColumn* col_ptr = &nullable_column->get_nested_column();
    auto data = datas[0];
    assert_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_data(
            reinterpret_cast<char*>(data), 0);
    nullable_column->get_null_map_data().emplace_back(0);
}

void SchemaScannerHelper::insert_int64_value(int col_index, int64_t int_val,
                                             vectorized::Block* block) {
    vectorized::MutableColumnPtr mutable_col_ptr;
    mutable_col_ptr = std::move(*block->get_by_position(col_index).column).assume_mutable();
    auto* nullable_column = assert_cast<vectorized::ColumnNullable*>(mutable_col_ptr.get());
    vectorized::IColumn* col_ptr = &nullable_column->get_nested_column();
    assert_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(int_val);
    nullable_column->get_null_map_data().emplace_back(0);
}

void SchemaScannerHelper::insert_double_value(int col_index, double double_val,
                                              vectorized::Block* block) {
    vectorized::MutableColumnPtr mutable_col_ptr;
    mutable_col_ptr = std::move(*block->get_by_position(col_index).column).assume_mutable();
    auto* nullable_column = assert_cast<vectorized::ColumnNullable*>(mutable_col_ptr.get());
    vectorized::IColumn* col_ptr = &nullable_column->get_nested_column();
    assert_cast<vectorized::ColumnVector<vectorized::Float64>*>(col_ptr)->insert_value(double_val);
    nullable_column->get_null_map_data().emplace_back(0);
}
} // namespace doris
