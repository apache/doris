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

#include "cast_base.h"
namespace doris::vectorized {
class CastToString {
public:
    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count,
                               const NullMap::value_type* null_map = nullptr) {
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IDataType& type = *col_with_type_and_name.type;
        const IColumn& col_from = *col_with_type_and_name.column;

        auto col_to = ColumnString::create();
        type.to_string_batch(col_from, *col_to);

        block.replace_by_position(result, std::move(col_to));
        return Status::OK();
    }
};

namespace CastWrapper {

WrapperType create_string_wrapper(const DataTypePtr& from_type) {
    return [](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
              uint32_t result, size_t input_rows_count,
              const NullMap::value_type* null_map = nullptr) {
        return CastToString::execute_impl(context, block, arguments, result, input_rows_count,
                                          null_map);
    };
}

}; // namespace CastWrapper
} // namespace doris::vectorized