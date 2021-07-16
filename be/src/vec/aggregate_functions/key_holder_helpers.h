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
#include "vec/common/hash_table/hash_table_key_holder.h"

namespace doris::vectorized {

template <bool is_plain_column = false>
static auto get_key_holder(const IColumn& column, size_t row_num, Arena& arena) {
    if constexpr (is_plain_column) {
        return ArenaKeyHolder{column.get_data_at(row_num), arena};
    } else {
        const char* begin = nullptr;
        StringRef serialized = column.serialize_value_into_arena(row_num, arena, begin);
        assert(serialized.data != nullptr);
        return SerializedKeyHolder{serialized, arena};
    }
}

template <bool is_plain_column>
static void deserialize_and_insert(StringRef str, IColumn& data_to) {
    if constexpr (is_plain_column)
        data_to.insert_data(str.data, str.size);
    else
        data_to.deserialize_and_insert_from_arena(str.data);
}

} // namespace doris::vectorized
