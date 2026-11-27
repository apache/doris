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
#include <cstdint>
#include <cstring>

#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type.h"

namespace doris {

// The block wire layout for variable-length binary columns is shared by
// VARBINARY and spatial columns, retaining their concrete column identities.
template <typename Column>
int64_t get_binary_column_serialized_bytes(const IColumn& column) {
    const bool is_const = is_column_const(column);
    const size_t stored_rows = is_const ? 1 : column.size();
    const IColumn* data_column = &column;
    if (is_const) {
        data_column = &assert_cast<const ColumnConst&>(column).get_data_column();
    }
    const auto& typed_column = assert_cast<const Column&>(*data_column);
    size_t payload_size = 0;
    for (size_t i = 0; i < stored_rows; ++i) {
        payload_size += typed_column.get_data_at(i).size;
    }
    return sizeof(bool) + sizeof(size_t) * (2 + stored_rows) + payload_size;
}

template <typename Column>
char* serialize_binary_column(const IColumn& column, char* buf) {
    const IColumn* data_column = &column;
    size_t stored_rows = 0;
    buf = serialize_const_flag_and_row_num(&data_column, buf, &stored_rows);
    const auto& typed_column = assert_cast<const Column&>(*data_column);
    auto* sizes = reinterpret_cast<size_t*>(buf);
    for (size_t i = 0; i < stored_rows; ++i) {
        unaligned_store<size_t>(&sizes[i], typed_column.get_data_at(i).size);
    }
    char* payload = buf + sizeof(size_t) * stored_rows;
    for (size_t i = 0; i < stored_rows; ++i) {
        const auto value = typed_column.get_data_at(i);
        memcpy(payload, value.data, value.size);
        payload += unaligned_load<size_t>(&sizes[i]);
    }
    return payload;
}

template <typename Column>
const char* deserialize_binary_column(const char* buf, MutableColumnPtr* column) {
    auto* original = column->get();
    size_t stored_rows = 0;
    buf = deserialize_const_flag_and_row_num(buf, column, &stored_rows);
    auto& typed_column = assert_cast<Column&>(*original);
    const auto* sizes = reinterpret_cast<const size_t*>(buf);
    const char* payload = buf + sizeof(size_t) * stored_rows;
    for (size_t i = 0; i < stored_rows; ++i) {
        const auto size = unaligned_load<size_t>(&sizes[i]);
        typed_column.insert_data(payload, size);
        payload += size;
    }
    return payload;
}

} // namespace doris
