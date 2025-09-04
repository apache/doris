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

#include "vec/data_types/data_type_varbinary.h"

#include <glog/logging.h>
#include <lz4/lz4.h>
#include <streamvbyte.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "agent/be_exec_version_manager.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_varbinary.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/common/string_view.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
std::string DataTypeVarbinary::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& value = assert_cast<const ColumnVarbinary&>(*ptr).get_data_at(row_num);
    return value.to_string();
}

void DataTypeVarbinary::to_string(const class doris::vectorized::IColumn& column, size_t row_num,
                                  class doris::vectorized::BufferWritable& ostr) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const auto& value = assert_cast<const ColumnVarbinary&>(*ptr).get_data_at(row_num);
    ostr.write(value.data, value.size);
}

Field DataTypeVarbinary::get_default() const {
    return Field::create_field<TYPE_VARBINARY>(StringView());
}

MutableColumnPtr DataTypeVarbinary::create_column() const {
    return ColumnVarbinary::create();
}

Status DataTypeVarbinary::check_column(const IColumn& column) const {
    return check_column_non_nested_type<ColumnVarbinary>(column);
}

bool DataTypeVarbinary::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

// binary: const flag| row num | real saved num | size array | data array
//            bool      size_t     size_t
// <size array>: data1 size | data2 size | ... sizeof(size_t) * real_need_copy_num
// <data array>: data1 value| data2 value| ... dataN value
int64_t DataTypeVarbinary::get_uncompressed_serialized_bytes(const IColumn& column,
                                                             int be_exec_version) const {
    DCHECK(be_exec_version >= USE_CONST_SERDE);
    auto size = sizeof(bool) + sizeof(size_t) + sizeof(size_t);
    bool is_const_column = is_column_const(column);
    auto real_need_copy_num = is_const_column ? 1 : column.size();

    const IColumn* varbinary_column = &column;
    if (is_const_column) {
        const auto& const_column = assert_cast<const ColumnConst&>(column);
        varbinary_column = &(const_column.get_data_column());
    }
    const auto& data_column = assert_cast<const ColumnVarbinary&>(*varbinary_column);
    auto allocate_len_size = sizeof(size_t) * real_need_copy_num;
    size_t allocate_content_size = 0;
    for (size_t i = 0; i < real_need_copy_num; ++i) {
        auto value = data_column.get_data()[i];
        allocate_content_size += value.size();
    }
    return size + allocate_len_size + allocate_content_size;
}

char* DataTypeVarbinary::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    DCHECK(be_exec_version >= USE_CONST_SERDE);
    const auto* varbinary_column = &column;
    size_t real_need_copy_num = 0;
    buf = serialize_const_flag_and_row_num(&varbinary_column, buf, &real_need_copy_num);

    const auto& data_column = assert_cast<const ColumnVarbinary&>(*varbinary_column);
    // serialize the varbinary size array
    auto* meta_ptr = reinterpret_cast<size_t*>(buf);
    for (size_t i = 0; i < real_need_copy_num; ++i) {
        auto value = data_column.get_data()[i];
        unaligned_store<size_t>(&meta_ptr[i], value.size());
    }

    // serialize each varbinary
    char* data_ptr = buf + sizeof(size_t) * real_need_copy_num;
    for (size_t i = 0; i < real_need_copy_num; ++i) {
        auto value = data_column.get_data()[i];
        memcpy(data_ptr, value.data(), value.size());
        data_ptr += unaligned_load<size_t>(&meta_ptr[i]);
    }
    return data_ptr;
}

const char* DataTypeVarbinary::deserialize(const char* buf, MutableColumnPtr* column,
                                           int be_exec_version) const {
    DCHECK(be_exec_version >= USE_CONST_SERDE);
    auto* origin_column = column->get();
    size_t real_have_saved_num = 0;
    buf = deserialize_const_flag_and_row_num(buf, column, &real_have_saved_num);

    // deserialize the varbinary size array
    auto& data_column = assert_cast<ColumnVarbinary&>(*origin_column);
    const auto* meta_ptr = reinterpret_cast<const size_t*>(buf);
    const char* data_ptr = buf + sizeof(size_t) * real_have_saved_num;
    for (size_t i = 0; i < real_have_saved_num; ++i) {
        auto size = unaligned_load<size_t>(&meta_ptr[i]);
        data_column.insert_data(data_ptr, size);
        data_ptr += size;
    }
    return data_ptr;
}

FieldWithDataType DataTypeVarbinary::get_field_with_data_type(const IColumn& column,
                                                              size_t row_num) const {
    const auto& column_data =
            assert_cast<const ColumnVarbinary&, TypeCheckOnRelease::DISABLE>(column);
    return FieldWithDataType {.field = Field::create_field<TYPE_VARBINARY>(
                                      doris::StringView(column_data.get_data_at(row_num))),
                              .base_scalar_type_id = get_primitive_type()};
}

} // namespace doris::vectorized
