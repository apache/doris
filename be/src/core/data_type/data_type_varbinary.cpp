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

#include "core/data_type/data_type_varbinary.h"

#include <gen_cpp/data.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <lz4/lz4.h>
#include <streamvbyte.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "agent/be_exec_version_manager.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_varbinary.h"
#include "core/data_type/data_type_binary_serialization.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "core/string_buffer.hpp"
#include "core/string_ref.h"
#include "core/string_view.h"
#include "core/types.h"

namespace doris {

MutableColumnPtr DataTypeVarbinary::create_column() const {
    return ColumnVarbinary::create();
}

Status DataTypeVarbinary::check_column(const IColumn& column) const {
    return check_column_non_nested_type<ColumnVarbinary>(column);
}

bool DataTypeVarbinary::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

void DataTypeVarbinary::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    // Fixed-width binary writers need the declared width after a Block crosses an exchange.
    col_meta->set_varbinary_length(_len);
}

// binary: const flag| row num | real saved num | size array | data array
//            bool      size_t     size_t
// <size array>: data1 size | data2 size | ... sizeof(size_t) * real_need_copy_num
// <data array>: data1 value| data2 value| ... dataN value
int64_t DataTypeVarbinary::get_uncompressed_serialized_bytes(const IColumn& column,
                                                             int be_exec_version) const {
    return get_binary_column_serialized_bytes<ColumnVarbinary>(column);
}

char* DataTypeVarbinary::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    return serialize_binary_column<ColumnVarbinary>(column, buf);
}

const char* DataTypeVarbinary::deserialize(const char* buf, MutableColumnPtr* column,
                                           int be_exec_version) const {
    return deserialize_binary_column<ColumnVarbinary>(buf, column);
}

FieldWithDataType DataTypeVarbinary::get_field_with_data_type(const IColumn& column,
                                                              size_t row_num) const {
    const auto& column_data =
            assert_cast<const ColumnVarbinary&, TypeCheckOnRelease::DISABLE>(column);
    return FieldWithDataType {.field = Field::create_field<TYPE_VARBINARY>(
                                      doris::StringView(column_data.get_data_at(row_num))),
                              .base_scalar_type_id = get_primitive_type()};
}

void DataTypeVarbinary::to_protobuf(PTypeDesc* ptype, PTypeNode* node,
                                    PScalarType* scalar_type) const {
    scalar_type->set_len(_len);
}

} // namespace doris
