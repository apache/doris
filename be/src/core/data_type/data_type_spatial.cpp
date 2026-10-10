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

#include "core/data_type/data_type_spatial.h"

#include <cstring>

#include "agent/be_exec_version_manager.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type.h"
#include "core/field.h"
#include "core/string_view.h"

namespace doris {

doris::FieldType DataTypeSpatial::get_storage_field_type() const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "Spatial types are supported only by external Iceberg tables");
    return FieldType::OLAP_FIELD_TYPE_UNKNOWN;
}

MutableColumnPtr DataTypeSpatial::create_column() const {
    return ColumnSpatial::create(_primitive_type);
}

Status DataTypeSpatial::check_column(const IColumn& column) const {
    const IColumn* nested_column = &column;
    if (is_column_const(column)) {
        nested_column = &assert_cast<const ColumnConst&>(column).get_data_column();
    }
    const auto* spatial = check_and_get_column<ColumnSpatial>(nested_column);
    if (spatial == nullptr || spatial->get_primitive_type() != _primitive_type) {
        return Status::InvalidArgument("Expected {} spatial column, got {}", get_name(),
                                       column.get_name());
    }
    return Status::OK();
}

int64_t DataTypeSpatial::get_uncompressed_serialized_bytes(const IColumn& column,
                                                           int be_exec_version) const {
    DCHECK(be_exec_version >= USE_CONST_SERDE);
    const IColumn* data_column = &column;
    const bool is_const = is_column_const(column);
    const size_t stored_rows = is_const ? 1 : column.size();
    if (is_const) {
        data_column = &assert_cast<const ColumnConst&>(column).get_data_column();
    }
    const auto& spatial = assert_cast<const ColumnSpatial&>(*data_column);
    size_t payload_size = 0;
    for (size_t i = 0; i < stored_rows; ++i) {
        payload_size += spatial.get_data_at(i).size;
    }
    return sizeof(bool) + sizeof(size_t) * (3 + stored_rows) + payload_size;
}

char* DataTypeSpatial::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    DCHECK(be_exec_version >= USE_CONST_SERDE);
    const IColumn* data_column = &column;
    size_t stored_rows = 0;
    buf = serialize_const_flag_and_row_num(&data_column, buf, &stored_rows);
    const auto& spatial = assert_cast<const ColumnSpatial&>(*data_column);
    auto* sizes = reinterpret_cast<size_t*>(buf);
    for (size_t i = 0; i < stored_rows; ++i) {
        unaligned_store<size_t>(&sizes[i], spatial.get_data_at(i).size);
    }
    char* payload = buf + sizeof(size_t) * stored_rows;
    for (size_t i = 0; i < stored_rows; ++i) {
        const auto value = spatial.get_data_at(i);
        memcpy(payload, value.data, value.size);
        payload += value.size;
    }
    return payload;
}

const char* DataTypeSpatial::deserialize(const char* buf, MutableColumnPtr* column,
                                         int be_exec_version) const {
    DCHECK(be_exec_version >= USE_CONST_SERDE);
    auto* original = column->get();
    size_t stored_rows = 0;
    buf = deserialize_const_flag_and_row_num(buf, column, &stored_rows);
    auto& spatial = assert_cast<ColumnSpatial&>(*original);
    const auto* sizes = reinterpret_cast<const size_t*>(buf);
    const char* payload = buf + sizeof(size_t) * stored_rows;
    for (size_t i = 0; i < stored_rows; ++i) {
        const size_t size = unaligned_load<size_t>(&sizes[i]);
        spatial.insert_data(payload, size);
        payload += size;
    }
    return payload;
}

Field DataTypeSpatial::get_field(const TExprNode& /* node */) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "Spatial literals must be constructed from WKB by a spatial function");
}

FieldWithDataType DataTypeSpatial::get_field_with_data_type(const IColumn& column,
                                                            size_t row_num) const {
    const auto value = assert_cast<const ColumnSpatial&>(column).get_data_at(row_num);
    return FieldWithDataType {.field = Field::create_field<TYPE_VARBINARY>(StringView(value)),
                              .base_scalar_type_id = _primitive_type};
}

bool DataTypeSpatial::equals(const IDataType& rhs) const {
    const auto* other = dynamic_cast<const DataTypeSpatial*>(&rhs);
    return other != nullptr && _primitive_type == other->_primitive_type && _crs == other->_crs &&
           _algorithm == other->_algorithm;
}

void DataTypeSpatial::to_protobuf(PTypeDesc* /* ptype */, PTypeNode* /* node */,
                                  PScalarType* /* scalar_type */) const {}

#ifdef BE_TEST
void DataTypeSpatial::to_thrift(TTypeDesc& thrift_type, TTypeNode& node) const {
    IDataType::to_thrift(thrift_type, node);
    node.scalar_type.__set_spatial_crs(_crs);
    if (_primitive_type == TYPE_GEOGRAPHY) {
        node.scalar_type.__set_spatial_algorithm(_algorithm);
    }
}
#endif

} // namespace doris
