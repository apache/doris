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

#include "data_type_geometry.h"

#include "geo/geo_types.h"
#include "vec/columns/column_const.h"
#include "vec/common/string_buffer.hpp"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized {

std::string DataTypeGeometry::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    const StringRef& s = assert_cast<const ColumnString&>(*ptr).get_data_at(row_num);
    return s.to_string();
}

void DataTypeGeometry::to_string(const class doris::vectorized::IColumn& column, size_t row_num,
                                 class doris::vectorized::BufferWritable& ostr) const {
    std::string str = to_string(column, row_num);
    ostr.write(str.c_str(), str.size());
}

Status DataTypeGeometry::from_string(ReadBuffer& rb, IColumn* column) const {
    GeometryBinaryValue value;
    RETURN_IF_ERROR(value.from_geometry_string(rb.position(), rb.count()));

    auto* column_string = static_cast<ColumnString*>(column);
    column_string->insert_data(value.value(), value.size());

    return Status::OK();
}

MutableColumnPtr DataTypeGeometry::create_column() const {
    return ColumnString::create();
}

bool DataTypeGeometry::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

int64_t DataTypeGeometry::get_uncompressed_serialized_bytes(const IColumn& column,
                                                            int data_version) const {
    return data_type_string.get_uncompressed_serialized_bytes(column, data_version);
}

char* DataTypeGeometry::serialize(const IColumn& column, char* buf, int data_version) const {
    return data_type_string.serialize(column, buf, data_version);
}

const char* DataTypeGeometry::deserialize(const char* buf, IColumn* column,
                                          int data_version) const {
    return data_type_string.deserialize(buf, column, data_version);
}

} // namespace doris::vectorized
