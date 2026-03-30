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

#include "core/data_type/data_type_file.h"

#include "common/exception.h"

namespace doris {

Status DataTypeFile::check_column(const IColumn& column) const {
    const auto* file_col = check_and_get_column<ColumnFile>(&column);
    if (file_col == nullptr) {
        return Status::InvalidArgument("column for FILE must be ColumnFile");
    }
    return file_col->check_schema(_schema);
}

Field DataTypeFile::get_field(const TExprNode& node) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Unimplemented get_field for file");
}

bool DataTypeFile::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

int64_t DataTypeFile::get_uncompressed_serialized_bytes(const IColumn& column,
                                                        int be_exec_version) const {
    return _physical_type.get_uncompressed_serialized_bytes(
            assert_cast<const ColumnFile&>(column).get_jsonb_column(), be_exec_version);
}

char* DataTypeFile::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    return _physical_type.serialize(assert_cast<const ColumnFile&>(column).get_jsonb_column(), buf,
                                    be_exec_version);
}

const char* DataTypeFile::deserialize(const char* buf, MutableColumnPtr* column,
                                      int be_exec_version) const {
    if (!*column) {
        *column = create_column();
    }
    auto& file_column = assert_cast<ColumnFile&>(*column->get());
    auto jsonb_column = file_column.get_mutable_jsonb_column_ptr();
    auto* jsonb_column_ptr = &jsonb_column;
    const char* next = _physical_type.deserialize(buf, jsonb_column_ptr, be_exec_version);
    file_column.set_jsonb_column_ptr(std::move(jsonb_column));
    return next;
}

void DataTypeFile::to_pb_column_meta(PColumnMeta* col_meta) const {
    _physical_type.to_pb_column_meta(col_meta);
}

std::optional<size_t> DataTypeFile::try_get_subfield(std::string_view name) const {
    return _schema.try_get_position(name);
}

const DataTypePtr& DataTypeFile::get_subfield_type(size_t idx) const {
    return _schema.field(idx).type;
}

} // namespace doris
