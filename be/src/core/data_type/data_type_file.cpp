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

#include <gen_cpp/data.pb.h>
#include <gen_cpp/types.pb.h>

#include "agent/be_exec_version_manager.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_map.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type_serde/data_type_file_serde.h"
#include "core/value/file_value.h"
#include "util/string_util.h"

namespace doris {
namespace {
void check_file_exec_version(int version) {
    if (version < SUPPORT_FILE_VERSION) {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "FILE requires be_exec_version >= {}",
                        SUPPORT_FILE_VERSION);
    }
}
} // namespace

bool contains_file_type(const DataTypePtr& original) {
    const auto type = remove_nullable(original);
    switch (type->get_primitive_type()) {
    case TYPE_FILE:
        return true;
    case TYPE_ARRAY:
        return contains_file_type(assert_cast<const DataTypeArray&>(*type).get_nested_type());
    case TYPE_MAP: {
        const auto& map = assert_cast<const DataTypeMap&>(*type);
        return contains_file_type(map.get_key_type()) || contains_file_type(map.get_value_type());
    }
    case TYPE_STRUCT:
        for (const auto& child : assert_cast<const DataTypeStruct&>(*type).get_elements()) {
            if (contains_file_type(child)) {
                return true;
            }
        }
        return false;
    default:
        return false;
    }
}

namespace {
Status validate_file_at(const IColumn& column, const DataTypePtr& type, size_t row) {
    if (const auto* constant = check_and_get_column<ColumnConst>(column)) {
        return validate_file_at(constant->get_data_column(), type, 0);
    }
    if (type->is_nullable()) {
        const auto& nullable = assert_cast<const ColumnNullable&>(column);
        if (nullable.is_null_at(row)) {
            return Status::OK();
        }
        return validate_file_at(nullable.get_nested_column(), remove_nullable(type), row);
    }
    switch (type->get_primitive_type()) {
    case TYPE_FILE: {
        const auto& file = assert_cast<const ColumnFile&>(column);
        return validate_file_row(file, row);
    }
    case TYPE_ARRAY: {
        const auto& array = assert_cast<const ColumnArray&>(column);
        const auto& nested_type = assert_cast<const DataTypeArray&>(*type).get_nested_type();
        for (size_t item = array.offset_at(row); item < array.get_offsets()[row]; ++item) {
            RETURN_IF_ERROR(validate_file_at(array.get_data(), nested_type, item));
        }
        return Status::OK();
    }
    case TYPE_MAP: {
        const auto& map = assert_cast<const ColumnMap&>(column);
        const auto& map_type = assert_cast<const DataTypeMap&>(*type);
        const bool validate_keys = contains_file_type(map_type.get_key_type());
        const bool validate_values = contains_file_type(map_type.get_value_type());
        for (size_t item = map.offset_at(row); item < map.get_offsets()[row]; ++item) {
            if (validate_keys) {
                RETURN_IF_ERROR(validate_file_at(map.get_keys(), map_type.get_key_type(), item));
            }
            if (validate_values) {
                RETURN_IF_ERROR(
                        validate_file_at(map.get_values(), map_type.get_value_type(), item));
            }
        }
        return Status::OK();
    }
    case TYPE_STRUCT: {
        const auto& structure = assert_cast<const ColumnStruct&>(column);
        const auto& elements = assert_cast<const DataTypeStruct&>(*type).get_elements();
        for (size_t child = 0; child < elements.size(); ++child) {
            if (contains_file_type(elements[child])) {
                RETURN_IF_ERROR(
                        validate_file_at(structure.get_column(child), elements[child], row));
            }
        }
        return Status::OK();
    }
    default:
        return Status::OK();
    }
}
} // namespace

Status validate_file_row(const ColumnFile& column, size_t row) {
    File fields(DataTypeFile::FIELD_COUNT);
    for (size_t child = 0; child < fields.size(); ++child) {
        fields[child] = column.get_column(child)[row];
    }
    // These local fields borrow inline bytes while the source column is alive.
    return validate_file(fields);
}

Status validate_file_column(const IColumn& column, const DataTypePtr& type) {
    return validate_file_column(column, type, 0, column.size());
}

Status validate_file_column(const IColumn& column, const DataTypePtr& type, size_t start,
                            size_t count) {
    DCHECK_LE(start, column.size());
    DCHECK_LE(count, column.size() - start);
    if (count == 0 || !contains_file_type(type)) {
        return Status::OK();
    }
    // Validate original top-level rows after all ancestor null maps are available.
    // A nonempty range of a constant refers to its single physical value.
    if (is_column_const(column)) {
        return validate_file_at(column, type, 0);
    }
    for (size_t row = start; row < start + count; ++row) {
        RETURN_IF_ERROR(validate_file_at(column, type, row));
    }
    return Status::OK();
}

DataTypeFile::DataTypeFile()
        : _elements {make_nullable(std::make_shared<DataTypeString>(65533, TYPE_VARCHAR)),
                     make_nullable(std::make_shared<DataTypeInt64>()),
                     make_nullable(std::make_shared<DataTypeInt64>()),
                     make_nullable(std::make_shared<DataTypeString>(1024, TYPE_VARCHAR)),
                     make_nullable(std::make_shared<DataTypeString>(1024, TYPE_VARCHAR)),
                     make_nullable(std::make_shared<DataTypeVarbinary>())},
          _names {"uri", "offset", "size", "content_type", "checksum", "inline"} {}

DataTypeFile::DataTypeFile(const DataTypes& elements, const Strings& names) : DataTypeFile() {
    if (names != _names || elements.size() != FIELD_COUNT) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "FILE requires its canonical six-child schema");
    }
    for (size_t i = 0; i < FIELD_COUNT; ++i) {
        if (!elements[i]->is_nullable()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "FILE child {} must be nullable",
                            _names[i]);
        }
        auto child = remove_nullable(elements[i]);
        auto expected = remove_nullable(_elements[i]);
        // Block metadata erases VARCHAR length/type to STRING. Restore the canonical type here.
        bool compatible = child->equals(*expected);
        if (i == 0 || i == 3 || i == 4) {
            compatible = child->get_primitive_type() == TYPE_STRING ||
                         (child->get_primitive_type() == TYPE_VARCHAR &&
                          assert_cast<const DataTypeString&>(*child).len() ==
                                  assert_cast<const DataTypeString&>(*expected).len());
        }
        if (!compatible) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid FILE child type for {}",
                            _names[i]);
        }
    }
}

MutableColumnPtr DataTypeFile::create_column() const {
    MutableColumns children;
    for (const auto& type : _elements) {
        children.push_back(type->create_column());
    }
    return ColumnFile::create(std::move(children));
}

Status DataTypeFile::check_column(const IColumn& column) const {
    const auto* file = DORIS_TRY(check_column_nested_type<ColumnFile>(column));
    for (size_t i = 0; i < FIELD_COUNT; ++i) {
        RETURN_IF_ERROR(_elements[i]->check_column(file->get_column(i)));
    }
    return Status::OK();
}

Field DataTypeFile::get_field(const TExprNode& node) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "FILE literals are materialized from child expressions");
}

bool DataTypeFile::equals(const IDataType& rhs) const {
    return dynamic_cast<const DataTypeFile*>(&rhs) != nullptr;
}

std::optional<size_t> DataTypeFile::try_get_position_by_name(const String& name) const {
    for (size_t i = 0; i < FIELD_COUNT; ++i) {
        if (iequal(_names[i], name)) {
            return i;
        }
    }
    return std::nullopt;
}

size_t DataTypeFile::get_position_by_name(const String& name) const {
    auto position = try_get_position_by_name(name);
    if (!position) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Unknown FILE child");
    }
    return *position;
}

int64_t DataTypeFile::get_uncompressed_serialized_bytes(const IColumn& column, int version) const {
    check_file_exec_version(version);
    const auto* data = &column;
    if (is_column_const(column)) {
        data = &assert_cast<const ColumnConst&>(column).get_data_column();
    }
    const auto& file = assert_cast<const ColumnFile&>(*data);
    int64_t size = sizeof(bool) + sizeof(size_t) + sizeof(size_t);
    for (size_t i = 0; i < FIELD_COUNT; ++i) {
        size += _elements[i]->get_uncompressed_serialized_bytes(file.get_column(i), version);
    }
    return size;
}

char* DataTypeFile::serialize(const IColumn& column, char* buf, int version) const {
    check_file_exec_version(version);
    const auto* data = &column;
    size_t rows = 0;
    buf = serialize_const_flag_and_row_num(&data, buf, &rows);
    const auto& file = assert_cast<const ColumnFile&>(*data);
    for (size_t i = 0; i < FIELD_COUNT; ++i) {
        buf = _elements[i]->serialize(file.get_column(i), buf, version);
    }
    return buf;
}

const char* DataTypeFile::deserialize(const char* buf, MutableColumnPtr* column,
                                      int version) const {
    check_file_exec_version(version);
    auto* original = column->get();
    size_t rows = 0;
    buf = deserialize_const_flag_and_row_num(buf, column, &rows);
    auto& file = assert_cast<ColumnFile&>(*original);
    for (size_t i = 0; i < FIELD_COUNT; ++i) {
        auto child = std::move(*file.get_column_ptr(i)).mutate();
        buf = _elements[i]->deserialize(buf, &child, version);
        file.get_column_ptr(i) = std::move(child);
    }
    file.sanity_check();
    return buf;
}

void DataTypeFile::to_pb_column_meta(PColumnMeta* meta) const {
    IDataType::to_pb_column_meta(meta);
    for (size_t i = 0; i < FIELD_COUNT; ++i) {
        auto* child = meta->add_children();
        child->set_name(_names[i]);
        _elements[i]->to_pb_column_meta(child);
    }
}

void DataTypeFile::to_protobuf(PTypeDesc* type, PTypeNode* node, PScalarType*) const {
    node->set_type(TTypeNodeType::FILE);
    node->clear_scalar_type();
    for (const auto& name : _names) {
        auto* child = node->add_struct_fields();
        child->set_name(name);
        child->set_contains_null(true);
    }
    for (const auto& child : _elements) {
        child->to_protobuf(type);
    }
}

DataTypeSerDeSPtr DataTypeFile::get_serde(int nesting_level) const {
    return std::make_shared<DataTypeFileSerDe>(nesting_level);
}

#ifdef BE_TEST
void DataTypeFile::to_thrift(TTypeDesc& type, TTypeNode& node) const {
    node.type = TTypeNodeType::FILE;
    node.__set_struct_fields(std::vector<TStructField>());
    for (const auto& name : _names) {
        TStructField child;
        child.__set_name(name);
        child.__set_contains_null(true);
        node.struct_fields.push_back(std::move(child));
    }
    for (const auto& child : _elements) {
        child->to_thrift(type);
    }
}
#endif

} // namespace doris
