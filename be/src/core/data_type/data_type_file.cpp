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

#include <algorithm>
#include <array>
#include <limits>

#include "agent/be_exec_version_manager.h"
#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
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
std::optional<std::string_view> file_text_at(const ColumnNullable& column, size_t row) {
    if (column.is_null_at(row)) {
        return std::nullopt;
    }
    const auto text = column.get_nested_column().get_data_at(row);
    return std::string_view(text.data, text.size);
}

Status validate_file_values(const ColumnFile& file, const NullMap* parent_nulls) {
    std::array<const ColumnNullable*, DataTypeFile::FIELD_COUNT> children;
    for (size_t child = 0; child < children.size(); ++child) {
        children[child] = &assert_cast<const ColumnNullable&>(file.get_column(child));
    }
    const auto& offsets =
            assert_cast<const ColumnInt64&>(children[1]->get_nested_column()).get_data();
    const auto& sizes =
            assert_cast<const ColumnInt64&>(children[2]->get_nested_column()).get_data();
    for (size_t row = 0; row < file.size(); ++row) {
        if (parent_nulls && (*parent_nulls)[row]) {
            continue;
        }
        const auto offset =
                children[1]->is_null_at(row) ? std::nullopt : std::optional(offsets[row]);
        const auto size = children[2]->is_null_at(row) ? std::nullopt : std::optional(sizes[row]);
        RETURN_IF_ERROR(validate_file(
                file_text_at(*children[0], row), offset, size, file_text_at(*children[3], row),
                file_text_at(*children[4], row), children[5]->is_null_at(row)));
    }
    return Status::OK();
}

} // namespace

Status validate_file_column(const IColumn& column, const DataTypePtr& type) {
    DCHECK_EQ(remove_nullable(type)->get_primitive_type(), TYPE_FILE);
    const IColumn* data = &column;
    if (const auto* constant = check_and_get_column<ColumnConst>(column)) {
        data = &constant->get_data_column();
    }
    const NullMap* nulls = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(data)) {
        // CAST merges ancestor NULL masks before validating the resulting FILE column.
        nulls = &nullable->get_null_map_data();
        data = &nullable->get_nested_column();
    }
    return validate_file_values(assert_cast<const ColumnFile&>(*data), nulls);
}

DataTypeFile::DataTypeFile()
        : _elements {make_nullable(std::make_shared<DataTypeString>(65533, TYPE_VARCHAR)),
                     make_nullable(std::make_shared<DataTypeInt64>()),
                     make_nullable(std::make_shared<DataTypeInt64>()),
                     make_nullable(std::make_shared<DataTypeString>(1024, TYPE_VARCHAR)),
                     make_nullable(std::make_shared<DataTypeString>(1024, TYPE_VARCHAR)),
                     make_nullable(std::make_shared<DataTypeVarbinary>(
                             std::numeric_limits<int32_t>::max()))},
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
