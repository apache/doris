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

#include <array>
#include <cstring>

#include "core/column/column_file.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_struct.h"
#include "core/value/file_value.h"
#include "exprs/function/cast/cast_to_variant.h"
#include "exprs/function/cast/cast_wrapper_decls.h"

namespace doris::CastWrapper {
namespace {

using FileFieldMapping = std::array<int, DataTypeFile::FIELD_COUNT>;

bool compatible_file_field_type(size_t index, PrimitiveType primitive) {
    if (index == 1 || index == 2) {
        return primitive == TYPE_BIGINT;
    }
    if (index == DataTypeFile::INLINE_FIELD_INDEX) {
        return primitive == TYPE_VARBINARY;
    }
    return is_string_type(primitive);
}

Status map_file_fields(const DataTypeStruct& source, FileFieldMapping& mapping) {
    if (source.get_elements().size() != DataTypeFile::FIELD_COUNT) {
        return Status::InvalidArgument("FILE CAST requires exactly six fields");
    }
    mapping.fill(-1);
    const DataTypeFile file;
    for (size_t i = 0; i < source.get_elements().size(); ++i) {
        const auto& name = source.get_element_names()[i];
        const auto position = file.try_get_position_by_name(name);
        if (!position) {
            return Status::InvalidArgument("Unknown FILE field {}", name);
        }
        if (mapping[*position] >= 0) {
            return Status::InvalidArgument("Duplicate FILE field {}", name);
        }
        const auto type = remove_nullable(source.get_elements()[i]);
        const auto primitive = type->get_primitive_type();
        const bool safe = compatible_file_field_type(*position, primitive);
        if (!safe && !type->is_null_literal()) {
            return Status::InvalidArgument("Unsafe conversion of {} to FILE field {}",
                                           type->get_name(), name);
        }
        mapping[*position] = cast_set<int>(i);
    }
    return Status::OK();
}

Result<ColumnPtr> cast_struct_to_file(const ColumnStruct& structure,
                                      const DataTypeStruct& structure_type,
                                      const FileFieldMapping& mapping,
                                      const NullMap::value_type* null_map) {
    const auto file_type = std::make_shared<DataTypeFile>();
    const auto rows = structure.size();
    auto output = ColumnFile::create();
    for (size_t i = 0; i < mapping.size(); ++i) {
        const auto source_index = cast_set<size_t>(mapping[i]);
        if (structure_type.get_elements()[source_index]->is_null_literal()) {
            auto child = file_type->get_element(i)->create_column();
            child->insert_many_defaults(rows);
            output->get_column_ptr(i) = std::move(child);
        } else {
            output->get_column_ptr(i) = make_nullable(
                    structure.get_column_ptr(source_index)->convert_to_full_column_if_const());
        }
    }
    const ColumnPtr prepared = std::move(output);
    if (null_map) {
        auto parent_nulls = ColumnUInt8::create(rows);
        std::memcpy(parent_nulls->get_data().data(), null_map, rows);
        const auto checked = ColumnNullable::create(prepared, std::move(parent_nulls));
        RETURN_IF_ERROR_RESULT(validate_file_column(*checked, make_nullable(file_type)));
    } else {
        RETURN_IF_ERROR_RESULT(validate_file_column(*prepared, file_type));
    }
    return prepared;
}

WrapperType create_cast_to_file(const DataTypePtr& from_type) {
    FileFieldMapping mapping {};
    const auto* source = check_and_get_data_type<DataTypeStruct>(from_type.get());
    if (!source) {
        return create_unsupport_wrapper(from_type->get_name(), "FILE");
    }
    auto status = map_file_fields(*source, mapping);
    if (!status.ok()) {
        return create_unsupport_wrapper(std::string(status.msg()));
    }
    return [from_type, mapping](FunctionContext*, Block& block, const ColumnNumbers& arguments,
                                uint32_t result, size_t, const NullMap::value_type* null_map) {
        const auto source =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        block.get_by_position(result).column = DORIS_TRY(cast_struct_to_file(
                assert_cast<const ColumnStruct&>(*source),
                assert_cast<const DataTypeStruct&>(*from_type), mapping, null_map));
        return Status::OK();
    };
}

} // namespace

WrapperType create_file_wrapper(const DataTypePtr& from_type, const DataTypePtr& to_type) {
    if (to_type->get_primitive_type() == TYPE_FILE) {
        return create_cast_to_file(from_type);
    }
    return create_unsupport_wrapper(from_type->get_name(), to_type->get_name());
}

} // namespace doris::CastWrapper
