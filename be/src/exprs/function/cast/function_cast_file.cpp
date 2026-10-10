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

#include "core/column/column_file.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/value/file_value.h"
#include "exprs/function/cast/cast_wrapper_decls.h"
#include "exprs/function/cast/variant_v2/cast_variant_v2.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"

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

WrapperType create_cast_to_file(const DataTypePtr& from_type) {
    FileFieldMapping mapping {};
    const bool from_json = from_type->get_primitive_type() == TYPE_JSONB;
    if (!from_json) {
        const auto* source = check_and_get_data_type<DataTypeStruct>(from_type.get());
        if (!source) return create_unsupport_wrapper(from_type->get_name(), "FILE");
        auto status = map_file_fields(*source, mapping);
        if (!status.ok()) return create_unsupport_wrapper(std::string(status.msg()));
    }
    return [from_json, mapping](FunctionContext* context, Block& block,
                                const ColumnNumbers& arguments, uint32_t result, size_t rows,
                                const NullMap::value_type* null_map) {
        const auto source =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto file_type = std::make_shared<DataTypeFile>();
        const auto serde = file_type->get_serde();
        auto output = file_type->create_column();
        auto output_nulls = ColumnUInt8::create(rows, 0);
        auto& nulls = output_nulls->get_data();
        bool has_result_null = false;
        CastParameters parameters;
        parameters.is_strict = context->enable_strict_mode();
        for (size_t row = 0; row < rows; ++row) {
            if (null_map && null_map[row]) {
                // The enclosing nullable wrapper owns this NULL; the FILE is a
                // placeholder.
                output->insert_default();
                continue;
            }
            Status status;
            if (from_json) {
                const auto bytes = source->get_data_at(row);
                const JsonbDocument* document = nullptr;
                status = JsonbDocument::checkAndCreateDocument(bytes.data, bytes.size, &document);
                if (status.ok()) {
                    const auto* value = document->getValue();
                    if (value->isNull()) {
                        output->insert_default();
                        nulls[row] = 1;
                        has_result_null = true;
                        continue;
                    }
                    status = serde->deserialize_column_from_jsonb(*output, value, parameters);
                }
            } else {
                const auto& structure = assert_cast<const ColumnStruct&>(*source);
                File value(DataTypeFile::FIELD_COUNT);
                for (size_t i = 0; i < mapping.size(); ++i) {
                    value[i] = structure.get_column(mapping[i])[row];
                }
                status = validate_file(value);
                if (status.ok()) output->insert(Field::create_field<TYPE_FILE>(std::move(value)));
            }
            if (!status.ok()) {
                if (parameters.is_strict) return status;
                // No child is appended until the whole FILE has passed validation.
                output->insert_default();
                nulls[row] = 1;
                has_result_null = true;
            }
        }
        if (has_result_null) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(output), std::move(output_nulls));
        } else {
            block.get_by_position(result).column = std::move(output);
        }
        return Status::OK();
    };
}

WrapperType create_cast_from_file(const DataTypePtr& from_type, const DataTypePtr& to_type) {
    const auto& file_type = assert_cast<const DataTypeFile&>(*from_type);
    const auto primitive = to_type->get_primitive_type();
    if (primitive == TYPE_STRUCT) {
        const auto& structure = assert_cast<const DataTypeStruct&>(*to_type);
        FileFieldMapping mapping {};
        auto status = map_file_fields(structure, mapping);
        if (!status.ok()) {
            return create_unsupport_wrapper(std::string(status.msg()));
        }
        DataTypes fields;
        FileFieldMapping source_positions {};
        for (size_t file_index = 0; file_index < mapping.size(); ++file_index) {
            source_positions[mapping[file_index]] = cast_set<int>(file_index);
        }
        for (size_t target_index = 0; target_index < source_positions.size(); ++target_index) {
            const auto& field = structure.get_elements()[target_index];
            const auto file_index = cast_set<size_t>(source_positions[target_index]);
            const auto field_primitive = remove_nullable(field)->get_primitive_type();
            const bool compatible = compatible_file_field_type(file_index, field_primitive);
            if (!field->is_nullable() || !compatible) {
                return create_unsupport_wrapper("FILE can only cast to its six-field STRUCT");
            }
            fields.push_back(file_type.get_elements()[file_index]);
        }
        const auto structure_type =
                std::make_shared<DataTypeStruct>(fields, structure.get_element_names());
        return [structure_type, to_type, source_positions](
                       FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                       uint32_t result, size_t rows, const NullMap::value_type* null_map) {
            const auto source =
                    block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
            const auto& file = assert_cast<const ColumnFile&>(*source);
            Columns children;
            for (size_t i = 0; i < DataTypeFile::FIELD_COUNT; ++i) {
                children.push_back(file.get_column_ptr(source_positions[i]));
            }
            Block projected {{ColumnStruct::create(children), structure_type, "file_fields"},
                             {nullptr, to_type, "result"}};
            RETURN_IF_ERROR(prepare_unpack_dictionaries(context, structure_type, to_type)(
                    context, projected, {0}, 1, rows, null_map));
            block.get_by_position(result).column = projected.get_by_position(1).column;
            return Status::OK();
        };
    }
    if (primitive != TYPE_JSONB) {
        return create_unsupport_wrapper(from_type->get_name(), to_type->get_name());
    }
    return [from_type](FunctionContext*, Block& block, const ColumnNumbers& arguments,
                       uint32_t result, size_t rows, const NullMap::value_type* null_map) {
        const auto& source = block.get_by_position(arguments[0]).column;
        const auto serde = from_type->get_serde();
        auto output = ColumnString::create();
        for (size_t row = 0; row < rows; ++row) {
            if (null_map && null_map[row]) {
                output->insert_default();
                continue;
            }
            JsonbWriter writer;
            RETURN_IF_ERROR(serde->serialize_column_to_jsonb(*source, row, writer));
            output->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
        }
        block.get_by_position(result).column = std::move(output);
        return Status::OK();
    };
}

} // namespace

WrapperType create_file_wrapper(const DataTypePtr& from_type, const DataTypePtr& to_type) {
    if (from_type->get_primitive_type() == TYPE_VARIANT) {
        return [from_type](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                           uint32_t result, size_t rows, const NullMap::value_type* null_map) {
            const auto json_type = std::make_shared<DataTypeJsonb>();
            const auto file_type = std::make_shared<DataTypeFile>();
            Block intermediate {
                    {block.get_by_position(arguments[0]).column->convert_to_full_column_if_const(),
                     from_type, "variant"},
                    {nullptr, make_nullable(json_type), "json"},
                    {nullptr, make_nullable(file_type), "file"}};
            RETURN_IF_ERROR(create_cast_from_variant_v2_wrapper(json_type)(context, intermediate,
                                                                           {0}, 1, rows, null_map));
            RETURN_IF_ERROR(prepare_unpack_dictionaries(context, make_nullable(json_type),
                                                        make_nullable(file_type))(
                    context, intermediate, {1}, 2, rows, nullptr));
            auto output = intermediate.get_by_position(2).column;
            const auto& nullable = assert_cast<const ColumnNullable&>(*output);
            if (!nullable.has_null()) {
                output = nullable.get_nested_column_ptr();
            }
            block.get_by_position(result).column = std::move(output);
            return Status::OK();
        };
    }
    if (to_type->get_primitive_type() == TYPE_VARIANT) {
        return [from_type](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                           uint32_t result, size_t rows, const NullMap::value_type* null_map) {
            const auto json_type = std::make_shared<DataTypeJsonb>();
            Block intermediate {
                    {block.get_by_position(arguments[0]).column->convert_to_full_column_if_const(),
                     from_type, "file"},
                    {nullptr, json_type, "json"},
                    {nullptr, block.get_by_position(result).type, "variant"}};
            RETURN_IF_ERROR(create_cast_from_file(from_type, json_type)(context, intermediate, {0},
                                                                        1, rows, null_map));
            RETURN_IF_ERROR(create_cast_to_variant_v2_wrapper(json_type)(context, intermediate, {1},
                                                                         2, rows, null_map));
            block.get_by_position(result).column = intermediate.get_by_position(2).column;
            return Status::OK();
        };
    }
    if (to_type->get_primitive_type() == TYPE_FILE) return create_cast_to_file(from_type);
    return create_cast_from_file(from_type, to_type);
}

} // namespace doris::CastWrapper
