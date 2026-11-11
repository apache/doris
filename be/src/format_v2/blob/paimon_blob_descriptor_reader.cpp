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

#include "format_v2/blob/paimon_blob_descriptor_reader.h"

#include <algorithm>

#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "format_v2/blob/paimon_blob_payload_reader.h"
#include "io/io_common.h"

namespace doris::format::paimon {

Status PaimonBlobDescriptorReader::read(const ColumnPtr& input_column,
                                        const DataTypePtr& input_column_type,
                                        const ReadContext& read_context, ColumnPtr* result_column) {
    return _convert(input_column->convert_to_full_column_if_const(), input_column_type,
                    _result_column_type, read_context, result_column);
}

Status PaimonBlobDescriptorReader::_convert(const ColumnPtr& input_column,
                                            const DataTypePtr& input_column_type,
                                            const DataTypePtr& result_column_type,
                                            const ReadContext& read_context,
                                            ColumnPtr* result_column) {
    const auto& input_value_type = remove_nullable(input_column_type);
    const auto& result_value_type = remove_nullable(result_column_type);
    const ColumnNullable* nullable_input_column =
            input_column->is_nullable() ? assert_cast<const ColumnNullable*>(input_column.get())
                                        : nullptr;
    const ColumnPtr input_values =
            nullable_input_column ? nullable_input_column->get_nested_column_ptr() : input_column;
    const NullMap* input_null_map =
            nullable_input_column ? &nullable_input_column->get_null_map_data() : nullptr;
    if (!result_column_type->is_nullable() && input_null_map != nullptr &&
        std::ranges::any_of(*input_null_map, [](auto value) { return value != 0; })) {
        return Status::Corruption("NULL in required Paimon BLOB value");
    }
    ColumnPtr converted_values;
    switch (result_value_type->get_primitive_type()) {
    case TYPE_FILE: {
        if (!is_string_type(input_value_type->get_primitive_type()) &&
            !is_varbinary(input_value_type->get_primitive_type())) {
            return Status::Corruption(
                    "Paimon BLOB descriptor requires binary physical data, got {}",
                    input_value_type->get_name());
        }
        PaimonBlobColumnBuilder result_builder(result_column_type, _output_mode,
                                               read_context.blob_max_size, input_column->size());
        for (size_t row_index = 0; row_index < input_column->size(); ++row_index) {
            if (read_context.io_ctx != nullptr && read_context.io_ctx->should_stop) {
                return Status::Cancelled("Paimon BLOB materialization cancelled");
            }
            if (input_null_map != nullptr && (*input_null_map)[row_index]) {
                RETURN_IF_ERROR(result_builder.append_null());
                continue;
            }
            const auto descriptor_bytes = input_values->get_data_at(row_index);
            PaimonBlobReference blob_reference;
            RETURN_IF_ERROR(PaimonBlobReference::decode(
                    std::string_view(descriptor_bytes.data, descriptor_bytes.size),
                    &blob_reference));
            RETURN_IF_ERROR(
                    result_builder.append(blob_reference, [&](PODArray<char>* payload_buffer) {
                        return _payload_reader.read_range(blob_reference, read_context,
                                                          payload_buffer);
                    }));
        }
        *result_column = result_builder.finish();
        return Status::OK();
    }
    case TYPE_ARRAY: {
        if (input_value_type->get_primitive_type() != TYPE_ARRAY) {
            return Status::Corruption("Paimon BLOB array has non-array physical data");
        }
        const auto& input_array = assert_cast<const ColumnArray&>(*input_values);
        const auto& input_container_type = assert_cast<const DataTypeArray&>(*input_value_type);
        const auto& result_container_type = assert_cast<const DataTypeArray&>(*result_value_type);
        ColumnPtr result_elements;
        RETURN_IF_ERROR(_convert(input_array.get_data_ptr(), input_container_type.get_nested_type(),
                                 result_container_type.get_nested_type(), read_context,
                                 &result_elements));
        converted_values = ColumnArray::create(result_elements, input_array.get_offsets_ptr());
        break;
    }
    case TYPE_MAP: {
        if (input_value_type->get_primitive_type() != TYPE_MAP) {
            return Status::Corruption("Paimon BLOB map has non-map physical data");
        }
        const auto& input_map = assert_cast<const ColumnMap&>(*input_values);
        const auto& input_container_type = assert_cast<const DataTypeMap&>(*input_value_type);
        const auto& result_container_type = assert_cast<const DataTypeMap&>(*result_value_type);
        ColumnPtr result_keys;
        ColumnPtr result_values;
        RETURN_IF_ERROR(_convert(input_map.get_keys_ptr(), input_container_type.get_key_type(),
                                 result_container_type.get_key_type(), read_context, &result_keys));
        RETURN_IF_ERROR(_convert(input_map.get_values_ptr(), input_container_type.get_value_type(),
                                 result_container_type.get_value_type(), read_context,
                                 &result_values));
        converted_values =
                ColumnMap::create(result_keys, result_values, input_map.get_offsets_ptr());
        break;
    }
    default:
        // Map keys retain their physical values; unsupported type evolution fails explicitly.
        if (!input_value_type->equals(*result_value_type)) {
            return Status::NotSupported("Paimon BLOB map key type mismatch: {} -> {}",
                                        input_value_type->get_name(),
                                        result_value_type->get_name());
        }
        if (input_column_type->equals(*result_column_type)) {
            *result_column = input_column;
            return Status::OK();
        }
        converted_values = input_values;
    }
    if (result_column_type->is_nullable()) {
        ColumnPtr result_null_map;
        if (nullable_input_column != nullptr) {
            result_null_map = nullable_input_column->get_null_map_column_ptr();
        } else {
            auto zero_null_map = ColumnUInt8::create();
            zero_null_map->insert_many_vals(0, input_column->size());
            result_null_map = std::move(zero_null_map);
        }
        *result_column = ColumnNullable::create(converted_values, result_null_map);
    } else {
        *result_column = std::move(converted_values);
    }
    return Status::OK();
}

} // namespace doris::format::paimon
