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

#include "format/transformer/json_cell_writer.h"

#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/string_buffer.hpp"
#include "util/jsonb_utils.h"

namespace doris::json_format {
namespace {

// Keep existing typed scalar conversion/number formatting, but escape strings
// with the length-aware format writer. JsonbToJson truncates embedded NULs.
Status write_jsonb_value(const JsonbValue& value, BufferWritable& output) {
    switch (value.type) {
    case JsonbType::T_String: {
        const auto* string = value.unpack<JsonbStringVal>();
        output.write_json_string(string->getBlob(), string->getBlobLen());
        return Status::OK();
    }
    case JsonbType::T_Object: {
        const auto* object = value.unpack<ObjectVal>();
        output.write('{');
        bool first = true;
        for (auto entry = object->begin(); entry != object->end(); ++entry) {
            if (!first) output.write(',');
            first = false;
            if (entry->klen() != 0) {
                output.write_json_string(entry->getKeyStr(), entry->klen());
            } else if (entry->getKeyId() == JsonbKeyValue::sMaxKeyId) {
                output.write_json_string("", 0);
            } else {
                // Dictionary keys belong to rowstore JSONB, not typed JSON.
                return Status::NotSupported("JSON OUTFILE does not support JSONB dictionary keys");
            }
            output.write(':');
            RETURN_IF_ERROR(write_jsonb_value(*entry->value(), output));
        }
        output.write('}');
        return Status::OK();
    }
    case JsonbType::T_Array: {
        const auto* array = value.unpack<ArrayVal>();
        output.write('[');
        bool first = true;
        for (auto entry = array->begin(); entry != array->end(); ++entry) {
            if (!first) output.write(',');
            first = false;
            RETURN_IF_ERROR(write_jsonb_value(*entry, output));
        }
        output.write(']');
        return Status::OK();
    }
    case JsonbType::T_Binary:
        return Status::NotSupported("JSON OUTFILE does not support JSONB binary values");
    default:
        break;
    }
    // Preserve TO_JSON's numeric text, including unquoted nan/inf/-inf.
    const auto text = JsonbToJson {}.to_json_string(&value);
    output.write(text.data(), text.size());
    return Status::OK();
}

} // namespace

Status validate_json_output_type(const DataTypePtr& original) {
    const auto type = remove_nullable(original);
    switch (type->get_primitive_type()) {
    case TYPE_FILE:
        // FILE exposes all six fields; inline is padded Base64 or NULL.
        return Status::OK();
    case TYPE_VARBINARY:
        return Status::NotSupported("JSON OUTFILE does not support ordinary VARBINARY");
    case TYPE_ARRAY:
        return validate_json_output_type(
                assert_cast<const DataTypeArray&>(*type).get_nested_type());
    case TYPE_MAP: {
        const auto& map = assert_cast<const DataTypeMap&>(*type);
        RETURN_IF_ERROR(validate_json_output_type(map.get_key_type()));
        return validate_json_output_type(map.get_value_type());
    }
    case TYPE_STRUCT:
        for (const auto& child : assert_cast<const DataTypeStruct&>(*type).get_elements()) {
            RETURN_IF_ERROR(validate_json_output_type(child));
        }
        return Status::OK();
    default:
        return Status::OK();
    }
}

const DataTypeSerDe& JsonCellWriterState::serde(const DataTypePtr& type) {
    const auto found = _serdes.find(type);
    if (found != _serdes.end()) return *found->second;
    // get_serde() constructs a new instance (including FILE's child serdes).
    // Prepare once per distinct schema node instead of once per cell.
    return *_serdes.emplace(type, type->get_serde()).first->second;
}

Status write_json_cell(const IColumn& column, const DataTypePtr& type, size_t row,
                       BufferWritable& output, DataTypeSerDe::FormatOptions& options,
                       JsonCellWriterState& state) {
    if (const auto* constant = check_and_get_column<ColumnConst>(column)) {
        return write_json_cell(constant->get_data_column(), type, 0, output, options, state);
    }
    if (type->is_nullable()) {
        const auto& nullable = assert_cast<const ColumnNullable&>(column);
        if (nullable.is_null_at(row)) {
            output.write("null", 4);
            return Status::OK();
        }
        return write_json_cell(nullable.get_nested_column(), remove_nullable(type), row, output,
                               options, state);
    }
    const auto primitive = type->get_primitive_type();
    if (is_string_type(primitive)) {
        // TO_JSON treats STRING as a JSON string, not JSON source text. Avoid
        // copying the whole value into a temporary JSONB string just to escape it.
        output.write_json_string(column.get_data_at(row));
        return Status::OK();
    }
    switch (primitive) {
    case TYPE_FILE:
        // JSON OUTFILE exposes the same six fields as FILE's public JSON display.
        state.serde(type).to_string(column, row, output, options);
        return Status::OK();
    case TYPE_ARRAY: {
        const auto& array = assert_cast<const ColumnArray&>(column);
        const auto& element_type = assert_cast<const DataTypeArray&>(*type).get_nested_type();
        const auto begin = array.get_offsets()[row - 1];
        const auto end = array.get_offsets()[row];
        output.write('[');
        for (size_t item = begin; item != end; ++item) {
            if (item != begin) output.write(',');
            RETURN_IF_ERROR(
                    write_json_cell(array.get_data(), element_type, item, output, options, state));
        }
        output.write(']');
        return Status::OK();
    }
    case TYPE_STRUCT: {
        const auto& structure = assert_cast<const ColumnStruct&>(column);
        const auto& structure_type = assert_cast<const DataTypeStruct&>(*type);
        const auto& names = structure_type.get_element_names();
        const auto& elements = structure_type.get_elements();
        output.write('{');
        for (size_t child = 0; child != elements.size(); ++child) {
            if (child != 0) output.write(',');
            // No JSONB object intermediary and no uint8_t key-length conversion.
            output.write_json_string(names[child]);
            output.write(':');
            RETURN_IF_ERROR(write_json_cell(structure.get_column(child), elements[child], row,
                                            output, options, state));
        }
        output.write('}');
        return Status::OK();
    }
    case TYPE_VARBINARY:
        return Status::NotSupported("JSON OUTFILE does not support ordinary VARBINARY");
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_DECIMALV2: {
        // These legacy types have no typed JSONB conversion. Reuse their CAST
        // text writer explicitly; do not catch arbitrary JSONB failures and fall
        // back.
        auto text = ColumnString::create();
        BufferWritable buffer(*text);
        state.serde(type).to_string(column, row, buffer, options);
        buffer.commit();
        const auto value = text->get_data_at(0);
        if (primitive == TYPE_DECIMALV2) {
            output.write(value.data, value.size);
        } else {
            output.write_json_string(value);
        }
        return Status::OK();
    }
    default: {
        // Typed interface used by TO_JSON, not the rowstore
        // write_one_cell_to_jsonb. MAP uses its existing key/value semantics;
        // nested FILE serdes encode inline as padded Base64 in their JSON object.
        state.jsonb.reset();
        RETURN_IF_ERROR(state.serde(type).serialize_column_to_jsonb(column, row, state.jsonb));
        const JsonbDocument* document = nullptr;
        RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(state.jsonb.getOutput()->getBuffer(),
                                                              state.jsonb.getOutput()->getSize(),
                                                              &document));
        return write_jsonb_value(*document->getValue(), output);
    }
    }
}

} // namespace doris::json_format
