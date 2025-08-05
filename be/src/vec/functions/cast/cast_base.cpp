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

#include "cast_base.h"

#include <cstdint>

#include "util/jsonb_writer.h"
namespace doris::vectorized::CastWrapper {

Status cast_from_generic_to_jsonb(FunctionContext* context, Block& block,
                                  const ColumnNumbers& arguments, uint32_t result,
                                  size_t input_rows_count, const NullMap::value_type* null_map) {
    auto data_type_to = block.get_by_position(result).type;
    const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
    const IDataType& type = *col_with_type_and_name.type;
    const IColumn& col_from = *col_with_type_and_name.column;

    auto column_string = ColumnString::create();
    JsonbWriter writer;

    ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(col_from.size(), 0);
    ColumnUInt8::Container* vec_null_map_to = &col_null_map_to->get_data();
    DataTypeSerDe::FormatOptions format_options;
    format_options.converted_from_string = true;
    DataTypeSerDeSPtr from_serde = type.get_serde();
    DataTypeSerDeSPtr to_serde = data_type_to->get_serde();
    auto col_to = data_type_to->create_column();

    auto tmp_col = ColumnString::create();
    vectorized::DataTypeSerDe::FormatOptions options;
    options.escape_char = '\\';
    for (size_t i = 0; i < input_rows_count; i++) {
        // convert to string
        tmp_col->clear();
        VectorBufferWriter write_buffer(*tmp_col.get());
        Status st = from_serde->serialize_column_to_json(col_from, i, i + 1, write_buffer, options);
        // if serialized failed, will return null
        (*vec_null_map_to)[i] = !st.ok();
        if (!st.ok()) {
            col_to->insert_default();
            continue;
        }
        write_buffer.commit();
        writer.reset();
        auto str_ref = tmp_col->get_data_at(0);
        Slice data((char*)(str_ref.data), str_ref.size);
        // first try to parse string
        st = to_serde->deserialize_one_cell_from_json(*col_to, data, format_options);
        // if parsing failed, will return null
        (*vec_null_map_to)[i] = !st.ok();
        if (!st.ok()) {
            col_to->insert_default();
        }
    }

    block.replace_by_position(
            result, ColumnNullable::create(std::move(col_to), std::move(col_null_map_to)));
    return Status::OK();
}

Status cast_from_string_to_generic(FunctionContext* context, Block& block,
                                   const ColumnNumbers& arguments, uint32_t result,
                                   size_t input_rows_count, const NullMap::value_type* null_map) {
    const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
    const IColumn& col_from = *col_with_type_and_name.column;
    // result column must set type
    DCHECK(block.get_by_position(result).type != nullptr);
    auto data_type_to = block.get_by_position(result).type;
    if (const auto* col_from_string = check_and_get_column<ColumnString>(&col_from)) {
        auto col_to = data_type_to->create_column();
        auto serde = data_type_to->get_serde();
        size_t size = col_from.size();
        col_to->reserve(size);

        ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(size, 0);
        ColumnUInt8::Container* vec_null_map_to = &col_null_map_to->get_data();
        const bool is_complex = is_complex_type(data_type_to->get_primitive_type());
        DataTypeSerDe::FormatOptions format_options;
        format_options.converted_from_string = true;
        format_options.escape_char = '\\';

        for (size_t i = 0; i < size; ++i) {
            const auto& val = col_from_string->get_data_at(i);
            // Note: here we should handle the null element
            if (val.size == 0) {
                col_to->insert_default();
                // empty string('') is an invalid format for complex type, set null_map to 1
                if (is_complex) {
                    (*vec_null_map_to)[i] = 1;
                }
                continue;
            }
            Slice string_slice(val.data, val.size);
            Status st =
                    serde->deserialize_one_cell_from_json(*col_to, string_slice, format_options);
            // if parsing failed, will return null
            (*vec_null_map_to)[i] = !st.ok();
            if (!st.ok()) {
                col_to->insert_default();
            }
        }
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
    } else {
        return Status::RuntimeError(
                "Illegal column {} of first argument of conversion function from string",
                col_from.get_name());
    }
    return Status::OK();
}

ElementWrappers get_element_wrappers(FunctionContext* context, const DataTypes& from_element_types,
                                     const DataTypes& to_element_types) {
    DCHECK(from_element_types.size() == to_element_types.size());
    ElementWrappers element_wrappers;
    element_wrappers.reserve(from_element_types.size());
    for (size_t i = 0; i < from_element_types.size(); ++i) {
        const DataTypePtr& from_element_type = from_element_types[i];
        const DataTypePtr& to_element_type = to_element_types[i];
        element_wrappers.push_back(
                prepare_unpack_dictionaries(context, from_element_type, to_element_type));
    }
    return element_wrappers;
}

WrapperType create_unsupport_wrapper(const String error_msg) {
    return [error_msg](FunctionContext* /*context*/, Block& /*block*/,
                       const ColumnNumbers& /*arguments*/, uint32_t /*result*/,
                       size_t /*input_rows_count*/, const NullMap::value_type* null_map = nullptr) {
        return Status::InvalidArgument(error_msg);
    };
}

WrapperType create_unsupport_wrapper(const String from_type_name, const String to_type_name) {
    const String error_msg =
            fmt::format("Conversion from {} to {} is not supported", from_type_name, to_type_name);
    return create_unsupport_wrapper(error_msg);
}

WrapperType create_identity_wrapper(const DataTypePtr&) {
    return [](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
              uint32_t result, size_t /*input_rows_count*/,
              const NullMap::value_type* null_map = nullptr) {
        block.get_by_position(result).column = block.get_by_position(arguments.front()).column;
        return Status::OK();
    };
}

WrapperType create_nothing_wrapper(const IDataType* to_type) {
    ColumnPtr res = to_type->create_column_const_with_default_value(1);
    return [res](FunctionContext* context, Block& block, const ColumnNumbers&, uint32_t result,
                 size_t input_rows_count, const NullMap::value_type* null_map = nullptr) {
        /// Column of Nothing type is trivially convertible to any other column
        block.get_by_position(result).column =
                res->clone_resized(input_rows_count)->convert_to_full_column_if_const();
        return Status::OK();
    };
}

Status cast_from_string_to_complex_type(FunctionContext* context, Block& block,
                                        const ColumnNumbers& arguments, uint32_t result,
                                        size_t input_rows_count,
                                        const NullMap::value_type* null_map) {
    const auto* col_from = check_and_get_column<DataTypeString::ColumnType>(
            block.get_by_position(arguments[0]).column.get());

    auto to_type = block.get_by_position(result).type;
    auto to_serde = remove_nullable(to_type)->get_serde();
    MutableColumnPtr to_column = make_nullable(to_type)->create_column();
    auto& nullable_col_to = assert_cast<ColumnNullable&>(*to_column);
    auto& nested_column = nullable_col_to.get_nested_column();
    DataTypeSerDe::FormatOptions options;
    options.converted_from_string = true;
    options.escape_char = '\\';
    options.timezone = &context->state()->timezone_obj();
    for (size_t i = 0; i < input_rows_count; ++i) {
        if (null_map && null_map[i]) {
            nullable_col_to.insert_default();
        } else {
            auto str = col_from->get_data_at(i);
            Status st = to_serde->from_string(str, nested_column, options);
            if (st.ok()) {
                nullable_col_to.get_null_map_data().push_back(0);
            } else {
                nullable_col_to.insert_default(); // fill null if fail
            }
        }
    }

    block.get_by_position(result).column = std::move(to_column);
    return Status::OK();
}

Status cast_from_string_to_complex_type_strict_mode(FunctionContext* context, Block& block,
                                                    const ColumnNumbers& arguments, uint32_t result,
                                                    size_t input_rows_count,
                                                    const NullMap::value_type* null_map) {
    const auto* col_from = check_and_get_column<DataTypeString::ColumnType>(
            block.get_by_position(arguments[0]).column.get());

    auto to_type = block.get_by_position(result).type;
    auto to_serde = remove_nullable(to_type)->get_serde();
    MutableColumnPtr to_column = remove_nullable(to_type)->create_column();
    DataTypeSerDe::FormatOptions options;
    options.converted_from_string = true;
    options.escape_char = '\\';
    options.timezone = &context->state()->timezone_obj();
    for (size_t i = 0; i < input_rows_count; ++i) {
        if (null_map && null_map[i]) {
            to_column->insert_default();
        } else {
            auto str = col_from->get_data_at(i);
            RETURN_IF_ERROR(to_serde->from_string_strict_mode(str, *to_column, options));
        }
    }
    block.get_by_position(result).column = std::move(to_column);
    return Status::OK();
}

} // namespace doris::vectorized::CastWrapper