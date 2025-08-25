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
#include "runtime/jsonb_value.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/functions/cast/cast_to_string.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized::CastWrapper {
#include "common/compile_check_begin.h"

struct ConvertImplGenericFromJsonb {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count,
                          const NullMap::value_type* null_map = nullptr) {
        auto data_type_to = block.get_by_position(result).type;
        auto data_type_serde_to = data_type_to->get_serde();

        DataTypeSerDe::FormatOptions options;
        options.converted_from_string = true;
        options.escape_char = '\\';
        options.timezone = &context->state()->timezone_obj();

        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IColumn& col_from = *col_with_type_and_name.column;
        if (const ColumnString* col_from_string = check_and_get_column<ColumnString>(&col_from)) {
            auto col_to = data_type_to->create_column();

            size_t size = col_from.size();
            col_to->reserve(size);

            ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(size, 0);
            ColumnUInt8::Container* vec_null_map_to = &col_null_map_to->get_data();
            const bool is_complex = is_complex_type(data_type_to->get_primitive_type());
            const bool is_dst_string = is_string_type(data_type_to->get_primitive_type());
            for (size_t i = 0; i < size; ++i) {
                const auto& val = col_from_string->get_data_at(i);
                JsonbDocument* doc = nullptr;
                auto st = JsonbDocument::checkAndCreateDocument(val.data, val.size, &doc);
                if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
                    (*vec_null_map_to)[i] = 1;
                    col_to->insert_default();
                    continue;
                }

                // value is NOT necessary to be deleted since JsonbValue will not allocate memory
                JsonbValue* value = doc->getValue();
                if (UNLIKELY(!value)) {
                    (*vec_null_map_to)[i] = 1;
                    col_to->insert_default();
                    continue;
                }
                // Note: here we should handle the null element
                if (val.size == 0) {
                    col_to->insert_default();
                    // empty string('') is an invalid format for complex type, set null_map to 1
                    if (is_complex) {
                        (*vec_null_map_to)[i] = 1;
                    }
                    continue;
                }
                // add string to string column
                if (context->jsonb_string_as_string() && is_dst_string && value->isString()) {
                    const auto* blob = value->unpack<JsonbBinaryVal>();
                    assert_cast<ColumnString&, TypeCheckOnRelease::DISABLE>(*col_to).insert_data(
                            blob->getBlob(), blob->getBlobLen());
                    (*vec_null_map_to)[i] = 0;
                    continue;
                }
                std::string input_str;
                if (context->jsonb_string_as_string() && value->isString()) {
                    const auto* blob = value->unpack<JsonbBinaryVal>();
                    input_str = std::string(blob->getBlob(), blob->getBlobLen());
                } else {
                    input_str = JsonbToJson::jsonb_to_json_string(val.data, val.size);
                }
                if (input_str.empty()) {
                    col_to->insert_default();
                    (*vec_null_map_to)[i] = 1;
                    continue;
                }
                StringRef read_buffer((char*)(input_str.data()), input_str.size());
                st = data_type_serde_to->from_string(read_buffer, *col_to, options);
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
};

inline bool can_cast_json_type(PrimitiveType pt) {
    return is_int_or_bool(pt) || is_float_or_double(pt) || is_string_type(pt) || is_decimal(pt) ||
           pt == TYPE_ARRAY || pt == TYPE_STRUCT;
}

// check jsonb value type and get to_type value
WrapperType create_cast_from_jsonb_wrapper(const DataTypeJsonb& from_type,
                                           const DataTypePtr& to_type,
                                           bool jsonb_string_as_string) {
    if (is_string_type(to_type->get_primitive_type()) && jsonb_string_as_string) {
        return ConvertImplGenericFromJsonb::execute;
    }

    return [](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
              uint32_t result, size_t input_rows_count, const NullMap::value_type*) {
        CastParameters params;
        params.is_strict = context->enable_strict_mode();
        auto data_type_to = make_nullable(block.get_by_position(result).type);
        auto serde_to = data_type_to->get_serde();
        const auto& col_from_json =
                assert_cast<const ColumnString&>(*block.get_by_position(arguments[0]).column);
        size_t size = col_from_json.size();
        auto col_to = data_type_to->create_column();
        for (size_t i = 0; i < size; ++i) {
            const auto& val = col_from_json.get_data_at(i);
            JsonbDocument* doc = nullptr;
            auto st = JsonbDocument::checkAndCreateDocument(val.data, val.size, &doc);
            if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
                col_to->insert_default();
                continue;
            }
            JsonbValue* value = doc->getValue();
            if (UNLIKELY(!value)) {
                col_to->insert_default();
                continue;
            }
            if (val.size == 0) {
                col_to->insert_default();
                continue;
            }
            RETURN_IF_ERROR(serde_to->deserialize_column_from_jsonb(*col_to, value, params));
        }
        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    };
}

struct ParseJsonbFromString {
    static Status parse_json(const StringRef& str, ColumnString& column_string) {
        if (str.empty()) {
            return Status::InvalidArgument("Empty string cannot be parsed as jsonb");
        }
        JsonBinaryValue value;
        auto st = (value.from_json_string(str.data, str.size));
        if (!st.ok()) {
            return Status::InvalidArgument("Failed to parse json string: {}, error: {}",
                                           str.to_string(), st.msg());
        }
        column_string.insert_data(value.value(), value.size());
        return Status::OK();
    }

    static Status execute_non_strict(const ColumnString& col_from, size_t size,
                                     ColumnPtr& column_result) {
        auto col_to = ColumnString::create();
        auto col_null = ColumnUInt8::create(size, 0);
        auto& vec_null_map_to = col_null->get_data();
        for (size_t i = 0; i < size; ++i) {
            Status st = parse_json(col_from.get_data_at(i), *col_to);
            vec_null_map_to[i] = !st.ok();
            if (!st.ok()) {
                col_to->insert_default();
            }
        }
        column_result = ColumnNullable::create(std::move(col_to), std::move(col_null));
        return Status::OK();
    }

    static Status execute_strict(const ColumnString& col_from, const NullMap::value_type* null_map,
                                 size_t size, ColumnPtr& column_result) {
        auto col_to = ColumnString::create();
        for (size_t i = 0; i < size; ++i) {
            if (null_map && null_map[i]) {
                col_to->insert_default();
                continue;
            }
            RETURN_IF_ERROR(parse_json(col_from.get_data_at(i), *col_to));
        }
        column_result = std::move(col_to);
        return Status::OK();
    }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count,
                          const NullMap::value_type* null_map) {
        const auto& col_from =
                assert_cast<const ColumnString&>(*block.get_by_position(arguments[0]).column);
        const auto size = col_from.size();

        ColumnPtr column_result;
        if (context->enable_strict_mode()) {
            RETURN_IF_ERROR(execute_strict(col_from, null_map, size, column_result));

        } else {
            RETURN_IF_ERROR(execute_non_strict(col_from, size, column_result));
        }
        block.get_by_position(result).column = std::move(column_result);

        return Status::OK();
    }
};

// create cresponding jsonb value with type to_type
// use jsonb writer to create jsonb value
WrapperType create_cast_to_jsonb_wrapper(const DataTypePtr& from_type, const DataTypeJsonb& to_type,
                                         bool string_as_jsonb_string) {
    // parse string as jsonb
    if (is_string_type(from_type->get_primitive_type()) && !string_as_jsonb_string) {
        return ParseJsonbFromString::execute;
    }

    return [](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
              uint32_t result, size_t input_rows_count, const NullMap::value_type*) {
        // same as to_json function
        auto to_column = ColumnString::create();
        auto from_type_serde = block.get_by_position(arguments[0]).type->get_serde();
        auto from_column = block.get_by_position(arguments[0]).column;
        RETURN_IF_ERROR(
                from_type_serde->serialize_column_to_jsonb_vector(*from_column, *to_column));
        block.get_by_position(result).column = std::move(to_column);
        return Status::OK();
    };
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized::CastWrapper