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
#include "vec/columns/column_array.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/functions/cast/cast_to_string.h"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized::CastWrapper {

struct ConvertNothingToJsonb {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IColumn& col_from = *col_with_type_and_name.column;
        auto data_type_to = block.get_by_position(result).type;
        size_t size = col_from.size();
        auto col_to = data_type_to->create_column_const_with_default_value(size);
        ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(size, 1);
        block.replace_by_position(result, ColumnNullable::create(col_to->assume_mutable(),
                                                                 std::move(col_null_map_to)));
        return Status::OK();
    }
};

struct ConvertImplStringToJsonbAsJsonbString {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
        auto data_type_to = block.get_by_position(result).type;
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IColumn& col_from = *col_with_type_and_name.column;
        auto dst = ColumnString::create();
        auto* dst_str = assert_cast<ColumnString*>(dst.get());
        const auto* from_string = assert_cast<const ColumnString*>(&col_from);
        JsonbWriter writer;
        for (size_t i = 0; i < input_rows_count; i++) {
            auto str_ref = from_string->get_data_at(i);
            writer.reset();
            // write raw string to jsonb
            writer.writeStartString();
            writer.writeString(str_ref.data, str_ref.size);
            writer.writeEndString();
            dst_str->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
        }
        block.replace_by_position(result, std::move(dst));
        return Status::OK();
    }
};

// Generic conversion of number to jsonb.
template <typename ColumnType>
struct ConvertImplNumberToJsonb {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);

        auto column_string = ColumnString::create();
        JsonbWriter writer;

        const auto* col =
                check_and_get_column<const ColumnType>(col_with_type_and_name.column.get());
        const auto& data = col->get_data();

        for (size_t i = 0; i < input_rows_count; i++) {
            writer.reset();
            if constexpr (std::is_same_v<ColumnUInt8, ColumnType>) {
                writer.writeBool(data[i]);
            } else if constexpr (std::is_same_v<ColumnInt8, ColumnType>) {
                writer.writeInt8(data[i]);
            } else if constexpr (std::is_same_v<ColumnInt16, ColumnType>) {
                writer.writeInt16(data[i]);
            } else if constexpr (std::is_same_v<ColumnInt32, ColumnType>) {
                writer.writeInt32(data[i]);
            } else if constexpr (std::is_same_v<ColumnInt64, ColumnType>) {
                writer.writeInt64(data[i]);
            } else if constexpr (std::is_same_v<ColumnInt128, ColumnType>) {
                writer.writeInt128(data[i]);
            } else if constexpr (std::is_same_v<ColumnFloat64, ColumnType>) {
                writer.writeDouble(data[i]);
            } else {
                static_assert(std::is_same_v<ColumnType, ColumnUInt8> ||
                                      std::is_same_v<ColumnType, ColumnInt8> ||
                                      std::is_same_v<ColumnType, ColumnInt16> ||
                                      std::is_same_v<ColumnType, ColumnInt32> ||
                                      std::is_same_v<ColumnType, ColumnInt64> ||
                                      std::is_same_v<ColumnType, ColumnInt128> ||
                                      std::is_same_v<ColumnType, ColumnFloat64>,
                              "unsupported type");
                __builtin_unreachable();
            }
            column_string->insert_data(writer.getOutput()->getBuffer(),
                                       writer.getOutput()->getSize());
        }

        block.replace_by_position(result, std::move(column_string));
        return Status::OK();
    }
};

struct ConvertImplGenericFromJsonb {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
        auto data_type_to = block.get_by_position(result).type;
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IColumn& col_from = *col_with_type_and_name.column;
        if (const auto* col_from_string = check_and_get_column<ColumnString>(&col_from)) {
            auto col_to = data_type_to->create_column();

            size_t size = col_from.size();
            col_to->reserve(size);

            ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(size, 0);
            ColumnUInt8::Container* vec_null_map_to = &col_null_map_to->get_data();
            const bool is_complex = is_complex_type(data_type_to->get_primitive_type());
            const bool is_dst_string = is_string_type(data_type_to->get_primitive_type());
            for (size_t i = 0; i < size; ++i) {
                const auto& val = col_from_string->get_data_at(i);
                JsonbDocument* doc = JsonbDocument::checkAndCreateDocument(val.data, val.size);
                if (UNLIKELY(!doc || !doc->getValue())) {
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
                DCHECK(context->jsonb_string_as_string());
                // add string to string column
                if (is_dst_string && value->isString()) {
                    const auto* blob = static_cast<const JsonbBlobVal*>(value);
                    assert_cast<ColumnString&, TypeCheckOnRelease::DISABLE>(*col_to).insert_data(
                            blob->getBlob(), blob->getBlobLen());
                    (*vec_null_map_to)[i] = 0;
                    continue;
                }
                std::string input_str;
                if (context->jsonb_string_as_string() && value->isString()) {
                    const auto* blob = static_cast<const JsonbBlobVal*>(value);
                    input_str = std::string(blob->getBlob(), blob->getBlobLen());
                } else {
                    input_str = JsonbToJson::jsonb_to_json_string(val.data, val.size);
                }
                if (input_str.empty()) {
                    col_to->insert_default();
                    (*vec_null_map_to)[i] = 1;
                    continue;
                }
                ReadBuffer read_buffer((char*)(input_str.data()), input_str.size());
                Status st = data_type_to->from_string(read_buffer, col_to.get());
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

template <PrimitiveType type, typename ColumnType>
struct ConvertImplFromJsonb {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IColumn& col_from = *col_with_type_and_name.column;
        // result column must set type
        DCHECK(block.get_by_position(result).type != nullptr);
        auto data_type_to = block.get_by_position(result).type;
        if (const auto* column_string = check_and_get_column<ColumnString>(&col_from)) {
            auto null_map_col = ColumnUInt8::create(input_rows_count, 0);
            auto& null_map = null_map_col->get_data();
            auto col_to = ColumnType::create();

            //IColumn & col_to = *res;
            // size_t size = col_from.size();
            col_to->reserve(input_rows_count);
            auto& res = col_to->get_data();
            res.resize(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i) {
                const auto& val = column_string->get_data_at(i);
                // ReadBuffer read_buffer((char*)(val.data), val.size);
                // RETURN_IF_ERROR(data_type_to->from_string(read_buffer, col_to));

                if (val.size == 0) {
                    null_map[i] = 1;
                    res[i] = 0;
                    continue;
                }

                // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
                JsonbDocument* doc = JsonbDocument::checkAndCreateDocument(val.data, val.size);
                if (UNLIKELY(!doc || !doc->getValue())) {
                    null_map[i] = 1;
                    res[i] = 0;
                    continue;
                }

                // value is NOT necessary to be deleted since JsonbValue will not allocate memory
                JsonbValue* value = doc->getValue();
                if (UNLIKELY(!value)) {
                    null_map[i] = 1;
                    res[i] = 0;
                    continue;
                }
                if constexpr (type == PrimitiveType::TYPE_BOOLEAN) {
                    // cast from json value to boolean type
                    if (value->isTrue()) {
                        res[i] = 1;
                    } else if (value->isFalse()) {
                        res[i] = 0;
                    } else if (value->isInt()) {
                        res[i] = ((const JsonbIntVal*)value)->val() == 0 ? 0 : 1;
                    } else if (value->isDouble()) {
                        res[i] = static_cast<ColumnType::value_type>(
                                         ((const JsonbDoubleVal*)value)->val()) == 0
                                         ? 0
                                         : 1;
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else if constexpr (type == PrimitiveType::TYPE_TINYINT ||
                                     type == PrimitiveType::TYPE_SMALLINT ||
                                     type == PrimitiveType::TYPE_INT ||
                                     type == PrimitiveType::TYPE_BIGINT ||
                                     type == PrimitiveType::TYPE_LARGEINT) {
                    // cast from json value to integer types
                    if (value->isInt()) {
                        res[i] = ((const JsonbIntVal*)value)->val();
                    } else if (value->isDouble()) {
                        res[i] = static_cast<ColumnType::value_type>(
                                ((const JsonbDoubleVal*)value)->val());
                    } else if (value->isTrue()) {
                        res[i] = 1;
                    } else if (value->isFalse()) {
                        res[i] = 0;
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else if constexpr (type == PrimitiveType::TYPE_FLOAT ||
                                     type == PrimitiveType::TYPE_DOUBLE) {
                    // cast from json value to floating point types
                    if (value->isDouble()) {
                        res[i] = ((const JsonbDoubleVal*)value)->val();
                    } else if (value->isFloat()) {
                        res[i] = ((const JsonbFloatVal*)value)->val();
                    } else if (value->isTrue()) {
                        res[i] = 1;
                    } else if (value->isFalse()) {
                        res[i] = 0;
                    } else if (value->isInt()) {
                        res[i] = ((const JsonbIntVal*)value)->val();
                    } else {
                        null_map[i] = 1;
                        res[i] = 0;
                    }
                } else {
                    throw Exception(Status::FatalError("unsupported type"));
                }
            }

            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_to), std::move(null_map_col)));
        } else {
            return Status::RuntimeError(
                    "Illegal column {} of first argument of conversion function from string",
                    col_from.get_name());
        }
        return Status::OK();
    }
};

// check jsonb value type and get to_type value
WrapperType create_cast_from_jsonb_wrapper(const DataTypeJsonb& from_type,
                                           const DataTypePtr& to_type,
                                           bool jsonb_string_as_string) {
    switch (to_type->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return &ConvertImplFromJsonb<PrimitiveType::TYPE_BOOLEAN, ColumnUInt8>::execute;
    case PrimitiveType::TYPE_TINYINT:
        return &ConvertImplFromJsonb<PrimitiveType::TYPE_TINYINT, ColumnInt8>::execute;
    case PrimitiveType::TYPE_SMALLINT:
        return &ConvertImplFromJsonb<PrimitiveType::TYPE_SMALLINT, ColumnInt16>::execute;
    case PrimitiveType::TYPE_INT:
        return &ConvertImplFromJsonb<PrimitiveType::TYPE_INT, ColumnInt32>::execute;
    case PrimitiveType::TYPE_BIGINT:
        return &ConvertImplFromJsonb<PrimitiveType::TYPE_BIGINT, ColumnInt64>::execute;
    case PrimitiveType::TYPE_LARGEINT:
        return &ConvertImplFromJsonb<PrimitiveType::TYPE_LARGEINT, ColumnInt128>::execute;
    case PrimitiveType::TYPE_DOUBLE:
        return &ConvertImplFromJsonb<PrimitiveType::TYPE_DOUBLE, ColumnFloat64>::execute;
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
        if (!jsonb_string_as_string) {
            // Conversion from String through parsing.
            return &CastToString::execute_impl;
        } else {
            return ConvertImplGenericFromJsonb::execute;
        }
    default:
        return ConvertImplGenericFromJsonb::execute;
    }
}

// create cresponding jsonb value with type to_type
// use jsonb writer to create jsonb value
WrapperType create_cast_to_jsonb_wrapper(const DataTypePtr& from_type, const DataTypeJsonb& to_type,
                                         bool string_as_jsonb_string) {
    switch (from_type->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return &ConvertImplNumberToJsonb<ColumnUInt8>::execute;
    case PrimitiveType::TYPE_TINYINT:
        return &ConvertImplNumberToJsonb<ColumnInt8>::execute;
    case PrimitiveType::TYPE_SMALLINT:
        return &ConvertImplNumberToJsonb<ColumnInt16>::execute;
    case PrimitiveType::TYPE_INT:
        return &ConvertImplNumberToJsonb<ColumnInt32>::execute;
    case PrimitiveType::TYPE_BIGINT:
        return &ConvertImplNumberToJsonb<ColumnInt64>::execute;
    case PrimitiveType::TYPE_LARGEINT:
        return &ConvertImplNumberToJsonb<ColumnInt128>::execute;
    case PrimitiveType::TYPE_DOUBLE:
        return &ConvertImplNumberToJsonb<ColumnFloat64>::execute;
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
        if (string_as_jsonb_string) {
            // We convert column string to jsonb type just add a string jsonb field to dst column instead of parse
            // each line in original string column.
            return &ConvertImplStringToJsonbAsJsonbString::execute;
        } else {
            return cast_from_string_to_generic;
        }
    case PrimitiveType::INVALID_TYPE:
        return &ConvertNothingToJsonb::execute;
    default:
        return cast_from_generic_to_jsonb;
    }
}

} // namespace doris::vectorized::CastWrapper