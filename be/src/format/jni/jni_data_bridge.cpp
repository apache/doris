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

#include "jni_data_bridge.h"

#include <glog/logging.h>

#include <sstream>
#include <variant>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_varbinary.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "core/value/decimalv2_value.h"

namespace doris {

#define FOR_FIXED_LENGTH_TYPES(M)                                  \
    M(PrimitiveType::TYPE_TINYINT, ColumnInt8, Int8)               \
    M(PrimitiveType::TYPE_BOOLEAN, ColumnUInt8, UInt8)             \
    M(PrimitiveType::TYPE_SMALLINT, ColumnInt16, Int16)            \
    M(PrimitiveType::TYPE_INT, ColumnInt32, Int32)                 \
    M(PrimitiveType::TYPE_BIGINT, ColumnInt64, Int64)              \
    M(PrimitiveType::TYPE_LARGEINT, ColumnInt128, Int128)          \
    M(PrimitiveType::TYPE_FLOAT, ColumnFloat32, Float32)           \
    M(PrimitiveType::TYPE_DOUBLE, ColumnFloat64, Float64)          \
    M(PrimitiveType::TYPE_DECIMALV2, ColumnDecimal128V2, Int128)   \
    M(PrimitiveType::TYPE_DECIMAL128I, ColumnDecimal128V3, Int128) \
    M(PrimitiveType::TYPE_DECIMAL32, ColumnDecimal32, Int32)       \
    M(PrimitiveType::TYPE_DECIMAL64, ColumnDecimal64, Int64)       \
    M(PrimitiveType::TYPE_DATE, ColumnDate, Int64)                 \
    M(PrimitiveType::TYPE_DATEV2, ColumnDateV2, UInt32)            \
    M(PrimitiveType::TYPE_DATETIME, ColumnDateTime, Int64)         \
    M(PrimitiveType::TYPE_DATETIMEV2, ColumnDateTimeV2, UInt64)    \
    M(PrimitiveType::TYPE_TIMESTAMPTZ, ColumnTimeStampTz, UInt64)  \
    M(PrimitiveType::TYPE_IPV4, ColumnIPv4, IPv4)                  \
    M(PrimitiveType::TYPE_IPV6, ColumnIPv6, IPv6)

Status JniDataBridge::fill_block(Block* block, const ColumnNumbers& arguments, long table_address) {
    if (table_address == 0) {
        return Status::InternalError("table_address is 0");
    }
    TableMetaAddress table_meta(table_address);
    long num_rows = table_meta.next_meta_as_long();
    for (size_t i : arguments) {
        if (block->get_by_position(i).column.get() == nullptr) {
            auto return_type = block->get_data_type(i);
            bool result_nullable = return_type->is_nullable();
            ColumnUInt8::MutablePtr null_col = nullptr;
            if (result_nullable) {
                return_type = remove_nullable(return_type);
                null_col = ColumnUInt8::create();
            }
            auto res_col = return_type->create_column();
            if (result_nullable) {
                block->replace_by_position(
                        i, ColumnNullable::create(std::move(res_col), std::move(null_col)));
            } else {
                block->replace_by_position(i, std::move(res_col));
            }
        } else if (is_column_const(*(block->get_by_position(i).column))) {
            auto doris_column = block->get_by_position(i).column->convert_to_full_column_if_const();
            bool is_nullable = block->get_by_position(i).type->is_nullable();
            block->replace_by_position(i, is_nullable ? make_nullable(doris_column) : doris_column);
        }
        auto& column_with_type_and_name = block->get_by_position(i);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        RETURN_IF_ERROR(fill_column(table_meta, column_ptr, column_type, num_rows));
    }
    return Status::OK();
}

Status JniDataBridge::fill_column(TableMetaAddress& address, ColumnPtr& doris_column,
                                  const DataTypePtr& data_type, size_t num_rows) {
    auto logical_type = data_type->get_primitive_type();
    void* null_map_ptr = address.next_meta_as_ptr();
    if (null_map_ptr == nullptr) {
        // org.apache.doris.common.jni.vec.ColumnType.Type#UNSUPPORTED will set column address as 0
        return Status::InternalError("Unsupported type {} in java side", data_type->get_name());
    }
    MutableColumnPtr data_column;
    if (doris_column->is_nullable()) {
        auto* nullable_column =
                reinterpret_cast<ColumnNullable*>(doris_column->assume_mutable().get());
        data_column = nullable_column->get_nested_column_ptr();
        NullMap& null_map = nullable_column->get_null_map_data();
        size_t origin_size = null_map.size();
        null_map.resize(origin_size + num_rows);
        memcpy(null_map.data() + origin_size, static_cast<bool*>(null_map_ptr), num_rows);
    } else {
        data_column = doris_column->assume_mutable();
    }
    // Date and DateTime are deprecated and not supported.
    switch (logical_type) {
#define DISPATCH(TYPE_INDEX, COLUMN_TYPE, CPP_TYPE)              \
    case TYPE_INDEX:                                             \
        return _fill_fixed_length_column<COLUMN_TYPE, CPP_TYPE>( \
                data_column, reinterpret_cast<CPP_TYPE*>(address.next_meta_as_ptr()), num_rows);
        FOR_FIXED_LENGTH_TYPES(DISPATCH)
#undef DISPATCH
    case PrimitiveType::TYPE_STRING:
        [[fallthrough]];
    case PrimitiveType::TYPE_CHAR:
        [[fallthrough]];
    case PrimitiveType::TYPE_VARCHAR:
        return _fill_string_column(address, data_column, num_rows);
    case PrimitiveType::TYPE_ARRAY:
        return _fill_array_column(address, data_column, data_type, num_rows);
    case PrimitiveType::TYPE_MAP:
        return _fill_map_column(address, data_column, data_type, num_rows);
    case PrimitiveType::TYPE_STRUCT:
        return _fill_struct_column(address, data_column, data_type, num_rows);
    case PrimitiveType::TYPE_VARBINARY:
        return _fill_varbinary_column(address, data_column, num_rows);
    default:
        return Status::InvalidArgument("Unsupported type {} in jni scanner", data_type->get_name());
    }
    return Status::OK();
}

Status JniDataBridge::_fill_varbinary_column(TableMetaAddress& address,
                                             MutableColumnPtr& doris_column, size_t num_rows) {
    auto* meta_base = reinterpret_cast<char*>(address.next_meta_as_ptr());
    auto& varbinary_col = assert_cast<ColumnVarbinary&>(*doris_column);
    // Java side writes per-row metadata as 16 bytes: [len: long][addr: long]
    for (size_t i = 0; i < num_rows; ++i) {
        // Read length (first 8 bytes)
        int64_t len = 0;
        memcpy(&len, meta_base + 16 * i, sizeof(len));
        if (len <= 0) {
            varbinary_col.insert_default();
        } else {
            // Read address (next 8 bytes)
            uint64_t addr_u = 0;
            memcpy(&addr_u, meta_base + 16 * i + 8, sizeof(addr_u));
            const char* src = reinterpret_cast<const char*>(addr_u);
            varbinary_col.insert_data(src, static_cast<size_t>(len));
        }
    }
    return Status::OK();
}

Status JniDataBridge::_fill_string_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                          size_t num_rows) {
    auto& string_col = static_cast<ColumnString&>(*doris_column);
    ColumnString::Chars& string_chars = string_col.get_chars();
    ColumnString::Offsets& string_offsets = string_col.get_offsets();
    int* offsets = reinterpret_cast<int*>(address.next_meta_as_ptr());
    char* chars = reinterpret_cast<char*>(address.next_meta_as_ptr());

    // This judgment is necessary, otherwise the following statement `offsets[num_rows - 1]` out of bounds
    // What's more, This judgment must be placed after `address.next_meta_as_ptr()`
    // because `address.next_meta_as_ptr` will make `address._meta_index` plus 1
    if (num_rows == 0) {
        return Status::OK();
    }

    size_t origin_chars_size = string_chars.size();
    string_chars.resize(origin_chars_size + offsets[num_rows - 1]);
    memcpy(string_chars.data() + origin_chars_size, chars, offsets[num_rows - 1]);

    size_t origin_offsets_size = string_offsets.size();
    size_t start_offset = string_offsets[origin_offsets_size - 1];
    string_offsets.resize(origin_offsets_size + num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        string_offsets[origin_offsets_size + i] =
                static_cast<unsigned int>(offsets[i] + start_offset);
    }
    return Status::OK();
}

Status JniDataBridge::_fill_array_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                         const DataTypePtr& data_type, size_t num_rows) {
    ColumnPtr& element_column = static_cast<ColumnArray&>(*doris_column).get_data_ptr();
    const DataTypePtr& element_type =
            (assert_cast<const DataTypeArray*>(remove_nullable(data_type).get()))
                    ->get_nested_type();
    ColumnArray::Offsets64& offsets_data = static_cast<ColumnArray&>(*doris_column).get_offsets();

    int64_t* offsets = reinterpret_cast<int64_t*>(address.next_meta_as_ptr());
    size_t origin_size = offsets_data.size();
    offsets_data.resize(origin_size + num_rows);
    size_t start_offset = offsets_data[origin_size - 1];
    for (size_t i = 0; i < num_rows; ++i) {
        offsets_data[origin_size + i] = offsets[i] + start_offset;
    }

    return fill_column(address, element_column, element_type,
                       offsets_data[origin_size + num_rows - 1] - start_offset);
}

Status JniDataBridge::_fill_map_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                       const DataTypePtr& data_type, size_t num_rows) {
    auto& map = static_cast<ColumnMap&>(*doris_column);
    const DataTypePtr& key_type =
            reinterpret_cast<const DataTypeMap*>(remove_nullable(data_type).get())->get_key_type();
    const DataTypePtr& value_type =
            reinterpret_cast<const DataTypeMap*>(remove_nullable(data_type).get())
                    ->get_value_type();
    ColumnPtr& key_column = map.get_keys_ptr();
    ColumnPtr& value_column = map.get_values_ptr();
    ColumnArray::Offsets64& map_offsets = map.get_offsets();

    int64_t* offsets = reinterpret_cast<int64_t*>(address.next_meta_as_ptr());
    size_t origin_size = map_offsets.size();
    map_offsets.resize(origin_size + num_rows);
    size_t start_offset = map_offsets[origin_size - 1];
    for (size_t i = 0; i < num_rows; ++i) {
        map_offsets[origin_size + i] = offsets[i] + start_offset;
    }

    RETURN_IF_ERROR(fill_column(address, key_column, key_type,
                                map_offsets[origin_size + num_rows - 1] - start_offset));
    RETURN_IF_ERROR(fill_column(address, value_column, value_type,
                                map_offsets[origin_size + num_rows - 1] - start_offset));
    return Status::OK();
}

Status JniDataBridge::_fill_struct_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                          const DataTypePtr& data_type, size_t num_rows) {
    auto& doris_struct = static_cast<ColumnStruct&>(*doris_column);
    const DataTypeStruct* doris_struct_type =
            reinterpret_cast<const DataTypeStruct*>(remove_nullable(data_type).get());
    for (int i = 0; i < doris_struct.tuple_size(); ++i) {
        ColumnPtr& struct_field = doris_struct.get_column_ptr(i);
        const DataTypePtr& field_type = doris_struct_type->get_element(i);
        RETURN_IF_ERROR(fill_column(address, struct_field, field_type, num_rows));
    }
    return Status::OK();
}

std::string JniDataBridge::get_jni_type(const DataTypePtr& data_type) {
    DataTypePtr type = remove_nullable(data_type);
    std::ostringstream buffer;
    switch (type->get_primitive_type()) {
    case TYPE_BOOLEAN:
        return "boolean";
    case TYPE_TINYINT:
        return "tinyint";
    case TYPE_SMALLINT:
        return "smallint";
    case TYPE_INT:
        return "int";
    case TYPE_BIGINT:
        return "bigint";
    case TYPE_LARGEINT:
        return "largeint";
    case TYPE_FLOAT:
        return "float";
    case TYPE_DOUBLE:
        return "double";
    case TYPE_IPV4:
        return "ipv4";
    case TYPE_IPV6:
        return "ipv6";
    case TYPE_VARCHAR:
        [[fallthrough]];
    case TYPE_CHAR:
        [[fallthrough]];
    case TYPE_STRING:
        return "string";
    case TYPE_DATE:
        return "datev1";
    case TYPE_DATEV2:
        return "datev2";
    case TYPE_DATETIME:
        return "datetimev1";
    case TYPE_DATETIMEV2:
        [[fallthrough]];
    case TYPE_TIMEV2: {
        buffer << "datetimev2(" << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_TIMESTAMPTZ: {
        buffer << "timestamptz(" << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_BINARY:
        return "binary";
    case TYPE_DECIMALV2: {
        buffer << "decimalv2(" << DecimalV2Value::PRECISION << "," << DecimalV2Value::SCALE << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL32: {
        buffer << "decimal32(" << type->get_precision() << "," << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL64: {
        buffer << "decimal64(" << type->get_precision() << "," << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL128I: {
        buffer << "decimal128(" << type->get_precision() << "," << type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_STRUCT: {
        const DataTypeStruct* struct_type = reinterpret_cast<const DataTypeStruct*>(type.get());
        buffer << "struct<";
        for (int i = 0; i < struct_type->get_elements().size(); ++i) {
            if (i != 0) {
                buffer << ",";
            }
            buffer << struct_type->get_element_names()[i] << ":"
                   << get_jni_type(struct_type->get_element(i));
        }
        buffer << ">";
        return buffer.str();
    }
    case TYPE_ARRAY: {
        const DataTypeArray* array_type = reinterpret_cast<const DataTypeArray*>(type.get());
        buffer << "array<" << get_jni_type(array_type->get_nested_type()) << ">";
        return buffer.str();
    }
    case TYPE_MAP: {
        const DataTypeMap* map_type = reinterpret_cast<const DataTypeMap*>(type.get());
        buffer << "map<" << get_jni_type(map_type->get_key_type()) << ","
               << get_jni_type(map_type->get_value_type()) << ">";
        return buffer.str();
    }
    case TYPE_VARBINARY:
        return "varbinary";
    // bitmap, hll, quantile_state, jsonb are transferred as strings via JNI
    case TYPE_BITMAP:
        [[fallthrough]];
    case TYPE_HLL:
        [[fallthrough]];
    case TYPE_QUANTILE_STATE:
        [[fallthrough]];
    case TYPE_JSONB:
        return "string";
    default:
        return "unsupported";
    }
}

std::string JniDataBridge::get_jni_type_with_different_string(const DataTypePtr& data_type) {
    DataTypePtr type = remove_nullable(data_type);
    std::ostringstream buffer;
    switch (data_type->get_primitive_type()) {
    case TYPE_BOOLEAN:
        return "boolean";
    case TYPE_TINYINT:
        return "tinyint";
    case TYPE_SMALLINT:
        return "smallint";
    case TYPE_INT:
        return "int";
    case TYPE_BIGINT:
        return "bigint";
    case TYPE_LARGEINT:
        return "largeint";
    case TYPE_FLOAT:
        return "float";
    case TYPE_DOUBLE:
        return "double";
    case TYPE_IPV4:
        return "ipv4";
    case TYPE_IPV6:
        return "ipv6";
    case TYPE_VARCHAR: {
        buffer << "varchar("
               << assert_cast<const DataTypeString*>(remove_nullable(data_type).get())->len()
               << ")";
        return buffer.str();
    }
    case TYPE_DATE:
        return "datev1";
    case TYPE_DATEV2:
        return "datev2";
    case TYPE_DATETIME:
        return "datetimev1";
    case TYPE_DATETIMEV2:
        [[fallthrough]];
    case TYPE_TIMEV2: {
        buffer << "datetimev2(" << data_type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_TIMESTAMPTZ: {
        buffer << "timestamptz(" << data_type->get_scale() << ")";
        return buffer.str();
    }
    case TYPE_BINARY:
        return "binary";
    case TYPE_CHAR: {
        buffer << "char("
               << assert_cast<const DataTypeString*>(remove_nullable(data_type).get())->len()
               << ")";
        return buffer.str();
    }
    case TYPE_STRING:
        return "string";
    case TYPE_VARBINARY:
        buffer << "varbinary("
               << assert_cast<const DataTypeVarbinary*>(remove_nullable(data_type).get())->len()
               << ")";
        return buffer.str();
    case TYPE_DECIMALV2: {
        buffer << "decimalv2(" << DecimalV2Value::PRECISION << "," << DecimalV2Value::SCALE << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL32: {
        buffer << "decimal32(" << data_type->get_precision() << "," << data_type->get_scale()
               << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL64: {
        buffer << "decimal64(" << data_type->get_precision() << "," << data_type->get_scale()
               << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL128I: {
        buffer << "decimal128(" << data_type->get_precision() << "," << data_type->get_scale()
               << ")";
        return buffer.str();
    }
    case TYPE_STRUCT: {
        const auto* type_struct =
                assert_cast<const DataTypeStruct*>(remove_nullable(data_type).get());
        buffer << "struct<";
        for (int i = 0; i < type_struct->get_elements().size(); ++i) {
            if (i != 0) {
                buffer << ",";
            }
            buffer << type_struct->get_element_name(i) << ":"
                   << get_jni_type_with_different_string(type_struct->get_element(i));
        }
        buffer << ">";
        return buffer.str();
    }
    case TYPE_ARRAY: {
        const auto* type_arr = assert_cast<const DataTypeArray*>(remove_nullable(data_type).get());
        buffer << "array<" << get_jni_type_with_different_string(type_arr->get_nested_type())
               << ">";
        return buffer.str();
    }
    case TYPE_MAP: {
        const auto* type_map = assert_cast<const DataTypeMap*>(remove_nullable(data_type).get());
        buffer << "map<" << get_jni_type_with_different_string(type_map->get_key_type()) << ","
               << get_jni_type_with_different_string(type_map->get_value_type()) << ">";
        return buffer.str();
    }
    // bitmap, hll, quantile_state, jsonb are transferred as strings via JNI
    case TYPE_BITMAP:
        [[fallthrough]];
    case TYPE_HLL:
        [[fallthrough]];
    case TYPE_QUANTILE_STATE:
        [[fallthrough]];
    case TYPE_JSONB:
        return "string";
    default:
        return "unsupported";
    }
}

Status JniDataBridge::_fill_column_meta(const ColumnPtr& doris_column, const DataTypePtr& data_type,
                                        std::vector<long>& meta_data) {
    auto logical_type = data_type->get_primitive_type();
    const IColumn* column = nullptr;
    // insert const flag
    if (is_column_const(*doris_column)) {
        meta_data.emplace_back((long)1);
        const auto& const_column = assert_cast<const ColumnConst&>(*doris_column);
        column = &(const_column.get_data_column());
    } else {
        meta_data.emplace_back((long)0);
        column = &(*doris_column);
    }

    // insert null map address
    const IColumn* data_column = nullptr;
    if (column->is_nullable()) {
        const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
        data_column = &(nullable_column.get_nested_column());
        const auto& null_map = nullable_column.get_null_map_data();
        meta_data.emplace_back((long)null_map.data());
    } else {
        meta_data.emplace_back(0);
        data_column = column;
    }
    switch (logical_type) {
#define DISPATCH(TYPE_INDEX, COLUMN_TYPE, CPP_TYPE)                                          \
    case TYPE_INDEX: {                                                                       \
        meta_data.emplace_back(_get_fixed_length_column_address<COLUMN_TYPE>(*data_column)); \
        break;                                                                               \
    }
        FOR_FIXED_LENGTH_TYPES(DISPATCH)
#undef DISPATCH
    case PrimitiveType::TYPE_STRING:
        [[fallthrough]];
    case PrimitiveType::TYPE_CHAR:
        [[fallthrough]];
    case PrimitiveType::TYPE_VARCHAR: {
        const auto& string_column = assert_cast<const ColumnString&>(*data_column);
        // insert offsets
        meta_data.emplace_back((long)string_column.get_offsets().data());
        meta_data.emplace_back((long)string_column.get_chars().data());
        break;
    }
    case PrimitiveType::TYPE_ARRAY: {
        const auto& element_column = assert_cast<const ColumnArray&>(*data_column).get_data_ptr();
        meta_data.emplace_back(
                (long)assert_cast<const ColumnArray&>(*data_column).get_offsets().data());
        const auto& element_type = assert_cast<const DataTypePtr&>(
                (assert_cast<const DataTypeArray*>(remove_nullable(data_type).get()))
                        ->get_nested_type());
        RETURN_IF_ERROR(_fill_column_meta(element_column, element_type, meta_data));
        break;
    }
    case PrimitiveType::TYPE_STRUCT: {
        const auto& doris_struct = assert_cast<const ColumnStruct&>(*data_column);
        const auto* doris_struct_type =
                assert_cast<const DataTypeStruct*>(remove_nullable(data_type).get());
        for (int i = 0; i < doris_struct.tuple_size(); ++i) {
            const auto& struct_field = doris_struct.get_column_ptr(i);
            const auto& field_type =
                    assert_cast<const DataTypePtr&>(doris_struct_type->get_element(i));
            RETURN_IF_ERROR(_fill_column_meta(struct_field, field_type, meta_data));
        }
        break;
    }
    case PrimitiveType::TYPE_MAP: {
        const auto& map = assert_cast<const ColumnMap&>(*data_column);
        const auto& key_type = assert_cast<const DataTypePtr&>(
                assert_cast<const DataTypeMap*>(remove_nullable(data_type).get())->get_key_type());
        const auto& value_type = assert_cast<const DataTypePtr&>(
                assert_cast<const DataTypeMap*>(remove_nullable(data_type).get())
                        ->get_value_type());
        const auto& key_column = map.get_keys_ptr();
        const auto& value_column = map.get_values_ptr();
        meta_data.emplace_back((long)map.get_offsets().data());
        RETURN_IF_ERROR(_fill_column_meta(key_column, key_type, meta_data));
        RETURN_IF_ERROR(_fill_column_meta(value_column, value_type, meta_data));
        break;
    }
    case PrimitiveType::TYPE_VARBINARY: {
        const auto& varbinary_col = assert_cast<const ColumnVarbinary&>(*data_column);
        meta_data.emplace_back(
                (long)assert_cast<const ColumnVarbinary&>(varbinary_col).get_data().data());
        break;
    }
    default:
        return Status::InternalError("Unsupported type: {}", data_type->get_name());
    }
    return Status::OK();
}

Status JniDataBridge::to_java_table(Block* block, std::unique_ptr<long[]>& meta) {
    ColumnNumbers arguments;
    for (size_t i = 0; i < block->columns(); ++i) {
        arguments.emplace_back(i);
    }
    return to_java_table(block, block->rows(), arguments, meta);
}

Status JniDataBridge::to_java_table(Block* block, size_t num_rows, const ColumnNumbers& arguments,
                                    std::unique_ptr<long[]>& meta) {
    std::vector<long> meta_data;
    // insert number of rows
    meta_data.emplace_back(num_rows);
    for (size_t i : arguments) {
        auto& column_with_type_and_name = block->get_by_position(i);
        RETURN_IF_ERROR(_fill_column_meta(column_with_type_and_name.column,
                                          column_with_type_and_name.type, meta_data));
    }

    meta.reset(new long[meta_data.size()]);
    memcpy(meta.get(), &meta_data[0], meta_data.size() * 8);
    return Status::OK();
}

std::pair<std::string, std::string> JniDataBridge::parse_table_schema(
        Block* block, const ColumnNumbers& arguments, bool ignore_column_name) {
    // prepare table schema
    std::ostringstream required_fields;
    std::ostringstream columns_types;
    for (int i = 0; i < arguments.size(); ++i) {
        std::string type = JniDataBridge::get_jni_type(block->get_by_position(arguments[i]).type);
        if (i == 0) {
            if (ignore_column_name) {
                required_fields << "_col_" << arguments[i];
            } else {
                required_fields << block->get_by_position(arguments[i]).name;
            }
            columns_types << type;
        } else {
            if (ignore_column_name) {
                required_fields << ","
                                << "_col_" << arguments[i];
            } else {
                required_fields << "," << block->get_by_position(arguments[i]).name;
            }
            columns_types << "#" << type;
        }
    }
    return std::make_pair(required_fields.str(), columns_types.str());
}

std::pair<std::string, std::string> JniDataBridge::parse_table_schema(Block* block) {
    ColumnNumbers arguments;
    for (size_t i = 0; i < block->columns(); ++i) {
        arguments.emplace_back(i);
    }
    return parse_table_schema(block, arguments, true);
}

} // namespace doris
