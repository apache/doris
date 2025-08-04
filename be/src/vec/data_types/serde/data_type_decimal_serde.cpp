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

#include "data_type_decimal_serde.h"

#include <arrow/array/array_base.h>
#include <arrow/array/array_decimal.h>
#include <arrow/builder.h>
#include <arrow/util/decimal.h>

#include <type_traits>

#include "arrow/type.h"
#include "orc/Int128.hh"
#include "util/jsonb_document.h"
#include "util/jsonb_document_cast.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/columns/column_decimal.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/functions/cast/cast_to_decimal.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
// #include "common/compile_check_begin.h"

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::from_string_batch(const ColumnString& str, ColumnNullable& column,
                                                  const FormatOptions& options) const {
    const auto row = str.size();
    column.resize(row);

    const ColumnString::Chars* chars = &str.get_chars();
    const IColumn::Offsets* offsets = &str.get_offsets();

    auto& column_to = assert_cast<ColumnType&>(column.get_nested_column());
    auto& vec_to = column_to.get_data();
    auto& null_map = column.get_null_map_data();
    size_t current_offset = 0;
    auto arg_precision = static_cast<UInt32>(precision);
    auto arg_scale = static_cast<UInt32>(scale);
    CastParameters params;
    params.is_strict = false;
    for (size_t i = 0; i < row; ++i) {
        size_t next_offset = (*offsets)[i];
        size_t string_size = next_offset - current_offset;

        null_map[i] = !CastToDecimal::from_string(StringRef(&(*chars)[current_offset], string_size),
                                                  vec_to[i], arg_precision, arg_scale, params);
        current_offset = next_offset;
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::from_string_strict_mode_batch(
        const ColumnString& str, IColumn& column, const FormatOptions& options,
        const NullMap::value_type* null_map) const {
    const auto row = str.size();
    column.resize(row);

    const ColumnString::Chars* chars = &str.get_chars();
    const IColumn::Offsets* offsets = &str.get_offsets();

    auto& column_to = assert_cast<ColumnType&>(column);
    auto& vec_to = column_to.get_data();
    size_t current_offset = 0;
    auto arg_precision = static_cast<UInt32>(precision);
    auto arg_scale = static_cast<UInt32>(scale);
    CastParameters params;
    params.is_strict = true;
    for (size_t i = 0; i < row; ++i) {
        if (null_map && null_map[i]) {
            continue;
        }
        size_t next_offset = (*offsets)[i];
        size_t string_size = next_offset - current_offset;

        if (!CastToDecimal::from_string(StringRef(&(*chars)[current_offset], string_size),
                                        vec_to[i], arg_precision, arg_scale, params)) {
            return Status::InvalidArgument(
                    "parse number fail, string: '{}'",
                    std::string((char*)&(*chars)[current_offset], string_size));
        }
        current_offset = next_offset;
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::from_string(StringRef& str, IColumn& column,
                                            const FormatOptions& options) const {
    auto& column_to = assert_cast<ColumnType&>(column);
    FieldType to;

    CastParameters params;
    params.is_strict = false;

    auto arg_precision = static_cast<UInt32>(precision);
    auto arg_scale = static_cast<UInt32>(scale);

    if (!CastToDecimal::from_string(str, to, arg_precision, arg_scale, params)) {
        return Status::InvalidArgument("parse Decimal fail, string: '{}'", str.to_string());
    }
    column_to.insert_value(to);
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::from_string_strict_mode(StringRef& str, IColumn& column,
                                                        const FormatOptions& options) const {
    auto& column_to = assert_cast<ColumnType&>(column);
    FieldType to;

    CastParameters params;
    params.is_strict = true;

    auto arg_precision = static_cast<UInt32>(precision);
    auto arg_scale = static_cast<UInt32>(scale);

    if (!CastToDecimal::from_string(str, to, arg_precision, arg_scale, params)) {
        return Status::InvalidArgument("parse Decimal fail, string: '{}'", str.to_string());
    }
    column_to.insert_value(to);
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                         int64_t end_idx, BufferWritable& bw,
                                                         FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                           BufferWritable& bw,
                                                           FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    auto& col = assert_cast<const ColumnDecimal<T>&>(*ptr);
    if constexpr (T != TYPE_DECIMALV2) {
        FieldType value = col.get_element(row_num);
        auto decimal_str = value.to_string(scale);
        bw.write(decimal_str.data(), decimal_str.size());
    } else {
        char buf[FieldType::max_string_length()];
        auto length = col.get_element(row_num).to_string(buf, scale, scale_multiplier);
        bw.write(buf, length);
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                               const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnDecimal<T>&>(column).get_data();
    FieldType val = {};
    StringRef str_ref(slice.data, slice.size);
    StringParser::ParseResult res =
            read_decimal_text_impl<get_primitive_type(), FieldType>(val, str_ref, precision, scale);
    if (res == StringParser::PARSE_SUCCESS || res == StringParser::PARSE_UNDERFLOW) {
        column_data.emplace_back(val);
        return Status::OK();
    }
    return Status::InvalidArgument("parse decimal fail, string: '{}', primitive type: '{}'",
                                   std::string(slice.data, slice.size).c_str(),
                                   get_primitive_type());
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::write_column_to_arrow(const IColumn& column,
                                                      const NullMap* null_map,
                                                      arrow::ArrayBuilder* array_builder,
                                                      int64_t start, int64_t end,
                                                      const cctz::time_zone& ctz) const {
    auto& col = reinterpret_cast<const ColumnDecimal<T>&>(column);
    if constexpr (T == TYPE_DECIMALV2) {
        auto& builder = reinterpret_cast<arrow::Decimal128Builder&>(*array_builder);
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal128Type>(27, 9);
        for (size_t i = start; i < end; ++i) {
            if (null_map && (*null_map)[i]) {
                RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column.get_name(),
                                                 array_builder->type()->name()));
                continue;
            }
            const auto& data_ref = col.get_data_at(i);
            const PackedInt128* p_value = reinterpret_cast<const PackedInt128*>(data_ref.data);
            int64_t high = (p_value->value) >> 64;
            uint64_t low = cast_set<uint64_t>((p_value->value) & 0xFFFFFFFFFFFFFFFF);
            arrow::Decimal128 value(high, low);
            RETURN_IF_ERROR(checkArrowStatus(builder.Append(value), column.get_name(),
                                             array_builder->type()->name()));
        }
    } else if constexpr (T == TYPE_DECIMAL128I) {
        auto& builder = reinterpret_cast<arrow::Decimal128Builder&>(*array_builder);
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal128Type>(38, col.get_scale());
        for (size_t i = start; i < end; ++i) {
            if (null_map && (*null_map)[i]) {
                RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column.get_name(),
                                                 array_builder->type()->name()));
                continue;
            }
            const auto& data_ref = col.get_data_at(i);
            const PackedInt128* p_value = reinterpret_cast<const PackedInt128*>(data_ref.data);
            int64_t high = (p_value->value) >> 64;
            uint64_t low = cast_set<uint64_t>((p_value->value) & 0xFFFFFFFFFFFFFFFF);
            arrow::Decimal128 value(high, low);
            RETURN_IF_ERROR(checkArrowStatus(builder.Append(value), column.get_name(),
                                             array_builder->type()->name()));
        }
    } else if constexpr (T == TYPE_DECIMAL32) {
        auto& builder = reinterpret_cast<arrow::Decimal128Builder&>(*array_builder);
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal128Type>(8, col.get_scale());
        for (size_t i = start; i < end; ++i) {
            if (null_map && (*null_map)[i]) {
                RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column.get_name(),
                                                 array_builder->type()->name()));
                continue;
            }
            Int128 p_value = col.get_element(i).value;
            arrow::Decimal128 value(reinterpret_cast<const uint8_t*>(&p_value));
            RETURN_IF_ERROR(checkArrowStatus(builder.Append(value), column.get_name(),
                                             array_builder->type()->name()));
        }
    } else if constexpr (T == TYPE_DECIMAL64) {
        auto& builder = reinterpret_cast<arrow::Decimal128Builder&>(*array_builder);
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal128Type>(18, col.get_scale());
        for (size_t i = start; i < end; ++i) {
            if (null_map && (*null_map)[i]) {
                RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column.get_name(),
                                                 array_builder->type()->name()));
                continue;
            }
            Int128 p_value = col.get_element(i).value;
            arrow::Decimal128 value(reinterpret_cast<const uint8_t*>(&p_value));
            RETURN_IF_ERROR(checkArrowStatus(builder.Append(value), column.get_name(),
                                             array_builder->type()->name()));
        }
    } else if constexpr (T == TYPE_DECIMAL256) {
        auto& builder = reinterpret_cast<arrow::Decimal256Builder&>(*array_builder);
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal256Type>(76, col.get_scale());
        for (size_t i = start; i < end; ++i) {
            if (null_map && (*null_map)[i]) {
                RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column.get_name(),
                                                 array_builder->type()->name()));
                continue;
            }
            auto p_value = wide::Int256(col.get_element(i));
            using half_type = wide::Int256::base_type; // uint64_t
            half_type a0 = p_value.items[wide::Int256::_impl::little(0)];
            half_type a1 = p_value.items[wide::Int256::_impl::little(1)];
            half_type a2 = p_value.items[wide::Int256::_impl::little(2)];
            half_type a3 = p_value.items[wide::Int256::_impl::little(3)];

            std::array<uint64_t, 4> word_array = {a0, a1, a2, a3};
            arrow::Decimal256 value(arrow::Decimal256::LittleEndianArray, word_array);
            RETURN_IF_ERROR(checkArrowStatus(builder.Append(value), column.get_name(),
                                             array_builder->type()->name()));
        }
    } else {
        return Status::InvalidArgument("write_column_to_arrow with type " + column.get_name());
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::read_column_from_arrow(IColumn& column,
                                                       const arrow::Array* arrow_array,
                                                       int64_t start, int64_t end,
                                                       const cctz::time_zone& ctz) const {
    auto& column_data = static_cast<ColumnDecimal<T>&>(column).get_data();
    // Decimal<Int128> for decimalv2
    // Decimal<Int128I> for deicmalv3
    if constexpr (T == TYPE_DECIMALV2) {
        const auto* concrete_array = dynamic_cast<const arrow::DecimalArray*>(arrow_array);
        const auto* arrow_decimal_type =
                static_cast<const arrow::DecimalType*>(arrow_array->type().get());
        const auto arrow_scale = arrow_decimal_type->scale();
        // TODO check precision
        for (auto value_i = start; value_i < end; ++value_i) {
            auto value = *reinterpret_cast<const vectorized::Decimal128V2*>(
                    concrete_array->Value(value_i));
            // convert scale to 9;
            if (9 > arrow_scale) {
                using MaxNativeType = typename Decimal128V2::NativeType;
                MaxNativeType converted_value = common::exp10_i128(9 - arrow_scale);
                if (common::mul_overflow(static_cast<MaxNativeType>(value), converted_value,
                                         converted_value)) {
                    VLOG_DEBUG << "Decimal convert overflow";
                    value = converted_value < 0
                                    ? std::numeric_limits<typename Decimal128V2 ::NativeType>::min()
                                    : std::numeric_limits<
                                              typename Decimal128V2 ::NativeType>::max();
                } else {
                    value = converted_value;
                }
            } else if (9 < arrow_scale) {
                value = value / common::exp10_i128(arrow_scale - 9);
            }
            column_data.emplace_back(value);
        }
    } else if constexpr (T == TYPE_DECIMAL32 || T == TYPE_DECIMAL64 || T == TYPE_DECIMAL128I) {
        const auto* concrete_array = dynamic_cast<const arrow::DecimalArray*>(arrow_array);
        for (auto value_i = start; value_i < end; ++value_i) {
            column_data.emplace_back(
                    *reinterpret_cast<const FieldType*>(concrete_array->Value(value_i)));
        }
    } else if constexpr (T == TYPE_DECIMAL256) {
        const auto* concrete_array = dynamic_cast<const arrow::Decimal256Array*>(arrow_array);
        for (auto value_i = start; value_i < end; ++value_i) {
            column_data.emplace_back(
                    *reinterpret_cast<const FieldType*>(concrete_array->Value(value_i)));
        }
    } else {
        return Status::Error(ErrorCode::NOT_IMPLEMENTED_ERROR,
                             "read_column_from_arrow with type " + column.get_name());
    }
    return Status::OK();
}

template <PrimitiveType T>
template <bool is_binary_format>
Status DataTypeDecimalSerDe<T>::_write_column_to_mysql(const IColumn& column,
                                                       MysqlRowBuffer<is_binary_format>& result,
                                                       int64_t row_idx, bool col_const,
                                                       const FormatOptions& options) const {
    auto& data = assert_cast<const ColumnDecimal<T>&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    if constexpr (T == TYPE_DECIMALV2) {
        DecimalV2Value decimal_val(data[col_index]);
        auto decimal_str = decimal_val.to_string(scale);
        if (UNLIKELY(0 != result.push_string(decimal_str.c_str(), decimal_str.size()))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    } else {
        char buf[FieldType::max_string_length()];
        auto length = data[col_index].to_string(buf, scale, scale_multiplier);
        if (UNLIKELY(0 != result.push_string(buf, length))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<true>& row_buffer,
                                                      int64_t row_idx, bool col_const,
                                                      const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<false>& row_buffer,
                                                      int64_t row_idx, bool col_const,
                                                      const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::write_column_to_orc(const std::string& timezone,
                                                    const IColumn& column, const NullMap* null_map,
                                                    orc::ColumnVectorBatch* orc_col_batch,
                                                    int64_t start, int64_t end,
                                                    vectorized::Arena& arena) const {
    if constexpr (T == TYPE_DECIMAL256) {
        return Status::NotSupported("write_column_to_orc with type " + column.get_name());
    } else {
        constexpr bool use_int128 =
                (sizeof(typename ColumnDecimal<T>::value_type) >= sizeof(Int128));
        auto& col_data = assert_cast<const ColumnDecimal<T>&>(column).get_data();
        auto* cur_batch =
                dynamic_cast<std::conditional_t<use_int128, orc::Decimal128VectorBatch,
                                                orc::Decimal64VectorBatch>*>(orc_col_batch);
        for (size_t row_id = start; row_id < end; row_id++) {
            if (cur_batch->notNull[row_id] == 1) {
                const auto& int_value = col_data[row_id].value;
                if constexpr (use_int128) {
                    // orc::Int128 only support construct from two int64_t values
                    // so we need to split the int128 value into two int64_t values
                    orc::Int128 value(int_value >> 64, (uint64_t)int_value);
                    cur_batch->values[row_id] = value;
                } else {
                    cur_batch->values[row_id] = int_value;
                }
            }
        }
        cur_batch->numElements = end - start;
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::deserialize_column_from_fixed_json(
        IColumn& column, Slice& slice, uint64_t rows, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    if (rows < 1) [[unlikely]] {
        return Status::OK();
    }
    Status st = deserialize_one_cell_from_json(column, slice, options);
    if (!st.ok()) {
        return st;
    }

    DataTypeDecimalSerDe::insert_column_last_value_multiple_times(column, rows - 1);
    *num_deserialized = rows;
    return Status::OK();
}

template <PrimitiveType T>
void DataTypeDecimalSerDe<T>::insert_column_last_value_multiple_times(IColumn& column,
                                                                      uint64_t times) const {
    if (times < 1) [[unlikely]] {
        return;
    }
    auto& col = static_cast<ColumnDecimal<T>&>(column);
    auto sz = col.size();

    FieldType val = col.get_element(sz - 1);
    for (int i = 0; i < times; i++) {
        col.insert_value(val);
    }
}

template <PrimitiveType T>
void DataTypeDecimalSerDe<T>::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                      Arena& mem_pool, int32_t col_id,
                                                      int64_t row_num) const {
    StringRef data_ref = column.get_data_at(row_num);
    result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    if constexpr (T == TYPE_DECIMALV2) {
        Decimal128V2::NativeType val =
                *reinterpret_cast<const Decimal128V2::NativeType*>(data_ref.data);
        result.writeInt128(val);
    } else if constexpr (T == TYPE_DECIMAL128I) {
        Decimal128V3::NativeType val =
                *reinterpret_cast<const Decimal128V3::NativeType*>(data_ref.data);
        result.writeInt128(val);
    } else if constexpr (T == TYPE_DECIMAL32) {
        Decimal32::NativeType val = *reinterpret_cast<const Decimal32::NativeType*>(data_ref.data);
        result.writeInt32(val);
    } else if constexpr (T == TYPE_DECIMAL64) {
        Decimal64::NativeType val = *reinterpret_cast<const Decimal64::NativeType*>(data_ref.data);
        result.writeInt64(val);
    } else if constexpr (T == TYPE_DECIMAL256) {
        // use binary type, since jsonb does not support int256
        result.writeStartBinary();
        result.writeBinary(data_ref.data, data_ref.size);
        result.writeEndBinary();
    } else {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "write_one_cell_to_jsonb with type " + column.get_name());
    }
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::serialize_column_to_jsonb(const IColumn& from_column,
                                                          int64_t row_num,
                                                          JsonbWriter& writer) const {
    if constexpr (T == TYPE_DECIMALV2) {
        return Status::NotSupported("DECIMALV2 does not support serialize_column_to_jsonb");
    } else {
        const auto& data = assert_cast<const ColumnDecimal<T>&>(from_column).get_element(row_num);
        if (!writer.writeDecimal(data, precision, scale)) {
            return Status::InvalidArgument(
                    "DataTypeDecimalSerDe<T>::serialize_column_to_jsonb failed");
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::serialize_column_to_jsonb_vector(const IColumn& from_column,
                                                                 ColumnString& to_column) const {
    if constexpr (T == TYPE_DECIMALV2) {
        return Status::NotSupported("DECIMALV2 does not support serialize_column_to_jsonb_vector");
    } else {
        const auto size = from_column.size();
        JsonbWriter writer;
        const auto& data = assert_cast<const ColumnDecimal<T>&>(from_column).get_data();
        for (int i = 0; i < size; i++) {
            writer.reset();
            if (!writer.writeDecimal(data[i], precision, scale)) {
                return Status::InvalidArgument(
                        "DataTypeDecimalSerDe<T>::serialize_column_to_jsonb failed");
            }
            to_column.insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDecimalSerDe<T>::deserialize_column_from_jsonb(IColumn& column,
                                                              const JsonbValue* jsonb_value,
                                                              CastParameters& castParms) const {
    if constexpr (T == TYPE_DECIMALV2) {
        return Status::NotSupported("DECIMALV2 does not support deserialize_column_from_jsonb");
    } else {
        if (jsonb_value->isString()) {
            RETURN_IF_ERROR(parse_column_from_jsonb_string(column, jsonb_value, castParms));
            return Status::OK();
        }
        auto& data = assert_cast<ColumnDecimal<T>&>(column).get_data();
        FieldType to;
        if (!JsonbCast::cast_from_json_to_decimal(jsonb_value, to, precision, scale, castParms)) {
            return JsonbCast::report_error(jsonb_value, T);
        }
        data.push_back(to);
        return Status::OK();
    }
}

template <PrimitiveType T>
void DataTypeDecimalSerDe<T>::read_one_cell_from_jsonb(IColumn& column,
                                                       const JsonbValue* arg) const {
    auto& col = reinterpret_cast<ColumnDecimal<T>&>(column);
    if constexpr (T == TYPE_DECIMALV2) {
        col.insert_value(arg->unpack<JsonbInt128Val>()->val());
    } else if constexpr (T == TYPE_DECIMAL128I) {
        col.insert_value(arg->unpack<JsonbInt128Val>()->val());
    } else if constexpr (T == TYPE_DECIMAL32) {
        col.insert_value(arg->unpack<JsonbInt32Val>()->val());
    } else if constexpr (T == TYPE_DECIMAL64) {
        col.insert_value(arg->unpack<JsonbInt64Val>()->val());
    } else if constexpr (T == TYPE_DECIMAL256) {
        // use binary type, since jsonb does not support int256
        const wide::Int256 val =
                *reinterpret_cast<const wide::Int256*>(arg->unpack<JsonbBinaryVal>()->getBlob());
        col.insert_value(Decimal256(val));
    } else {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "read_one_cell_from_jsonb with type " + column.get_name());
    }
}

template class DataTypeDecimalSerDe<TYPE_DECIMAL32>;
template class DataTypeDecimalSerDe<TYPE_DECIMAL64>;
template class DataTypeDecimalSerDe<TYPE_DECIMAL128I>;
template class DataTypeDecimalSerDe<TYPE_DECIMALV2>;
template class DataTypeDecimalSerDe<TYPE_DECIMAL256>;
} // namespace doris::vectorized
