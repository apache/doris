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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypesDecimal.cpp
// and modified by Doris

#include "vec/data_types/data_type_decimal.h"

#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <streamvbyte.h>
#include <sys/types.h>

#include <cstring>

#include "agent/be_exec_version_manager.h"
#include "runtime/decimalv2_value.h"
#include "util/string_parser.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/int_exp.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/typeid_cast.h"
#include "vec/core/types.h"
#include "vec/io/io_helper.h"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized {

template <typename T>
std::string DataTypeDecimal<T>::do_get_name() const {
    std::stringstream ss;
    ss << "Decimal(" << precision << ", " << scale << ")";
    return ss.str();
}

template <typename T>
bool DataTypeDecimal<T>::equals(const IDataType& rhs) const {
    if (auto* ptype = typeid_cast<const DataTypeDecimal<T>*>(&rhs)) {
        return precision == ptype->get_precision() && scale == ptype->get_scale();
    }
    return false;
}

template <typename T>
std::string DataTypeDecimal<T>::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    if constexpr (!IsDecimalV2<T>) {
        auto value = assert_cast<const ColumnType&>(*ptr).get_element(row_num);
        return value.to_string(scale);
    } else {
        auto value = (DecimalV2Value)assert_cast<const ColumnType&>(*ptr).get_element(row_num);
        return value.to_string(get_format_scale());
    }
}

template <typename T>
void DataTypeDecimal<T>::to_string(const IColumn& column, size_t row_num,
                                   BufferWritable& ostr) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    if constexpr (!IsDecimalV2<T>) {
        T value = assert_cast<const ColumnType&>(*ptr).get_element(row_num);
        auto str = value.to_string(scale);
        ostr.write(str.data(), str.size());
    } else {
        auto value = (DecimalV2Value)assert_cast<const ColumnType&>(*ptr).get_element(row_num);
        auto str = value.to_string(get_format_scale());
        ostr.write(str.data(), str.size());
    }
}

template <typename T>
void DataTypeDecimal<T>::to_string_batch(const IColumn& column, ColumnString& column_to) const {
    // column may be column const
    const auto& col_ptr = column.get_ptr();
    const auto& [column_ptr, is_const] = unpack_if_const(col_ptr);
    if (is_const) {
        to_string_batch_impl<true>(column_ptr, column_to);
    } else {
        to_string_batch_impl<false>(column_ptr, column_to);
    }
}

template <typename T>
template <bool is_const>
void DataTypeDecimal<T>::to_string_batch_impl(const ColumnPtr& column_ptr,
                                              ColumnString& column_to) const {
    auto& col_vec = assert_cast<const ColumnType&>(*column_ptr);
    const auto size = col_vec.size();
    auto& chars = column_to.get_chars();
    auto& offsets = column_to.get_offsets();
    offsets.resize(size);
    chars.reserve(4 * sizeof(T));
    for (int row_num = 0; row_num < size; row_num++) {
        auto num = is_const ? col_vec.get_element(0) : col_vec.get_element(row_num);
        if constexpr (!IsDecimalV2<T>) {
            T value = num;
            auto str = value.to_string(scale);
            chars.insert(str.begin(), str.end());
        } else {
            auto value = (DecimalV2Value)num;
            auto str = value.to_string(get_format_scale());
            chars.insert(str.begin(), str.end());
        }
        offsets[row_num] = chars.size();
    }
}

template <typename T>
std::string DataTypeDecimal<T>::to_string(const T& value) const {
    return value.to_string(get_format_scale());
}

template <typename T>
Status DataTypeDecimal<T>::from_string(ReadBuffer& rb, IColumn* column) const {
    auto& column_data = static_cast<ColumnType&>(*column).get_data();
    T val {};
    StringParser::ParseResult res =
            read_decimal_text_impl<DataTypeDecimalSerDe<T>::get_primitive_type(), T>(
                    val, rb, precision, scale);
    if (res == StringParser::PARSE_SUCCESS || res == StringParser::PARSE_UNDERFLOW) {
        column_data.emplace_back(val);
        return Status::OK();
    }
    return Status::InvalidArgument("parse decimal fail, string: '{}', primitive type: '{}'",
                                   std::string(rb.position(), rb.count()).c_str(),
                                   DataTypeDecimalSerDe<T>::get_primitive_type());
}

// binary: row_num | value1 | value2 | ...
template <typename T>
int64_t DataTypeDecimal<T>::get_uncompressed_serialized_bytes(const IColumn& column,
                                                              int be_exec_version) const {
    if (be_exec_version >= USE_NEW_SERDE) {
        auto size = sizeof(T) * column.size();
        if (size <= SERIALIZED_MEM_SIZE_LIMIT) {
            return sizeof(uint32_t) + size;
        } else {
            return sizeof(uint32_t) + sizeof(size_t) +
                   std::max(size, streamvbyte_max_compressedbytes(upper_int32(size)));
        }
    } else {
        return sizeof(uint32_t) + column.size() * sizeof(FieldType);
    }
}

template <typename T>
char* DataTypeDecimal<T>::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    if (be_exec_version >= USE_NEW_SERDE) {
        // row num
        const auto mem_size = column.size() * sizeof(T);
        *reinterpret_cast<uint32_t*>(buf) = mem_size;
        buf += sizeof(uint32_t);
        // column data
        auto ptr = column.convert_to_full_column_if_const();
        const auto* origin_data =
                assert_cast<const ColumnDecimal<T>&>(*ptr.get()).get_data().data();
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, origin_data, mem_size);
            return buf + mem_size;
        }

        auto encode_size =
                streamvbyte_encode(reinterpret_cast<const uint32_t*>(origin_data),
                                   upper_int32(mem_size), (uint8_t*)(buf + sizeof(size_t)));
        *reinterpret_cast<size_t*>(buf) = encode_size;
        buf += sizeof(size_t);
        return buf + encode_size;
    } else {
        // row num
        const auto row_num = column.size();
        *reinterpret_cast<uint32_t*>(buf) = row_num;
        buf += sizeof(uint32_t);
        // column values
        auto ptr = column.convert_to_full_column_if_const();
        const auto* origin_data = assert_cast<const ColumnType&>(*ptr.get()).get_data().data();
        memcpy(buf, origin_data, row_num * sizeof(FieldType));
        buf += row_num * sizeof(FieldType);
        return buf;
    }
}

template <typename T>
const char* DataTypeDecimal<T>::deserialize(const char* buf, IColumn* column,
                                            int be_exec_version) const {
    if (be_exec_version >= USE_NEW_SERDE) {
        // row num
        uint32_t mem_size = *reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        // column data
        auto& container = assert_cast<ColumnDecimal<T>*>(column)->get_data();
        container.resize(mem_size / sizeof(T));
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(container.data(), buf, mem_size);
            return buf + mem_size;
        }

        size_t encode_size = *reinterpret_cast<const size_t*>(buf);
        buf += sizeof(size_t);
        streamvbyte_decode((const uint8_t*)buf, (uint32_t*)(container.data()),
                           upper_int32(mem_size));
        return buf + encode_size;
    } else {
        // row num
        uint32_t row_num = *reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        // column values
        auto& container = assert_cast<ColumnType*>(column)->get_data();
        container.resize(row_num);
        memcpy(container.data(), buf, row_num * sizeof(FieldType));
        buf += row_num * sizeof(FieldType);
        return buf;
    }
}

template <typename T>
void DataTypeDecimal<T>::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    col_meta->mutable_decimal_param()->set_precision(precision);
    col_meta->mutable_decimal_param()->set_scale(scale);
}

template <typename T>
Field DataTypeDecimal<T>::get_default() const {
    return DecimalField(T(), scale);
}
template <typename T>
MutableColumnPtr DataTypeDecimal<T>::create_column() const {
    if constexpr (IsDecimalV2<T>) {
        auto col = ColumnDecimal128V2::create(0, scale);
        return col;
    } else {
        return ColumnType::create(0, scale);
    }
}

template <typename T>
bool DataTypeDecimal<T>::parse_from_string(const std::string& str, T* res) const {
    StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
    res->value = StringParser::string_to_decimal<DataTypeDecimalSerDe<T>::get_primitive_type()>(
            str.c_str(), str.size(), precision, scale, &result);
    return result == StringParser::PARSE_SUCCESS || result == StringParser::PARSE_UNDERFLOW;
}

DataTypePtr create_decimal(UInt64 precision_value, UInt64 scale_value, bool use_v2) {
    auto max_precision =
            use_v2 ? max_decimal_precision<Decimal128V2>() : max_decimal_precision<Decimal256>();
    if (precision_value < min_decimal_precision() || precision_value > max_precision) {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Wrong precision {}, min: {}, max: {}", precision_value,
                               min_decimal_precision(), max_precision);
    }

    if (static_cast<UInt64>(scale_value) > precision_value) {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Negative scales and scales larger than precision are not "
                               "supported, scale_value: {}, precision_value: {}",
                               scale_value, precision_value);
    }

    if (use_v2) {
        return std::make_shared<DataTypeDecimal<Decimal128V2>>(precision_value, scale_value);
    }

    if (precision_value <= max_decimal_precision<Decimal32>()) {
        return std::make_shared<DataTypeDecimal<Decimal32>>(precision_value, scale_value);
    } else if (precision_value <= max_decimal_precision<Decimal64>()) {
        return std::make_shared<DataTypeDecimal<Decimal64>>(precision_value, scale_value);
    } else if (precision_value <= max_decimal_precision<Decimal128V3>()) {
        return std::make_shared<DataTypeDecimal<Decimal128V3>>(precision_value, scale_value);
    }
    return std::make_shared<DataTypeDecimal<Decimal256>>(precision_value, scale_value);
}

template <>
Decimal32 DataTypeDecimal<Decimal32>::get_scale_multiplier(UInt32 scale) {
    return common::exp10_i32(scale);
}

template <>
Decimal64 DataTypeDecimal<Decimal64>::get_scale_multiplier(UInt32 scale) {
    return common::exp10_i64(scale);
}

template <>
Decimal128V2 DataTypeDecimal<Decimal128V2>::get_scale_multiplier(UInt32 scale) {
    return common::exp10_i128(scale);
}

template <>
Decimal128V3 DataTypeDecimal<Decimal128V3>::get_scale_multiplier(UInt32 scale) {
    return common::exp10_i128(scale);
}

template <>
Decimal256 DataTypeDecimal<Decimal256>::get_scale_multiplier(UInt32 scale) {
    return Decimal256(common::exp10_i256(scale));
}

template <>
Decimal32 DataTypeDecimal<Decimal32>::get_max_digits_number(UInt32 digit_count) {
    return common::max_i32(digit_count);
}
template <>
Decimal64 DataTypeDecimal<Decimal64>::get_max_digits_number(UInt32 digit_count) {
    return common::max_i64(digit_count);
}
template <>
Decimal128V2 DataTypeDecimal<Decimal128V2>::get_max_digits_number(UInt32 digit_count) {
    return common::max_i128(digit_count);
}
template <>
Decimal128V3 DataTypeDecimal<Decimal128V3>::get_max_digits_number(UInt32 digit_count) {
    return common::max_i128(digit_count);
}
template <>
Decimal256 DataTypeDecimal<Decimal256>::get_max_digits_number(UInt32 digit_count) {
    return Decimal256(common::max_i256(digit_count));
}

/// Explicit template instantiations.
template class DataTypeDecimal<Decimal32>;
template class DataTypeDecimal<Decimal64>;
template class DataTypeDecimal<Decimal128V2>;
template class DataTypeDecimal<Decimal128V3>;
template class DataTypeDecimal<Decimal256>;

} // namespace doris::vectorized
