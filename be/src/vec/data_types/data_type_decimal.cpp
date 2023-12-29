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
#include <string.h>

#include <utility>

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

    auto value = assert_cast<const ColumnType&>(*ptr).get_element(row_num);
    return value.to_string(scale);
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
        DecimalV2Value value =
                (DecimalV2Value)assert_cast<const ColumnType&>(*ptr).get_element(row_num);
        auto str = value.to_string(scale);
        ostr.write(str.data(), str.size());
    }
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
    return sizeof(uint32_t) + column.size() * sizeof(FieldType);
}

template <typename T>
char* DataTypeDecimal<T>::serialize(const IColumn& column, char* buf, int be_exec_version) const {
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

template <typename T>
const char* DataTypeDecimal<T>::deserialize(const char* buf, IColumn* column,
                                            int be_exec_version) const {
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
    return result == StringParser::PARSE_SUCCESS;
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
