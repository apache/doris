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

#include "gen_cpp/data.pb.h"
#include "vec/common/assert_cast.h"
#include "vec/common/int_exp.h"
#include "vec/common/typeid_cast.h"
#include "vec/io/io_helper.h"

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
        return scale == ptype->get_scale();
    }
    return false;
}

template <typename T>
std::string DataTypeDecimal<T>::to_string(const IColumn& column, size_t row_num) const {
    T value = assert_cast<const ColumnType&>(*column.convert_to_full_column_if_const().get())
                      .get_data()[row_num];
    std::ostringstream buf;
    write_text(value, scale, buf);
    return buf.str();
}

template <typename T>
void DataTypeDecimal<T>::to_string(const IColumn& column, size_t row_num,
                                   BufferWritable& ostr) const {
    // TODO: Reduce the copy in std::string mem to ostr, like DataTypeNumber
    if constexpr (!IsDecimalV2<T>) {
        T value = assert_cast<const ColumnType&>(*column.convert_to_full_column_if_const().get())
                          .get_data()[row_num];
        std::ostringstream buf;
        write_text(value, scale, buf);
        std::string str = buf.str();
        ostr.write(str.data(), str.size());
    } else {
        DecimalV2Value value = (DecimalV2Value)assert_cast<const ColumnType&>(
                                       *column.convert_to_full_column_if_const().get())
                                       .get_data()[row_num];
        auto str = value.to_string();
        ostr.write(str.data(), str.size());
    }
}

template <typename T>
Status DataTypeDecimal<T>::from_string(ReadBuffer& rb, IColumn* column) const {
    auto& column_data = static_cast<ColumnType&>(*column).get_data();
    T val = 0;
    if (!read_decimal_text_impl<T>(val, rb, precision, scale)) {
        return Status::InvalidArgument("parse decimal fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.emplace_back(val);
    return Status::OK();
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
    return DecimalField(T(0), scale);
}

template <typename T>
DataTypePtr DataTypeDecimal<T>::promote_numeric_type() const {
    using PromotedType = std::conditional_t<IsDecimalV2<T>, DataTypeDecimal<Decimal128>,
                                            DataTypeDecimal<Decimal128I>>;
    return std::make_shared<PromotedType>(PromotedType::max_precision(), scale);
}

template <typename T>
MutableColumnPtr DataTypeDecimal<T>::create_column() const {
    if constexpr (IsDecimalV2<T>) {
        auto col = ColumnDecimal128::create(0, scale);
        col->set_decimalv2_type();
        return col;
    } else {
        return ColumnType::create(0, scale);
    }
}

template <typename T>
bool DataTypeDecimal<T>::parse_from_string(const std::string& str, T* res) const {
    StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
    *res = StringParser::string_to_decimal<__int128>(str.c_str(), str.size(), precision, scale,
                                                     &result);
    return result == StringParser::PARSE_SUCCESS;
}

DataTypePtr create_decimal(UInt64 precision_value, UInt64 scale_value, bool use_v2) {
    if (precision_value < min_decimal_precision() ||
        precision_value > max_decimal_precision<Decimal128>()) {
        LOG(WARNING) << "Wrong precision " << precision_value;
        return nullptr;
    }

    if (static_cast<UInt64>(scale_value) > precision_value) {
        LOG(WARNING) << "Negative scales and scales larger than precision are not supported";
        return nullptr;
    }

    if (use_v2) {
        return std::make_shared<DataTypeDecimal<Decimal128>>(precision_value, scale_value);
    }

    if (precision_value <= max_decimal_precision<Decimal32>()) {
        return std::make_shared<DataTypeDecimal<Decimal32>>(precision_value, scale_value);
    } else if (precision_value <= max_decimal_precision<Decimal64>()) {
        return std::make_shared<DataTypeDecimal<Decimal64>>(precision_value, scale_value);
    }
    return std::make_shared<DataTypeDecimal<Decimal128I>>(precision_value, scale_value);
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
Decimal128 DataTypeDecimal<Decimal128>::get_scale_multiplier(UInt32 scale) {
    return common::exp10_i128(scale);
}

template <>
Decimal128I DataTypeDecimal<Decimal128I>::get_scale_multiplier(UInt32 scale) {
    return common::exp10_i128(scale);
}

template <typename T>
typename T::NativeType max_decimal_value(UInt32 precision) {
    return 0;
}
template <>
Int32 max_decimal_value<Decimal32>(UInt32 precision) {
    return 999999999 / DataTypeDecimal<Decimal32>::get_scale_multiplier(
                               (UInt32)(max_decimal_precision<Decimal32>() - precision));
}
template <>
Int64 max_decimal_value<Decimal64>(UInt32 precision) {
    return 999999999999999999 / DataTypeDecimal<Decimal64>::get_scale_multiplier(
                                        (UInt64)max_decimal_precision<Decimal64>() - precision);
}
template <>
Int128 max_decimal_value<Decimal128>(UInt32 precision) {
    return (static_cast<int128_t>(999999999999999999ll) * 100000000000000000ll * 1000ll +
            static_cast<int128_t>(99999999999999999ll) * 1000ll + 999ll) /
           DataTypeDecimal<Decimal128>::get_scale_multiplier(
                   (UInt64)max_decimal_precision<Decimal128>() - precision);
}

template <typename T>
typename T::NativeType min_decimal_value(UInt32 precision) {
    return 0;
}
template <>
Int32 min_decimal_value<Decimal32>(UInt32 precision) {
    return -999999999 / DataTypeDecimal<Decimal32>::get_scale_multiplier(
                                (UInt32)max_decimal_precision<Decimal32>() - precision);
}
template <>
Int64 min_decimal_value<Decimal64>(UInt32 precision) {
    return -999999999999999999 / DataTypeDecimal<Decimal64>::get_scale_multiplier(
                                         (UInt64)max_decimal_precision<Decimal64>() - precision);
}
template <>
Int128 min_decimal_value<Decimal128>(UInt32 precision) {
    return -(static_cast<int128_t>(999999999999999999ll) * 100000000000000000ll * 1000ll +
             static_cast<int128_t>(99999999999999999ll) * 1000ll + 999ll) /
           DataTypeDecimal<Decimal128>::get_scale_multiplier(
                   (UInt64)max_decimal_precision<Decimal128>() - precision);
}
/// Explicit template instantiations.
template class DataTypeDecimal<Decimal32>;
template class DataTypeDecimal<Decimal64>;
template class DataTypeDecimal<Decimal128>;
template class DataTypeDecimal<Decimal128I>;

} // namespace doris::vectorized
