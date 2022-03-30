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
    if (auto* ptype = typeid_cast<const DataTypeDecimal<T>*>(&rhs))
        return scale == ptype->get_scale();
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
    DecimalV2Value value = (DecimalV2Value)assert_cast<const ColumnType&>(
                                   *column.convert_to_full_column_if_const().get())
                                   .get_data()[row_num];
    auto str = value.to_string();
    ostr.write(str.data(), str.size());
}

// binary: row_num | value1 | value2 | ...
template <typename T>
int64_t DataTypeDecimal<T>::get_uncompressed_serialized_bytes(const IColumn& column) const {
    return sizeof(uint32_t) + column.size() * sizeof(FieldType);
}

template <typename T>
char* DataTypeDecimal<T>::serialize(const IColumn& column, char* buf) const {
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
const char* DataTypeDecimal<T>::deserialize(const char* buf, IColumn* column) const {
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
    using PromotedType = DataTypeDecimal<Decimal128>;
    return std::make_shared<PromotedType>(PromotedType::max_precision(), scale);
}

template <typename T>
MutableColumnPtr DataTypeDecimal<T>::create_column() const {
    return ColumnType::create(0, scale);
}

DataTypePtr create_decimal(UInt64 precision_value, UInt64 scale_value) {
    if (precision_value < min_decimal_precision() ||
        precision_value > max_decimal_precision<Decimal128>()) {
        LOG(FATAL) << "Wrong precision";
    }

    if (static_cast<UInt64>(scale_value) > precision_value) {
        LOG(FATAL) << "Negative scales and scales larger than precision are not supported";
    }

    if (precision_value <= max_decimal_precision<Decimal32>())
        return std::make_shared<DataTypeDecimal<Decimal32>>(precision_value, scale_value);
    else if (precision_value <= max_decimal_precision<Decimal64>())
        return std::make_shared<DataTypeDecimal<Decimal64>>(precision_value, scale_value);
    return std::make_shared<DataTypeDecimal<Decimal128>>(precision_value, scale_value);
}

template <>
Decimal32 DataTypeDecimal<Decimal32>::get_scale_multiplier(UInt32 scale_) {
    return common::exp10_i32(scale_);
}

template <>
Decimal64 DataTypeDecimal<Decimal64>::get_scale_multiplier(UInt32 scale_) {
    return common::exp10_i64(scale_);
}

template <>
Decimal128 DataTypeDecimal<Decimal128>::get_scale_multiplier(UInt32 scale_) {
    return common::exp10_i128(scale_);
}

/// Explicit template instantiations.
template class DataTypeDecimal<Decimal32>;
template class DataTypeDecimal<Decimal64>;
template class DataTypeDecimal<Decimal128>;

} // namespace doris::vectorized
