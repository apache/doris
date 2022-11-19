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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeNumberBase.cpp
// and modified by Doris

#include "vec/data_types/data_type_number_base.h"

#include <type_traits>

#include "gutil/strings/numbers.h"
#include "util/mysql_global.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <typename T>
void DataTypeNumberBase<T>::to_string(const IColumn& column, size_t row_num,
                                      BufferWritable& ostr) const {
    if constexpr (std::is_same<T, UInt128>::value) {
        std::string hex = int128_to_string(
                assert_cast<const ColumnVector<T>&>(*column.convert_to_full_column_if_const().get())
                        .get_data()[row_num]);
        ostr.write(hex.data(), hex.size());
    } else if constexpr (std::is_same_v<T, float>) {
        // fmt::format_to maybe get inaccurate results at float type, so we use gutil implement.
        char buf[MAX_FLOAT_STR_LENGTH + 2];

        int len = FloatToBuffer(
                assert_cast<const ColumnVector<T>&>(*column.convert_to_full_column_if_const().get())
                        .get_data()[row_num],
                MAX_FLOAT_STR_LENGTH + 2, buf);

        ostr.write(buf, len);
    } else if constexpr (std::is_integral<T>::value || std::numeric_limits<T>::is_iec559) {
        ostr.write_number(
                assert_cast<const ColumnVector<T>&>(*column.convert_to_full_column_if_const().get())
                        .get_data()[row_num]);
    }
}

template <typename T>
Status DataTypeNumberBase<T>::from_string(ReadBuffer& rb, IColumn* column) const {
    auto* column_data = static_cast<ColumnVector<T>*>(column);
    if constexpr (std::is_same<T, UInt128>::value) {
        // TODO support for Uint128
        return Status::InvalidArgument("uint128 is not support");
    } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
        T val = 0;
        if (!read_float_text_fast_impl(val, rb)) {
            return Status::InvalidArgument("parse number fail, string: '{}'",
                                           std::string(rb.position(), rb.count()).c_str());
        }
        column_data->insert_value(val);
    } else if constexpr (std::is_same_v<T, uint8_t>) {
        // Note: here we should handle the bool type
        T val = 0;
        if (!try_read_bool_text(val, rb)) {
            return Status::InvalidArgument("parse boolean fail, string: '{}'",
                                           std::string(rb.position(), rb.count()).c_str());
        }
        column_data->insert_value(val);
    } else if constexpr (std::is_integral<T>::value) {
        T val = 0;
        if (!read_int_text_impl(val, rb)) {
            return Status::InvalidArgument("parse number fail, string: '{}'",
                                           std::string(rb.position(), rb.count()).c_str());
        }
        column_data->insert_value(val);
    } else {
        DCHECK(false);
    }
    return Status::OK();
}

template <typename T>
Field DataTypeNumberBase<T>::get_default() const {
    return NearestFieldType<FieldType>();
}

template <typename T>
std::string DataTypeNumberBase<T>::to_string(const IColumn& column, size_t row_num) const {
    if constexpr (std::is_same<T, __int128_t>::value || std::is_same<T, UInt128>::value) {
        return int128_to_string(
                assert_cast<const ColumnVector<T>&>(*column.convert_to_full_column_if_const().get())
                        .get_data()[row_num]);
    } else if constexpr (std::is_integral<T>::value) {
        return std::to_string(
                assert_cast<const ColumnVector<T>&>(*column.convert_to_full_column_if_const().get())
                        .get_data()[row_num]);
    } else if constexpr (std::numeric_limits<T>::is_iec559) {
        fmt::memory_buffer buffer;
        fmt::format_to(
                buffer, "{}",
                assert_cast<const ColumnVector<T>&>(*column.convert_to_full_column_if_const().get())
                        .get_data()[row_num]);
        return std::string(buffer.data(), buffer.size());
    }
}

// binary: row num | value1 | value2 | ...
template <typename T>
int64_t DataTypeNumberBase<T>::get_uncompressed_serialized_bytes(const IColumn& column,
                                                                 int be_exec_version) const {
    return sizeof(uint32_t) + column.size() * sizeof(FieldType);
}

template <typename T>
char* DataTypeNumberBase<T>::serialize(const IColumn& column, char* buf,
                                       int be_exec_version) const {
    // row num
    const auto row_num = column.size();
    *reinterpret_cast<uint32_t*>(buf) = row_num;
    buf += sizeof(uint32_t);
    // column data
    auto ptr = column.convert_to_full_column_if_const();
    const auto* origin_data = assert_cast<const ColumnVector<T>&>(*ptr.get()).get_data().data();
    memcpy(buf, origin_data, row_num * sizeof(FieldType));
    buf += row_num * sizeof(FieldType);

    return buf;
}

template <typename T>
const char* DataTypeNumberBase<T>::deserialize(const char* buf, IColumn* column,
                                               int be_exec_version) const {
    // row num
    uint32_t row_num = *reinterpret_cast<const uint32_t*>(buf);
    buf += sizeof(uint32_t);
    // column data
    auto& container = assert_cast<ColumnVector<T>*>(column)->get_data();
    container.resize(row_num);
    memcpy(container.data(), buf, row_num * sizeof(FieldType));
    buf += row_num * sizeof(FieldType);

    return buf;
}

template <typename T>
MutableColumnPtr DataTypeNumberBase<T>::create_column() const {
    return ColumnVector<T>::create();
}

template <typename T>
bool DataTypeNumberBase<T>::is_value_represented_by_integer() const {
    return std::is_integral_v<T>;
}

template <typename T>
bool DataTypeNumberBase<T>::is_value_represented_by_unsigned_integer() const {
    return std::is_integral_v<T> && std::is_unsigned_v<T>;
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class DataTypeNumberBase<UInt8>;
template class DataTypeNumberBase<UInt16>;
template class DataTypeNumberBase<UInt32>;
template class DataTypeNumberBase<UInt64>;
template class DataTypeNumberBase<UInt128>;
template class DataTypeNumberBase<Int8>;
template class DataTypeNumberBase<Int16>;
template class DataTypeNumberBase<Int32>;
template class DataTypeNumberBase<Int64>;
template class DataTypeNumberBase<Int128>;
template class DataTypeNumberBase<Float32>;
template class DataTypeNumberBase<Float64>;

} // namespace doris::vectorized
