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

#include "vec/data_types/data_type_number_base.h"

#include <type_traits>

#include "gen_cpp/data.pb.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/nan_utils.h"
#include "vec/common/typeid_cast.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <typename T>
void DataTypeNumberBase<T>::to_string(const IColumn& column, size_t row_num,
                                      BufferWritable& ostr) const {
    if constexpr (std::is_same<T, __int128_t>::value || std::is_same<T, UInt128>::value) {
        std::string hex = int128_to_string(
                assert_cast<const ColumnVector<T>&>(*column.convert_to_full_column_if_const().get())
                        .get_data()[row_num]);
        ostr.write(hex.data(), hex.size());
    } else if constexpr (std::is_integral<T>::value || std::numeric_limits<T>::is_iec559) {
        ostr.write_number(
                assert_cast<const ColumnVector<T>&>(*column.convert_to_full_column_if_const().get())
                        .get_data()[row_num]);
    }
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
    } else if constexpr (std::is_integral<T>::value || std::numeric_limits<T>::is_iec559) {
        return std::to_string(
                assert_cast<const ColumnVector<T>&>(*column.convert_to_full_column_if_const().get())
                        .get_data()[row_num]);
    }
}

template <typename T>
size_t DataTypeNumberBase<T>::serialize(const IColumn& column, PColumn* pcolumn) const {
    const auto column_len = column.size();
    pcolumn->mutable_binary()->resize(column_len * sizeof(FieldType));
    auto* data = pcolumn->mutable_binary()->data();

    // copy the data
    auto ptr = column.convert_to_full_column_if_const();
    const auto* origin_data =
            assert_cast<const ColumnVector<T>&>(*ptr.get()).get_data().data();
    memcpy(data, origin_data, column_len * sizeof(FieldType));

    return compress_binary(pcolumn);
}

template <typename T>
void DataTypeNumberBase<T>::deserialize(const PColumn& pcolumn, IColumn* column) const {
    std::string uncompressed;
    read_binary(pcolumn, &uncompressed);

    // read column_size
    auto& container = assert_cast<ColumnVector<T>*>(column)->get_data();
    container.resize(uncompressed.size() / sizeof(T));
    memcpy(container.data(), uncompressed.data(), uncompressed.size());
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
