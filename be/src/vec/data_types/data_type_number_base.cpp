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

#include <fmt/format.h>
#include <glog/logging.h>
#include <streamvbyte.h>

#include <cstring>
#include <limits>
#include <type_traits>

#include "agent/be_exec_version_manager.h"
#include "gutil/strings/numbers.h"
#include "runtime/large_int_value.h"
#include "util/mysql_global.h"
#include "util/string_parser.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/io/io_helper.h"
#include "vec/io/reader_buffer.h"

namespace doris::vectorized {

template <typename T>
void DataTypeNumberBase<T>::to_string(const IColumn& column, size_t row_num,
                                      BufferWritable& ostr) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    if constexpr (std::is_same<T, UInt128>::value) {
        std::string hex = int128_to_string(
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*ptr).get_element(
                        row_num));
        ostr.write(hex.data(), hex.size());
    } else if constexpr (std::is_same_v<T, float>) {
        // fmt::format_to maybe get inaccurate results at float type, so we use gutil implement.
        char buf[MAX_FLOAT_STR_LENGTH + 2];
        int len = FloatToBuffer(
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*ptr).get_element(
                        row_num),
                MAX_FLOAT_STR_LENGTH + 2, buf);
        ostr.write(buf, len);
    } else if constexpr (std::is_integral<T>::value || std::numeric_limits<T>::is_iec559) {
        ostr.write_number(
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*ptr).get_element(
                        row_num));
    }
}

template <typename T>
std::string DataTypeNumberBase<T>::to_string(const T& value) const {
    if constexpr (std::is_same<T, int128_t>::value || std::is_same<T, uint128_t>::value ||
                  std::is_same<T, UInt128>::value) {
        return int128_to_string(value);
    } else if constexpr (std::is_integral<T>::value) {
        return std::to_string(value);
    } else if constexpr (std::numeric_limits<T>::is_iec559) {
        fmt::memory_buffer buffer; // only use in size-predictable type.
        fmt::format_to(buffer, "{}", value);
        return std::string(buffer.data(), buffer.size());
    }
}
template <typename T>
Status DataTypeNumberBase<T>::from_string(ReadBuffer& rb, IColumn* column) const {
    auto* column_data = static_cast<ColumnVector<T>*>(column);
    if constexpr (std::is_same<T, UInt128>::value) {
        // TODO: support for Uint128
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
Field DataTypeNumberBase<T>::get_field(const TExprNode& node) const {
    if constexpr (std::is_same_v<TypeId<T>, TypeId<UInt8>>) {
        return UInt8(node.bool_literal.value);
    }
    if constexpr (std::is_same_v<TypeId<T>, TypeId<Int8>>) {
        return Int8(node.int_literal.value);
    }
    if constexpr (std::is_same_v<TypeId<T>, TypeId<Int16>>) {
        return Int16(node.int_literal.value);
    }
    if constexpr (std::is_same_v<TypeId<T>, TypeId<Int32>>) {
        return Int32(node.int_literal.value);
    }
    if constexpr (std::is_same_v<TypeId<T>, TypeId<Int64>>) {
        return Int64(node.int_literal.value);
    }
    if constexpr (std::is_same_v<TypeId<T>, TypeId<Int128>>) {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto value = StringParser::string_to_int<__int128>(node.large_int_literal.value.c_str(),
                                                           node.large_int_literal.value.size(),
                                                           &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            value = MAX_INT128;
        }
        return Int128(value);
    }
    if constexpr (std::is_same_v<TypeId<T>, TypeId<Float32>>) {
        return Float32(node.float_literal.value);
    }
    if constexpr (std::is_same_v<TypeId<T>, TypeId<Float64>>) {
        return Float64(node.float_literal.value);
    }
    LOG(FATAL) << "__builtin_unreachable";
    __builtin_unreachable();
}

template <typename T>
std::string DataTypeNumberBase<T>::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    if constexpr (std::is_same<T, int128_t>::value || std::is_same<T, uint128_t>::value ||
                  std::is_same<T, UInt128>::value) {
        return int128_to_string(
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*ptr).get_element(
                        row_num));
    } else if constexpr (std::is_integral<T>::value) {
        return std::to_string(
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*ptr).get_element(
                        row_num));
    } else if constexpr (std::numeric_limits<T>::is_iec559) {
        fmt::memory_buffer buffer; // only use in size-predictable type.
        fmt::format_to(
                buffer, "{}",
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*ptr).get_element(
                        row_num));
        return std::string(buffer.data(), buffer.size());
    }
}

// binary: row num | value1 | value2 | ...
template <typename T>
int64_t DataTypeNumberBase<T>::get_uncompressed_serialized_bytes(const IColumn& column,
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
char* DataTypeNumberBase<T>::serialize(const IColumn& column, char* buf,
                                       int be_exec_version) const {
    if (be_exec_version >= USE_NEW_SERDE) {
        // row num
        const auto mem_size = column.size() * sizeof(T);
        *reinterpret_cast<uint32_t*>(buf) = mem_size;
        buf += sizeof(uint32_t);
        // column data
        auto ptr = column.convert_to_full_column_if_const();
        const auto* origin_data = assert_cast<const ColumnVector<T>&>(*ptr.get()).get_data().data();
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
        // column data
        auto ptr = column.convert_to_full_column_if_const();
        const auto* origin_data = assert_cast<const ColumnVector<T>&>(*ptr.get()).get_data().data();
        memcpy(buf, origin_data, row_num * sizeof(FieldType));
        buf += row_num * sizeof(FieldType);

        return buf;
    }
}

template <typename T>
const char* DataTypeNumberBase<T>::deserialize(const char* buf, IColumn* column,
                                               int be_exec_version) const {
    if (be_exec_version >= USE_NEW_SERDE) {
        // row num
        uint32_t mem_size = *reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        // column data
        auto& container = assert_cast<ColumnVector<T>*>(column)->get_data();
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
        // column data
        auto& container = assert_cast<ColumnVector<T>*>(column)->get_data();
        container.resize(row_num);
        memcpy(container.data(), buf, row_num * sizeof(FieldType));
        buf += row_num * sizeof(FieldType);
        return buf;
    }
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
template class DataTypeNumberBase<UInt32>; // IPv4
template class DataTypeNumberBase<UInt64>;
template class DataTypeNumberBase<UInt128>;
template class DataTypeNumberBase<Int8>;
template class DataTypeNumberBase<Int16>;
template class DataTypeNumberBase<Int32>;
template class DataTypeNumberBase<Int64>;
template class DataTypeNumberBase<Int128>;
template class DataTypeNumberBase<Float32>;
template class DataTypeNumberBase<Float64>;
template class DataTypeNumberBase<IPv6>; // IPv6

} // namespace doris::vectorized
