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

#include "data_type_serde.h"
#include "vec/columns/column_vector.h"

namespace doris {

namespace vectorized {

template <typename T>
class DataTypeNumberSerDe : public DataTypeSerDe {
    static_assert(IsNumber<T>);

public:
    using ColumnType = ColumnVector<T>;
    Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                              int end) const override;
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;
};

template <typename T>
Status DataTypeNumberSerDe<T>::read_column_from_pb(IColumn& column, const PValues& arg) const {
    if constexpr (std::is_same_v<T, UInt8> || std::is_same_v<T, UInt16> ||
                  std::is_same_v<T, UInt32>) {
        column.resize(arg.uint32_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.uint32_value_size(); ++i) {
            data[i] = arg.uint32_value(i);
        }
    } else if constexpr (std::is_same_v<T, Int8> || std::is_same_v<T, Int16> ||
                         std::is_same_v<T, Int32>) {
        column.resize(arg.int32_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.int32_value_size(); ++i) {
            data[i] = arg.int32_value(i);
        }
    } else if constexpr (std::is_same_v<T, UInt64>) {
        column.resize(arg.uint64_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.uint64_value_size(); ++i) {
            data[i] = arg.uint64_value(i);
        }
    } else if constexpr (std::is_same_v<T, Int64>) {
        column.resize(arg.int64_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.int64_value_size(); ++i) {
            data[i] = arg.int64_value(i);
        }
    } else if constexpr (std::is_same_v<T, float>) {
        column.resize(arg.float_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.float_value_size(); ++i) {
            data[i] = arg.float_value(i);
        }
    } else if constexpr (std::is_same_v<T, double>) {
        column.resize(arg.double_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.float_value_size(); ++i) {
            data[i] = arg.double_value(i);
        }
    } else if constexpr (std::is_same_v<T, Int128>) {
        column.resize(arg.bytes_value_size());
        auto& data = reinterpret_cast<ColumnType&>(column).get_data();
        for (int i = 0; i < arg.bytes_value_size(); ++i) {
            data[i] = *(int128_t*)(arg.bytes_value(i).c_str());
        }
    } else {
        return Status::NotSupported("unknown ColumnType for reading from pb");
    }
    return Status::OK();
}

template <typename T>
Status DataTypeNumberSerDe<T>::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                                  int end) const {
    int row_count = end - start;
    auto ptype = result.mutable_type();
    const auto* col = check_and_get_column<ColumnVector<T>>(column);
    if constexpr (std::is_same_v<T, Int128>) {
        ptype->set_id(PGenericType::INT128);
        result.mutable_bytes_value()->Reserve(row_count);
        for (size_t row_num = start; row_num < end; ++row_num) {
            StringRef single_data = col->get_data_at(row_num);
            result.add_bytes_value(single_data.data, single_data.size);
        }
        return Status::OK();
    }
    auto& data = col->get_data();
    if constexpr (std::is_same_v<T, UInt8>) {
        ptype->set_id(PGenericType::UINT8);
        auto* values = result.mutable_uint32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, UInt16>) {
        ptype->set_id(PGenericType::UINT16);
        auto* values = result.mutable_uint32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, UInt32>) {
        ptype->set_id(PGenericType::UINT32);
        auto* values = result.mutable_uint32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, UInt64>) {
        ptype->set_id(PGenericType::UINT64);
        auto* values = result.mutable_uint64_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, Int8>) {
        ptype->set_id(PGenericType::INT8);
        auto* values = result.mutable_int32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, Int16>) {
        ptype->set_id(PGenericType::INT16);
        auto* values = result.mutable_int32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, Int32>) {
        ptype->set_id(PGenericType::INT32);
        auto* values = result.mutable_int32_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, Int64>) {
        ptype->set_id(PGenericType::INT64);
        auto* values = result.mutable_int64_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, float>) {
        ptype->set_id(PGenericType::FLOAT);
        auto* values = result.mutable_float_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else if constexpr (std::is_same_v<T, double>) {
        ptype->set_id(PGenericType::DOUBLE);
        auto* values = result.mutable_double_value();
        values->Reserve(row_count);
        values->Add(data.begin() + start, data.begin() + end);
    } else {
        return Status::NotSupported("unknown ColumnType for writing to pb");
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace doris