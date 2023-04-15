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
#include "vec/columns/column_decimal.h"

namespace doris {

namespace vectorized {

template <typename T>
class DataTypeDecimalSerDe : public DataTypeSerDe {
    static_assert(IsDecimalNumber<T>);

public:
    Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                              int end) const override;
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;
};

template <typename T>
Status DataTypeDecimalSerDe<T>::write_column_to_pb(const IColumn& column, PValues& result,
                                                   int start, int end) const {
    int row_count = end - start;
    const auto* col = check_and_get_column<ColumnDecimal<T>>(column);
    auto ptype = result.mutable_type();
    if constexpr (std::is_same_v<T, Decimal<Int128>>) {
        ptype->set_id(PGenericType::DECIMAL128);
    } else if constexpr (std::is_same_v<T, Decimal<Int128I>>) {
        ptype->set_id(PGenericType::DECIMAL128I);
    } else if constexpr (std::is_same_v<T, Decimal<Int32>>) {
        ptype->set_id(PGenericType::INT32);
    } else if constexpr (std::is_same_v<T, Decimal<Int64>>) {
        ptype->set_id(PGenericType::INT64);
    } else {
        return Status::NotSupported("unknown ColumnType for writing to pb");
    }
    result.mutable_bytes_value()->Reserve(row_count);
    for (size_t row_num = start; row_num < end; ++row_num) {
        StringRef single_data = col->get_data_at(row_num);
        result.add_bytes_value(single_data.data, single_data.size);
    }
    return Status::OK();
}

template <typename T>
Status DataTypeDecimalSerDe<T>::read_column_from_pb(IColumn& column, const PValues& arg) const {
    if constexpr (std::is_same_v<T, Decimal<Int128>> || std::is_same_v<T, Decimal<Int128I>> ||
                  std::is_same_v<T, Decimal<Int16>> || std::is_same_v<T, Decimal<Int32>>) {
        column.resize(arg.bytes_value_size());
        auto& data = reinterpret_cast<ColumnDecimal<T>&>(column).get_data();
        for (int i = 0; i < arg.bytes_value_size(); ++i) {
            data[i] = *(int128_t*)(arg.bytes_value(i).c_str());
        }
        return Status::OK();
    }

    return Status::NotSupported("unknown ColumnType for reading from pb");
}
} // namespace vectorized
} // namespace doris
