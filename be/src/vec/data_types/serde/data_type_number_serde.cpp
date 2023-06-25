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

#include "data_type_number_serde.h"

#include <arrow/builder.h>

#include <type_traits>

#include "gutil/casts.h"

namespace doris {
namespace vectorized {

// Type map的基本结构
template <typename Key, typename Value, typename... Rest>
struct TypeMap {
    using KeyType = Key;
    using ValueType = Value;
    using Next = TypeMap<Rest...>;
};

// Type map的末端
template <>
struct TypeMap<void, void> {};

// TypeMapLookup 前向声明
template <typename Key, typename Map>
struct TypeMapLookup;

// Type map查找：找到匹配的键时的情况
template <typename Key, typename Value, typename... Rest>
struct TypeMapLookup<Key, TypeMap<Key, Value, Rest...>> {
    using ValueType = Value;
};

// Type map查找：递归查找
template <typename Key, typename K, typename V, typename... Rest>
struct TypeMapLookup<Key, TypeMap<K, V, Rest...>> {
    using ValueType = typename TypeMapLookup<Key, TypeMap<Rest...>>::ValueType;
};

using DORIS_NUMERIC_ARROW_BUILDER =
        TypeMap<UInt8, arrow::BooleanBuilder, Int8, arrow::Int8Builder, UInt16,
                arrow::UInt16Builder, Int16, arrow::Int16Builder, UInt32, arrow::UInt32Builder,
                Int32, arrow::Int32Builder, UInt64, arrow::UInt64Builder, Int64,
                arrow::Int64Builder, UInt128, arrow::FixedSizeBinaryBuilder, Int128,
                arrow::FixedSizeBinaryBuilder, Float32, arrow::FloatBuilder, Float64,
                arrow::DoubleBuilder, void,
                void // 添加这一行来表示TypeMap的末端
                >;

template <typename T>
void DataTypeNumberSerDe<T>::write_column_to_arrow(const IColumn& column, const UInt8* null_map,
                                                   arrow::ArrayBuilder* array_builder, int start,
                                                   int end) const {
    auto& col_data = assert_cast<const ColumnType&>(column).get_data();
    using ARROW_BUILDER_TYPE = typename TypeMapLookup<T, DORIS_NUMERIC_ARROW_BUILDER>::ValueType;
    if constexpr (std::is_same_v<T, UInt8>) {
        ARROW_BUILDER_TYPE& builder = assert_cast<ARROW_BUILDER_TYPE&>(*array_builder);
        checkArrowStatus(
                builder.AppendValues(reinterpret_cast<const uint8_t*>(col_data.data() + start),
                                     end - start, reinterpret_cast<const uint8_t*>(null_map)),
                column.get_name(), array_builder->type()->name());
    } else if constexpr (std::is_same_v<T, Int128> || std::is_same_v<T, UInt128>) {
        ARROW_BUILDER_TYPE& builder = assert_cast<ARROW_BUILDER_TYPE&>(*array_builder);
        size_t fixed_length = sizeof(typename ColumnType::value_type);
        const uint8_t* data_start =
                reinterpret_cast<const uint8_t*>(col_data.data()) + start * fixed_length;
        checkArrowStatus(builder.AppendValues(data_start, end - start,
                                              reinterpret_cast<const uint8_t*>(null_map)),
                         column.get_name(), array_builder->type()->name());
    } else {
        ARROW_BUILDER_TYPE& builder = assert_cast<ARROW_BUILDER_TYPE&>(*array_builder);
        checkArrowStatus(builder.AppendValues(col_data.data() + start, end - start,
                                              reinterpret_cast<const uint8_t*>(null_map)),
                         column.get_name(), array_builder->type()->name());
    }
}

template <typename T>
void DataTypeNumberSerDe<T>::read_column_from_arrow(IColumn& column,
                                                    const arrow::Array* arrow_array, int start,
                                                    int end, const cctz::time_zone& ctz) const {
    int row_count = end - start;
    auto& col_data = static_cast<ColumnVector<T>&>(column).get_data();

    // now uint8 for bool
    if constexpr (std::is_same_v<T, UInt8>) {
        auto concrete_array = down_cast<const arrow::BooleanArray*>(arrow_array);
        for (size_t bool_i = 0; bool_i != static_cast<size_t>(concrete_array->length()); ++bool_i) {
            col_data.emplace_back(concrete_array->Value(bool_i));
        }
        return;
    }
    /// buffers[0] is a null bitmap and buffers[1] are actual values
    std::shared_ptr<arrow::Buffer> buffer = arrow_array->data()->buffers[1];
    const auto* raw_data = reinterpret_cast<const T*>(buffer->data()) + start;
    col_data.insert(raw_data, raw_data + row_count);
}

template <typename T>
template <bool is_binary_format>
Status DataTypeNumberSerDe<T>::_write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<is_binary_format>& result,
                                                      int row_idx, bool col_const) const {
    int buf_ret = 0;
    auto& data = assert_cast<const ColumnType&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    if constexpr (std::is_same_v<T, Int8> || std::is_same_v<T, UInt8>) {
        buf_ret = result.push_tinyint(data[col_index]);
    } else if constexpr (std::is_same_v<T, Int16> || std::is_same_v<T, UInt16>) {
        buf_ret = result.push_smallint(data[col_index]);
    } else if constexpr (std::is_same_v<T, Int32> || std::is_same_v<T, UInt32>) {
        buf_ret = result.push_int(data[col_index]);
    } else if constexpr (std::is_same_v<T, Int64> || std::is_same_v<T, UInt64>) {
        buf_ret = result.push_bigint(data[col_index]);
    } else if constexpr (std::is_same_v<T, Int128>) {
        buf_ret = result.push_largeint(data[col_index]);
    } else if constexpr (std::is_same_v<T, float>) {
        buf_ret = result.push_float(data[col_index]);
    } else if constexpr (std::is_same_v<T, double>) {
        buf_ret = result.push_double(data[col_index]);
    }
    if (UNLIKELY(buf_ret != 0)) {
        return Status::InternalError("pack mysql buffer failed.");
    } else {
        return Status::OK();
    }
}

template <typename T>
Status DataTypeNumberSerDe<T>::write_column_to_mysql(const IColumn& column,
                                                     MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                     bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

template <typename T>
Status DataTypeNumberSerDe<T>::write_column_to_mysql(const IColumn& column,
                                                     MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                     bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class DataTypeNumberSerDe<UInt8>;
template class DataTypeNumberSerDe<UInt16>;
template class DataTypeNumberSerDe<UInt32>;
template class DataTypeNumberSerDe<UInt64>;
template class DataTypeNumberSerDe<UInt128>;
template class DataTypeNumberSerDe<Int8>;
template class DataTypeNumberSerDe<Int16>;
template class DataTypeNumberSerDe<Int32>;
template class DataTypeNumberSerDe<Int64>;
template class DataTypeNumberSerDe<Int128>;
template class DataTypeNumberSerDe<Float32>;
template class DataTypeNumberSerDe<Float64>;
} // namespace vectorized
} // namespace doris