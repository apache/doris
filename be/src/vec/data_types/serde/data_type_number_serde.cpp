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
        TypeMap<UInt8, arrow::UInt8Builder, Int8, arrow::Int8Builder, UInt16, arrow::UInt16Builder,
                Int16, arrow::Int16Builder, UInt32, arrow::UInt32Builder, Int32,
                arrow::Int32Builder, UInt64, arrow::UInt64Builder, Int64, arrow::Int64Builder,
                UInt128, arrow::FixedSizeBinaryBuilder, Int128, arrow::FixedSizeBinaryBuilder,
                Float32, arrow::FloatBuilder, Float64, arrow::DoubleBuilder, void,
                void // 添加这一行来表示TypeMap的末端
                >;

template <typename T>
void DataTypeNumberSerDe<T>::write_column_to_arrow(const IColumn& column,
                                                   const PaddedPODArray<UInt8>* null_bytemap,
                                                   arrow::ArrayBuilder* array_builder, int start,
                                                   int end) const {
    auto& col_data = assert_cast<const ColumnType&>(column).get_data();
    using ARROW_BUILDER_TYPE = typename TypeMapLookup<T, DORIS_NUMERIC_ARROW_BUILDER>::ValueType;
    ////  DataTypeDateTimeV2 => T:UInt64
    ////  DataTypeTime => T:Float64
    ////  DataTypeDate => T:Int64
    ////  DataTypeDateTime => T:Int64
    if constexpr (std::is_same_v<T, Int64>) {
        if (column.get_data_type() == TypeIndex::Date ||
            column.get_data_type() == TypeIndex::DateTime) {
            auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
            for (size_t i = start; i < end; ++i) {
                char buf[64];
                const vectorized::VecDateTimeValue* time_val =
                        (const vectorized::VecDateTimeValue*)(col_data[i]);
                int len = time_val->to_buffer(buf);
                if (null_bytemap && (*null_bytemap)[i]) {
                    checkArrowStatus(string_builder.AppendNull(), column.get_name(),
                                     array_builder->type()->name());
                } else {
                    checkArrowStatus(string_builder.Append(buf, len), column.get_name(),
                                     array_builder->type()->name());
                }
            }
        } else {
            ARROW_BUILDER_TYPE& builder = assert_cast<ARROW_BUILDER_TYPE&>(*array_builder);
            // normal int64
            checkArrowStatus(builder.AppendValues(col_data.data() + start, end - start,
                                                  reinterpret_cast<const uint8_t*>(null_bytemap)),
                             column.get_name(), array_builder->type()->name());
        }
    } else if constexpr (std::is_same_v<T, UInt64>) {
        if (column.get_data_type() == TypeIndex::DateTimeV2) {
            auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
            for (size_t i = start; i < end; ++i) {
                char buf[64];
                const vectorized::DateV2Value<vectorized::DateTimeV2ValueType>* time_val =
                        (const vectorized::DateV2Value<
                                vectorized::DateTimeV2ValueType>*)(col_data[i]);
                int len = time_val->to_buffer(buf);
                if (null_bytemap && (*null_bytemap)[i]) {
                    checkArrowStatus(string_builder.AppendNull(), column.get_name(),
                                     array_builder->type()->name());
                } else {
                    checkArrowStatus(string_builder.Append(buf, len), column.get_name(),
                                     array_builder->type()->name());
                }
            }
        } else {
            ARROW_BUILDER_TYPE& builder = assert_cast<ARROW_BUILDER_TYPE&>(*array_builder);
            // normal Float64 or uint64
            checkArrowStatus(builder.AppendValues(col_data.data() + start, end - start,
                                                  reinterpret_cast<const uint8_t*>(null_bytemap)),
                             column.get_name(), array_builder->type()->name());
        }
    } else if constexpr (std::is_same_v<T, UInt8>) {
        ARROW_BUILDER_TYPE& builder = assert_cast<ARROW_BUILDER_TYPE&>(*array_builder);
        checkArrowStatus(
                builder.AppendValues(reinterpret_cast<const uint8_t*>(col_data.data() + start),
                                     end - start, reinterpret_cast<const uint8_t*>(null_bytemap)),
                column.get_name(), array_builder->type()->name());
    } else if constexpr (std::is_same_v<T, Int128> || std::is_same_v<T, UInt128>) {
        ARROW_BUILDER_TYPE& builder = assert_cast<ARROW_BUILDER_TYPE&>(*array_builder);
        size_t fixed_length = sizeof(typename ColumnType::value_type);
        const uint8_t* data_start =
                reinterpret_cast<const uint8_t*>(col_data.data()) + start * fixed_length;
        checkArrowStatus(builder.AppendValues(data_start, end - start,
                                              reinterpret_cast<const uint8_t*>(null_bytemap)),
                         column.get_name(), array_builder->type()->name());
    } else {
        ARROW_BUILDER_TYPE& builder = assert_cast<ARROW_BUILDER_TYPE&>(*array_builder);
        checkArrowStatus(builder.AppendValues(col_data.data() + start, end - start,
                                              reinterpret_cast<const uint8_t*>(null_bytemap)),
                         column.get_name(), array_builder->type()->name());
    }
}
static int64_t time_unit_divisor(arrow::TimeUnit::type unit) {
    // Doris only supports seconds
    switch (unit) {
    case arrow::TimeUnit::type::SECOND: {
        return 1L;
    }
    case arrow::TimeUnit::type::MILLI: {
        return 1000L;
    }
    case arrow::TimeUnit::type::MICRO: {
        return 1000000L;
    }
    case arrow::TimeUnit::type::NANO: {
        return 1000000000L;
    }
    default:
        return 0L;
    }
}

template <typename T>
void DataTypeNumberSerDe<T>::read_column_from_arrow(IColumn& column,
                                                    const arrow::Array* arrow_array, int start,
                                                    int end, const cctz::time_zone& ctz) const {
    int row_count = end - start;
    auto& col_data = static_cast<ColumnVector<T>&>(column).get_data();
    if constexpr (std::is_same_v<T, UInt8>) {
        if (arrow_array->type()->id() == arrow::Type::BOOL) {
            auto concrete_array = down_cast<const arrow::BooleanArray*>(arrow_array);
            for (size_t bool_i = start; bool_i < end; ++bool_i) {
                col_data.emplace_back(concrete_array->Value(bool_i));
            }
        } else {
            /// buffers[0] is a null bitmap and buffers[1] are actual values
            std::shared_ptr<arrow::Buffer> buffer = arrow_array->data()->buffers[1];
            const auto* raw_data = reinterpret_cast<const T*>(buffer->data()) + start;
            col_data.insert(raw_data, raw_data + row_count);
        }
    } else if constexpr (std::is_same_v<T, UInt32>) {
        if (arrow_array->type()->id() == arrow::Type::DATE32) {
            auto concrete_array = down_cast<const arrow::Date32Array*>(arrow_array);
            int64_t divisor = 1;
            int64_t multiplier = 1;
            multiplier = 24 * 60 * 60; // day => secs
            for (size_t value_i = start; value_i < end; ++value_i) {
                DateV2Value<DateV2ValueType> v;
                v.from_unixtime(
                        static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier,
                        ctz);
                col_data.emplace_back(binary_cast<DateV2Value<DateV2ValueType>, UInt32>(v));
            }
        } else {
            /// buffers[0] is a null bitmap and buffers[1] are actual values
            std::shared_ptr<arrow::Buffer> buffer = arrow_array->data()->buffers[1];
            const auto* raw_data = reinterpret_cast<const T*>(buffer->data()) + start;
            col_data.insert(raw_data, raw_data + row_count);
        }
    } else if constexpr (std::is_same_v<T, UInt64>) {
        if (arrow_array->type()->id() == arrow::Type::DATE64) {
            int64_t multiplier = 1;
            auto concrete_array = down_cast<const arrow::Date64Array*>(arrow_array);
            int64_t divisor = 1000; //ms => secs
            for (size_t value_i = start; value_i < end; ++value_i) {
                DateV2Value<DateTimeV2ValueType> v;
                v.from_unixtime(
                        static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier,
                        ctz);
                col_data.emplace_back(binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(v));
            }
        } else if (arrow_array->type()->id() == arrow::Type::TIMESTAMP) {
            int64_t multiplier = 1;
            auto concrete_array = down_cast<const arrow::TimestampArray*>(arrow_array);
            int64_t divisor = 1;
            const auto type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
            divisor = time_unit_divisor(type->unit());
            if (divisor == 0L) {
                LOG(FATAL) << "Invalid Time Type:" << type->name();
            }
            for (size_t value_i = start; value_i < end; ++value_i) {
                DateV2Value<DateTimeV2ValueType> v;
                v.from_unixtime(
                        static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier,
                        ctz);
                col_data.emplace_back(binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(v));
            }
        } else {
            /// buffers[0] is a null bitmap and buffers[1] are actual values
            std::shared_ptr<arrow::Buffer> buffer = arrow_array->data()->buffers[1];
            const auto* raw_data = reinterpret_cast<const T*>(buffer->data()) + start;
            col_data.insert(raw_data, raw_data + row_count);
        }
    } else if constexpr (std::is_same_v<T, Int64>) {
        int64_t divisor = 1;
        int64_t multiplier = 1;
        if (arrow_array->type()->id() == arrow::Type::DATE32) {
            auto concrete_array = down_cast<const arrow::Date32Array*>(arrow_array);
            multiplier = 24 * 60 * 60; // day => secs
            for (size_t value_i = start; value_i < end; ++value_i) {
                VecDateTimeValue v;
                v.from_unixtime(
                        static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier,
                        ctz);
                v.cast_to_date();
                col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
            }
        } else if (arrow_array->type()->id() == arrow::Type::DATE64) {
            auto concrete_array = down_cast<const arrow::Date64Array*>(arrow_array);
            divisor = 1000; //ms => secs
            for (size_t value_i = start; value_i < end; ++value_i) {
                VecDateTimeValue v;
                v.from_unixtime(
                        static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier,
                        ctz);
                col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
            }
        } else if (arrow_array->type()->id() == arrow::Type::TIMESTAMP) {
            auto concrete_array = down_cast<const arrow::TimestampArray*>(arrow_array);
            const auto type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
            divisor = time_unit_divisor(type->unit());
            if (divisor == 0L) {
                LOG(FATAL) << "Invalid Time Type:" << type->name();
            }
            for (size_t value_i = start; value_i < end; ++value_i) {
                VecDateTimeValue v;
                v.from_unixtime(
                        static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier,
                        ctz);
                col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
            }
        } else {
            /// buffers[0] is a null bitmap and buffers[1] are actual values
            std::shared_ptr<arrow::Buffer> buffer = arrow_array->data()->buffers[1];
            const auto* raw_data = reinterpret_cast<const T*>(buffer->data()) + start;
            col_data.insert(raw_data, raw_data + row_count);
        }
    } else {
        /// buffers[0] is a null bitmap and buffers[1] are actual values
        std::shared_ptr<arrow::Buffer> buffer = arrow_array->data()->buffers[1];
        const auto* raw_data = reinterpret_cast<const T*>(buffer->data()) + start;
        col_data.insert(raw_data, raw_data + row_count);
    }
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