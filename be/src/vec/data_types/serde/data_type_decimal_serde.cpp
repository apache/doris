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

#include "data_type_decimal_serde.h"

#include <arrow/array/array_base.h>
#include <arrow/array/array_decimal.h>
#include <arrow/builder.h>
#include <arrow/util/decimal.h>

#include "arrow/type.h"
#include "gutil/casts.h"
#include "vec/columns/column_decimal.h"
#include "vec/common/arithmetic_overflow.h"

namespace doris {

namespace vectorized {

template <typename T>
void DataTypeDecimalSerDe<T>::write_column_to_arrow(const IColumn& column, const UInt8* null_map,
                                                    arrow::ArrayBuilder* array_builder, int start,
                                                    int end) const {
    auto& col = reinterpret_cast<const ColumnDecimal<T>&>(column);
    auto& builder = reinterpret_cast<arrow::Decimal128Builder&>(*array_builder);
    if constexpr (std::is_same_v<T, Decimal<Int128>>) {
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal128Type>(27, 9);
        for (size_t i = start; i < end; ++i) {
            if (null_map && null_map[i]) {
                checkArrowStatus(builder.AppendNull(), column.get_name(),
                                 array_builder->type()->name());
                continue;
            }
            const auto& data_ref = col.get_data_at(i);
            const PackedInt128* p_value = reinterpret_cast<const PackedInt128*>(data_ref.data);
            int64_t high = (p_value->value) >> 64;
            uint64 low = p_value->value;
            arrow::Decimal128 value(high, low);
            checkArrowStatus(builder.Append(value), column.get_name(),
                             array_builder->type()->name());
        }
    } else if constexpr (std::is_same_v<T, Decimal<Int128I>>) {
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal128Type>(38, col.get_scale());
        for (size_t i = start; i < end; ++i) {
            if (null_map && null_map[i]) {
                checkArrowStatus(builder.AppendNull(), column.get_name(),
                                 array_builder->type()->name());
                continue;
            }
            const auto& data_ref = col.get_data_at(i);
            const PackedInt128* p_value = reinterpret_cast<const PackedInt128*>(data_ref.data);
            int64_t high = (p_value->value) >> 64;
            uint64 low = p_value->value;
            arrow::Decimal128 value(high, low);
            checkArrowStatus(builder.Append(value), column.get_name(),
                             array_builder->type()->name());
        }
    } else if constexpr (std::is_same_v<T, Decimal<Int32>>) {
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal128Type>(8, col.get_scale());
        for (size_t i = start; i < end; ++i) {
            if (null_map && null_map[i]) {
                checkArrowStatus(builder.AppendNull(), column.get_name(),
                                 array_builder->type()->name());
                continue;
            }
            const auto& data_ref = col.get_data_at(i);
            const int32_t* p_value = reinterpret_cast<const int32_t*>(data_ref.data);
            int64_t high = *p_value > 0 ? 0 : 1UL << 63;
            arrow::Decimal128 value(high, *p_value > 0 ? *p_value : -*p_value);
            checkArrowStatus(builder.Append(value), column.get_name(),
                             array_builder->type()->name());
        }
    } else if constexpr (std::is_same_v<T, Decimal<Int64>>) {
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal128Type>(18, col.get_scale());
        for (size_t i = start; i < end; ++i) {
            if (null_map && null_map[i]) {
                checkArrowStatus(builder.AppendNull(), column.get_name(),
                                 array_builder->type()->name());
                continue;
            }
            const auto& data_ref = col.get_data_at(i);
            const int64_t* p_value = reinterpret_cast<const int64_t*>(data_ref.data);
            int64_t high = *p_value > 0 ? 0 : 1UL << 63;
            arrow::Decimal128 value(high, *p_value > 0 ? *p_value : -*p_value);
            checkArrowStatus(builder.Append(value), column.get_name(),
                             array_builder->type()->name());
        }
    } else {
        LOG(FATAL) << "Not support write " << column.get_name() << " to arrow";
    }
}

template <typename T>
void DataTypeDecimalSerDe<T>::read_column_from_arrow(IColumn& column,
                                                     const arrow::Array* arrow_array, int start,
                                                     int end, const cctz::time_zone& ctz) const {
    if constexpr (std::is_same_v<T, Decimal<Int128>>) {
        auto& column_data = static_cast<ColumnDecimal<vectorized::Decimal128>&>(column).get_data();
        auto concrete_array = down_cast<const arrow::DecimalArray*>(arrow_array);
        const auto* arrow_decimal_type =
                static_cast<const arrow::DecimalType*>(arrow_array->type().get());
        // TODO check precision
        const auto arrow_scale = arrow_decimal_type->scale();
        for (size_t value_i = start; value_i < end; ++value_i) {
            auto value = *reinterpret_cast<const vectorized::Decimal128*>(
                    concrete_array->Value(value_i));
            // convert scale to 9;
            if (9 > arrow_scale) {
                using MaxNativeType = typename Decimal128::NativeType;
                MaxNativeType converted_value = common::exp10_i128(9 - arrow_scale);
                if (common::mul_overflow(static_cast<MaxNativeType>(value), converted_value,
                                         converted_value)) {
                    VLOG_DEBUG << "Decimal convert overflow";
                    value = converted_value < 0
                                    ? std::numeric_limits<typename Decimal128 ::NativeType>::min()
                                    : std::numeric_limits<typename Decimal128 ::NativeType>::max();
                } else {
                    value = converted_value;
                }
            } else if (9 < arrow_scale) {
                value = value / common::exp10_i128(arrow_scale - 9);
            }
            column_data.emplace_back(value);
        }
    } else {
        LOG(FATAL) << "Not support read " << column.get_name() << " from arrow";
    }
}

template class DataTypeDecimalSerDe<Decimal32>;
template class DataTypeDecimalSerDe<Decimal64>;
template class DataTypeDecimalSerDe<Decimal128>;
template class DataTypeDecimalSerDe<Decimal128I>;
} // namespace vectorized
} // namespace doris