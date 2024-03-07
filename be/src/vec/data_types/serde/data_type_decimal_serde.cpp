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
#include "vec/columns/column_decimal.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/io/io_helper.h"

namespace doris {

namespace vectorized {

template <typename T>
Status DataTypeDecimalSerDe<T>::serialize_column_to_json(const IColumn& column, int start_idx,
                                                         int end_idx, BufferWritable& bw,
                                                         FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

template <typename T>
Status DataTypeDecimalSerDe<T>::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                           BufferWritable& bw,
                                                           FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    auto& col = assert_cast<const ColumnDecimal<T>&>(*ptr);
    if constexpr (!IsDecimalV2<T>) {
        T value = col.get_element(row_num);
        auto decimal_str = value.to_string(scale);
        bw.write(decimal_str.data(), decimal_str.size());
    } else {
        auto length = col.get_element(row_num).to_string(buf, scale, scale_multiplier);
        bw.write(buf, length);
    }
    return Status::OK();
}

template <typename T>
Status DataTypeDecimalSerDe<T>::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, int* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

template <typename T>
Status DataTypeDecimalSerDe<T>::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                               const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnDecimal<T>&>(column).get_data();
    T val = {};
    ReadBuffer rb(slice.data, slice.size);
    StringParser::ParseResult res =
            read_decimal_text_impl<get_primitive_type(), T>(val, rb, precision, scale);
    if (res == StringParser::PARSE_SUCCESS || res == StringParser::PARSE_UNDERFLOW) {
        column_data.emplace_back(val);
        return Status::OK();
    }
    return Status::InvalidArgument("parse decimal fail, string: '{}', primitive type: '{}'",
                                   std::string(rb.position(), rb.count()).c_str(),
                                   get_primitive_type());
}

template <typename T>
void DataTypeDecimalSerDe<T>::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                    arrow::ArrayBuilder* array_builder, int start,
                                                    int end) const {
    auto& col = reinterpret_cast<const ColumnDecimal<T>&>(column);
    auto& builder = reinterpret_cast<arrow::Decimal128Builder&>(*array_builder);
    if constexpr (std::is_same_v<T, Decimal<Int128>>) {
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal128Type>(27, 9);
        for (size_t i = start; i < end; ++i) {
            if (null_map && (*null_map)[i]) {
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
        // TODO: decimal256
    } else if constexpr (std::is_same_v<T, Decimal128V3>) {
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal128Type>(38, col.get_scale());
        for (size_t i = start; i < end; ++i) {
            if (null_map && (*null_map)[i]) {
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
            if (null_map && (*null_map)[i]) {
                checkArrowStatus(builder.AppendNull(), column.get_name(),
                                 array_builder->type()->name());
                continue;
            }
            Int128 p_value = Int128(col.get_element(i));
            arrow::Decimal128 value(reinterpret_cast<const uint8_t*>(&p_value));
            checkArrowStatus(builder.Append(value), column.get_name(),
                             array_builder->type()->name());
        }
    } else if constexpr (std::is_same_v<T, Decimal<Int64>>) {
        std::shared_ptr<arrow::DataType> s_decimal_ptr =
                std::make_shared<arrow::Decimal128Type>(18, col.get_scale());
        for (size_t i = start; i < end; ++i) {
            if (null_map && (*null_map)[i]) {
                checkArrowStatus(builder.AppendNull(), column.get_name(),
                                 array_builder->type()->name());
                continue;
            }
            Int128 p_value = Int128(col.get_element(i));
            arrow::Decimal128 value(reinterpret_cast<const uint8_t*>(&p_value));
            checkArrowStatus(builder.Append(value), column.get_name(),
                             array_builder->type()->name());
        }
    } else {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "write_column_to_arrow with type " + column.get_name());
    }
}

template <typename T>
void DataTypeDecimalSerDe<T>::read_column_from_arrow(IColumn& column,
                                                     const arrow::Array* arrow_array, int start,
                                                     int end, const cctz::time_zone& ctz) const {
    auto concrete_array = dynamic_cast<const arrow::DecimalArray*>(arrow_array);
    const auto* arrow_decimal_type =
            static_cast<const arrow::DecimalType*>(arrow_array->type().get());
    const auto arrow_scale = arrow_decimal_type->scale();
    auto& column_data = static_cast<ColumnDecimal<T>&>(column).get_data();
    // Decimal<Int128> for decimalv2
    // Decimal<Int128I> for deicmalv3
    if constexpr (std::is_same_v<T, Decimal<Int128>>) {
        // TODO check precision
        for (size_t value_i = start; value_i < end; ++value_i) {
            auto value = *reinterpret_cast<const vectorized::Decimal128V2*>(
                    concrete_array->Value(value_i));
            // convert scale to 9;
            if (9 > arrow_scale) {
                using MaxNativeType = typename Decimal128V2::NativeType;
                MaxNativeType converted_value = common::exp10_i128(9 - arrow_scale);
                if (common::mul_overflow(static_cast<MaxNativeType>(value), converted_value,
                                         converted_value)) {
                    VLOG_DEBUG << "Decimal convert overflow";
                    value = converted_value < 0
                                    ? std::numeric_limits<typename Decimal128V2 ::NativeType>::min()
                                    : std::numeric_limits<
                                              typename Decimal128V2 ::NativeType>::max();
                } else {
                    value = converted_value;
                }
            } else if (9 < arrow_scale) {
                value = value / common::exp10_i128(arrow_scale - 9);
            }
            column_data.emplace_back(value);
        }
    } else if constexpr (std::is_same_v<T, Decimal128V3> || std::is_same_v<T, Decimal64> ||
                         std::is_same_v<T, Decimal32>) {
        for (size_t value_i = start; value_i < end; ++value_i) {
            column_data.emplace_back(*reinterpret_cast<const T*>(concrete_array->Value(value_i)));
        }
    } else {
        LOG(WARNING) << "Unsuppoted convertion to decimal from " << column.get_name();
    }
}

template <typename T>
template <bool is_binary_format>
Status DataTypeDecimalSerDe<T>::_write_column_to_mysql(const IColumn& column,
                                                       MysqlRowBuffer<is_binary_format>& result,
                                                       int row_idx, bool col_const) const {
    auto& data = assert_cast<const ColumnDecimal<T>&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    if constexpr (IsDecimalV2<T>) {
        DecimalV2Value decimal_val(data[col_index]);
        auto decimal_str = decimal_val.to_string(scale);
        if (UNLIKELY(0 != result.push_string(decimal_str.c_str(), decimal_str.size()))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    } else {
        auto length = data[col_index].to_string(buf, scale, scale_multiplier);
        if (UNLIKELY(0 != result.push_string(buf, length))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

template <typename T>
Status DataTypeDecimalSerDe<T>::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                      bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

template <typename T>
Status DataTypeDecimalSerDe<T>::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<false>& row_buffer,
                                                      int row_idx, bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

template <typename T>
Status DataTypeDecimalSerDe<T>::write_column_to_orc(const std::string& timezone,
                                                    const IColumn& column, const NullMap* null_map,
                                                    orc::ColumnVectorBatch* orc_col_batch,
                                                    int start, int end,
                                                    std::vector<StringRef>& buffer_list) const {
    auto& col_data = assert_cast<const ColumnDecimal<T>&>(column).get_data();

    if constexpr (IsDecimal128V2<T> || IsDecimal128V3<T>) {
        orc::Decimal128VectorBatch* cur_batch =
                dynamic_cast<orc::Decimal128VectorBatch*>(orc_col_batch);

        for (size_t row_id = start; row_id < end; row_id++) {
            if (cur_batch->notNull[row_id] == 1) {
                auto& v = col_data[row_id];
                orc::Int128 value(v >> 64, (uint64_t)v);
                cur_batch->values[row_id] = value;
            }
        }
        cur_batch->numElements = end - start;
    } else {
        orc::Decimal64VectorBatch* cur_batch =
                dynamic_cast<orc::Decimal64VectorBatch*>(orc_col_batch);

        for (size_t row_id = start; row_id < end; row_id++) {
            if (cur_batch->notNull[row_id] == 1) {
                cur_batch->values[row_id] = col_data[row_id];
            }
        }
        cur_batch->numElements = end - start;
    }
    return Status::OK();
}

template class DataTypeDecimalSerDe<Decimal32>;
template class DataTypeDecimalSerDe<Decimal64>;
template class DataTypeDecimalSerDe<Decimal128V2>;
template class DataTypeDecimalSerDe<Decimal128V3>;
template class DataTypeDecimalSerDe<Decimal256>;
} // namespace vectorized
} // namespace doris
