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

#pragma once

#include <cstdint>
#include <orc/Vector.hh>
#include <type_traits>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "exec/common/int_exp.h"
#include "format/format_common.h"

namespace doris::orc_serde {

static constexpr char EMPTY_STRING_FOR_OVERFLOW[ColumnString::MAX_STRINGS_OVERFLOW_SIZE] = "";

inline size_t trim_right(const char* s, size_t size) {
    while (size > 0 && s[size - 1] == ' ') {
        size--;
    }
    return size;
}

template <PrimitiveType PType, typename OrcColumnType>
Status read_flat_column(IColumn& column, const orc::ColumnVectorBatch* orc_col_batch, int64_t start,
                        int64_t end, const UInt8* filter) {
    const auto* data = dynamic_cast<const OrcColumnType*>(orc_col_batch);
    if (data == nullptr) {
        return Status::InternalError("Wrong ORC data type for column {}, actual {}",
                                     column.get_name(), orc_col_batch->toString());
    }

    auto& column_data = assert_cast<ColumnVector<PType>&>(column).get_data();
    const auto origin_size = column_data.size();
    const auto rows = end - start;
    column_data.resize(origin_size + rows);
    const auto* cvb_data = data->data.data();
    for (int64_t row_id = start; row_id < end; ++row_id) {
        const auto result_index = origin_size + row_id - start;
        if (data->hasNulls && !data->notNull[row_id]) {
            continue;
        }
        column_data[result_index] =
                static_cast<typename PrimitiveTypeTraits<PType>::CppType>(cvb_data[row_id]);
    }
    return Status::OK();
}

inline Status read_int32_column(IColumn& column, const orc::ColumnVectorBatch* orc_col_batch,
                                int64_t start, int64_t end, const UInt8* filter) {
    if (dynamic_cast<const orc::LongVectorBatch*>(orc_col_batch) != nullptr) {
        return read_flat_column<TYPE_INT, orc::LongVectorBatch>(column, orc_col_batch, start, end,
                                                                filter);
    }

    const auto* data = dynamic_cast<const orc::EncodedStringVectorBatch*>(orc_col_batch);
    if (data == nullptr) {
        return Status::InternalError("Wrong ORC data type for int column {}, actual {}",
                                     column.get_name(), orc_col_batch->toString());
    }

    auto& column_data = assert_cast<ColumnInt32&>(column).get_data();
    const auto origin_size = column_data.size();
    const auto rows = end - start;
    column_data.resize(origin_size + rows);
    const auto* cvb_data = data->index.data();
    for (int64_t row_id = start; row_id < end; ++row_id) {
        const auto result_index = origin_size + row_id - start;
        if (data->hasNulls && !data->notNull[row_id]) {
            continue;
        }
        column_data[result_index] = static_cast<Int32>(cvb_data[row_id]);
    }
    return Status::OK();
}

template <PrimitiveType DecimalPrimitiveType, typename OrcColumnType>
Status read_explicit_decimal_column(IColumn& column, const orc::ColumnVectorBatch* orc_col_batch,
                                    int64_t start, int64_t end, UInt32 dest_scale,
                                    const UInt8* filter) {
    using DecimalType = typename PrimitiveTypeTraits<DecimalPrimitiveType>::CppType;
    using NativeType = typename DecimalType::NativeType;
    const auto* data = dynamic_cast<const OrcColumnType*>(orc_col_batch);
    if (data == nullptr) {
        return Status::InternalError("Wrong ORC data type for decimal column {}, actual {}",
                                     column.get_name(), orc_col_batch->toString());
    }

    DecimalScaleParams::ScaleType scale_type;
    NativeType scale_factor = 1;
    if (dest_scale > data->scale) {
        scale_type = DecimalScaleParams::SCALE_UP;
        scale_factor = DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(dest_scale -
                                                                                  data->scale);
    } else if (dest_scale < data->scale) {
        scale_type = DecimalScaleParams::SCALE_DOWN;
        scale_factor = DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(data->scale -
                                                                                  dest_scale);
    } else {
        scale_type = DecimalScaleParams::NO_SCALE;
    }

    const auto* cvb_data = data->values.data();
    auto& column_data = assert_cast<ColumnDecimal<DecimalPrimitiveType>&>(column).get_data();
    const auto origin_size = column_data.size();
    const auto rows = end - start;
    column_data.resize(origin_size + rows);

    for (int64_t row_id = start; row_id < end; ++row_id) {
        const auto result_index = origin_size + row_id - start;
        if (data->hasNulls && !data->notNull[row_id]) {
            continue;
        }

        NativeType value;
        if constexpr (std::is_same_v<OrcColumnType, orc::Decimal64VectorBatch>) {
            value = static_cast<NativeType>(cvb_data[row_id]);
        } else {
            auto* non_const_data = const_cast<OrcColumnType*>(data);
            uint64_t hi = non_const_data->values[row_id].getHighBits();
            uint64_t lo = non_const_data->values[row_id].getLowBits();
            int128_t int128_value = (static_cast<int128_t>(hi) << 64) | static_cast<int128_t>(lo);
            value = static_cast<NativeType>(int128_value);
        }

        if (scale_type == DecimalScaleParams::SCALE_UP) {
            value *= scale_factor;
        } else if (scale_type == DecimalScaleParams::SCALE_DOWN) {
            value /= scale_factor;
        }
        auto& result = column_data[result_index];
        if constexpr (DecimalPrimitiveType == TYPE_DECIMALV2) {
            result.set_value(value);
        } else {
            result.value = value;
        }
    }
    return Status::OK();
}

template <PrimitiveType DecimalPrimitiveType>
Status read_decimal_column(IColumn& column, const orc::ColumnVectorBatch* orc_col_batch,
                           int64_t start, int64_t end, UInt32 dest_scale, const UInt8* filter) {
    if (dynamic_cast<const orc::Decimal64VectorBatch*>(orc_col_batch) != nullptr) {
        return read_explicit_decimal_column<DecimalPrimitiveType, orc::Decimal64VectorBatch>(
                column, orc_col_batch, start, end, dest_scale, filter);
    }
    return read_explicit_decimal_column<DecimalPrimitiveType, orc::Decimal128VectorBatch>(
            column, orc_col_batch, start, end, dest_scale, filter);
}

inline Status read_datev2_column(IColumn& column, const orc::ColumnVectorBatch* orc_col_batch,
                                 int64_t start, int64_t end, const UInt8* filter) {
    const auto* data = dynamic_cast<const orc::LongVectorBatch*>(orc_col_batch);
    if (data == nullptr) {
        return Status::InternalError("Wrong ORC data type for datev2 column {}, actual {}",
                                     column.get_name(), orc_col_batch->toString());
    }

    date_day_offset_dict& date_dict = date_day_offset_dict::get();
    auto& column_data = assert_cast<ColumnDateV2&>(column).get_data();
    const auto origin_size = column_data.size();
    const auto rows = end - start;
    column_data.resize(origin_size + rows);
    for (int64_t row_id = start; row_id < end; ++row_id) {
        const auto result_index = origin_size + row_id - start;
        const auto filter_index = row_id - start;
        if (filter != nullptr && !filter[filter_index]) {
            continue;
        }
        if (data->hasNulls && !data->notNull[row_id]) {
            continue;
        }
        column_data[result_index] = date_dict[cast_set<int32_t>(data->data[row_id])];
    }
    return Status::OK();
}

inline Status read_string_column(IColumn& column, const orc::ColumnVectorBatch* orc_col_batch,
                                 int64_t start, int64_t end, bool is_char, const UInt8* filter) {
    const auto* data = dynamic_cast<const orc::EncodedStringVectorBatch*>(orc_col_batch);
    if (data == nullptr) {
        return Status::InternalError("Wrong ORC data type for string column {}, actual {}",
                                     column.get_name(), orc_col_batch->toString());
    }

    std::vector<StringRef> string_values;
    string_values.reserve(end - start);
    size_t max_value_length = 0;

    if (data->isEncoded) {
        for (int64_t row_id = start; row_id < end; ++row_id) {
            const auto filter_index = row_id - start;
            if (filter != nullptr && !filter[filter_index]) {
                string_values.emplace_back(EMPTY_STRING_FOR_OVERFLOW, 0);
                continue;
            }
            if (data->hasNulls && !data->notNull[row_id]) {
                string_values.emplace_back(EMPTY_STRING_FOR_OVERFLOW, 0);
                continue;
            }

            char* val_ptr;
            int64_t length;
            data->dictionary->getValueByIndex(data->index.data()[row_id], val_ptr, length);
            if (is_char) {
                length = trim_right(val_ptr, length);
            }
            if (length > max_value_length) {
                max_value_length = length;
            }
            string_values.emplace_back(length > 0 ? val_ptr : EMPTY_STRING_FOR_OVERFLOW, length);
        }

        if (!string_values.empty()) {
            column.insert_many_strings_overflow(string_values.data(), string_values.size(),
                                                max_value_length);
        }
        return Status::OK();
    }

    const static std::string empty_string;
    for (int64_t row_id = start; row_id < end; ++row_id) {
        if (data->hasNulls && !data->notNull[row_id]) {
            string_values.emplace_back(empty_string.data(), 0);
            continue;
        }

        size_t length = data->length[row_id];
        if (is_char) {
            length = trim_right(data->data[row_id], length);
        }
        string_values.emplace_back(length > 0 ? data->data[row_id] : empty_string.data(), length);
    }

    if (!string_values.empty()) {
        column.insert_many_strings(string_values.data(), string_values.size());
    }
    return Status::OK();
}

} // namespace doris::orc_serde
