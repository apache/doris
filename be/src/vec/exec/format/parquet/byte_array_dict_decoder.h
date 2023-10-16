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

#include <string.h>

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "util/bit_util.h"
#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/parquet/decoder.h"
#include "vec/exec/format/parquet/parquet_common.h"

namespace doris {
namespace vectorized {
class ColumnString;
template <typename T>
class ColumnDecimal;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class ByteArrayDictDecoder final : public BaseDictDecoder {
public:
    ByteArrayDictDecoder() = default;
    ~ByteArrayDictDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override;

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter);

    Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) override;

    Status read_dict_values_to_column(MutableColumnPtr& doris_column) override;

    Status get_dict_codes(const ColumnString* column_string,
                          std::vector<int32_t>* dict_codes) override;

    MutableColumnPtr convert_dict_column_to_string_column(const ColumnInt32* dict_column) override;

protected:
    template <typename DecimalPrimitiveType, bool has_filter>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                  ColumnSelectVector& select_vector);

    // For dictionary encoding
    std::vector<StringRef> _dict_items;
    std::vector<uint8_t> _dict_data;
    size_t _max_value_length;
    std::unordered_map<StringRef, int32_t> _dict_value_to_code;

private:
    template <typename DecimalPrimitiveType, bool has_filter,
              DecimalScaleParams::ScaleType ScaleType>
    Status _decode_binary_decimal_internal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                           ColumnSelectVector& select_vector);
};

template <typename DecimalPrimitiveType, bool has_filter>
Status ByteArrayDictDecoder::_decode_binary_decimal(MutableColumnPtr& doris_column,
                                                    DataTypePtr& data_type,
                                                    ColumnSelectVector& select_vector) {
    init_decimal_converter<DecimalPrimitiveType>(data_type);
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;
    if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
        return _decode_binary_decimal_internal<DecimalPrimitiveType, has_filter,
                                               DecimalScaleParams::SCALE_UP>(
                doris_column, data_type, select_vector);
    } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
        return _decode_binary_decimal_internal<DecimalPrimitiveType, has_filter,
                                               DecimalScaleParams::SCALE_DOWN>(
                doris_column, data_type, select_vector);
    } else {
        return _decode_binary_decimal_internal<DecimalPrimitiveType, has_filter,
                                               DecimalScaleParams::NO_SCALE>(
                doris_column, data_type, select_vector);
    }
}

template <typename DecimalPrimitiveType, bool has_filter, DecimalScaleParams::ScaleType ScaleType>
Status ByteArrayDictDecoder::_decode_binary_decimal_internal(MutableColumnPtr& doris_column,
                                                             DataTypePtr& data_type,
                                                             ColumnSelectVector& select_vector) {
    auto& column_data =
            static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    size_t dict_index = 0;
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                StringRef& slice = _dict_items[_indexes[dict_index++]];
                char* buf_start = const_cast<char*>(slice.data);
                uint32_t length = (uint32_t)slice.size;
                // When Decimal in parquet is stored in byte arrays, binary and fixed,
                // the unscaled number must be encoded as two's complement using big-endian byte order.
                DecimalPrimitiveType value = 0;
                memcpy(reinterpret_cast<char*>(&value), buf_start, length);
                value = BitUtil::big_endian_to_host(value);
                value = value >> ((sizeof(value) - length) * 8);
                if constexpr (ScaleType == DecimalScaleParams::SCALE_UP) {
                    value *= scale_params.scale_factor;
                } else if constexpr (ScaleType == DecimalScaleParams::SCALE_DOWN) {
                    value /= scale_params.scale_factor;
                } else if constexpr (ScaleType == DecimalScaleParams::NO_SCALE) {
                    // do nothing
                } else {
                    LOG(FATAL) << "__builtin_unreachable";
                    __builtin_unreachable();
                }
                auto& v = reinterpret_cast<DecimalPrimitiveType&>(column_data[data_index++]);
                v = (DecimalPrimitiveType)value;
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            dict_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            // do nothing
            break;
        }
        }
    }
    return Status::OK();
}
} // namespace doris::vectorized
