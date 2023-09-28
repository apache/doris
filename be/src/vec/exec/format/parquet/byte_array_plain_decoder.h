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

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "util/bit_util.h"
#include "util/coding.h"
#include "util/slice.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/parquet/decoder.h"
#include "vec/exec/format/parquet/parquet_common.h"

namespace doris {
namespace vectorized {
template <typename T>
class ColumnDecimal;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class ByteArrayPlainDecoder final : public Decoder {
public:
    ByteArrayPlainDecoder() = default;
    ~ByteArrayPlainDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override;

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter);

    Status skip_values(size_t num_values) override;

protected:
    template <typename DecimalPrimitiveType, bool has_filter>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                  ColumnSelectVector& select_vector);

private:
    template <typename DecimalPrimitiveType, bool has_filter,
              DecimalScaleParams::ScaleType ScaleType>
    Status _decode_binary_decimal_internal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                           ColumnSelectVector& select_vector);
};

template <typename DecimalPrimitiveType, bool has_filter>
Status ByteArrayPlainDecoder::_decode_binary_decimal(MutableColumnPtr& doris_column,
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
Status ByteArrayPlainDecoder::_decode_binary_decimal_internal(MutableColumnPtr& doris_column,
                                                              DataTypePtr& data_type,
                                                              ColumnSelectVector& select_vector) {
    auto& column_data =
            static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                if (UNLIKELY(_offset + 4 > _data->size)) {
                    return Status::IOError("Can't read byte array length from plain decoder");
                }
                uint32_t length =
                        decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data->data) + _offset);
                _offset += 4;
                char* buf_start = _data->data + _offset;
                _offset += length;
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
            _offset += _type_length * run_length;
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
