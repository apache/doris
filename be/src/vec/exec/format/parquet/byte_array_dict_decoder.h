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

#include "gutil/endian.h"
#include "util/rle_encoding.h"
#include "vec/exec/format/parquet/decoder.h"

namespace doris::vectorized {

class ByteArrayDictDecoder final : public BaseDictDecoder {
public:
    ByteArrayDictDecoder() = default;
    ~ByteArrayDictDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector) override;

    Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) override;

protected:
    template <typename DecimalPrimitiveType>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                  ColumnSelectVector& select_vector);

    // For dictionary encoding
    std::vector<StringRef> _dict_items;
    std::vector<uint8_t> _dict_data;
    size_t _max_value_length;
};

template <typename DecimalPrimitiveType>
Status ByteArrayDictDecoder::_decode_binary_decimal(MutableColumnPtr& doris_column,
                                                    DataTypePtr& data_type,
                                                    ColumnSelectVector& select_vector) {
    init_decimal_converter<DecimalPrimitiveType>(data_type);
    auto& column_data =
            static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    size_t dict_index = 0;
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                StringRef& slice = _dict_items[_indexes[dict_index++]];
                char* buf_start = const_cast<char*>(slice.data);
                uint32_t length = (uint32_t)slice.size;
                // When Decimal in parquet is stored in byte arrays, binary and fixed,
                // the unscaled number must be encoded as two's complement using big-endian byte order.
                Int128 value = buf_start[0] & 0x80 ? -1 : 0;
                memcpy(reinterpret_cast<char*>(&value) + sizeof(Int128) - length, buf_start,
                       length);
                value = BigEndian::ToHost128(value);
                if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
                    value *= scale_params.scale_factor;
                } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
                    value /= scale_params.scale_factor;
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
