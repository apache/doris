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

#include "format/parquet/byte_array_plain_decoder.h"

#include <algorithm>

#include "core/column/column.h"
#include "core/data_type/data_type_nullable.h"
#include "core/string_ref.h"

namespace doris {
namespace {
Status read_length(const Slice* data, uint32_t* offset, uint32_t* length) {
    if (UNLIKELY(*offset > data->size || data->size - *offset < sizeof(uint32_t))) {
        return Status::IOError("Can't read byte array length from plain decoder");
    }
    *length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(data->data) + *offset);
    *offset += sizeof(uint32_t);
    return Status::OK();
}
} // namespace

Status ByteArrayPlainDecoder::skip_values(size_t num_values) {
    for (int i = 0; i < num_values; ++i) {
        uint32_t length = 0;
        RETURN_IF_ERROR(read_length(_data, &_offset, &length));
        if (UNLIKELY(_offset > _data->size || length > _data->size - _offset)) {
            return Status::IOError("Can't skip enough bytes in plain decoder");
        }
        _offset += length;
    }
    return Status::OK();
}

Status ByteArrayPlainDecoder::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                            ColumnSelectVector& select_vector,
                                            bool is_dict_filter) {
    if (select_vector.has_filter()) {
        return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
    } else {
        return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
    }
}

template <bool has_filter>
Status ByteArrayPlainDecoder::_decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                             ColumnSelectVector& select_vector,
                                             bool is_dict_filter) {
    _selected_values.clear();
    _selected_values.reserve(select_vector.num_values() - select_vector.num_filtered());
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                uint32_t length = 0;
                RETURN_IF_ERROR(read_length(_data, &_offset, &length));
                if (UNLIKELY(_offset > _data->size || length > _data->size - _offset)) {
                    return Status::IOError("Can't read enough bytes in plain decoder");
                }
                _selected_values.emplace_back(_data->data + _offset, length);
                _offset += length;
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            _selected_values.insert(_selected_values.end(), run_length, StringRef("", 0));
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            RETURN_IF_ERROR(skip_values(run_length));
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            // do nothing
            break;
        }
        }
    }
    DCHECK_EQ(_selected_values.size(), select_vector.num_values() - select_vector.num_filtered());
    if (!_selected_values.empty()) {
        // ColumnString calculates the aggregate byte length before copying, so this call grows the
        // chars buffer and offsets exactly once for the whole selected batch.
        doris_column->insert_many_strings(_selected_values.data(), _selected_values.size());
    }
    return Status::OK();
}

} // namespace doris
