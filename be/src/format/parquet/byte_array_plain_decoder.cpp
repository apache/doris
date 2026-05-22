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
#include <vector>

#include "core/column/column.h"
#include "core/data_type/data_type_nullable.h"
#include "core/string_ref.h"

namespace doris {
#include "common/compile_check_begin.h"

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
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            std::vector<StringRef> string_values;
            string_values.reserve(run_length);
            for (size_t i = 0; i < run_length; ++i) {
                uint32_t length = 0;
                RETURN_IF_ERROR(read_length(_data, &_offset, &length));
                if (UNLIKELY(_offset > _data->size || length > _data->size - _offset)) {
                    return Status::IOError("Can't read enough bytes in plain decoder");
                }
                string_values.emplace_back(_data->data + _offset, length);
                _offset += length;
            }
            doris_column->insert_many_strings(&string_values[0], run_length);
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            doris_column->insert_many_defaults(run_length);
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            for (int i = 0; i < run_length; ++i) {
                uint32_t length = 0;
                RETURN_IF_ERROR(read_length(_data, &_offset, &length));
                if (UNLIKELY(_offset > _data->size || length > _data->size - _offset)) {
                    return Status::IOError("Can't read enough bytes in plain decoder");
                }
                _offset += length;
            }
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
#include "common/compile_check_end.h"

} // namespace doris
