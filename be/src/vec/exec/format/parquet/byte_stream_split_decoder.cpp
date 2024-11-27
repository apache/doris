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

#include "byte_stream_split_decoder.h"

#include <cstdint>

#include "util/byte_stream_split.h"

namespace doris::vectorized {

Status ByteStreamSplitDecoder::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                             ColumnSelectVector& select_vector,
                                             bool is_dict_filter) {
    if (select_vector.has_filter()) {
        return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
    } else {
        return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
    }
}

template <bool has_filter>
Status ByteStreamSplitDecoder::_decode_values(MutableColumnPtr& doris_column,
                                              DataTypePtr& data_type,
                                              ColumnSelectVector& select_vector,
                                              bool is_dict_filter) {
    size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
    if (UNLIKELY(_offset + non_null_size > _data->size)) {
        return Status::IOError(
                "Out-of-bounds access in parquet data decoder: offset = {}, non_null_size = "
                "{},size = {}",
                _offset, non_null_size, _data->size);
    }

    size_t primitive_length = remove_nullable(data_type)->get_size_of_value_in_memory();
    size_t data_index = doris_column->size() * primitive_length;
    size_t scale_size = (select_vector.num_values() - select_vector.num_filtered()) *
                        (_type_length / primitive_length);
    doris_column->resize(doris_column->size() + scale_size);
    char* raw_data = const_cast<char*>(doris_column->get_raw_data().data);
    ColumnSelectVector::DataReadType read_type;
    DCHECK(_data->get_size() % _type_length == 0);
    int64_t stride = _data->get_size() / _type_length;

    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            byte_stream_split_decode(reinterpret_cast<const uint8_t*>(_data->get_data()),
                                     _type_length, _offset / _type_length, run_length, stride,
                                     reinterpret_cast<uint8_t*>(raw_data) + data_index);
            _offset += run_length * _type_length;
            data_index += run_length * _type_length;
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length * _type_length;
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

Status ByteStreamSplitDecoder::skip_values(size_t num_values) {
    _offset += _type_length * num_values;
    if (UNLIKELY(_offset > _data->size)) {
        return Status::IOError(
                "Out-of-bounds access in parquet data decoder: offset = {}, size = {}", _offset,
                _data->size);
    }
    return Status::OK();
}
}; // namespace doris::vectorized
