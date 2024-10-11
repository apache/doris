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

namespace doris::vectorized {
void ByteStreamSplitDecoder::set_data(Slice* slice) {
    Decoder::set_data(slice);
    DCHECK(_data->get_size() % _type_length == 0);
    auto byteSplitStreamNum = _data->get_size() / _type_length;
    _byteSplitStreams = std::vector<char*>(_type_length);
    for (size_t i = 0; i < _type_length; ++i) {
        _byteSplitStreams[i] = _data->data + i * byteSplitStreamNum;
    }
}

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
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }

    size_t primitive_length = remove_nullable(data_type)->get_size_of_value_in_memory();
    size_t data_index = doris_column->size() * primitive_length;
    size_t scale_size = (select_vector.num_values() - select_vector.num_filtered()) *
                        (_type_length / primitive_length);
    doris_column->resize(doris_column->size() + scale_size);
    char* raw_data = const_cast<char*>(doris_column->get_raw_data().data);
    ColumnSelectVector::DataReadType read_type;

    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (int i = 0; i < run_length; ++i) {
                // read byte split value from _byteSplitStreams
                for (size_t j = 0; j < _type_length; ++j) {
                    raw_data[data_index + i * _type_length + j] =
                            _byteSplitStreams[j][_offset / _type_length + i];
                }
            }
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
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    return Status::OK();
}
}; // namespace doris::vectorized