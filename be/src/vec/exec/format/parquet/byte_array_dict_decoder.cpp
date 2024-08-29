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

#include "vec/exec/format/parquet/byte_array_dict_decoder.h"

#include <utility>

#include "util/coding.h"
#include "util/rle_encoding.h"
#include "vec/columns/column.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

Status ByteArrayDictDecoder::set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length,
                                      size_t num_values) {
    _dict = std::move(dict);
    if (_dict == nullptr) {
        return Status::Corruption("Wrong dictionary data for byte array type, dict is null.");
    }
    _dict_items.reserve(num_values);
    uint32_t offset_cursor = 0;
    char* dict_item_address = reinterpret_cast<char*>(_dict.get());

    size_t total_length = 0;
    for (int i = 0; i < num_values; ++i) {
        uint32_t l = decode_fixed32_le(_dict.get() + offset_cursor);
        offset_cursor += 4;
        offset_cursor += l;
        total_length += l;
    }

    // For insert_many_strings_overflow
    _dict_data.resize(total_length + ColumnString::MAX_STRINGS_OVERFLOW_SIZE);
    _max_value_length = 0;
    size_t offset = 0;
    offset_cursor = 0;
    for (int i = 0; i < num_values; ++i) {
        uint32_t l = decode_fixed32_le(_dict.get() + offset_cursor);
        offset_cursor += 4;
        memcpy(&_dict_data[offset], dict_item_address + offset_cursor, l);
        _dict_items.emplace_back(&_dict_data[offset], l);
        offset_cursor += l;
        offset += l;
        if (offset_cursor > length) {
            return Status::Corruption("Wrong data length in dictionary");
        }
        if (l > _max_value_length) {
            _max_value_length = l;
        }
    }
    if (offset_cursor != length) {
        return Status::Corruption("Wrong dictionary data for byte array type");
    }
    return Status::OK();
}

Status ByteArrayDictDecoder::read_dict_values_to_column(MutableColumnPtr& doris_column) {
    doris_column->insert_many_strings_overflow(_dict_items.data(), _dict_items.size(),
                                               _max_value_length);
    return Status::OK();
}

MutableColumnPtr ByteArrayDictDecoder::convert_dict_column_to_string_column(
        const ColumnInt32* dict_column) {
    auto res = ColumnString::create();
    std::vector<StringRef> dict_values(dict_column->size());
    const auto& data = dict_column->get_data();
    for (size_t i = 0; i < dict_column->size(); ++i) {
        dict_values[i] = _dict_items[data[i]];
    }
    res->insert_many_strings_overflow(dict_values.data(), dict_values.size(), _max_value_length);
    return res;
}

Status ByteArrayDictDecoder::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                           ColumnSelectVector& select_vector, bool is_dict_filter) {
    if (select_vector.has_filter()) {
        return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
    } else {
        return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
    }
}

template <bool has_filter>
Status ByteArrayDictDecoder::_decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                            ColumnSelectVector& select_vector,
                                            bool is_dict_filter) {
    size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
    if (doris_column->is_column_dictionary()) {
        ColumnDictI32& dict_column = assert_cast<ColumnDictI32&>(*doris_column);
        if (dict_column.dict_size() == 0) {
            dict_column.insert_many_dict_data(_dict_items.data(), _dict_items.size());
        }
    }
    _indexes.resize(non_null_size);
    _index_batch_decoder->GetBatch(_indexes.data(), non_null_size);

    if (doris_column->is_column_dictionary() || is_dict_filter) {
        return _decode_dict_values<has_filter>(doris_column, select_vector, is_dict_filter);
    }

    size_t dict_index = 0;

    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            std::vector<StringRef> string_values;
            string_values.reserve(run_length);
            for (size_t i = 0; i < run_length; ++i) {
                string_values.emplace_back(_dict_items[_indexes[dict_index++]]);
            }
            doris_column->insert_many_strings_overflow(string_values.data(), run_length,
                                                       _max_value_length);
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            doris_column->insert_many_defaults(run_length);
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
