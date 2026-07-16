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

#include "format_v2/parquet/reader/native/byte_array_dict_decoder.h"

#include "core/custom_allocator.h"
#include "util/coding.h"

namespace doris::format::parquet::native {
Status ByteArrayDictDecoder::set_dict(DorisUniqueBufferPtr<uint8_t>& dict, int32_t length,
                                      size_t num_values) {
    _dict_items.clear();
    _dict = std::move(dict);
    if (_dict == nullptr) {
        return Status::Corruption("Wrong dictionary data for byte array type, dict is null.");
    }
    _dict_items.reserve(num_values);
    if (UNLIKELY(length < 0)) {
        return Status::Corruption("Wrong data length in dictionary");
    }
    const size_t dict_length = cast_set<size_t>(length);
    size_t offset_cursor = 0;
    char* dict_item_address = reinterpret_cast<char*>(_dict.get());
    for (int i = 0; i < num_values; ++i) {
        if (UNLIKELY(offset_cursor > dict_length ||
                     dict_length - offset_cursor < sizeof(uint32_t))) {
            return Status::Corruption("Wrong data length in dictionary");
        }
        uint32_t l = decode_fixed32_le(_dict.get() + offset_cursor);
        offset_cursor += sizeof(uint32_t);
        if (UNLIKELY(l > dict_length - offset_cursor)) {
            return Status::Corruption("Wrong data length in dictionary");
        }
        _dict_items.emplace_back(dict_item_address + offset_cursor, l);
        offset_cursor += l;
    }
    if (offset_cursor != dict_length) {
        return Status::Corruption("Wrong dictionary data for byte array type");
    }
    ++_dictionary_generation;
    return Status::OK();
}

Status ByteArrayDictDecoder::decode_dictionary(ParquetFixedValueConsumer& fixed_consumer,
                                               ParquetBinaryValueConsumer& binary_consumer) {
    return binary_consumer.consume(_dict_items.data(), _dict_items.size());
}

} // namespace doris::format::parquet::native
