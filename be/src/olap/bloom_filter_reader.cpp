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

#include "olap/bloom_filter_reader.h"

namespace doris {

BloomFilterIndexReader::~BloomFilterIndexReader() {
    _entry.reset();

    if (!_is_using_cache) {
        SAFE_DELETE_ARRAY(_buffer);
    }
}

Status BloomFilterIndexReader::init(char* buffer, size_t buffer_size, bool is_using_cache,
                                    uint32_t hash_function_num, uint32_t bit_num) {
    Status res = Status::OK();

    _buffer = buffer;
    _buffer_size = buffer_size;
    _is_using_cache = is_using_cache;

    BloomFilterIndexHeader* header = reinterpret_cast<BloomFilterIndexHeader*>(_buffer);
    _step_size = bit_num >> 3;
    _entry_count = header->block_count;
    _hash_function_num = hash_function_num;
    _start_offset = sizeof(BloomFilterIndexHeader);
    if (_step_size * _entry_count + _start_offset > _buffer_size) {
        OLAP_LOG_WARNING(
                "invalid param found. "
                "[buffer_size=%lu bit_num=%u block_count=%lu header_size=%lu]",
                buffer_size, bit_num, _entry_count, _start_offset);
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }

    return res;
}

const BloomFilter& BloomFilterIndexReader::entry(uint64_t entry_id) {
    _entry.init((uint64_t*)(_buffer + _start_offset + _step_size * entry_id),
                _step_size / sizeof(uint64_t), _hash_function_num);
    return _entry;
}

size_t BloomFilterIndexReader::entry_count() {
    return _entry_count;
}

} // namespace doris
