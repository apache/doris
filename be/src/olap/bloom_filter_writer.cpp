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

#include "olap/bloom_filter_writer.h"

#include <vector>

namespace doris {

BloomFilterIndexWriter::~BloomFilterIndexWriter() {
    for (std::vector<BloomFilter*>::iterator it = _bloom_filters.begin();
         it != _bloom_filters.end(); ++it) {
        SAFE_DELETE(*it);
    }
}

OLAPStatus BloomFilterIndexWriter::add_bloom_filter(BloomFilter* bf) {
    try {
        _bloom_filters.push_back(bf);
    } catch (...) {
        OLAP_LOG_WARNING("add bloom filter to vector fail");
        return OLAP_ERR_STL_ERROR;
    }

    return OLAP_SUCCESS;
}

uint64_t BloomFilterIndexWriter::estimate_buffered_memory() {
    uint64_t buffered_size = sizeof(_header);
    if (_bloom_filters.size() > 0) {
        buffered_size +=
                _bloom_filters.size() * _bloom_filters[0]->bit_set_data_len() * sizeof(uint64_t);
    }
    return buffered_size;
}

OLAPStatus BloomFilterIndexWriter::write_to_buffer(OutStream* out_stream) {
    OLAPStatus res = OLAP_SUCCESS;
    if (nullptr == out_stream) {
        OLAP_LOG_WARNING("out stream is null");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // write header
    _header.block_count = _bloom_filters.size();
    res = out_stream->write(reinterpret_cast<char*>(&_header), sizeof(_header));
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("write bloom filter index header fail");
        return res;
    }

    // write bloom filters
    for (size_t i = 0; i < _bloom_filters.size(); ++i) {
        uint64_t* data = _bloom_filters[i]->bit_set_data();
        uint32_t data_len = _bloom_filters[i]->bit_set_data_len();
        res = out_stream->write(reinterpret_cast<char*>(data), sizeof(uint64_t) * data_len);
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("write bloom filter index fail, i=%u", i);
            return res;
        }
    }

    return res;
}

OLAPStatus BloomFilterIndexWriter::write_to_buffer(char* buffer, size_t buffer_size) {
    OLAPStatus res = OLAP_SUCCESS;
    if (nullptr == buffer) {
        OLAP_LOG_WARNING("out stream is nullptr.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (estimate_buffered_memory() > buffer_size) {
        OLAP_LOG_WARNING("need more buffer. [scr_size=%lu buffer_size=%lu]",
                         estimate_buffered_memory(), buffer_size);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // write header
    _header.block_count = _bloom_filters.size();
    memcpy(buffer, reinterpret_cast<char*>(&_header), sizeof(_header));
    buffer += sizeof(_header);

    // write bloom filters
    for (size_t i = 0; i < _bloom_filters.size(); ++i) {
        uint64_t* data = _bloom_filters[i]->bit_set_data();
        uint32_t data_len = _bloom_filters[i]->bit_set_data_len();
        memcpy(buffer, reinterpret_cast<char*>(data), sizeof(uint64_t) * data_len);
        buffer += sizeof(uint64_t) * data_len;
    }

    return res;
}

} // namespace doris
