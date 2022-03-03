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

#ifndef DORIS_BE_SRC_OLAP_COLUMN_FILE_BLOOM_FILTER_READER_H
#define DORIS_BE_SRC_OLAP_COLUMN_FILE_BLOOM_FILTER_READER_H

#include <vector>

#include "olap/bloom_filter.hpp"
#include "olap/bloom_filter_writer.h"

namespace doris {

// Each bloom filter index contains multiple bloom filter entries,
//     each of which is related to a data block.
//     BloomFilterIndexReader allow caller to get specified bloom filter entry
//     by parsing bloom filter index buffer.
class BloomFilterIndexReader {
public:
    BloomFilterIndexReader() {}
    ~BloomFilterIndexReader();

    // Init BloomFilterIndexReader with given bloom filter index buffer
    OLAPStatus init(char* buffer, size_t buffer_size, bool is_using_cache,
                    uint32_t hash_function_num, uint32_t bit_num);

    // Get specified bloom filter entry
    const BloomFilter& entry(uint64_t entry_id);

    // Get total number of bloom filter entries in current bloom filter index buffer
    size_t entry_count();

private:
    // Bloom filter index buffer and length
    char* _buffer;
    size_t _buffer_size;

    // Total bloom filter entries count, start offset, each bloom filter entry size
    size_t _entry_count;
    size_t _start_offset;
    size_t _step_size;

    // Bloom filter param
    uint32_t _hash_function_num;

    // BloomFilterIndexReader will not release bloom filter index buffer in destructor
    // when it is cached in memory
    bool _is_using_cache;

    BloomFilter _entry;
};

} // namespace doris
#endif // DORIS_BE_SRC_OLAP_COLUMN_FILE_BLOOM_FILTER_READER_H
