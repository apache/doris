// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_BLOOM_FILTER_WRITER_H
#define BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_BLOOM_FILTER_WRITER_H

#include <vector>

#include "olap/column_file/bloom_filter.hpp"
#include "olap/column_file/out_stream.h"

namespace palo {
namespace column_file {

class BloomFilterIndexWriter {
public:
    BloomFilterIndexWriter() {}
    ~BloomFilterIndexWriter();

    OLAPStatus add_bloom_filter(BloomFilter* bf);
    uint64_t estimate_buffered_memory();
    OLAPStatus write_to_buffer(OutStream* out_stream);
    OLAPStatus write_to_buffer(char* buffer, size_t buffer_size);

private:
    std::vector<BloomFilter*> _bloom_filters;
    BloomFilterIndexHeader _header;
};

} // namespace column_file
} // namespace palo
#endif // BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_BLOOM_FILTER_WRITER_H
