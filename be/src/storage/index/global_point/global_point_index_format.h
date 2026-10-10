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

#pragma once

#include <stdint.h>

#include <cstddef>

namespace doris::segment_v2 {

// On-disk layout of a GLOBAL_POINT index file (.gpidx), shared by the writer and the reader.
//
// A .gpidx file holds the bloom filter of one indexed column for one whole rowset (all of its
// segments): this header, followed by the bloom body (BloomFilter::data(), BloomFilter::size()
// bytes, which includes the trailing has-null byte). The header makes the file self-validating,
// so a truncated or corrupt file is detected and treated as "may contain any value".
//
// The header is written as raw bytes in host byte order. Doris only runs on little-endian hosts;
// the static_assert below pins the size so a layout change cannot go unnoticed.
struct GlobalPointIndexHeader {
    static constexpr char kMagic[4] = {'G', 'P', 'I', 'X'};
    static constexpr uint32_t kFormatVersion = 1;

    char magic[4];
    uint32_t format_version;
    int32_t hash_strategy;
    uint64_t num_bits;
    int64_t total_rows;
    uint32_t body_crc32;
    uint64_t reserved = 0;
};

static_assert(sizeof(GlobalPointIndexHeader) == 48, "the .gpidx header layout is persisted");

constexpr size_t kGlobalPointIndexHeaderSize = sizeof(GlobalPointIndexHeader);

} // namespace doris::segment_v2
