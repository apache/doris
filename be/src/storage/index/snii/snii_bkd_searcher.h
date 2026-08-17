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

#include <cstdint>
#include <memory>

#include "storage/index/snii/bkd/bkd_reader.h"

// What one opened SNII BKD logical index amounts to, and therefore what the
// searcher cache holds on to.
//
// The BkdReader alone is not enough: the null bitmap is a THIRD sub-file of the
// same blob entry, and re-resolving the container directory to find it on every
// read_null_bitmap would undo the point of caching. Its extent is 16 bytes of
// metadata, so it rides along.
namespace doris::snii::bkd {

struct BkdSearcher {
    std::unique_ptr<BkdReader> reader;
    // Extent of the bkd_nulls sub-file inside the container. length == 0 means
    // the column had no NULL row -- a legal state, not a missing section.
    uint64_t null_bitmap_offset = 0;
    uint64_t null_bitmap_length = 0;

    // Real resident cost, for the searcher cache's accounting. The null extent
    // is not counted because its bytes are not held here.
    size_t memory_usage() const {
        return sizeof(*this) + (reader != nullptr ? reader->memory_usage() : 0);
    }
};

} // namespace doris::snii::bkd
