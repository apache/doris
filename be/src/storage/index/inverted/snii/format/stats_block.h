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

namespace doris::snii::format {

// Runtime counting statistics used for query planning and BM25. Core protobuf
// metadata owns their on-disk representation.
struct StatsBlock {
    uint64_t doc_count = 0;           // total doc count at segment level (including unindexed/NULL)
    uint64_t indexed_doc_count = 0;   // number of docs actually indexed (denominator for avgdl)
    uint64_t term_count = 0;          // number of unique terms in this index
    uint64_t sum_total_term_freq = 0; // total token count across all indexed docs
    uint64_t null_count = 0;          // number of NULL / not-indexed docs
};

} // namespace doris::snii::format
