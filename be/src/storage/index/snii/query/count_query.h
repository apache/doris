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
#include <string_view>

#include "common/status.h"
#include "storage/index/snii/reader/logical_index_reader.h"

// Answers single-term counts from dictionary frequency without reading postings.
// TextIndexReader and SegmentIterator check whether deletes and other predicates allow it.
namespace doris::snii::query {

// df of `term` in this segment without decoding postings. An absent term is a
// deterministic answer too: *count = 0 (mirrors term_query's empty result).
// Increments the count_fastpath_hits test seam.
Status count_only_term_df(const reader::LogicalIndexReader& idx, std::string_view term,
                          uint64_t* count);

} // namespace doris::snii::query
