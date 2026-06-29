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
#include <vector>

#include "common/status.h"
#include "snii/query/docid_sink.h"
#include "snii/query/query_profile.h"
#include "snii/reader/logical_index_reader.h"

// regexp_query -- MATCH_REGEXP semantics over dictionary terms. The pattern is
// evaluated with std::regex_match, so it must match the whole term. Matching
// terms are executed as a sorted deduplicated docid union.
namespace snii::query {

doris::Status regexp_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                    std::vector<uint32_t>* const docids, int32_t max_expansions = 0);
doris::Status regexp_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                    std::vector<uint32_t>* const docids, QueryProfile* profile,
                    int32_t max_expansions = 0);
doris::Status regexp_query(const snii::reader::LogicalIndexReader& idx, std::string_view pattern,
                    DocIdSink* const sink, int32_t max_expansions = 0);

} // namespace snii::query
