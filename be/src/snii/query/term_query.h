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

// term_query -- the simplest SNII query: return the sorted docid set that
// contains term. It runs the term lookup on the logical index, then issues a
// single batched .frq range read (one serial round) to decode the postings.
// Absent term -> empty result (OK status).
namespace snii::query {

doris::Status term_query(const snii::reader::LogicalIndexReader& idx, std::string_view term,
                         std::vector<uint32_t>* docids);
doris::Status term_query(const snii::reader::LogicalIndexReader& idx, std::string_view term,
                         DocIdSink* sink);
doris::Status term_query(const snii::reader::LogicalIndexReader& idx, std::string_view term,
                         std::vector<uint32_t>* docids, QueryProfile* profile);

} // namespace snii::query
