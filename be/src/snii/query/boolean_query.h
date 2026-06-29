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
#include <string>
#include <vector>

#include "common/status.h"
#include "snii/query/docid_sink.h"
#include "snii/query/query_profile.h"
#include "snii/reader/logical_index_reader.h"

// boolean_or -- MATCH_ANY semantics: return the sorted docid set containing at
// least one query term. Empty terms or all-absent terms produce an empty
// result. Duplicate input terms are ignored semantically and do not duplicate
// output docids.
namespace snii::query {

doris::Status boolean_or(const snii::reader::LogicalIndexReader& idx,
                  const std::vector<std::string>& terms, std::vector<uint32_t>* docids);
doris::Status boolean_or(const snii::reader::LogicalIndexReader& idx,
                  const std::vector<std::string>& terms, std::vector<uint32_t>* docids,
                  QueryProfile* profile);
doris::Status boolean_or(const snii::reader::LogicalIndexReader& idx,
                  const std::vector<std::string>& terms, DocIdSink* sink);

// boolean_and (MATCH all-terms): sorted docid set of docs containing EVERY
// term, no positional constraint. Valid on docs-only indexes. Empty terms or
// any absent term -> empty result.
doris::Status boolean_and(const snii::reader::LogicalIndexReader& idx,
                   const std::vector<std::string>& terms, std::vector<uint32_t>* docids);
doris::Status boolean_and(const snii::reader::LogicalIndexReader& idx,
                   const std::vector<std::string>& terms, std::vector<uint32_t>* docids,
                   QueryProfile* profile);

} // namespace snii::query
