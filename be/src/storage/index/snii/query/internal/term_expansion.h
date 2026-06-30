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
#include <functional>
#include <string_view>

#include "common/status.h"
#include "storage/index/snii/query/docid_sink.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::snii::query::internal {

using TermMatcher = std::function<bool(std::string_view)>;

// Enumerates dictionary terms from `enum_prefix`, filters them with `matches`,
// and emits the sorted docid union for matching entries. PrefixHit carries the
// DictEntry and block bases, so callers avoid a second lookup per expanded term.
Status emit_expanded_docid_union(const reader::LogicalIndexReader& idx,
                                 std::string_view enum_prefix, const TermMatcher& matches,
                                 DocIdSink* const sink, int32_t max_expansions = 0);

} // namespace doris::snii::query::internal
