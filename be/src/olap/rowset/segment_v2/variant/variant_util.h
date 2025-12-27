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
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "vec/columns/column_variant.h"
#include "vec/json/json_parser.h"

namespace doris {
class TabletSchema;
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::segment_v2::variant_util {

// Parse variant columns by picking variant positions from `column_pos` and generating ParseConfig
// based on tablet schema settings (flatten nested / doc snapshot mode).
Status parse_variant_columns(vectorized::Block& block, const TabletSchema& tablet_schema,
                             const std::vector<uint32_t>& column_pos);

// Moved from `vec/common/schema_util.{h,cpp}`.
Status parse_variant_columns(vectorized::Block& block, const std::vector<uint32_t>& variant_pos,
                             const std::vector<vectorized::ParseConfig>& configs);

// Parse doc snapshot column (paths/values/offsets stored in ColumnVariant) into per-path subcolumns.
// NOTE: Returned map keys are `std::string_view` pointing into the underlying doc snapshot paths
// column, so the input `variant` must outlive the returned map.
std::unordered_map<std::string_view, vectorized::ColumnVariant::Subcolumn>
parse_doc_snapshot_to_subcolumns(const vectorized::ColumnVariant& variant);

} // namespace doris::segment_v2::variant_util