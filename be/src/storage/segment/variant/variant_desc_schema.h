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

#include <optional>
#include <string>
#include <vector>

#include "storage/rowset/rowset_fwd.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris::segment_v2 {
class SegmentFooterPB;
}

namespace doris::variant_util {

// A missing summary requires falling back to the segment reader. A present empty
// summary means the segment contributes no extracted subcolumns.
std::optional<std::string> build_segment_desc_summary(const segment_v2::SegmentFooterPB& footer,
                                                      const TabletSchema& schema);
std::optional<std::string> merge_desc_summaries(const std::vector<std::string>& summaries);

TabletSchemaSPtr calculate_variant_extended_schema(const std::vector<RowsetSharedPtr>& rowsets,
                                                   const TabletSchemaSPtr& base_schema);

} // namespace doris::variant_util
