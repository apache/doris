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
#include <utility>
#include <vector>

namespace doris {
class RowsetMeta;
class TabletSchema;

namespace segment_v2 {
class SegmentFooterPB;
}

namespace variant_util {

// An empty result means the segment's layout could not be fingerprinted safely.
std::string build_segment_desc_fingerprint(const segment_v2::SegmentFooterPB& footer,
                                           const TabletSchema& schema);

// The rowset may use its fingerprint only if all final physical segments are covered.
void set_rowset_desc_fingerprint(RowsetMeta& rowset,
                                 const std::vector<std::pair<int64_t, std::string>>& segments);

} // namespace variant_util
} // namespace doris
