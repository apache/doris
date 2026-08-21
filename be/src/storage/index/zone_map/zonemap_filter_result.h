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

namespace doris {

// What a zone map tells us about one condition over one zone. A zone is a segment or a page.
// A condition accepts some set of values. A zone map describes a set that covers every value
// the zone holds. The answer is which way those two sets sit:
//
//   Z = the set the zone map covers, P = the set the condition accepts
//
//     Z  [---]          Z  [-----]        Z  [------]       Z     [--]
//     P        [---]    P     [-----]     P    [--]         P  [------]
//     disjoint          overlapping       P inside Z        Z inside P
//     kNoMatch          kMayMatch         kMayMatch         kAllMatch
//
// Z only has to cover the zone, so it may be wider than what is really stored there. A wider Z
// never produces a wrong kNoMatch or kAllMatch, it only makes those two answers harder to reach.
enum class ZoneMapFilterResult : uint8_t {
    // No row in the zone can satisfy the condition. Skip the whole zone without reading it.
    kNoMatch = 0,
    // Some rows may satisfy the condition and some may not. Read the zone and evaluate it.
    kMayMatch = 1,
    // The zone map cannot answer, for example the column has no zone map, or this kind of
    // condition has no zone map rule. Read the zone as with kMayMatch, but count it separately.
    kUnsupported = 2,
    // Every row in the zone satisfies the condition. Read the zone but drop the condition,
    // it can no longer remove any row.
    kAllMatch = 3,
};

} // namespace doris
