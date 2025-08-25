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

#include "util/key_util.h"

namespace doris {

bool key_is_not_in_segment(Slice key, const KeyBoundsPB& segment_key_bounds,
                           bool is_segments_key_bounds_truncated) {
    Slice maybe_truncated_min_key {segment_key_bounds.min_key()};
    Slice maybe_truncated_max_key {segment_key_bounds.max_key()};
    bool res1 = Slice::lhs_is_strictly_less_than_rhs(key, false, maybe_truncated_min_key,
                                                     is_segments_key_bounds_truncated);
    bool res2 = Slice::lhs_is_strictly_less_than_rhs(maybe_truncated_max_key,
                                                     is_segments_key_bounds_truncated, key, false);
    return res1 || res2;
}
} // namespace doris