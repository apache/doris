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

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace doris::cloud {

template <typename RowsetMetaPB>
int64_t rowset_segment_id(const RowsetMetaPB& rowset_meta, int64_t pos) {
    DCHECK_GE(pos, 0);
    DCHECK_LT(pos, rowset_meta.num_segments());
    if (rowset_meta.segment_ids_size() == 0) {
        return pos;
    }
    DCHECK_EQ(rowset_meta.segment_ids_size(), rowset_meta.num_segments());
    return rowset_meta.segment_ids(static_cast<int>(pos));
}

template <typename RowsetMetaPB>
std::vector<int64_t> rowset_segment_ids(const RowsetMetaPB& rowset_meta) {
    DCHECK_GE(rowset_meta.num_segments(), 0);
    std::vector<int64_t> segment_ids;
    segment_ids.reserve(static_cast<size_t>(rowset_meta.num_segments()));
    for (int64_t pos = 0; pos < rowset_meta.num_segments(); ++pos) {
        segment_ids.push_back(rowset_segment_id(rowset_meta, pos));
    }
    return segment_ids;
}

} // namespace doris::cloud
