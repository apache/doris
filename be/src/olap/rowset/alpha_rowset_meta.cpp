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

#include "olap/rowset/alpha_rowset_meta.h"

#include "common/logging.h"

namespace doris {

void AlphaRowsetMeta::get_segment_groups(std::vector<SegmentGroupPB>* segment_groups) {
    if (!_has_alpha_rowset_extra_meta_pb()) {
        return;
    }
    const AlphaRowsetExtraMetaPB& alpha_rowset_extra_meta = alpha_rowset_extra_meta_pb();
    for (auto& segment_group : alpha_rowset_extra_meta.segment_groups()) {
        segment_groups->push_back(segment_group);
    }
}

void AlphaRowsetMeta::add_segment_group(const SegmentGroupPB& segment_group) {
    AlphaRowsetExtraMetaPB* alpha_rowset_extra_meta_pb = _mutable_alpha_rowset_extra_meta_pb();
    SegmentGroupPB* new_segment_group = alpha_rowset_extra_meta_pb->add_segment_groups();
    *new_segment_group = segment_group;
}

void AlphaRowsetMeta::clear_segment_group() {
    if (!_has_alpha_rowset_extra_meta_pb()) {
        return;
    }
    AlphaRowsetExtraMetaPB* alpha_rowset_extra_meta_pb = _mutable_alpha_rowset_extra_meta_pb();
    alpha_rowset_extra_meta_pb->clear_segment_groups();
}

} // namespace doris
