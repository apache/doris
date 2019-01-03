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

bool AlphaRowsetMeta::deserialize_extra_properties() {
    std::string extra_properties = get_extra_properties();
    bool parsed = _extra_meta_pb.ParseFromString(extra_properties);
    return parsed;
}

void AlphaRowsetMeta::get_segment_groups(std::vector<SegmentGroupPB>* segment_groups) {
    for (auto& segment_group : _extra_meta_pb.segment_groups()) {
        segment_groups->push_back(segment_group);
    }
}

void AlphaRowsetMeta::add_segment_group(const SegmentGroupPB& segment_group) {
    SegmentGroupPB* new_segment_group = _extra_meta_pb.add_segment_groups();
    *new_segment_group = segment_group;
}

void AlphaRowsetMeta::get_pending_segment_groups(
        std::vector<PendingSegmentGroupPB>* pending_segment_groups) {
    for (auto& pending_segment_group : _extra_meta_pb.pending_segment_groups()) {
        pending_segment_groups->push_back(pending_segment_group);
    }
}

void AlphaRowsetMeta::add_pending_segment_group(const PendingSegmentGroupPB& pending_segment_group) {
    for (int i = 0; i < _extra_meta_pb.pending_segment_groups_size(); i++) {
        const PendingSegmentGroupPB& present_segment_group = _extra_meta_pb.pending_segment_groups(i);
        if (present_segment_group.pending_segment_group_id() ==
                pending_segment_group.pending_segment_group_id()) {
            LOG(WARNING) << "pending segment_group already exists in meta."
                        << "rowset_id:" << rowset_id()
                        << ", pending_segment_group_id: " << pending_segment_group.pending_segment_group_id();
            return;
        }
    }
    PendingSegmentGroupPB* new_pending_segment_group = _extra_meta_pb.add_pending_segment_groups();
    *new_pending_segment_group = pending_segment_group;
}

void AlphaRowsetMeta::_serialize_extra_meta_pb() {
    std::string extra_properties;
    _extra_meta_pb.SerializeToString(&extra_properties);
    set_extra_properties(extra_properties);
}

}  // namespace doris
