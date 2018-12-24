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

namespace doris {

bool AlphaRowsetMeta::deserialize_extra_properties() {
    std::string extra_properties = _rowset_meta->extra_properties();
    bool parsed = _extra_meta_pb->ParseFromString(extra_properties);
    if (!parsed) {
        LOG(WARNING) << "alpha rowset parse extra propertis failed.";
    }
    return parsed;
}

void AlphaRowsetMeta::get_segment_groups(std::vector<SegmentGroupPB>* segment_groups) {
    for (auto& segment_group : _extra_meta_pb.segment_groups()) {
        segment_groups->push_back(segment_group);
    }
}

void AlphaRowsetMeta::add_segment_group(SegmentGroupPB& segment_group) {
    SegmentGroupPB* new_segment_group = _extra_meta_pb.mutable_segment_groups();
    *new_segment_group = segment_group;
}

void AlphaRowsetMeta::_serialize_extra_meta_pb() {
    std::string extra_properties;
    _extra_meta_pb->SerializeToString(&extra_properties);
    _rowset_meta->set_extra_properties(extra_properties);
}

}  // namespace doris
