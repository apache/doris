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

#include "olap/rowset/segment_v2/zone_map.h"
#include "gen_cpp/segment_v2.pb.h" // for ZoneMapPB
#include "util/coding.h"

namespace doris {
namespace segment_v2 {

ZoneMap::ZoneMap() : ZoneMap(ZONE_MAP_V1) { }

ZoneMap::ZoneMap(ZoneMapVersion version) :_version(version) { }

Status ZoneMap::serialize(std::string* dst) const {
    if (_version == ZONE_MAP_V1) {
        ZoneMapPB zone_map;
        zone_map.set_min(_min_value);
        zone_map.set_max(_max_value);
        zone_map.set_null_flag(_is_null);
        bool ret = zone_map.SerializeToString(dst);
        if (!ret) {
            return Status::InternalError("serialize zone map failed");
        }
    }
    return Status::OK();
}

Status ZoneMap::deserialize(const std::string& src) {
    if (_version == ZONE_MAP_V1) {
        ZoneMapPB zone_map;
        bool ret = zone_map.ParseFromString(src);
        if (!ret) {
            return Status::InternalError("parse zone map failed");
        }
        _min_value = zone_map.min();
        _max_value = zone_map.max();
        _is_null = zone_map.null_flag();
    }
    return Status::OK();
}

}
}
