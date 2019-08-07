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

#include "olap/rowset/segment_v2/column_zone_map.h"

namespace doris {
namespace segment_v2 {

Status ColumnZoneMap::load() {
    DCHECK_GE(_data.size, 4) << "block size must greate than header";
    _num_pages = decode_fixed32_le((const uint8_t*)_data.data);
    _data.remove_prefix(4);
    _page_zone_maps.reserve(_num_pages);
    for (int i = 0; i < _num_pages; ++i) {
        uint32_t zone_map_length = 0;
        get_varint32(&_data, &zone_map_length);
        DCHECK(zone_map_length > 0);
        ZoneMap zone_map;
        zone_map.deserialize(std::string(_data.data, zone_map_length));
        _page_zone_maps.emplace_back(zone_map);
        _data.remove_prefix(zone_map_length);
    }
    DCHECK(_data.size == 0) << "there is additional data.";
    _loaded = true;
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
