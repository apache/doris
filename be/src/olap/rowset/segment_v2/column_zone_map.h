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

#include <vector>

#include "common/status.h"
#include "util/coding.h"
#include "util/slice.h"
#include "util/faststring.h"
#include "olap/olap_cond.h"
#include "olap/rowset/segment_v2/zone_map.h"

namespace doris {

namespace segment_v2 {

// This class encode column pages' zone map.
// The binary format is like that
// Header | Content
// Header: 
//      number of elements (4 Bytes)
// Content:
//      array of page zone map
class ColumnZoneMapBuilder {
public:
    ColumnZoneMapBuilder() : _num_pages(0) {
        _buffer.reserve(4 * 1024);
        // reserve space for number of elements
        _buffer.resize(4);
    }

    void append_entry(const ZoneMap& page_zone_map) {
        // use BinaryPlainPageBuilder?
        std::string serialized_zone_map;
        page_zone_map.serialize(&serialized_zone_map);
        put_varint32(&_buffer, serialized_zone_map.size());
        _buffer.append(serialized_zone_map.data(), serialized_zone_map.size());
        _num_pages++;
    }

    Slice finish() {
        // encoded number of elements
        encode_fixed32_le((uint8_t*)_buffer.data(), _num_pages);
        return Slice(_buffer.data(), _buffer.size());
    }

private:
    faststring _buffer;
    uint32_t _num_pages;
};

// ColumnZoneMap
class ColumnZoneMap {
public:
    ColumnZoneMap(const Slice& data)
        : _data(data), _loaded(false), _num_pages(0) {
    }
    
    Status load();

    std::vector<ZoneMap> get_column_zone_map() const {
        return _page_zone_maps;
    }

    int32_t num_pages() const {
        DCHECK(_loaded) << "column zone map is not loaded";
        return _num_pages;
    }

private:
    Slice _data;
    int32_t _column_id;
    FieldType _type;

    // valid after load
    bool _loaded;
    int32_t _num_pages;
    std::vector<ZoneMap> _page_zone_maps;
};

} // namespace segment_v2
} // namespace doris
