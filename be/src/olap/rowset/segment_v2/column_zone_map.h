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
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "gen_cpp/segment_v2.pb.h"

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
    ColumnZoneMapBuilder() : _page_builder(nullptr) {
        PageBuilderOptions options;
        options.data_page_size = 0;
        _page_builder = new BinaryPlainPageBuilder(options);
    }

    ~ColumnZoneMapBuilder() {
        delete _page_builder;
    }

    Status append_entry(const ZoneMapPB& page_zone_map) {
        std::string serialized_zone_map;
        bool ret = page_zone_map.SerializeToString(&serialized_zone_map);
        if (!ret) {
            return Status::InternalError("serialize zone map failed");
        }
        Slice data(serialized_zone_map.data(), serialized_zone_map.size());
        size_t num = 1;
        RETURN_IF_ERROR(_page_builder->add((const uint8_t*)&data, &num));
        return Status::OK();
    }

    Slice finish() {
        return _page_builder->finish();
    }

private:
    BinaryPlainPageBuilder* _page_builder;
};

// ColumnZoneMap
class ColumnZoneMap {
public:
    ColumnZoneMap(const Slice& data)
        : _data(data), _page_decoder(nullptr), _num_pages(0) {
        _page_decoder = new BinaryPlainPageDecoder(_data, PageDecoderOptions());
    }

    ~ColumnZoneMap() {
        delete _page_decoder;
    }
    
    Status load() {
        RETURN_IF_ERROR(_page_decoder->init());
        _num_pages = _page_decoder->count();
        for (int i = 0; i < _num_pages; ++i) {
            Slice data = _page_decoder->string_at_index(i);
            ZoneMapPB zone_map;
            bool ret = zone_map.ParseFromString(std::string(data.data, data.size));
            if (!ret) {
                return Status::InternalError("parse zone map failed");
            }
            _page_zone_maps.emplace_back(zone_map);
        }
        return Status::OK();
    }

    const std::vector<ZoneMapPB>& get_column_zone_map() const {
        return _page_zone_maps;
    }

    int32_t num_pages() const {
        return _num_pages;
    }

private:
    Slice _data;
    BinaryPlainPageDecoder* _page_decoder;

    // valid after load
    int32_t _num_pages;
    std::vector<ZoneMapPB> _page_zone_maps;
};

} // namespace segment_v2
} // namespace doris
