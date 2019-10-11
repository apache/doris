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

#include "olap/olap_define.h"

namespace doris {

namespace segment_v2 {

ColumnZoneMapBuilder::ColumnZoneMapBuilder(const TypeInfo* type_info) : _type_info(type_info) {
    PageBuilderOptions options;
    options.data_page_size = 0;
    _page_builder.reset(new BinaryPlainPageBuilder(options));
    _field.reset(FieldFactory::create_by_type(_type_info->type()));
    _zone_map.min_value = _field->allocate_value_from_arena(&_arena);
    _zone_map.max_value = _field->allocate_value_from_arena(&_arena);
    _reset_page_zone_map();
    _segment_zone_map.min_value = _field->allocate_value_from_arena(&_arena);
    _segment_zone_map.max_value = _field->allocate_value_from_arena(&_arena);
    _reset_segment_zone_map();
}

Status ColumnZoneMapBuilder::add(const uint8_t *vals, size_t count) {
    if (vals != nullptr) {
        for (int i = 0; i < count; ++i) {
            if (_field->compare(_zone_map.min_value, (char *)vals) > 0) {
                _field->direct_copy_content(_zone_map.min_value, (const char *)vals);
            }
            if (_field->compare(_zone_map.max_value, (char *)vals) < 0) {
                _field->direct_copy_content(_zone_map.max_value, (const char *)vals);
            }
            vals += _type_info->size();
            if (!_zone_map.has_not_null) {
                _zone_map.has_not_null = true;
            }
        }
    }
    else {
        if (!_zone_map.has_null) {
            _zone_map.has_null = true;
        }
    }
    return Status::OK();
}

void ColumnZoneMapBuilder::fill_segment_zone_map(ZoneMapPB* const to) {
    _fill_zone_map_to_pb(_segment_zone_map, to);
}

Status ColumnZoneMapBuilder::flush() {
    // Update segment zone map.
    if (_field->compare(_segment_zone_map.min_value, _zone_map.min_value) > 0) {
        _field->direct_copy_content(_segment_zone_map.min_value, _zone_map.min_value);
    }
    if (_field->compare(_segment_zone_map.max_value, _zone_map.max_value) < 0) {
        _field->direct_copy_content(_segment_zone_map.max_value, _zone_map.max_value);
    }
    if (!_segment_zone_map.has_null && _zone_map.has_null) {
        _segment_zone_map.has_null = true;
    }
    if (!_segment_zone_map.has_not_null && _zone_map.has_not_null) {
        _segment_zone_map.has_not_null = true;
    }

    ZoneMapPB page_zone_map;
    _fill_zone_map_to_pb(_zone_map, &page_zone_map);

    std::string serialized_zone_map;
    bool ret = page_zone_map.SerializeToString(&serialized_zone_map);
    if (!ret) {
        return Status::InternalError("serialize zone map failed");
    }
    Slice data(serialized_zone_map.data(), serialized_zone_map.size());
    size_t num = 1;
    RETURN_IF_ERROR(_page_builder->add((const uint8_t *)&data, &num));
    // reset the variables
    // we should allocate max varchar length and set to max for min value
    _reset_page_zone_map();
    return Status::OK();
}

void ColumnZoneMapBuilder::_reset_zone_map(ZoneMap* zone_map) {
    _field->set_to_max(zone_map->min_value);
    _field->set_to_min(zone_map->max_value);
    zone_map->has_null = false;
    zone_map->has_not_null = false;
}

void ColumnZoneMapBuilder::_fill_zone_map_to_pb(const ZoneMap& from, ZoneMapPB* const to) {
    to->set_has_not_null(from.has_not_null);
    to->set_has_null(from.has_null);
    to->set_max(_field->to_string(from.max_value));
    to->set_min(_field->to_string(from.min_value));
}

Status ColumnZoneMap::load() {
    BinaryPlainPageDecoder page_decoder(_data);
    RETURN_IF_ERROR(page_decoder.init());
    _num_pages = page_decoder.count();
    _page_zone_maps.resize(_num_pages);
    for (int i = 0; i < _num_pages; ++i) {
        Slice data = page_decoder.string_at_index(i);
        bool ret = _page_zone_maps[i].ParseFromString(std::string(data.data, data.size));
        if (!ret) {
            return Status::Corruption("parse zone map failed");
        }
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
