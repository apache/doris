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
    _max_string_value = _arena.Allocate(OLAP_STRING_MAX_LENGTH);
    _zone_map.min_value = _arena.Allocate(_type_info->size());
    _zone_map.max_value = _arena.Allocate(_type_info->size());
    _reset_zone_map();
}

Status ColumnZoneMapBuilder::add(const uint8_t *vals, size_t count) {
    if (vals != nullptr) {
        for (int i = 0; i < count; ++i) {
            if (_field->compare(_zone_map.min_value, (char *)vals) > 0) {
                _field->deep_copy_content(_zone_map.min_value, (const char *)vals, &_arena);
            }
            if (_field->compare(_zone_map.max_value, (char *)vals) < 0) {
                _field->deep_copy_content(_zone_map.max_value, (const char *)vals, &_arena);
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

Status ColumnZoneMapBuilder::flush() {
    ZoneMapPB page_zone_map;
    page_zone_map.set_min(_field->to_string(_zone_map.min_value));
    page_zone_map.set_max(_field->to_string(_zone_map.max_value));
    page_zone_map.set_has_null(_zone_map.has_null);
    page_zone_map.set_has_not_null(_zone_map.has_not_null);
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
    _reset_zone_map();
    return Status::OK();
}

void ColumnZoneMapBuilder::_reset_zone_map() {
    // we should allocate max varchar length and set to max for min value
    Slice *min_slice = (Slice *)_zone_map.min_value;
    min_slice->data = _max_string_value;
    min_slice->size = OLAP_STRING_MAX_LENGTH;
    _field->set_to_max(_zone_map.min_value);
    _field->set_to_min(_zone_map.max_value);
    _zone_map.has_null = false;
    _zone_map.has_not_null = false;
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
