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
#include <memory>

#include "common/status.h"
#include "util/coding.h"
#include "util/slice.h"
#include "util/faststring.h"
#include "olap/olap_cond.h"
#include "olap/olap_define.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "gen_cpp/segment_v2.pb.h"

namespace doris {

namespace segment_v2 {

// This class encode column pages' zone map.
// The binary is encoded by BinaryPlainPageBuilder
class ColumnZoneMapBuilder {
public:
    ColumnZoneMapBuilder(const TypeInfo* type_info) : _type_info(type_info),
            _min_value(nullptr), _max_value(nullptr),
            _null_flag(false), _non_null_flag(false) {
        PageBuilderOptions options;
        options.data_page_size = 0;
        _page_builder.reset(new BinaryPlainPageBuilder(options));
        _field.reset(Field::create_by_type(_type_info->type()));
        _max_string_value = _arena.Allocate(OLAP_STRING_MAX_LENGTH);
        _min_value = _arena.Allocate(_type_info->size());
        // we should allocate max varchar length and set to max for min value
        Slice* min_slice = (Slice*)_min_value;
        min_slice->data = _max_string_value;
        min_slice->size = OLAP_STRING_MAX_LENGTH;
        _field->set_to_max(_min_value);
        _max_value = _arena.Allocate(_type_info->size());
        _field->set_to_min(_max_value);
    }

    Status add(const uint8_t* vals, size_t count) {
        if (vals != nullptr) {
            for (int i = 0; i < count; ++i) {
                if (_field->compare(_min_value, (char*)vals) > 0) {
                    _field->deep_copy_content(_min_value, (const char*)vals, &_arena);
                }
                if (_field->compare(_max_value, (char*)vals) < 0) {
                    _field->deep_copy_content(_max_value, (const char*)vals, &_arena);
                }
                vals += _type_info->size();
                if (!_non_null_flag) {
                    _non_null_flag = true;
                }
            }
        } else {
            if (!_null_flag) {
                _null_flag = true;
            }
        }
        return Status::OK();
    }

    Status flush() {
        ZoneMapPB page_zone_map;
        page_zone_map.set_min(_field->to_string(_min_value));
        page_zone_map.set_max(_field->to_string(_max_value));
        page_zone_map.set_null_flag(_null_flag);
        page_zone_map.set_non_null_flag(_non_null_flag);
        std::string serialized_zone_map;
        bool ret = page_zone_map.SerializeToString(&serialized_zone_map);
        if (!ret) {
            return Status::InternalError("serialize zone map failed");
        }
        Slice data(serialized_zone_map.data(), serialized_zone_map.size());
        size_t num = 1;
        RETURN_IF_ERROR(_page_builder->add((const uint8_t*)&data, &num));
        // reset the variables
        // we should allocate max varchar length and set to max for min value
        Slice* min_slice = (Slice*)_min_value;
        min_slice->data = _max_string_value;
        min_slice->size = OLAP_STRING_MAX_LENGTH;
        _field->set_to_max(_min_value);
        _field->set_to_min(_max_value);
        _null_flag = false;
        _non_null_flag = false;
        return Status::OK();
    }

    Slice finish() {
        return _page_builder->finish();
    }

private:
    const TypeInfo* _type_info;
    std::unique_ptr<BinaryPlainPageBuilder> _page_builder;
    std::unique_ptr<Field> _field;
    // memory will be managed by arena
    char* _min_value;
    char* _max_value;
    char* _max_string_value;
    // if both _null_flag and _non_full_flag is false, means no rows.
    // if _null_flag is true and _non_full_flag is false, means all rows is null.
    // if _null_flag is false and _non_full_flag is true, means all rows is not null.
    // if _null_flag is true and _non_full_flag is true, means some rows is null and others are not.
    bool _null_flag;
    bool _non_null_flag;
    Arena _arena;
};

// ColumnZoneMap
class ColumnZoneMap {
public:
    ColumnZoneMap(const Slice& data)
        : _data(data), _num_pages(0) { }
    
    Status load() {
        BinaryPlainPageDecoder page_decoder(_data);
        RETURN_IF_ERROR(page_decoder.init());
        _num_pages = page_decoder.count();
        for (int i = 0; i < _num_pages; ++i) {
            Slice data = page_decoder.string_at_index(i);
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

    // valid after load
    int32_t _num_pages;
    std::vector<ZoneMapPB> _page_zone_maps;
};

} // namespace segment_v2
} // namespace doris
