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
#include "util/slice.h"
#include "olap/field.h"
#include "gen_cpp/segment_v2.pb.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"

namespace doris {

namespace segment_v2 {

struct ZoneMap {
    // min value of zone
    char* min_value = nullptr;
    // max value of zone
    char* max_value = nullptr;

    // if both has_null and has_not_null is false, means no rows.
    // if has_null is true and has_not_null is false, means all rows is null.
    // if has_null is false and has_not_null is true, means all rows is not null.
    // if has_null is true and has_not_null is true, means some rows is null and others are not.
    // has_null means whether zone has null value
    bool has_null = false;
    // has_not_null means whether zone has none-null value
    bool has_not_null = false;
};

// This class encode column pages' zone map.
// The binary is encoded by BinaryPlainPageBuilder
class ColumnZoneMapBuilder {
public:
    ColumnZoneMapBuilder(const TypeInfo* type_info);

    Status add(const uint8_t* vals, size_t count);

    Status flush();

    Slice finish() {
        return _page_builder->finish();
    }

private:
    void _reset_zone_map();

private:
    const TypeInfo* _type_info;
    std::unique_ptr<BinaryPlainPageBuilder> _page_builder;
    std::unique_ptr<Field> _field;
    // memory will be managed by arena
    ZoneMap _zone_map;
    char* _max_string_value;
    Arena _arena;
};

// ColumnZoneMap
class ColumnZoneMap {
public:
    ColumnZoneMap(const Slice& data) : _data(data), _num_pages(0) { }
    
    Status load();

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
