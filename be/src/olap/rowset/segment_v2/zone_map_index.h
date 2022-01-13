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

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "env/env.h"
#include "gen_cpp/segment_v2.pb.h"
#include "olap/field.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/slice.h"

namespace doris {

namespace fs {
class WritableBlock;
}

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

    bool pass_all = false;

    void to_proto(ZoneMapPB* dst, Field* field) {
        if (pass_all) {
            dst->set_min("");
            dst->set_max("");
        } else {
            dst->set_min(field->to_string(min_value));
            dst->set_max(field->to_string(max_value));
        }
        dst->set_has_null(has_null);
        dst->set_has_not_null(has_not_null);
        dst->set_pass_all(pass_all);
    }
};

// Zone map index is represented by an IndexedColumn with ordinal index.
// The IndexedColumn stores serialized ZoneMapPB for each data page.
// It also create and store the segment-level zone map in the index meta so that
// reader can prune an entire segment without reading pages.
class ZoneMapIndexWriter {
public:
    explicit ZoneMapIndexWriter(Field* field);

    void add_values(const void* values, size_t count);

    void add_nulls(uint32_t count) { _page_zone_map.has_null = true; }

    // mark the end of one data page so that we can finalize the corresponding zone map
    Status flush();

    Status finish(fs::WritableBlock* wblock, ColumnIndexMetaPB* index_meta);

    void moidfy_index_before_flush(ZoneMap& zone_map);

    uint64_t size() { return _estimated_size; }

    void reset_page_zone_map();
    void reset_segment_zone_map();

private:
    void _reset_zone_map(ZoneMap* zone_map) {
        // we should allocate max varchar length and set to max for min value
        _field->set_to_zone_map_max(zone_map->min_value);
        _field->set_to_zone_map_min(zone_map->max_value);
        zone_map->has_null = false;
        zone_map->has_not_null = false;
        zone_map->pass_all = false;
    }

    Field* _field;
    // memory will be managed by MemPool
    ZoneMap _page_zone_map;
    ZoneMap _segment_zone_map;
    // TODO(zc): we should replace this memory pool later, we only allocate min/max
    // for field. But MemPool allocate 4KB least, it will a waste for most cases.
    std::shared_ptr<MemTracker> _tracker;
    MemPool _pool;

    // serialized ZoneMapPB for each data page
    std::vector<std::string> _values;
    uint64_t _estimated_size = 0;
};

class ZoneMapIndexReader {
public:
    explicit ZoneMapIndexReader(const FilePathDesc& path_desc, const ZoneMapIndexPB* index_meta)
            : _path_desc(path_desc), _index_meta(index_meta) {}

    // load all page zone maps into memory
    Status load(bool use_page_cache, bool kept_in_memory);

    const std::vector<ZoneMapPB>& page_zone_maps() const { return _page_zone_maps; }

    int32_t num_pages() const { return _page_zone_maps.size(); }

private:
    FilePathDesc _path_desc;
    const ZoneMapIndexPB* _index_meta;

    std::vector<ZoneMapPB> _page_zone_maps;
};

} // namespace segment_v2
} // namespace doris
