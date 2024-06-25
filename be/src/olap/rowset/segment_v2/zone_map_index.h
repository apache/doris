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

#include <gen_cpp/segment_v2.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/field.h"
#include "runtime/define_primitive_type.h"
#include "util/once.h"
#include "vec/common/arena.h"

namespace doris {

namespace io {
class FileWriter;
} // namespace io

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

    void to_proto(ZoneMapPB* dst, Field* field) const {
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

class ZoneMapIndexWriter {
public:
    static Status create(Field* field, std::unique_ptr<ZoneMapIndexWriter>& res);

    ZoneMapIndexWriter() = default;

    virtual ~ZoneMapIndexWriter() = default;

    virtual void add_values(const void* values, size_t count) = 0;

    virtual void add_nulls(uint32_t count) = 0;

    // mark the end of one data page so that we can finalize the corresponding zone map
    virtual Status flush() = 0;

    virtual Status finish(io::FileWriter* file_writer, ColumnIndexMetaPB* index_meta) = 0;

    virtual void moidfy_index_before_flush(ZoneMap& zone_map) = 0;

    virtual uint64_t size() const = 0;

    virtual void reset_page_zone_map() = 0;
};

// Zone map index is represented by an IndexedColumn with ordinal index.
// The IndexedColumn stores serialized ZoneMapPB for each data page.
// It also create and store the segment-level zone map in the index meta so that
// reader can prune an entire segment without reading pages.
template <PrimitiveType Type>
class TypedZoneMapIndexWriter final : public ZoneMapIndexWriter {
public:
    explicit TypedZoneMapIndexWriter(Field* field);

    void add_values(const void* values, size_t count) override;

    void add_nulls(uint32_t count) override { _page_zone_map.has_null = true; }

    // mark the end of one data page so that we can finalize the corresponding zone map
    Status flush() override;

    Status finish(io::FileWriter* file_writer, ColumnIndexMetaPB* index_meta) override;

    void moidfy_index_before_flush(ZoneMap& zone_map) override;

    uint64_t size() const override { return _estimated_size; }

    void reset_page_zone_map() override;

private:
    void _reset_zone_map(ZoneMap* zone_map) {
        // we should allocate max varchar length and set to max for min value
        _field->set_to_zone_map_max(zone_map->min_value);
        _field->set_to_zone_map_min(zone_map->max_value);
        zone_map->has_null = false;
        zone_map->has_not_null = false;
        zone_map->pass_all = false;
    }

    Field* _field = nullptr;
    // memory will be managed by Arena
    ZoneMap _page_zone_map;
    ZoneMap _segment_zone_map;
    // TODO(zc): we should replace this arena later, we only allocate min/max
    // for field. But Arena allocate 4KB least, it will a waste for most cases.
    vectorized::Arena _arena;

    // serialized ZoneMapPB for each data page
    std::vector<std::string> _values;
    uint64_t _estimated_size = 0;
};

class ZoneMapIndexReader {
public:
    explicit ZoneMapIndexReader(io::FileReaderSPtr file_reader,
                                const IndexedColumnMetaPB& page_zone_maps)
            : _file_reader(std::move(file_reader)) {
        _page_zone_maps_meta.reset(new IndexedColumnMetaPB(page_zone_maps));
    }

    virtual ~ZoneMapIndexReader();

    // load all page zone maps into memory
    Status load(bool use_page_cache, bool kept_in_memory);

    const std::vector<ZoneMapPB>& page_zone_maps() const { return _page_zone_maps; }

    int32_t num_pages() const { return _page_zone_maps.size(); }

private:
    Status _load(bool use_page_cache, bool kept_in_memory, std::unique_ptr<IndexedColumnMetaPB>);

private:
    DorisCallOnce<Status> _load_once;
    // TODO: yyq, we shoud remove file_reader from here.
    io::FileReaderSPtr _file_reader;
    std::unique_ptr<IndexedColumnMetaPB> _page_zone_maps_meta;
    std::vector<ZoneMapPB> _page_zone_maps;
};

} // namespace segment_v2
} // namespace doris
