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

#include <map>
#include <vector>

#include "common/cast_set.h"
#include "olap/olap_common.h"
#include "olap/rowid_conversion_storage.h"
#include "olap/utils.h"
#include "runtime/thread_context.h"

namespace doris {
#include "common/compile_check_begin.h"

// For unique key merge on write table, we should update delete bitmap
// of destination rowset when compaction finished.
// Through the row id correspondence between the source rowset and the
// destination rowset, we can quickly update the delete bitmap of the
// destination rowset.
class RowIdConversion {
public:
    RowIdConversion() = default;
    ~RowIdConversion() { RELEASE_THREAD_MEM_TRACKER(_mem_used); }

    static size_t calculate_memory_usage(const std::vector<uint32_t>& segment_rows) {
        size_t total = 0;
        for (uint32_t rows : segment_rows) {
            total += sizeof(std::pair<uint32_t, uint32_t>) * rows;
        }
        return total;
    }

    Status init_segment_map(const RowsetId& src_rowset_id,
                            const std::vector<uint32_t>& segment_rows) {
        size_t required_mem = calculate_memory_usage(segment_rows);

        // Choose storage implementation based on memory requirement
        if (config::enable_rowid_conversion_spill &&
            required_mem > config::rowid_conversion_max_mb * 1024 * 1024) {
            _storage = std::make_unique<detail::SpillableStorage>();
        } else {
            _storage = std::make_unique<detail::MemoryStorage>();
        }

        RETURN_IF_ERROR(_storage->init(segment_rows));

        for (size_t i = 0; i < segment_rows.size(); i++) {
            constexpr size_t RESERVED_MEMORY = 10 * 1024 * 1024;
            if (doris::GlobalMemoryArbitrator::is_exceed_hard_mem_limit(RESERVED_MEMORY)) {
                return Status::MemoryLimitExceeded("Memory limit exceeded during init");
            }

            uint32_t id = cast_set<uint32_t>(_segment_to_id_map.size());
            _segment_to_id_map.emplace(std::pair<RowsetId, uint32_t> {src_rowset_id, i}, id);
            _id_to_segment_map.emplace_back(src_rowset_id, i);

            RETURN_IF_ERROR(_storage->init_segment(id, segment_rows[i]));
        }

        track_mem_usage(_storage->memory_usage());
        return Status::OK();
    }

    void set_dst_rowset_id(const RowsetId& dst_rowset_id) { _dst_rowst_id = dst_rowset_id; }
    RowsetId get_dst_rowset_id() { return _dst_rowst_id; }

    Status add(const std::vector<RowLocation>& rss_row_ids,
               const std::vector<uint32_t>& dst_segments_num_row) {
        for (const auto& item : rss_row_ids) {
            if (item.row_id == -1) {
                continue;
            }

            uint32_t id = _segment_to_id_map.at(
                    std::pair<RowsetId, uint32_t> {item.rowset_id, item.segment_id});

            if (_cur_dst_segment_id < dst_segments_num_row.size() &&
                _cur_dst_segment_rowid >= dst_segments_num_row[_cur_dst_segment_id]) {
                _cur_dst_segment_id++;
                _cur_dst_segment_rowid = 0;
            }

            size_t old_mem = _storage->memory_usage();
            RETURN_IF_ERROR(_storage->add(id, item.row_id,
                                          {_cur_dst_segment_id, _cur_dst_segment_rowid++}));
            track_mem_usage(_storage->memory_usage() - old_mem);
        }
        return Status::OK();
    }

    // get destination RowLocation
    // return non-zero if the src RowLocation does not exist
    int get(const RowLocation& src, RowLocation* dst) const {
        auto iter = _segment_to_id_map.find({src.rowset_id, src.segment_id});
        if (iter == _segment_to_id_map.end()) {
            return -1;
        }

        std::pair<uint32_t, uint32_t> value;
        auto st = _storage->get(iter->second, src.row_id, &value);
        if (!st.ok()) {
            return -1;
        }

        dst->rowset_id = _dst_rowst_id;
        dst->segment_id = value.first;
        dst->row_id = value.second;
        return 0;
    }

    const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& get_rowid_conversion_map()
            const {
        return _storage->get_rowid_conversion_map();
    }

    const std::map<std::pair<RowsetId, uint32_t>, uint32_t>& get_src_segment_to_id_map() {
        return _segment_to_id_map;
    }

private:
    void track_mem_usage(ssize_t delta_bytes) {
        _mem_used += delta_bytes;
        CONSUME_THREAD_MEM_TRACKER(delta_bytes);
    }

    std::unique_ptr<detail::RowIdConversionStorage> _storage;
    size_t _mem_used {0};

    std::map<std::pair<RowsetId, uint32_t>, uint32_t> _segment_to_id_map;
    std::vector<std::pair<RowsetId, uint32_t>> _id_to_segment_map;
    RowsetId _dst_rowst_id;
    std::uint32_t _cur_dst_segment_id {0};
    std::uint32_t _cur_dst_segment_rowid {0};
};

#include "common/compile_check_end.h"
} // namespace doris
