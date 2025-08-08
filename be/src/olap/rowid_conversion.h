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
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/logging.h"
#include "io/cache/block_file_cache_factory.h"
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
    RowIdConversion(int64_t tablet_id, std::string tablet_path)
            : _tablet_id(tablet_id), _tablet_path(std::move(tablet_path)) {}
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
        for (size_t i = 0; i < segment_rows.size(); i++) {
            constexpr size_t RESERVED_MEMORY = 10 * 1024 * 1024;
            if (doris::GlobalMemoryArbitrator::is_exceed_hard_mem_limit(RESERVED_MEMORY)) {
                return Status::MemoryLimitExceeded("Memory limit exceeded during init");
            }

            uint32_t id = cast_set<uint32_t>(_segment_to_id_map.size());
            _segment_to_id_map.emplace(std::pair<RowsetId, uint32_t> {src_rowset_id, i}, id);
            _id_to_segment_map.emplace_back(src_rowset_id, i);
        }

        // Choose storage implementation based on memory requirement
        size_t required_mem = calculate_memory_usage(segment_rows);
        if (config::enable_rowid_conversion_spill &&
            required_mem > config::rowid_conversion_max_mb * 1024 * 1024) {
            std::string file_path;
            if (config::is_cloud_mode()) {
                std::vector<std::string> paths = io::FileCacheFactory::instance()->get_base_paths();
                if (paths.empty()) {
                    return Status::InternalError(
                            "fail to create rowid conversion backend file due to missing cache "
                            "path");
                }
                std::size_t hash_val = std::hash<int64_t> {}(_tablet_id);
                int64_t idx = hash_val % paths.size();
                file_path = fmt::format("{}/rowid_conversion_{}", paths[idx], _tablet_id);
            } else {
                file_path = fmt::format("{}/rowid_conversion_{}", _tablet_path, _tablet_id);
            }

            _storage = std::make_unique<detail::RowIdSpillableStorage>(file_path);
        } else {
            _storage = std::make_unique<detail::RowIdMemoryStorage>();
        }
        RETURN_IF_ERROR(_storage->init(segment_rows));
        track_mem_usage(_storage->memory_usage());
        return Status::OK();
    }

    void set_dst_rowset_id(const RowsetId& dst_rowset_id) { _dst_rowst_id = dst_rowset_id; }
    RowsetId get_dst_rowset_id() { return _dst_rowst_id; }

    Status add(const std::vector<RowLocation>& rss_row_ids,
               const std::vector<uint32_t>& dst_segments_num_row) {
        CHECK(_phase == Phase::BUILD) << "Cannot add row ids in READ phase";
        size_t old_mem = _storage->memory_usage();
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

            RETURN_IF_ERROR(_storage->add(id, item.row_id,
                                          {_cur_dst_segment_id, _cur_dst_segment_rowid++}));
        }
        track_mem_usage(_storage->memory_usage() - old_mem);
        return Status::OK();
    }

    // get destination RowLocation
    // return non-zero if the src RowLocation does not exist
    int get(const RowLocation& src, RowLocation* dst) {
        if (_phase == Phase::BUILD) {
            _phase = Phase::READ;
        }
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

    void prune_segment_mapping(RowsetId rowset_id, uint32_t segment_id) {
        CHECK(_phase == Phase::READ) << "Cannot prune segment mapping in BUILD phase";
        auto iter = _segment_to_id_map.find({rowset_id, segment_id});
        if (iter != _segment_to_id_map.end()) {
            _storage->prune_segment_mapping(iter->second);
        }
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

    enum class Phase { BUILD, READ } _phase {Phase::BUILD};

    std::unique_ptr<detail::RowIdConversionStorage> _storage;
    size_t _mem_used {0};

    std::map<std::pair<RowsetId, uint32_t>, uint32_t> _segment_to_id_map;
    std::vector<std::pair<RowsetId, uint32_t>> _id_to_segment_map;
    RowsetId _dst_rowst_id;
    std::uint32_t _cur_dst_segment_id {0};
    std::uint32_t _cur_dst_segment_rowid {0};

    int64_t _tablet_id {};
    std::string _tablet_path;
};

#include "common/compile_check_end.h"
} // namespace doris
