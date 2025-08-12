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

#include "olap/rowid_conversion.h"

#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "io/cache/block_file_cache_factory.h"
#include "olap/rowid_conversion_storage.h"
#include "olap/utils.h"
#include "runtime/thread_context.h"
namespace doris {
#include "common/compile_check_begin.h"

RowIdConversion::RowIdConversion() = default;

RowIdConversion::~RowIdConversion() {
    RELEASE_THREAD_MEM_TRACKER(_mem_used);
}

size_t RowIdConversion::calculate_memory_usage(size_t rows) {
    return sizeof(std::pair<uint32_t, uint32_t>) * rows;
}

Status RowIdConversion::init(bool use_spill, int64_t memory_limit, int64_t tablet_id,
                             std::string tablet_path, bool can_prune) {
    _can_prune = can_prune;
    if (use_spill) {
        std::string file_path;
        if (config::is_cloud_mode()) {
            std::vector<std::string> paths = io::FileCacheFactory::instance()->get_base_paths();
            if (paths.empty()) {
                return Status::InternalError(
                        "fail to create rowid conversion backend file due to missing cache "
                        "path");
            }
            std::size_t hash_val = std::hash<int64_t> {}(tablet_id);
            int64_t idx = hash_val % paths.size();
            file_path = fmt::format("{}/rowid_conversion_{}", paths[idx], tablet_id);
        } else {
            file_path = fmt::format("{}/rowid_conversion_{}", tablet_path, tablet_id);
        }

        _storage = std::make_unique<detail::RowIdSpillableStorage>(memory_limit, file_path);
    } else {
        _storage = std::make_unique<detail::RowIdMemoryStorage>();
    }
    return _storage->init();
}

Status RowIdConversion::init() {
    _storage = std::make_unique<detail::RowIdMemoryStorage>();
    return _storage->init();
}

Status RowIdConversion::init_segment_map(const RowsetId& src_rowset_id,
                                         const std::vector<uint32_t>& segment_rows) {
    CHECK(_storage != nullptr) << "RowIdConversion storage is not initialized";
    RETURN_IF_ERROR(_storage->init_new_segments(src_rowset_id, segment_rows));
    track_mem_usage(_storage->memory_usage());
    return Status::OK();
}

void RowIdConversion::set_dst_rowset_id(const RowsetId& dst_rowset_id) {
    _dst_rowst_id = dst_rowset_id;
}

RowsetId RowIdConversion::get_dst_rowset_id() const {
    return _dst_rowst_id;
}

Status RowIdConversion::add(const std::vector<RowLocation>& rss_row_ids,
                            const std::vector<uint32_t>& dst_segments_num_row) {
    CHECK(_phase == Phase::BUILD) << "Cannot add row ids in READ phase";
    size_t old_mem = _storage->memory_usage();
    VLOG_DEBUG << fmt::format(
            "[verbose] RowIdConversion::add, rows size={}, current memory usage={}",
            rss_row_ids.size(), old_mem);
    for (const auto& item : rss_row_ids) {
        if (item.row_id == -1) {
            continue;
        }

        if (_cur_dst_segment_id < dst_segments_num_row.size() &&
            _cur_dst_segment_rowid >= dst_segments_num_row[_cur_dst_segment_id]) {
            _cur_dst_segment_id++;
            _cur_dst_segment_rowid = 0;
        }

        RETURN_IF_ERROR(_storage->add(item.rowset_id, item.segment_id, item.row_id,
                                      {_cur_dst_segment_id, _cur_dst_segment_rowid++}));
    }
    RETURN_IF_ERROR(_storage->spill_if_eligible());
    VLOG_DEBUG << fmt::format(
            "[verbose] after RowIdConversion::add, rows size={}, current memory usage={}",
            rss_row_ids.size(), _storage->memory_usage());
    track_mem_usage(_storage->memory_usage() - old_mem);
    return Status::OK();
}

// get destination RowLocation
// return non-zero if the src RowLocation does not exist
int RowIdConversion::get(const RowLocation& src, RowLocation* dst) {
    if (_phase == Phase::BUILD) {
        _phase = Phase::READ;
    }

    std::pair<uint32_t, uint32_t> value;
    auto st = _storage->get(src.rowset_id, src.segment_id, src.row_id, &value);
    if (!st.ok()) {
        return -1;
    }

    dst->rowset_id = _dst_rowst_id;
    dst->segment_id = value.first;
    dst->row_id = value.second;
    return 0;
}

void RowIdConversion::prune_segment_mapping(RowsetId rowset_id, uint32_t segment_id) {
    CHECK(_phase == Phase::READ) << "Cannot prune segment mapping in BUILD phase";
    _storage->prune_segment_mapping(rowset_id, segment_id);
}

const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>&
RowIdConversion::get_rowid_conversion_map() const {
    return _storage->get_rowid_conversion_map();
}

const std::map<std::pair<RowsetId, uint32_t>, uint32_t>&
RowIdConversion::get_src_segment_to_id_map() const {
    return _storage->get_src_segment_to_id_map();
}

void RowIdConversion::track_mem_usage(ssize_t delta_bytes) {
    _mem_used += delta_bytes;
    CONSUME_THREAD_MEM_TRACKER(delta_bytes);
}

bool RowIdConversion::can_prune() const {
    return _can_prune;
}

#include "common/compile_check_end.h"
} // namespace doris