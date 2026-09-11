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

#include <algorithm>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/check.h"
#include "runtime/thread_context.h"
#include "storage/olap_common.h"
#include "storage/utils.h"

namespace doris {

// For unique key merge on write table, we should update delete bitmap
// of destination rowset when compaction finished.
// Through the row id correspondence between the source rowset and the
// destination rowset, we can quickly update the delete bitmap of the
// destination rowset.
class RowIdConversion {
public:
    enum class Mode { DENSE, LAZY_CHUNKED };

    struct DestinationRowId {
        uint32_t segment_pos;
        uint32_t row_id;
    };

    explicit RowIdConversion(Mode mode = Mode::DENSE) : _mode(mode) {}
    ~RowIdConversion() { RELEASE_THREAD_MEM_TRACKER(_seg_rowid_map_mem_used); }

    Status init_segment_map(const RowsetId& src_rowset_id, const std::vector<uint32_t>& segment_ids,
                            const std::vector<uint32_t>& num_rows) {
        DCHECK_EQ(segment_ids.size(), num_rows.size());
        for (size_t i = 0; i < num_rows.size(); i++) {
            auto src_segment = std::pair<RowsetId, uint32_t> {src_rowset_id, segment_ids[i]};
            auto iter = _segment_to_id_map.find(src_segment);
            // A segment-group reader can be reopened, so reuse existing source-segment maps.
            if (iter != _segment_to_id_map.end()) {
                DORIS_CHECK_LT(iter->second, _segment_num_rows.size());
                DORIS_CHECK_EQ(_segment_num_rows[iter->second], num_rows[i]);
                continue;
            }

            constexpr size_t RESERVED_MEMORY = 10 * 1024 * 1024; // 10M
            RETURN_IF_ERROR(check_memory_limit(RESERVED_MEMORY));

            uint32_t id = cast_set<uint32_t>(_segment_num_rows.size());
            auto insert_result = _segment_to_id_map.emplace(src_segment, id);
            DORIS_CHECK(insert_result.second);
            _id_to_segment_map.push_back(src_segment);
            _segment_num_rows.push_back(num_rows[i]);
            if (_mode == Mode::LAZY_CHUNKED) {
                _lazy_segments_rowid_map.emplace_back();
                auto& chunks = _lazy_segments_rowid_map.back();
                chunks.resize((cast_set<size_t>(num_rows[i]) + ROWS_PER_CHUNK - 1) /
                              ROWS_PER_CHUNK);
                track_lazy_mem_usage(chunks.capacity() * sizeof(std::unique_ptr<RowIdPair[]>));
                continue;
            }

            std::vector<std::pair<uint32_t, uint32_t>> vec(
                    num_rows[i], std::pair<uint32_t, uint32_t>(UINT32_MAX, UINT32_MAX));

            //NOTE: manually count _segments_rowid_map's memory here, because _segments_rowid_map could be used by indexCompaction.
            // indexCompaction is a thridparty code, it's too complex to modify it.
            // refer compact_column.
            track_mem_usage(vec.capacity());
            _segments_rowid_map.emplace_back(std::move(vec));
        }
        return Status::OK();
    }

    // set dst rowset id
    void set_dst_rowset_id(const RowsetId& dst_rowset_id) { _dst_rowst_id = dst_rowset_id; }
    const RowsetId& get_dst_rowset_id() const { return _dst_rowst_id; }

    // add row id to the map
    Status add(const std::vector<RowLocation>& rss_row_ids,
               const std::vector<uint32_t>& dst_segments_num_row) {
        for (auto& item : rss_row_ids) {
            if (item.row_id == -1) {
                continue;
            }
            uint32_t id = _segment_to_id_map.at(
                    std::pair<RowsetId, uint32_t> {item.rowset_id, item.segment_id});
            if (_cur_dst_segment_pos < dst_segments_num_row.size() &&
                _cur_dst_segment_rowid >= dst_segments_num_row[_cur_dst_segment_pos]) {
                _cur_dst_segment_pos++;
                _cur_dst_segment_rowid = 0;
            }
            if (_mode == Mode::DENSE) {
                _segments_rowid_map[id][item.row_id] = std::pair<uint32_t, uint32_t> {
                        _cur_dst_segment_pos, _cur_dst_segment_rowid++};
                continue;
            }
            RowIdPair* destination = nullptr;
            RETURN_IF_ERROR(get_or_create_lazy_destination(id, item.row_id, &destination));
            *destination = {_cur_dst_segment_pos, _cur_dst_segment_rowid++};
        }
        return Status::OK();
    }

    // Get the destination segment position and row id. The physical destination segment id is
    // resolved only after the output rowset is built.
    // return non-zero if the src RowLocation does not exist
    int get(const RowLocation& src, DestinationRowId* dst) const {
        auto iter = _segment_to_id_map.find({src.rowset_id, src.segment_id});
        if (iter == _segment_to_id_map.end()) {
            return -1;
        }
        const RowIdPair* destination = nullptr;
        if (_mode == Mode::DENSE) {
            const auto& rowid_map = _segments_rowid_map[iter->second];
            if (src.row_id >= rowid_map.size()) {
                return -1;
            }
            destination = &rowid_map[src.row_id];
        } else {
            const auto id = iter->second;
            if (src.row_id >= _segment_num_rows[id]) {
                return -1;
            }
            destination = get_lazy_destination(id, src.row_id);
        }
        if (destination == nullptr) {
            return -1;
        }
        const auto& [dst_segment_pos, dst_rowid] = *destination;
        if (dst_segment_pos == UINT32_MAX && dst_rowid == UINT32_MAX) {
            return -1;
        }

        dst->segment_pos = dst_segment_pos;
        dst->row_id = dst_rowid;
        return 0;
    }

    const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& get_rowid_conversion_map()
            const {
        DORIS_CHECK(_mode == Mode::DENSE);
        return _segments_rowid_map;
    }

    size_t memory_usage() const { return _seg_rowid_map_mem_used; }

    const std::map<std::pair<RowsetId, uint32_t>, uint32_t>& get_src_segment_to_id_map() const {
        return _segment_to_id_map;
    }

    std::pair<RowsetId, uint32_t> get_segment_by_id(uint32_t id) const {
        DCHECK_GT(_id_to_segment_map.size(), id);
        return _id_to_segment_map.at(id);
    }

    uint32_t get_id_by_segment(const std::pair<RowsetId, uint32_t>& segment) const {
        return _segment_to_id_map.at(segment);
    }

private:
    using RowIdPair = std::pair<uint32_t, uint32_t>;
    using LazySegmentRowIdMap = std::vector<std::unique_ptr<RowIdPair[]>>;
    // A 4096-row chunk uses 32 KiB, balancing sparse-range waste and allocation overhead.
    static constexpr uint32_t ROWS_PER_CHUNK = 4096;

    Status check_memory_limit(size_t reserved_memory) const {
        if (!doris::GlobalMemoryArbitrator::is_exceed_hard_mem_limit(reserved_memory)) {
            return Status::OK();
        }
        return Status::MemoryLimitExceeded(fmt::format(
                "RowIdConversion allocation failed, process memory exceed limit or sys available "
                "memory less than low water mark, {}, consuming tracker:<{}>, peak used {}, "
                "current used {}.",
                doris::GlobalMemoryArbitrator::process_mem_log_str(),
                doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(),
                doris::thread_context()
                        ->thread_mem_tracker_mgr->limiter_mem_tracker()
                        ->peak_consumption(),
                doris::thread_context()
                        ->thread_mem_tracker_mgr->limiter_mem_tracker()
                        ->consumption()));
    }

    Status get_or_create_lazy_destination(uint32_t segment_id, uint32_t row_id,
                                          RowIdPair** destination) {
        DORIS_CHECK_LT(segment_id, _segment_num_rows.size());
        DORIS_CHECK_LT(row_id, _segment_num_rows[segment_id]);
        auto& chunks = _lazy_segments_rowid_map[segment_id];
        auto& chunk = chunks[row_id / ROWS_PER_CHUNK];
        if (chunk == nullptr) {
            constexpr size_t CHUNK_BYTES = ROWS_PER_CHUNK * sizeof(RowIdPair);
            RETURN_IF_ERROR(check_memory_limit(CHUNK_BYTES));
            chunk = std::make_unique<RowIdPair[]>(ROWS_PER_CHUNK);
            std::fill_n(chunk.get(), ROWS_PER_CHUNK, RowIdPair {UINT32_MAX, UINT32_MAX});
            track_lazy_mem_usage(CHUNK_BYTES);
        }
        *destination = &chunk[row_id % ROWS_PER_CHUNK];
        return Status::OK();
    }

    const RowIdPair* get_lazy_destination(uint32_t segment_id, uint32_t row_id) const {
        const auto& chunks = _lazy_segments_rowid_map[segment_id];
        const auto& chunk = chunks[row_id / ROWS_PER_CHUNK];
        return chunk == nullptr ? nullptr : &chunk[row_id % ROWS_PER_CHUNK];
    }

    void track_mem_usage(size_t delta_std_pair_cap) {
        _std_pair_cap += delta_std_pair_cap;

        size_t new_size =
                _std_pair_cap * sizeof(std::pair<uint32_t, uint32_t>) +
                _segments_rowid_map.capacity() * sizeof(std::vector<std::pair<uint32_t, uint32_t>>);
        CONSUME_THREAD_MEM_TRACKER(new_size - _seg_rowid_map_mem_used);
        _seg_rowid_map_mem_used = new_size;
    }

    void track_lazy_mem_usage(size_t bytes) {
        CONSUME_THREAD_MEM_TRACKER(bytes);
        _seg_rowid_map_mem_used += bytes;
    }

private:
    // the first level vector: index indicates src segment.
    // the second level vector: index indicates row id of source segment,
    // value indicates destination segment position and row id.
    // <UINT32_MAX, UINT32_MAX> indicates current row not exist.
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> _segments_rowid_map;
    // The first-level index indicates the internal source segment id.
    // The second-level index is source row_id / ROWS_PER_CHUNK and selects a lazy chunk.
    // The chunk offset is source row_id % ROWS_PER_CHUNK.
    // The value indicates destination segment position and row id.
    std::vector<LazySegmentRowIdMap> _lazy_segments_rowid_map;
    std::vector<uint32_t> _segment_num_rows;
    size_t _seg_rowid_map_mem_used {0};
    size_t _std_pair_cap {0};
    Mode _mode;

    // Map source segment to 0 to n
    std::map<std::pair<RowsetId, uint32_t>, uint32_t> _segment_to_id_map;

    // Map 0 to n to source segment
    std::vector<std::pair<RowsetId, uint32_t>> _id_to_segment_map;

    // dst rowset id
    RowsetId _dst_rowst_id;

    // current dst segment position
    std::uint32_t _cur_dst_segment_pos = 0;

    // current rowid of dst segment
    std::uint32_t _cur_dst_segment_rowid = 0;
};

} // namespace doris
