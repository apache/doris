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

#include "olap/olap_common.h"
#include "olap/utils.h"
#include "runtime/memory/mem_tracker_limiter.h"

namespace doris {

// For unique key merge on write table, we should update delete bitmap
// of destination rowset when compaction finished.
// Through the row id correspondence between the source rowset and the
// destination rowset, we can quickly update the delete bitmap of the
// destination rowset.
class RowIdConversion {
public:
    RowIdConversion() = default;
    ~RowIdConversion() {
        if (_rowid_convert_mem_tracker.get() != nullptr) {
            _rowid_convert_mem_tracker->release(_rowid_convert_mem_tracker->consumption());
        }
    }

    // resize segment rowid map to its rows num
    void init_segment_map(const RowsetId& src_rowset_id, const std::vector<uint32_t>& num_rows) {
        for (size_t i = 0; i < num_rows.size(); i++) {
            uint32_t id = _segments_rowid_map.size();
            _segment_to_id_map.emplace(std::pair<RowsetId, uint32_t> {src_rowset_id, i}, id);
            _id_to_segment_map.emplace_back(src_rowset_id, i);
            _segments_rowid_map.emplace_back(std::vector<std::pair<uint32_t, uint32_t>>(
                    num_rows[i], std::pair<uint32_t, uint32_t>(UINT32_MAX, UINT32_MAX)));
        }
    }

    // set dst rowset id
    void set_dst_rowset_id(const RowsetId& dst_rowset_id) { _dst_rowst_id = dst_rowset_id; }
    const RowsetId get_dst_rowset_id() { return _dst_rowst_id; }

    // add row id to the map
    void add(const std::vector<RowLocation>& rss_row_ids,
             const std::vector<uint32_t>& dst_segments_num_row) {
        for (auto& item : rss_row_ids) {
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
            _segments_rowid_map[id][item.row_id] =
                    std::pair<uint32_t, uint32_t> {_cur_dst_segment_id, _cur_dst_segment_rowid++};
        }
    }

    // get destination RowLocation
    // return non-zero if the src RowLocation does not exist
    int get(const RowLocation& src, RowLocation* dst) const {
        auto iter = _segment_to_id_map.find({src.rowset_id, src.segment_id});
        if (iter == _segment_to_id_map.end()) {
            return -1;
        }
        const auto& rowid_map = _segments_rowid_map[iter->second];
        if (src.row_id >= rowid_map.size()) {
            return -1;
        }
        auto& [dst_segment_id, dst_rowid] = rowid_map[src.row_id];
        if (dst_segment_id == UINT32_MAX && dst_rowid == UINT32_MAX) {
            return -1;
        }

        dst->rowset_id = _dst_rowst_id;
        dst->segment_id = dst_segment_id;
        dst->row_id = dst_rowid;
        return 0;
    }

    const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& get_rowid_conversion_map()
            const {
        return _segments_rowid_map;
    }

    const std::map<std::pair<RowsetId, uint32_t>, uint32_t>& get_src_segment_to_id_map() {
        return _segment_to_id_map;
    }

    std::pair<RowsetId, uint32_t> get_segment_by_id(uint32_t id) const {
        DCHECK_GT(_id_to_segment_map.size(), id);
        return _id_to_segment_map.at(id);
    }

    uint32_t get_id_by_segment(const std::pair<RowsetId, uint32_t>& segment) const {
        return _segment_to_id_map.at(segment);
    }

    void set_mem_tracker(std::shared_ptr<MemTracker> mem_tracker) {
        this->_rowid_convert_mem_tracker = mem_tracker;
    }
    std::shared_ptr<MemTracker> get_mem_tracker() { return _rowid_convert_mem_tracker; }

private:
    // the first level vector: index indicates src segment.
    // the second level vector: index indicates row id of source segment,
    // value indicates row id of destination segment.
    // <UINT32_MAX, UINT32_MAX> indicates current row not exist.
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> _segments_rowid_map;

    // Map source segment to 0 to n
    std::map<std::pair<RowsetId, uint32_t>, uint32_t> _segment_to_id_map;

    // Map 0 to n to source segment
    std::vector<std::pair<RowsetId, uint32_t>> _id_to_segment_map;

    // dst rowset id
    RowsetId _dst_rowst_id;

    // current dst segment id
    std::uint32_t _cur_dst_segment_id = 0;

    // current rowid of dst segment
    std::uint32_t _cur_dst_segment_rowid = 0;

    // rowid conversion mem tracker
    std::shared_ptr<MemTracker> _rowid_convert_mem_tracker;
};

} // namespace doris
