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

#include <unordered_map>
#include <vector>

#include "olap/olap_common.h"
#include "olap/utils.h"

namespace doris {

// For unique key merge on write table, we should update delete bitmap
// of destination rowset when compaction finished.
// Through the row id correspondence between the source rowset and the
// destination rowset, we can quickly update the delete bitmap of the
// destination rowset.
class RowIdConversion {
public:
    RowIdConversion() = default;
    ~RowIdConversion() = default;

    // resize segment map to its rows num
    void init_segment_map(const RowsetId& src_rowset_id, const std::vector<uint32_t>& num_rows) {
        for (size_t i = 0; i < num_rows.size(); i++) {
            _segments_rowid_map[{src_rowset_id, i}].resize(num_rows[i], UINT64_MAX);
        }
    }

    // add row id to the map
    void add(const std::vector<RowLocation>& rss_row_ids) {
        for (auto& item : rss_row_ids) {
            _segments_rowid_map[{item.rowset_id, item.segment_id}][item.row_id] =
                    _dst_rowset_num_rows;
            _dst_rowset_num_rows++;
        }
    }

    // get destination RowLocation
    // return non-zero if the src RowLocation does not exist
    int get(const RowLocation& src, RowLocation* dst) const {
        auto iter = _segments_rowid_map.find({src.rowset_id, src.segment_id});
        if (iter == _segments_rowid_map.end()) {
            return -1;
        }
        const RowIdMap& rowid_map = iter->second;
        if (src.row_id >= rowid_map.size()) {
            return -1;
        }
        uint64_t dst_rowset_rowid = rowid_map[src.row_id];
        if (dst_rowset_rowid == UINT64_MAX) {
            return -1;
        }

        dst->rowset_id = _dst_rowst_id;
        // get dst segment id and row id
        for (auto i = 0; i < _dst_rowset_segment_cumu_num_rows.size(); i++) {
            if (dst_rowset_rowid < _dst_rowset_segment_cumu_num_rows[i]) {
                dst->segment_id = i;
                if (i == 0) {
                    dst->row_id = dst_rowset_rowid;
                } else {
                    dst->row_id = dst_rowset_rowid - _dst_rowset_segment_cumu_num_rows[i - 1];
                }
                return 0;
            }
        }
        return -1;
    }

    // set segment rows number of destination rowset
    // record cumulative value
    void set_dst_segment_num_rows(const RowsetId& dst_rowset_rowid,
                                  const std::vector<uint32_t>& segment_num_rows) {
        _dst_rowst_id = dst_rowset_rowid;
        uint64_t sum = 0;
        for (auto num_rows : segment_num_rows) {
            sum += num_rows;
            _dst_rowset_segment_cumu_num_rows.push_back(sum);
        }
        DCHECK_EQ(sum, _dst_rowset_num_rows);
    }

private:
    using RowId = uint32_t;
    using SegmentId = uint32_t;
    // key: vector index indicates row id of source segment, value: row id of destination rowset
    // UINT64_MAX indicates not exist
    using RowIdMap = std::vector<uint64_t>;
    // key: src segment
    using SegmentsRowIdMap = std::map<std::pair<RowsetId, SegmentId>, RowIdMap>;

    SegmentsRowIdMap _segments_rowid_map;

    // segement rows number of destination rowset
    std::vector<uint64_t> _dst_rowset_segment_cumu_num_rows;

    RowsetId _dst_rowst_id;
    // rows number of destination rowset
    uint64_t _dst_rowset_num_rows = 0;
};

} // namespace doris
