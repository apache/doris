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

namespace doris {

// Simple verion of rowid conversion, for segcompaction
// convert rows from several segments to rows in 1 segment
class SimpleRowIdConversion {
public:
    SimpleRowIdConversion(const RowsetId& rowset_id) : _rowst_id(rowset_id) {};
    ~SimpleRowIdConversion() = default;

    // resize segment rowid map to its rows num
    void reset_segment_map(const std::map<uint32_t, uint32_t>& num_rows) {
        _cur_dst_segment_rowid = 0;
        for (auto seg_rows : num_rows) {
            _segments_rowid_map.emplace(seg_rows.first,
                                        std::vector<uint32_t>(seg_rows.second, UINT32_MAX));
        }
    }

    // add row id to the map
    void add(const std::vector<RowLocation>& rss_row_ids) {
        for (auto& item : rss_row_ids) {
            if (item.row_id == -1) {
                continue;
            }
            DCHECK(_segments_rowid_map.find(item.segment_id) != _segments_rowid_map.end() &&
                   _segments_rowid_map[item.segment_id].size() > item.row_id);
            _segments_rowid_map[item.segment_id][item.row_id] = _cur_dst_segment_rowid++;
        }
    }

    // get destination RowLocation
    // return non-zero if the src RowLocation does not exist
    int get(const RowLocation& src) const {
        auto it = _segments_rowid_map.find(src.segment_id);
        if (it == _segments_rowid_map.end()) {
            return -1;
        }
        const auto& rowid_map = it->second;
        if (src.row_id >= rowid_map.size() || UINT32_MAX == rowid_map[src.row_id]) {
            return -1;
        }

        return rowid_map[src.row_id];
    }

private:
    // key:   index indicates src segment.
    // value: index indicates row id of source segment, value indicates row id of destination
    //        segment. UINT32_MAX indicates current row not exist.
    std::map<uint32_t, std::vector<uint32_t>> _segments_rowid_map;

    // dst rowset id
    RowsetId _rowst_id;

    // current rowid of dst segment
    std::uint32_t _cur_dst_segment_rowid = 0;
};

} // namespace doris
