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

#include <cstdint>
#include <map>
#include <utility>
#include <vector>

#include "olap/olap_common.h"
namespace doris {
#include "common/compile_check_begin.h"
namespace detail {
class RowIdConversionStorage;
}
struct RowLocation;

// For unique key merge on write table, we should update delete bitmap
// of destination rowset when compaction finished.
// Through the row id correspondence between the source rowset and the
// destination rowset, we can quickly update the delete bitmap of the
// destination rowset.
class RowIdConversion {
public:
    RowIdConversion();
    ~RowIdConversion();

    static size_t calculate_memory_usage(size_t rows);

    Status init(bool use_spill, int64_t memory_limit, int64_t tablet_id, std::string tablet_path,
                bool can_prune);
    Status init();

    Status init_segment_map(const RowsetId& src_rowset_id,
                            const std::vector<uint32_t>& segment_rows);

    void set_dst_rowset_id(const RowsetId& dst_rowset_id);
    RowsetId get_dst_rowset_id() const;

    Status add(const std::vector<RowLocation>& rss_row_ids,
               const std::vector<uint32_t>& dst_segments_num_row);

    // get destination RowLocation
    // return non-zero if the src RowLocation does not exist
    int get(const RowLocation& src, RowLocation* dst);

    void prune_segment_mapping(RowsetId rowset_id, uint32_t segment_id);

    const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& get_rowid_conversion_map() const;

    const std::map<std::pair<RowsetId, uint32_t>, uint32_t>& get_src_segment_to_id_map() const;

    bool can_prune() const;

private:
    void track_mem_usage(ssize_t delta_bytes);

    enum class Phase { BUILD, READ } _phase {Phase::BUILD};

    std::unique_ptr<detail::RowIdConversionStorage> _storage;
    size_t _mem_used {0};

    RowsetId _dst_rowst_id;
    std::uint32_t _cur_dst_segment_id {0};
    std::uint32_t _cur_dst_segment_rowid {0};

    bool _can_prune {false};
};

#include "common/compile_check_end.h"
} // namespace doris
