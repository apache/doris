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

#include "common/status.h"
#include "exec/tablet_info.h"
#include "util/bitmap.h"
#include "vec/core/block.h"

namespace doris::vectorized {

class OlapTabletFinder {
public:
    // FIND_TABLET_EVERY_ROW is used for both hash and random distribution info, which indicates that we
    // should compute tablet index for every row
    // FIND_TABLET_EVERY_BATCH is only used for random distribution info, which indicates that we should
    // compute tablet index for every row batch
    // FIND_TABLET_EVERY_SINK is only used for random distribution info, which indicates that we should
    // only compute tablet index in the corresponding partition once for the whole time in olap table sink
    enum FindTabletMode { FIND_TABLET_EVERY_ROW, FIND_TABLET_EVERY_BATCH, FIND_TABLET_EVERY_SINK };

    OlapTabletFinder(VOlapTablePartitionParam* vpartition, FindTabletMode mode)
            : _vpartition(vpartition), _find_tablet_mode(mode), _filter_bitmap(1024) {};

    Status find_tablet(RuntimeState* state, vectorized::Block* block, int row_index,
                       const VOlapTablePartition** partition, uint32_t& tablet_index,
                       bool& filtered, bool& is_continue, bool* missing_partition = nullptr);

    bool is_find_tablet_every_sink() {
        return _find_tablet_mode == FindTabletMode::FIND_TABLET_EVERY_SINK;
    }

    void clear_for_new_batch() {
        if (_find_tablet_mode == FindTabletMode::FIND_TABLET_EVERY_BATCH) {
            _partition_to_tablet_map.clear();
        }
    }

    bool is_single_tablet() { return _partition_to_tablet_map.size() == 1; }

    const std::set<int64_t>& partition_ids() { return _partition_ids; }

    int64_t num_filtered_rows() const { return _num_filtered_rows; }

    int64_t num_immutable_partition_filtered_rows() const {
        return _num_immutable_partition_filtered_rows;
    }

    Bitmap& filter_bitmap() { return _filter_bitmap; }

private:
    VOlapTablePartitionParam* _vpartition;
    FindTabletMode _find_tablet_mode;
    std::map<int64_t, int64_t> _partition_to_tablet_map;
    std::set<int64_t> _partition_ids;

    int64_t _num_filtered_rows = 0;
    int64_t _num_immutable_partition_filtered_rows = 0;
    Bitmap _filter_bitmap;
};

} // namespace doris::vectorized