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

#include "vec/sink/vtablet_finder.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <glog/logging.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "exec/tablet_info.h"
#include "exprs/runtime_filter.h"
#include "gutil/integral_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

Status OlapTabletFinder::find_tablet(RuntimeState* state, Block* block, int row_index,
                                     const VOlapTablePartition** partition, uint32_t& tablet_index,
                                     bool& stop_processing, bool& is_continue,
                                     bool* missing_partition) {
    Status status = Status::OK();
    *partition = nullptr;
    tablet_index = 0;
    BlockRow block_row;
    block_row = {block, row_index};
    if (!_vpartition->find_partition(&block_row, partition)) {
        if (missing_partition != nullptr) { // auto partition table
            *missing_partition = true;
            return status;
        } else {
            RETURN_IF_ERROR(state->append_error_msg_to_file(
                    []() -> std::string { return ""; },
                    [&]() -> std::string {
                        fmt::memory_buffer buf;
                        fmt::format_to(buf, "no partition for this tuple. tuple=\n{}",
                                       block->dump_data(row_index, 1));
                        return fmt::to_string(buf);
                    },
                    &stop_processing));
            _num_filtered_rows++;
            _filter_bitmap.Set(row_index, true);
            if (stop_processing) {
                return Status::EndOfFile("Encountered unqualified data, stop processing");
            }
            is_continue = true;
            return status;
        }
    }
    if (!(*partition)->is_mutable) {
        _num_immutable_partition_filtered_rows++;
        is_continue = true;
        return status;
    }
    if ((*partition)->num_buckets <= 0) {
        std::stringstream ss;
        ss << "num_buckets must be greater than 0, num_buckets=" << (*partition)->num_buckets;
        return Status::InternalError(ss.str());
    }
    _partition_ids.emplace((*partition)->id);
    if (_find_tablet_mode != FindTabletMode::FIND_TABLET_EVERY_ROW) {
        if (_partition_to_tablet_map.find((*partition)->id) == _partition_to_tablet_map.end()) {
            tablet_index = _vpartition->find_tablet(&block_row, **partition);
            _partition_to_tablet_map.emplace((*partition)->id, tablet_index);
        } else {
            tablet_index = _partition_to_tablet_map[(*partition)->id];
        }
    } else {
        tablet_index = _vpartition->find_tablet(&block_row, **partition);
    }

    return status;
}

} // namespace doris::vectorized