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
Status OlapTabletFinder::find_tablets(RuntimeState* state, Block* block, int rows,
                                      std::vector<VOlapTablePartition*>& partitions,
                                      std::vector<uint32_t>& tablet_index, bool& stop_processing,
                                      std::vector<bool>& skip, std::vector<int64_t>* miss_rows) {
    for (int index = 0; index < rows; index++) {
        _vpartition->find_partition(block, index, partitions[index]);
    }

    std::vector<uint32_t> indexes;
    indexes.reserve(rows);

    for (int row_index = 0; row_index < rows; row_index++) {
        if (partitions[row_index] == nullptr) [[unlikely]] {
            if (miss_rows != nullptr) {          // auto partition table
                miss_rows->push_back(row_index); // already reserve memory outside
                skip[row_index] = true;
                continue;
            }
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
            skip[row_index] = true;
            continue;
        }
        if (!partitions[row_index]->is_mutable) [[unlikely]] {
            _num_immutable_partition_filtered_rows++;
            skip[row_index] = true;
            continue;
        }
        if (partitions[row_index]->num_buckets <= 0) [[unlikely]] {
            std::stringstream ss;
            ss << "num_buckets must be greater than 0, num_buckets="
               << partitions[row_index]->num_buckets;
            return Status::InternalError(ss.str());
        }

        _partition_ids.emplace(partitions[row_index]->id);

        indexes.push_back(row_index);
    }

    if (_find_tablet_mode == FindTabletMode::FIND_TABLET_EVERY_ROW) {
        _vpartition->find_tablets(block, indexes, partitions, tablet_index);
    } else {
        _vpartition->find_tablets(block, indexes, partitions, tablet_index,
                                  &_partition_to_tablet_map);
    }

    return Status::OK();
}

} // namespace doris::vectorized