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
#include <limits>
#include <vector>

namespace doris {

struct IcebergSorterReserveMemory {
    size_t retained_growth = 0;
    size_t transient_workspace = 0;
};

inline size_t bounded_iceberg_reserve_size(
        const std::vector<IcebergSorterReserveMemory>& per_partition_reservations) {
    size_t retained_growth = 0;
    size_t transient_workspace = 0;
    for (const auto& reservation : per_partition_reservations) {
        retained_growth = std::min(std::numeric_limits<size_t>::max() - retained_growth,
                                   reservation.retained_growth) +
                          retained_growth;
        transient_workspace = std::max(transient_workspace, reservation.transient_workspace);
    }
    return std::min(std::numeric_limits<size_t>::max() - retained_growth, transient_workspace) +
           retained_growth;
}

inline size_t iceberg_reserve_size(
        const std::vector<IcebergSorterReserveMemory>& per_partition_reservations,
        size_t incoming_block_bytes) {
    size_t sorter_reserve = bounded_iceberg_reserve_size(per_partition_reservations);
    // The incoming block creates cold partition writers before they can appear in the published snapshot.
    return std::min(std::numeric_limits<size_t>::max() - sorter_reserve, incoming_block_bytes) +
           sorter_reserve;
}

} // namespace doris
