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

#include <gtest/gtest.h>

#include "exec/operator/iceberg_sorter_reserve_memory.h"

namespace doris {

TEST(SpillIcebergTableSinkOperatorTest, BoundsManyPartitionReservationToOneInputBlock) {
    std::vector<IcebergSorterReserveMemory> per_partition_reservations(
            128, {.retained_growth = 0, .transient_workspace = 8 * 1024 * 1024});

    EXPECT_EQ(8 * 1024 * 1024, bounded_iceberg_reserve_size(per_partition_reservations));
}

TEST(SpillIcebergTableSinkOperatorTest, AccumulatesRetainedGrowthAcrossTouchedPartitions) {
    std::vector<IcebergSorterReserveMemory> per_partition_reservations {
            {.retained_growth = 3 * 1024 * 1024, .transient_workspace = 7 * 1024 * 1024},
            {.retained_growth = 4 * 1024 * 1024, .transient_workspace = 5 * 1024 * 1024}};

    EXPECT_EQ(14 * 1024 * 1024, bounded_iceberg_reserve_size(per_partition_reservations));
}

TEST(SpillIcebergTableSinkOperatorTest, ReservesIncomingBlockBeforeAnyPartitionWriterExists) {
    std::vector<IcebergSorterReserveMemory> no_published_sorters;

    EXPECT_EQ(6 * 1024 * 1024, iceberg_reserve_size(no_published_sorters, 6 * 1024 * 1024));
}

} // namespace doris
