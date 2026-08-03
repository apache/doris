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
#include "exec/sink/writer/async_result_writer.h"

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

TEST(SpillIcebergTableSinkOperatorTest, WaitsUntilDequeuedBlockUpdatesSorterState) {
    AsyncWriterQueueAdmission stateful_admission;
    stateful_admission.wait_for_processing_before_next_sink();

    EXPECT_TRUE(stateful_admission.is_available(0));
    EXPECT_FALSE(stateful_admission.is_available(1));
    stateful_admission.begin_processing();
    // Dequeueing does not admit block 2 until block 1 changes the state sampled by admission.
    EXPECT_FALSE(stateful_admission.is_available(0));
    stateful_admission.finish_processing();
    EXPECT_TRUE(stateful_admission.is_available(0));

    // Writers without state-dependent admission retain the existing three-block queue behavior.
    AsyncWriterQueueAdmission buffered_admission;
    buffered_admission.begin_processing();
    EXPECT_TRUE(buffered_admission.is_available(2));
    EXPECT_FALSE(buffered_admission.is_available(3));
}

} // namespace doris
