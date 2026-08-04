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

#include <deque>
#include <memory>
#include <string>

#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_string.h"
#include "exec/operator/iceberg_sorter_reserve_memory.h"
#include "exec/sink/writer/async_writer_queue_admission.h"
#include "exec/sink/writer/hive_multipart_compatibility.h"

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

TEST(SpillIcebergTableSinkOperatorTest, ColdWriterReserveUsesFirstBlockLargerThanOperatorFloor) {
    constexpr size_t operator_floor = 32 * 1024 * 1024;
    auto strings = ColumnString::create();
    std::string payload(40 * 1024 * 1024, 'x');
    strings->insert_data(payload.data(), payload.size());
    Block block;
    block.insert({std::move(strings), std::make_shared<DataTypeString>(), "payload"});

    ASSERT_GT(block.allocated_bytes(), operator_floor);
    EXPECT_GT(iceberg_cold_writer_reserve_size(block, operator_floor), 2 * block.allocated_bytes());
}

TEST(SpillIcebergTableSinkOperatorTest, ReservesAllMergeInputsAndOutputAtEos) {
    constexpr size_t MB = 1024 * 1024;

    EXPECT_EQ(72 * MB, iceberg_spill_merge_workspace(12, 8 * MB, 64 * MB));
    EXPECT_EQ(32 * MB, iceberg_spill_merge_workspace(3, 8 * MB, 64 * MB));
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

TEST(SpillIcebergTableSinkOperatorTest, TerminalWriterDrainsQueuedReservations) {
    int live_reservations = 0;
    struct Reservation {
        explicit Reservation(int* live) : live(live) { ++*live; }
        ~Reservation() { --*live; }
        int* live;
    };
    struct Queued {
        size_t bytes;
        std::unique_ptr<Reservation> reservation;
    };
    std::deque<Queued> queue;
    queue.push_back({7, std::make_unique<Reservation>(&live_reservations)});
    queue.push_back({11, std::make_unique<Reservation>(&live_reservations)});
    size_t released_bytes = 0;

    drain_async_writer_queue(queue, [&](const Queued& queued) { released_bytes += queued.bytes; });

    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(0, live_reservations);
    EXPECT_EQ(18, released_bytes);
}

TEST(SpillIcebergTableSinkOperatorTest, AzureDeferredMultipartRequiresCoordinatorCapability) {
    EXPECT_TRUE(hive_multipart_protocol_supported(io::ObjStorageType::AWS, false));
    EXPECT_FALSE(hive_multipart_protocol_supported(io::ObjStorageType::AZURE, false));
    EXPECT_TRUE(hive_multipart_protocol_supported(io::ObjStorageType::AZURE, true));
}

} // namespace doris
