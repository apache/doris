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

#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "exec/partitioner/partitioner.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/hash_util.hpp"

namespace doris {

namespace {

constexpr uint32_t kPartitions = 1U << 20;

const double kQuietNaN = std::numeric_limits<double>::quiet_NaN();
const double kPayloadNaN = std::bit_cast<double>(std::bit_cast<uint64_t>(kQuietNaN) | 0x1234ULL);

// Legacy zlib CRC32 convention of Crc32HashPartitioner<ShuffleChannelIds>: hash of the raw
// bytes seeded with 0, then modulo partition count.
uint32_t legacy_channel(double value) {
    return HashUtil::zlib_crc32_fixed(value, 0) % kPartitions;
}

std::vector<uint32_t> partition(Crc32HashPartitioner<ShuffleChannelIds>& partitioner,
                                RuntimeState* state, Block& block) {
    EXPECT_TRUE(partitioner.do_partitioning(state, &block).ok());
    return partitioner.get_channel_ids();
}

} // namespace

// From NORMALIZE_FLOAT_HASH_KEY_VERSION on, -0.0 / +0.0 and every NaN reach the same channel;
// with an older query version the legacy raw-bit convention is kept so that senders of mixed
// BE versions agree during a rolling upgrade. The shuffled rows themselves are never rewritten.
TEST(PartitionerFloatKeyTest, DoubleKeyIsNormalizedOnlyFromTheNewVersion) {
    Crc32HashPartitioner<ShuffleChannelIds> partitioner(kPartitions);
    partitioner._partition_expr_ctxs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeFloat64>()});
    MockRuntimeState state;

    Block block = ColumnHelper::create_block<DataTypeFloat64>(
            {-0.0, 0.0, kPayloadNaN, kQuietNaN, 1.5, -1.5});

    state.set_be_exec_version(NORMALIZE_FLOAT_HASH_KEY_VERSION);
    auto channels = partition(partitioner, &state, block);
    ASSERT_EQ(channels.size(), 6);
    EXPECT_EQ(channels[0], channels[1]);
    EXPECT_EQ(channels[1], legacy_channel(0.0));
    EXPECT_EQ(channels[2], channels[3]);
    EXPECT_EQ(channels[3], legacy_channel(kQuietNaN));
    EXPECT_EQ(channels[4], legacy_channel(1.5));
    EXPECT_EQ(channels[5], legacy_channel(-1.5));
    // The block is left with its stored values (and without the hashing copy).
    ASSERT_EQ(block.columns(), 1);
    const auto& data =
            assert_cast<const ColumnFloat64&>(*block.get_by_position(0).column).get_data();
    EXPECT_TRUE(std::signbit(data[0]));
    EXPECT_EQ(std::bit_cast<uint64_t>(data[2]), std::bit_cast<uint64_t>(kPayloadNaN));

    state.set_be_exec_version(NORMALIZE_FLOAT_HASH_KEY_VERSION - 1);
    channels = partition(partitioner, &state, block);
    ASSERT_EQ(channels.size(), 6);
    EXPECT_EQ(channels[0], legacy_channel(-0.0));
    EXPECT_EQ(channels[1], legacy_channel(0.0));
    EXPECT_EQ(channels[2], legacy_channel(kPayloadNaN));
    EXPECT_EQ(channels[3], legacy_channel(kQuietNaN));
    EXPECT_NE(legacy_channel(-0.0), legacy_channel(0.0));
}

// Nested float leaves go through the same normalized copy, so the legacy nested CRC path
// (ColumnVector::update_crc_with_value_without_null) no longer separates array(-0.0) from
// array(+0.0).
TEST(PartitionerFloatKeyTest, NestedFloatKeyIsNormalized) {
    auto array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>()));
    Crc32HashPartitioner<ShuffleChannelIds> partitioner(kPartitions);
    partitioner._partition_expr_ctxs = MockSlotRef::create_mock_contexts(DataTypes {array_type});
    MockRuntimeState state;

    auto column = array_type->create_column();
    for (double value : {-0.0, 0.0, kPayloadNaN, kQuietNaN}) {
        Array array;
        array.push_back(Field::create_field<TYPE_DOUBLE>(value));
        array.push_back(Field::create_field<TYPE_DOUBLE>(2.0));
        column->insert(Field::create_field<TYPE_ARRAY>(array));
    }
    Block block({ColumnWithTypeAndName(std::move(column), array_type, "k")});

    state.set_be_exec_version(NORMALIZE_FLOAT_HASH_KEY_VERSION);
    auto channels = partition(partitioner, &state, block);
    ASSERT_EQ(channels.size(), 4);
    EXPECT_EQ(channels[0], channels[1]);
    EXPECT_EQ(channels[2], channels[3]);
    const auto& leaves = assert_cast<const ColumnFloat64&>(
            assert_cast<const ColumnNullable&>(
                    assert_cast<const ColumnArray&>(*block.get_by_position(0).column).get_data())
                    .get_nested_column());
    EXPECT_TRUE(std::signbit(leaves.get_data()[0]));
}

// Every instantiation shares the gated body: the crc32c exchange partitioner (production
// default), the spill partitioner and FLOAT keys must all agree with their own legacy channel
// of +0.0 / quiet NaN once the version is raised, and keep the legacy channel of ordinary values.
template <typename Partitioner, typename DataType>
void check_float_key_partitioner() {
    using FieldType = typename DataType::FieldType;
    const FieldType quiet_nan = std::numeric_limits<FieldType>::quiet_NaN();
    const FieldType payload_nan = [&] {
        if constexpr (std::is_same_v<FieldType, float>) {
            return std::bit_cast<float>(std::bit_cast<uint32_t>(quiet_nan) | 0x12U);
        } else {
            return std::bit_cast<double>(std::bit_cast<uint64_t>(quiet_nan) | 0x1234ULL);
        }
    }();
    Partitioner partitioner(kPartitions);
    partitioner._partition_expr_ctxs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataType>()});
    MockRuntimeState state;
    Block block =
            ColumnHelper::create_block<DataType>({FieldType(-0.0), FieldType(0.0), payload_nan,
                                                  quiet_nan, FieldType(1.5), FieldType(-1.5)});

    state.set_be_exec_version(NORMALIZE_FLOAT_HASH_KEY_VERSION - 1);
    EXPECT_TRUE(partitioner.do_partitioning(&state, &block).ok());
    const std::vector<uint32_t> legacy = partitioner.get_channel_ids();
    ASSERT_EQ(legacy.size(), 6);

    state.set_be_exec_version(NORMALIZE_FLOAT_HASH_KEY_VERSION);
    EXPECT_TRUE(partitioner.do_partitioning(&state, &block).ok());
    const std::vector<uint32_t> normalized = partitioner.get_channel_ids();
    ASSERT_EQ(normalized.size(), 6);

    // The legacy convention really separated the special values in this instantiation.
    EXPECT_NE(legacy[0], legacy[1]);
    EXPECT_NE(legacy[2], legacy[3]);
    EXPECT_EQ(normalized[0], legacy[1]); // -0.0 now routes like +0.0
    EXPECT_EQ(normalized[1], legacy[1]);
    EXPECT_EQ(normalized[2], legacy[3]); // payload NaN now routes like quiet NaN
    EXPECT_EQ(normalized[3], legacy[3]);
    EXPECT_EQ(normalized[4], legacy[4]);
    EXPECT_EQ(normalized[5], legacy[5]);
    ASSERT_EQ(block.columns(), 1);
    EXPECT_TRUE(std::signbit(
            assert_cast<const typename DataType::ColumnType&>(*block.get_by_position(0).column)
                    .get_data()[0]));
}

TEST(PartitionerFloatKeyTest, EveryInstantiationAndFloatWidth) {
    check_float_key_partitioner<Crc32HashPartitioner<ShuffleChannelIds>, DataTypeFloat32>();
    check_float_key_partitioner<Crc32HashPartitioner<SpillPartitionChannelIds>, DataTypeFloat64>();
    check_float_key_partitioner<Crc32HashPartitioner<SpillRePartitionChannelIds>,
                                DataTypeFloat32>();
    check_float_key_partitioner<Crc32CHashPartitioner, DataTypeFloat64>();
    check_float_key_partitioner<Crc32CHashPartitioner, DataTypeFloat32>();
}

} // namespace doris
