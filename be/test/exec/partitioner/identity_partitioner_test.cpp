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

#include <array>
#include <memory>
#include <vector>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/value/decimalv2_value.h"
#include "core/value/ipv4_value.h"
#include "core/value/ipv6_value.h"
#include "core/value/vdatetime_value.h"
#include "exec/partitioner/partitioner.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/raw_value.h"

namespace doris {

// Unit tests for the BE-side identity reshuffle partitioner used by bucket-shuffle join when the
// target table is bucketed with distribution_hash_type = identity. It must interpret every value's
// canonical little-endian bytes as unsigned and compose multiple columns identically to FE pruning
// and BE tablet routing.
class IdentityPartitionerTest : public ::testing::Test {
protected:
    void SetUp() override {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_INT)
                                       .nullable(true)
                                       .column_name("c1")
                                       .column_pos(1)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_STRING)
                                       .nullable(false)
                                       .column_name("c2")
                                       .column_pos(2)
                                       .build());
        tuple_builder.build(&dtb);
        TDescriptorTable thrift_tbl = dtb.desc_tbl();

        DescriptorTbl* desc_tbl = nullptr;
        auto st = DescriptorTbl::create(&_pool, thrift_tbl, &desc_tbl);
        ASSERT_TRUE(st.ok()) << st.to_string();
        _state.set_desc_tbl(desc_tbl);

        _tuple_id = thrift_tbl.tupleDescriptors[0].id;
        _row_desc = std::make_unique<RowDescriptor>(*desc_tbl, std::vector<TTupleId> {_tuple_id});
        _slot_ids.push_back(thrift_tbl.slotDescriptors[0].id);
        _slot_ids.push_back(thrift_tbl.slotDescriptors[1].id);
    }

    TExpr make_slot_ref(size_t slot_index, PrimitiveType type, bool nullable) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::SLOT_REF);
        node.__set_num_children(0);
        TSlotRef slot_ref;
        slot_ref.__set_slot_id(_slot_ids[slot_index]);
        slot_ref.__set_tuple_id(_tuple_id);
        node.__set_slot_ref(slot_ref);
        TTypeDesc type_desc = create_type_desc(type);
        type_desc.__set_is_nullable(nullable);
        node.__set_type(type_desc);
        node.__set_is_nullable(nullable);
        TExpr expr;
        expr.nodes.emplace_back(std::move(node));
        return expr;
    }

    TExpr make_int_slot_ref() { return make_slot_ref(0, TYPE_INT, true); }

    TExpr make_string_slot_ref() { return make_slot_ref(1, TYPE_STRING, false); }

    template <typename Partitioner>
    std::vector<PartitionerBase::HashValType> run(int partition_count, Block block,
                                                  std::vector<TExpr> exprs) {
        Partitioner partitioner(partition_count);
        EXPECT_TRUE(partitioner.init(exprs).ok());
        EXPECT_TRUE(partitioner.prepare(&_state, *_row_desc).ok());
        EXPECT_TRUE(partitioner.open(&_state).ok());
        EXPECT_TRUE(partitioner.do_partitioning(&_state, &block).ok());
        return partitioner.get_channel_ids();
    }

    template <typename Partitioner>
    std::vector<PartitionerBase::HashValType> run(int partition_count, Block block) {
        return run<Partitioner>(partition_count, std::move(block), {make_int_slot_ref()});
    }

    ObjectPool _pool;
    MockRuntimeState _state;
    std::unique_ptr<RowDescriptor> _row_desc;
    TTupleId _tuple_id = 0;
    std::vector<TSlotId> _slot_ids;
};

// Positive integers retain value-modulo behavior; for a power-of-two bucket count, two's-complement
// unsigned bytes also place negative values in the same buckets as negative-safe signed modulo.
TEST_F(IdentityPartitionerTest, ChannelIsValueModBucketCount) {
    constexpr int n = 8;
    std::vector<int32_t> values = {3, 8, 100, 999, -1, -8};
    auto channels =
            run<IdentityHashPartitioner>(n, ColumnHelper::create_block<DataTypeInt32>(values));
    ASSERT_EQ(values.size(), channels.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(static_cast<PartitionerBase::HashValType>(((values[i] % n) + n) % n), channels[i])
                << "row " << i << " value " << values[i];
    }
}

// Canonical two's-complement bytes are unsigned, so negative values need no special branch.
TEST_F(IdentityPartitionerTest, NegativeValueUsesUnsignedBytes) {
    constexpr int n = 10;
    auto channels =
            run<IdentityHashPartitioner>(n, ColumnHelper::create_block<DataTypeInt32>({-1, -8}));
    ASSERT_EQ(2u, channels.size());
    EXPECT_EQ(5u, channels[0]); // UINT32_MAX % 10
    EXPECT_EQ(8u, channels[1]); // (UINT32_MAX - 7) % 10
}

TEST_F(IdentityPartitionerTest, SupportsMultipleTypedColumns) {
    constexpr int n = 257;
    auto block = ColumnHelper::create_block<DataTypeInt32>({1, 2});
    auto strings = ColumnHelper::create_block<DataTypeString>({"A", "BC"});
    block.insert(strings.get_by_position(0));
    auto channels = run<IdentityHashPartitioner>(n, std::move(block),
                                                 {make_int_slot_ref(), make_string_slot_ref()});
    ASSERT_EQ(2u, channels.size());
    EXPECT_EQ(64u, channels[0]); // (1 * 256 + 'A') % 257
    // unsigned_le("BC") = 0x4342; append it after uint32_le(2).
    EXPECT_EQ((2u * 256u * 256u + 0x4342u) % n, channels[1]);
}

// A null distribution value is represented by four zero bytes.
TEST_F(IdentityPartitionerTest, NullGoesToChannelZero) {
    constexpr int n = 8;
    // row 0 null -> 0; row 1 = 300 -> 300 % 8 = 4
    auto channels = run<IdentityHashPartitioner>(
            n, ColumnHelper::create_nullable_block<DataTypeInt32>({0, 300}, {1, 0}));
    ASSERT_EQ(2u, channels.size());
    EXPECT_EQ(0u, channels[0]);
    EXPECT_EQ(4u, channels[1]);
}

// Guard against the two branches being swapped: crc32 reshuffle must differ from identity for at
// least one row (crc32 does not collapse to value % n).
TEST_F(IdentityPartitionerTest, Crc32DiffersFromIdentity) {
    constexpr int n = 8;
    std::vector<int32_t> values = {3, 8, 100, 999, 5, 6, 7, 12};
    auto identity =
            run<IdentityHashPartitioner>(n, ColumnHelper::create_block<DataTypeInt32>(values));
    auto crc32 = run<Crc32HashPartitioner<ShuffleChannelIds>>(
            n, ColumnHelper::create_block<DataTypeInt32>(values));
    ASSERT_EQ(values.size(), identity.size());
    ASSERT_EQ(values.size(), crc32.size());
    bool differs = false;
    for (size_t i = 0; i < values.size(); i++) {
        if (identity[i] != crc32[i]) {
            differs = true;
            break;
        }
    }
    EXPECT_TRUE(differs);
}

TEST(IdentityHashTest, FixedWidthAndLegacyTypes) {
    constexpr uint32_t n = 257;
    auto hash_bytes = [](const void* value, size_t size, uint32_t seed = 0) {
        const auto* bytes = reinterpret_cast<const uint8_t*>(value);
        uint64_t remainder = seed;
        for (size_t i = size; i > 0; --i) {
            remainder = (remainder * 256 + bytes[i - 1]) % n;
        }
        return static_cast<uint32_t>(remainder);
    };

    std::array<uint8_t, 32> bytes {};
    bytes[0] = 0x34;
    bytes[1] = 0x12;
    EXPECT_EQ(hash_bytes(bytes.data(), 2),
              RawValue::identity_hash(bytes.data(), 2, TYPE_VARCHAR, 0, n));
    EXPECT_EQ(hash_bytes(bytes.data(), 1),
              RawValue::identity_hash(bytes.data(), bytes.size(), TYPE_BOOLEAN, 0, n));
    EXPECT_EQ(hash_bytes(bytes.data(), 2),
              RawValue::identity_hash(bytes.data(), bytes.size(), TYPE_SMALLINT, 0, n));
    EXPECT_EQ(hash_bytes(bytes.data(), 8),
              RawValue::identity_hash(bytes.data(), bytes.size(), TYPE_BIGINT, 0, n));
    EXPECT_EQ(hash_bytes(bytes.data(), 16),
              RawValue::identity_hash(bytes.data(), bytes.size(), TYPE_LARGEINT, 0, n));
    EXPECT_EQ(hash_bytes(bytes.data(), bytes.size()),
              RawValue::identity_hash(bytes.data(), bytes.size(), TYPE_DECIMAL256, 0, n));

    auto date = VecDateTimeValue::create_from_olap_date(20260102);
    char date_buffer[64];
    const int date_length = date.to_buffer(date_buffer);
    EXPECT_EQ(hash_bytes(date_buffer, date_length),
              RawValue::identity_hash(&date, sizeof(date), TYPE_DATE, 0, n));

    const DecimalV2Value decimal(123, 456000000);
    const int32_t fraction = decimal.frac_value();
    const int64_t integer = decimal.int_value();
    const uint32_t fraction_hash = hash_bytes(&fraction, sizeof(fraction));
    EXPECT_EQ(hash_bytes(&integer, sizeof(integer), fraction_hash),
              RawValue::identity_hash(&decimal, sizeof(decimal), TYPE_DECIMALV2, 0, n));
}

TEST(IdentityHashTest, IpCanonicalBytes) {
    constexpr uint32_t n = 257;
    IPv4 ipv4 = 0;
    ASSERT_TRUE(IPv4Value::from_string(ipv4, "1.2.3.4"));
    EXPECT_EQ(2u, RawValue::identity_hash(&ipv4, sizeof(ipv4), TYPE_IPV4, 0, n));

    IPv6 ipv6 = 0;
    ASSERT_TRUE(IPv6Value::from_string(ipv6, "::1"));
    EXPECT_EQ(1u, RawValue::identity_hash(&ipv6, sizeof(ipv6), TYPE_IPV6, 0, n));
}

} // namespace doris
