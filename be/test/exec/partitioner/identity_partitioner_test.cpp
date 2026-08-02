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

#include <memory>
#include <vector>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/data_type/primitive_type.h"
#include "exec/partitioner/partitioner.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

// Unit tests for the BE-side identity reshuffle partitioner used by bucket-shuffle join when the
// target table is bucketed with distribution_hash_type = identity. It must place a row on the
// channel matching the row's storage bucket: bucket = ((int128(v) % n) + n) % n, NULL -> 0, so it
// stays bit-identical with FE HashDistributionPruner and BE tablet_info's identity tablet index.
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
        tuple_builder.build(&dtb);
        TDescriptorTable thrift_tbl = dtb.desc_tbl();

        DescriptorTbl* desc_tbl = nullptr;
        auto st = DescriptorTbl::create(&_pool, thrift_tbl, &desc_tbl);
        ASSERT_TRUE(st.ok()) << st.to_string();
        _state.set_desc_tbl(desc_tbl);

        _tuple_id = thrift_tbl.tupleDescriptors[0].id;
        _row_desc = std::make_unique<RowDescriptor>(*desc_tbl, std::vector<TTupleId> {_tuple_id});
        _slot_id = thrift_tbl.slotDescriptors[0].id;
    }

    TExpr make_int_slot_ref() {
        TExprNode node;
        node.__set_node_type(TExprNodeType::SLOT_REF);
        node.__set_num_children(0);
        TSlotRef slot_ref;
        slot_ref.__set_slot_id(_slot_id);
        slot_ref.__set_tuple_id(_tuple_id);
        node.__set_slot_ref(slot_ref);
        TTypeDesc type_desc = create_type_desc(TYPE_INT);
        type_desc.__set_is_nullable(true);
        node.__set_type(type_desc);
        node.__set_is_nullable(true);
        TExpr expr;
        expr.nodes.emplace_back(std::move(node));
        return expr;
    }

    template <typename Partitioner>
    std::vector<PartitionerBase::HashValType> run(int partition_count, Block block) {
        Partitioner partitioner(partition_count);
        EXPECT_TRUE(partitioner.init({make_int_slot_ref()}).ok());
        EXPECT_TRUE(partitioner.prepare(&_state, *_row_desc).ok());
        EXPECT_TRUE(partitioner.open(&_state).ok());
        EXPECT_TRUE(partitioner.do_partitioning(&_state, &block).ok());
        return partitioner.get_channel_ids();
    }

    ObjectPool _pool;
    MockRuntimeState _state;
    std::unique_ptr<RowDescriptor> _row_desc;
    TTupleId _tuple_id = 0;
    TSlotId _slot_id = -1;
};

// bucket = ((v % n) + n) % n; negatives and out-of-range values wrap the same way BE find_tablets
// and FE pruning compute them.
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

// A null distribution value lands on channel 0, matching the storage bucket rule.
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

} // namespace doris
