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

#include "exec/sink/external_table_sink_hash_partitioner.h"

#include <gen_cpp/Exprs_types.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class ExternalTableSinkHashPartitionerTest : public testing::Test {
protected:
    void SetUp() override {
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_INT)
                                       .nullable(false)
                                       .column_name("key")
                                       .column_pos(1)
                                       .build());
        tuple_builder.build(&table_builder);
        TDescriptorTable thrift_table = table_builder.desc_tbl();

        DescriptorTbl* descriptor_table = nullptr;
        ASSERT_TRUE(DescriptorTbl::create(&_pool, thrift_table, &descriptor_table).ok());
        _state.set_desc_tbl(descriptor_table);
        _tuple_id = thrift_table.tupleDescriptors[0].id;
        _slot_id = thrift_table.slotDescriptors[0].id;
        _row_descriptor = std::make_unique<RowDescriptor>(
                *descriptor_table, std::vector<TTupleId> {_tuple_id}, std::vector<bool> {false});
    }

    TExpr slot_ref() const {
        TExprNode node;
        node.__set_node_type(TExprNodeType::SLOT_REF);
        node.__set_num_children(0);
        node.__set_type(create_type_desc(TYPE_INT));
        node.__set_is_nullable(false);

        TSlotRef slot_ref;
        slot_ref.__set_slot_id(_slot_id);
        slot_ref.__set_tuple_id(_tuple_id);
        node.__set_slot_ref(slot_ref);

        TExpr expression;
        expression.nodes.emplace_back(std::move(node));
        return expression;
    }

    Block block(std::initializer_list<int32_t> values) const {
        auto column = ColumnInt32::create();
        for (int32_t value : values) {
            column->insert_value(value);
        }
        Block block;
        block.insert(
                ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeInt32>(), "key"));
        return block;
    }

    ObjectPool _pool;
    MockRuntimeState _state;
    std::unique_ptr<RowDescriptor> _row_descriptor;
    TTupleId _tuple_id = -1;
    TSlotId _slot_id = -1;
};

TEST_F(ExternalTableSinkHashPartitionerTest, DirectHashKeepsOneKeyOnOneWriter) {
    TExternalTableSinkHashPartitionInfo info;
    info.__set_algorithm(TExternalTableSinkHashAlgorithm::DIRECT_HASH);
    info.__set_writer_assignment(TExternalTableSinkWriterAssignment::IDENTITY);
    ExternalTableSinkHashPartitioner partitioner(8, false, info);
    ASSERT_TRUE(partitioner.init({slot_ref()}).ok());
    ASSERT_TRUE(partitioner.prepare(&_state, *_row_descriptor).ok());
    ASSERT_TRUE(partitioner.open(&_state).ok());

    Block input = block({7, 3, 7, 9, 3});
    ASSERT_TRUE(partitioner.do_partitioning(&_state, &input).ok());
    const auto& channels = partitioner.get_channel_ids();
    ASSERT_EQ(5, channels.size());
    EXPECT_EQ(channels[0], channels[2]);
    EXPECT_EQ(channels[1], channels[4]);
    EXPECT_EQ(1, input.columns());

    ASSERT_TRUE(partitioner.close(&_state).ok());
}

TEST_F(ExternalTableSinkHashPartitionerTest, MissingWriterAssignmentFailsClosed) {
    TExternalTableSinkHashPartitionInfo info;
    info.__set_algorithm(TExternalTableSinkHashAlgorithm::DIRECT_HASH);
    ExternalTableSinkHashPartitioner partitioner(8, false, info);

    Status status = partitioner.init({slot_ref()});
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("writer assignment is missing"), std::string::npos);
}

TEST_F(ExternalTableSinkHashPartitionerTest, DirectHashSupportsSkewedWriterAssignment) {
    TExternalTableSinkHashPartitionInfo info;
    info.__set_algorithm(TExternalTableSinkHashAlgorithm::DIRECT_HASH);
    info.__set_writer_assignment(TExternalTableSinkWriterAssignment::SKEWED);
    ExternalTableSinkHashPartitioner partitioner(4, false, info);
    ASSERT_TRUE(partitioner.init({slot_ref()}).ok());
    ASSERT_TRUE(partitioner.prepare(&_state, *_row_descriptor).ok());
    ASSERT_TRUE(partitioner.open(&_state).ok());

    Block input = block({7, 3, 7, 9, 3});
    ASSERT_TRUE(partitioner.do_partitioning(&_state, &input).ok());
    const auto& channels = partitioner.get_channel_ids();
    ASSERT_EQ(5, channels.size());
    EXPECT_EQ(channels[0], channels[2]);
    EXPECT_EQ(channels[1], channels[4]);
    for (uint32_t channel : channels) {
        EXPECT_LT(channel, 4);
    }

    ASSERT_TRUE(partitioner.close(&_state).ok());
}

TEST_F(ExternalTableSinkHashPartitionerTest, IcebergTransformHashesTransformedValue) {
    TExternalTableSinkHashPartitionInfo info;
    info.__set_algorithm(TExternalTableSinkHashAlgorithm::ICEBERG_TRANSFORM);
    info.__set_writer_assignment(TExternalTableSinkWriterAssignment::SKEWED);
    info.__set_partition_transforms({"truncate[10]"});
    ExternalTableSinkHashPartitioner partitioner(64, false, info);
    ASSERT_TRUE(partitioner.init({slot_ref()}).ok());
    ASSERT_TRUE(partitioner.prepare(&_state, *_row_descriptor).ok());
    ASSERT_TRUE(partitioner.open(&_state).ok());

    Block input = block({11, 19, 20, 29});
    ASSERT_TRUE(partitioner.do_partitioning(&_state, &input).ok());
    const auto& channels = partitioner.get_channel_ids();
    ASSERT_EQ(4, channels.size());
    EXPECT_EQ(channels[0], channels[1]);
    EXPECT_EQ(channels[2], channels[3]);
    EXPECT_EQ(1, input.columns());

    ASSERT_TRUE(partitioner.close(&_state).ok());
}

TEST_F(ExternalTableSinkHashPartitionerTest, UnsupportedTransformFailsClosed) {
    TExternalTableSinkHashPartitionInfo info;
    info.__set_algorithm(TExternalTableSinkHashAlgorithm::ICEBERG_TRANSFORM);
    info.__set_writer_assignment(TExternalTableSinkWriterAssignment::IDENTITY);
    info.__set_partition_transforms({"unsupported"});
    ExternalTableSinkHashPartitioner partitioner(4, false, info);
    ASSERT_TRUE(partitioner.init({slot_ref()}).ok());
    ASSERT_TRUE(partitioner.prepare(&_state, *_row_descriptor).ok());
    EXPECT_FALSE(partitioner.open(&_state).ok());
}

} // namespace doris
