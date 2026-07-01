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

#include "exec/partitioner/paimon_fixed_bucket_partitioner.h"

#include <gen_cpp/Exprs_types.h>
#include <gtest/gtest.h>

#include <cstdint>
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

class PaimonFixedBucketPartitionerTest : public ::testing::Test {
protected:
    void SetUp() override { _build_descriptors(); }

    TExpr _make_slot_ref_expr(TSlotId slot_id, TTupleId tuple_id, PrimitiveType type) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::SLOT_REF);
        node.__set_num_children(0);

        TSlotRef slot_ref;
        slot_ref.__set_slot_id(slot_id);
        slot_ref.__set_tuple_id(tuple_id);
        node.__set_slot_ref(slot_ref);

        TTypeDesc type_desc = create_type_desc(type);
        type_desc.__set_is_nullable(false);
        node.__set_type(type_desc);
        node.__set_is_nullable(false);

        TExpr expr;
        expr.nodes.emplace_back(std::move(node));
        return expr;
    }

    TPaimonRouteBucketInfo _make_route_info(int32_t bucket_num,
                                            TPaimonBucketFunctionType::type function_type,
                                            std::vector<TExpr> bucket_key_exprs) {
        TPaimonRouteBucketInfo info;
        info.__set_bucket_num(bucket_num);
        info.__set_bucket_function_type(function_type);
        info.__set_bucket_key_exprs(std::move(bucket_key_exprs));
        return info;
    }

    Block _build_int_bigint_block(const std::vector<int32_t>& int_values,
                                  const std::vector<int64_t>& bigint_values) {
        EXPECT_EQ(int_values.size(), bigint_values.size());
        auto int_col = ColumnInt32::create();
        auto bigint_col = ColumnInt64::create();
        for (size_t i = 0; i < int_values.size(); ++i) {
            int_col->insert_value(int_values[i]);
            bigint_col->insert_value(bigint_values[i]);
        }

        Block block;
        block.insert(ColumnWithTypeAndName(std::move(int_col), std::make_shared<DataTypeInt32>(),
                                           "int_key"));
        block.insert(ColumnWithTypeAndName(std::move(bigint_col), std::make_shared<DataTypeInt64>(),
                                           "bigint_key"));
        return block;
    }

    void _build_descriptors() {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_INT)
                                       .nullable(false)
                                       .column_name("int_key")
                                       .column_pos(1)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_BIGINT)
                                       .nullable(false)
                                       .column_name("bigint_key")
                                       .column_pos(2)
                                       .build());
        tuple_builder.build(&dtb);
        TDescriptorTable thrift_tbl = dtb.desc_tbl();

        DescriptorTbl* desc_tbl = nullptr;
        auto st = DescriptorTbl::create(&_pool, thrift_tbl, &desc_tbl);
        ASSERT_TRUE(st.ok()) << st.to_string();
        _state.set_desc_tbl(desc_tbl);

        ASSERT_FALSE(thrift_tbl.tupleDescriptors.empty());
        _tuple_id = thrift_tbl.tupleDescriptors[0].id;
        _row_desc = std::make_unique<RowDescriptor>(*desc_tbl, std::vector<TTupleId> {_tuple_id});

        for (const auto& slot : thrift_tbl.slotDescriptors) {
            if (slot.colName == "int_key") {
                _int_slot_id = slot.id;
            } else if (slot.colName == "bigint_key") {
                _bigint_slot_id = slot.id;
            }
        }
        ASSERT_GE(_int_slot_id, 0);
        ASSERT_GE(_bigint_slot_id, 0);
    }

    ObjectPool _pool;
    MockRuntimeState _state;
    std::unique_ptr<RowDescriptor> _row_desc;
    TTupleId _tuple_id = 0;
    TSlotId _int_slot_id = -1;
    TSlotId _bigint_slot_id = -1;
};

TEST_F(PaimonFixedBucketPartitionerTest, DefaultBucketGoldenValues) {
    TPaimonRouteBucketInfo route_info =
            _make_route_info(11, TPaimonBucketFunctionType::DEFAULT,
                             {_make_slot_ref_expr(_int_slot_id, _tuple_id, TYPE_INT),
                              _make_slot_ref_expr(_bigint_slot_id, _tuple_id, TYPE_BIGINT)});
    PaimonFixedBucketPartitioner partitioner(4, route_info);
    ASSERT_TRUE(partitioner.init({}).ok());
    ASSERT_TRUE(partitioner.prepare(&_state, *_row_desc).ok());
    ASSERT_TRUE(partitioner.open(&_state).ok());

    Block block = _build_int_bigint_block({17}, {1234567890123L});
    std::vector<int32_t> bucket_ids;
    ASSERT_TRUE(partitioner.compute_bucket_ids_for_test(&block, block.rows(), bucket_ids).ok());

    ASSERT_EQ(std::vector<int32_t>({5}), bucket_ids);
}

TEST_F(PaimonFixedBucketPartitionerTest, ModBucketGoldenValues) {
    TPaimonRouteBucketInfo route_info =
            _make_route_info(5, TPaimonBucketFunctionType::MOD,
                             {_make_slot_ref_expr(_bigint_slot_id, _tuple_id, TYPE_BIGINT)});
    PaimonFixedBucketPartitioner partitioner(4, route_info);
    ASSERT_TRUE(partitioner.init({}).ok());
    ASSERT_TRUE(partitioner.prepare(&_state, *_row_desc).ok());
    ASSERT_TRUE(partitioner.open(&_state).ok());

    Block block = _build_int_bigint_block({0, 0}, {17, -3});
    std::vector<int32_t> bucket_ids;
    ASSERT_TRUE(partitioner.compute_bucket_ids_for_test(&block, block.rows(), bucket_ids).ok());

    ASSERT_EQ((std::vector<int32_t> {2, 2}), bucket_ids);
}

TEST_F(PaimonFixedBucketPartitionerTest, SamePartitionAndBucketRouteToSameChannel) {
    TPaimonRouteBucketInfo route_info =
            _make_route_info(5, TPaimonBucketFunctionType::MOD,
                             {_make_slot_ref_expr(_bigint_slot_id, _tuple_id, TYPE_BIGINT)});
    PaimonFixedBucketPartitioner partitioner(8, route_info);
    ASSERT_TRUE(partitioner.init({_make_slot_ref_expr(_int_slot_id, _tuple_id, TYPE_INT)}).ok());
    ASSERT_TRUE(partitioner.prepare(&_state, *_row_desc).ok());
    ASSERT_TRUE(partitioner.open(&_state).ok());

    Block block = _build_int_bigint_block({1, 1, 2}, {17, -3, 17});
    ASSERT_TRUE(partitioner.do_partitioning(&_state, &block).ok());

    const auto& channel_ids = partitioner.get_channel_ids();
    ASSERT_EQ(3, channel_ids.size());
    ASSERT_EQ(channel_ids[0], channel_ids[1]);
}

} // namespace doris
