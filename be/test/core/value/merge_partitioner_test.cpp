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

#include "vec/runtime/merge_partitioner.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Partitions_types.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized {

namespace {

template <typename T>
class ScopedConfigValue {
public:
    ScopedConfigValue(T& ref, T value) : _ref(ref), _old(ref) { _ref = value; }
    ~ScopedConfigValue() { _ref = _old; }

private:
    T& _ref;
    T _old;
};

} // namespace

class MergePartitionerTest : public ::testing::Test {
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

    TMergePartitionInfo _make_base_merge_info(bool insert_random) {
        TMergePartitionInfo merge_info;
        merge_info.__set_operation_expr(
                _make_slot_ref_expr(_operation_slot_id, _tuple_id, TYPE_TINYINT));
        merge_info.__set_insert_random(insert_random);
        return merge_info;
    }

    Block _build_block(const std::vector<int8_t>& ops, const std::vector<std::string>& file_paths,
                       const std::vector<int64_t>& row_positions,
                       const std::vector<int32_t>& insert_keys,
                       const std::vector<std::string>& delete_keys) {
        const size_t rows = ops.size();
        EXPECT_EQ(rows, file_paths.size());
        EXPECT_EQ(rows, row_positions.size());
        EXPECT_EQ(rows, insert_keys.size());
        EXPECT_EQ(rows, delete_keys.size());

        Block block;

        auto op_col = ColumnInt8::create();
        for (auto op : ops) {
            op_col->insert_value(op);
        }
        block.insert(ColumnWithTypeAndName(std::move(op_col), std::make_shared<DataTypeInt8>(),
                                           "operation"));

        auto file_path_col = ColumnString::create();
        auto row_pos_col = ColumnInt64::create();
        for (size_t i = 0; i < rows; ++i) {
            file_path_col->insert_data(file_paths[i].data(), file_paths[i].size());
            row_pos_col->insert_value(row_positions[i]);
        }
        Columns struct_cols;
        struct_cols.push_back(std::move(file_path_col));
        struct_cols.push_back(std::move(row_pos_col));

        auto struct_col = ColumnStruct::create(std::move(struct_cols));
        DataTypes struct_types {std::make_shared<DataTypeString>(),
                                std::make_shared<DataTypeInt64>()};
        Strings field_names {"file_path", "row_position"};
        auto struct_type = std::make_shared<DataTypeStruct>(struct_types, field_names);

        block.insert(ColumnWithTypeAndName(std::move(struct_col), struct_type, "row_id"));

        auto insert_col = ColumnInt32::create();
        for (auto key : insert_keys) {
            insert_col->insert_value(key);
        }
        block.insert(ColumnWithTypeAndName(std::move(insert_col), std::make_shared<DataTypeInt32>(),
                                           "insert_key"));

        auto delete_col = ColumnString::create();
        for (const auto& key : delete_keys) {
            delete_col->insert_data(key.data(), key.size());
        }
        block.insert(ColumnWithTypeAndName(std::move(delete_col),
                                           std::make_shared<DataTypeString>(), "delete_key"));

        return block;
    }

    const ColumnInt8& _get_op_column(const Block& block) const {
        const auto& column = block.get_by_position(0).column;
        if (auto* nullable_col = check_and_get_column<ColumnNullable>(column.get())) {
            return *check_and_get_column<ColumnInt8>(nullable_col->get_nested_column_ptr().get());
        }
        return *check_and_get_column<ColumnInt8>(column.get());
    }

    void _build_descriptors() {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_TINYINT)
                                       .nullable(false)
                                       .column_name("operation")
                                       .column_pos(1)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_STRING)
                                       .nullable(false)
                                       .column_name("row_id")
                                       .column_pos(2)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_INT)
                                       .nullable(false)
                                       .column_name("insert_key")
                                       .column_pos(3)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_STRING)
                                       .nullable(false)
                                       .column_name("delete_key")
                                       .column_pos(4)
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

        auto find_slot_id = [&](const std::string& name) -> TSlotId {
            for (const auto& slot : thrift_tbl.slotDescriptors) {
                if (slot.colName == name) {
                    return slot.id;
                }
            }
            return -1;
        };
        _operation_slot_id = find_slot_id("operation");
        _row_id_slot_id = find_slot_id("row_id");
        _insert_key_slot_id = find_slot_id("insert_key");
        _delete_key_slot_id = find_slot_id("delete_key");

        ASSERT_GE(_operation_slot_id, 0);
        ASSERT_GE(_row_id_slot_id, 0);
        ASSERT_GE(_insert_key_slot_id, 0);
        ASSERT_GE(_delete_key_slot_id, 0);
    }

    ObjectPool _pool;
    MockRuntimeState _state;
    std::unique_ptr<RowDescriptor> _row_desc;
    TTupleId _tuple_id = 0;
    TSlotId _operation_slot_id = -1;
    TSlotId _row_id_slot_id = -1;
    TSlotId _insert_key_slot_id = -1;
    TSlotId _delete_key_slot_id = -1;
};

TEST_F(MergePartitionerTest, TestInsertDeleteUpdatePartitioning) {
    TMergePartitionInfo merge_info = _make_base_merge_info(false);
    merge_info.__set_insert_partition_exprs(
            {_make_slot_ref_expr(_insert_key_slot_id, _tuple_id, TYPE_INT)});
    merge_info.__set_delete_partition_exprs(
            {_make_slot_ref_expr(_row_id_slot_id, _tuple_id, TYPE_STRING)});

    MergePartitioner partitioner(4, merge_info, false);
    ASSERT_TRUE(partitioner.init({}).ok());
    ASSERT_TRUE(partitioner.prepare(&_state, *_row_desc).ok());
    ASSERT_TRUE(partitioner.open(&_state).ok());

    Block block =
            _build_block({3, 1, 1, 2, 2}, {"path_u", "path_i1", "path_i2", "path_d", "path_d"},
                         {10, 20, 21, 30, 31}, {100, 42, 42, 7, 8}, {"d1", "d2", "d3", "d4", "d4"});

    ASSERT_TRUE(partitioner.do_partitioning(&_state, &block).ok());

    const auto& channel_ids = partitioner.get_channel_ids();
    ASSERT_EQ(block.rows(), channel_ids.size());
    EXPECT_EQ(6, block.rows());

    const auto& op_values = _get_op_column(block).get_data();
    ASSERT_EQ(6, op_values.size());
    EXPECT_EQ(5, op_values[0]);
    EXPECT_EQ(1, op_values[1]);
    EXPECT_EQ(1, op_values[2]);
    EXPECT_EQ(2, op_values[3]);
    EXPECT_EQ(2, op_values[4]);
    EXPECT_EQ(4, op_values[5]);

    EXPECT_EQ(channel_ids[1], channel_ids[2]);
    EXPECT_EQ(channel_ids[3], channel_ids[4]);

    ASSERT_TRUE(partitioner.close(&_state).ok());
}

TEST_F(MergePartitionerTest, TestMissingInsertPartitionExprs) {
    TMergePartitionInfo merge_info = _make_base_merge_info(false);

    MergePartitioner partitioner(4, merge_info, false);
    ASSERT_TRUE(partitioner.init({}).ok());
    ASSERT_TRUE(partitioner.prepare(&_state, *_row_desc).ok());
    ASSERT_TRUE(partitioner.open(&_state).ok());

    Block block = _build_block({1}, {"path_i"}, {1}, {100}, {"d1"});
    Status status = partitioner.do_partitioning(&_state, &block);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("insert exprs are empty"));

    ASSERT_TRUE(partitioner.close(&_state).ok());
}

TEST_F(MergePartitionerTest, TestMissingDeletePartitionExprs) {
    TMergePartitionInfo merge_info = _make_base_merge_info(true);

    MergePartitioner partitioner(4, merge_info, false);
    ASSERT_TRUE(partitioner.init({}).ok());
    ASSERT_TRUE(partitioner.prepare(&_state, *_row_desc).ok());
    ASSERT_TRUE(partitioner.open(&_state).ok());

    Block block = _build_block({2}, {"path_d"}, {10}, {1}, {"d1"});
    Status status = partitioner.do_partitioning(&_state, &block);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("delete exprs are empty"));

    ASSERT_TRUE(partitioner.close(&_state).ok());
}

TEST_F(MergePartitionerTest, TestInsertPartitionFieldsIdentity) {
    ScopedConfigValue<int32_t> max_partition_guard(
            config::table_sink_partition_write_max_partition_nums_per_writer, 0);

    TMergePartitionInfo merge_info = _make_base_merge_info(false);
    TIcebergPartitionField field;
    field.__set_transform("identity");
    field.__set_source_expr(_make_slot_ref_expr(_insert_key_slot_id, _tuple_id, TYPE_INT));
    field.__set_name("id");
    field.__set_source_id(1);
    merge_info.__set_insert_partition_fields({field});

    MergePartitioner partitioner(4, merge_info, false);
    ASSERT_TRUE(partitioner.init({}).ok());
    ASSERT_TRUE(partitioner.prepare(&_state, *_row_desc).ok());
    ASSERT_TRUE(partitioner.open(&_state).ok());

    Block block = _build_block({1, 1}, {"path_i1", "path_i2"}, {1, 2}, {9, 9}, {"d1", "d2"});
    ASSERT_TRUE(partitioner.do_partitioning(&_state, &block).ok());

    const auto& channel_ids = partitioner.get_channel_ids();
    ASSERT_EQ(2, channel_ids.size());
    EXPECT_EQ(channel_ids[0], channel_ids[1]);

    ASSERT_TRUE(partitioner.close(&_state).ok());
}

TEST_F(MergePartitionerTest, TestInvalidTransformFallbacksToRandom) {
    ScopedConfigValue<int64_t> threshold_guard(
            config::table_sink_non_partition_write_scaling_data_processed_threshold, 0);

    TMergePartitionInfo merge_info = _make_base_merge_info(false);
    TIcebergPartitionField field;
    field.__set_transform("invalid");
    field.__set_source_expr(_make_slot_ref_expr(_insert_key_slot_id, _tuple_id, TYPE_INT));
    field.__set_name("id");
    field.__set_source_id(1);
    merge_info.__set_insert_partition_fields({field});

    MergePartitioner partitioner(3, merge_info, false);
    ASSERT_TRUE(partitioner.init({}).ok());
    ASSERT_TRUE(partitioner.prepare(&_state, *_row_desc).ok());
    ASSERT_TRUE(partitioner.open(&_state).ok());

    Block block = _build_block({1, 1, 1, 1}, {"path_1", "path_2", "path_3", "path_4"}, {1, 2, 3, 4},
                               {10, 11, 12, 13}, {"d1", "d2", "d3", "d4"});
    ASSERT_TRUE(partitioner.do_partitioning(&_state, &block).ok());

    const auto& channel_ids = partitioner.get_channel_ids();
    ASSERT_EQ(4, channel_ids.size());
    EXPECT_EQ(0U, channel_ids[0]);
    EXPECT_EQ(1U, channel_ids[1]);
    EXPECT_EQ(2U, channel_ids[2]);
    EXPECT_EQ(0U, channel_ids[3]);

    ASSERT_TRUE(partitioner.close(&_state).ok());
}

} // namespace doris::vectorized
