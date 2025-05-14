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

#include "pipeline/dependency.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::pipeline {

class MaterializationSharedStateTest : public testing::Test {
protected:
    void SetUp() override {
        _shared_state = std::make_shared<MaterializationSharedState>();

        // Setup test data types
        _string_type = std::make_shared<vectorized::DataTypeString>();
        _int_type = std::make_shared<vectorized::DataTypeInt32>();

        // Create origin block with rowid column (ColumnString type)
        _shared_state->origin_block = vectorized::Block();
        _shared_state->origin_block.insert({_string_type->create_column(), _string_type, "rowid"});
        _shared_state->origin_block.insert({_int_type->create_column(), _int_type, "value"});

        // Add rowid location
        _shared_state->rowid_locs = {0}; // First column is rowid

        // Setup RPC structs for two backends
        _backend_id1 = 1001;
        _backend_id2 = 1002;
        _shared_state->rpc_struct_map[_backend_id1] = FetchRpcStruct();
        _shared_state->rpc_struct_map[_backend_id2] = FetchRpcStruct();
        _shared_state->rpc_struct_map[_backend_id1].request.add_request_block_descs();
        _shared_state->rpc_struct_map[_backend_id2].request.add_request_block_descs();
    }

    std::shared_ptr<MaterializationSharedState> _shared_state;
    std::shared_ptr<vectorized::DataTypeString> _string_type;
    std::shared_ptr<vectorized::DataTypeInt32> _int_type;
    int64_t _backend_id1;
    int64_t _backend_id2;
};

TEST_F(MaterializationSharedStateTest, TestCreateSourceDependency) {
    // Test creating source dependencies
    int test_op_id = 100;
    int test_node_id = 200;
    std::string test_name = "TEST";

    auto* dep = _shared_state->create_source_dependency(test_op_id, test_node_id, test_name);

    // Verify the dependency was created correctly
    ASSERT_NE(dep, nullptr);
    EXPECT_EQ(dep->id(), test_op_id);
    EXPECT_EQ(dep->name(), test_name + "_DEPENDENCY");

    // Verify it was added to source_deps
    EXPECT_EQ(_shared_state->source_deps.size(), 1);
    EXPECT_EQ(_shared_state->source_deps[0].get(), dep);
}

TEST_F(MaterializationSharedStateTest, TestCreateMultiGetResult) {
    // Create test columns for rowids
    vectorized::Columns columns;
    auto rowid_col = _string_type->create_column();
    auto* col_data = reinterpret_cast<vectorized::ColumnString*>(rowid_col.get());

    // Create test GlobalRowLoacationV2 data
    GlobalRowLoacationV2 loc1(0, _backend_id1, 1, 1);
    GlobalRowLoacationV2 loc2(0, _backend_id2, 2, 2);

    col_data->insert_data(reinterpret_cast<const char*>(&loc1), sizeof(GlobalRowLoacationV2));
    col_data->insert_data(reinterpret_cast<const char*>(&loc2), sizeof(GlobalRowLoacationV2));
    columns.push_back(std::move(rowid_col));

    // Test creating multiget result
    Status st = _shared_state->create_muiltget_result(columns, true, true);
    EXPECT_TRUE(st.ok());

    // Verify block_order_results
    EXPECT_EQ(_shared_state->block_order_results.size(), columns.size());
    EXPECT_EQ(_shared_state->last_block, true);
}

TEST_F(MaterializationSharedStateTest, TestMergeMultiResponse) {
    // 1. Setup origin block with nullable rowid column
    auto nullable_rowid_col = vectorized::ColumnNullable::create(_string_type->create_column(),
                                                                 vectorized::ColumnUInt8::create());
    nullable_rowid_col->insert_data((char*)&nullable_rowid_col, 4);
    nullable_rowid_col->insert_data(nullptr, 4);
    nullable_rowid_col->insert_data((char*)&nullable_rowid_col, 4);

    auto value_col = _int_type->create_column();
    value_col->insert(100);
    value_col->insert(101);
    value_col->insert(200);

    // Add test data to origin block
    _shared_state->origin_block = vectorized::Block(
            {{std::move(nullable_rowid_col), vectorized::make_nullable(_string_type), "rowid"},
             {std::move(value_col), _int_type, "value"}});

    // Set rowid column location
    _shared_state->rowid_locs = {0};
    _shared_state->response_blocks = std::vector<vectorized::MutableBlock>(1);

    // 2. Setup response blocks from multiple backends
    // Backend 1's response
    {
        vectorized::Block resp_block1;
        auto resp_value_col1 = _int_type->create_column();
        auto* value_col_data1 =
                reinterpret_cast<vectorized::ColumnVector<int32_t>*>(resp_value_col1.get());
        value_col_data1->insert(100);
        value_col_data1->insert(101);
        resp_block1.insert(
                {make_nullable(std::move(resp_value_col1)), make_nullable(_int_type), "value"});

        auto callback1 = std::make_shared<doris::DummyBrpcCallback<PMultiGetResponseV2>>();
        callback1->response_.reset(new PMultiGetResponseV2());
        auto serialized_block = callback1->response_->add_blocks()->mutable_block();
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        auto s = resp_block1.serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       CompressionTypePB::LZ4);
        EXPECT_TRUE(s.ok());

        _shared_state->rpc_struct_map[_backend_id1].callback = callback1;
        // init the response blocks
        _shared_state->response_blocks[0] = resp_block1.clone_empty();
    }

    // Backend 2's response
    {
        vectorized::Block resp_block2;
        auto resp_value_col2 = _int_type->create_column();
        auto* value_col_data2 =
                reinterpret_cast<vectorized::ColumnVector<int32_t>*>(resp_value_col2.get());
        value_col_data2->insert(200);
        resp_block2.insert(
                {make_nullable(std::move(resp_value_col2)), make_nullable(_int_type), "value"});

        auto callback2 = std::make_shared<doris::DummyBrpcCallback<PMultiGetResponseV2>>();
        callback2->response_.reset(new PMultiGetResponseV2());
        auto serialized_block = callback2->response_->add_blocks()->mutable_block();

        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        auto s = resp_block2.serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       CompressionTypePB::LZ4);
        EXPECT_TRUE(s.ok());

        _shared_state->rpc_struct_map[_backend_id2].callback = callback2;
    }

    // 3. Setup block order results to control merge order
    _shared_state->block_order_results = {
            {_backend_id1, 0, _backend_id2} // First block order: BE1,BE1,BE2
    };

    // 4. Test merging responses
    vectorized::Block result_block;
    Status st = _shared_state->merge_multi_response(&result_block);
    EXPECT_TRUE(st.ok());

    // 5. Verify merged result
    EXPECT_EQ(result_block.columns(), 2); // Should have original rowid column and value column
    EXPECT_EQ(result_block.rows(), 3);    // Total 3 rows from both backends

    // Verify the value column data is merged in correct order
    auto* merged_value_col = result_block.get_by_position(0).column.get();
    EXPECT_EQ(*((int*)merged_value_col->get_data_at(0).data), 100); // First value from BE1
    EXPECT_EQ(merged_value_col->get_data_at(1).data,
              nullptr); // Second value from BE1, replace by null
    EXPECT_EQ(*((int*)merged_value_col->get_data_at(2).data), 200); // Third value from BE2
}

TEST_F(MaterializationSharedStateTest, TestMergeMultiResponseMultiBlocks) {
    // 1. Setup origin block with multiple nullable rowid columns
    auto nullable_rowid_col1 = vectorized::ColumnNullable::create(
            _string_type->create_column(), vectorized::ColumnUInt8::create());
    nullable_rowid_col1->insert_data((char*)&nullable_rowid_col1, 4);
    nullable_rowid_col1->insert_data(nullptr, 4);
    nullable_rowid_col1->insert_data((char*)&nullable_rowid_col1, 4);

    auto nullable_rowid_col2 = vectorized::ColumnNullable::create(
            _string_type->create_column(), vectorized::ColumnUInt8::create());
    nullable_rowid_col2->insert_data((char*)&nullable_rowid_col2, 4);
    nullable_rowid_col2->insert_data((char*)&nullable_rowid_col2, 4);
    nullable_rowid_col2->insert_data(nullptr, 4);

    auto value_col1 = _int_type->create_column();
    value_col1->insert(100);
    value_col1->insert(101);
    value_col1->insert(102);

    auto value_col2 = _int_type->create_column();
    value_col2->insert(200);
    value_col2->insert(201);
    value_col2->insert(202);

    // Add test data to origin block with multiple columns
    _shared_state->origin_block = vectorized::Block(
            {{std::move(nullable_rowid_col1), vectorized::make_nullable(_string_type), "rowid1"},
             {std::move(nullable_rowid_col2), vectorized::make_nullable(_string_type), "rowid2"},
             {std::move(value_col1), _int_type, "value1"},
             {std::move(value_col2), _int_type, "value2"}});

    // Set multiple rowid column locations
    _shared_state->rowid_locs = {0, 1};
    _shared_state->response_blocks = std::vector<vectorized::MutableBlock>(2);

    // 2. Setup response blocks from multiple backends for first rowid
    {
        vectorized::Block resp_block1;
        auto resp_value_col1 = _int_type->create_column();
        auto* value_col_data1 =
                reinterpret_cast<vectorized::ColumnVector<int32_t>*>(resp_value_col1.get());
        value_col_data1->insert(100);
        resp_block1.insert(
                {make_nullable(std::move(resp_value_col1)), make_nullable(_int_type), "value1"});

        auto callback1 = std::make_shared<doris::DummyBrpcCallback<PMultiGetResponseV2>>();
        callback1->response_.reset(new PMultiGetResponseV2());
        auto serialized_block = callback1->response_->add_blocks()->mutable_block();
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        auto s = resp_block1.serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       CompressionTypePB::LZ4);
        EXPECT_TRUE(s.ok());

        _shared_state->rpc_struct_map[_backend_id1].callback = callback1;
        _shared_state->response_blocks[0] = resp_block1.clone_empty();
    }

    // Backend 2's response for first rowid
    {
        vectorized::Block resp_block2;
        auto resp_value_col2 = _int_type->create_column();
        auto* value_col_data2 =
                reinterpret_cast<vectorized::ColumnVector<int32_t>*>(resp_value_col2.get());
        value_col_data2->insert(102);
        resp_block2.insert(
                {make_nullable(std::move(resp_value_col2)), make_nullable(_int_type), "value1"});

        auto callback2 = std::make_shared<doris::DummyBrpcCallback<PMultiGetResponseV2>>();
        callback2->response_.reset(new PMultiGetResponseV2());
        auto serialized_block = callback2->response_->add_blocks()->mutable_block();
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        auto s = resp_block2.serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       CompressionTypePB::LZ4);
        EXPECT_TRUE(s.ok());

        _shared_state->rpc_struct_map[_backend_id2].callback = callback2;
    }

    // Add second block responses for second rowid
    {
        vectorized::Block resp_block1;
        auto resp_value_col1 = _int_type->create_column();
        auto* value_col_data1 =
                reinterpret_cast<vectorized::ColumnVector<int32_t>*>(resp_value_col1.get());
        value_col_data1->insert(200);
        resp_block1.insert(
                {make_nullable(std::move(resp_value_col1)), make_nullable(_int_type), "value2"});

        auto serialized_block = _shared_state->rpc_struct_map[_backend_id1]
                                        .callback->response_->add_blocks()
                                        ->mutable_block();
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        auto s = resp_block1.serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       CompressionTypePB::LZ4);
        EXPECT_TRUE(s.ok());
        _shared_state->response_blocks[1] = resp_block1.clone_empty();
    }

    {
        vectorized::Block resp_block2;
        auto resp_value_col2 = _int_type->create_column();
        auto* value_col_data2 =
                reinterpret_cast<vectorized::ColumnVector<int32_t>*>(resp_value_col2.get());
        value_col_data2->insert(201);
        resp_block2.insert(
                {make_nullable(std::move(resp_value_col2)), make_nullable(_int_type), "value2"});

        auto serialized_block = _shared_state->rpc_struct_map[_backend_id2]
                                        .callback->response_->add_blocks()
                                        ->mutable_block();
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        auto s = resp_block2.serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       CompressionTypePB::LZ4);
        EXPECT_TRUE(s.ok());
    }

    // 3. Setup block order results for both rowids
    _shared_state->block_order_results = {
            {_backend_id1, 0, _backend_id2}, // First block order: BE1,null,BE2
            {_backend_id1, _backend_id2, 0}  // Second block order: BE1,BE2,null
    };

    // 4. Test merging responses
    vectorized::Block result_block;
    Status st = _shared_state->merge_multi_response(&result_block);
    EXPECT_TRUE(st.ok());

    // 5. Verify merged result
    EXPECT_EQ(result_block.columns(), 4); // Should have two rowid columns and two value columns
    EXPECT_EQ(result_block.rows(), 3);    // Total 3 rows from both backends

    // Verify the first value column data is merged in correct order
    auto* merged_value_col1 = result_block.get_by_position(0).column.get();
    EXPECT_EQ(*((int*)merged_value_col1->get_data_at(0).data), 100);
    EXPECT_EQ(merged_value_col1->get_data_at(1).data, nullptr);
    EXPECT_EQ(*((int*)merged_value_col1->get_data_at(2).data), 102);

    // Verify the second value column data is merged in correct order
    auto* merged_value_col2 = result_block.get_by_position(1).column.get();
    EXPECT_EQ(*((int*)merged_value_col2->get_data_at(0).data), 200);
    EXPECT_EQ(*((int*)merged_value_col2->get_data_at(1).data), 201);
    EXPECT_EQ(merged_value_col2->get_data_at(2).data, nullptr);
}

} // namespace doris::pipeline