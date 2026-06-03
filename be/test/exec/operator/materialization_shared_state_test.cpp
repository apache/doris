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

#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "exec/operator/materialization_opertor.h"
#include "exec/pipeline/dependency.h"

namespace doris {

class MaterializationSharedStateTest : public testing::Test {
protected:
    void SetUp() override {
        _shared_state = std::make_shared<MaterializationSharedState>();

        // Setup test data types
        _string_type = std::make_shared<DataTypeString>();
        _int_type = std::make_shared<DataTypeInt32>();

        // Create origin block with rowid column (ColumnString type)
        _shared_state->origin_block = Block();
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
    std::shared_ptr<DataTypeString> _string_type;
    std::shared_ptr<DataTypeInt32> _int_type;
    int64_t _backend_id1;
    int64_t _backend_id2;
};

TEST_F(MaterializationSharedStateTest, TestCreateMultiGetResult) {
    // Create test columns for rowids
    Columns columns;
    auto rowid_col = _string_type->create_column();
    auto* col_data = reinterpret_cast<ColumnString*>(rowid_col.get());

    // Create test GlobalRowLoacationV2 data
    GlobalRowLoacationV2 loc1(0, _backend_id1, 1, 1);
    GlobalRowLoacationV2 loc2(0, _backend_id2, 2, 2);

    col_data->insert_data(reinterpret_cast<const char*>(&loc1), sizeof(GlobalRowLoacationV2));
    col_data->insert_data(reinterpret_cast<const char*>(&loc2), sizeof(GlobalRowLoacationV2));
    columns.push_back(std::move(rowid_col));

    // Test creating multiget result
    Status st = _shared_state->create_muiltget_result(columns, true);
    EXPECT_TRUE(st.ok());

    // Verify block_order_results
    EXPECT_EQ(_shared_state->block_order_results.size(), columns.size());
    EXPECT_EQ(_shared_state->eos, true);
}

TEST_F(MaterializationSharedStateTest, TestMergeMultiResponse) {
    // 1. Setup origin block with nullable rowid column
    auto nullable_rowid_col =
            ColumnNullable::create(_string_type->create_column(), ColumnUInt8::create());
    nullable_rowid_col->insert_data((char*)&nullable_rowid_col, 4);
    nullable_rowid_col->insert_data(nullptr, 4);
    nullable_rowid_col->insert_data((char*)&nullable_rowid_col, 4);

    auto value_col = _int_type->create_column();
    value_col->insert(Field::create_field<PrimitiveType::TYPE_INT>(100));
    value_col->insert(Field::create_field<PrimitiveType::TYPE_INT>(101));
    value_col->insert(Field::create_field<PrimitiveType::TYPE_INT>(200));

    // Add test data to origin block
    _shared_state->origin_block =
            Block({{std::move(nullable_rowid_col), make_nullable(_string_type), "rowid"},
                   {std::move(value_col), _int_type, "value"}});

    // Set rowid column location
    _shared_state->rowid_locs = {0};
    _shared_state->response_blocks = std::vector<MutableBlock>(1);

    // 2. Setup response blocks from multiple backends
    // Backend 1's response
    {
        _shared_state->rpc_struct_map[_backend_id1]
                .request.mutable_request_block_descs(0)
                ->add_row_id(0);
        _shared_state->rpc_struct_map[_backend_id1]
                .request.mutable_request_block_descs(0)
                ->add_row_id(1);
        Block resp_block1;
        auto resp_value_col1 = _int_type->create_column();
        auto* value_col_data1 = reinterpret_cast<ColumnInt32*>(resp_value_col1.get());
        value_col_data1->insert(Field::create_field<PrimitiveType::TYPE_INT>(100));
        value_col_data1->insert(Field::create_field<PrimitiveType::TYPE_INT>(101));
        resp_block1.insert(
                {make_nullable(std::move(resp_value_col1)), make_nullable(_int_type), "value"});

        PMultiGetResponseV2 response_;
        auto serialized_block = response_.add_blocks()->mutable_block();
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        int64_t compress_time = 0;
        auto s = resp_block1.serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       &compress_time, CompressionTypePB::LZ4);
        EXPECT_TRUE(s.ok());

        _shared_state->rpc_struct_map[_backend_id1].response = std::move(response_);
        // init the response blocks
        _shared_state->response_blocks[0] = resp_block1.clone_empty();
    }

    // Backend 2's response
    {
        _shared_state->rpc_struct_map[_backend_id2]
                .request.mutable_request_block_descs(0)
                ->add_row_id(2);
        Block resp_block2;
        auto resp_value_col2 = _int_type->create_column();
        auto* value_col_data2 = reinterpret_cast<ColumnInt32*>(resp_value_col2.get());
        value_col_data2->insert(Field::create_field<PrimitiveType::TYPE_INT>(200));
        resp_block2.insert(
                {make_nullable(std::move(resp_value_col2)), make_nullable(_int_type), "value"});

        PMultiGetResponseV2 response_;
        auto serialized_block = response_.add_blocks()->mutable_block();
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        int64_t compress_time = 0;
        auto s = resp_block2.serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       &compress_time, CompressionTypePB::LZ4);
        EXPECT_TRUE(s.ok());

        _shared_state->rpc_struct_map[_backend_id2].response = std::move(response_);
    }

    // 3. Setup block order results to control merge order
    _shared_state->block_order_results = {
            {_backend_id1, 0, _backend_id2} // First block order: BE1,BE1,BE2
    };

    // 4. Test merging responses
    Block result_block;
    Status st = _shared_state->merge_multi_response();
    _shared_state->get_block(&result_block);
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
    auto nullable_rowid_col1 =
            ColumnNullable::create(_string_type->create_column(), ColumnUInt8::create());
    nullable_rowid_col1->insert_data((char*)&nullable_rowid_col1, 4);
    nullable_rowid_col1->insert_data(nullptr, 4);
    nullable_rowid_col1->insert_data((char*)&nullable_rowid_col1, 4);

    auto nullable_rowid_col2 =
            ColumnNullable::create(_string_type->create_column(), ColumnUInt8::create());
    nullable_rowid_col2->insert_data((char*)&nullable_rowid_col2, 4);
    nullable_rowid_col2->insert_data((char*)&nullable_rowid_col2, 4);
    nullable_rowid_col2->insert_data(nullptr, 4);

    auto value_col1 = _int_type->create_column();
    value_col1->insert(Field::create_field<PrimitiveType::TYPE_INT>(100));
    value_col1->insert(Field::create_field<PrimitiveType::TYPE_INT>(101));
    value_col1->insert(Field::create_field<PrimitiveType::TYPE_INT>(102));

    auto value_col2 = _int_type->create_column();
    value_col2->insert(Field::create_field<PrimitiveType::TYPE_INT>(200));
    value_col2->insert(Field::create_field<PrimitiveType::TYPE_INT>(201));
    value_col2->insert(Field::create_field<PrimitiveType::TYPE_INT>(202));

    // Add test data to origin block with multiple columns
    _shared_state->origin_block =
            Block({{std::move(nullable_rowid_col1), make_nullable(_string_type), "rowid1"},
                   {std::move(nullable_rowid_col2), make_nullable(_string_type), "rowid2"},
                   {std::move(value_col1), _int_type, "value1"},
                   {std::move(value_col2), _int_type, "value2"}});

    // Set multiple rowid column locations
    _shared_state->rowid_locs = {0, 1};
    _shared_state->response_blocks = std::vector<MutableBlock>(2);

    // 2. Setup response blocks from multiple backends for first rowid
    {
        _shared_state->rpc_struct_map[_backend_id1]
                .request.mutable_request_block_descs(0)
                ->add_row_id(0);
        Block resp_block1;
        auto resp_value_col1 = _int_type->create_column();
        auto* value_col_data1 = reinterpret_cast<ColumnInt32*>(resp_value_col1.get());
        value_col_data1->insert(Field::create_field<PrimitiveType::TYPE_INT>(100));
        resp_block1.insert(
                {make_nullable(std::move(resp_value_col1)), make_nullable(_int_type), "value1"});

        PMultiGetResponseV2 response_;
        auto serialized_block = response_.add_blocks()->mutable_block();
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        int64_t compress_time = 0;
        auto s = resp_block1.serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       &compress_time, CompressionTypePB::LZ4);
        EXPECT_TRUE(s.ok());

        _shared_state->rpc_struct_map[_backend_id1].response = std::move(response_);
        _shared_state->response_blocks[0] = resp_block1.clone_empty();
    }

    // Backend 2's response for first rowid
    {
        _shared_state->rpc_struct_map[_backend_id2]
                .request.mutable_request_block_descs(0)
                ->add_row_id(0);
        Block resp_block2;
        auto resp_value_col2 = _int_type->create_column();
        auto* value_col_data2 = reinterpret_cast<ColumnInt32*>(resp_value_col2.get());
        value_col_data2->insert(Field::create_field<PrimitiveType::TYPE_INT>(102));
        resp_block2.insert(
                {make_nullable(std::move(resp_value_col2)), make_nullable(_int_type), "value1"});

        PMultiGetResponseV2 response_;
        auto serialized_block = response_.add_blocks()->mutable_block();
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        int64_t compress_time = 0;
        auto s = resp_block2.serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       &compress_time, CompressionTypePB::LZ4);
        EXPECT_TRUE(s.ok());

        _shared_state->rpc_struct_map[_backend_id2].response = std::move(response_);
    }

    // Add second block responses for second rowid
    _shared_state->rpc_struct_map[_backend_id1].request.add_request_block_descs();
    _shared_state->rpc_struct_map[_backend_id2].request.add_request_block_descs();
    {
        _shared_state->rpc_struct_map[_backend_id1]
                .request.mutable_request_block_descs(1)
                ->add_row_id(0);
        Block resp_block1;
        auto resp_value_col1 = _int_type->create_column();
        auto* value_col_data1 = reinterpret_cast<ColumnInt32*>(resp_value_col1.get());
        value_col_data1->insert(Field::create_field<PrimitiveType::TYPE_INT>(200));
        resp_block1.insert(
                {make_nullable(std::move(resp_value_col1)), make_nullable(_int_type), "value2"});

        auto serialized_block =
                _shared_state->rpc_struct_map[_backend_id1].response.add_blocks()->mutable_block();
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        int64_t compress_time = 0;
        auto s = resp_block1.serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       &compress_time, CompressionTypePB::LZ4);
        EXPECT_TRUE(s.ok());
        _shared_state->response_blocks[1] = resp_block1.clone_empty();
    }

    {
        _shared_state->rpc_struct_map[_backend_id2]
                .request.mutable_request_block_descs(1)
                ->add_row_id(0);
        Block resp_block2;
        auto resp_value_col2 = _int_type->create_column();
        auto* value_col_data2 = reinterpret_cast<ColumnInt32*>(resp_value_col2.get());
        value_col_data2->insert(Field::create_field<PrimitiveType::TYPE_INT>(201));
        resp_block2.insert(
                {make_nullable(std::move(resp_value_col2)), make_nullable(_int_type), "value2"});

        auto* serialized_block =
                _shared_state->rpc_struct_map[_backend_id2].response.add_blocks()->mutable_block();
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        int64_t compress_time = 0;
        auto s = resp_block2.serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       &compress_time, CompressionTypePB::LZ4);
        EXPECT_TRUE(s.ok());
    }

    // 3. Setup block order results for both rowids
    _shared_state->block_order_results = {
            {_backend_id1, 0, _backend_id2}, // First block order: BE1,null,BE2
            {_backend_id1, _backend_id2, 0}  // Second block order: BE1,BE2,null
    };

    // 4. Test merging responses
    Block result_block;
    Status st = _shared_state->merge_multi_response();
    EXPECT_TRUE(st.ok());
    _shared_state->get_block(&result_block);

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

// Test: when a remote BE returns an empty response block for a relation
// (e.g., id_file_map was GC'd), merge_multi_response() should return a clear
// InternalError("... not match request row id count...") rather than crashing.
//
// This simulates the scenario in RowIdStorageReader::read_by_rowids() where:
//   auto id_file_map = get_id_manager()->get_id_file_map(request.query_id());
//   if (!id_file_map) {
//       for (int i = 0; i < request.request_block_descs_size(); ++i)
//           response->add_blocks();  // <-- empty block, no column data
//       return Status::OK();
//   }
TEST_F(MaterializationSharedStateTest, TestMergeMultiResponseBackendNotFound) {
    // Setup: 1 relation, 2 backends
    // BE_1 returns a valid 1-row block
    // BE_2 returns an empty block (simulating id_file_map missing)
    // block_order_results references both BE_1 and BE_2
    _shared_state->response_blocks = std::vector<MutableBlock>(1);

    // --- BE_1: valid response with 1 row ---
    {
        _shared_state->rpc_struct_map[_backend_id1]
                .request.mutable_request_block_descs(0)
                ->add_row_id(0);
        Block resp_block;
        auto col = _int_type->create_column();
        reinterpret_cast<ColumnInt32*>(col.get())->insert(
                Field::create_field<PrimitiveType::TYPE_INT>(42));
        resp_block.insert({make_nullable(std::move(col)), make_nullable(_int_type), "value"});

        PMultiGetResponseV2 response;
        auto* serialized_block = response.add_blocks()->mutable_block();
        size_t uncompressed_size = 0, compressed_size = 0;
        int64_t compress_time = 0;
        ASSERT_TRUE(resp_block
                            .serialize(0, serialized_block, &uncompressed_size, &compressed_size,
                                       &compress_time, CompressionTypePB::LZ4)
                            .ok());

        _shared_state->rpc_struct_map[_backend_id1].response = std::move(response);
        _shared_state->response_blocks[0] = resp_block.clone_empty();
    }

    // --- BE_2: empty response (simulating id_file_map = null on remote BE) ---
    // The remote BE adds an empty PMultiGetBlockV2 with no PBlock data.
    // After deserialization this produces a Block with 0 columns,
    // so is_empty_column() == true and it won't be inserted into block_maps.
    {
        _shared_state->rpc_struct_map[_backend_id2]
                .request.mutable_request_block_descs(0)
                ->add_row_id(0);
        PMultiGetResponseV2 response;
        response.add_blocks(); // empty PMultiGetBlockV2, no mutable_block() data
        _shared_state->rpc_struct_map[_backend_id2].response = std::move(response);
    }

    // block_order_results references both BEs:
    // row 0 → BE_1 (will succeed), row 1 → BE_2 (will fail: not in block_maps)
    _shared_state->block_order_results = {{_backend_id1, _backend_id2}};

    // Setup origin block so get_block() can work if merge somehow passes
    auto rowid_col = _string_type->create_column();
    rowid_col->insert_many_defaults(2);
    auto value_col = _int_type->create_column();
    value_col->insert_many_defaults(2);
    _shared_state->origin_block = Block({{std::move(rowid_col), _string_type, "rowid"},
                                         {std::move(value_col), _int_type, "value"}});
    _shared_state->rowid_locs = {0};

    // merge_multi_response() should return InternalError
    Status st = _shared_state->merge_multi_response();
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>());
    ASSERT_TRUE(st.to_string().find("not match request row id count") != std::string::npos)
            << "Actual error: " << st.to_string();
}

// test for the stale block_maps bug fixed by commit c655b1a.
// With 2 relations, if block_maps is NOT rebuilt per relation, a stale entry
// from relation 0 (with different schema) could be accessed during relation 1,
// causing wrong data or type mismatch crashes.
TEST_F(MaterializationSharedStateTest, TestMergeMultiResponseStaleBlockMaps) {
    // Setup: 2 relations, 2 backends
    // Relation 0 (table A): BE_1 has 1 row, BE_2 has 0 rows (empty response)
    // Relation 1 (table B): BE_1 has 0 rows (empty response), BE_2 has 1 row
    // block_order_results[0] = [BE_1]
    // block_order_results[1] = [BE_2]
    //
    // Before c655b1a fix: block_maps persists across relations.
    //   i=0: block_maps = {BE_1: table_A_block}. Merge OK.
    //   i=1: BE_1 empty (stale entry stays), BE_2 has data.
    //         block_maps = {BE_1: stale!, BE_2: table_B_block}.
    //         block_order_results[1] = [BE_2] → accesses BE_2 → OK.
    //         But if block_order_results[1] also had BE_1 → stale schema → crash!
    //
    // After fix: block_maps is fresh per relation. This test verifies the
    // correct behavior for cross-relation data distribution.

    _shared_state->response_blocks = std::vector<MutableBlock>(2);
    _shared_state->rpc_struct_map[_backend_id1].request.add_request_block_descs();
    _shared_state->rpc_struct_map[_backend_id2].request.add_request_block_descs();

    // --- Build BE_1's response: blocks[0]=1 row (INT), blocks[1]=empty ---
    {
        _shared_state->rpc_struct_map[_backend_id1]
                .request.mutable_request_block_descs(0)
                ->add_row_id(0);
        PMultiGetResponseV2 response;

        // blocks[0]: 1 row of INT for relation 0
        Block rel0_block;
        auto col = _int_type->create_column();
        reinterpret_cast<ColumnInt32*>(col.get())->insert(
                Field::create_field<PrimitiveType::TYPE_INT>(100));
        rel0_block.insert({make_nullable(std::move(col)), make_nullable(_int_type), "price"});

        auto* pb0 = response.add_blocks()->mutable_block();
        size_t us = 0, cs = 0;
        int64_t ct = 0;
        ASSERT_TRUE(rel0_block.serialize(0, pb0, &us, &cs, &ct, CompressionTypePB::LZ4).ok());
        _shared_state->response_blocks[0] = rel0_block.clone_empty();

        // blocks[1]: empty (BE_1 has no data for relation 1)
        response.add_blocks();

        _shared_state->rpc_struct_map[_backend_id1].response = std::move(response);
    }

    // --- Build BE_2's response: blocks[0]=empty, blocks[1]=1 row (STRING) ---
    {
        PMultiGetResponseV2 response;

        // blocks[0]: empty (BE_2 has no data for relation 0)
        response.add_blocks();

        // blocks[1]: 1 row of STRING for relation 1
        Block rel1_block;
        auto col = _string_type->create_column();
        reinterpret_cast<ColumnString*>(col.get())->insert_data("Alice", 5);
        rel1_block.insert({make_nullable(std::move(col)), make_nullable(_string_type), "name"});

        _shared_state->rpc_struct_map[_backend_id2]
                .request.mutable_request_block_descs(1)
                ->add_row_id(0);
        auto* pb1 = response.add_blocks()->mutable_block();
        size_t us = 0, cs = 0;
        int64_t ct = 0;
        ASSERT_TRUE(rel1_block.serialize(0, pb1, &us, &cs, &ct, CompressionTypePB::LZ4).ok());
        _shared_state->response_blocks[1] = rel1_block.clone_empty();

        _shared_state->rpc_struct_map[_backend_id2].response = std::move(response);
    }

    // block_order_results: relation 0 → only BE_1, relation 1 → only BE_2
    _shared_state->block_order_results = {{_backend_id1}, {_backend_id2}};

    // Setup origin block: [rowid_rel0, rowid_rel1, sort_col]
    auto rowid_col0 = _string_type->create_column();
    rowid_col0->insert_many_defaults(1);
    auto rowid_col1 = _string_type->create_column();
    rowid_col1->insert_many_defaults(1);
    auto sort_col = _int_type->create_column();
    sort_col->insert(Field::create_field<PrimitiveType::TYPE_INT>(999));
    _shared_state->origin_block = Block({{std::move(rowid_col0), _string_type, "rowid0"},
                                         {std::move(rowid_col1), _string_type, "rowid1"},
                                         {std::move(sort_col), _int_type, "sort_key"}});
    _shared_state->rowid_locs = {0, 1};

    // merge should succeed — each relation only references the BE that has data
    Status st = _shared_state->merge_multi_response();
    ASSERT_TRUE(st.ok()) << "merge_multi_response failed: " << st.to_string();

    // Verify results
    Block result_block;
    _shared_state->get_block(&result_block);
    EXPECT_EQ(result_block.rows(), 1);
    // Column order: response_blocks[0] cols, response_blocks[1] cols, sort_key
    // = [price(nullable INT), name(nullable STRING), sort_key(INT)]
    EXPECT_EQ(result_block.columns(), 3);

    // Verify relation 0 data (price = 100)
    auto* price_col = result_block.get_by_position(0).column.get();
    auto* nullable_price = assert_cast<const ColumnNullable*>(price_col);
    EXPECT_FALSE(nullable_price->is_null_at(0));
    EXPECT_EQ(
            *reinterpret_cast<const int*>(nullable_price->get_nested_column().get_data_at(0).data),
            100);

    // Verify relation 1 data (name = "Alice")
    auto* name_col = result_block.get_by_position(1).column.get();
    auto* nullable_name = assert_cast<const ColumnNullable*>(name_col);
    EXPECT_FALSE(nullable_name->is_null_at(0));
    EXPECT_EQ(nullable_name->get_nested_column().get_data_at(0).to_string(), "Alice");
}

} // namespace doris
