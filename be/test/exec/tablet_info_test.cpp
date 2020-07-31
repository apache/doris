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

#include "exec/tablet_info.h"

#include <gtest/gtest.h>

#include "runtime/descriptor_helper.h"
#include "runtime/mem_tracker.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"

namespace doris {

class OlapTablePartitionParamTest : public testing::Test {
public:
    OlapTablePartitionParamTest() { }
    virtual ~OlapTablePartitionParamTest() { }
    void SetUp() override { }
private:
};

TOlapTableSchemaParam get_schema(TDescriptorTable* desc_tbl) {
    TOlapTableSchemaParam t_schema_param;
    t_schema_param.db_id = 1;
    t_schema_param.table_id = 2;
    t_schema_param.version = 0;

    // descriptor
    {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_INT).column_name("c1").column_pos(1).build());
        tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("c2").column_pos(2).build());
        tuple_builder.add_slot(
            TSlotDescriptorBuilder().string_type(20).column_name("c3").column_pos(3).build());

        tuple_builder.build(&dtb);

        *desc_tbl = dtb.desc_tbl();
        t_schema_param.slot_descs = desc_tbl->slotDescriptors;
        t_schema_param.tuple_desc = desc_tbl->tupleDescriptors[0];
    }
    // index
    t_schema_param.indexes.resize(2);
    t_schema_param.indexes[0].id = 4;
    t_schema_param.indexes[0].columns = {"c1", "c2", "c3"};
    t_schema_param.indexes[1].id = 5;
    t_schema_param.indexes[1].columns = {"c1", "c3"};

    return t_schema_param;
}

TEST_F(OlapTablePartitionParamTest, normal) {
    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    auto st = schema->init(t_schema);
    ASSERT_TRUE(st.ok());
    LOG(INFO) << schema->debug_string();

    // (-oo, 10] | [10.50) | [60, +oo)
    TOlapTablePartitionParam t_partition_param;
    t_partition_param.db_id = 1;
    t_partition_param.table_id = 2;
    t_partition_param.version = 0;
    t_partition_param.__set_partition_column("c2");
    t_partition_param.__set_distributed_columns({"c1", "c3"});
    t_partition_param.partitions.resize(3);
    t_partition_param.partitions[0].id = 10;
    t_partition_param.partitions[0].__isset.end_key = true;
    t_partition_param.partitions[0].end_key.node_type = TExprNodeType::INT_LITERAL;
    t_partition_param.partitions[0].end_key.type = t_desc_tbl.slotDescriptors[1].slotType;
    t_partition_param.partitions[0].end_key.num_children = 0;
    t_partition_param.partitions[0].end_key.__isset.int_literal = true;
    t_partition_param.partitions[0].end_key.int_literal.value = 10;
    t_partition_param.partitions[0].num_buckets = 1;
    t_partition_param.partitions[0].indexes.resize(2);
    t_partition_param.partitions[0].indexes[0].index_id = 4;
    t_partition_param.partitions[0].indexes[0].tablets = {21};
    t_partition_param.partitions[0].indexes[1].index_id = 5;
    t_partition_param.partitions[0].indexes[1].tablets = {22};

    t_partition_param.partitions[1].id = 11;
    t_partition_param.partitions[1].__isset.start_key = true;
    t_partition_param.partitions[1].start_key.node_type = TExprNodeType::INT_LITERAL;
    t_partition_param.partitions[1].start_key.type = t_desc_tbl.slotDescriptors[1].slotType;
    t_partition_param.partitions[1].start_key.num_children = 0;
    t_partition_param.partitions[1].start_key.__isset.int_literal = true;
    t_partition_param.partitions[1].start_key.int_literal.value = 10;
    t_partition_param.partitions[1].__isset.end_key = true;
    t_partition_param.partitions[1].end_key.node_type = TExprNodeType::INT_LITERAL;
    t_partition_param.partitions[1].end_key.type = t_desc_tbl.slotDescriptors[1].slotType;
    t_partition_param.partitions[1].end_key.num_children = 0;
    t_partition_param.partitions[1].end_key.__isset.int_literal = true;
    t_partition_param.partitions[1].end_key.int_literal.value = 50;
    t_partition_param.partitions[1].num_buckets = 2;
    t_partition_param.partitions[1].indexes.resize(2);
    t_partition_param.partitions[1].indexes[0].index_id = 4;
    t_partition_param.partitions[1].indexes[0].tablets = {31, 32};
    t_partition_param.partitions[1].indexes[1].index_id = 5;
    t_partition_param.partitions[1].indexes[1].tablets = {33, 34};

    t_partition_param.partitions[2].id = 12;
    t_partition_param.partitions[2].__isset.start_key = true;
    t_partition_param.partitions[2].start_key.node_type = TExprNodeType::INT_LITERAL;
    t_partition_param.partitions[2].start_key.type = t_desc_tbl.slotDescriptors[1].slotType;
    t_partition_param.partitions[2].start_key.num_children = 0;
    t_partition_param.partitions[2].start_key.__isset.int_literal = true;
    t_partition_param.partitions[2].start_key.int_literal.value = 60;
    t_partition_param.partitions[2].num_buckets = 4;
    t_partition_param.partitions[2].indexes.resize(2);
    t_partition_param.partitions[2].indexes[0].index_id = 4;
    t_partition_param.partitions[2].indexes[0].tablets = {41, 42, 43, 44};
    t_partition_param.partitions[2].indexes[1].index_id = 5;
    t_partition_param.partitions[2].indexes[1].tablets = {45, 46, 47, 48};

    OlapTablePartitionParam part(schema, t_partition_param);
    st = part.init();
    ASSERT_TRUE(st.ok());
    LOG(INFO) << part.debug_string();

    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    st = DescriptorTbl::create(&pool, t_desc_tbl, &desc_tbl);
    ASSERT_TRUE(st.ok());
    RowDescriptor row_desc(*desc_tbl, {0}, {false});
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    auto tracker = std::make_shared<MemTracker>();
    RowBatch batch(row_desc, 1024, tracker.get());
    // 12, 9, "abc"
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 12;
        *reinterpret_cast<int64_t*>(tuple->get_slot(8)) = 9;
        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(16));
        str_val->ptr = (char*)batch.tuple_data_pool()->allocate(10);
        str_val->len = 3;
        memcpy(str_val->ptr, "abc", str_val->len);

        // 9: 
        uint32_t dist_hash = 0;
        const OlapTablePartition* partition = nullptr;
        auto found = part.find_tablet(tuple, &partition, &dist_hash);
        ASSERT_TRUE(found);
        ASSERT_EQ(10, partition->id);
    }
    // 13, 25, "abcd"
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 13;
        *reinterpret_cast<int64_t*>(tuple->get_slot(8)) = 25;
        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(16));
        str_val->ptr = (char*)batch.tuple_data_pool()->allocate(10);
        str_val->len = 4;
        memcpy(str_val->ptr, "abcd", str_val->len);

        // 25: 
        uint32_t dist_hash = 0;
        const OlapTablePartition* partition = nullptr;
        auto found = part.find_tablet(tuple, &partition, &dist_hash);
        ASSERT_TRUE(found);
        ASSERT_EQ(11, partition->id);
    }
    // 14, 50, "abcde"
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 14;
        *reinterpret_cast<int64_t*>(tuple->get_slot(8)) = 50;
        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(16));
        str_val->ptr = reinterpret_cast<char*>(batch.tuple_data_pool()->allocate(10));
        str_val->len = 5;
        memcpy(str_val->ptr, "abcde", str_val->len);

        // 50: 
        uint32_t dist_hash = 0;
        const OlapTablePartition* partition = nullptr;
        auto found = part.find_tablet(tuple, &partition, &dist_hash);
        ASSERT_FALSE(found);
    }

    // 15, 60, "abcdef"
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 15;
        *reinterpret_cast<int64_t*>(tuple->get_slot(8)) = 60;
        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(16));
        str_val->ptr = reinterpret_cast<char*>(batch.tuple_data_pool()->allocate(10));
        str_val->len = 6;
        memcpy(str_val->ptr, "abcdef", str_val->len);

        // 60: 
        uint32_t dist_hash = 0;
        const OlapTablePartition* partition = nullptr;
        auto found = part.find_tablet(tuple, &partition, &dist_hash);
        ASSERT_TRUE(found);
        ASSERT_EQ(12, partition->id);
    }
}

TEST_F(OlapTablePartitionParamTest, to_protobuf) {
    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    auto st = schema->init(t_schema);
    ASSERT_TRUE(st.ok());
    POlapTableSchemaParam pschema;
    schema->to_protobuf(&pschema);
    {
        std::shared_ptr<OlapTableSchemaParam> schema2(new OlapTableSchemaParam());
        auto st = schema2->init(pschema);
        ASSERT_TRUE(st.ok());

        ASSERT_STREQ(schema->debug_string().c_str(), schema2->debug_string().c_str());
    }
}

TEST_F(OlapTablePartitionParamTest, unknown_index_column) {
    TDescriptorTable t_desc_tbl;
    auto tschema = get_schema(&t_desc_tbl);
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    tschema.indexes[0].columns.push_back("unknown_col");
    auto st = schema->init(tschema);
    ASSERT_FALSE(st.ok());
}

TEST_F(OlapTablePartitionParamTest, unpartitioned) {
    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    auto st = schema->init(t_schema);
    ASSERT_TRUE(st.ok());

    // (-oo, 10] | [10.50) | [60, +oo)
    TOlapTablePartitionParam t_partition_param;
    t_partition_param.db_id = 1;
    t_partition_param.table_id = 2;
    t_partition_param.version = 0;
    t_partition_param.__set_distributed_columns({"c1", "c3"});
    t_partition_param.partitions.resize(1);
    t_partition_param.partitions[0].id = 10;
    t_partition_param.partitions[0].num_buckets = 1;
    t_partition_param.partitions[0].indexes.resize(2);
    t_partition_param.partitions[0].indexes[0].index_id = 4;
    t_partition_param.partitions[0].indexes[0].tablets = {21};
    t_partition_param.partitions[0].indexes[1].index_id = 5;

    OlapTablePartitionParam part(schema, t_partition_param);
    st = part.init();
    ASSERT_TRUE(st.ok());

    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    st = DescriptorTbl::create(&pool, t_desc_tbl, &desc_tbl);
    ASSERT_TRUE(st.ok());
    RowDescriptor row_desc(*desc_tbl, {0}, {false});
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    auto tracker = std::make_shared<MemTracker>();
    RowBatch batch(row_desc, 1024, tracker.get());
    // 12, 9, "abc"
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 12;
        *reinterpret_cast<int64_t*>(tuple->get_slot(8)) = 9;
        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(16));
        str_val->ptr = (char*)batch.tuple_data_pool()->allocate(10);
        str_val->len = 3;
        memcpy(str_val->ptr, "abc", str_val->len);

        // 9: 
        uint32_t dist_hash = 0;
        const OlapTablePartition* partition = nullptr;
        auto found = part.find_tablet(tuple, &partition, &dist_hash);
        ASSERT_TRUE(found);
        ASSERT_EQ(10, partition->id);
    }
}

TEST_F(OlapTablePartitionParamTest, unknown_partition_column) {
    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    auto st = schema->init(t_schema);
    ASSERT_TRUE(st.ok());

    // (-oo, 10] | [10.50) | [60, +oo)
    TOlapTablePartitionParam t_partition_param;
    t_partition_param.db_id = 1;
    t_partition_param.table_id = 2;
    t_partition_param.version = 0;
    t_partition_param.__set_partition_column("c4");
    t_partition_param.__set_distributed_columns({"c1", "c3"});
    t_partition_param.partitions.resize(1);
    t_partition_param.partitions[0].id = 10;
    t_partition_param.partitions[0].num_buckets = 1;
    t_partition_param.partitions[0].indexes.resize(2);
    t_partition_param.partitions[0].indexes[0].index_id = 4;
    t_partition_param.partitions[0].indexes[0].tablets = {21};
    t_partition_param.partitions[0].indexes[1].index_id = 5;

    OlapTablePartitionParam part(schema, t_partition_param);
    st = part.init();
    ASSERT_FALSE(st.ok());
}

TEST_F(OlapTablePartitionParamTest, unknown_distributed_col) {
    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    auto st = schema->init(t_schema);
    ASSERT_TRUE(st.ok());

    // (-oo, 10] | [10.50) | [60, +oo)
    TOlapTablePartitionParam t_partition_param;
    t_partition_param.db_id = 1;
    t_partition_param.table_id = 2;
    t_partition_param.version = 0;
    t_partition_param.__set_distributed_columns({"c4"});
    t_partition_param.partitions.resize(1);
    t_partition_param.partitions[0].id = 10;
    t_partition_param.partitions[0].num_buckets = 1;
    t_partition_param.partitions[0].indexes.resize(2);
    t_partition_param.partitions[0].indexes[0].index_id = 4;
    t_partition_param.partitions[0].indexes[0].tablets = {21};
    t_partition_param.partitions[0].indexes[1].index_id = 5;

    OlapTablePartitionParam part(schema, t_partition_param);
    st = part.init();
    ASSERT_FALSE(st.ok());
}

TEST_F(OlapTablePartitionParamTest, bad_index) {
    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    auto st = schema->init(t_schema);
    ASSERT_TRUE(st.ok());

    {
        // (-oo, 10] | [10.50) | [60, +oo)
        TOlapTablePartitionParam t_partition_param;
        t_partition_param.db_id = 1;
        t_partition_param.table_id = 2;
        t_partition_param.version = 0;
        t_partition_param.__set_distributed_columns({"c1", "c3"});
        t_partition_param.partitions.resize(1);
        t_partition_param.partitions[0].id = 10;
        t_partition_param.partitions[0].num_buckets = 1;
        t_partition_param.partitions[0].indexes.resize(1);
        t_partition_param.partitions[0].indexes[0].index_id = 4;
        t_partition_param.partitions[0].indexes[0].tablets = {21};

        OlapTablePartitionParam part(schema, t_partition_param);
        st = part.init();
        ASSERT_FALSE(st.ok());
    }
    {
        // (-oo, 10] | [10.50) | [60, +oo)
        TOlapTablePartitionParam t_partition_param;
        t_partition_param.db_id = 1;
        t_partition_param.table_id = 2;
        t_partition_param.version = 0;
        t_partition_param.__set_partition_column("c4");
        t_partition_param.__set_distributed_columns({"c1", "c3"});
        t_partition_param.partitions.resize(1);
        t_partition_param.partitions[0].id = 10;
        t_partition_param.partitions[0].num_buckets = 1;
        t_partition_param.partitions[0].indexes.resize(2);
        t_partition_param.partitions[0].indexes[0].index_id = 4;
        t_partition_param.partitions[0].indexes[0].tablets = {21};
        t_partition_param.partitions[0].indexes[1].index_id = 6;

        OlapTablePartitionParam part(schema, t_partition_param);
        st = part.init();
        ASSERT_FALSE(st.ok());
    }
}

TEST_F(OlapTablePartitionParamTest, tableLoacation) {
    TOlapTableLocationParam tparam;
    tparam.tablets.resize(1);
    tparam.tablets[0].tablet_id = 1;
    OlapTableLocationParam location(tparam);
    {
        auto loc = location.find_tablet(1);
        ASSERT_TRUE(loc != nullptr);
    }
    {
        auto loc = location.find_tablet(2);
        ASSERT_TRUE(loc == nullptr);
    }
}

TEST_F(OlapTablePartitionParamTest, NodesInfo) {
    TPaloNodesInfo tinfo;
    tinfo.nodes.resize(1);
    tinfo.nodes[0].id = 1;
    DorisNodesInfo nodes(tinfo);
    {
        auto node = nodes.find_node(1);
        ASSERT_TRUE(node != nullptr);
    }
    {
        auto node = nodes.find_node(2);
        ASSERT_TRUE(node == nullptr);
    }
}

}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
