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

#include "exec/schema_scanner/schema_columns_scanner.h"

#include <gtest/gtest.h>

#include <string>

#include "common/object_pool.h"
#include "exec/schema_scanner/schema_jni_helper.h"
#include "gen_cpp/Frontend_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"

namespace doris {

int db_num = 0;
Status s_db_result;
Status SchemaJniHelper::get_db_names(const TGetDbsParams& db_params, TGetDbsResult* db_result) {
    for (int i = 0; i < db_num; ++i) {
        db_result->dbs.push_back("abc");
    }
    return s_db_result;
}

int table_num = 0;
Status s_table_result;
Status SchemaJniHelper::get_table_names(const TGetTablesParams& table_params,
                                        TGetTablesResult* table_result) {
    for (int i = 0; i < table_num; ++i) {
        table_result->tables.push_back("bac");
    }
    return s_table_result;
}

int desc_num = 0;
Status s_desc_result;
Status SchemaJniHelper::describe_table(const TDescribeTableParams& desc_params,
                                       TDescribeTableResult* desc_result) {
    for (int i = 0; i < desc_num; ++i) {
        TColumnDesc column_desc;
        column_desc.__set_columnName("abc");
        column_desc.__set_columnType(TPrimitiveType::BOOLEAN);
        TColumnDef column_def;
        column_def.columnDesc = column_desc;
        column_def.comment = "bac";
        desc_result->columns.push_back(column_def);
    }
    return s_desc_result;
}

void init_mock() {
    db_num = 0;
    table_num = 0;
    desc_num = 0;
    s_db_result = Status::OK();
    s_table_result = Status::OK();
    s_desc_result = Status::OK();
}

class SchemaColumnsScannerTest : public testing::Test {
public:
    SchemaColumnsScannerTest() {}

    virtual void SetUp() {
        _param.db = &_db;
        _param.table = &_table;
        _param.wild = &_wild;
    }

private:
    ObjectPool _obj_pool;
    MemPool _mem_pool;
    SchemaScannerParam _param;
    std::string _db;
    std::string _table;
    std::string _wild;
};

char g_tuple_buf[10000]; // enough for tuple
TEST_F(SchemaColumnsScannerTest, normal_use) {
    SchemaColumnsScanner scanner;
    Status status = scanner.init(&_param, &_obj_pool);
    ASSERT_TRUE(status.ok());
    const TupleDescriptor* tuple_desc = scanner.tuple_desc();
    ASSERT_TRUE(NULL != tuple_desc);
    status = scanner.start((RuntimeState*)1);
    ASSERT_TRUE(status.ok());
    Tuple* tuple = (Tuple*)g_tuple_buf;
    bool eos = false;
    status = scanner.get_next_row(tuple, &_mem_pool, &eos);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(eos);
}
TEST_F(SchemaColumnsScannerTest, one_column) {
    table_num = 1;
    db_num = 1;
    desc_num = 1;
    SchemaColumnsScanner scanner;
    Status status = scanner.init(&_param, &_obj_pool);
    ASSERT_TRUE(status.ok());
    const TupleDescriptor* tuple_desc = scanner.tuple_desc();
    ASSERT_TRUE(NULL != tuple_desc);
    status = scanner.start((RuntimeState*)1);
    ASSERT_TRUE(status.ok());
    Tuple* tuple = (Tuple*)g_tuple_buf;
    bool eos = false;
    status = scanner.get_next_row(tuple, &_mem_pool, &eos);
    ASSERT_TRUE(status.ok());
    ASSERT_FALSE(eos);
    status = scanner.get_next_row(tuple, &_mem_pool, &eos);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(eos);
}
TEST_F(SchemaColumnsScannerTest, op_before_init) {
    table_num = 1;
    db_num = 1;
    desc_num = 1;
    SchemaColumnsScanner scanner;
    Status status = scanner.start((RuntimeState*)1);
    ASSERT_FALSE(status.ok());
    Tuple* tuple = (Tuple*)g_tuple_buf;
    bool eos = false;
    status = scanner.get_next_row(tuple, &_mem_pool, &eos);
    ASSERT_FALSE(status.ok());
}
TEST_F(SchemaColumnsScannerTest, input_fail) {
    table_num = 1;
    db_num = 1;
    desc_num = 1;
    SchemaColumnsScanner scanner;
    Status status = scanner.init(NULL, &_obj_pool);
    ASSERT_FALSE(status.ok());
    status = scanner.init(&_param, &_obj_pool);
    ASSERT_TRUE(status.ok());
    status = scanner.start((RuntimeState*)1);
    ASSERT_TRUE(status.ok());
    bool eos = false;
    status = scanner.get_next_row(NULL, &_mem_pool, &eos);
    ASSERT_FALSE(status.ok());
}
TEST_F(SchemaColumnsScannerTest, table_fail) {
    table_num = 1;
    db_num = 1;
    desc_num = 1;
    SchemaColumnsScanner scanner;
    Status status = scanner.init(&_param, &_obj_pool);
    ASSERT_TRUE(status.ok());
    const TupleDescriptor* tuple_desc = scanner.tuple_desc();
    ASSERT_TRUE(NULL != tuple_desc);
    status = scanner.start((RuntimeState*)1);
    ASSERT_TRUE(status.ok());
    Tuple* tuple = (Tuple*)g_tuple_buf;
    bool eos = false;
    s_table_result = Status::InternalError("get table failed");
    status = scanner.get_next_row(tuple, &_mem_pool, &eos);
    ASSERT_FALSE(status.ok());
}
TEST_F(SchemaColumnsScannerTest, desc_fail) {
    table_num = 1;
    db_num = 1;
    desc_num = 1;
    SchemaColumnsScanner scanner;
    Status status = scanner.init(&_param, &_obj_pool);
    ASSERT_TRUE(status.ok());
    const TupleDescriptor* tuple_desc = scanner.tuple_desc();
    ASSERT_TRUE(NULL != tuple_desc);
    status = scanner.start((RuntimeState*)1);
    ASSERT_TRUE(status.ok());
    Tuple* tuple = (Tuple*)g_tuple_buf;
    bool eos = false;
    s_desc_result = Status::InternalError("get desc failed");
    status = scanner.get_next_row(tuple, &_mem_pool, &eos);
    ASSERT_FALSE(status.ok());
}

TEST_F(SchemaColumnsScannerTest, start_fail) {
    table_num = 1;
    db_num = 1;
    desc_num = 1;
    SchemaColumnsScanner scanner;
    Status status = scanner.init(&_param, &_obj_pool);
    ASSERT_TRUE(status.ok());
    s_db_result = Status::InternalError("get db failed.");
    status = scanner.start((RuntimeState*)1);
    ASSERT_FALSE(status.ok());
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
