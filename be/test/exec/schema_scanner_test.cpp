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

#include "exec/schema_scanner.h"

#include <gtest/gtest.h>

#include <string>

#include "common/object_pool.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"

namespace doris {

class SchemaScannerTest : public testing::Test {
public:
    SchemaScannerTest() {}

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

SchemaScanner::ColumnDesc s_test_columns[] = {
        //   name,       type,          size,           is_null
        {"Name", TYPE_VARCHAR, sizeof(StringValue), false},
        {"Location", TYPE_VARCHAR, sizeof(StringValue), false},
        {"Comment", TYPE_VARCHAR, sizeof(StringValue), false},
        {"is_null", TYPE_VARCHAR, sizeof(StringValue), true},
};

char g_tuple_buf[10000]; // enough for tuple
TEST_F(SchemaScannerTest, normal_use) {
    SchemaScanner scanner(s_test_columns,
                          sizeof(s_test_columns) / sizeof(SchemaScanner::ColumnDesc));
    Status status = scanner.init(&_param, &_obj_pool);
    ASSERT_TRUE(status.ok());
    status = scanner.init(&_param, &_obj_pool);
    ASSERT_TRUE(status.ok());
    status = scanner.start((RuntimeState*)1);
    ASSERT_TRUE(status.ok());
    const TupleDescriptor* tuple_desc = scanner.tuple_desc();
    ASSERT_TRUE(nullptr != tuple_desc);
    ASSERT_EQ(65, tuple_desc->byte_size());
    Tuple* tuple = (Tuple*)g_tuple_buf;
    bool eos;
    status = scanner.get_next_row(tuple, &_mem_pool, &eos);
    ASSERT_TRUE(status.ok());
}
TEST_F(SchemaScannerTest, input_fail) {
    SchemaScanner scanner(s_test_columns,
                          sizeof(s_test_columns) / sizeof(SchemaScanner::ColumnDesc));
    Status status = scanner.init(&_param, &_obj_pool);
    ASSERT_TRUE(status.ok());
    status = scanner.init(&_param, &_obj_pool);
    ASSERT_TRUE(status.ok());
    status = scanner.start((RuntimeState*)1);
    ASSERT_TRUE(status.ok());
    const TupleDescriptor* tuple_desc = scanner.tuple_desc();
    ASSERT_TRUE(nullptr != tuple_desc);
    ASSERT_EQ(65, tuple_desc->byte_size());
    bool eos;
    status = scanner.get_next_row(nullptr, &_mem_pool, &eos);
    ASSERT_FALSE(status.ok());
}
TEST_F(SchemaScannerTest, invalid_param) {
    SchemaScanner scanner(nullptr, sizeof(s_test_columns) / sizeof(SchemaScanner::ColumnDesc));
    Status status = scanner.init(&_param, &_obj_pool);
    ASSERT_FALSE(status.ok());
}
TEST_F(SchemaScannerTest, no_init_use) {
    SchemaScanner scanner(s_test_columns,
                          sizeof(s_test_columns) / sizeof(SchemaScanner::ColumnDesc));
    Status status = scanner.start((RuntimeState*)1);
    ASSERT_FALSE(status.ok());
    const TupleDescriptor* tuple_desc = scanner.tuple_desc();
    ASSERT_TRUE(nullptr == tuple_desc);
    Tuple* tuple = (Tuple*)g_tuple_buf;
    bool eos;
    status = scanner.get_next_row(tuple, &_mem_pool, &eos);
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
