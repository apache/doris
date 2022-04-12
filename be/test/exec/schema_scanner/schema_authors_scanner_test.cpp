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

#include "exec/schema_scanner/schema_authors_scanner.h"

#include <gtest/gtest.h>

#include <string>

#include "common/object_pool.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"

namespace doris {

class SchemaAuthorScannerTest : public testing::Test {
public:
    SchemaAuthorScannerTest() {}

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
TEST_F(SchemaAuthorScannerTest, normal_use) {
    SchemaAuthorsScanner scanner;
    Status status = scanner.init(&_param, &_obj_pool);
    EXPECT_TRUE(status.ok());
    const TupleDescriptor* tuple_desc = scanner.tuple_desc();
    EXPECT_TRUE(nullptr != tuple_desc);
    status = scanner.start((RuntimeState*)1);
    EXPECT_TRUE(status.ok());
    Tuple* tuple = (Tuple*)g_tuple_buf;
    bool eos = false;
    while (!eos) {
        status = scanner.get_next_row(tuple, &_mem_pool, &eos);
        EXPECT_TRUE(status.ok());
        for (int i = 0; i < 3; ++i) {
            LOG(INFO)
                    << ((StringValue*)tuple->get_slot(tuple_desc->slots()[i]->tuple_offset()))->ptr;
        }
    }
}

TEST_F(SchemaAuthorScannerTest, use_with_no_init) {
    SchemaAuthorsScanner scanner;
    const TupleDescriptor* tuple_desc = scanner.tuple_desc();
    EXPECT_TRUE(nullptr == tuple_desc);
    Status status = scanner.start((RuntimeState*)1);
    EXPECT_FALSE(status.ok());
    Tuple* tuple = (Tuple*)g_tuple_buf;
    bool eos = false;
    status = scanner.get_next_row(tuple, &_mem_pool, &eos);
    EXPECT_FALSE(status.ok());
}

TEST_F(SchemaAuthorScannerTest, invalid_param) {
    SchemaAuthorsScanner scanner;
    Status status = scanner.init(&_param, nullptr);
    EXPECT_FALSE(status.ok());
    status = scanner.init(&_param, &_obj_pool);
    EXPECT_TRUE(status.ok());
    const TupleDescriptor* tuple_desc = scanner.tuple_desc();
    EXPECT_TRUE(nullptr != tuple_desc);
    status = scanner.start((RuntimeState*)1);
    EXPECT_TRUE(status.ok());
    Tuple* tuple = (Tuple*)g_tuple_buf;
    bool eos = false;
    status = scanner.get_next_row(tuple, nullptr, &eos);
    EXPECT_FALSE(status.ok());
}

} // namespace doris
