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

#include "exec/mysql_scanner.h"

#include <gtest/gtest.h>

#include <string>

#include "common/object_pool.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"

namespace doris {

class MysqlScannerTest : public testing::Test {
public:
    MysqlScannerTest() {
        _param.host = "host";
        _param.port = "port";
        _param.user = "user";
        _param.passwd = "passwd";
        _param.db = "db";
    }

protected:
    virtual void SetUp() {}
    MysqlScannerParam _param;
};

TEST_F(MysqlScannerTest, normal_use) {
    MysqlScanner scanner(_param);
    Status status = scanner.open();
    EXPECT_TRUE(status.ok());
    std::vector<std::string> fields;
    fields.push_back("*");
    std::vector<std::string> filters;
    filters.push_back("id = 1");
    status = scanner.query("dim_lbs_device", fields, filters);
    EXPECT_TRUE(status.ok());
    bool eos = false;
    char** buf;
    unsigned long* length;
    status = scanner.get_next_row(nullptr, &length, &eos);
    EXPECT_FALSE(status.ok());

    while (!eos) {
        status = scanner.get_next_row(&buf, &length, &eos);

        if (eos) {
            break;
        }

        EXPECT_TRUE(status.ok());

        for (int i = 0; i < scanner.field_num(); ++i) {
            if (buf[i]) {
                LOG(WARNING) << buf[i];
            } else {
                LOG(WARNING) << "NULL";
            }
        }
    }
}

TEST_F(MysqlScannerTest, no_init) {
    MysqlScanner scanner(_param);
    std::vector<std::string> fields;
    fields.push_back("*");
    std::vector<std::string> filters;
    filters.push_back("id = 1");
    Status status = scanner.query("dim_lbs_device", fields, filters);
    EXPECT_FALSE(status.ok());
    status = scanner.query("select 1");
    EXPECT_FALSE(status.ok());
    bool eos = false;
    char** buf;
    unsigned long* length;
    status = scanner.get_next_row(&buf, &length, &eos);
    EXPECT_FALSE(status.ok());
}

TEST_F(MysqlScannerTest, query_failed) {
    MysqlScanner scanner(_param);
    Status status = scanner.open();
    EXPECT_TRUE(status.ok());
    std::vector<std::string> fields;
    fields.push_back("*");
    std::vector<std::string> filters;
    filters.push_back("id = 1");
    status = scanner.query("no_such_table", fields, filters);
    EXPECT_FALSE(status.ok());
}

TEST_F(MysqlScannerTest, open_failed) {
    MysqlScannerParam invalid_param;
    MysqlScanner scanner(invalid_param);
    Status status = scanner.open();
    EXPECT_FALSE(status.ok());
}

} // namespace doris
