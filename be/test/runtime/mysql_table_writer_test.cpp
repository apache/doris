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

#include "runtime/mysql_table_writer.h"

#include <string>

#include <gtest/gtest.h>

#include "common/logging.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
//#include "exprs/int_literal.h"
#include "exprs/float_literal.h"
#include "exprs/date_literal.h"
#include "exprs/string_literal.h"
#include "exprs/expr.h"
#include "runtime/primitive_type.h"
#include "gen_cpp/Types_types.h"

namespace doris {

struct TestDataTuple {
    int8_t tiny_val;
    int16_t small_val;
    int32_t int_val;
    int64_t big_val;
    float float_val;
    double double_val;
    std::string date_val;
    std::string datetime_val;
    std::string char_val;
    std::string varchar_val;
};

class MysqlTableWriterTest : public testing::Test {
public:
    MysqlTableWriterTest() :
            _row_batch(_row_desc, 1024) {
        _conn_info.host = "host";
        _conn_info.port = 9999;
        _conn_info.user = "user";
        _conn_info.db = "db";
        _conn_info.passwd = "passwd";
        _row_batch.add_row();
        _row_batch.commit_last_row();
    }

    ~MysqlTableWriterTest() {
    }
protected:
    virtual void SetUp() {
    }
    virtual void TearDown() {
    }
    void format_one(TestDataTuple& data, std::vector<Expr*>& output_expr);
private:
    RowDescriptor _row_desc;
    RowBatch _row_batch;
    MysqlConnInfo _conn_info;
};

void MysqlTableWriterTest::format_one(TestDataTuple& data, std::vector<Expr*>& output_expr) {
    // Tinyint
    {
        ColumnType type(TYPE_TINYINT, 0);
        IntLiteral* expr = new IntLiteral(type, &data.tiny_val);
        expr->prepare(nullptr, _row_desc);
        output_expr.push_back(expr);
    }
    // Smallint
    {
        ColumnType type(TYPE_SMALLINT, 0);
        IntLiteral* expr = new IntLiteral(type, &data.small_val);
        expr->prepare(nullptr, _row_desc);
        output_expr.push_back(expr);
    }
    // Int
    {
        ColumnType type(TYPE_INT, 0);
        IntLiteral* expr = new IntLiteral(type, &data.int_val);
        expr->prepare(nullptr, _row_desc);
        output_expr.push_back(expr);
    }
    // Long
    {
        ColumnType type(TYPE_BIGINT, 0);
        IntLiteral* expr = new IntLiteral(type, &data.big_val);
        expr->prepare(nullptr, _row_desc);
        output_expr.push_back(expr);
    }
    // Float
    {
        ColumnType type(TYPE_FLOAT, 0);
        FloatLiteral* expr = new FloatLiteral(type, &data.float_val);
        expr->prepare(nullptr, _row_desc);
        output_expr.push_back(expr);
    }
    // Double
    {
        ColumnType type(TYPE_DOUBLE, 0);
        FloatLiteral* expr = new FloatLiteral(type, &data.double_val);
        expr->prepare(nullptr, _row_desc);
        output_expr.push_back(expr);
    }
    // Date
    {
        TExprNode node;
        TColumnType t_type;
        t_type.__set_type(TPrimitiveType::DATE);
        node.type = t_type;
        node.date_literal.value = data.date_val;
        DateLiteral* expr = new DateLiteral(node);
        expr->prepare(nullptr, _row_desc);
        output_expr.push_back(expr);
    }
    // Datetime
    {
        TExprNode node;
        TColumnType t_type;
        t_type.__set_type(TPrimitiveType::DATETIME);
        node.type = t_type;
        node.date_literal.value = data.datetime_val;
        DateLiteral* expr = new DateLiteral(node);
        expr->prepare(nullptr, _row_desc);
        output_expr.push_back(expr);
    }
    // char
    {
        TExprNode node;
        TColumnType t_type;
        t_type.__set_type(TPrimitiveType::CHAR);
        t_type.__set_len(20);
        node.type = t_type;
        node.string_literal.value = data.char_val;
        node.string_literal.value.append(20 - data.char_val.size(), '\0');
        StringLiteral* expr = new StringLiteral(node);
        expr->prepare(nullptr, _row_desc);
        output_expr.push_back(expr);
    }
    // Varchar
    {
        TExprNode node;
        TColumnType t_type;
        t_type.__set_type(TPrimitiveType::VARCHAR);
        node.type = t_type;
        node.string_literal.value = data.varchar_val;
        StringLiteral* expr = new StringLiteral(node);
        expr->prepare(nullptr, _row_desc);
        output_expr.push_back(expr);
    }
}

TEST_F(MysqlTableWriterTest, NormalTest) {
    std::vector<Expr*> output_expr;
    TestDataTuple data;

    data.tiny_val = 23;
    data.small_val = 2345;
    data.int_val = 23456789;
    data.big_val = 23456789987654321;
    data.float_val = 23.45;
    data.double_val = 2345.678;
    data.date_val = "22101112";
    data.datetime_val = "22101112123456";
    data.char_val = "i am char too";
    data.varchar_val = "i am varchar too";
    format_one(data, output_expr);

    MysqlTableWriter writer(output_expr);
    writer.open(_conn_info, "mysql_writer_test");
    ASSERT_TRUE(writer.append(&_row_batch).ok());
}

}

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
