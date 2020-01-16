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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <gtest/gtest.h>
#include "runtime/result_writer.h"
#include "exprs/expr.h"
#include "exprs/int_literal.h"
#include "exprs/bool_literal.h"
#include "exprs/float_literal.h"
#include "exprs/string_literal.h"
#include "exprs/timestamp_literal.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "runtime/runtime_state.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/buffer_control_block.h"
#include "util/mysql_row_buffer.h"
#include "gen_cpp/PaloInternalService_types.h"

namespace doris {

class ResultWriterTest : public testing::Test {
public:
    ResultWriterTest() {
        _sink = new BufferControlBlock(TUniqueId(),  1024);
        _runtime_state = new RuntimeState("ResultWriterTest");

        RowDescriptor row_desc;

        {
            int8_t data = 1;
            Expr* expr = new IntLiteral(ColumnType(TYPE_TINYINT), &data);
            expr->prepare(NULL, row_desc);
            _exprs.push_back(expr);
        }
        {
            int16_t data = 2;
            Expr* expr = new IntLiteral(ColumnType(TYPE_SMALLINT), &data);
            expr->prepare(NULL, row_desc);
            _exprs.push_back(expr);
        }
        {
            int32_t data = 3;
            Expr* expr = new IntLiteral(ColumnType(TYPE_INT), &data);
            expr->prepare(NULL, row_desc);
            _exprs.push_back(expr);
        }
        {
            int64_t data = 4;
            Expr* expr = new IntLiteral(ColumnType(TYPE_BIGINT), &data);
            expr->prepare(NULL, row_desc);
            _exprs.push_back(expr);
        }
        {
            Expr* expr = new BoolLiteral(false);
            expr->prepare(NULL, row_desc);
            _exprs.push_back(expr);
        }
        {
            float data = 1.1;
            Expr* expr = new FloatLiteral(ColumnType(TYPE_FLOAT), &data);
            expr->prepare(NULL, row_desc);
            _exprs.push_back(expr);
        }
        {
            double data = 2.2;
            Expr* expr = new FloatLiteral(ColumnType(TYPE_DOUBLE), &data);
            expr->prepare(NULL, row_desc);
            _exprs.push_back(expr);
        }
        {
            Expr* expr = new StringLiteral("this is string");
            expr->prepare(NULL, row_desc);
            _exprs.push_back(expr);
        }
        {
            Expr* expr = new TimestampLiteral(0);
            expr->prepare(NULL, row_desc);
            _exprs.push_back(expr);
        }
    }
    virtual ~ResultWriterTest() {
        delete _sink;
        delete _runtime_state;

        for (int i = 0; i < _exprs.size(); ++i) {
            delete _exprs[i];
        }
    }

protected:
    virtual void SetUp() {
    }

private:
    std::vector<Expr*> _exprs;
    BufferControlBlock* _sink;
    RuntimeState* _runtime_state;
};

TEST_F(ResultWriterTest, init_normal) {
    ResultWriter writer(_sink, _exprs);
    ASSERT_TRUE(writer.init(_runtime_state).ok());
}

TEST_F(ResultWriterTest, init_failed) {
    ResultWriter writer(NULL, _exprs);
    ASSERT_FALSE(writer.init(_runtime_state).ok());
}

TEST_F(ResultWriterTest, AppendRowBatch_empty) {
    ResultWriter writer(_sink, _exprs);
    ASSERT_TRUE(writer.init(_runtime_state).ok());
    ASSERT_TRUE(writer.append_row_batch(NULL).ok());
}

TEST_F(ResultWriterTest, AppendRowBatch_normal) {
    ResultWriter writer(_sink, _exprs);
    ASSERT_TRUE(writer.init(_runtime_state).ok());

    RowDescriptor row_desc;
    RowBatch row_batch(row_desc, 1024);
    row_batch.add_rows(10);
    row_batch.commit_rows(10);

    ASSERT_TRUE(writer.append_row_batch(&row_batch).ok());

    TFetchDataResult result;
    ASSERT_TRUE(_sink->get_batch(&result).ok());
    ASSERT_EQ(10U, result.result_batch.rows.size());
}

}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
