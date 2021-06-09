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

#include "runtime/result_sink.h"

#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>

#include <iostream>

#include "exprs/bool_literal.h"
#include "exprs/expr.h"
#include "exprs/float_literal.h"
#include "exprs/int_literal.h"
#include "exprs/string_literal.h"
#include "exprs/timestamp_literal.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/buffer_control_block.h"
#include "runtime/primitive_type.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "util/cpu_info.h"
#include "util/logging.h"
#include "util/mysql_row_buffer.h"

namespace doris {

class ResultSinkTest : public testing::Test {
public:
    ResultSinkTest() {
        _runtime_state = new RuntimeState("ResultWriterTest");
        _runtime_state->_exec_env = &_exec_env;

        {
            TExpr expr;
            {
                TExprNode node;

                node.node_type = TExprNodeType::INT_LITERAL;
                node.type = to_tcolumn_type_thrift(TPrimitiveType::TINYINT);
                node.num_children = 0;
                TIntLiteral data;
                data.value = 1;
                node.__set_int_literal(data);
                expr.nodes.push_back(node);
            }
            _exprs.push_back(expr);
        }
    }
    virtual ~ResultSinkTest() { delete _runtime_state; }

protected:
    virtual void SetUp() {}

private:
    ExecEnv _exec_env;
    std::vector<TExpr> _exprs;
    RuntimeState* _runtime_state;
    RowDescriptor _row_desc;
    TResultSink _tsink;
};

TEST_F(ResultSinkTest, init_normal) {
    ResultSink sink(_row_desc, _exprs, _tsink, 1024);
    ASSERT_TRUE(sink.init(_runtime_state).ok());
    RowBatch row_batch(_row_desc, 1024);
    row_batch.add_row();
    row_batch.commit_last_row();
    ASSERT_TRUE(sink.send(_runtime_state, &row_batch).ok());
    ASSERT_TRUE(sink.close(_runtime_state, Status::OK()).ok());
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
