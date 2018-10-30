// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

#include "common/logging.h"
#include "exec/set_executor.h"
#include "runtime/exec_env.h"
#include "service/palo_server.h"

namespace palo {

class SetExecutorTest : public testing::Test {
public:
    SetExecutorTest() :
        _runtim_state("tmp") {
    }

    virtual void SetUp() {
    }
private:
    RuntimeState _runtim_state;
};

TEST_F(SetExecutorTest, normal_case) {
    ExecEnv exec_env;
    PaloServer palo_server(&exec_env);
    TSetParams params;
    {
        TSetVar set_var;

        set_var.type = TSetType::OPT_SESSION;
        set_var.variable = "key1";
        TExprNode expr;
        expr.node_type = TExprNodeType::STRING_LITERAL;
        expr.type = TPrimitiveType::STRING;
        expr.__isset.string_literal = true;
        expr.string_literal.value = "value1";
        set_var.value.nodes.push_back(expr);

        params.set_vars.push_back(set_var);
    }
    {
        TSetVar set_var;

        set_var.type = TSetType::OPT_GLOBAL;
        set_var.variable = "key2";
        TExprNode expr;
        expr.node_type = TExprNodeType::STRING_LITERAL;
        expr.type = TPrimitiveType::STRING;
        expr.__isset.string_literal = true;
        expr.string_literal.value = "value2";
        set_var.value.nodes.push_back(expr);

        params.set_vars.push_back(set_var);
    }
    {
        TSetVar set_var;

        set_var.type = TSetType::OPT_DEFAULT;
        set_var.variable = "key3";
        TExprNode expr;
        expr.node_type = TExprNodeType::STRING_LITERAL;
        expr.type = TPrimitiveType::STRING;
        expr.__isset.string_literal = true;
        expr.string_literal.value = "value3";
        set_var.value.nodes.push_back(expr);

        params.set_vars.push_back(set_var);
    }
    SetExecutor executor(&palo_server, params);
    RowDescriptor row_desc;
    Status status = executor.prepare((RuntimeState*)&_runtim_state, row_desc);
    ASSERT_TRUE(status.ok());
    LOG(INFO) << executor.debug_string();
}
TEST_F(SetExecutorTest, failed_case) {
    ExecEnv exec_env;
    PaloServer palo_server(&exec_env);
    TSetParams params;
    {
        TSetVar set_var;

        set_var.type = TSetType::OPT_SESSION;
        set_var.variable = "key1";
        TExprNode expr;
        expr.node_type = TExprNodeType::INT_LITERAL;
        expr.type = TPrimitiveType::INT;
        expr.__isset.int_literal = true;
        set_var.value.nodes.push_back(expr);

        params.set_vars.push_back(set_var);
    }
    SetExecutor executor(&palo_server, params);
    RowDescriptor row_desc;
    Status status = executor.prepare((RuntimeState*)&_runtim_state, row_desc);
    ASSERT_FALSE(status.ok());
    LOG(INFO) << executor.debug_string();
}
}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!palo::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    palo::CpuInfo::Init();
    return RUN_ALL_TESTS();
}

