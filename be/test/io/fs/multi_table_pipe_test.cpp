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

#include "io/fs/multi_table_pipe.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "runtime/stream_load/stream_load_context.h"

namespace doris {
using namespace doris::io;
using namespace testing;
class MultiTablePipeTest : public testing::Test {
public:
    MultiTablePipeTest() {}

protected:
    virtual void SetUp() {}

    virtual void TearDown() {}
};

TEST_F(MultiTablePipeTest, append_json) {
    config::multi_table_batch_plan_threshold = 3;

    std::string data1 = "test_table_1|data1";
    std::string data2 = "test_table_2|data2";
    std::string data3 = "test_table_1|data3";
    std::string data4 = "test_table_1|data4";
    std::string data5 = "test_table_3|data5";
    std::string data6 = "test_table_2|data6";
    std::string data7 = "test_table_3|data7";
    std::string data8 = "test_table_3|data8";

    auto exec_env = doris::ExecEnv::GetInstance();
    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(exec_env);
    MultiTablePipe pipe(ctx);

    static_cast<void>(pipe.append_json(data1.c_str(), data1.size()));
    static_cast<void>(pipe.append_json(data2.c_str(), data2.size()));
    static_cast<void>(pipe.append_json(data3.c_str(),
                                       data3.size())); // should trigger 1st plan, for table 1&2
    EXPECT_EQ(pipe.get_pipe_by_table("test_table_1")->get_queue_size(), 2);
    EXPECT_EQ(pipe.get_pipe_by_table("test_table_2")->get_queue_size(), 1);
    static_cast<void>(pipe.append_json(data4.c_str(), data4.size()));
    static_cast<void>(pipe.append_json(data5.c_str(), data5.size()));
    static_cast<void>(pipe.append_json(data6.c_str(), data6.size()));
    static_cast<void>(pipe.append_json(data7.c_str(), data7.size()));
    static_cast<void>(
            pipe.append_json(data8.c_str(), data8.size())); // should trigger 2nd plan, for table 3
    EXPECT_EQ(pipe.get_pipe_by_table("test_table_1")->get_queue_size(), 3);
    EXPECT_EQ(pipe.get_pipe_by_table("test_table_2")->get_queue_size(), 2);
    EXPECT_EQ(pipe.get_pipe_by_table("test_table_3")->get_queue_size(), 3);
}

} // end namespace doris
