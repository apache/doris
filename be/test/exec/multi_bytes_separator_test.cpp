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

#include "exec/broker_scanner.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "exec/local_file_reader.h"
#include "exprs/cast_functions.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/user_function_cache.h"

namespace doris {

class MultiBytesSeparatorTest: public testing::Test {
public:
    MultiBytesSeparatorTest() {}

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
};


TEST_F(MultiBytesSeparatorTest, normal) {
    TBrokerScanRangeParams params;
    params.column_separator = ',';
    params.line_delimiter = '\n';
    params.column_separator_str = "AAAA";
    params.line_delimiter_str = "BBB";
    params.column_separator_length = 4;
    params.line_delimiter_length = 3;

    const std::vector<TBrokerRangeDesc> ranges;
    const std::vector<TNetworkAddress> broker_addresses;
    const std::vector<TExpr> pre_filter_texprs;
    BrokerScanner scanner(nullptr, nullptr, params, ranges, broker_addresses, pre_filter_texprs, nullptr);

#define private public

    // 1.
    {
        std::string line = "AAAA";
        Slice s(line);
        scanner.split_line(s);
        ASSERT_EQ(2, scanner._split_values.size());
        ASSERT_EQ(0, scanner._split_values[0].size);
        ASSERT_EQ(0, scanner._split_values[1].size);
    }

    // 2.
    {
        std::string line = "ABAA";
        Slice s(line);
        scanner.split_line(s);
        ASSERT_EQ(1, scanner._split_values.size());
        ASSERT_EQ(4, scanner._split_values[0].size);
    }

    // 3.
    {
        std::string line = "";
        Slice s(line);
        scanner.split_line(s);
        ASSERT_EQ(1, scanner._split_values.size());
        ASSERT_EQ(0, scanner._split_values[0].size);
    }

    // 4.
    {
        // 1234, AAAB, , AA
        std::string line = "1234AAAAAAABAAAAAAAAAA";
        Slice s(line);
        scanner.split_line(s);
        ASSERT_EQ(4, scanner._split_values.size());
        ASSERT_EQ(4, scanner._split_values[0].size);
        ASSERT_EQ(4, scanner._split_values[1].size);
        ASSERT_EQ(0, scanner._split_values[2].size);
        ASSERT_EQ(2, scanner._split_values[3].size);
    }
}


} // end namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
