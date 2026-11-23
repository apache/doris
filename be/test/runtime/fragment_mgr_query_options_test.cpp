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

#include <gen_cpp/DorisExternalService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>

#include "runtime/fragment_mgr.h"
#include "runtime/query_options_validation.h"

namespace doris {
TEST(ExternalScanQueryOptionsTest, OmittedTimeoutKeepsQueryDefault) {
    // One empty request is enough. No descriptor, tablet, or row is required.
    // The scan field stores zero when absent, but the query default is 3600 seconds.
    TScanOpenParams params;
    ASSERT_FALSE(params.__isset.execution_timeout);
    ASSERT_EQ(0, params.execution_timeout);
    auto options = FragmentMgr::_build_external_scan_query_options(params);
    EXPECT_EQ(3600, options.execution_timeout);
    EXPECT_TRUE(validate_query_options(options).ok());
}

TEST(ExternalScanQueryOptionsTest, ExplicitTimeoutRetainsValidation) {
    TScanOpenParams params;
    // Do not turn explicit invalid values into defaults. The caller must receive an error.
    for (int32_t timeout : {-1, 0}) {
        params.__set_execution_timeout(timeout);
        auto options = FragmentMgr::_build_external_scan_query_options(params);
        EXPECT_TRUE(options.__isset.execution_timeout);
        EXPECT_EQ(timeout, options.execution_timeout);
        auto status = validate_query_options(options);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(std::string::npos, status.to_string().find("execution_timeout"));
    }
    // Both positive endpoints fit the i32 wire field and must retain their exact values.
    for (int32_t timeout : {1, std::numeric_limits<int32_t>::max()}) {
        params.__set_execution_timeout(timeout);
        auto options = FragmentMgr::_build_external_scan_query_options(params);
        EXPECT_TRUE(options.__isset.execution_timeout);
        EXPECT_EQ(timeout, options.execution_timeout);
        EXPECT_TRUE(validate_query_options(options).ok());
    }
}
} // namespace doris
