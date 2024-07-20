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

#include "common/exception.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "gtest/gtest_pred_impl.h"

namespace doris {

class ExceptionTest : public testing::Test {};

TEST_F(ExceptionTest, OK) {
    // default
    try {
        throw doris::Exception();
    } catch (doris::Exception& e) {
        EXPECT_TRUE(e.code() == ErrorCode::OK);
    }
}

TEST_F(ExceptionTest, SingleError) {
    try {
        throw doris::Exception(ErrorCode::OS_ERROR, "test OS_ERROR {}", "bug");
    } catch (doris::Exception& e) {
        EXPECT_TRUE(e.to_string().find("OS_ERROR") != std::string::npos);
    }
}

TEST_F(ExceptionTest, NestedError) {
    try {
        throw doris::Exception(ErrorCode::OS_ERROR, "test OS_ERROR {}", "bug");
    } catch (doris::Exception& e1) {
        EXPECT_TRUE(e1.to_string().find("OS_ERROR") != std::string::npos);
    }
}

} // namespace doris
