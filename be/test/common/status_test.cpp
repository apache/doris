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

#include "common/status.h"

#include <gtest/gtest.h>

#include "gen_cpp/Types_types.h"
#include "util/logging.h"

namespace doris {

class StatusTest : public testing::Test {};

TEST_F(StatusTest, OK) {
    // default
    Status st;
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("", st.get_error_msg());
    ASSERT_EQ("OK", st.to_string());
    // copy
    {
        Status other = st;
        ASSERT_TRUE(other.ok());
    }
    // move assign
    st = Status();
    ASSERT_TRUE(st.ok());
    // move construct
    {
        Status other = std::move(st);
        ASSERT_TRUE(other.ok());
    }
}

TEST_F(StatusTest, Error) {
    // default
    Status st = Status::InternalError("123");
    ASSERT_FALSE(st.ok());
    ASSERT_EQ("123", st.get_error_msg());
    ASSERT_EQ("Internal error: 123", st.to_string());
    // copy
    {
        Status other = st;
        ASSERT_FALSE(other.ok());
        ASSERT_EQ("123", st.get_error_msg());
    }
    // move assign
    st = Status::InternalError("456");
    ASSERT_FALSE(st.ok());
    ASSERT_EQ("456", st.get_error_msg());
    // move construct
    {
        Status other = std::move(st);
        ASSERT_FALSE(other.ok());
        ASSERT_EQ("456", other.get_error_msg());
        ASSERT_EQ("Internal error: 456", other.to_string());
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
