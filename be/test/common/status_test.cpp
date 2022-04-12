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
    EXPECT_TRUE(st.ok());
    EXPECT_EQ("", st.get_error_msg());
    EXPECT_EQ("OK", st.to_string());
    // copy
    {
        Status other = st;
        EXPECT_TRUE(other.ok());
    }
    // move assign
    st = Status();
    EXPECT_TRUE(st.ok());
    // move construct
    {
        Status other = std::move(st);
        EXPECT_TRUE(other.ok());
    }
}

TEST_F(StatusTest, Error) {
    // default
    Status st = Status::InternalError("123");
    EXPECT_FALSE(st.ok());
    EXPECT_EQ("123", st.get_error_msg());
    EXPECT_EQ("Internal error: 123", st.to_string());
    // copy
    {
        Status other = st;
        EXPECT_FALSE(other.ok());
        EXPECT_EQ("123", st.get_error_msg());
    }
    // move assign
    st = Status::InternalError("456");
    EXPECT_FALSE(st.ok());
    EXPECT_EQ("456", st.get_error_msg());
    // move construct
    {
        Status other = std::move(st);
        EXPECT_FALSE(other.ok());
        EXPECT_EQ("456", other.get_error_msg());
        EXPECT_EQ("Internal error: 456", other.to_string());
    }
}

} // namespace doris
