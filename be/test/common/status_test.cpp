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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"

namespace doris {

class StatusTest : public testing::Test {};

TEST_F(StatusTest, OK) {
    // default
    Status st;
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(st.to_string().find("[OK]") != std::string::npos);
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
    EXPECT_TRUE(st.to_string().find("[INTERNAL_ERROR]") != std::string::npos);
    EXPECT_TRUE(st.to_string().find("123") != std::string::npos);
    // copy
    {
        Status other = st;
        EXPECT_FALSE(other.ok());
        EXPECT_TRUE(other.to_string().find("[INTERNAL_ERROR]") != std::string::npos);
        EXPECT_TRUE(other.to_string().find("123") != std::string::npos);
    }
    // move assign
    st = Status::InternalError("456");
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("[INTERNAL_ERROR]") != std::string::npos);
    EXPECT_TRUE(st.to_string().find("456") != std::string::npos);
    // move construct
    {
        Status other = std::move(st);
        EXPECT_FALSE(other.ok());
        EXPECT_TRUE(other.to_string().find("[INTERNAL_ERROR]") != std::string::npos);
        EXPECT_TRUE(other.to_string().find("456") != std::string::npos);
    }
}

TEST_F(StatusTest /*unused*/, Format /*unused*/) {
    // status == ok
    Status st_ok = Status::OK();
    EXPECT_TRUE(fmt::format("{}", st_ok).compare(fmt::format("{}", st_ok.to_string())) == 0);

    // status == error
    Status st_error = Status::InternalError("123");
    EXPECT_TRUE(fmt::format("{}", st_error).compare(fmt::format("{}", st_error.to_string())) == 0);
}

} // namespace doris
