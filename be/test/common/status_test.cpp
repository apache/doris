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

// This test is used to make sure TStatusCode is defined in status.h
TEST_F(StatusTest, TStatusCodeWithStatus) {
    // The definition in status.h
    //extern ErrorCode::ErrorCodeState error_states[ErrorCode::MAX_ERROR_CODE_DEFINE_NUM];
    // The definition in Status_types.h
    extern const std::map<int, const char*> _TStatusCode_VALUES_TO_NAMES;
    ErrorCode::error_code_init.check_init();
    // Check if all code in status.h with error_code > 0, is in Status_types.h
    for (int i = 0; i < ErrorCode::MAX_ERROR_CODE_DEFINE_NUM; ++i) {
        //std::cout << "begin to check number " << i << ", error status "
        //          << ErrorCode::error_states[i].error_code << std::endl;
        if (ErrorCode::error_states[i].error_code > 0) {
            //std::cout << "Status info " << ErrorCode::error_states[i].error_code << ","
            //          << ErrorCode::error_states[i].description << ","
            //          << ::doris::_TStatusCode_VALUES_TO_NAMES.at(i) << std::endl;
            EXPECT_TRUE(::doris::_TStatusCode_VALUES_TO_NAMES.find(i) !=
                        ::doris::_TStatusCode_VALUES_TO_NAMES.end());
            // also check name is equal
            EXPECT_TRUE(ErrorCode::error_states[i].description.compare(
                                ::doris::_TStatusCode_VALUES_TO_NAMES.at(i)) == 0);
        }
    }
    // Check all code in Status_types.h in status.h
    for (auto& tstatus_st : _TStatusCode_VALUES_TO_NAMES) {
        // std::cout << "TStatusCode info " << tstatus_st.first << "," << tstatus_st.second
        //          << std::endl;
        if (tstatus_st.first < 1) {
            // OK with error code == 0, is not defined with tstatus, ignore it.
            continue;
        }
        EXPECT_TRUE(tstatus_st.first < ErrorCode::MAX_ERROR_CODE_DEFINE_NUM);
        EXPECT_TRUE(ErrorCode::error_states[tstatus_st.first].description.compare(
                            tstatus_st.second) == 0);
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
