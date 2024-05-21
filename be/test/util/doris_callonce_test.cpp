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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <time.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "util/once.h"

namespace doris {
class DorisCallOnceTest : public ::testing::Test {};

TEST_F(DorisCallOnceTest, TestNormal) {
    DorisCallOnce<Status> call1;
    EXPECT_EQ(call1.has_called(), false);

    call1.call([&]() -> Status { return Status::OK(); });
    EXPECT_EQ(call1.has_called(), true);
    EXPECT_EQ(call1.stored_result().error_code(), ErrorCode::OK);

    call1.call([&]() -> Status { return Status::InternalError(""); });
    EXPECT_EQ(call1.has_called(), true);
    // The error code should not changed
    EXPECT_EQ(call1.stored_result().error_code(), ErrorCode::OK);
}

// Test that, if the string contents is shorter than the initial capacity
// of the faststring, shrink_to_fit() leaves the string in the built-in
// array.
TEST_F(DorisCallOnceTest, TestErrorHappens) {
    DorisCallOnce<Status> call1;
    EXPECT_EQ(call1.has_called(), false);

    call1.call([&]() -> Status { return Status::InternalError(""); });
    EXPECT_EQ(call1.has_called(), true);
    EXPECT_EQ(call1.stored_result().error_code(), ErrorCode::INTERNAL_ERROR);

    call1.call([&]() -> Status { return Status::OK(); });
    EXPECT_EQ(call1.has_called(), true);
    // The error code should not changed
    EXPECT_EQ(call1.stored_result().error_code(), ErrorCode::INTERNAL_ERROR);
}

TEST_F(DorisCallOnceTest, TestExceptionHappens) {
    DorisCallOnce<Status> call1;
    EXPECT_EQ(call1.has_called(), false);
    bool exception_occured = false;
    try {
        call1.call([&]() -> Status {
            throw std::exception();
            return Status::InternalError("");
        });
    } catch (...) {
        // Exception has to throw to the call method
        exception_occured = true;
    }

    EXPECT_EQ(exception_occured, true);
    EXPECT_EQ(call1.has_called(), false);
    EXPECT_EQ(call1.stored_result().error_code(), ErrorCode::OK);

    call1.call([&]() -> Status { return Status::InternalError(""); });
    EXPECT_EQ(call1.has_called(), true);
    // The error code should not changed
    EXPECT_EQ(call1.stored_result().error_code(), ErrorCode::INTERNAL_ERROR);
}

} // namespace doris
