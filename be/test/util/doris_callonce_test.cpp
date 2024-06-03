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

    Status st = call1.call([&]() -> Status { return Status::OK(); });
    EXPECT_EQ(call1.has_called(), true);
    EXPECT_EQ(call1.stored_result().code(), ErrorCode::OK);

    st = call1.call([&]() -> Status { return Status::InternalError(""); });
    EXPECT_EQ(call1.has_called(), true);
    // The error code should not changed
    EXPECT_EQ(call1.stored_result().code(), ErrorCode::OK);
}

// Test that, if the string contents is shorter than the initial capacity
// of the faststring, shrink_to_fit() leaves the string in the built-in
// array.
TEST_F(DorisCallOnceTest, TestErrorHappens) {
    DorisCallOnce<Status> call1;
    EXPECT_EQ(call1.has_called(), false);

    Status st = call1.call([&]() -> Status { return Status::InternalError(""); });
    EXPECT_EQ(call1.has_called(), true);
    EXPECT_EQ(call1.stored_result().code(), ErrorCode::INTERNAL_ERROR);

    st = call1.call([&]() -> Status { return Status::OK(); });
    EXPECT_EQ(call1.has_called(), true);
    // The error code should not changed
    EXPECT_EQ(call1.stored_result().code(), ErrorCode::INTERNAL_ERROR);
}

TEST_F(DorisCallOnceTest, TestExceptionHappens) {
    DorisCallOnce<Status> call1;
    EXPECT_EQ(call1.has_called(), false);
    bool exception_occured = false;
    bool runtime_occured = false;
    try {
        Status st = call1.call([&]() -> Status {
            throw std::exception();
            return Status::InternalError("");
        });
    } catch (...) {
        // Exception has to throw to the call method
        exception_occured = true;
    }
    EXPECT_EQ(exception_occured, true);
    EXPECT_EQ(call1.has_called(), true);

    // call again, should throw the same exception.
    exception_occured = false;
    runtime_occured = false;
    try {
        Status st = call1.call([&]() -> Status { return Status::InternalError(""); });
    } catch (...) {
        // Exception has to throw to the call method
        exception_occured = true;
    }
    EXPECT_EQ(exception_occured, true);
    EXPECT_EQ(call1.has_called(), true);

    // If call get result, should catch exception.
    exception_occured = false;
    runtime_occured = false;
    try {
        Status st = call1.stored_result();
    } catch (...) {
        // Exception has to throw to the call method
        exception_occured = true;
    }
    EXPECT_EQ(exception_occured, true);

    // Test the exception should actually the same one throwed by the callback method.
    DorisCallOnce<Status> call2;
    exception_occured = false;
    runtime_occured = false;
    try {
        try {
            Status st = call2.call([&]() -> Status {
                throw std::runtime_error("runtime error happens");
                return Status::InternalError("");
            });
        } catch (const std::runtime_error&) {
            // Exception has to throw to the call method
            runtime_occured = true;
        }
    } catch (...) {
        // Exception has to throw to the call method, the runtime error is captured,
        // so this code will not hit.
        exception_occured = true;
    }
    EXPECT_EQ(runtime_occured, true);
    EXPECT_EQ(exception_occured, false);

    // Test the exception should actually the same one throwed by the callback method.
    DorisCallOnce<Status> call3;
    exception_occured = false;
    runtime_occured = false;
    try {
        try {
            Status st = call3.call([&]() -> Status {
                throw std::exception();
                return Status::InternalError("");
            });
        } catch (const std::runtime_error&) {
            // Exception has to throw to the call method, but not runtime error
            // so that this code will not hit
            runtime_occured = true;
        }
    } catch (...) {
        // Exception has to throw to the call method, the runtime error is not captured,
        // so this code will  hit.
        exception_occured = true;
    }
    EXPECT_EQ(runtime_occured, false);
    EXPECT_EQ(exception_occured, true);
}

} // namespace doris
