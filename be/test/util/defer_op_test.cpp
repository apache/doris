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

#include "util/defer_op.h"

#include <gtest/gtest.h>

namespace doris {

TEST(DeferOpTest, ThrowEscapesWhenNotUnwinding) {
    bool threw = false;
    try {
        doris::Defer guard([]() { throw std::runtime_error("defer-throws"); });
        // destructor will run at scope exit and should propagate the exception
    } catch (const std::runtime_error& e) {
        threw = true;
        EXPECT_STREQ(e.what(), "defer-throws");
    }
    EXPECT_TRUE(threw);
}

TEST(DeferOpTest, SwallowDuringUnwind) {
    // This test ensures that if we're already unwinding, the Defer's exception
    // does not call std::terminate. To actually run the Defer destructor
    // during stack unwinding we must create it in a frame that is being
    // unwound. Creating it inside a catch-handler would be after unwind
    // completed, so that does not test the intended behavior.
    bool reached_catch = false;

    auto throwing_func = []() {
        doris::Defer guard([]() { throw std::runtime_error("inner-defer"); });
        // throwing here will cause stack unwind while `guard` is destroyed
        throw std::runtime_error("outer");
    };

    try {
        throwing_func();
    } catch (const std::runtime_error& e) {
        // We should catch the outer exception here; the inner exception
        // thrown by the Defer's closure must have been swallowed during
        // the unwind (otherwise we'd have terminated or a different
        // exception would propagate).
        reached_catch = true;
        EXPECT_STREQ(e.what(), "outer");
    }

    EXPECT_TRUE(reached_catch);
}

} // namespace doris
