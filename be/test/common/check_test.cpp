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

#include "common/check.h"

#include <gtest/gtest.h>

#include <ios>
#include <string>
#include <utility>

#include "common/exception.h"

namespace doris {

namespace {

struct NotOstreamPrintable {};

std::ios& TouchIos(std::ios& ios) {
    return ios;
}

#ifdef NDEBUG
template <typename Fn>
std::string CaptureDorisCheckException(Fn&& fn) {
    try {
        std::forward<Fn>(fn)();
    } catch (const Exception& e) {
        return e.to_string();
    }
    ADD_FAILURE() << "Expected doris::Exception";
    return {};
}
#endif

} // namespace

TEST(DorisCheckTest, CheckFailHandlesFatalError) {
#ifndef NDEBUG
    EXPECT_DEATH(doris_check_fail("manual failure"), "manual failure");
#else
    const auto message = CaptureDorisCheckException([] { doris_check_fail("manual failure"); });
    EXPECT_NE(message.find("[FATAL_ERROR]"), std::string::npos) << message;
    EXPECT_NE(message.find("manual failure"), std::string::npos) << message;
#endif
}

TEST(DorisCheckTest, ValueToString) {
    EXPECT_EQ(detail::doris_check_value_to_string(nullptr), "nullptr");
    EXPECT_EQ(detail::doris_check_value_to_string(true), "true");
    EXPECT_EQ(detail::doris_check_value_to_string(false), "false");
    EXPECT_EQ(detail::doris_check_value_to_string(123), "123");
    EXPECT_EQ(detail::doris_check_value_to_string(std::string("value")), "value");
    EXPECT_EQ(detail::doris_check_value_to_string(NotOstreamPrintable {}), "<unprintable>");
}

TEST(DorisCheckTest, BinaryOpResult) {
    auto ok_result = detail::doris_check_binary_op_result(
            1, 2, "lhs", "rhs", "<", [](const auto& lhs, const auto& rhs) { return lhs < rhs; });
    EXPECT_TRUE(ok_result.ok());
    EXPECT_TRUE(ok_result.message().empty());

    auto failed_result = detail::doris_check_binary_op_result(
            1, 2, "lhs", "rhs", ">", [](const auto& lhs, const auto& rhs) { return lhs > rhs; });
    EXPECT_FALSE(failed_result.ok());
    EXPECT_EQ(failed_result.message(), "Check failed: lhs > rhs (1 vs 2)");

    auto bool_failed_result = detail::doris_check_binary_op_result(
            true, false, "lhs", "rhs",
            "==", [](const auto& lhs, const auto& rhs) { return lhs == rhs; });
    EXPECT_FALSE(bool_failed_result.ok());
    EXPECT_EQ(bool_failed_result.message(), "Check failed: lhs == rhs (true vs false)");
}

TEST(DorisCheckTest, CheckMessageSupportsStreaming) {
#ifndef NDEBUG
    EXPECT_DEATH(DORIS_CHECK(false) << " with context " << std::boolalpha << false << std::endl
                                    << "done",
                 "Check failed: false with context false");

    EXPECT_DEATH(
            {
                detail::DorisCheckMessage check_message("lvalue message ");
                check_message << TouchIos << 42;
                detail::DorisCheckMessageVoidify() & check_message;
            },
            "lvalue message 42");

    EXPECT_DEATH(detail::DorisCheckMessageVoidify() & detail::DorisCheckMessage("rvalue message"),
                 "rvalue message");
#else
    const auto message = CaptureDorisCheckException([] {
        DORIS_CHECK(false) << " with context " << std::boolalpha << false << std::endl << "done";
    });
    EXPECT_NE(message.find("Check failed: false with context false"), std::string::npos) << message;
    EXPECT_NE(message.find("done"), std::string::npos) << message;

    const auto lvalue_message = CaptureDorisCheckException([] {
        detail::DorisCheckMessage check_message("lvalue message ");
        check_message << TouchIos << 42;
        detail::DorisCheckMessageVoidify() & check_message;
    });
    EXPECT_NE(lvalue_message.find("lvalue message 42"), std::string::npos) << lvalue_message;

    const auto rvalue_message = CaptureDorisCheckException([] {
        detail::DorisCheckMessageVoidify() & detail::DorisCheckMessage("rvalue message");
    });
    EXPECT_NE(rvalue_message.find("rvalue message"), std::string::npos) << rvalue_message;
#endif
}

TEST(DorisCheckTest, CheckMacroSkipsMessageWhenOk) {
    int stream_value = 0;
    DORIS_CHECK(true) << ++stream_value;
    EXPECT_EQ(stream_value, 0);
}

TEST(DorisCheckTest, ComparisonMacrosEvaluateOnceAndCheckSuccess) {
    DORIS_CHECK_EQ(1, 1);
    DORIS_CHECK_NE(1, 2);
    DORIS_CHECK_LT(1, 2);
    DORIS_CHECK_LE(1, 1);
    DORIS_CHECK_GT(2, 1);
    DORIS_CHECK_GE(1, 1);

    int lhs = 0;
    int rhs = 0;
    DORIS_CHECK_EQ(++lhs, ++rhs);
    EXPECT_EQ(lhs, 1);
    EXPECT_EQ(rhs, 1);
}

#ifdef NDEBUG
TEST(DorisCheckTest, ComparisonMacrosThrowInRelease) {
    const auto message =
            CaptureDorisCheckException([] { DORIS_CHECK_EQ(1, 2) << " with context " << 43; });
    EXPECT_NE(message.find("Check failed: 1 == 2 (1 vs 2) with context 43"), std::string::npos)
            << message;
}
#elif DCHECK_IS_ON()
TEST(DorisCheckTest, ComparisonMacrosDcheckInDebug) {
    EXPECT_DEATH(DORIS_CHECK_EQ(1, 2), "Check failed");
}
#endif

} // namespace doris
