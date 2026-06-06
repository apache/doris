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

#pragma once

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstddef>
#include <ios>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace doris {

[[noreturn]] void doris_check_fail(std::string_view message);

namespace detail {
template <typename T>
concept OstreamPrintable = requires(std::ostream& os, const T& value) { os << value; };

class DorisCheckMessage {
public:
    explicit DorisCheckMessage(std::string_view message) { _stream << message; }

    template <typename T>
    DorisCheckMessage& operator<<(const T& value) {
        _stream << value;
        return *this;
    }

    DorisCheckMessage& operator<<(std::ostream& (*func)(std::ostream&)) {
        func(_stream);
        return *this;
    }

    DorisCheckMessage& operator<<(std::ios& (*func)(std::ios&)) {
        func(_stream);
        return *this;
    }

    DorisCheckMessage& operator<<(std::ios_base& (*func)(std::ios_base&)) {
        func(_stream);
        return *this;
    }

    [[noreturn]] void fail() { doris_check_fail(_stream.str()); }

private:
    std::ostringstream _stream;
};

class DorisCheckMessageVoidify {
public:
    [[noreturn]] void operator&(DorisCheckMessage& message) const { message.fail(); }
    [[noreturn]] void operator&(DorisCheckMessage&& message) const { message.fail(); }
};

class DorisCheckResult {
public:
    explicit DorisCheckResult(bool ok) : _ok(ok) {}
    explicit DorisCheckResult(std::string message) : _ok(false), _message(std::move(message)) {}

    bool ok() const { return _ok; }
    const std::string& message() const { return _message; }

private:
    bool _ok;
    std::string _message;
};

template <typename T>
std::string doris_check_value_to_string(const T& value) {
    if constexpr (std::is_same_v<std::decay_t<T>, std::nullptr_t>) {
        return "nullptr";
    } else if constexpr (std::is_same_v<std::decay_t<T>, bool>) {
        return value ? "true" : "false";
    } else if constexpr (OstreamPrintable<T>) {
        std::ostringstream oss;
        oss << std::boolalpha << value;
        return oss.str();
    } else {
        return "<unprintable>";
    }
}

template <typename Lhs, typename Rhs, typename Comparator>
DorisCheckResult doris_check_binary_op_result(const Lhs& lhs, const Rhs& rhs,
                                              std::string_view lhs_expr, std::string_view rhs_expr,
                                              std::string_view op_expr, Comparator comparator) {
    if (static_cast<bool>(comparator(lhs, rhs))) {
        return DorisCheckResult(true);
    }
    return DorisCheckResult(fmt::format("Check failed: {} {} {} ({} vs {})", lhs_expr, op_expr,
                                        rhs_expr, doris_check_value_to_string(lhs),
                                        doris_check_value_to_string(rhs)));
}
} // namespace detail

} // namespace doris

// core in Debug mode, exception in Release mode.
#define DORIS_CHECK(stmt)                                                  \
    if (bool _doris_check_ok = static_cast<bool>(stmt); _doris_check_ok) { \
    } else [[unlikely]]                                                    \
        ::doris::detail::DorisCheckMessageVoidify() &                      \
                ::doris::detail::DorisCheckMessage("Check failed: " #stmt)

// Use DORIS_CHECK_* only for invariants that must also be checked in Release builds.
// Keep DCHECK_* in loops or other hot paths where Release checks would add overhead.
#ifndef NDEBUG
#define DORIS_CHECK_EQ(val1, val2) DCHECK_EQ(val1, val2)
#define DORIS_CHECK_NE(val1, val2) DCHECK_NE(val1, val2)
#define DORIS_CHECK_LT(val1, val2) DCHECK_LT(val1, val2)
#define DORIS_CHECK_LE(val1, val2) DCHECK_LE(val1, val2)
#define DORIS_CHECK_GT(val1, val2) DCHECK_GT(val1, val2)
#define DORIS_CHECK_GE(val1, val2) DCHECK_GE(val1, val2)
#else
#define DORIS_CHECK_BINARY_OP(val1, val2, op, op_str)                             \
    if (auto _doris_check_result = ::doris::detail::doris_check_binary_op_result( \
                (val1), (val2), #val1, #val2, op_str,                             \
                [](const auto& _doris_check_lhs, const auto& _doris_check_rhs) {  \
                    return _doris_check_lhs op _doris_check_rhs;                  \
                });                                                               \
        _doris_check_result.ok()) {                                               \
    } else [[unlikely]]                                                           \
        ::doris::detail::DorisCheckMessageVoidify() &                             \
                ::doris::detail::DorisCheckMessage(_doris_check_result.message())

#define DORIS_CHECK_EQ(val1, val2) DORIS_CHECK_BINARY_OP(val1, val2, ==, "==")
#define DORIS_CHECK_NE(val1, val2) DORIS_CHECK_BINARY_OP(val1, val2, !=, "!=")
#define DORIS_CHECK_LT(val1, val2) DORIS_CHECK_BINARY_OP(val1, val2, <, "<")
#define DORIS_CHECK_LE(val1, val2) DORIS_CHECK_BINARY_OP(val1, val2, <=, "<=")
#define DORIS_CHECK_GT(val1, val2) DORIS_CHECK_BINARY_OP(val1, val2, >, ">")
#define DORIS_CHECK_GE(val1, val2) DORIS_CHECK_BINARY_OP(val1, val2, >=, ">=")
#endif
