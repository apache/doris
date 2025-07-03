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

#include <fmt/core.h>

#include <ostream>

namespace doris::cloud {

enum class [[nodiscard]] TxnErrorCode : int {
    TXN_OK = 0,
    TXN_KEY_NOT_FOUND = 1,
    TXN_CONFLICT = -1,
    TXN_TOO_OLD = -2,
    TXN_MAYBE_COMMITTED = -3,
    TXN_RETRYABLE_NOT_COMMITTED = -4,
    TXN_TIMEOUT = -5,
    TXN_INVALID_ARGUMENT = -6,
    TXN_KEY_TOO_LARGE = -7,
    TXN_VALUE_TOO_LARGE = -8,
    TXN_BYTES_TOO_LARGE = -9,
    // other unidentified errors.
    TXN_UNIDENTIFIED_ERROR = -10,
};

inline const char* format_as(TxnErrorCode code) {
    // clang-format off
    switch (code) {
    case TxnErrorCode::TXN_OK: return "Ok";
    case TxnErrorCode::TXN_KEY_NOT_FOUND: return "KeyNotFound";
    case TxnErrorCode::TXN_CONFLICT: return "Conflict";
    case TxnErrorCode::TXN_TOO_OLD: return "TxnTooOld";
    case TxnErrorCode::TXN_MAYBE_COMMITTED: return "MaybeCommitted";
    case TxnErrorCode::TXN_RETRYABLE_NOT_COMMITTED: return "RetryableNotCommitted";
    case TxnErrorCode::TXN_TIMEOUT: return "Timeout";
    case TxnErrorCode::TXN_INVALID_ARGUMENT: return "InvalidArgument";
    case TxnErrorCode::TXN_KEY_TOO_LARGE: return "Key length exceeds limit";
    case TxnErrorCode::TXN_VALUE_TOO_LARGE: return "Value length exceeds limit";
    case TxnErrorCode::TXN_BYTES_TOO_LARGE: return "Transaction exceeds byte limit";
    case TxnErrorCode::TXN_UNIDENTIFIED_ERROR: return "Unknown";
    }
    return "NotImplemented";
    // clang-format on
}

inline std::ostream& operator<<(std::ostream& out, TxnErrorCode code) {
    out << format_as(code);
    return out;
}

} // namespace doris::cloud

template <>
struct fmt::formatter<doris::cloud::TxnErrorCode> {
    constexpr auto parse(format_parse_context& ctx) -> format_parse_context::iterator {
        return ctx.begin();
    }

    auto format(const doris::cloud::TxnErrorCode& code, format_context& ctx) const
            -> format_context::iterator {
        return fmt::format_to(ctx.out(), "{}", doris::cloud::format_as(code));
    }
};
