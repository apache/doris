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

// Regression test for unaligned __int128 dereference UB.
//
// Several call sites used to dereference a `__int128*` produced from a
// `StringRef::data` (or similar byte pointer) without any alignment
// guarantee. This file pins the contract that the helpers that build
// literal TExprNodes from a raw `const void* data` pointer must accept
// pointers that are *not* 16-byte aligned, since on alignment-strict
// platforms (e.g. some aarch64 / SPARC builds, and UBSan
// -fsanitize=alignment) such reads are undefined behavior and may
// SIGBUS.

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "core/value/decimalv2_value.h"
#include "core/value/large_int_value.h"
#include "exprs/vexpr.h"

namespace doris {

// Returns a pointer guaranteed to be 1 byte off any 16-byte boundary.
static char* misaligned_slot(std::vector<char>& buf, std::size_t bytes) {
    buf.assign(bytes + 32, 0);
    char* base = buf.data();
    // Move forward until we land on an odd address.
    std::size_t off = 0;
    while ((reinterpret_cast<std::uintptr_t>(base + off) & 0xF) != 1) {
        ++off;
    }
    return base + off;
}

TEST(UnalignedInt128Test, LargeIntLiteralFromUnalignedBuffer) {
    std::vector<char> buf;
    char* p = misaligned_slot(buf, sizeof(__int128));
    ASSERT_NE(reinterpret_cast<std::uintptr_t>(p) % alignof(__int128), 0u);

    // 2^126 - 1: a value that uses both 64-bit halves.
    __int128 expected = (static_cast<__int128>(0x3FFFFFFFFFFFFFFFLL) << 64) |
                        static_cast<__int128>(0xFEEDFACECAFEBEEFULL);
    std::memcpy(p, &expected, sizeof(expected));

    TExprNode node;
    Status st = create_texpr_literal_node<TYPE_LARGEINT>(p, &node);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(node.__isset.large_int_literal);
    EXPECT_EQ(node.large_int_literal.value, LargeIntValue::to_string(expected));
}

TEST(UnalignedInt128Test, Decimal128ILiteralFromUnalignedBuffer) {
    std::vector<char> buf;
    char* p = misaligned_slot(buf, sizeof(__int128));
    ASSERT_NE(reinterpret_cast<std::uintptr_t>(p) % alignof(__int128), 0u);

    // Decimal(20, 4) value: 1234567890123456.7890
    __int128 raw = static_cast<__int128>(1234567890123456789LL) * 10 + 1;
    std::memcpy(p, &raw, sizeof(raw));

    TExprNode node;
    Status st = create_texpr_literal_node<TYPE_DECIMAL128I>(p, &node, /*precision=*/20,
                                                            /*scale=*/4);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(node.__isset.decimal_literal);
    // Sanity: the formatted value must contain "1234567890123456".
    EXPECT_NE(node.decimal_literal.value.find("1234567890123456"), std::string::npos)
            << node.decimal_literal.value;
}

TEST(UnalignedInt128Test, DecimalV2LiteralFromUnalignedBuffer) {
    std::vector<char> buf;
    char* p = misaligned_slot(buf, sizeof(DecimalV2Value));
    ASSERT_NE(reinterpret_cast<std::uintptr_t>(p) % alignof(__int128), 0u);

    DecimalV2Value src;
    // 12345.6789 * 1e9 (DecimalV2 internal scale = 9).
    src.set_value(static_cast<__int128>(12345678900000LL));
    std::memcpy(p, &src, sizeof(src));

    TExprNode node;
    Status st = create_texpr_literal_node<TYPE_DECIMALV2>(p, &node, /*precision=*/27,
                                                          /*scale=*/9);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(node.__isset.decimal_literal);
    EXPECT_EQ(node.decimal_literal.value, src.to_string());
}

} // namespace doris
