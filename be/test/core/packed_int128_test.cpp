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

#include "core/packed_int128.h"

#include <gtest/gtest.h>

#include <array>
#include <cstring>

namespace doris {

TEST(PackedInt128Test, SignedPackedValueSupportsCopyAndAssignment) {
    const __int128 positive = (static_cast<__int128>(1) << 100) + 12345;
    const __int128 negative = -((static_cast<__int128>(1) << 96) + 54321);

    PackedInt128 packed(positive);
    EXPECT_TRUE(packed.value == positive);

    PackedInt128 copied(packed);
    EXPECT_TRUE(copied.value == positive);

    copied = negative;
    EXPECT_TRUE(copied.value == negative);

    PackedInt128 assigned;
    assigned = copied;
    EXPECT_TRUE(assigned.value == negative);
}

TEST(PackedInt128Test, UnsignedPackedValueSupportsCopyAndAssignment) {
    const uint128_t first = (static_cast<uint128_t>(1) << 100) + 12345;
    const uint128_t second = (static_cast<uint128_t>(1) << 96) + 54321;

    PackedUInt128 packed(first);
    EXPECT_TRUE(packed.value == first);

    PackedUInt128 copied(packed);
    EXPECT_TRUE(copied.value == first);

    copied = second;
    EXPECT_TRUE(copied.value == second);

    PackedUInt128 assigned;
    assigned = copied;
    EXPECT_TRUE(assigned.value == second);
}

TEST(PackedInt128Test, ReadInt128FromUnalignedAddress) {
    const int128_t expected = -((static_cast<int128_t>(1) << 100) + 98765);
    std::array<char, sizeof(int128_t) + 1> buffer {};
    std::memcpy(buffer.data() + 1, &expected, sizeof(expected));

    EXPECT_TRUE(get_int128_from_unalign(buffer.data() + 1) == expected);
}

} // namespace doris
