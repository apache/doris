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

// the following code are modified from RocksDB:
// https://github.com/facebook/rocksdb/blob/master/util/crc32c_test.cc

#include <crc32c/crc32c.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <string.h>

#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "util/slice.h"

namespace doris {

TEST(CRC, StandardResults) {
    // Original Fast_CRC32 tests.
    // From rfc3720 section B.4.
    char buf[32];

    memset(buf, 0, sizeof(buf));
    EXPECT_EQ(0x8a9136aaU, crc32c::Crc32c(buf, sizeof(buf)));

    memset(buf, 0xff, sizeof(buf));
    EXPECT_EQ(0x62a8ab43U, crc32c::Crc32c(buf, sizeof(buf)));

    for (int i = 0; i < 32; i++) {
        buf[i] = static_cast<char>(i);
    }
    EXPECT_EQ(0x46dd794eU, crc32c::Crc32c(buf, sizeof(buf)));

    for (int i = 0; i < 32; i++) {
        buf[i] = static_cast<char>(31 - i);
    }
    EXPECT_EQ(0x113fdb5cU, crc32c::Crc32c(buf, sizeof(buf)));

    unsigned char data[48] = {
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
            0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    EXPECT_EQ(0xd9963a56, crc32c::Crc32c(reinterpret_cast<char*>(data), sizeof(data)));
}

TEST(CRC, Values) {
    EXPECT_NE(crc32c::Crc32c(std::string("a")), crc32c::Crc32c(std::string("foo")));
}

TEST(CRC, Extend) {
    auto s1 = std::string("hello ");
    auto s2 = std::string("world");
    EXPECT_EQ(crc32c::Crc32c(std::string("hello world")),
              crc32c::Extend(crc32c::Crc32c(s1), (const uint8_t*)s2.data(), s2.size()));
    std::vector<std::string_view> slices = {s1, s2};
    EXPECT_EQ(crc32c::Crc32c(std::string("hello world")),
              crc32c::Extend(crc32c::Crc32c(slices[0]), (const uint8_t*)s2.data(), s2.size()));
}

} // namespace doris
