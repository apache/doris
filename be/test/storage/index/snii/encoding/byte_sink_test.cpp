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

#include "storage/index/snii/encoding/byte_sink.h"

#include <gtest/gtest.h>

#include <cstdint>

#include "common/status.h"
#include "storage/index/snii/encoding/varint.h"

using namespace doris::snii;

TEST(SniiByteSink, Fixed32LittleEndian) {
    ByteSink s;
    s.put_fixed32(0x04030201U);
    ASSERT_EQ(s.size(), 4U);
    const auto& b = s.buffer();
    EXPECT_EQ(b[0], 0x01);
    EXPECT_EQ(b[1], 0x02);
    EXPECT_EQ(b[3], 0x04);
}

TEST(SniiByteSink, Fixed64LittleEndian) {
    ByteSink s;
    s.put_fixed64(0x0807060504030201ULL);
    ASSERT_EQ(s.size(), 8U);
    EXPECT_EQ(s.buffer()[0], 0x01);
    EXPECT_EQ(s.buffer()[7], 0x08);
}

TEST(SniiByteSink, VarintThenBytes) {
    ByteSink s;
    s.put_varint32(300);
    const uint8_t payload[] = {0xAA, 0xBB};
    s.put_bytes(Slice(payload, 2));
    EXPECT_EQ(s.size(), varint_len(300) + 2);
}
