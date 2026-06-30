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

#include "storage/index/snii/encoding/byte_source.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"

using namespace doris::snii;

TEST(SniiByteSource, RoundTripWithSink) {
    ByteSink s;
    s.put_fixed32(0xDEADBEEF);
    s.put_varint64(123456789);
    s.put_zigzag(-42);
    ByteSource src(s.view());
    uint32_t a;
    uint64_t b;
    int64_t c;
    ASSERT_TRUE(src.get_fixed32(&a).ok());
    EXPECT_EQ(a, 0xDEADBEEFU);
    ASSERT_TRUE(src.get_varint64(&b).ok());
    EXPECT_EQ(b, 123456789U);
    ASSERT_TRUE(src.get_zigzag(&c).ok());
    EXPECT_EQ(c, -42);
    EXPECT_TRUE(src.eof());
}

TEST(SniiByteSource, GetBytesAdvances) {
    ByteSink s;
    const uint8_t p[] = {1, 2, 3, 4, 5};
    s.put_bytes(Slice(p, 5));
    ByteSource src(s.view());
    Slice got;
    ASSERT_TRUE(src.get_bytes(3, &got).ok());
    ASSERT_EQ(got.size(), 3U);
    EXPECT_EQ(got[0], 1U);
    EXPECT_EQ(src.remaining(), 2U);
}

TEST(SniiByteSource, OverrunFails) {
    uint8_t one[1] = {0x01};
    ByteSource src(Slice(one, 1));
    uint32_t a;
    EXPECT_FALSE(src.get_fixed32(&a).ok());
}
