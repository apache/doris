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

#include "snii/encoding/section_framer.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "snii/encoding/byte_sink.h"
#include "snii/encoding/byte_source.h"

using namespace snii;

TEST(SniiSectionFramer, RoundTrip) {
    ByteSink sink;
    const uint8_t p[] = {9, 8, 7};
    SectionFramer::write(sink, 0x42, Slice(p, 3));
    ByteSource src(sink.view());
    FramedSection sec;
    ASSERT_TRUE(SectionFramer::read(src, &sec).ok());
    EXPECT_EQ(sec.type, 0x42);
    ASSERT_EQ(sec.payload.size(), 3U);
    EXPECT_EQ(sec.payload[0], 9U);
    EXPECT_TRUE(src.eof());
}

TEST(SniiSectionFramer, DetectsCorruption) {
    ByteSink sink;
    const uint8_t p[] = {1, 2, 3, 4};
    SectionFramer::write(sink, 1, Slice(p, 4));
    auto bytes = sink.buffer();
    bytes[3] ^= 0xFF; // flip one byte in the payload
    Slice corrupted(bytes);
    ByteSource src(corrupted);
    FramedSection sec;
    EXPECT_FALSE(SectionFramer::read(src, &sec).ok());
}

TEST(SniiSectionFramer, SkipMultiple) {
    ByteSink sink;
    const uint8_t a[] = {1};
    const uint8_t b[] = {2, 2};
    SectionFramer::write(sink, 10, Slice(a, 1));
    SectionFramer::write(sink, 11, Slice(b, 2));
    ByteSource src(sink.view());
    FramedSection s1, s2;
    ASSERT_TRUE(SectionFramer::read(src, &s1).ok());
    ASSERT_TRUE(SectionFramer::read(src, &s2).ok());
    EXPECT_EQ(s1.type, 10);
    EXPECT_EQ(s2.type, 11);
    EXPECT_TRUE(src.eof());
}

TEST(SniiSectionFramer, EmptyPayload) {
    ByteSink sink;
    SectionFramer::write(sink, 7, Slice());
    ByteSource src(sink.view());
    FramedSection sec;
    ASSERT_TRUE(SectionFramer::read(src, &sec).ok());
    EXPECT_EQ(sec.type, 7);
    EXPECT_EQ(sec.payload.size(), 0U);
}
