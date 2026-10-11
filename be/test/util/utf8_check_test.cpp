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

#include "util/utf8_check.h"

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace doris {

class Utf8CheckTest : public testing::Test {
protected:
    static void check(std::string_view input, bool expected) {
        // Exact-sized buffers let an instrumented validator detect reads past the input.
        auto data = std::make_unique<char[]>(input.size());
        std::memcpy(data.get(), input.data(), input.size());
        EXPECT_EQ(validate_utf8(data.get(), input.size()), expected);
    }

    /* positive tests */
    std::vector<std::string_view> pos = {{"", 0},
                                         {"\x00", 1},
                                         {"f", 1},
                                         {"\x7F", 1},
                                         {"\x00\x7F", 2},
                                         {"\x7F\x00", 2},
                                         {"\xC2\x80", 2},
                                         {"\xDF\xBF", 2},
                                         {"\xE0\xA0\x80", 3},
                                         {"\xE0\xA0\xBF", 3},
                                         {"\xED\x9F\x80", 3},
                                         {"\xEF\x80\xBF", 3},
                                         {"\xF0\x90\xBF\x80", 4},
                                         {"\xF2\x81\xBE\x99", 4},
                                         {"\xF4\x8F\x88\xAA", 4},
                                         {"\xED\x9F\xBF", 3},
                                         {"\xEE\x80\x80", 3},
                                         {"\xEF\xBF\xBF", 3},
                                         {"\xF0\x90\x80\x80", 4},
                                         {"\xF4\x8F\xBF\xBF", 4}};

    /* negative tests */
    std::vector<std::string_view> neg = {
            {"\xC2", 1},
            {"\xE0", 1},
            {"\xE0\xA0", 2},
            {"\xF0", 1},
            {"\xF0\x90", 2},
            {"\xF0\x90\x80", 3},
            {"\xF5\x80\x80\x80", 4},
            {"\xFE", 1},
            {"\xFF", 1},
            {"\x80", 1},
            {"\xBF", 1},
            {"\xC0\x80", 2},
            {"\xC1\x00", 2},
            {"\xC2\x7F", 2},
            {"\xDF\xC0", 2},
            {"\xE0\x9F\x80", 3},
            {"\xE0\xC2\x80", 3},
            {"\xED\xA0\x80", 3},
            {"\xED\x7F\x80", 3},
            {"\xEF\x80\x00", 3},
            {"\xF0\x8F\x80\x80", 4},
            {"\xF0\xEE\x80\x80", 4},
            {"\xF2\x90\x91\x7F", 4},
            {"\xF4\x90\x88\xAA", 4},
            {"\xF4\x00\xBF\xBF", 4},
            {"\x00\x00\x00\x00\x00\xC2\x80\x00\x00\x00\xE1\x80\x80\x00\x00\xC2"
             "\xC2\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
             32},
            {"\x00\x00\x00\x00\x00\xC2\xC2\x80\x00\x00\xE1\x80\x80\x00\x00\x00", 16},
            {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
             "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80",
             32},
            {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
             "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1",
             32},
            {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
             "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80"
             "\x80",
             33},
            {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
             "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF1\x80"
             "\xC2\x80",
             34},
            {"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
             "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF0"
             "\x80\x80\x80",
             35}};
};
TEST_F(Utf8CheckTest, empty) {
    EXPECT_TRUE(validate_utf8(nullptr, 0));
    check("", true);
}

TEST_F(Utf8CheckTest, normal) {
    for (const auto& value : pos) {
        check(value, true);
    }
}

TEST_F(Utf8CheckTest, abnormal) {
    for (const auto& value : neg) {
        check(value, false);
    }
}

TEST_F(Utf8CheckTest, embedded_nul) {
    check(std::string_view("a\0\xE4\xB8\xAD\0z", 7), true);
    check(std::string_view("a\0\xFF", 3), false);
    check(std::string_view("a\0\xE4\xB8", 4), false);
}

TEST_F(Utf8CheckTest, block_boundaries) {
    // Exercise every sequence across 16-, 32- and 64-byte SIMD boundaries,
    // both at the end of the input and followed by another complete block.
    for (size_t prefix_size = 0; prefix_size < 130; ++prefix_size) {
        SCOPED_TRACE(prefix_size);
        for (size_t suffix_size : {0, 1, 16, 32, 64}) {
            SCOPED_TRACE(suffix_size);
            const std::string prefix(prefix_size, 'a');
            const std::string suffix(suffix_size, 'b');
            for (const auto& value : pos) {
                check(prefix + std::string(value) + suffix, true);
            }
            for (const auto& value : neg) {
                check(prefix + std::string(value) + suffix, false);
            }
        }
    }
}

TEST_F(Utf8CheckTest, unaligned_inputs) {
    for (size_t offset = 0; offset < 64; ++offset) {
        SCOPED_TRACE(offset);
        for (size_t size : {1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 1024}) {
            SCOPED_TRACE(size);
            auto data = std::make_unique<char[]>(offset + size);
            std::memset(data.get(), 'a', offset + size);
            EXPECT_TRUE(validate_utf8(data.get() + offset, size));
            data[offset + size - 1] = '\xFF';
            EXPECT_FALSE(validate_utf8(data.get() + offset, size));
        }
    }
}

TEST_F(Utf8CheckTest, long_inputs) {
    for (size_t size : {1024, 4096, 65536}) {
        SCOPED_TRACE(size);
        std::string data(size, 'a');
        check(data, true);
        for (size_t offset : {size_t(0), size / 2, size - 1}) {
            data[offset] = '\xFF';
            check(data, false);
            data[offset] = 'a';
        }
        data.clear();
        for (size_t i = 0; i < size; ++i) {
            data += "a\xE4\xB8\xAD\xF0\x9F\x98\x80";
        }
        check(data, true);
        data.pop_back();
        check(data, false);
    }
}

TEST_F(Utf8CheckTest, independent_inputs) {
    // A truncated sequence in one row must not consume the next row's bytes.
    const std::string data = "\xE4\xB8\xAD";
    EXPECT_FALSE(validate_utf8(data.data(), 2));
    EXPECT_FALSE(validate_utf8(data.data() + 2, 1));
    EXPECT_TRUE(validate_utf8(data.data(), data.size()));
}

TEST_F(Utf8CheckTest, file_scan_validation_setting) {
    TFileScanRangeParams params;
    const std::string invalid = "\xFF";
    EXPECT_FALSE(validate_utf8(params, invalid.data(), invalid.size()));

    TFileAttributes attributes;
    params.__set_file_attributes(attributes);
    EXPECT_FALSE(validate_utf8(params, invalid.data(), invalid.size()));

    attributes.__set_enable_text_validate_utf8(false);
    params.__set_file_attributes(attributes);
    EXPECT_TRUE(validate_utf8(params, invalid.data(), invalid.size()));

    attributes.__set_enable_text_validate_utf8(true);
    params.__set_file_attributes(attributes);
    EXPECT_FALSE(validate_utf8(params, invalid.data(), invalid.size()));
    EXPECT_TRUE(validate_utf8(params, "valid", 5));
    EXPECT_TRUE(validate_utf8(params, nullptr, 0));
}

} // namespace doris
