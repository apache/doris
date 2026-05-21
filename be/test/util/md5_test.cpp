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

#include "util/md5.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest_pred_impl.h"

namespace doris {
namespace {

std::string scalar_md5_hex(const std::string& input) {
    Md5Digest digest;
    digest.update(input.data(), input.size());
    digest.digest();
    return digest.hex();
}

std::string make_patterned_input(size_t length, size_t seed) {
    std::string input(length, '\0');
    for (size_t i = 0; i < length; ++i) {
        input[i] = static_cast<char>('!' + ((i * 17 + seed * 31) % 94));
    }
    return input;
}

std::string make_binary_input(size_t length, size_t seed) {
    std::string input(length, '\0');
    for (size_t i = 0; i < length; ++i) {
        input[i] = static_cast<char>((i * 37 + seed * 13) & 0xff);
    }
    return input;
}

bool is_lower_hex_digest(const std::string& value) {
    return value.size() == MD5_HEX_LENGTH && std::all_of(value.begin(), value.end(), [](char c) {
               return ('0' <= c && c <= '9') || ('a' <= c && c <= 'f');
           });
}

void expect_batch_matches_scalar(const std::vector<std::string>& inputs) {
    if (inputs.empty()) {
        std::array<char, MD5_HEX_LENGTH> sentinel;
        sentinel.fill('x');
        md5_hex_batch(nullptr, nullptr, sentinel.data(), 0);
        EXPECT_TRUE(std::all_of(sentinel.begin(), sentinel.end(), [](char c) { return c == 'x'; }));
        return;
    }

    std::vector<const unsigned char*> data(inputs.size());
    std::vector<size_t> lengths(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
        data[i] = reinterpret_cast<const unsigned char*>(inputs[i].data());
        lengths[i] = inputs[i].size();
    }

    std::vector<char> output(inputs.size() * MD5_HEX_LENGTH);
    md5_hex_batch(data.data(), lengths.data(), output.data(), inputs.size());

    for (size_t i = 0; i < inputs.size(); ++i) {
        const std::string actual(output.data() + i * MD5_HEX_LENGTH, MD5_HEX_LENGTH);
        EXPECT_TRUE(is_lower_hex_digest(actual))
                << "input index " << i << ", length " << inputs[i].size();
        EXPECT_EQ(scalar_md5_hex(inputs[i]), actual)
                << "input index " << i << ", length " << inputs[i].size();
    }
}

} // namespace

class Md5Test : public testing::Test {
public:
    Md5Test() {}
    virtual ~Md5Test() {}
};

TEST_F(Md5Test, empty) {
    Md5Digest digest;
    digest.digest();
    EXPECT_STREQ("d41d8cd98f00b204e9800998ecf8427e", digest.hex().c_str());
}

TEST_F(Md5Test, normal) {
    Md5Digest digest;
    digest.update("abcdefg", 7);
    digest.digest();
    EXPECT_STREQ("7ac66c0f148de9519b8bd264312c4d64", digest.hex().c_str());
}

TEST_F(Md5Test, batch_known_vectors) {
    const std::vector<std::pair<std::string, std::string>> cases = {
            {"", "d41d8cd98f00b204e9800998ecf8427e"},
            {"a", "0cc175b9c0f1b6a831c399e269772661"},
            {"abc", "900150983cd24fb0d6963f7d28e17f72"},
            {"message digest", "f96b697d7cb7938d525a2f31aaf161d0"},
            {"abcdefghijklmnopqrstuvwxyz", "c3fcd3d76192e4007dfb496cca67e13b"},
            {"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
             "d174ab98d277d9f5a5611c2c9f419d9f"},
            {"1234567890123456789012345678901234567890123456789012345678901234567890"
             "1234567890",
             "57edf4a22be3c955ac49da2e2107b67a"}};

    std::vector<std::string> inputs;
    std::vector<const unsigned char*> data(cases.size());
    std::vector<size_t> lengths(cases.size());
    inputs.reserve(cases.size());
    for (const auto& entry : cases) {
        inputs.push_back(entry.first);
    }
    for (size_t i = 0; i < inputs.size(); ++i) {
        data[i] = reinterpret_cast<const unsigned char*>(inputs[i].data());
        lengths[i] = inputs[i].size();
    }

    std::vector<char> output(inputs.size() * MD5_HEX_LENGTH);
    md5_hex_batch(data.data(), lengths.data(), output.data(), inputs.size());

    for (size_t i = 0; i < cases.size(); ++i) {
        const std::string actual(output.data() + i * MD5_HEX_LENGTH, MD5_HEX_LENGTH);
        EXPECT_TRUE(is_lower_hex_digest(actual)) << "input index " << i;
        EXPECT_EQ(cases[i].second, actual) << "input index " << i;
    }
}

TEST_F(Md5Test, batch_block_padding_boundaries) {
    const std::vector<size_t> lengths = {0,  1,   15,  16,  31,  32,  55,  56,  57,  63,  64,
                                         65, 119, 120, 121, 127, 128, 129, 255, 256, 4096};
    std::vector<std::string> inputs;
    inputs.reserve(lengths.size());
    for (size_t i = 0; i < lengths.size(); ++i) {
        inputs.push_back(make_patterned_input(lengths[i], i));
    }

    expect_batch_matches_scalar(inputs);
}

TEST_F(Md5Test, batch_size_boundaries) {
    const std::vector<size_t> counts = {0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 47, 48, 49};
    const std::vector<size_t> boundary_lengths = {0, 1, 55, 56, 57, 63, 64, 65, 119, 120, 121};

    for (size_t count : counts) {
        SCOPED_TRACE("count=" + std::to_string(count));
        std::vector<std::string> inputs;
        inputs.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            const size_t length = (i % 3 == 0) ? boundary_lengths[(i / 3) % boundary_lengths.size()]
                                               : ((i * 37 + count * 11) % 190);
            inputs.push_back(make_patterned_input(length, i + count));
        }
        expect_batch_matches_scalar(inputs);
    }
}

TEST_F(Md5Test, batch_binary_payloads) {
    std::string all_bytes;
    all_bytes.reserve(256);
    for (int i = 0; i < 256; ++i) {
        all_bytes.push_back(static_cast<char>(i));
    }

    std::vector<std::string> inputs = {std::string(1, '\0'),       std::string("a\0b", 3),
                                       std::string("\0\0\0\0", 4), all_bytes,
                                       all_bytes + all_bytes,      make_binary_input(55, 1),
                                       make_binary_input(56, 2),   make_binary_input(57, 3),
                                       make_binary_input(64, 4),   make_binary_input(65, 5),
                                       make_binary_input(128, 6),  make_binary_input(129, 7)};

    expect_batch_matches_scalar(inputs);
}

} // namespace doris
