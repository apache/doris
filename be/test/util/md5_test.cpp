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

#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"

namespace doris {

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

TEST_F(Md5Test, batch) {
    std::vector<std::string> inputs = {
            "",
            "a",
            "abc",
            "message digest",
            "abcdefghijklmnopqrstuvwxyz",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
            "12345678901234567890123456789012345678901234567890123456789012345678901234567890",
            std::string(55, 'x'),
            std::string(56, 'y'),
            std::string(63, 'z'),
            std::string(64, 'q'),
            std::string(120, 'm'),
            std::string(4096, 'n'),
    };

    std::vector<const unsigned char*> data(inputs.size());
    std::vector<size_t> lengths(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
        data[i] = reinterpret_cast<const unsigned char*>(inputs[i].data());
        lengths[i] = inputs[i].size();
    }

    std::vector<char> output(inputs.size() * MD5_HEX_LENGTH);
    md5_hex_batch(data.data(), lengths.data(), output.data(), inputs.size());

    for (size_t i = 0; i < inputs.size(); ++i) {
        Md5Digest digest;
        digest.update(inputs[i].data(), inputs[i].size());
        digest.digest();
        EXPECT_EQ(digest.hex(), std::string(output.data() + i * MD5_HEX_LENGTH, MD5_HEX_LENGTH))
                << "input index " << i << ", length " << inputs[i].size();
    }
}

} // namespace doris
