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

#include "http/http_channel.h"

#include <gtest/gtest.h>

#include "util/logging.h"
#include "util/zlib.h"

namespace doris {

class HttpChannelTest : public testing::Test {
public:
    void check_data_eq(const std::string& output, const std::string& expected) {
        std::ostringstream oss;
        EXPECT_TRUE(zlib::Uncompress(Slice(output), &oss).ok());
        EXPECT_EQ(expected, oss.str());
    }
};

TEST_F(HttpChannelTest, CompressContent) {
    EXPECT_FALSE(HttpChannel::compress_content("gzip", "", nullptr));
    EXPECT_FALSE(HttpChannel::compress_content("", "test", nullptr));
    EXPECT_FALSE(HttpChannel::compress_content("Gzip", "", nullptr));

    const std::string& intput("test_data_0123456789abcdefg");
    std::string output;

    EXPECT_TRUE(HttpChannel::compress_content("gzip", intput, &output));
    EXPECT_NO_FATAL_FAILURE(check_data_eq(output, intput));

    EXPECT_TRUE(HttpChannel::compress_content("123,gzip,321", intput, &output));
    EXPECT_NO_FATAL_FAILURE(check_data_eq(output, intput));
}

} // namespace doris
