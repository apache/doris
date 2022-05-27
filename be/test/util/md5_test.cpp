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

#include <gtest/gtest.h>

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

} // namespace doris
