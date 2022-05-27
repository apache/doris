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

#include "olap/rowset/segment_v2/encoding_info.h"

#include <gtest/gtest.h>

#include <iostream>

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/types.h"

namespace doris {
namespace segment_v2 {

class EncodingInfoTest : public testing::Test {
public:
    EncodingInfoTest() {}
    virtual ~EncodingInfoTest() {}
};

TEST_F(EncodingInfoTest, normal) {
    const auto* type_info = get_scalar_type_info<OLAP_FIELD_TYPE_BIGINT>();
    const EncodingInfo* encoding_info = nullptr;
    auto status = EncodingInfo::get(type_info, PLAIN_ENCODING, &encoding_info);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(nullptr, encoding_info);
}

TEST_F(EncodingInfoTest, no_encoding) {
    const auto* type_info = get_scalar_type_info<OLAP_FIELD_TYPE_BIGINT>();
    const EncodingInfo* encoding_info = nullptr;
    auto status = EncodingInfo::get(type_info, DICT_ENCODING, &encoding_info);
    EXPECT_FALSE(status.ok());
}

} // namespace segment_v2
} // namespace doris
