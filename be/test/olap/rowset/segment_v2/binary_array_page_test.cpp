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

#include "olap/rowset/segment_v2/binary_array_page.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/types.h"
#include "util/coding.h"

namespace doris {
namespace segment_v2 {

class BinaryArrayPageTest : public testing::Test {
public:
    BinaryArrayPageTest() {}

    virtual ~BinaryArrayPageTest() {}

    void TestSmallPage() {
        std::vector<Slice> slices;
        slices.emplace_back("1");
        slices.emplace_back("2");
        slices.emplace_back("3");
        slices.emplace_back("4");

        PageBuilderOptions options;
        options.data_page_size = 10;
        BinaryArrayPageBuilder page_builder(options);
        {
            size_t count = slices.size();

            Slice* ptr = &slices[0];
            Status ret = page_builder.add(reinterpret_cast<const uint8_t*>(ptr), &count);
            EXPECT_TRUE(ret.ok());
            EXPECT_EQ(3, count);

            auto page_actual = page_builder.finish();
            std::string page_expected;
            page_expected.push_back(slices[0][0]);
            page_expected.push_back(slices[1][0]);
            page_expected.push_back(slices[2][0]);
            put_fixed32_le(&page_expected, count);
            put_fixed32_le(&page_expected, 1);
            for (size_t i = 0; i < page_expected.size(); i++) {
                EXPECT_EQ(page_expected[i], page_actual.slice()[i]);
            }
        }
        page_builder.reset();
        slices.clear();
        slices.push_back("4");
        slices.push_back("5");
        slices.push_back("6");
        {
            size_t count = slices.size();

            Slice* ptr = &slices[0];
            Status ret = page_builder.add(reinterpret_cast<const uint8_t*>(ptr), &count);
            EXPECT_TRUE(ret.ok());
            EXPECT_EQ(3, count);

            auto page_actual = page_builder.finish();
            std::string page_expected;
            page_expected.push_back(slices[0][0]);
            page_expected.push_back(slices[1][0]);
            page_expected.push_back(slices[2][0]);
            put_fixed32_le(&page_expected, count);
            put_fixed32_le(&page_expected, 1);
            for (size_t i = 0; i < page_expected.size(); i++) {
                std::cout << i << std::endl;
                EXPECT_EQ(page_expected[i], page_actual.slice()[i]);
            }
        }
    }
};

TEST_F(BinaryArrayPageTest, TestSmallPage) {
    TestSmallPage();
}

} // namespace segment_v2
} // namespace doris
