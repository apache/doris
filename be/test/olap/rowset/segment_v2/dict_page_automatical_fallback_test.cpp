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

#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>

#include "common/config.h"
#include "common/logging.h"
#include "olap/rowset/segment_v2/binary_dict_page.h"
#include "testutil/test_util.h"

namespace doris {
namespace segment_v2 {

class DictPageAutomaticalFallbackTest : public testing::Test {
public:
    void test_binary_dict_page_state() {
        int start = 1000;
        int end = 9000;
        std::vector<std::string> data;
        std::vector<Slice> slices;
        for (int i = start; i < end; i++) {
            data.emplace_back(std::to_string(i));
        }
        for (const auto& s : data) {
            slices.emplace_back(s);
        }

        auto test_convert_status = [&](size_t dict_page_size, size_t data_page_size,
                                       bool expected_should_covert,
                                       bool expected_is_dict_encoding) {
            config::enable_dict_page_automatically_fall_back = true;
            PageBuilderOptions options;
            options.data_page_size = data_page_size;
            options.dict_page_size = dict_page_size;
            BinaryDictPageBuilder page_builder(options);
            size_t count = slices.size();
            LOG_INFO("count: {}", count);
            bool is_page_full = false;

            for (int i = 0; i < count;) {
                size_t add_num = 1;
                const Slice* ptr = &slices[i];
                Status ret = page_builder.add(reinterpret_cast<const uint8_t*>(ptr), &add_num);
                EXPECT_TRUE(ret.ok());
                if (page_builder.is_page_full()) {
                    is_page_full = true;
                    EXPECT_EQ(page_builder.should_convert_previous_data(), expected_should_covert);
                    EXPECT_EQ(page_builder.is_dict_encoding(), expected_is_dict_encoding);
                    break;
                }
                i += add_num;
            }
            EXPECT_TRUE(is_page_full);
            config::enable_dict_page_automatically_fall_back = false;
        };

        {
            // 1. dict page is full before data page is full, should re-write previous data
            EXPECT_NO_FATAL_FAILURE(test_convert_status(100, 1 * 1024 * 1024, true, false));
        }

        {
            // 2. data page is full before dict page is full, don't fallback
            EXPECT_NO_FATAL_FAILURE(test_convert_status(1 * 1024 * 1024, 100, false, true));
        }
    };
};

TEST_F(DictPageAutomaticalFallbackTest, TestBinaryDictPageState) {
    test_binary_dict_page_state();
}

} // namespace segment_v2
} // namespace doris
