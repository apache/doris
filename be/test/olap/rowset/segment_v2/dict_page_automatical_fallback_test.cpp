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

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>

#include "common/config.h"
#include "common/logging.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/binary_dict_page.h"
#include "testutil/test_util.h"

namespace doris {
namespace segment_v2 {

static const std::string TEST_DIR = "./ut_dir/dict_page_automatical_fallback_test";

class DictPageAutomaticalFallbackTest : public testing::Test {
public:
    DictPageAutomaticalFallbackTest() = default;
    ~DictPageAutomaticalFallbackTest() override = default;

protected:
    void SetUp() override {
        config::disable_storage_page_cache = true;
        EXPECT_TRUE(io::global_local_filesystem()->delete_and_create_directory(TEST_DIR).ok());
        config::enable_dict_page_automatically_fall_back = true;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
        config::enable_dict_page_automatically_fall_back = false;
    }

public:
    std::vector<std::string> data;
    std::vector<Slice> slices;
    std::vector<OwnedSlice> results;
    bool converted = false;

public:
    void finish_current_page(BinaryDictPageBuilder& page_builder) {
        OwnedSlice s = page_builder.finish();
        page_builder.reset();
        results.emplace_back(std::move(s));
    }

    void append_data(BinaryDictPageBuilder& page_builder, const Slice* data, size_t count) {
        for (int i = 0; i < count;) {
            size_t add_num = 1;
            const Slice* ptr = &data[i];
            EXPECT_TRUE(page_builder.add(reinterpret_cast<const uint8_t*>(ptr), &add_num).ok());
            if (page_builder.is_page_full()) {
                if (page_builder.should_convert_previous_data()) {
                    converted = true;
                    std::vector<Slice> previous_data = page_builder.get_previous_data();
                    page_builder.fallback_data_page_builder();
                    append_data(page_builder, previous_data.data(), previous_data.size());
                    continue;
                }
                finish_current_page(page_builder);
            }
            i += add_num;
        }
    }

    void test_dict_page_fallback(size_t dict_page_size, size_t data_page_size,
                                 bool expected_should_covert) {
        PageBuilderOptions options;
        options.data_page_size = data_page_size;
        options.dict_page_size = dict_page_size;
        BinaryDictPageBuilder page_builder(options);
        size_t count = slices.size();
        results.clear();
        converted = false;

        append_data(page_builder, slices.data(), count);
        finish_current_page(page_builder);

        if (expected_should_covert) {
            EXPECT_TRUE(converted);
            for (auto& s : results) {
                Slice slice = s.slice();
                size_t type = decode_fixed32_le((const uint8_t*)&slice.data[0]);
                auto encoding_type = static_cast<EncodingTypePB>(type);
                LOG_INFO("encoding_type: {}", encoding_type);
                EXPECT_EQ(encoding_type, PLAIN_ENCODING);
            }
        } else {
            EXPECT_FALSE(converted);
            int dict_encoding_count = 0;
            for (auto& s : results) {
                Slice slice = s.slice();
                size_t type = decode_fixed32_le((const uint8_t*)&slice.data[0]);
                auto encoding_type = static_cast<EncodingTypePB>(type);
                if (encoding_type == PLAIN_ENCODING) {
                    ++dict_encoding_count;
                }
            }
            EXPECT_GT(dict_encoding_count, 0);
        }
    }
};

TEST_F(DictPageAutomaticalFallbackTest, TestBinaryDictPageShouldConvert) {
    int start = 1000;
    int end = 5000;
    data.clear();
    slices.clear();
    for (int i = start; i < end; i++) {
        data.emplace_back(std::to_string(i));
    }
    for (const auto& s : data) {
        slices.emplace_back(s);
    }
    // dict page is full before data page is full, should re-write previous data
    EXPECT_NO_FATAL_FAILURE(test_dict_page_fallback(100, 1 * 1024 * 1024, true));
}

TEST_F(DictPageAutomaticalFallbackTest, TestBinaryDictPageShouldNotConvert) {
    int start = 1000;
    int end = 5000;
    data.clear();
    slices.clear();
    for (int i = start; i < end; i++) {
        data.emplace_back(std::to_string(i));
    }
    for (const auto& s : data) {
        slices.emplace_back(s);
    }
    // data page is full before dict page is full, don't fallback
    EXPECT_NO_FATAL_FAILURE(test_dict_page_fallback(100, 1 * 1024 * 1024, true));
}

} // namespace segment_v2
} // namespace doris
