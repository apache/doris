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

#include "olap/rowset/segment_v2/binary_prefix_page.h"

#include <gtest/gtest.h>

#include <fstream>
#include <iostream>

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/types.h"
#include "util/debug_util.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
using namespace ErrorCode;
namespace segment_v2 {

class BinaryPrefixPageTest : public testing::Test {
public:
    void test_encode_and_decode() {
        std::vector<std::string> test_data;
        for (int i = 1000; i < 1038; ++i) {
            test_data.emplace_back(std::to_string(i));
        }
        std::vector<Slice> slices;
        for (const auto& data : test_data) {
            slices.emplace_back(Slice(data));
        }
        // encode
        PageBuilderOptions options;
        BinaryPrefixPageBuilder page_builder(options);
        Status ret0 = page_builder.init();
        EXPECT_TRUE(ret0.ok());

        size_t count = slices.size();
        const Slice* ptr = &slices[0];
        Status ret = page_builder.add(reinterpret_cast<const uint8_t*>(ptr), &count);

        OwnedSlice dict_slice = page_builder.finish();
        EXPECT_EQ(slices.size(), page_builder.count());
        EXPECT_FALSE(page_builder.is_page_full());

        //check first value and last value
        Slice first_value;
        page_builder.get_first_value(&first_value);
        EXPECT_EQ(slices[0], first_value);
        Slice last_value;
        page_builder.get_last_value(&last_value);
        EXPECT_EQ(slices[count - 1], last_value);

        PageDecoderOptions dict_decoder_options;
        std::unique_ptr<BinaryPrefixPageDecoder> page_decoder(
                new BinaryPrefixPageDecoder(dict_slice.slice(), dict_decoder_options));
        ret = page_decoder->init();
        EXPECT_TRUE(ret.ok());
        // because every slice is unique
        EXPECT_EQ(slices.size(), page_decoder->count());

        //check values
        vectorized::Arena pool;
        auto type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_VARCHAR);
        size_t size = slices.size();
        std::unique_ptr<ColumnVectorBatch> cvb;
        ColumnVectorBatch::create(size, false, type_info, nullptr, &cvb);
        ColumnBlock column_block(cvb.get(), &pool);
        ColumnBlockView block_view(&column_block);

        ret = page_decoder->next_batch(&size, &block_view);
        Slice* values = reinterpret_cast<Slice*>(column_block.data());
        EXPECT_TRUE(ret.ok());
        EXPECT_EQ(slices.size(), size);
        for (int i = 1000; i < 1038; ++i) {
            EXPECT_EQ(std::to_string(i), values[i - 1000].to_string());
        }

        ret = page_decoder->seek_to_position_in_page(0);
        EXPECT_TRUE(ret.ok());
        int n = 0;
        while (true) {
            //check values
            vectorized::Arena pool;
            auto type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_VARCHAR);
            std::unique_ptr<ColumnVectorBatch> cvb;
            size_t size = 6;
            ColumnVectorBatch::create(size, false, type_info, nullptr, &cvb);
            ColumnBlock column_block(cvb.get(), &pool);
            ColumnBlockView block_view(&column_block);
            ret = page_decoder->next_batch(&size, &block_view);
            EXPECT_TRUE(ret.ok());
            if (size == 0) {
                break;
            }
            Slice* values = reinterpret_cast<Slice*>(column_block.data());
            for (int i = 0; i < size; ++i) {
                EXPECT_EQ(std::to_string(1000 + 6 * n + i), values[i].to_string());
            }
            n++;
        }

        std::unique_ptr<ColumnVectorBatch> cvb2;
        ColumnVectorBatch::create(size, false, type_info, nullptr, &cvb2);
        ColumnBlock column_block2(cvb2.get(), &pool);
        ColumnBlockView block_view2(&column_block2);
        ret = page_decoder->seek_to_position_in_page(15);
        EXPECT_TRUE(ret.ok());

        ret = page_decoder->next_batch(&size, &block_view2);
        values = reinterpret_cast<Slice*>(column_block2.data());
        EXPECT_TRUE(ret.ok());
        EXPECT_EQ(23, size);
        for (int i = 1015; i < 1038; ++i) {
            EXPECT_EQ(std::to_string(i), values[i - 1015].to_string());
        }

        Slice v1 = Slice("1039");
        bool exact_match;
        ret = page_decoder->seek_at_or_after_value(&v1, &exact_match);
        EXPECT_TRUE(ret.is<ENTRY_NOT_FOUND>());

        Slice v2 = Slice("1000");
        ret = page_decoder->seek_at_or_after_value(&v2, &exact_match);
        EXPECT_TRUE(ret.ok());
        EXPECT_TRUE(exact_match);

        Slice v3 = Slice("1037");
        ret = page_decoder->seek_at_or_after_value(&v3, &exact_match);
        EXPECT_TRUE(ret.ok());
        EXPECT_TRUE(exact_match);

        Slice v4 = Slice("100");
        ret = page_decoder->seek_at_or_after_value(&v4, &exact_match);
        EXPECT_TRUE(ret.ok());
        EXPECT_TRUE(!exact_match);
    }

    void test_encode_and_decode_vec() {
        std::vector<std::string> test_data;
        for (int i = 1000; i < 1038; ++i) {
            test_data.emplace_back(std::to_string(i));
        }
        std::vector<Slice> slices;
        for (const auto& data : test_data) {
            slices.emplace_back(Slice(data));
        }
        // encode
        PageBuilderOptions options;
        BinaryPrefixPageBuilder page_builder(options);
        Status ret0 = page_builder.init();
        EXPECT_TRUE(ret0.ok());

        size_t count = slices.size();
        const Slice* ptr = &slices[0];
        Status ret = page_builder.add(reinterpret_cast<const uint8_t*>(ptr), &count);

        OwnedSlice dict_slice = page_builder.finish();
        EXPECT_EQ(slices.size(), page_builder.count());
        EXPECT_FALSE(page_builder.is_page_full());

        //check first value and last value
        Slice first_value;
        page_builder.get_first_value(&first_value);
        EXPECT_EQ(slices[0], first_value);
        Slice last_value;
        page_builder.get_last_value(&last_value);
        EXPECT_EQ(slices[count - 1], last_value);

        PageDecoderOptions dict_decoder_options;
        std::unique_ptr<BinaryPrefixPageDecoder> page_decoder(
                new BinaryPrefixPageDecoder(dict_slice.slice(), dict_decoder_options));
        ret = page_decoder->init();
        EXPECT_TRUE(ret.ok());
        // because every slice is unique
        EXPECT_EQ(slices.size(), page_decoder->count());
        auto type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_VARCHAR);
        size_t size = slices.size();

        {
            //check values
            auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                    type_info->type(), 1, 0);
            auto column = data_type->create_column();

            ret = page_decoder->next_batch(&size, column);
            EXPECT_TRUE(ret.ok());
            EXPECT_EQ(slices.size(), size);
            for (int i = 1000; i < 1038; ++i) {
                EXPECT_EQ(std::to_string(i), column->get_data_at(i - 1000).to_string());
            }
        }

        {
            ret = page_decoder->seek_to_position_in_page(0);
            EXPECT_TRUE(ret.ok());
            int n = 0;
            while (true) {
                //check values
                auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                        type_info->type(), 1, 0);
                auto column = data_type->create_column();
                size_t size = 6;
                ret = page_decoder->next_batch(&size, column);
                EXPECT_TRUE(ret.ok());
                if (size == 0) {
                    break;
                }
                for (int i = 0; i < size; ++i) {
                    EXPECT_EQ(std::to_string(1000 + 6 * n + i), column->get_data_at(i).to_string());
                }
                n++;
            }
        }

        {
            auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                    type_info->type(), 1, 0);
            auto column = data_type->create_column();
            ret = page_decoder->seek_to_position_in_page(15);
            EXPECT_TRUE(ret.ok());

            ret = page_decoder->next_batch(&size, column);
            EXPECT_TRUE(ret.ok());
            EXPECT_EQ(23, size);
            for (int i = 1015; i < 1038; ++i) {
                EXPECT_EQ(std::to_string(i), column->get_data_at(i - 1015).to_string());
            }
        }

        Slice v1 = Slice("1039");
        bool exact_match;
        ret = page_decoder->seek_at_or_after_value(&v1, &exact_match);
        EXPECT_TRUE(ret.is<ENTRY_NOT_FOUND>());

        Slice v2 = Slice("1000");
        ret = page_decoder->seek_at_or_after_value(&v2, &exact_match);
        EXPECT_TRUE(ret.ok());
        EXPECT_TRUE(exact_match);

        Slice v3 = Slice("1037");
        ret = page_decoder->seek_at_or_after_value(&v3, &exact_match);
        EXPECT_TRUE(ret.ok());
        EXPECT_TRUE(exact_match);

        Slice v4 = Slice("100");
        ret = page_decoder->seek_at_or_after_value(&v4, &exact_match);
        EXPECT_TRUE(ret.ok());
        EXPECT_TRUE(!exact_match);
    }

    void test_encode_and_decode2() {
        std::vector<std::string> test_data;
        test_data.push_back("ab");
        test_data.push_back("c");
        std::vector<Slice> slices;
        for (int i = 0; i < test_data.size(); ++i) {
            Slice s(test_data[i]);
            slices.emplace_back(s);
        }
        // encode
        PageBuilderOptions options;
        BinaryPrefixPageBuilder page_builder(options);
        Status ret0 = page_builder.init();
        EXPECT_TRUE(ret0.ok());

        size_t count = slices.size();
        const Slice* ptr = &slices[0];
        Status ret = page_builder.add(reinterpret_cast<const uint8_t*>(ptr), &count);

        OwnedSlice dict_slice = page_builder.finish();

        PageDecoderOptions dict_decoder_options;
        std::unique_ptr<BinaryPrefixPageDecoder> page_decoder(
                new BinaryPrefixPageDecoder(dict_slice.slice(), dict_decoder_options));
        ret = page_decoder->init();
        EXPECT_TRUE(ret.ok());

        Slice slice("c");
        bool exact_match;
        ret = page_decoder->seek_at_or_after_value(&slice, &exact_match);
        EXPECT_TRUE(ret.ok());
        EXPECT_TRUE(exact_match);
    }
};

TEST_F(BinaryPrefixPageTest, TestEncodeAndDecode) {
    test_encode_and_decode();
    test_encode_and_decode_vec();
}

TEST_F(BinaryPrefixPageTest, TestEncodeAndDecode2) {
    test_encode_and_decode2();
}

} // namespace segment_v2
} // namespace doris
