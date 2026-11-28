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

#include "storage/segment/rle_page.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "storage/segment/options.h"
#include "storage/segment/page_builder.h"
#include "storage/segment/page_decoder.h"

using doris::segment_v2::PageBuilderOptions;
using doris::segment_v2::PageDecoderOptions;

namespace doris {

class RlePageTest : public testing::Test {
public:
    virtual ~RlePageTest() {}

    template <FieldType type, class PageDecoderType>
    void copy_one(PageDecoderType* decoder, typename TypeTraits<type>::CppType* ret) {
        auto column = DataTypeFactory::instance().create_data_type(type, 0, 0)->create_column();

        size_t n = 1;
        EXPECT_TRUE(decoder->next_batch(&n, column).ok());
        EXPECT_EQ(1, n);
        *ret = *reinterpret_cast<const typename TypeTraits<type>::CppType*>(
                column->get_raw_data().data);
    }

    template <FieldType Type, class PageBuilderType, class PageDecoderType>
    void test_encode_decode_page_template(typename TypeTraits<Type>::CppType* src, size_t size) {
        typedef typename TypeTraits<Type>::CppType CppType;
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;
        segment_v2::PageBuilder* builder = nullptr;
        ASSERT_TRUE(PageBuilderType::create(&builder, builder_options).ok());
        std::unique_ptr<segment_v2::PageBuilder> rle_page_builder(builder);
        EXPECT_TRUE(rle_page_builder->add(reinterpret_cast<const uint8_t*>(src), &size).ok());
        OwnedSlice s;
        EXPECT_TRUE(rle_page_builder->finish(&s).ok());
        EXPECT_EQ(size, rle_page_builder->count());

        PageDecoderOptions decodeder_options;
        PageDecoderType rle_page_decoder(s.slice(), decodeder_options);
        Status status = rle_page_decoder.init();
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(0, rle_page_decoder.current_index());
        EXPECT_EQ(size, rle_page_decoder.count());

        auto column = DataTypeFactory::instance().create_data_type(Type, 0, 0)->create_column();
        size_t size_to_fetch = size;
        // The analyzer wrongly thinks the decoder is still being constructed.
        // NOLINTNEXTLINE(clang-analyzer-optin.cplusplus.VirtualCall)
        status = rle_page_decoder.next_batch(&size_to_fetch, column);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(size, size_to_fetch);

        const auto* values = reinterpret_cast<const CppType*>(column->get_raw_data().data);
        for (uint i = 0; i < size; i++) {
            if (src[i] != values[i]) {
                FAIL() << "Fail at index " << i << " inserted=" << src[i] << " got=" << values[i];
            }
        }

        // Test Seek within block by ordinal
        for (int i = 0; i < 100; i++) {
            int seek_off = random() % size;
            EXPECT_TRUE(rle_page_decoder.seek_to_position_in_page(seek_off).ok());
            EXPECT_EQ((int32_t)(seek_off), rle_page_decoder.current_index());
            CppType ret;
            copy_one<Type, PageDecoderType>(&rle_page_decoder, &ret);
            EXPECT_EQ(values[seek_off], ret);
        }
    }
};

// Test for rle block, for INT32, BOOL
TEST_F(RlePageTest, TestRleInt32BlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<FieldType::OLAP_FIELD_TYPE_INT,
                                     segment_v2::RlePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
                                     segment_v2::RlePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(
            ints.get(), size);
}

TEST_F(RlePageTest, TestRleInt32BlockEncoderEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345;
    }

    test_encode_decode_page_template<FieldType::OLAP_FIELD_TYPE_INT,
                                     segment_v2::RlePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
                                     segment_v2::RlePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(
            ints.get(), size);
}

TEST_F(RlePageTest, TestRleInt32BlockEncoderSequence) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345 + i;
    }

    test_encode_decode_page_template<FieldType::OLAP_FIELD_TYPE_INT,
                                     segment_v2::RlePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
                                     segment_v2::RlePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(
            ints.get(), size);
}

TEST_F(RlePageTest, TestRleInt32BlockEncoderSize) {
    size_t size = 100;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 0;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    segment_v2::PageBuilder* builder = nullptr;
    ASSERT_TRUE(segment_v2::RlePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>::create(&builder,
                                                                                   builder_options)
                        .ok());
    std::unique_ptr<segment_v2::PageBuilder> rle_page_builder(builder);
    EXPECT_TRUE(rle_page_builder->add(reinterpret_cast<const uint8_t*>(ints.get()), &size).ok());
    OwnedSlice s;
    EXPECT_TRUE(rle_page_builder->finish(&s).ok());
    // 4 bytes header
    // 2 bytes indicate_value(): 0x64 << 1 | 1 = 201
    // 4 bytes values
    EXPECT_EQ(10, s.slice().size);
}

TEST_F(RlePageTest, TestRleBoolBlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<uint8_t[]> bools(new uint8_t[size]);
    for (int i = 0; i < size; i++) {
        if (random() % 2 == 0) {
            bools.get()[i] = true;
        } else {
            bools.get()[i] = false;
        }
    }

    test_encode_decode_page_template<FieldType::OLAP_FIELD_TYPE_BOOL,
                                     segment_v2::RlePageBuilder<FieldType::OLAP_FIELD_TYPE_BOOL>,
                                     segment_v2::RlePageDecoder<FieldType::OLAP_FIELD_TYPE_BOOL>>(
            bools.get(), size);
}

TEST_F(RlePageTest, BooleanBatchReadAcrossRuns) {
    constexpr auto type = FieldType::OLAP_FIELD_TYPE_BOOL;
    std::vector<uint8_t> values(1536);
    for (size_t i = 128; i < 1392; ++i) {
        values[i] = (i + 1) % 2;
    }
    std::fill(values.begin() + 1392, values.end(), 1);

    segment_v2::PageBuilder* builder = nullptr;
    ASSERT_TRUE(segment_v2::RlePageBuilder<type>::create(&builder, PageBuilderOptions {}).ok());
    std::unique_ptr<segment_v2::PageBuilder> page_builder(builder);
    size_t count = values.size();
    ASSERT_TRUE(page_builder->add(values.data(), &count).ok());
    ASSERT_EQ(values.size(), count);
    OwnedSlice page;
    ASSERT_TRUE(page_builder->finish(&page).ok());

    PageDecoderOptions options;
    segment_v2::RlePageDecoder<type> decoder(page.slice(), options);
    ASSERT_TRUE(decoder.init().ok());
    MutableColumnPtr column = ColumnUInt8::create();
    auto& data = assert_cast<ColumnUInt8&>(*column).get_data();
    data.push_back(1);
    size_t offset = 0;
    for (size_t batch_size : {131, 397, 65, 1000}) {
        size_t n = batch_size;
        ASSERT_TRUE(decoder.next_batch(&n, column).ok());
        ASSERT_EQ(std::min(batch_size, values.size() - offset), n);
        offset += n;
        ASSERT_EQ(offset, decoder.current_index());
        ASSERT_EQ(offset + 1, data.size());
    }
    ASSERT_EQ(values.size(), offset);
    ASSERT_EQ(1, data[0]);
    EXPECT_TRUE(std::equal(values.begin(), values.end(), data.begin() + 1));

    ASSERT_TRUE(decoder.seek_to_position_in_page(135).ok());
    const segment_v2::rowid_t rowids[] = {135, 137, 167};
    size_t n = std::size(rowids);
    ASSERT_TRUE(decoder.read_by_rowids(rowids, 0, &n, column).ok());
    ASSERT_EQ(std::size(rowids), n);
    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(values[rowids[i]], data[values.size() + 1 + i]);
    }
    n = 133;
    ASSERT_TRUE(decoder.next_batch(&n, column).ok());
    ASSERT_EQ(133, n);
    EXPECT_TRUE(std::equal(values.begin() + 168, values.begin() + 168 + n, data.end() - n));
}

TEST_F(RlePageTest, TestRleBoolBlockEncoderSize) {
    size_t size = 100;

    std::unique_ptr<uint8_t[]> bools(new uint8_t[size]);
    for (int i = 0; i < size; i++) {
        bools.get()[i] = true;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    segment_v2::PageBuilder* builder = nullptr;
    ASSERT_TRUE(segment_v2::RlePageBuilder<FieldType::OLAP_FIELD_TYPE_BOOL>::create(&builder,
                                                                                    builder_options)
                        .ok());
    std::unique_ptr<segment_v2::PageBuilder> rle_page_builder(builder);
    EXPECT_TRUE(rle_page_builder->add(reinterpret_cast<const uint8_t*>(bools.get()), &size).ok());
    OwnedSlice s;
    EXPECT_TRUE(rle_page_builder->finish(&s).ok());
    // 4 bytes header
    // 2 bytes indicate_value(): 0x64 << 1 | 1 = 201
    // 1 bytes values
    EXPECT_EQ(7, s.slice().size);
}

} // namespace doris
