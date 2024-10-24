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

#include "olap/rowset/segment_v2/rle_page.h"

#include <gtest/gtest.h>

#include <memory>

#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"

using doris::segment_v2::PageBuilderOptions;
using doris::segment_v2::PageDecoderOptions;

namespace doris {

class RlePageTest : public testing::Test {
public:
    virtual ~RlePageTest() {}

    template <FieldType type, class PageDecoderType>
    void copy_one(PageDecoderType* decoder, typename TypeTraits<type>::CppType* ret) {
        vectorized::Arena pool;
        std::unique_ptr<ColumnVectorBatch> cvb;
        ColumnVectorBatch::create(1, true, get_scalar_type_info(type), nullptr, &cvb);
        ColumnBlock block(cvb.get(), &pool);
        ColumnBlockView column_block_view(&block);

        size_t n = 1;
        decoder->next_batch(&n, &column_block_view);
        EXPECT_EQ(1, n);
        *ret = *reinterpret_cast<const typename TypeTraits<type>::CppType*>(block.cell_ptr(0));
    }

    template <FieldType Type, class PageBuilderType, class PageDecoderType>
    void test_encode_decode_page_template(typename TypeTraits<Type>::CppType* src, size_t size) {
        typedef typename TypeTraits<Type>::CppType CppType;
        PageBuilderOptions builder_options;
        builder_options.data_page_size = 256 * 1024;
        PageBuilderType rle_page_builder(builder_options);
        Status ret0 = rle_page_builder.init();
        EXPECT_TRUE(ret0.ok());
        rle_page_builder.add(reinterpret_cast<const uint8_t*>(src), &size);
        OwnedSlice s = rle_page_builder.finish();
        EXPECT_EQ(size, rle_page_builder.count());

        //check first value and last value
        CppType first_value;
        rle_page_builder.get_first_value(&first_value);
        EXPECT_EQ(src[0], first_value);
        CppType last_value;
        rle_page_builder.get_last_value(&last_value);
        EXPECT_EQ(src[size - 1], last_value);

        PageDecoderOptions decodeder_options;
        PageDecoderType rle_page_decoder(s.slice(), decodeder_options);
        Status status = rle_page_decoder.init();
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(0, rle_page_decoder.current_index());
        EXPECT_EQ(size, rle_page_decoder.count());

        vectorized::Arena pool;
        std::unique_ptr<ColumnVectorBatch> cvb;
        ColumnVectorBatch::create(size, true, get_scalar_type_info(Type), nullptr, &cvb);
        ColumnBlock block(cvb.get(), &pool);
        ColumnBlockView column_block_view(&block);
        size_t size_to_fetch = size;
        status = rle_page_decoder.next_batch(&size_to_fetch, &column_block_view);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(size, size_to_fetch);

        CppType* values = reinterpret_cast<CppType*>(block.data());
        for (uint i = 0; i < size; i++) {
            if (src[i] != values[i]) {
                FAIL() << "Fail at index " << i << " inserted=" << src[i] << " got=" << values[i];
            }
        }

        // Test Seek within block by ordinal
        for (int i = 0; i < 100; i++) {
            int seek_off = random() % size;
            rle_page_decoder.seek_to_position_in_page(seek_off);
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
    segment_v2::RlePageBuilder<FieldType::OLAP_FIELD_TYPE_INT> rle_page_builder(builder_options);
    Status ret0 = rle_page_builder.init();
    EXPECT_TRUE(ret0.ok());
    rle_page_builder.add(reinterpret_cast<const uint8_t*>(ints.get()), &size);
    OwnedSlice s = rle_page_builder.finish();
    // 4 bytes header
    // 2 bytes indicate_value(): 0x64 << 1 | 1 = 201
    // 4 bytes values
    EXPECT_EQ(10, s.slice().size);
}

TEST_F(RlePageTest, TestRleBoolBlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<bool[]> bools(new bool[size]);
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

TEST_F(RlePageTest, TestRleBoolBlockEncoderSize) {
    size_t size = 100;

    std::unique_ptr<bool[]> bools(new bool[size]);
    for (int i = 0; i < size; i++) {
        bools.get()[i] = true;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    segment_v2::RlePageBuilder<FieldType::OLAP_FIELD_TYPE_BOOL> rle_page_builder(builder_options);
    Status ret0 = rle_page_builder.init();
    EXPECT_TRUE(ret0.ok());
    rle_page_builder.add(reinterpret_cast<const uint8_t*>(bools.get()), &size);
    OwnedSlice s = rle_page_builder.finish();
    // 4 bytes header
    // 2 bytes indicate_value(): 0x64 << 1 | 1 = 201
    // 1 bytes values
    EXPECT_EQ(7, s.slice().size);
}

} // namespace doris
