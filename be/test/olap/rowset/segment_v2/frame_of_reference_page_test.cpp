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

#include "olap/rowset/segment_v2/frame_of_reference_page.h"

#include <gtest/gtest.h>

#include <memory>

#include "olap/rowset/segment_v2/options.h"
#include "runtime/large_int_value.h"

using doris::segment_v2::PageBuilderOptions;
using doris::segment_v2::PageDecoderOptions;

namespace doris {
class FrameOfReferencePageTest : public testing::Test {
public:
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
        PageBuilderType for_page_builder(builder_options);
        Status ret0 = for_page_builder.init();
        EXPECT_TRUE(ret0.ok());
        for_page_builder.add(reinterpret_cast<const uint8_t*>(src), &size);
        OwnedSlice s = for_page_builder.finish();
        EXPECT_EQ(size, for_page_builder.count());
        LOG(INFO) << "FrameOfReference Encoded size for " << size << " values: " << s.slice().size
                  << ", original size:" << size * sizeof(CppType);

        PageDecoderOptions decoder_options;
        PageDecoderType for_page_decoder(s.slice(), decoder_options);
        Status status = for_page_decoder.init();
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(0, for_page_decoder.current_index());
        EXPECT_EQ(size, for_page_decoder.count());

        vectorized::Arena pool;
        std::unique_ptr<ColumnVectorBatch> cvb;
        ColumnVectorBatch::create(size, true, get_scalar_type_info(Type), nullptr, &cvb);
        ColumnBlock block(cvb.get(), &pool);
        ColumnBlockView column_block_view(&block);
        size_t size_to_fetch = size;
        status = for_page_decoder.next_batch(&size_to_fetch, &column_block_view);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(size, size_to_fetch);

        CppType* values = reinterpret_cast<CppType*>(column_block_view.data());

        for (uint i = 0; i < size; i++) {
            if (src[i] != values[i]) {
                if constexpr (Type == FieldType::OLAP_FIELD_TYPE_LARGEINT) {
                    FAIL() << "Fail at index " << i
                           << " inserted=" << LargeIntValue::to_string(src[i])
                           << " got=" << LargeIntValue::to_string(values[i]);
                } else {
                    FAIL() << "Fail at index " << i << " inserted=" << src[i]
                           << " got=" << values[i];
                }
            }
        }

        // Test Seek within block by ordinal
        for (int i = 0; i < 100; i++) {
            int seek_off = random() % size;
            for_page_decoder.seek_to_position_in_page(seek_off);
            EXPECT_EQ((int32_t)(seek_off), for_page_decoder.current_index());
            CppType ret;
            copy_one<Type, PageDecoderType>(&for_page_decoder, &ret);
            EXPECT_EQ(values[seek_off], ret);
        }
    }
};

TEST_F(FrameOfReferencePageTest, TestInt32BlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_INT,
            segment_v2::FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
            segment_v2::FrameOfReferencePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(ints.get(),
                                                                                     size);
}

TEST_F(FrameOfReferencePageTest, TestInt32BlockEncoderEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_INT,
            segment_v2::FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
            segment_v2::FrameOfReferencePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(ints.get(),
                                                                                     size);
}

TEST_F(FrameOfReferencePageTest, TestInt32BlockEncoderSequence) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345 + i;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_INT,
            segment_v2::FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
            segment_v2::FrameOfReferencePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(ints.get(),
                                                                                     size);
}

TEST_F(FrameOfReferencePageTest, TestInt64BlockEncoderSequence) {
    const uint32_t size = 10000;

    std::unique_ptr<int64_t[]> ints(new int64_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 21474836478 + i;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_BIGINT,
            segment_v2::FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_BIGINT>,
            segment_v2::FrameOfReferencePageDecoder<FieldType::OLAP_FIELD_TYPE_BIGINT>>(ints.get(),
                                                                                        size);

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_DATETIME,
            segment_v2::FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_DATETIME>,
            segment_v2::FrameOfReferencePageDecoder<FieldType::OLAP_FIELD_TYPE_DATETIME>>(
            ints.get(), size);
}

TEST_F(FrameOfReferencePageTest, TestInt24BlockEncoderSequence) {
    const uint32_t size = 1311;

    std::unique_ptr<uint24_t[]> ints(new uint24_t[size]);
    // to guarantee the last value is 0xFFFFFF
    uint24_t first_value = 0xFFFFFF - size + 1;
    for (int i = 0; i < size; i++) {
        ints.get()[i] = first_value + i;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_DATE,
            segment_v2::FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_DATE>,
            segment_v2::FrameOfReferencePageDecoder<FieldType::OLAP_FIELD_TYPE_DATE>>(ints.get(),
                                                                                      size);
}

TEST_F(FrameOfReferencePageTest, TestInt128BlockEncoderSequence) {
    const uint32_t size = 1000;

    std::unique_ptr<int128_t[]> ints(new int128_t[size]);
    // to guarantee the last value is numeric_limits<uint128_t>::max()
    int128_t first_value = numeric_limits<int128_t>::max() - size + 1;
    for (int i = 0; i < size; i++) {
        ints.get()[i] = first_value + i;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_LARGEINT,
            segment_v2::FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_LARGEINT>,
            segment_v2::FrameOfReferencePageDecoder<FieldType::OLAP_FIELD_TYPE_LARGEINT>>(
            ints.get(), size);
}

TEST_F(FrameOfReferencePageTest, TestInt24BlockEncoderMinMax) {
    std::unique_ptr<uint24_t[]> ints(new uint24_t[2]);
    ints.get()[0] = 0;
    ints.get()[1] = 0xFFFFFF;

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_DATE,
            segment_v2::FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_DATE>,
            segment_v2::FrameOfReferencePageDecoder<FieldType::OLAP_FIELD_TYPE_DATE>>(ints.get(),
                                                                                      2);
}

TEST_F(FrameOfReferencePageTest, TestInt128BlockEncoderMinMax) {
    std::unique_ptr<int128_t[]> ints(new int128_t[2]);
    ints.get()[0] = numeric_limits<int128_t>::min();
    ints.get()[1] = numeric_limits<int128_t>::max();

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_LARGEINT,
            segment_v2::FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_LARGEINT>,
            segment_v2::FrameOfReferencePageDecoder<FieldType::OLAP_FIELD_TYPE_LARGEINT>>(
            ints.get(), 2);
}

TEST_F(FrameOfReferencePageTest, TestInt32SequenceBlockEncoderSize) {
    size_t size = 128;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = i;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    segment_v2::FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_INT> page_builder(
            builder_options);
    Status ret0 = page_builder.init();
    EXPECT_TRUE(ret0.ok());
    page_builder.add(reinterpret_cast<const uint8_t*>(ints.get()), &size);
    OwnedSlice s = page_builder.finish();
    // body: 4 bytes min value + 128 * 1 /8 packing value = 20
    // footer: (1 + 1) * 1 + 1 + 4 = 7
    EXPECT_EQ(27, s.slice().size);
}

TEST_F(FrameOfReferencePageTest, TestFirstLastValue) {
    size_t size = 128;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = i;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    segment_v2::FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_INT> page_builder(
            builder_options);
    Status ret0 = page_builder.init();
    EXPECT_TRUE(ret0.ok());
    page_builder.add(reinterpret_cast<const uint8_t*>(ints.get()), &size);
    OwnedSlice s = page_builder.finish();
    int32_t first_value = -1;
    page_builder.get_first_value(&first_value);
    EXPECT_EQ(0, first_value);
    int32_t last_value = 0;
    page_builder.get_last_value(&last_value);
    EXPECT_EQ(127, last_value);
}

TEST_F(FrameOfReferencePageTest, TestInt32NormalBlockEncoderSize) {
    size_t size = 128;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 128 - i;
    }
    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    segment_v2::FrameOfReferencePageBuilder<FieldType::OLAP_FIELD_TYPE_INT> page_builder(
            builder_options);
    Status ret0 = page_builder.init();
    EXPECT_TRUE(ret0.ok());
    page_builder.add(reinterpret_cast<const uint8_t*>(ints.get()), &size);
    OwnedSlice s = page_builder.finish();
    // body: 4 bytes min value + 128 * 7 /8 packing value = 116
    // footer: (1 + 1) * 1 + 1 + 4 = 7
    EXPECT_EQ(123, s.slice().size);
}

TEST_F(FrameOfReferencePageTest, TestFindBitsOfInt) {
    int8_t bits_3 = 0x06;
    EXPECT_EQ(3, bits(bits_3));

    uint8_t bits_4 = 0x0F;
    EXPECT_EQ(4, bits(bits_4));

    int32_t bits_17 = 0x000100FF;
    EXPECT_EQ(17, bits(bits_17));

    int64_t bits_33 = 0x00000001FFFFFFFF;
    EXPECT_EQ(33, bits(bits_33));

    int128_t bits_0 = 0;
    EXPECT_EQ(0, bits(bits_0));

    int128_t bits_127 = numeric_limits<int128_t>::max();
    EXPECT_EQ(127, bits(bits_127));

    uint128_t bits_128 = numeric_limits<uint128_t>::max();
    EXPECT_EQ(128, bits(bits_128));

    int128_t bits_65 = ((int128_t)1) << 64;
    EXPECT_EQ(65, bits(bits_65));
}

} // namespace doris
