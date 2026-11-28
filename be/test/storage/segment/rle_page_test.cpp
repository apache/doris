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

#include <memory>

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

TEST_F(RlePageTest, TestReadByRowidsDuplicateOutOfOrderAndTailRepeat) {
    constexpr size_t kSize = 64;
    uint8_t bools[kSize];
    for (size_t i = 0; i < kSize; ++i) {
        // Mix repeated runs and alternating literals.
        bools[i] = (i < 20) ? false : ((i < 40) ? true : static_cast<uint8_t>(i & 1));
    }

    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    segment_v2::PageBuilder* builder = nullptr;
    ASSERT_TRUE(segment_v2::RlePageBuilder<FieldType::OLAP_FIELD_TYPE_BOOL>::create(&builder,
                                                                                    builder_options)
                        .ok());
    std::unique_ptr<segment_v2::PageBuilder> page_builder(builder);
    size_t add_count = kSize;
    ASSERT_TRUE(page_builder->add(bools, &add_count).ok());
    OwnedSlice slice;
    ASSERT_TRUE(page_builder->finish(&slice).ok());

    PageDecoderOptions decoder_options;
    segment_v2::RlePageDecoder<FieldType::OLAP_FIELD_TYPE_BOOL> decoder(slice.slice(),
                                                                        decoder_options);
    ASSERT_TRUE(decoder.init().ok());

    // Duplicate, backward, page-tail (63), page-head (0), and cross-page boundary rowids.
    const segment_v2::ordinal_t page_first_ordinal = 100;
    const segment_v2::rowid_t rowids[] = {163, 163, 100, 145, 112, 112, 163, 200};
    auto col = DataTypeFactory::instance()
                       .create_data_type(FieldType::OLAP_FIELD_TYPE_BOOL, 0, 0)
                       ->create_column();
    size_t n = std::size(rowids);
    ASSERT_TRUE(decoder.read_by_rowids(rowids, page_first_ordinal, &n, col).ok());
    // Stops cleanly at 200 (>= 100 + 64) after reading the 7 in-page rowids.
    ASSERT_EQ(7u, n);
    ASSERT_EQ(7u, col->size());
    const auto* out = reinterpret_cast<const uint8_t*>(col->get_raw_data().data);
    for (size_t i = 0; i < 7; ++i) {
        EXPECT_EQ(bools[rowids[i] - page_first_ordinal], out[i]) << "i=" << i;
    }

    // Even when _cur_index reached _num_elements (after reading ordinal 63), a
    // subsequent read_by_rowids call on the same page decoder must still succeed.
    ASSERT_EQ(kSize, decoder.current_index());
    const segment_v2::rowid_t second_batch[] = {163, 105, 163};
    size_t n2 = std::size(second_batch);
    ASSERT_TRUE(decoder.read_by_rowids(second_batch, page_first_ordinal, &n2, col).ok());
    ASSERT_EQ(3u, n2);
    ASSERT_EQ(10u, col->size());
    out = reinterpret_cast<const uint8_t*>(col->get_raw_data().data);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(bools[second_batch[i] - page_first_ordinal], out[7 + i]) << "i=" << i;
    }

    // Below page_first_ordinal stops at the out-of-page rowid without underflowing.
    const segment_v2::rowid_t backward_cross_page[] = {120, 99};
    size_t n3 = std::size(backward_cross_page);
    ASSERT_TRUE(decoder.read_by_rowids(backward_cross_page, page_first_ordinal, &n3, col).ok());
    ASSERT_EQ(1u, n3);
}

TEST_F(RlePageTest, TestTruncatedPageReturnsCorruptionWithoutSpinning) {
    constexpr size_t kSize = 64;
    uint8_t bools[kSize];
    for (size_t i = 0; i < kSize; ++i) {
        bools[i] = static_cast<uint8_t>((i / 4) & 1);
    }

    PageBuilderOptions builder_options;
    builder_options.data_page_size = 256 * 1024;
    segment_v2::PageBuilder* builder = nullptr;
    ASSERT_TRUE(segment_v2::RlePageBuilder<FieldType::OLAP_FIELD_TYPE_BOOL>::create(&builder,
                                                                                    builder_options)
                        .ok());
    std::unique_ptr<segment_v2::PageBuilder> page_builder(builder);
    size_t add_count = kSize;
    ASSERT_TRUE(page_builder->add(bools, &add_count).ok());
    OwnedSlice full_slice;
    ASSERT_TRUE(page_builder->finish(&full_slice).ok());
    ASSERT_GT(full_slice.slice().size, segment_v2::RLE_PAGE_HEADER_SIZE + 2);

    // Keep the 4-byte header claiming 64 elements, but truncate the RLE payload to 1 byte.
    Slice truncated(full_slice.slice().data, segment_v2::RLE_PAGE_HEADER_SIZE + 1);
    PageDecoderOptions decoder_options;
    segment_v2::RlePageDecoder<FieldType::OLAP_FIELD_TYPE_BOOL> decoder(truncated, decoder_options);
    ASSERT_TRUE(decoder.init().ok());

    auto col = DataTypeFactory::instance()
                       .create_data_type(FieldType::OLAP_FIELD_TYPE_BOOL, 0, 0)
                       ->create_column();
    const segment_v2::rowid_t rowids[] = {60};
    size_t n = 1;
    Status st = decoder.read_by_rowids(rowids, 0, &n, col);
    EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st.to_string();

    Status seek_st = decoder.seek_to_position_in_page(60);
    EXPECT_TRUE(seek_st.is<ErrorCode::CORRUPTION>()) << seek_st.to_string();

    // Partial next_batch on a truncated page must return Corruption instead of reading garbage.
    {
        segment_v2::RlePageDecoder<FieldType::OLAP_FIELD_TYPE_BOOL> nb_decoder(truncated,
                                                                               decoder_options);
        ASSERT_TRUE(nb_decoder.init().ok());
        auto nb_col = DataTypeFactory::instance()
                              .create_data_type(FieldType::OLAP_FIELD_TYPE_BOOL, 0, 0)
                              ->create_column();
        size_t nb_n = 10;
        Status nb_st = nb_decoder.next_batch(&nb_n, nb_col);
        EXPECT_TRUE(nb_st.is<ErrorCode::CORRUPTION>()) << nb_st.to_string();
    }
}

} // namespace doris
