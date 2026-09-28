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

#include "storage/segment/bitshuffle_page.h"

#include <gtest/gtest.h>

#include <memory>

#include "core/data_type/data_type_factory.hpp"
#include "storage/segment/bitshuffle_page_pre_decoder.h"
#include "storage/segment/options.h"
#include "storage/segment/page_builder.h"
#include "storage/segment/page_decoder.h"

using doris::segment_v2::PageBuilderOptions;
using doris::OlapReaderStatistics;

namespace doris {
using namespace ErrorCode;

class BitShufflePageTest : public testing::Test {
public:
    virtual ~BitShufflePageTest() {}

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
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        segment_v2::PageBuilder* builder = nullptr;
        ASSERT_TRUE(PageBuilderType::create(&builder, options).ok());
        std::unique_ptr<segment_v2::PageBuilder> page_builder(builder);

        EXPECT_TRUE(page_builder->add(reinterpret_cast<const uint8_t*>(src), &size).ok());
        OwnedSlice s;
        EXPECT_TRUE(page_builder->finish(&s).ok());

        segment_v2::PageDecoderOptions decoder_options;
        PageDecoderType page_decoder_(s.slice(), decoder_options);
        Status status = page_decoder_.init();
        EXPECT_FALSE(status.ok());

        segment_v2::BitShufflePagePreDecoder pre_decoder;
        Slice page_slice = s.slice();
        std::unique_ptr<DataPage> decoded_page;
        EXPECT_TRUE(pre_decoder
                            .decode(&decoded_page, &page_slice, 0, false,
                                    segment_v2::PageTypePB::DATA_PAGE, "")
                            .ok());
        PageDecoderType page_decoder(page_slice, decoder_options);
        status = page_decoder.init();
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(0, page_decoder.current_index());

        auto column = DataTypeFactory::instance().create_data_type(Type, 0, 0)->create_column();
        status = page_decoder.next_batch(&size, column);
        EXPECT_TRUE(status.ok());

        const auto* decoded = reinterpret_cast<const CppType*>(column->get_raw_data().data);
        for (uint i = 0; i < size; i++) {
            if (src[i] != decoded[i]) {
                FAIL() << "Fail at index " << i << " inserted=" << src[i] << " got=" << decoded[i];
            }
        }

        // Test Seek within block by ordinal
        for (int i = 0; i < 100; i++) {
            int seek_off = random() % size;
            EXPECT_TRUE(page_decoder.seek_to_position_in_page(seek_off).ok());
            EXPECT_EQ((int32_t)(seek_off), page_decoder.current_index());
            CppType ret;
            copy_one<Type, PageDecoderType>(&page_decoder, &ret);
            EXPECT_EQ(decoded[seek_off], ret);
        }
    }

    // The values inserted should be sorted.
    template <FieldType Type, class PageBuilderType, class PageDecoderType>
    void test_seek_at_or_after_value_template(
            typename TypeTraits<Type>::CppType* src, size_t size,
            typename TypeTraits<Type>::CppType* small_than_smallest,
            typename TypeTraits<Type>::CppType* bigger_than_biggest) {
        typedef typename TypeTraits<Type>::CppType CppType;
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        segment_v2::PageBuilder* builder = nullptr;
        ASSERT_TRUE(PageBuilderType::create(&builder, options).ok());
        std::unique_ptr<segment_v2::PageBuilder> page_builder(builder);

        EXPECT_TRUE(page_builder->add(reinterpret_cast<const uint8_t*>(src), &size).ok());
        OwnedSlice s;
        EXPECT_TRUE(page_builder->finish(&s).ok());

        segment_v2::PageDecoderOptions decoder_options;
        PageDecoderType page_decoder_(s.slice(), decoder_options);
        Status status = page_decoder_.init();
        EXPECT_FALSE(status.ok());

        segment_v2::BitShufflePagePreDecoder pre_decoder;
        Slice page_slice = s.slice();
        std::unique_ptr<DataPage> decoded_page;
        EXPECT_TRUE(pre_decoder
                            .decode(&decoded_page, &page_slice, 0, false,
                                    segment_v2::PageTypePB::DATA_PAGE, "")
                            .ok());
        PageDecoderType page_decoder(page_slice, decoder_options);
        status = page_decoder.init();
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(0, page_decoder.current_index());

        size_t index = random() % size;
        CppType seek_value = src[index];
        bool exact_match;
        status = page_decoder.seek_at_or_after_value(&seek_value, &exact_match);
        EXPECT_EQ(index, page_decoder.current_index());
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(exact_match);

        CppType last_value = src[size - 1];
        status = page_decoder.seek_at_or_after_value(&last_value, &exact_match);
        EXPECT_EQ(size - 1, page_decoder.current_index());
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(exact_match);

        CppType first_value = src[0];
        status = page_decoder.seek_at_or_after_value(&first_value, &exact_match);
        EXPECT_EQ(0, page_decoder.current_index());
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(exact_match);

        status = page_decoder.seek_at_or_after_value(small_than_smallest, &exact_match);
        EXPECT_EQ(0, page_decoder.current_index());
        EXPECT_TRUE(status.ok());
        EXPECT_FALSE(exact_match);

        status = page_decoder.seek_at_or_after_value(bigger_than_biggest, &exact_match);
        EXPECT_EQ(status.code(), ENTRY_NOT_FOUND);
    }
};

// Test for bitshuffle block, for INT32, INT64, FLOAT, DOUBLE
TEST_F(BitShufflePageTest, TestBitShuffleInt32BlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_INT,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(ints.get(), size);
}

TEST_F(BitShufflePageTest, TestBitShuffleInt64BlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<int64_t[]> ints(new int64_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_BIGINT,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_BIGINT>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_BIGINT>>(ints.get(), size);
}

TEST_F(BitShufflePageTest, TestBitShuffleFloatBlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<float[]> floats(new float[size]);
    for (int i = 0; i < size; i++) {
        floats.get()[i] = random() + static_cast<float>(random()) / static_cast<float>(INT_MAX);
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_FLOAT,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_FLOAT>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_FLOAT>>(floats.get(),
                                                                                 size);
}

TEST_F(BitShufflePageTest, TestBitShuffleDoubleBlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        doubles.get()[i] = random() + static_cast<double>(random()) / INT_MAX;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_DOUBLE,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_DOUBLE>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_DOUBLE>>(doubles.get(),
                                                                                  size);
}

TEST_F(BitShufflePageTest, TestBitShuffleDoubleBlockEncoderEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        doubles.get()[i] = 19880217.19890323;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_DOUBLE,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_DOUBLE>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_DOUBLE>>(doubles.get(),
                                                                                  size);
}

TEST_F(BitShufflePageTest, TestBitShuffleDoubleBlockEncoderSequence) {
    const uint32_t size = 10000;

    double base = 19880217.19890323;
    double delta = 13.14;
    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        base = base + delta;
        doubles.get()[i] = base;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_DOUBLE,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_DOUBLE>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_DOUBLE>>(doubles.get(),
                                                                                  size);
}

TEST_F(BitShufflePageTest, TestBitShuffleInt32BlockEncoderEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_INT,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(ints.get(), size);
}

TEST_F(BitShufflePageTest, TestBitShuffleInt32BlockEncoderMaxNumberEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 1234567890;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_INT,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(ints.get(), size);
}

TEST_F(BitShufflePageTest, TestBitShuffleInt32BlockEncoderSequence) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    int32_t number = 0;
    for (int i = 0; i < size; i++) {
        ints.get()[i] = ++number;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_INT,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(ints.get(), size);
}

TEST_F(BitShufflePageTest, TestBitShuffleInt32BlockEncoderMaxNumberSequence) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    int32_t number = 0;
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 1234567890 + number;
        ++number;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_INT,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(ints.get(), size);
}

TEST_F(BitShufflePageTest, TestBitShuffleFloatBlockEncoderSeekValue) {
    const uint32_t size = 1000;
    std::unique_ptr<float[]> floats(new float[size]);
    for (int i = 0; i < size; i++) {
        floats.get()[i] = i + 100 + static_cast<float>(random()) / static_cast<float>(INT_MAX);
    }

    float small_than_smallest = 99.9;
    float bigger_than_biggest = 1111.1;
    test_seek_at_or_after_value_template<
            FieldType::OLAP_FIELD_TYPE_FLOAT,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_FLOAT>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_FLOAT>>(
            floats.get(), size, &small_than_smallest, &bigger_than_biggest);
}

TEST_F(BitShufflePageTest, TestBitShuffleDoubleBlockEncoderSeekValue) {
    const uint32_t size = 1000;
    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        doubles.get()[i] = i + 100 + static_cast<double>(random()) / INT_MAX;
    }

    double small_than_smallest = 99.9;
    double bigger_than_biggest = 1111.1;
    test_seek_at_or_after_value_template<
            FieldType::OLAP_FIELD_TYPE_DOUBLE,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_DOUBLE>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_DOUBLE>>(
            doubles.get(), size, &small_than_smallest, &bigger_than_biggest);
}

TEST_F(BitShufflePageTest, TestBitShuffleDecimal12BlockEncoderSeekValue) {
    const uint32_t size = 1000;
    std::unique_ptr<decimal12_t[]> decimals(new decimal12_t[size]);
    for (int i = 0; i < size; i++) {
        decimals.get()[i] = {i + 100, std::rand()};
    }

    decimal12_t small_than_smallest = {99, 9};
    decimal12_t bigger_than_biggest = {1111, 1};
    test_seek_at_or_after_value_template<
            FieldType::OLAP_FIELD_TYPE_DECIMAL,
            segment_v2::BitshufflePageBuilder<FieldType::OLAP_FIELD_TYPE_DECIMAL>,
            segment_v2::BitShufflePageDecoder<FieldType::OLAP_FIELD_TYPE_DECIMAL>>(
            decimals.get(), size, &small_than_smallest, &bigger_than_biggest);
}

} // namespace doris
