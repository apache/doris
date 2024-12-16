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

#include "olap/rowset/segment_v2/bitshuffle_page.h"

#include <gtest/gtest.h>

#include <memory>

#include "olap/rowset/segment_v2/bitshuffle_page_pre_decoder.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"

using doris::segment_v2::PageBuilderOptions;
using doris::OlapReaderStatistics;

namespace doris {
using namespace ErrorCode;

class BitShufflePageTest : public testing::Test {
public:
    virtual ~BitShufflePageTest() {}

    template <FieldType type, class PageDecoderType>
    void copy_one(PageDecoderType* decoder, typename TypeTraits<type>::CppType* ret) {
        vectorized::Arena pool;
        std::unique_ptr<ColumnVectorBatch> cvb;
        ColumnVectorBatch::create(1, true, get_scalar_type_info(type), nullptr, &cvb);
        ColumnBlock block(cvb.get(), &pool);
        ColumnBlockView column_block_view(&block);

        size_t n = 1;
        decoder->_copy_next_values(n, column_block_view.data());
        EXPECT_EQ(1, n);
        *ret = *reinterpret_cast<const typename TypeTraits<type>::CppType*>(block.cell_ptr(0));
    }

    template <FieldType Type, class PageBuilderType, class PageDecoderType>
    void test_encode_decode_page_template(typename TypeTraits<Type>::CppType* src, size_t size) {
        typedef typename TypeTraits<Type>::CppType CppType;
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        PageBuilderType page_builder(options);
        Status ret0 = page_builder.init();
        EXPECT_TRUE(ret0.ok());

        page_builder.add(reinterpret_cast<const uint8_t*>(src), &size);
        OwnedSlice s = page_builder.finish();

        //check first value and last value
        CppType first_value;
        page_builder.get_first_value(&first_value);
        EXPECT_EQ(src[0], first_value);
        CppType last_value;
        page_builder.get_last_value(&last_value);
        EXPECT_EQ(src[size - 1], last_value);

        segment_v2::PageDecoderOptions decoder_options;
        PageDecoderType page_decoder_(s.slice(), decoder_options);
        Status status = page_decoder_.init();
        EXPECT_FALSE(status.ok());

        segment_v2::BitShufflePagePreDecoder<false> pre_decoder;
        Slice page_slice = s.slice();
        std::unique_ptr<char[]> auto_release;
        pre_decoder.decode(&auto_release, &page_slice, 0);
        PageDecoderType page_decoder(page_slice, decoder_options);
        status = page_decoder.init();
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(0, page_decoder.current_index());

        vectorized::Arena pool;

        std::unique_ptr<ColumnVectorBatch> cvb;
        ColumnVectorBatch::create(size, false, get_scalar_type_info(Type), nullptr, &cvb);
        ColumnBlock block(cvb.get(), &pool);
        ColumnBlockView column_block_view(&block);

        status = page_decoder.next_batch(&size, &column_block_view);
        EXPECT_TRUE(status.ok());

        CppType* values = reinterpret_cast<CppType*>(block.data());
        CppType* decoded = (CppType*)values;
        for (uint i = 0; i < size; i++) {
            if (src[i] != decoded[i]) {
                FAIL() << "Fail at index " << i << " inserted=" << src[i] << " got=" << decoded[i];
            }
        }

        // Test Seek within block by ordinal
        for (int i = 0; i < 100; i++) {
            int seek_off = random() % size;
            page_decoder.seek_to_position_in_page(seek_off);
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
        PageBuilderType page_builder(options);
        Status ret0 = page_builder.init();
        EXPECT_TRUE(ret0.ok());

        page_builder.add(reinterpret_cast<const uint8_t*>(src), &size);
        OwnedSlice s = page_builder.finish();

        segment_v2::PageDecoderOptions decoder_options;
        PageDecoderType page_decoder_(s.slice(), decoder_options);
        Status status = page_decoder_.init();
        EXPECT_FALSE(status.ok());

        segment_v2::BitShufflePagePreDecoder<false> pre_decoder;
        Slice page_slice = s.slice();
        std::unique_ptr<char[]> auto_release;
        pre_decoder.decode(&auto_release, &page_slice, 0);
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
