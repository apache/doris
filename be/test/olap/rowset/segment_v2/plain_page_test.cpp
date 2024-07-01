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

#include "olap/rowset/segment_v2/plain_page.h"

#include <gtest/gtest.h>

#include <iostream>

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/types.h"

namespace doris {
using namespace ErrorCode;
namespace segment_v2 {

class PlainPageTest : public testing::Test {
public:
    PlainPageTest() {}

    virtual ~PlainPageTest() {}

    PageBuilderOptions* new_builder_options() {
        auto ret = new PageBuilderOptions();
        ret->data_page_size = 256 * 1024;
        return ret;
    }

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

        PageDecoderOptions decoder_options;
        PageDecoderType page_decoder(s.slice(), decoder_options);
        Status status = page_decoder.init();
        EXPECT_TRUE(status.ok());

        EXPECT_EQ(0, page_decoder.current_index());

        vectorized::Arena pool;

        std::unique_ptr<ColumnVectorBatch> cvb;
        ColumnVectorBatch::create(size, true, get_scalar_type_info(Type), nullptr, &cvb);
        ColumnBlock block(cvb.get(), &pool);
        ColumnBlockView column_block_view(&block);
        status = page_decoder.next_batch(&size, &column_block_view);
        EXPECT_TRUE(status.ok());

        CppType* decoded = reinterpret_cast<CppType*>(block.data());
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

        PageDecoderOptions decoder_options;
        PageDecoderType page_decoder(s.slice(), decoder_options);
        Status status = page_decoder.init();

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

        if (small_than_smallest != nullptr) {
            status = page_decoder.seek_at_or_after_value(small_than_smallest, &exact_match);
            EXPECT_EQ(0, page_decoder.current_index());
            EXPECT_TRUE(status.ok());
            EXPECT_FALSE(exact_match);
        }

        if (bigger_than_biggest != nullptr) {
            status = page_decoder.seek_at_or_after_value(bigger_than_biggest, &exact_match);
            EXPECT_EQ(status.code(), ENTRY_NOT_FOUND);
        }
    }
};

TEST_F(PlainPageTest, TestInt32PlainPageRandom) {
    const uint32_t size = 10000;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<FieldType::OLAP_FIELD_TYPE_INT,
                                     segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
                                     segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(
            ints.get(), size);
}

TEST_F(PlainPageTest, TestInt32PlainPageSeekValue) {
    const uint32_t size = 1000;
    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = i + 100;
    }
    int32_t small_than_smallest = 99;
    int32_t bigger_than_biggest = 1111;

    test_seek_at_or_after_value_template<
            FieldType::OLAP_FIELD_TYPE_INT,
            segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
            segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(
            ints.get(), size, &small_than_smallest, &bigger_than_biggest);
}

TEST_F(PlainPageTest, TestInt64PlainPageRandom) {
    const uint32_t size = 10000;
    std::unique_ptr<int64_t[]> ints(new int64_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = random();
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_BIGINT,
            segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_BIGINT>,
            segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_BIGINT>>(ints.get(), size);
}

TEST_F(PlainPageTest, TestInt64PlainPageSeekValue) {
    const uint32_t size = 1000;
    std::unique_ptr<int64_t[]> ints(new int64_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = i + 100;
    }
    int64_t small_than_smallest = 99;
    int64_t bigger_than_biggest = 1111;

    test_seek_at_or_after_value_template<
            FieldType::OLAP_FIELD_TYPE_BIGINT,
            segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_BIGINT>,
            segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_BIGINT>>(
            ints.get(), size, &small_than_smallest, &bigger_than_biggest);
}

TEST_F(PlainPageTest, TestPlainFloatBlockEncoderRandom) {
    const uint32_t size = 10000;

    std::unique_ptr<float[]> floats(new float[size]);
    for (int i = 0; i < size; i++) {
        floats.get()[i] = random() + static_cast<float>(random()) / static_cast<float>(INT_MAX);
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_FLOAT,
            segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_FLOAT>,
            segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_FLOAT>>(floats.get(), size);
}

TEST_F(PlainPageTest, TestDoublePageEncoderRandom) {
    const uint32_t size = 10000;
    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        doubles.get()[i] = random() + static_cast<double>(random()) / INT_MAX;
    }
    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_DOUBLE,
            segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_DOUBLE>,
            segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_DOUBLE>>(doubles.get(), size);
}

TEST_F(PlainPageTest, TestDoublePageEncoderEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<double[]> doubles(new double[size]);
    for (int i = 0; i < size; i++) {
        doubles.get()[i] = 19880217.19890323;
    }

    test_encode_decode_page_template<
            FieldType::OLAP_FIELD_TYPE_DOUBLE,
            segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_DOUBLE>,
            segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_DOUBLE>>(doubles.get(), size);
}

TEST_F(PlainPageTest, TestDoublePageEncoderSequence) {
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
            segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_DOUBLE>,
            segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_DOUBLE>>(doubles.get(), size);
}

TEST_F(PlainPageTest, TestPlainInt32PageEncoderEqual) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    for (int i = 0; i < size; i++) {
        ints.get()[i] = 12345;
    }

    test_encode_decode_page_template<FieldType::OLAP_FIELD_TYPE_INT,
                                     segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
                                     segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(
            ints.get(), size);
}

TEST_F(PlainPageTest, TestInt32PageEncoderSequence) {
    const uint32_t size = 10000;

    std::unique_ptr<int32_t[]> ints(new int32_t[size]);
    int32_t number = 0;
    for (int i = 0; i < size; i++) {
        ints.get()[i] = ++number;
    }

    test_encode_decode_page_template<FieldType::OLAP_FIELD_TYPE_INT,
                                     segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_INT>,
                                     segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_INT>>(
            ints.get(), size);
}

TEST_F(PlainPageTest, TestBoolPlainPageSeekValue) {
    std::unique_ptr<bool[]> bools(new bool[2]);
    bools.get()[0] = false;
    bools.get()[1] = true;

    test_seek_at_or_after_value_template<
            FieldType::OLAP_FIELD_TYPE_BOOL,
            segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_BOOL>,
            segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_BOOL>>(bools.get(), 2, nullptr,
                                                                           nullptr);

    bool t = true;
    test_seek_at_or_after_value_template<
            FieldType::OLAP_FIELD_TYPE_BOOL,
            segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_BOOL>,
            segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_BOOL>>(bools.get(), 1, nullptr,
                                                                           &t);

    t = false;
    test_seek_at_or_after_value_template<
            FieldType::OLAP_FIELD_TYPE_BOOL,
            segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_BOOL>,
            segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_BOOL>>(&bools.get()[1], 1, &t,
                                                                           nullptr);
}

} // namespace segment_v2
} // namespace doris
