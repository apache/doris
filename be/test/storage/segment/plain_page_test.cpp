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

#include "storage/segment/plain_page.h"

#include <gtest/gtest.h>

#include <iostream>

#include "common/logging.h"
#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/uint24.h"
#include "core/value/vdatetime_value.h"
#include "storage/olap_common.h"
#include "storage/segment/page_builder.h"
#include "storage/segment/page_decoder.h"
#include "storage/storage_layout.h"
#include "storage/types.h"

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
        PageBuilder* builder = nullptr;
        ASSERT_TRUE(PageBuilderType::create(&builder, options).ok());
        std::unique_ptr<PageBuilder> page_builder(builder);

        EXPECT_TRUE(page_builder->add(reinterpret_cast<const uint8_t*>(src), &size).ok());
        OwnedSlice s;
        EXPECT_TRUE(page_builder->finish(&s).ok());

        PageDecoderOptions decoder_options;
        PageDecoderType page_decoder(s.slice(), decoder_options);
        Status status = page_decoder.init();
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

    template <FieldType Type, class PageBuilderType, class PageDecoderType>
    void test_seek_at_or_after_value_template(
            typename TypeTraits<Type>::CppType* src, size_t size,
            typename TypeTraits<Type>::CppType* small_than_smallest,
            typename TypeTraits<Type>::CppType* bigger_than_biggest) {
        typedef typename TypeTraits<Type>::CppType CppType;

        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        PageBuilder* builder = nullptr;
        ASSERT_TRUE(PageBuilderType::create(&builder, options).ok());
        std::unique_ptr<PageBuilder> page_builder(builder);

        EXPECT_TRUE(page_builder->add(reinterpret_cast<const uint8_t*>(src), &size).ok());
        OwnedSlice s;
        EXPECT_TRUE(page_builder->finish(&s).ok());

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

    // Eight consecutive days from 2020-01-01 in one plain page of V1 DATE cells, each date
    // stored as a uint24.
    void build_date_v1_page(std::vector<VecDateTimeValue>* days, OwnedSlice* page) {
        using Layout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>;
        constexpr size_t kRows = 8;
        days->resize(kRows);
        std::vector<uint24_t> cells(kRows);
        for (size_t i = 0; i < kRows; ++i) {
            ASSERT_TRUE((*days)[i].from_date_int64(static_cast<int64_t>(20200101 + i)));
            cells[i] = Layout::to_storage((*days)[i]);
        }
        PageBuilderOptions options;
        options.data_page_size = 256 * 1024;
        PageBuilder* builder = nullptr;
        ASSERT_TRUE(
                PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_DATE>::create(&builder, options).ok());
        std::unique_ptr<PageBuilder> page_builder(builder);
        size_t added = kRows;
        ASSERT_TRUE(page_builder->add(reinterpret_cast<const uint8_t*>(cells.data()), &added).ok());
        ASSERT_EQ(added, kRows);
        ASSERT_TRUE(page_builder->finish(page).ok());
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

TEST_F(PlainPageTest, TestBoolPlainPageSeekValue) {
    std::unique_ptr<uint8_t[]> bools(new uint8_t[2]);
    bools.get()[0] = false;
    bools.get()[1] = true;

    test_seek_at_or_after_value_template<
            FieldType::OLAP_FIELD_TYPE_BOOL,
            segment_v2::PlainPageBuilder<FieldType::OLAP_FIELD_TYPE_BOOL>,
            segment_v2::PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_BOOL>>(bools.get(), 2, nullptr,
                                                                           nullptr);

    uint8_t t = true;
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

// A V1 DATE page holds each date as a uint24 cell; a batch decodes the cells into the dates
// of a ColumnDate.
TEST_F(PlainPageTest, TestDateV1PlainPageDecodesIntoDates) {
    std::vector<VecDateTimeValue> days;
    OwnedSlice page;
    ASSERT_NO_FATAL_FAILURE(build_date_v1_page(&days, &page));
    PageDecoderOptions decoder_options;
    PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_DATE> decoder(page.slice(), decoder_options);
    ASSERT_TRUE(decoder.init().ok());

    MutableColumnPtr dates = ColumnDate::create();
    size_t n = days.size();
    ASSERT_TRUE(decoder.next_batch(&n, dates).ok());
    ASSERT_EQ(n, days.size());
    const auto& decoded = assert_cast<const ColumnDate&>(*dates).get_data();
    for (size_t i = 0; i < days.size(); ++i) {
        EXPECT_EQ(decoded[i], days[i]) << "row " << i;
    }
}

// Rows read by id into a ColumnDate come back as their dates.
TEST_F(PlainPageTest, TestDateV1PlainPageReadsRowIdsIntoDates) {
    std::vector<VecDateTimeValue> days;
    OwnedSlice page;
    ASSERT_NO_FATAL_FAILURE(build_date_v1_page(&days, &page));
    PageDecoderOptions decoder_options;
    PlainPageDecoder<FieldType::OLAP_FIELD_TYPE_DATE> decoder(page.slice(), decoder_options);
    ASSERT_TRUE(decoder.init().ok());

    const rowid_t rowids[] = {1, 3, 6};
    MutableColumnPtr picked = ColumnDate::create();
    size_t n = 3;
    ASSERT_TRUE(decoder.read_by_rowids(rowids, 0, &n, picked).ok());
    ASSERT_EQ(n, 3);
    const auto& picked_dates = assert_cast<const ColumnDate&>(*picked).get_data();
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(picked_dates[i], days[rowids[i]]) << "row id " << rowids[i];
    }
}

} // namespace segment_v2
} // namespace doris
