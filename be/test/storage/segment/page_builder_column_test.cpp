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

// Every page builder takes rows either as StorageValues or as a compute-layer
// column. These tests feed the same rows through both entries and expect the
// same pages, byte for byte, including where the pages break.

#include <gtest/gtest.h>

#include <bit>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/value/bitmap_value.h"
#include "core/value/decimalv2_value.h"
#include "core/value/hll.h"
#include "core/value/vdatetime_value.h"
#include "storage/segment/binary_dict_page.h"
#include "storage/segment/binary_plain_page.h"
#include "storage/segment/binary_plain_page_v2.h"
#include "storage/segment/binary_plain_page_v3.h"
#include "storage/segment/binary_prefix_page.h"
#include "storage/segment/bitshuffle_page.h"
#include "storage/segment/frame_of_reference_page.h"
#include "storage/segment/options.h"
#include "storage/segment/page_builder.h"
#include "storage/segment/plain_page.h"
#include "storage/segment/rle_page.h"
#include "storage/storage_layout.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

// The prefix and dict builders are not templates: every FieldType they serve
// has the same StorageValue. Spell them with one so they fit expect_same_pages below.
template <FieldType>
using PrefixPageBuilder = BinaryPrefixPageBuilder;
template <FieldType>
using DictPageBuilder = BinaryDictPageBuilder;

// The pages a builder produced for its input, in order, and each one's count.
struct Pages {
    std::vector<std::string> bytes;
    std::vector<size_t> counts;
};

template <typename Builder>
std::unique_ptr<Builder> make_builder(const PageBuilderOptions& options) {
    PageBuilder* raw = nullptr;
    EXPECT_TRUE(Builder::create(&raw, options).ok());
    return std::unique_ptr<Builder>(static_cast<Builder*>(raw));
}

// Drives a builder the way ScalarColumnWriter does: add until every row is
// taken, finishing a page whenever the builder reports it full.
template <typename AddFn>
Pages drain(PageBuilder& builder, size_t num_rows, AddFn add) {
    Pages pages;
    auto flush = [&] {
        pages.counts.push_back(builder.count());
        OwnedSlice page;
        EXPECT_TRUE(builder.finish(&page).ok());
        pages.bytes.emplace_back(page.slice().to_string());
        EXPECT_TRUE(builder.reset().ok());
    };
    size_t pos = 0;
    while (pos < num_rows) {
        size_t taken = num_rows - pos;
        EXPECT_TRUE(add(pos, &taken).ok());
        pos += taken;
        if (builder.is_page_full()) {
            flush();
        }
    }
    if (builder.count() > 0) {
        flush();
    }
    return pages;
}

// The cells of a column as the cell entry takes them: the StorageValues back to
// back for a fixed-width FieldType, one Slice per row otherwise.
template <FieldType FT>
struct ReferenceCells {
    using StorageValue = typename StorageLayout<FT>::StorageValue;
    static constexpr bool kSlices = std::is_same_v<StorageValue, Slice>;

    std::vector<uint8_t> bytes;
    std::vector<Slice> slices;
    std::vector<PaddedPODArray<char>> tmp_buffers;

    explicit ReferenceCells(const IColumn& column) {
        const size_t n = column.size();
        if constexpr (kSlices) {
            tmp_buffers.resize(n);
            for (size_t i = 0; i < n; ++i) {
                const StringRef value = StorageLayout<FT>::storage_at(column, i, tmp_buffers[i]);
                slices.emplace_back(value.data, value.size);
            }
        } else {
            bytes.resize(n * sizeof(StorageValue));
            StorageLayout<FT>::column_to_storage(column, 0, n, bytes.data());
        }
    }

    // The cells from `pos` on, through the builder's own cell entry.
    template <class Builder>
    Status add(Builder& builder, size_t pos, size_t* n) const {
        if constexpr (kSlices) {
            return builder.add_slices(slices.data() + pos, n);
        } else {
            return builder.add_cells(reinterpret_cast<const StorageValue*>(bytes.data()) + pos, n);
        }
    }
};

// Feeds `column` through the cell entry and through the column entry of a
// fresh builder each, and expects identical pages.
template <FieldType FT, template <FieldType> class Builder>
void expect_same_pages(const IColumn& column, PageBuilderOptions options, size_t min_pages = 1) {
    const ReferenceCells<FT> cells(column);

    auto by_cells = make_builder<Builder<FT>>(options);
    const Pages expected = drain(*by_cells, column.size(), [&](size_t pos, size_t* n) {
        return cells.add(*by_cells, pos, n);
    });

    auto by_column = make_builder<Builder<FT>>(options);
    const Pages actual = drain(*by_column, column.size(), [&](size_t pos, size_t* n) {
        size_t added = 0;
        const Status status = by_column->add(column, pos, *n, &added);
        *n = added;
        return status;
    });

    EXPECT_GE(expected.bytes.size(), min_pages);
    EXPECT_EQ(expected.counts, actual.counts);
    ASSERT_EQ(expected.bytes.size(), actual.bytes.size());
    for (size_t i = 0; i < expected.bytes.size(); ++i) {
        EXPECT_EQ(expected.bytes[i], actual.bytes[i]) << "page " << i;
    }
}

PageBuilderOptions small_pages(size_t data_page_size) {
    PageBuilderOptions options;
    options.data_page_size = data_page_size;
    return options;
}

ColumnPtr int32_column(size_t n) {
    auto column = ColumnInt32::create();
    for (size_t i = 0; i < n; ++i) {
        column->insert_value(static_cast<int32_t>(i * 7) - 100);
    }
    return column;
}

ColumnPtr date_column(size_t n) {
    auto column = ColumnDate::create();
    for (size_t i = 0; i < n; ++i) {
        VecDateTimeValue value;
        EXPECT_TRUE(value.from_date_int64(20200101 + static_cast<int64_t>(i % 28)));
        column->insert_value(value);
    }
    return column;
}

ColumnPtr datetime_column(size_t n) {
    auto column = ColumnDateTime::create();
    for (size_t i = 0; i < n; ++i) {
        VecDateTimeValue value;
        EXPECT_TRUE(value.from_date_int64(20200101000000 + static_cast<int64_t>(i % 59)));
        column->insert_value(value);
    }
    return column;
}

ColumnPtr string_column(size_t n, size_t max_length) {
    auto column = ColumnString::create();
    for (size_t i = 0; i < n; ++i) {
        std::string value = "v" + std::to_string(i);
        value.resize(std::min(max_length, 1 + (i % max_length)), 'x');
        column->insert_data(value.data(), value.size());
    }
    return column;
}

} // namespace

TEST(PageBuilderColumnTest, PlainInt) {
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_INT, PlainPageBuilder>(*int32_column(200),
                                                                        small_pages(256), 3);
}

TEST(PageBuilderColumnTest, PlainDateV1) {
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DATE, PlainPageBuilder>(*date_column(50),
                                                                         small_pages(64), 2);
}

TEST(PageBuilderColumnTest, BitshuffleFloatWithAndWithoutNaN) {
    auto column = ColumnFloat32::create();
    const auto payload_nan = std::bit_cast<float>(0x7FC12345U);
    for (size_t i = 0; i < 100; ++i) {
        column->insert_value(i % 9 == 0 ? payload_nan : static_cast<float>(i) * 0.5F);
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_FLOAT, BitshufflePageBuilder>(*column,
                                                                               small_pages(128), 2);

    auto without_nan = ColumnFloat32::create();
    for (size_t i = 0; i < 100; ++i) {
        without_nan->insert_value(static_cast<float>(i) * 0.5F);
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_FLOAT, BitshufflePageBuilder>(*without_nan,
                                                                               small_pages(128), 2);
}

TEST(PageBuilderColumnTest, BitshuffleDateTimeV1AndDecimalV1) {
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DATETIME, BitshufflePageBuilder>(
            *datetime_column(70), small_pages(128), 2);

    auto decimals = ColumnDecimal128V2::create(0, 9);
    for (size_t i = 0; i < 40; ++i) {
        decimals->insert_value(DecimalV2Value(static_cast<int64_t>(i) - 20,
                                              static_cast<int64_t>(i) * 1000000 - 5000000));
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DECIMAL, BitshufflePageBuilder>(
            *decimals, small_pages(128), 2);
}

TEST(PageBuilderColumnTest, RleBool) {
    auto column = ColumnUInt8::create();
    for (size_t i = 0; i < 300; ++i) {
        column->insert_value((i / 17) % 2);
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_BOOL, RlePageBuilder>(*column, small_pages(16));
}

TEST(PageBuilderColumnTest, FrameOfReferenceBigIntAndDateV1) {
    auto bigints = ColumnInt64::create();
    for (size_t i = 0; i < 300; ++i) {
        bigints->insert_value(static_cast<int64_t>(i) * 1000 + 12345);
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_BIGINT, FrameOfReferencePageBuilder>(
            *bigints, small_pages(64));
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_DATE, FrameOfReferencePageBuilder>(
            *date_column(300), small_pages(64));
}

TEST(PageBuilderColumnTest, BinaryPlainStrings) {
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_VARCHAR, BinaryPlainPageBuilder>(
            *string_column(60, 12), small_pages(96), 3);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_CHAR, BinaryPlainPageBuilder>(
            *string_column(60, 8), small_pages(96), 3);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_STRING, BinaryPlainPageV2Builder>(
            *string_column(60, 12), small_pages(96), 3);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_CHAR, BinaryPlainPageV3Builder>(
            *string_column(60, 8), small_pages(96), 3);
}

TEST(PageBuilderColumnTest, BinaryPlainObjects) {
    auto bitmaps = ColumnBitmap::create();
    auto hlls = ColumnHLL::create();
    for (size_t i = 0; i < 30; ++i) {
        BitmapValue bitmap;
        for (size_t j = 0; j <= i % 5; ++j) {
            bitmap.add(i * 10 + j);
        }
        bitmaps->insert_value(bitmap);
        hlls->insert_value(HyperLogLog(static_cast<uint64_t>(i * 31)));
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_BITMAP, BinaryPlainPageBuilder>(
            *bitmaps, small_pages(128), 2);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_HLL, BinaryPlainPageV3Builder>(
            *hlls, small_pages(128), 2);
}

TEST(PageBuilderColumnTest, DictionaryOverflowFallsBackToPlain) {
    PageBuilderOptions options = small_pages(256);
    options.dict_page_size = 128;
    // Few distinct values first, then many: the dictionary page fills up and the
    // builder falls back to plain pages of this column's own cells.
    auto column = ColumnString::create();
    for (size_t i = 0; i < 200; ++i) {
        const std::string value = i < 100 ? "k" + std::to_string(i % 4) : "u" + std::to_string(i);
        column->insert_data(value.data(), value.size());
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_CHAR, DictPageBuilder>(*column, options, 2);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_VARCHAR, DictPageBuilder>(*column, options, 2);
}

TEST(PageBuilderColumnTest, PrefixStrings) {
    auto column = ColumnString::create();
    for (size_t i = 0; i < 120; ++i) {
        const std::string value = "prefix-" + std::to_string(1000 + i);
        column->insert_data(value.data(), value.size());
    }
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_VARCHAR, PrefixPageBuilder>(*column,
                                                                             small_pages(128), 2);
    expect_same_pages<FieldType::OLAP_FIELD_TYPE_CHAR, PrefixPageBuilder>(*column, small_pages(128),
                                                                          2);
}

} // namespace doris::segment_v2
