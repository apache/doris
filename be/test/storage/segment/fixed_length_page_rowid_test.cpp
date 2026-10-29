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
#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/decimal12.h"
#include "core/value/decimalv2_value.h"
#include "storage/segment/bitshuffle_page.h"
#include "storage/segment/bitshuffle_page_pre_decoder.h"
#include "storage/segment/plain_page.h"
#include "storage/storage_layout.h"

namespace doris::segment_v2 {
namespace {

template <PrimitiveType P, FieldType F>
struct RowidType {
    static constexpr auto primitive = P;
    static constexpr auto field = F;
    using Storage = typename TypeTraits<F>::CppType;
    using Column = typename PrimitiveTypeTraits<P>::ColumnType;

    static MutableColumnPtr column() {
        MutableColumnPtr result;
        if constexpr (is_decimal(P)) {
            result = Column::create(0, 9);
        } else {
            result = Column::create();
        }
        return result;
    }

    static std::vector<Storage> values() {
        // A non-multiple of eight exercises bitshuffle padding. Byte patterns also cover
        // wide values without narrowing their representation to a small integer.
        std::vector<Storage> result(67);
        for (size_t i = 0; i < result.size(); ++i) {
            std::array<unsigned char, sizeof(Storage)> bytes;
            for (size_t j = 0; j < bytes.size(); ++j) {
                bytes[j] = static_cast<unsigned char>(i * 31 + j * 17);
            }
            memcpy(&result[i], bytes.data(), bytes.size());
            if constexpr (P == TYPE_DATE) {
                result[i] = uint24_t((2024U << 9) | (2U << 5) | (i % 28 + 1));
            } else if constexpr (P == TYPE_DATETIME) {
                result[i] = 20240201010203L + (i % 28) * 1000000;
            } else if constexpr (P == TYPE_DECIMALV2) {
                result[i] = decimal12_t {.integer = static_cast<int64_t>(i) - 9,
                                         .fraction = (static_cast<int32_t>(i) - 9) * 1234567};
            } else if constexpr (P == TYPE_BOOLEAN) {
                result[i] = i % 2;
            }
        }
        return result;
    }
};

using RowidTypes =
        ::testing::Types<RowidType<TYPE_BOOLEAN, FieldType::OLAP_FIELD_TYPE_BOOL>,
                         RowidType<TYPE_TINYINT, FieldType::OLAP_FIELD_TYPE_TINYINT>,
                         RowidType<TYPE_SMALLINT, FieldType::OLAP_FIELD_TYPE_SMALLINT>,
                         RowidType<TYPE_INT, FieldType::OLAP_FIELD_TYPE_INT>,
                         RowidType<TYPE_BIGINT, FieldType::OLAP_FIELD_TYPE_BIGINT>,
                         RowidType<TYPE_LARGEINT, FieldType::OLAP_FIELD_TYPE_LARGEINT>,
                         RowidType<TYPE_UINT32, FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>,
                         RowidType<TYPE_UINT64, FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>,
                         RowidType<TYPE_FLOAT, FieldType::OLAP_FIELD_TYPE_FLOAT>,
                         RowidType<TYPE_DOUBLE, FieldType::OLAP_FIELD_TYPE_DOUBLE>,
                         RowidType<TYPE_DATE, FieldType::OLAP_FIELD_TYPE_DATE>,
                         RowidType<TYPE_DATETIME, FieldType::OLAP_FIELD_TYPE_DATETIME>,
                         RowidType<TYPE_DATEV2, FieldType::OLAP_FIELD_TYPE_DATEV2>,
                         RowidType<TYPE_DATETIMEV2, FieldType::OLAP_FIELD_TYPE_DATETIMEV2>,
                         RowidType<TYPE_TIMESTAMPTZ, FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ>,
                         RowidType<TYPE_DECIMALV2, FieldType::OLAP_FIELD_TYPE_DECIMAL>,
                         RowidType<TYPE_DECIMAL32, FieldType::OLAP_FIELD_TYPE_DECIMAL32>,
                         RowidType<TYPE_DECIMAL64, FieldType::OLAP_FIELD_TYPE_DECIMAL64>,
                         RowidType<TYPE_DECIMAL128I, FieldType::OLAP_FIELD_TYPE_DECIMAL128I>,
                         RowidType<TYPE_DECIMAL256, FieldType::OLAP_FIELD_TYPE_DECIMAL256>,
                         RowidType<TYPE_IPV4, FieldType::OLAP_FIELD_TYPE_IPV4>,
                         RowidType<TYPE_IPV6, FieldType::OLAP_FIELD_TYPE_IPV6>,
                         RowidType<TYPE_UUID, FieldType::OLAP_FIELD_TYPE_UUID>,
                         RowidType<TYPE_TIMESTAMP_NS, FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS>>;

template <typename T>
class FixedLengthPageRowidTest : public ::testing::Test {
protected:
    void expect_equal(const IColumn& expected, const IColumn& actual) {
        ASSERT_EQ(expected.size(), actual.size());
        const IColumn* expected_data = &expected;
        const IColumn* actual_data = &actual;
        if (expected.is_nullable()) {
            const auto& e = assert_cast<const ColumnNullable&>(expected);
            const auto& a = assert_cast<const ColumnNullable&>(actual);
            ASSERT_EQ(e.get_null_map_data().size(), a.get_null_map_data().size());
            for (size_t i = 0; i < expected.size(); ++i) {
                EXPECT_EQ(e.is_null_at(i), a.is_null_at(i));
            }
            expected_data = &e.get_nested_column();
            actual_data = &a.get_nested_column();
        }
        for (size_t i = 0; i < expected.size(); ++i) {
            auto e = expected_data->get_data_at(i);
            auto a = actual_data->get_data_at(i);
            ASSERT_EQ(e.size, a.size);
            EXPECT_EQ(0, memcmp(e.data, a.data, e.size)) << "row " << i;
        }
    }

    template <typename Append>
    void check_append(Append&& append) {
        const auto values = T::values();
        const uint32_t base = 1000;
        const uint32_t page_end = base + static_cast<uint32_t>(values.size());
        const uint32_t indices[] = {base,      base + 2, base + 2,    base + 8,
                                    base + 18, page_end, page_end + 4};
        auto all = T::column();
        StorageLayout<T::field>::storage_to_column(reinterpret_cast<const uint8_t*>(values.data()),
                                                   values.size(), *all);
        auto empty_actual = T::column();
        auto empty_expected = T::column();
        EXPECT_EQ(0, append(nullptr, 0, base, empty_actual));
        EXPECT_EQ(0, append(indices + 5, 2, base, empty_actual));
        EXPECT_TRUE(empty_actual->empty());
        for (size_t i = 0; i < 5; ++i) {
            empty_expected->insert_from(*all, indices[i] - base);
        }
        EXPECT_EQ(5, append(indices, std::size(indices), base, empty_actual));
        expect_equal(*empty_expected, *empty_actual);

        auto expected = T::column();
        auto actual = T::column();
        expected->insert_default();
        actual->insert_default();
        for (size_t i = 0; i < 5; ++i) {
            expected->insert_from(*all, indices[i] - base);
        }
        EXPECT_EQ(5, append(indices, std::size(indices), base, actual));
        expect_equal(*expected, *actual);

        EXPECT_EQ(0, append(nullptr, 0, base, actual));
        EXPECT_EQ(0, append(indices + 5, 2, base, actual));
        expect_equal(*expected, *actual);

        // A subsequent call appends without overwriting either the old prefix or the first batch.
        for (size_t i = 0; i < 3; ++i) {
            expected->insert_from(*all, indices[i] - base);
        }
        EXPECT_EQ(3, append(indices, 3, base, actual));
        expect_equal(*expected, *actual);

        // Check complete runs, page-end truncation, duplicates and descending rowids.
        // The last two cases must stop at the first rowid below the page's base.
        std::vector<std::vector<uint32_t>> selections = {
                {base, base + 1, base + 2, base + 3, base + 4, base + 5},
                {base + 1, base + 2, base + 3, base + 7, base + 8, base + 9, base + 9, base + 8,
                 base + 15, base + 16, base + 17, base + 18, base + 19, base + 20},
                {page_end - 2, page_end - 1, page_end},
                {base + 4},
                {base - 1, base},
                {base, base - 1, base + 1}};
        std::vector<uint32_t> complete;
        for (uint32_t rowid = base; rowid < page_end; ++rowid) {
            complete.push_back(rowid);
        }
        selections.push_back(complete);
        // Equal endpoints can hide duplicate or reordered rowids inside a block.
        auto duplicate = complete;
        duplicate[14] = duplicate[13];
        selections.push_back(duplicate);
        auto descending = complete;
        std::swap(descending[5], descending[6]);
        selections.push_back(descending);
        complete.insert(complete.end(), selections.back().begin(), selections.back().end());
        selections.push_back(complete);
        for (const auto& selection : selections) {
            auto wanted = T::column();
            auto result = T::column();
            wanted->insert_default();
            result->insert_default();
            for (auto rowid : selection) {
                const uint64_t index = static_cast<uint64_t>(rowid) - base;
                if (index >= values.size()) {
                    break;
                }
                wanted->insert_from(*all, index);
            }
            EXPECT_EQ(wanted->size() - 1, append(selection.data(), selection.size(), base, result));
            expect_equal(*wanted, *result);
        }
    }

    template <bool BitShuffle>
    void check_page() {
        const auto values = T::values();
        using Builder = std::conditional_t<BitShuffle, BitshufflePageBuilder<T::field>,
                                           PlainPageBuilder<T::field>>;
        using Decoder = std::conditional_t<BitShuffle, BitShufflePageDecoder<T::field>,
                                           PlainPageDecoder<T::field>>;
        PageBuilderOptions options;
        options.data_page_size = 65536;
        PageBuilder* builder_raw = nullptr;
        ASSERT_TRUE(Builder::create(&builder_raw, options).ok());
        std::unique_ptr<PageBuilder> builder(builder_raw);
        size_t count = values.size();
        ASSERT_TRUE(builder->add(reinterpret_cast<const uint8_t*>(values.data()), &count).ok());
        ASSERT_EQ(values.size(), count);
        OwnedSlice encoded;
        ASSERT_TRUE(builder->finish(&encoded).ok());
        Slice body = encoded.slice();
        std::unique_ptr<DataPage> decoded;
        if constexpr (BitShuffle) {
            BitShufflePagePreDecoder pre_decoder;
            ASSERT_TRUE(
                    pre_decoder.decode(&decoded, &body, 0, false, DATA_PAGE, "rowid_test").ok());
        }
        Decoder decoder(body, {});
        ASSERT_TRUE(decoder.init().ok());
        ASSERT_TRUE(decoder.seek_to_position_in_page(3).ok());
        check_append([&](const uint32_t* indices, size_t n, uint64_t base, MutableColumnPtr& dst) {
            EXPECT_TRUE(decoder.read_by_rowids(indices, base, &n, dst).ok());
            EXPECT_EQ(3, decoder.current_index());
            return n;
        });
    }
};

TYPED_TEST_SUITE(FixedLengthPageRowidTest, RowidTypes);

TYPED_TEST(FixedLengthPageRowidTest, ColumnUnalignedSource) {
    const auto values = TypeParam::values();
    std::vector<char> storage(values.size() * sizeof(typename TypeParam::Storage) + 1);
    memcpy(storage.data() + 1, values.data(), storage.size() - 1);
    this->check_append(
            [&](const uint32_t* indices, size_t n, uint64_t base, MutableColumnPtr& dst) {
                return storage_to_column_by_indices<TypeParam::field>(
                        reinterpret_cast<const uint8_t*>(storage.data() + 1), values.size(),
                        indices, n, base, *dst);
            });
}

TYPED_TEST(FixedLengthPageRowidTest, PlainPage) {
    this->template check_page<false>();
}

TYPED_TEST(FixedLengthPageRowidTest, BitShufflePage) {
    this->template check_page<true>();
}

TEST(FixedLengthRowidConversionTest, LegacyDateAndDatetime) {
    const uint24_t dates[] = {uint24_t((2024U << 9) | (2U << 5) | 29U),
                              uint24_t((1970U << 9) | (1U << 5) | 1U)};
    const uint64_t datetimes[] = {20240229235959, 19700101000000};
    const uint32_t indices[] = {8, 9};
    auto date = ColumnDate::create();
    auto datetime = ColumnDateTime::create();
    ASSERT_EQ(2, storage_to_column_by_indices<FieldType::OLAP_FIELD_TYPE_DATE>(
                         reinterpret_cast<const uint8_t*>(dates), 2, indices, 2, 8, *date));
    ASSERT_EQ(2, storage_to_column_by_indices<FieldType::OLAP_FIELD_TYPE_DATETIME>(
                         reinterpret_cast<const uint8_t*>(datetimes), 2, indices, 2, 8, *datetime));
    for (size_t i = 0; i < 2; ++i) {
        EXPECT_EQ(static_cast<uint32_t>(dates[i]), date->get_data()[i].to_olap_date());
        EXPECT_EQ(datetimes[i], datetime->get_data()[i].to_olap_datetime());
    }
    EXPECT_EQ(29, date->get_data()[0].day());
    EXPECT_EQ(23, datetime->get_data()[0].hour());
}

TEST(FixedLengthRowidConversionTest, LegacyDecimalSigns) {
    const decimal12_t values[] = {{.integer = -12, .fraction = -345678901},
                                  {.integer = 0, .fraction = -1},
                                  {.integer = 12, .fraction = 345678901},
                                  {.integer = 0, .fraction = 0}};
    const uint32_t indices[] = {0, 1, 2, 3};
    auto column = ColumnDecimal128V2::create(0, 9);
    ASSERT_EQ(4, storage_to_column_by_indices<FieldType::OLAP_FIELD_TYPE_DECIMAL>(
                         reinterpret_cast<const uint8_t*>(values), 4, indices, 4, 0, *column));
    const int128_t expected[] = {-12345678901LL, -1, 12345678901LL, 0};
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_EQ(expected[i], column->get_data()[i].value());
    }
}

TEST(FixedLengthRowidConversionTest, FloatBitsAndLargeIndexBase) {
    const uint64_t bits[] = {0x8000000000000000ULL, 0x7ff0000000000000ULL, 0xfff0000000000000ULL,
                             0x7ff8000000000042ULL};
    const uint32_t base = std::numeric_limits<uint32_t>::max() - 3;
    const uint32_t indices[] = {base, base + 1, base + 2, base + 3};
    auto column = ColumnFloat64::create();
    ASSERT_EQ(4, storage_to_column_by_indices<FieldType::OLAP_FIELD_TYPE_DOUBLE>(
                         reinterpret_cast<const uint8_t*>(bits), 4, indices, 4, base, *column));
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_EQ(bits[i], unaligned_load<uint64_t>(column->get_data_at(i).data));
    }

    // UINT32_MAX -> 0 is not a consecutive rowid within this page.
    const uint32_t wrapped[] = {base + 1, base + 2, base + 3, 0};
    column->clear();
    ASSERT_EQ(3, storage_to_column_by_indices<FieldType::OLAP_FIELD_TYPE_DOUBLE>(
                         reinterpret_cast<const uint8_t*>(bits), 4, wrapped, 4, base, *column));
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(bits[i + 1], unaligned_load<uint64_t>(column->get_data_at(i).data));
    }
}

} // namespace
} // namespace doris::segment_v2
