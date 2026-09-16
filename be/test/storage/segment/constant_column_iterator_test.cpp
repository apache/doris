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

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/field.h"
#include "core/value/bitmap_value.h"
#include "core/value/hll.h"
#include "storage/index/index_iterator.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/comparison_predicate.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/common.h"
#include "storage/segment/segment.h"
#include "storage/segment/variant/binary_column_reader.h"

using namespace doris::segment_v2;

namespace doris {

class ConstantColumnIteratorTest : public testing::Test {};

namespace {
TColumnAccessPath make_data_access_path(std::vector<std::string> path) {
    TColumnAccessPath access_path;
    access_path.__set_type(TAccessPathType::DATA);
    TDataAccessPath data_access_path;
    data_access_path.__set_path(std::move(path));
    access_path.__set_data_access_path(std::move(data_access_path));
    return access_path;
}
} // namespace

TEST_F(ConstantColumnIteratorTest, ConstantColumnReaderExposesConstantValue) {
    const int64_t kValue = 8888;
    std::shared_ptr<ColumnReader> reader = std::make_shared<ConstantColumnReader>(
            Field::create_field<TYPE_BIGINT>(kValue), FieldType::OLAP_FIELD_TYPE_BIGINT);
    EXPECT_TRUE(reader->has_zone_map());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_BIGINT, reader->get_meta_type());

    segment_v2::ZoneMap zone_map;
    auto st = reader->get_segment_zone_map(&zone_map);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(kValue, zone_map.min_value.get<TYPE_BIGINT>());
    EXPECT_EQ(kValue, zone_map.max_value.get<TYPE_BIGINT>());
    EXPECT_TRUE(zone_map.has_not_null);

    TabletColumn column;
    column.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    ColumnIteratorUPtr iter;
    st = reader->new_iterator(&iter, &column);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr dst = ColumnVector<TYPE_BIGINT>::create();
    size_t n = 3;
    bool has_null = true;
    st = iter->next_batch(&n, dst, &has_null);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(has_null);
    auto* col = assert_cast<ColumnInt64*>(dst.get());
    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(kValue, col->get_element(i));
    }
}

TEST_F(ConstantColumnIteratorTest, MatchConditionUsesConstantZoneMap) {
    const int64_t kValue = 100;
    ConstantColumnReader reader(Field::create_field<TYPE_BIGINT>(kValue),
                                FieldType::OLAP_FIELD_TYPE_BIGINT);
    auto make_gt_predicate = [](int64_t value) {
        std::shared_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_BIGINT, PredicateType::GT>(
                        0, "", Field::create_field<TYPE_BIGINT>(value)));
        return SingleColumnBlockPredicate::create_unique(pred);
    };

    bool matched = false;
    AndBlockColumnPredicate keep;
    keep.add_column_predicate(make_gt_predicate(kValue - 1));
    auto st = reader.match_condition(&keep, &matched);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(matched);

    AndBlockColumnPredicate prune;
    prune.add_column_predicate(make_gt_predicate(kValue));
    st = reader.match_condition(&prune, &matched);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_FALSE(matched);
}

// next_batch fills every row with the constant value, advances the ordinal,
// and reports has_null = false for a non-null value.
TEST_F(ConstantColumnIteratorTest, NextBatchFillsConstant) {
    const int64_t kValue = 12345;
    ConstantColumnIterator it(Field::create_field<TYPE_BIGINT>(kValue));

    MutableColumnPtr dst = ColumnVector<TYPE_BIGINT>::create();
    size_t n = 5;
    bool has_null = true;
    ASSERT_TRUE(it.next_batch(&n, dst, &has_null).ok());

    ASSERT_EQ(5, dst->size());
    ASSERT_FALSE(has_null);
    auto* col = assert_cast<ColumnInt64*>(dst.get());
    for (size_t i = 0; i < 5; i++) {
        ASSERT_EQ(kValue, col->get_element(i));
    }
    ASSERT_EQ(5, it.get_current_ordinal());
}

// read_by_rowids fills count rows with the constant value regardless of rowids.
TEST_F(ConstantColumnIteratorTest, ReadByRowidsFillsConstant) {
    const int64_t kValue = 777;
    ConstantColumnIterator it(Field::create_field<TYPE_BIGINT>(kValue));

    MutableColumnPtr dst = ColumnVector<TYPE_BIGINT>::create();
    rowid_t rowids[] = {3, 0, 9, 1};
    size_t count = sizeof(rowids) / sizeof(rowids[0]);
    ASSERT_TRUE(it.read_by_rowids(rowids, count, dst).ok());

    ASSERT_EQ(count, dst->size());
    auto* col = assert_cast<ColumnInt64*>(dst.get());
    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(kValue, col->get_element(i));
    }
}

// seek_to_ordinal only moves the cursor; the constant value is independent of position.
TEST_F(ConstantColumnIteratorTest, SeekThenNextBatch) {
    const int64_t kValue = 42;
    ConstantColumnIterator it(Field::create_field<TYPE_BIGINT>(kValue));

    ASSERT_TRUE(it.seek_to_ordinal(10).ok());
    ASSERT_EQ(10, it.get_current_ordinal());

    MutableColumnPtr dst = ColumnVector<TYPE_BIGINT>::create();
    size_t n = 3;
    bool has_null = false;
    ASSERT_TRUE(it.next_batch(&n, dst, &has_null).ok());
    ASSERT_EQ(13, it.get_current_ordinal());
    auto* col = assert_cast<ColumnInt64*>(dst.get());
    for (size_t i = 0; i < 3; i++) {
        ASSERT_EQ(kValue, col->get_element(i));
    }
}

TEST_F(ConstantColumnIteratorTest, LazyReadRecoversPredicatePlaceholder) {
    constexpr int32_t kValue = 42;
    ConstantColumnIterator iter(Field::create_field<TYPE_INT>(kValue));
    iter.set_read_requirement(ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    iter.set_read_phase(ColumnIterator::ReadPhase::PREDICATE);

    MutableColumnPtr dst = ColumnVector<TYPE_INT>::create();
    size_t n = 2;
    bool has_null = false;
    ASSERT_TRUE(iter.next_batch(&n, dst, &has_null).ok());
    ASSERT_EQ(2, dst->size());
    EXPECT_EQ(0, assert_cast<ColumnInt32*>(dst.get())->get_element(0));

    iter.set_read_phase(ColumnIterator::ReadPhase::LAZY);
    ASSERT_TRUE(iter.next_batch(&n, dst, &has_null).ok());
    ASSERT_EQ(2, dst->size());
    auto* col = assert_cast<ColumnInt32*>(dst.get());
    EXPECT_EQ(kValue, col->get_element(0));
    EXPECT_EQ(kValue, col->get_element(1));
}

// A predicate-column destination (used when the column has a pushed-down predicate,
// e.g. __DORIS_COMMIT_TSO_COL__ <= t) is, for BIGINT, the canonical ColumnInt64 and is
// already covered by NextBatchFillsConstant.

// next_batch_of_zone_map delegates to next_batch (constant column has min == max).
TEST_F(ConstantColumnIteratorTest, NextBatchOfZoneMap) {
    const int64_t kValue = 555;
    ConstantColumnIterator it(Field::create_field<TYPE_BIGINT>(kValue));

    MutableColumnPtr dst = ColumnVector<TYPE_BIGINT>::create();
    size_t n = 2;
    ASSERT_TRUE(it.next_batch_of_zone_map(&n, dst).ok());
    ASSERT_EQ(2, dst->size());
    auto* col = assert_cast<ColumnInt64*>(dst.get());
    ASSERT_EQ(kValue, col->get_element(0));
    ASSERT_EQ(kValue, col->get_element(1));
}

// A null Field reports has_null = true and inserts defaults (null on a nullable column).
TEST_F(ConstantColumnIteratorTest, NullValueInsertsNull) {
    ConstantColumnIterator it {Field()};

    MutableColumnPtr dst =
            ColumnNullable::create(ColumnVector<TYPE_BIGINT>::create(), ColumnUInt8::create());
    size_t n = 3;
    bool has_null = false;
    ASSERT_TRUE(it.next_batch(&n, dst, &has_null).ok());

    ASSERT_EQ(3, dst->size());
    ASSERT_TRUE(has_null);
    auto* nullable = assert_cast<ColumnNullable*>(dst.get());
    for (size_t i = 0; i < 3; i++) {
        ASSERT_TRUE(nullable->is_null_at(i));
    }
}

TEST_F(ConstantColumnIteratorTest, DefaultFieldUsesImplicitNullForNullableColumn) {
    TabletColumn column;
    column.set_name("nullable_int");
    column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    column.set_is_nullable(true);

    Field field;
    ASSERT_TRUE(Segment::get_default_value_field(column, &field).ok());
    ASSERT_TRUE(field.is_null());

    ConstantColumnReader reader(std::move(field), column.type());
    EXPECT_EQ(column.type(), reader.get_meta_type());
}

TEST_F(ConstantColumnIteratorTest, DefaultFieldRejectsMissingNonNullableDefault) {
    TabletColumn column;
    column.set_name("required_int");
    column.set_unique_id(42);
    column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    column.set_is_nullable(false);

    Field field;
    EXPECT_FALSE(Segment::get_default_value_field(column, &field).ok());
}

TEST_F(ConstantColumnIteratorTest, ConstantReaderRejectsMismatchedFieldType) {
    EXPECT_THROW(ConstantColumnReader(Field::create_field<TYPE_INT>(1),
                                      FieldType::OLAP_FIELD_TYPE_BIGINT),
                 Exception);
}

TEST_F(ConstantColumnIteratorTest, DefaultFieldParsesSupportedDefaults) {
    TabletColumn int_column;
    int_column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    int_column.set_default_value("123");

    Field field;
    ASSERT_TRUE(Segment::get_default_value_field(int_column, &field).ok());
    EXPECT_EQ(123, field.get<TYPE_INT>());

    TabletColumn array_column;
    array_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    array_column.set_default_value("[]");
    ASSERT_TRUE(Segment::get_default_value_field(array_column, &field).ok());
    EXPECT_TRUE(field.get<TYPE_ARRAY>().empty());

    TabletColumn map_column;
    map_column.set_type(FieldType::OLAP_FIELD_TYPE_MAP);
    map_column.set_default_value("{}");
    ASSERT_TRUE(Segment::get_default_value_field(map_column, &field).ok());
    const auto& map = field.get<TYPE_MAP>();
    ASSERT_EQ(2, map.size());
    EXPECT_TRUE(map[0].get<TYPE_ARRAY>().empty());
    EXPECT_TRUE(map[1].get<TYPE_ARRAY>().empty());

    map_column.set_default_value("{\"key\":\"value\"}");
    EXPECT_FALSE(Segment::get_default_value_field(map_column, &field).ok());
}

TEST_F(ConstantColumnIteratorTest, AllSupportedScalarDefaultsRoundTripThroughIterator) {
    struct ScalarDefaultCase {
        FieldType type;
        std::string value;
        int precision = 0;
        int scale = 0;
        int length = -1;
    };
    const std::vector<ScalarDefaultCase> cases {
            {FieldType::OLAP_FIELD_TYPE_BOOL, "true"},
            {FieldType::OLAP_FIELD_TYPE_TINYINT, "7"},
            {FieldType::OLAP_FIELD_TYPE_SMALLINT, "32000"},
            {FieldType::OLAP_FIELD_TYPE_INT, "123456789"},
            {FieldType::OLAP_FIELD_TYPE_BIGINT, "9223372036854775807"},
            {FieldType::OLAP_FIELD_TYPE_LARGEINT, "170141183460469231731687303715884105727"},
            {FieldType::OLAP_FIELD_TYPE_FLOAT, "3.125"},
            {FieldType::OLAP_FIELD_TYPE_DOUBLE, "2.718281828"},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL, "123456789.123456789", 27, 9},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL32, "1234567.89", 9, 2},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL64, "12345678901234.5678", 18, 4},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL128I, "12345678901234567890123456789.123456789", 38,
             9},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL256,
             "1234567890123456789012345678901234567890123456789012345678."
             "123456789012345678",
             76, 18},
            {FieldType::OLAP_FIELD_TYPE_DATE, "2025-01-02"},
            {FieldType::OLAP_FIELD_TYPE_DATETIME, "2025-01-02 03:04:05"},
            {FieldType::OLAP_FIELD_TYPE_DATEV2, "2025-01-02"},
            {FieldType::OLAP_FIELD_TYPE_DATETIMEV2, "2025-01-02 03:04:05.123456", 0, 6},
            {FieldType::OLAP_FIELD_TYPE_TIMEV2, "12:34:56.123456", 0, 6},
            {FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS, "2025-01-02 03:04:05.123456789"},
            {FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, "2025-01-02 03:04:05.123456+00:00", 0, 6},
            {FieldType::OLAP_FIELD_TYPE_CHAR, "oldchar", 0, 0, 8},
            {FieldType::OLAP_FIELD_TYPE_VARCHAR, "old-varchar", 0, 0, 32},
            {FieldType::OLAP_FIELD_TYPE_STRING, "old-string"},
            {FieldType::OLAP_FIELD_TYPE_IPV4, "192.168.1.1"},
            {FieldType::OLAP_FIELD_TYPE_IPV6, "2001:db8::1"},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(static_cast<int>(test_case.type));
        TabletColumn column;
        column.set_type(test_case.type);
        column.set_is_nullable(false);
        column.set_precision(test_case.precision);
        column.set_frac(test_case.scale);
        if (test_case.length >= 0) {
            column.set_length(test_case.length);
        }
        column.set_default_value(test_case.value);

        Field field;
        auto st = Segment::get_default_value_field(column, &field);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(field.is_null());

        ConstantColumnReader reader(field, test_case.type);
        ColumnIteratorUPtr iterator;
        st = reader.new_iterator(&iterator, &column, nullptr);
        ASSERT_TRUE(st.ok()) << st;
        auto data_type = column.get_vec_type();
        MutableColumnPtr dst = data_type->create_column();
        size_t rows = 3;
        bool has_null = true;
        st = iterator->next_batch(&rows, dst, &has_null);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(has_null);
        ASSERT_EQ(3, dst->size());
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_EQ(field, (*dst)[row]);
        }
    }
}

TEST_F(ConstantColumnIteratorTest, ConstantReaderHasNoPhysicalIndexIterator) {
    ConstantColumnReader reader(Field::create_field<TYPE_INT>(7), FieldType::OLAP_FIELD_TYPE_INT);
    std::unique_ptr<IndexIterator> index_iterator;
    auto st = reader.new_index_iterator(nullptr, nullptr, "rowset", 0, 10, &index_iterator);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(nullptr, index_iterator);
}

TEST_F(ConstantColumnIteratorTest, LegalObjectIdentityDefaultsRoundTripThroughIterator) {
    std::vector<std::pair<FieldType, Field>> cases;
    cases.emplace_back(FieldType::OLAP_FIELD_TYPE_BITMAP,
                       Field::create_field<TYPE_BITMAP>(BitmapValue {}));
    cases.emplace_back(FieldType::OLAP_FIELD_TYPE_HLL,
                       Field::create_field<TYPE_HLL>(HyperLogLog {}));

    for (const auto& [type, field] : cases) {
        SCOPED_TRACE(static_cast<int>(type));
        TabletColumn column;
        column.set_type(type);
        column.set_is_nullable(false);

        ConstantColumnReader reader(field, type);
        ColumnIteratorUPtr iterator;
        auto st = reader.new_iterator(&iterator, &column, nullptr);
        ASSERT_TRUE(st.ok()) << st;

        MutableColumnPtr dst = column.get_vec_type()->create_column();
        size_t rows = 2;
        bool has_null = true;
        st = iterator->next_batch(&rows, dst, &has_null);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(has_null);
        ASSERT_EQ(2, dst->size());
        EXPECT_EQ(field, (*dst)[0]);
        EXPECT_EQ(field, (*dst)[1]);
    }
}

TEST_F(ConstantColumnIteratorTest, NullableObjectTypesUseTypedNullConstants) {
    for (FieldType type :
         {FieldType::OLAP_FIELD_TYPE_JSONB, FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE,
          FieldType::OLAP_FIELD_TYPE_VARIANT}) {
        SCOPED_TRACE(static_cast<int>(type));
        TabletColumn column;
        column.set_type(type);
        column.set_is_nullable(true);

        Field field;
        auto st = Segment::get_default_value_field(column, &field);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_TRUE(field.is_null());

        ConstantColumnReader reader(field, type);
        ColumnIteratorUPtr iterator;
        st = reader.new_iterator(&iterator, &column, nullptr);
        ASSERT_TRUE(st.ok()) << st;

        MutableColumnPtr dst = column.get_vec_type()->create_column();
        size_t rows = 2;
        bool has_null = false;
        st = iterator->next_batch(&rows, dst, &has_null);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_TRUE(has_null);
        const auto* nullable = check_and_get_column<ColumnNullable>(dst.get());
        ASSERT_NE(nullptr, nullable);
        ASSERT_EQ(2, nullable->size());
        EXPECT_TRUE(nullable->is_null_at(0));
        EXPECT_TRUE(nullable->is_null_at(1));
    }
}

TEST_F(ConstantColumnIteratorTest, ComplexConstantsSupportPrunedDestinations) {
    ConstantColumnIterator map_iterator(Field::create_field<TYPE_MAP>(Map {
            Field::create_field<TYPE_ARRAY>(Array {}), Field::create_field<TYPE_ARRAY>(Array {})}));
    map_iterator.set_column_name("m");
    auto st = map_iterator.set_access_paths(
            {make_data_access_path({"m", ColumnIterator::ACCESS_MAP_VALUES})}, {});
    ASSERT_TRUE(st.ok()) << st;
    map_iterator.remove_pruned_sub_iterators();

    // The destination models a MAP after its value subtree has been projected to a different
    // scalar type. An empty constant map must not depend on the pruned physical child layout.
    MutableColumnPtr map_dst = ColumnMap::create(ColumnString::create(), ColumnInt32::create(),
                                                 ColumnArray::ColumnOffsets::create());
    size_t rows = 2;
    bool has_null = true;
    st = map_iterator.next_batch(&rows, map_dst, &has_null);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(has_null);
    const auto* map = assert_cast<const ColumnMap*>(map_dst.get());
    ASSERT_EQ(2, map->size());
    EXPECT_EQ(0, map->size_at(0));
    EXPECT_EQ(0, map->size_at(1));

    ConstantColumnIterator struct_iterator(Field());
    struct_iterator.set_column_name("s");
    st = struct_iterator.set_access_paths({make_data_access_path({"s", "kept"})}, {});
    ASSERT_TRUE(st.ok()) << st;
    struct_iterator.remove_pruned_sub_iterators();

    // Nullable STRUCT defaults are NULL. Reading into a one-field projected STRUCT must preserve
    // the row count and null map without requiring columns that were pruned from the destination.
    MutableColumnPtr struct_dst = ColumnNullable::create(
            ColumnStruct::create(Columns {ColumnInt32::create()}), ColumnUInt8::create());
    rows = 2;
    has_null = false;
    st = struct_iterator.next_batch(&rows, struct_dst, &has_null);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(has_null);
    const auto* nullable_struct = assert_cast<const ColumnNullable*>(struct_dst.get());
    ASSERT_EQ(2, nullable_struct->size());
    EXPECT_TRUE(nullable_struct->is_null_at(0));
    EXPECT_TRUE(nullable_struct->is_null_at(1));
}

TEST_F(ConstantColumnIteratorTest, DummyBinaryReaderFillsEmptyNonNullableMap) {
    DummyBinaryColumnReader reader;
    ColumnIteratorUPtr iter;
    ASSERT_TRUE(reader.new_binary_column_iterator(&iter).ok());

    MutableColumnPtr dst = ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                             ColumnArray::ColumnOffsets::create());
    size_t n = 3;
    bool has_null = true;
    ASSERT_TRUE(iter->next_batch(&n, dst, &has_null).ok());

    ASSERT_FALSE(has_null);
    ASSERT_EQ(n, dst->size());
    const auto* map = assert_cast<const ColumnMap*>(dst.get());
    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(0, map->size_at(i));
    }
}

} // namespace doris
