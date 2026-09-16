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

#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/field.h"
#include "core/value/bitmap_value.h"
#include "core/value/hll.h"
#include "core/value/jsonb_value.h"
#include "storage/index/index_iterator.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/comparison_predicate.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/common.h"
#include "storage/segment/segment.h"
#include "storage/segment/variant/binary_column_reader.h"
#include "util/json/path_in_data.h"

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

struct DefaultValueCase {
    FieldType type;
    std::string value;
    int precision = 0;
    int scale = 0;
    int length = -1;
};

TabletColumn make_default_column(const DefaultValueCase& test_case) {
    TabletColumn column;
    column.set_type(test_case.type);
    column.set_is_nullable(false);
    column.set_precision(test_case.precision);
    column.set_frac(test_case.scale);
    if (test_case.length >= 0) {
        column.set_length(test_case.length);
    }
    column.set_default_value(test_case.value);
    return column;
}

void expect_same_field(const Field& expected, const Field& actual) {
    if (expected.get_type() == PrimitiveType::TYPE_JSONB &&
        actual.get_type() == PrimitiveType::TYPE_STRING) {
        const auto& jsonb = expected.get<TYPE_JSONB>();
        EXPECT_EQ(std::string_view(jsonb.get_value(), jsonb.get_size()),
                  std::string_view(actual.get<TYPE_STRING>()));
        return;
    }
    const bool both_string_types =
            is_string_type(expected.get_type()) && is_string_type(actual.get_type());
    ASSERT_TRUE(expected.get_type() == actual.get_type() || both_string_types)
            << "expected=" << expected.get_type_name() << ", actual=" << actual.get_type_name();
    if (expected.is_nan()) {
        EXPECT_TRUE(actual.is_nan());
        return;
    }
    if (expected.get_type() == PrimitiveType::TYPE_FLOAT) {
        EXPECT_EQ(expected.get<TYPE_FLOAT>(), actual.get<TYPE_FLOAT>());
        if (expected.get<TYPE_FLOAT>() == 0.0F) {
            EXPECT_EQ(std::signbit(expected.get<TYPE_FLOAT>()),
                      std::signbit(actual.get<TYPE_FLOAT>()));
        }
        return;
    }
    if (expected.get_type() == PrimitiveType::TYPE_DOUBLE) {
        EXPECT_EQ(expected.get<TYPE_DOUBLE>(), actual.get<TYPE_DOUBLE>());
        if (expected.get<TYPE_DOUBLE>() == 0.0) {
            EXPECT_EQ(std::signbit(expected.get<TYPE_DOUBLE>()),
                      std::signbit(actual.get<TYPE_DOUBLE>()));
        }
        return;
    }
    EXPECT_EQ(expected, actual);
}

void assert_constant_pipeline(const TabletColumn& column, const Field& field) {
    ConstantColumnReader reader(field, column.type());
    EXPECT_TRUE(reader.has_zone_map());
    EXPECT_EQ(column.type(), reader.get_meta_type());

    segment_v2::ZoneMap zone_map;
    auto st = reader.get_segment_zone_map(&zone_map);
    ASSERT_TRUE(st.ok()) << st;
    expect_same_field(field, zone_map.min_value);
    expect_same_field(field, zone_map.max_value);
    EXPECT_EQ(field.is_null(), zone_map.has_null);
    EXPECT_EQ(!field.is_null(), zone_map.has_not_null);

    ColumnIteratorUPtr iterator;
    st = reader.new_iterator(&iterator, &column, nullptr);
    ASSERT_TRUE(st.ok()) << st;
    auto data_type = column.get_vec_type();
    ASSERT_NE(nullptr, data_type);
    MutableColumnPtr dst = data_type->create_column();
    size_t rows = 3;
    bool has_null = !field.is_null();
    st = iterator->next_batch(&rows, dst, &has_null);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(field.is_null(), has_null);
    ASSERT_EQ(rows, dst->size());
    for (size_t row = 0; row < rows; ++row) {
        expect_same_field(field, (*dst)[row]);
    }

    ColumnIteratorUPtr zone_map_iterator;
    st = reader.new_iterator(&zone_map_iterator, &column, nullptr);
    ASSERT_TRUE(st.ok()) << st;
    MutableColumnPtr zone_map_dst = data_type->create_column();
    rows = 2;
    st = zone_map_iterator->next_batch_of_zone_map(&rows, zone_map_dst);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows, zone_map_dst->size());
    for (size_t row = 0; row < rows; ++row) {
        expect_same_field(field, (*zone_map_dst)[row]);
    }
}
} // namespace

TEST_F(ConstantColumnIteratorTest, ConstantColumnReaderExposesConstantValue) {
    // Case: a reader synthesized for a missing BIGINT column exposes the same value through its
    // segment zone map and its ordinary column iterator.
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
    // Case: predicates can accept or prune an entire old segment using only the added column's
    // constant zone map.
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
    // Case: a constant output column participates in the predicate phase of lazy materialization.
    // The lazy phase must replace its placeholder with the actual constant value.
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
    // Case: adding a nullable column without an explicit default synthesizes a typed NULL.
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
    // Case: adding a non-nullable column without a default cannot synthesize historical values.
    TabletColumn column;
    column.set_name("required_int");
    column.set_unique_id(42);
    column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    column.set_is_nullable(false);

    Field field;
    EXPECT_FALSE(Segment::get_default_value_field(column, &field).ok());
}

TEST_F(ConstantColumnIteratorTest, ConstantReaderRejectsMismatchedFieldType) {
    // Case: the declared storage type and the typed constant must agree when a reader is created.
    EXPECT_THROW(ConstantColumnReader(Field::create_field<TYPE_INT>(1),
                                      FieldType::OLAP_FIELD_TYPE_BIGINT),
                 Exception);
}

TEST_F(ConstantColumnIteratorTest, DefaultFieldParsesScalarDefault) {
    // Case: a scalar schema default is converted into the matching typed Field.
    TabletColumn int_column;
    int_column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    int_column.set_default_value("123");

    Field field;
    ASSERT_TRUE(Segment::get_default_value_field(int_column, &field).ok());
    EXPECT_EQ(123, field.get<TYPE_INT>());
}

TEST_F(ConstantColumnIteratorTest, DefaultFieldParsesEmptyArrayDefault) {
    // Case: [] is the supported non-null default for an ARRAY added to historical segments.
    TabletColumn array_column;
    array_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    array_column.set_default_value("[]");
    Field field;
    ASSERT_TRUE(Segment::get_default_value_field(array_column, &field).ok());
    EXPECT_TRUE(field.get<TYPE_ARRAY>().empty());
}

TEST_F(ConstantColumnIteratorTest, DefaultFieldParsesEmptyMapDefault) {
    // Case: {} is represented as the two empty key/value arrays expected by a MAP Field.
    TabletColumn map_column;
    map_column.set_type(FieldType::OLAP_FIELD_TYPE_MAP);
    map_column.set_default_value("{}");
    Field field;
    ASSERT_TRUE(Segment::get_default_value_field(map_column, &field).ok());
    const auto& map = field.get<TYPE_MAP>();
    ASSERT_EQ(2, map.size());
    EXPECT_TRUE(map[0].get<TYPE_ARRAY>().empty());
    EXPECT_TRUE(map[1].get<TYPE_ARRAY>().empty());
}

TEST_F(ConstantColumnIteratorTest, ScalarDefaultsCoverRangesReaderIteratorAndZoneMap) {
    // Case: an old segment is read after scalar columns with defaults are added. Every boundary
    // value must be parsed once and then exposed consistently by the reader, iterator, and zone map.
    const std::vector<DefaultValueCase> cases {
            {FieldType::OLAP_FIELD_TYPE_BOOL, "false"},
            {FieldType::OLAP_FIELD_TYPE_BOOL, "true"},
            {FieldType::OLAP_FIELD_TYPE_TINYINT, "-128"},
            {FieldType::OLAP_FIELD_TYPE_TINYINT, "127"},
            {FieldType::OLAP_FIELD_TYPE_SMALLINT, "-32768"},
            {FieldType::OLAP_FIELD_TYPE_SMALLINT, "32767"},
            {FieldType::OLAP_FIELD_TYPE_INT, "-2147483648"},
            {FieldType::OLAP_FIELD_TYPE_INT, "2147483647"},
            {FieldType::OLAP_FIELD_TYPE_BIGINT, "-9223372036854775808"},
            {FieldType::OLAP_FIELD_TYPE_BIGINT, "9223372036854775807"},
            {FieldType::OLAP_FIELD_TYPE_LARGEINT, "-170141183460469231731687303715884105728"},
            {FieldType::OLAP_FIELD_TYPE_LARGEINT, "170141183460469231731687303715884105727"},
            {FieldType::OLAP_FIELD_TYPE_CHAR, "", 0, 0, 8},
            {FieldType::OLAP_FIELD_TYPE_CHAR, "oldchar", 0, 0, 8},
            {FieldType::OLAP_FIELD_TYPE_VARCHAR, "old-varchar", 0, 0, 32},
            {FieldType::OLAP_FIELD_TYPE_STRING, "Doris-默认值"},
            {FieldType::OLAP_FIELD_TYPE_IPV4, "0.0.0.0"},
            {FieldType::OLAP_FIELD_TYPE_IPV4, "255.255.255.255"},
            {FieldType::OLAP_FIELD_TYPE_IPV6, "::"},
            {FieldType::OLAP_FIELD_TYPE_IPV6, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(fmt::format("type={}, value={}", static_cast<int>(test_case.type),
                                 test_case.value));
        TabletColumn column = make_default_column(test_case);
        Field field;
        auto st = Segment::get_default_value_field(column, &field);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(field.is_null());
        assert_constant_pipeline(column, field);
    }
}

TEST_F(ConstantColumnIteratorTest, DecimalDefaultsCoverScalesReaderIteratorAndZoneMap) {
    // Case: decimal columns with different storage widths and scales are added to an old segment.
    // The synthesized value and its zone map must retain the exact unscaled integer and scale.
    const std::vector<DefaultValueCase> cases {
            {FieldType::OLAP_FIELD_TYPE_DECIMAL, "0", 27, 9},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL, "-0.000000001", 27, 9},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL, "999999999999999999.999999999", 27, 9},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL32, "999999999", 9, 0},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL32, "-9999999.99", 9, 2},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL32, "0.999999999", 9, 9},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL64, "999999999999999999", 18, 0},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL64, "-999999999999.999999", 18, 6},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL64, "0.999999999999999999", 18, 18},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL128I, "99999999999999999999999999999999999999", 38,
             0},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL128I, "-99999999999999999999999999999.999999999", 38,
             9},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL128I, "0.99999999999999999999999999999999999999", 38,
             38},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL256,
             "9999999999999999999999999999999999999999999999999999999999999999999999999999", 76, 0},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL256,
             "-9999999999999999999999999999999999999999999999999999999999."
             "999999999999999999",
             76, 18},
            {FieldType::OLAP_FIELD_TYPE_DECIMAL256,
             "0.9999999999999999999999999999999999999999999999999999999999999999999999999999", 76,
             76},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(fmt::format("type={}, precision={}, scale={}, value={}",
                                 static_cast<int>(test_case.type), test_case.precision,
                                 test_case.scale, test_case.value));
        TabletColumn column = make_default_column(test_case);
        Field field;
        auto st = Segment::get_default_value_field(column, &field);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(field.is_null());
        if (test_case.type != FieldType::OLAP_FIELD_TYPE_DECIMAL) {
            EXPECT_EQ(test_case.value, field.to_debug_string(test_case.scale));
        }
        assert_constant_pipeline(column, field);
    }
}

TEST_F(ConstantColumnIteratorTest, TemporalDefaultsCoverBoundariesReaderIteratorAndZoneMap) {
    // Case: temporal columns are added to an old segment. Cover calendar boundaries, fractional
    // precision, nanosecond timestamps, and timezone normalization through the full read pipeline.
    struct TemporalDefaultCase {
        DefaultValueCase input;
        std::string expected_debug;
        double expected_time_microseconds = 0;
    };
    const std::vector<TemporalDefaultCase> cases {
            {{FieldType::OLAP_FIELD_TYPE_DATE, "0001-01-01"}, "0001-01-01"},
            {{FieldType::OLAP_FIELD_TYPE_DATE, "1970-01-01"}, "1970-01-01"},
            {{FieldType::OLAP_FIELD_TYPE_DATE, "2000-02-29"}, "2000-02-29"},
            {{FieldType::OLAP_FIELD_TYPE_DATE, "9999-12-31"}, "9999-12-31"},
            {{FieldType::OLAP_FIELD_TYPE_DATEV2, "0001-01-01"}, "0001-01-01"},
            {{FieldType::OLAP_FIELD_TYPE_DATEV2, "1970-01-01"}, "1970-01-01"},
            {{FieldType::OLAP_FIELD_TYPE_DATEV2, "2000-02-29"}, "2000-02-29"},
            {{FieldType::OLAP_FIELD_TYPE_DATEV2, "9999-12-31"}, "9999-12-31"},
            {{FieldType::OLAP_FIELD_TYPE_DATETIME, "0001-01-01 00:00:00"}, "0001-01-01 00:00:00"},
            {{FieldType::OLAP_FIELD_TYPE_DATETIME, "1970-01-01 00:00:00"}, "1970-01-01 00:00:00"},
            {{FieldType::OLAP_FIELD_TYPE_DATETIME, "2000-02-29 23:59:59"}, "2000-02-29 23:59:59"},
            {{FieldType::OLAP_FIELD_TYPE_DATETIME, "9999-12-31 23:59:59"}, "9999-12-31 23:59:59"},
            {{FieldType::OLAP_FIELD_TYPE_DATETIMEV2, "0001-01-01 00:00:00.000000", 0, 0},
             "0001-01-01 00:00:00"},
            {{FieldType::OLAP_FIELD_TYPE_DATETIMEV2, "2000-02-29 23:59:59.9", 0, 1},
             "2000-02-29 23:59:59.9"},
            {{FieldType::OLAP_FIELD_TYPE_DATETIMEV2, "2038-01-19 03:14:07.123", 0, 3},
             "2038-01-19 03:14:07.123"},
            {{FieldType::OLAP_FIELD_TYPE_DATETIMEV2, "9999-12-31 23:59:59.999999", 0, 6},
             "9999-12-31 23:59:59.999999"},
            {{FieldType::OLAP_FIELD_TYPE_TIMEV2, "-838:59:59.999999", 0, 6}, "", -3020399999999.0},
            {{FieldType::OLAP_FIELD_TYPE_TIMEV2, "00:00:00", 0, 0}, "", 0.0},
            {{FieldType::OLAP_FIELD_TYPE_TIMEV2, "12:34:56.123", 0, 3}, "", 45296123000.0},
            {{FieldType::OLAP_FIELD_TYPE_TIMEV2, "838:59:59.999999", 0, 6}, "", 3020399999999.0},
            {{FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS, "1677-09-21 00:12:43.145224192"},
             "1677-09-21 00:12:43.145224192"},
            {{FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS, "1969-12-31 23:59:59.999999999"},
             "1969-12-31 23:59:59.999999999"},
            {{FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS, "1970-01-01 00:00:00.000000000"},
             "1970-01-01 00:00:00.000000000"},
            {{FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS, "2000-02-29 12:34:56.123456789"},
             "2000-02-29 12:34:56.123456789"},
            {{FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS, "2262-04-11 23:47:16.854775807"},
             "2262-04-11 23:47:16.854775807"},
            {{FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, "1970-01-01 00:00:00 +00:00", 0, 0},
             "1970-01-01 00:00:00+00:00"},
            {{FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, "2020-01-01 00:00:00 +14:00", 0, 0},
             "2019-12-31 10:00:00+00:00"},
            {{FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, "2020-01-01 00:00:00 -12:00", 0, 0},
             "2020-01-01 12:00:00+00:00"},
            {{FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, "2024-02-29 12:34:56.123 +08:00", 0, 3},
             "2024-02-29 04:34:56.123+00:00"},
            {{FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, "2038-01-19 03:14:07.123456 -05:30", 0, 6},
             "2038-01-19 08:44:07.123456+00:00"},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(fmt::format("type={}, scale={}, value={}",
                                 static_cast<int>(test_case.input.type), test_case.input.scale,
                                 test_case.input.value));
        TabletColumn column = make_default_column(test_case.input);
        Field field;
        auto st = Segment::get_default_value_field(column, &field);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(field.is_null());
        if (!test_case.expected_debug.empty()) {
            EXPECT_EQ(test_case.expected_debug, field.to_debug_string(test_case.input.scale));
        } else {
            ASSERT_EQ(FieldType::OLAP_FIELD_TYPE_TIMEV2, test_case.input.type);
            EXPECT_EQ(test_case.expected_time_microseconds, field.get<TYPE_TIMEV2>());
        }
        assert_constant_pipeline(column, field);
    }
}

TEST_F(ConstantColumnIteratorTest, FloatingDefaultsCoverFiniteBoundaries) {
    // Case: finite FLOAT/DOUBLE defaults, including subnormal values and signed zero, are readable
    // from old segments through the reader, iterator, and zone-map paths.
    const std::vector<DefaultValueCase> finite_cases {
            {FieldType::OLAP_FIELD_TYPE_FLOAT, "-3.4028234663852886e+38"},
            {FieldType::OLAP_FIELD_TYPE_FLOAT, "-1.401298464324817e-45"},
            {FieldType::OLAP_FIELD_TYPE_FLOAT, "-0"},
            {FieldType::OLAP_FIELD_TYPE_FLOAT, "0"},
            {FieldType::OLAP_FIELD_TYPE_FLOAT, "1.1754943508222875e-38"},
            {FieldType::OLAP_FIELD_TYPE_FLOAT, "1.401298464324817e-45"},
            {FieldType::OLAP_FIELD_TYPE_FLOAT, "3.4028234663852886e+38"},
            {FieldType::OLAP_FIELD_TYPE_DOUBLE, "-1.7976931348623157e+308"},
            {FieldType::OLAP_FIELD_TYPE_DOUBLE, "-4.9406564584124654e-324"},
            {FieldType::OLAP_FIELD_TYPE_DOUBLE, "-0"},
            {FieldType::OLAP_FIELD_TYPE_DOUBLE, "0"},
            {FieldType::OLAP_FIELD_TYPE_DOUBLE, "2.2250738585072014e-308"},
            {FieldType::OLAP_FIELD_TYPE_DOUBLE, "4.9406564584124654e-324"},
            {FieldType::OLAP_FIELD_TYPE_DOUBLE, "1.7976931348623157e+308"},
    };

    for (const auto& test_case : finite_cases) {
        SCOPED_TRACE(fmt::format("type={}, value={}", static_cast<int>(test_case.type),
                                 test_case.value));
        TabletColumn column = make_default_column(test_case);
        Field field;
        auto st = Segment::get_default_value_field(column, &field);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(field.is_nan());
        if (test_case.type == FieldType::OLAP_FIELD_TYPE_FLOAT) {
            const float actual = field.get<TYPE_FLOAT>();
            if (test_case.value == "-3.4028234663852886e+38") {
                EXPECT_EQ(-std::numeric_limits<float>::max(), actual);
            } else if (test_case.value == "-1.401298464324817e-45") {
                EXPECT_EQ(-std::numeric_limits<float>::denorm_min(), actual);
            } else if (test_case.value == "-0") {
                EXPECT_EQ(0.0F, actual);
                EXPECT_TRUE(std::signbit(actual));
            } else if (test_case.value == "0") {
                EXPECT_EQ(0.0F, actual);
                EXPECT_FALSE(std::signbit(actual));
            } else if (test_case.value == "1.1754943508222875e-38") {
                EXPECT_EQ(std::numeric_limits<float>::min(), actual);
            } else if (test_case.value == "1.401298464324817e-45") {
                EXPECT_EQ(std::numeric_limits<float>::denorm_min(), actual);
            } else {
                EXPECT_EQ(std::numeric_limits<float>::max(), actual);
            }
        } else {
            const double actual = field.get<TYPE_DOUBLE>();
            if (test_case.value == "-1.7976931348623157e+308") {
                EXPECT_EQ(-std::numeric_limits<double>::max(), actual);
            } else if (test_case.value == "-4.9406564584124654e-324") {
                EXPECT_EQ(-std::numeric_limits<double>::denorm_min(), actual);
            } else if (test_case.value == "-0") {
                EXPECT_EQ(0.0, actual);
                EXPECT_TRUE(std::signbit(actual));
            } else if (test_case.value == "0") {
                EXPECT_EQ(0.0, actual);
                EXPECT_FALSE(std::signbit(actual));
            } else if (test_case.value == "2.2250738585072014e-308") {
                EXPECT_EQ(std::numeric_limits<double>::min(), actual);
            } else if (test_case.value == "4.9406564584124654e-324") {
                EXPECT_EQ(std::numeric_limits<double>::denorm_min(), actual);
            } else {
                EXPECT_EQ(std::numeric_limits<double>::max(), actual);
            }
        }
        assert_constant_pipeline(column, field);
    }
}

TEST_F(ConstantColumnIteratorTest, FloatingTextDefaultsRejectNaNAndInfinity) {
    // Case: NaN and infinity spellings are not legal FLOAT/DOUBLE schema defaults and must be
    // rejected before a constant reader is constructed for historical rows.
    for (FieldType type : {FieldType::OLAP_FIELD_TYPE_FLOAT, FieldType::OLAP_FIELD_TYPE_DOUBLE}) {
        for (const std::string& value :
             {"NaN", "nan", "Inf", "+Inf", "-Inf", "Infinity", "-Infinity"}) {
            SCOPED_TRACE(fmt::format("type={}, value={}", static_cast<int>(type), value));
            TabletColumn column = make_default_column({type, value});
            Field field;
            EXPECT_TRUE(Segment::get_default_value_field(column, &field)
                                .is<ErrorCode::INVALID_ARGUMENT>());
        }
    }
}

TEST_F(ConstantColumnIteratorTest, FloatingSpecialFieldsRoundTripReaderIteratorAndZoneMap) {
    // Case: NaN and infinities can still arrive as typed runtime constants even though they cannot
    // be declared as text defaults. The constant reader and zone-map iterator must preserve them.
    struct FloatingCase {
        FieldType type;
        Field field;
    };
    const std::vector<FloatingCase> cases {
            {FieldType::OLAP_FIELD_TYPE_FLOAT,
             Field::create_field<TYPE_FLOAT>(std::numeric_limits<float>::quiet_NaN())},
            {FieldType::OLAP_FIELD_TYPE_FLOAT,
             Field::create_field<TYPE_FLOAT>(std::numeric_limits<float>::infinity())},
            {FieldType::OLAP_FIELD_TYPE_FLOAT,
             Field::create_field<TYPE_FLOAT>(-std::numeric_limits<float>::infinity())},
            {FieldType::OLAP_FIELD_TYPE_DOUBLE,
             Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN())},
            {FieldType::OLAP_FIELD_TYPE_DOUBLE,
             Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::infinity())},
            {FieldType::OLAP_FIELD_TYPE_DOUBLE,
             Field::create_field<TYPE_DOUBLE>(-std::numeric_limits<double>::infinity())},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(static_cast<int>(test_case.type));
        TabletColumn column;
        column.set_type(test_case.type);
        column.set_is_nullable(false);
        assert_constant_pipeline(column, test_case.field);
    }
}

TEST_F(ConstantColumnIteratorTest, ConstantReaderHasNoPhysicalIndexIterator) {
    // Case: an ALTER-added constant column has no physical index stream. Index setup must return
    // no iterator while leaving the normal constant-column read path valid.
    ConstantColumnReader reader(Field::create_field<TYPE_INT>(7), FieldType::OLAP_FIELD_TYPE_INT);
    std::unique_ptr<IndexIterator> index_iterator;
    auto st = reader.new_index_iterator(nullptr, nullptr, "rowset", 0, 10, &index_iterator);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(nullptr, index_iterator);
}

TEST_F(ConstantColumnIteratorTest, ObjectDefaultsRoundTripReaderIteratorAndZoneMap) {
    // Case: serialized object defaults are synthesized for old segments and remain readable by
    // both normal and zone-map paths.
    BitmapValue bitmap({1, 2, 1024});
    std::string bitmap_default(bitmap.getSizeInBytes(), '\0');
    bitmap.write_to(bitmap_default.data());

    HyperLogLog hll(42);
    std::string hll_default(hll.max_serialized_size(), '\0');
    hll_default.resize(hll.serialize(reinterpret_cast<uint8_t*>(hll_default.data())));

    QuantileState quantile;
    quantile.add_value(1.25);
    quantile.add_value(9.75);
    std::string quantile_default(quantile.get_serialized_size(), '\0');
    quantile_default.resize(
            quantile.serialize(reinterpret_cast<uint8_t*>(quantile_default.data())));

    const std::vector<DefaultValueCase> cases {
            {FieldType::OLAP_FIELD_TYPE_BITMAP, std::move(bitmap_default)},
            {FieldType::OLAP_FIELD_TYPE_HLL, std::move(hll_default)},
            {FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, std::move(quantile_default)},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(static_cast<int>(test_case.type));
        TabletColumn column = make_default_column(test_case);
        Field field;
        auto st = Segment::get_default_value_field(column, &field);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(field.is_null());
        assert_constant_pipeline(column, field);
    }
}

TEST_F(ConstantColumnIteratorTest, JsonbRuntimeConstantRoundTripsReaderIteratorAndZoneMap) {
    // Case: a JSONB value supplied as a typed runtime constant is materialized consistently by
    // the reader, iterator, and zone-map paths.
    TabletColumn column;
    column.set_type(FieldType::OLAP_FIELD_TYPE_JSONB);
    column.set_is_nullable(false);
    JsonBinaryValue value;
    ASSERT_TRUE(value.from_json_string(R"({"key":[1,true,"value"]})").ok());
    Field field = Field::create_field<TYPE_JSONB>(
            JsonbField(value.value(), static_cast<size_t>(value.size())));
    assert_constant_pipeline(column, field);
}

TEST_F(ConstantColumnIteratorTest, NullableObjectTypesUseTypedNullConstants) {
    // Case: nullable object and VARIANT columns added without an explicit default produce a typed
    // NULL that can pass through both the normal iterator and zone-map iterator.
    for (FieldType type :
         {FieldType::OLAP_FIELD_TYPE_BITMAP, FieldType::OLAP_FIELD_TYPE_HLL,
          FieldType::OLAP_FIELD_TYPE_JSONB, FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE,
          FieldType::OLAP_FIELD_TYPE_VARIANT}) {
        SCOPED_TRACE(static_cast<int>(type));
        TabletColumn column;
        column.set_type(type);
        column.set_is_nullable(true);

        Field field;
        auto st = Segment::get_default_value_field(column, &field);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_TRUE(field.is_null());
        assert_constant_pipeline(column, field);
    }

    TabletColumn variant_v2_column;
    variant_v2_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    variant_v2_column.set_variant_is_v2(true);
    variant_v2_column.set_is_nullable(true);
    Field variant_v2_field;
    auto st = Segment::get_default_value_field(variant_v2_column, &variant_v2_field);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(variant_v2_field.is_null());
    assert_constant_pipeline(variant_v2_column, variant_v2_field);
}

TEST_F(ConstantColumnIteratorTest, NullableAggStateUsesNullReaderIteratorAndZoneMap) {
    // Case: AGG_STATE has no generic vectorized type factory in TabletColumn. Verify its nullable
    // default with the concrete fixed-length destination used by the aggregate-state reader.
    TabletColumn column;
    column.set_type(FieldType::OLAP_FIELD_TYPE_AGG_STATE);
    column.set_is_nullable(true);

    Field field;
    auto st = Segment::get_default_value_field(column, &field);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(field.is_null());

    ConstantColumnReader reader(field, column.type());
    segment_v2::ZoneMap zone_map;
    st = reader.get_segment_zone_map(&zone_map);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(zone_map.min_value.is_null());
    EXPECT_TRUE(zone_map.max_value.is_null());
    EXPECT_TRUE(zone_map.has_null);
    EXPECT_FALSE(zone_map.has_not_null);

    ColumnIteratorUPtr iterator;
    st = reader.new_iterator(&iterator, &column, nullptr);
    ASSERT_TRUE(st.ok()) << st;
    MutableColumnPtr dst = ColumnNullable::create(ColumnFixedLengthObject::create(sizeof(int64_t)),
                                                  ColumnUInt8::create());
    size_t rows = 3;
    bool has_null = false;
    st = iterator->next_batch(&rows, dst, &has_null);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(has_null);
    const auto* nullable = assert_cast<const ColumnNullable*>(dst.get());
    ASSERT_EQ(rows, nullable->size());
    for (size_t row = 0; row < rows; ++row) {
        EXPECT_TRUE(nullable->is_null_at(row));
    }

    ColumnIteratorUPtr zone_map_iterator;
    st = reader.new_iterator(&zone_map_iterator, &column, nullptr);
    ASSERT_TRUE(st.ok()) << st;
    MutableColumnPtr zone_map_dst = ColumnNullable::create(
            ColumnFixedLengthObject::create(sizeof(int64_t)), ColumnUInt8::create());
    rows = 2;
    st = zone_map_iterator->next_batch_of_zone_map(&rows, zone_map_dst);
    ASSERT_TRUE(st.ok()) << st;
    const auto* nullable_zone_map = assert_cast<const ColumnNullable*>(zone_map_dst.get());
    ASSERT_EQ(rows, nullable_zone_map->size());
    EXPECT_TRUE(nullable_zone_map->is_null_at(0));
    EXPECT_TRUE(nullable_zone_map->is_null_at(1));
}

TEST_F(ConstantColumnIteratorTest, ArrayDefaultAndRuntimeConstantMaterialize) {
    // Case: an ARRAY column added with [] reads as an empty constant for old rows, while a typed
    // non-empty runtime constant repeats all nested elements for every requested row.
    TabletColumn int_child;
    int_child.set_name("number");
    int_child.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    int_child.set_is_nullable(false);

    TabletColumn array_column;
    array_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    array_column.set_is_nullable(false);
    array_column.set_default_value("[]");
    array_column.add_sub_column(int_child);
    Field array_default;
    auto st = Segment::get_default_value_field(array_column, &array_default);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(array_default.get<TYPE_ARRAY>().empty());
    assert_constant_pipeline(array_column, array_default);

    Field nonempty_array = Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(-2)});
    assert_constant_pipeline(array_column, nonempty_array);
    ConstantColumnIterator array_iterator(nonempty_array);
    MutableColumnPtr array_dst = array_column.get_vec_type()->create_column();
    size_t rows = 2;
    bool has_null = true;
    st = array_iterator.next_batch(&rows, array_dst, &has_null);
    ASSERT_TRUE(st.ok()) << st;
    const auto* materialized_array = assert_cast<const ColumnArray*>(array_dst.get());
    EXPECT_EQ(2, materialized_array->size_at(0));
    EXPECT_EQ(2, materialized_array->size_at(1));
    const auto& array_elements = assert_cast<const ColumnInt32&>(materialized_array->get_data());
    EXPECT_EQ(1, array_elements.get_element(0));
    EXPECT_EQ(-2, array_elements.get_element(1));
    EXPECT_EQ(1, array_elements.get_element(2));
    EXPECT_EQ(-2, array_elements.get_element(3));
}

TEST_F(ConstantColumnIteratorTest, MapDefaultAndRuntimeConstantMaterialize) {
    // Case: a MAP column added with {} reads as an empty constant for old rows, while a typed
    // non-empty runtime constant repeats matching keys and values for every requested row.
    TabletColumn key_child;
    key_child.set_name("key");
    key_child.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    key_child.set_is_nullable(false);

    TabletColumn value_child;
    value_child.set_name("value");
    value_child.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    value_child.set_is_nullable(false);

    TabletColumn map_column;
    map_column.set_type(FieldType::OLAP_FIELD_TYPE_MAP);
    map_column.set_is_nullable(false);
    map_column.set_default_value("{}");
    map_column.add_sub_column(key_child);
    map_column.add_sub_column(value_child);
    Field map_default;
    auto st = Segment::get_default_value_field(map_column, &map_default);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(2, map_default.get<TYPE_MAP>().size());
    ASSERT_TRUE(map_default.get<TYPE_MAP>()[0].get<TYPE_ARRAY>().empty());
    ASSERT_TRUE(map_default.get<TYPE_MAP>()[1].get<TYPE_ARRAY>().empty());
    assert_constant_pipeline(map_column, map_default);

    Field nonempty_map = Field::create_field<TYPE_MAP>(
            Map {Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_STRING>("k")}),
                 Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_INT>(7)})});
    assert_constant_pipeline(map_column, nonempty_map);
    ConstantColumnIterator map_iterator(nonempty_map);
    MutableColumnPtr map_dst = map_column.get_vec_type()->create_column();
    size_t rows = 2;
    bool has_null = true;
    st = map_iterator.next_batch(&rows, map_dst, &has_null);
    ASSERT_TRUE(st.ok()) << st;
    const auto* materialized_map = assert_cast<const ColumnMap*>(map_dst.get());
    EXPECT_EQ(1, materialized_map->size_at(0));
    EXPECT_EQ(1, materialized_map->size_at(1));
    EXPECT_EQ("k", materialized_map->get_keys().get_data_at(0).to_string());
    EXPECT_EQ("k", materialized_map->get_keys().get_data_at(1).to_string());
    EXPECT_EQ(7, assert_cast<const ColumnInt32&>(materialized_map->get_values()).get_element(0));
    EXPECT_EQ(7, assert_cast<const ColumnInt32&>(materialized_map->get_values()).get_element(1));
}

TEST_F(ConstantColumnIteratorTest, StructNullDefaultAndRuntimeConstantMaterialize) {
    // Case: a nullable STRUCT added with NULL keeps old rows null, while a typed non-null runtime
    // constant repeats every field into the nested destination columns.
    TabletColumn int_child;
    int_child.set_name("number");
    int_child.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    int_child.set_is_nullable(false);

    TabletColumn string_child;
    string_child.set_name("text");
    string_child.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    string_child.set_is_nullable(false);

    TabletColumn struct_column;
    struct_column.set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    struct_column.set_is_nullable(true);
    struct_column.set_default_value("NULL");
    struct_column.add_sub_column(int_child);
    struct_column.add_sub_column(string_child);
    Field struct_default;
    auto st = Segment::get_default_value_field(struct_column, &struct_default);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(struct_default.is_null());
    assert_constant_pipeline(struct_column, struct_default);

    struct_column.set_is_nullable(false);
    Field nonnull_struct = Field::create_field<TYPE_STRUCT>(
            Struct {Field::create_field<TYPE_INT>(99), Field::create_field<TYPE_STRING>("doris")});
    assert_constant_pipeline(struct_column, nonnull_struct);
    ConstantColumnIterator struct_iterator(nonnull_struct);
    MutableColumnPtr struct_dst = struct_column.get_vec_type()->create_column();
    size_t rows = 2;
    bool has_null = true;
    st = struct_iterator.next_batch(&rows, struct_dst, &has_null);
    ASSERT_TRUE(st.ok()) << st;
    const auto* materialized_struct = assert_cast<const ColumnStruct*>(struct_dst.get());
    EXPECT_EQ(99,
              assert_cast<const ColumnInt32&>(materialized_struct->get_column(0)).get_element(0));
    EXPECT_EQ(99,
              assert_cast<const ColumnInt32&>(materialized_struct->get_column(0)).get_element(1));
    EXPECT_EQ("doris", materialized_struct->get_column(1).get_data_at(0).to_string());
    EXPECT_EQ("doris", materialized_struct->get_column(1).get_data_at(1).to_string());
}

TEST_F(ConstantColumnIteratorTest, NonEmptyComplexTextDefaultsAreRejected) {
    // Case: schema defaults currently support only [] and {} for complex non-null values. Reject
    // non-empty text forms explicitly instead of silently synthesizing a different value.
    TabletColumn int_child;
    int_child.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    int_child.set_is_nullable(false);
    TabletColumn string_child;
    string_child.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    string_child.set_is_nullable(false);

    TabletColumn array_column;
    array_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    array_column.set_is_nullable(false);
    array_column.set_default_value("[1]");
    array_column.add_sub_column(int_child);
    Field field;
    EXPECT_TRUE(Segment::get_default_value_field(array_column, &field)
                        .is<ErrorCode::NOT_IMPLEMENTED_ERROR>());

    TabletColumn map_column;
    map_column.set_type(FieldType::OLAP_FIELD_TYPE_MAP);
    map_column.set_is_nullable(false);
    map_column.set_default_value("{\"k\":7}");
    map_column.add_sub_column(string_child);
    map_column.add_sub_column(int_child);
    EXPECT_TRUE(Segment::get_default_value_field(map_column, &field)
                        .is<ErrorCode::NOT_IMPLEMENTED_ERROR>());

    TabletColumn struct_column;
    struct_column.set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    struct_column.set_is_nullable(false);
    struct_column.set_default_value("(99, doris)");
    struct_column.add_sub_column(int_child);
    struct_column.add_sub_column(string_child);
    EXPECT_TRUE(Segment::get_default_value_field(struct_column, &field)
                        .is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
}

TEST_F(ConstantColumnIteratorTest, VariantConstantMaterializesSubcolumnsAndZoneMap) {
    // Case: a typed VARIANT constant contains both a direct child and a nested path. Repeating it
    // must preserve both generated subcolumns, while text VARIANT defaults remain unsupported.
    TabletColumn column;
    column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    column.set_is_nullable(false);

    VariantMap values;
    values.try_emplace(PathInData("number"),
                       FieldWithDataType {.field = Field::create_field<TYPE_INT>(42)});
    values.try_emplace(PathInData("nested.name"),
                       FieldWithDataType {.field = Field::create_field<TYPE_STRING>("doris")});
    Field field = Field::create_field<TYPE_VARIANT>(std::move(values));
    assert_constant_pipeline(column, field);

    ConstantColumnIterator iterator(field);
    MutableColumnPtr dst = column.get_vec_type()->create_column();
    size_t rows = 2;
    bool has_null = true;
    auto st = iterator.next_batch(&rows, dst, &has_null);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(has_null);
    const auto* variant = assert_cast<const ColumnVariant*>(dst.get());
    for (size_t row = 0; row < rows; ++row) {
        Field actual = (*variant)[row];
        const auto& actual_values = actual.get<TYPE_VARIANT>().legacy_map();
        ASSERT_TRUE(actual_values.contains(PathInData("number")));
        EXPECT_EQ(42, actual_values.at(PathInData("number")).field.get<TYPE_INT>());
        ASSERT_TRUE(actual_values.contains(PathInData("nested.name")));
        EXPECT_EQ("doris", actual_values.at(PathInData("nested.name")).field.get<TYPE_STRING>());
    }

    column.set_default_value("{}");
    Field default_field;
    EXPECT_TRUE(Segment::get_default_value_field(column, &default_field)
                        .is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
}

TEST_F(ConstantColumnIteratorTest, EmptyMapConstantSupportsPrunedValueDestination) {
    // Case: projection prunes a MAP value subtree and changes the destination child layout. An
    // empty constant map must still append one empty map per row without touching pruned children.
    ConstantColumnIterator map_iterator(Field::create_field<TYPE_MAP>(Map {
            Field::create_field<TYPE_ARRAY>(Array {}), Field::create_field<TYPE_ARRAY>(Array {})}));
    map_iterator.set_column_name("m");
    auto st = map_iterator.set_access_paths(
            {make_data_access_path({"m", ColumnIterator::ACCESS_MAP_VALUES})}, {});
    ASSERT_TRUE(st.ok()) << st;
    map_iterator.remove_pruned_sub_iterators();

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
}

TEST_F(ConstantColumnIteratorTest, NullStructConstantSupportsPrunedFieldDestination) {
    // Case: projection keeps only one STRUCT field. A NULL constant must preserve row count and
    // nullability without requiring destination columns for fields that were pruned away.
    ConstantColumnIterator struct_iterator {Field()};
    struct_iterator.set_column_name("s");
    auto st = struct_iterator.set_access_paths({make_data_access_path({"s", "kept"})}, {});
    ASSERT_TRUE(st.ok()) << st;
    struct_iterator.remove_pruned_sub_iterators();

    MutableColumnPtr struct_dst = ColumnNullable::create(
            ColumnStruct::create(Columns {ColumnInt32::create()}), ColumnUInt8::create());
    size_t rows = 2;
    bool has_null = false;
    st = struct_iterator.next_batch(&rows, struct_dst, &has_null);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(has_null);
    const auto* nullable_struct = assert_cast<const ColumnNullable*>(struct_dst.get());
    ASSERT_EQ(2, nullable_struct->size());
    EXPECT_TRUE(nullable_struct->is_null_at(0));
    EXPECT_TRUE(nullable_struct->is_null_at(1));
}

TEST_F(ConstantColumnIteratorTest, DummyBinaryReaderFillsEmptyNonNullableMap) {
    // Case: a legacy VARIANT segment has no binary stream. Its dummy reader must synthesize an
    // empty non-null MAP for every row instead of applying NULL to a non-nullable destination.
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
