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

// Doris DATETIMEV2 starts at 0000-01-01 00:00:00, but the Parquet reader used to reject anything
// below 0001-01-01: a value Doris accepts, stores and exports could not be read back out of
// Doris's own file. Two independent defects sit behind that.
//
//   1. The range gate was a whole year narrower than the target type, and it was applied to the
//      raw instant *before* the timezone offset, so it also lost values near year 1 east of UTC
//      and near year 9999 west of it.
//   2. The local-timestamp materialization added `calc_daynr(1970, 1, 1)` to a proleptic
//      Gregorian day ordinal. Doris follows MySQL's calendar, in which year 0 is not a leap year,
//      so the two numberings differ for 0000-01-01 .. 0000-02-28 and the whole window decoded one
//      day early.
//
// These tests pin both, against an oracle (cctz) that is independent of Doris's own arithmetic.
// See https://github.com/apache/doris/issues/67447

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "core/data_type_serde/parquet_decode_source.h"
#include "core/data_type_serde/parquet_timestamp.h"
#include "core/value/vdatetime_value.h"

namespace doris {
namespace {

#pragma pack(1)
struct TestInt96 {
    int64_t nanos_of_day;
    int32_t julian_day;
};
#pragma pack()
static_assert(sizeof(TestInt96) == 12);

constexpr int32_t JULIAN_UNIX_EPOCH = 2440588;
constexpr int64_t MICROS_PER_SECOND = 1000000LL;
constexpr int64_t MICROS_PER_DAY = 86400000000LL;

// Independent oracle: cctz uses the proleptic Gregorian calendar, which is exactly what Parquet
// timestamps are defined in. Nothing here goes through Doris's own day arithmetic.
int64_t utc_micros(int year, int month, int day, int hour = 0, int minute = 0, int second = 0,
                   int64_t microsecond = 0) {
    const auto tp = cctz::convert(cctz::civil_second(year, month, day, hour, minute, second),
                                  cctz::utc_time_zone());
    return tp.time_since_epoch().count() * MICROS_PER_SECOND + microsecond;
}

std::string civil_day_string(int64_t epoch_days) {
    const cctz::civil_day day = cctz::civil_day(1970, 1, 1) + epoch_days;
    char buffer[32];
    snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", static_cast<int>(day.year()),
             static_cast<int>(day.month()), static_cast<int>(day.day()));
    return buffer;
}

class VectorDecodeSource final : public ParquetDecodeSource {
public:
    template <typename T>
    void set_values(const std::vector<T>& values) {
        _width = sizeof(T);
        _values.resize(values.size() * sizeof(T));
        memcpy(_values.data(), values.data(), _values.size());
    }

    template <typename T>
    void set_dictionary(const std::vector<T>& values, std::vector<uint32_t> indices) {
        _dictionary_width = sizeof(T);
        _dictionary.resize(values.size() * sizeof(T));
        memcpy(_dictionary.data(), values.data(), _dictionary.size());
        _indices = std::move(indices);
        _index_offset = 0;
    }

    Status decode_fixed_values(size_t num_values, ParquetFixedValueConsumer& consumer) override {
        const uint8_t* begin = _values.data() + _offset * _width;
        _offset += num_values;
        return consumer.consume(begin, num_values, _width);
    }

    Status decode_binary_values(size_t num_values, ParquetBinaryValueConsumer& consumer) override {
        return Status::NotSupported("binary values are not part of these tests");
    }

    Status skip_values(size_t num_values) override {
        _offset += num_values;
        _index_offset += num_values;
        return Status::OK();
    }

    bool has_dictionary() const override { return !_dictionary.empty(); }
    uint64_t dictionary_generation() const override { return 1; }
    size_t dictionary_size() const override {
        return _dictionary_width == 0 ? 0 : _dictionary.size() / _dictionary_width;
    }

    Status decode_dictionary(ParquetFixedValueConsumer& fixed_consumer,
                             ParquetBinaryValueConsumer& binary_consumer) override {
        return fixed_consumer.consume(_dictionary.data(), dictionary_size(), _dictionary_width);
    }

    Status decode_dictionary_indices(size_t num_values, std::vector<uint32_t>* indices) override {
        indices->assign(_indices.begin() + _index_offset,
                        _indices.begin() + _index_offset + num_values);
        _index_offset += num_values;
        return Status::OK();
    }

private:
    std::vector<uint8_t> _values;
    std::vector<uint8_t> _dictionary;
    std::vector<uint32_t> _indices;
    size_t _width = 0;
    size_t _dictionary_width = 0;
    size_t _offset = 0;
    size_t _index_offset = 0;
};

struct CivilCase {
    const char* rendered;
    int year;
    int month;
    int day;
    int hour;
    int minute;
    int second;
    int64_t microsecond;
};

// Brackets both edges of the 0000-01-01 .. 0000-02-28 window where Doris's day numbering and the
// proleptic Gregorian ordinal disagree, plus the two range limits and the rows from the report.
const std::vector<CivilCase>& civil_cases() {
    static const std::vector<CivilCase> cases = {
            {"0000-01-01 00:00:00.000000", 0, 1, 1, 0, 0, 0, 0},
            {"0000-01-01 12:34:56.000000", 0, 1, 1, 12, 34, 56, 0},
            {"0000-01-02 00:00:00.000000", 0, 1, 2, 0, 0, 0, 0},
            {"0000-01-31 23:59:59.999999", 0, 1, 31, 23, 59, 59, 999999},
            {"0000-02-28 23:59:59.999999", 0, 2, 28, 23, 59, 59, 999999},
            {"0000-03-01 00:00:00.000000", 0, 3, 1, 0, 0, 0, 0},
            {"0000-12-31 00:00:00.000000", 0, 12, 31, 0, 0, 0, 0},
            {"0001-01-01 00:00:00.000000", 1, 1, 1, 0, 0, 0, 0},
            {"1969-12-31 23:59:59.000000", 1969, 12, 31, 23, 59, 59, 0},
            {"1970-01-01 00:00:00.000000", 1970, 1, 1, 0, 0, 0, 0},
            {"2024-01-01 12:00:00.000000", 2024, 1, 1, 12, 0, 0, 0},
            {"9999-12-31 23:59:59.999999", 9999, 12, 31, 23, 59, 59, 999999},
    };
    return cases;
}

int64_t case_micros(const CivilCase& c) {
    return utc_micros(c.year, c.month, c.day, c.hour, c.minute, c.second, c.microsecond);
}

// Materializes `values` as DATETIMEV2 through the plain Parquet path. `timezone` is only consulted
// when `adjusted_to_utc` is set, matching the reader.
Status materialize_datetime(const std::vector<int64_t>& values, bool adjusted_to_utc,
                            const cctz::time_zone* timezone, MutableColumnPtr* column,
                            IColumn::Filter* null_map = nullptr,
                            ParquetTimeUnit unit = ParquetTimeUnit::MICROS) {
    static DataTypeDateTimeV2 type(6);
    VectorDecodeSource source;
    source.set_values(values);
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                  .logical_type = ParquetLogicalType::TIMESTAMP,
                                  .time_unit = unit,
                                  .timestamp_is_adjusted_to_utc = adjusted_to_utc,
                                  .timezone = timezone};
    ParquetMaterializationState state;
    state.conversion_failure_null_map = null_map;
    *column = type.create_column();
    return type.get_serde()->read_column_from_parquet(**column, source, context, values.size(),
                                                      state);
}

std::string rendered(const IColumn& column, size_t row) {
    static DataTypeDateTimeV2 type(6);
    return type.to_string(column, row);
}

} // namespace

class DataTypeDateTimeV2SerDeCalendarTest : public ::testing::Test {};

// The gate must not be stricter than the type it materializes into. Deriving both ends from the
// type's own limits keeps that true if either side ever moves.
TEST_F(DataTypeDateTimeV2SerDeCalendarTest, RangeBoundsMatchTheDateTimeType) {
    const DateV2Value<DateV2ValueType> min_date(static_cast<uint32_t>(MIN_DATE_V2));
    const DateV2Value<DateV2ValueType> max_date(static_cast<uint32_t>(MAX_DATE_V2));
    ASSERT_EQ(0, min_date.year());
    ASSERT_EQ(1, min_date.month());
    ASSERT_EQ(1, min_date.day());
    ASSERT_EQ(9999, max_date.year());

    EXPECT_EQ(utc_micros(min_date.year(), min_date.month(), min_date.day()),
              MIN_DORIS_TIMESTAMP_MICROS);
    EXPECT_EQ(utc_micros(max_date.year(), max_date.month(), max_date.day(), 23, 59, 59, 999999),
              MAX_DORIS_TIMESTAMP_MICROS);
    // Year 0 is a leap year in the proleptic Gregorian calendar, so the old 0001-01-01 floor was
    // 366 days -- not 365 -- above the type's minimum.
    EXPECT_EQ(366 * MICROS_PER_DAY, utc_micros(1, 1, 1) - MIN_DORIS_TIMESTAMP_MICROS);
}

TEST_F(DataTypeDateTimeV2SerDeCalendarTest, LocalTimestampKeepsTheCivilValue) {
    std::vector<int64_t> values;
    for (const auto& c : civil_cases()) {
        values.push_back(case_micros(c));
    }

    MutableColumnPtr column;
    ASSERT_TRUE(materialize_datetime(values, false, nullptr, &column).ok());
    ASSERT_EQ(civil_cases().size(), column->size());
    for (size_t i = 0; i < civil_cases().size(); ++i) {
        EXPECT_EQ(civil_cases()[i].rendered, rendered(*column, i)) << "row " << i;
    }
}

// A UTC-adjusted timestamp is an instant. Read in UTC it must land on the same civil value as the
// local encoding of the same wall clock.
TEST_F(DataTypeDateTimeV2SerDeCalendarTest, UtcTimestampKeepsTheCivilValueInUtc) {
    std::vector<int64_t> values;
    for (const auto& c : civil_cases()) {
        values.push_back(case_micros(c));
    }

    const auto utc = cctz::utc_time_zone();
    MutableColumnPtr column;
    ASSERT_TRUE(materialize_datetime(values, true, &utc, &column).ok());
    ASSERT_EQ(civil_cases().size(), column->size());
    for (size_t i = 0; i < civil_cases().size(); ++i) {
        EXPECT_EQ(civil_cases()[i].rendered, rendered(*column, i)) << "row " << i;
    }
}

// The offset has to be applied before the civil range is judged. A local 0001-01-01 00:00:00 east
// of UTC is an instant *below* the civil minimum and a local 9999-12-31 23:59:59 west of it is an
// instant *above* the maximum; both are representable DATETIME values and must survive.
TEST_F(DataTypeDateTimeV2SerDeCalendarTest, UtcTimestampSurvivesOffsetsAtTheRangeEdges) {
    const auto plus_eight = cctz::fixed_time_zone(std::chrono::hours(8));
    const int64_t offset_eight = 8 * 3600 * MICROS_PER_SECOND;

    // Below the floor the gate used to sit at, which is why this value was lost even though
    // year 1 was supposed to be inside the accepted range.
    const int64_t year_one_instant = utc_micros(1, 1, 1) - offset_eight;
    ASSERT_LT(year_one_instant, utc_micros(1, 1, 1)) << "test no longer covers the old floor";
    MutableColumnPtr year_one_column;
    ASSERT_TRUE(materialize_datetime({year_one_instant}, true, &plus_eight, &year_one_column).ok());
    EXPECT_EQ("0001-01-01 00:00:00.000000", rendered(*year_one_column, 0));

    // Below the current floor as well: the civil range can only be judged after the offset, or
    // widening the constant would simply move the same defect down by one year.
    const int64_t year_zero_instant = MIN_DORIS_TIMESTAMP_MICROS - offset_eight;
    ASSERT_LT(year_zero_instant, MIN_DORIS_TIMESTAMP_MICROS)
            << "test no longer covers the low edge";
    MutableColumnPtr year_zero_column;
    ASSERT_TRUE(
            materialize_datetime({year_zero_instant}, true, &plus_eight, &year_zero_column).ok());
    EXPECT_EQ("0000-01-01 00:00:00.000000", rendered(*year_zero_column, 0));

    // The same asymmetry at the top, west of UTC.
    const auto minus_five = cctz::fixed_time_zone(std::chrono::hours(-5));
    const int64_t high_instant = MAX_DORIS_TIMESTAMP_MICROS + 5 * 3600 * MICROS_PER_SECOND;
    ASSERT_GT(high_instant, MAX_DORIS_TIMESTAMP_MICROS) << "test no longer covers the high edge";
    MutableColumnPtr high_column;
    ASSERT_TRUE(materialize_datetime({high_instant}, true, &minus_five, &high_column).ok());
    EXPECT_EQ("9999-12-31 23:59:59.999999", rendered(*high_column, 0));

    // An offset only shifts the window; it does not widen it. One microsecond further out on
    // either side still has no DATETIME representation in that timezone.
    MutableColumnPtr rejected;
    EXPECT_FALSE(materialize_datetime({year_zero_instant - 1}, true, &plus_eight, &rejected).ok());
    EXPECT_FALSE(materialize_datetime({high_instant + 1}, true, &minus_five, &rejected).ok());
}

// Every representable day of year zero, checked against cctz rather than against Doris's own
// day arithmetic. 0000-02-29 exists in the proleptic Gregorian calendar but not in Doris's, so it
// is the one day in the window that must fail instead of colliding with 0000-02-28 or 0000-03-01.
TEST_F(DataTypeDateTimeV2SerDeCalendarTest, LocalTimestampCoversEveryDayOfYearZero) {
    constexpr int64_t FIRST_DAY = -719528; // 0000-01-01
    constexpr int64_t LAST_DAY = -719163;  // 0000-12-31
    constexpr int64_t LEAP_DAY = -719469;  // 0000-02-29, proleptic Gregorian only
    const auto row_count = static_cast<size_t>(LAST_DAY - FIRST_DAY + 1);
    ASSERT_EQ(366, row_count);

    std::vector<int64_t> values;
    values.reserve(row_count);
    for (int64_t day = FIRST_DAY; day <= LAST_DAY; ++day) {
        values.push_back(day * MICROS_PER_DAY);
    }

    IColumn::Filter null_map(row_count, 0);
    MutableColumnPtr column;
    ASSERT_TRUE(materialize_datetime(values, false, nullptr, &column, &null_map).ok());
    ASSERT_EQ(row_count, column->size());

    size_t rejected = 0;
    for (size_t i = 0; i < row_count; ++i) {
        const int64_t day = FIRST_DAY + static_cast<int64_t>(i);
        if (day == LEAP_DAY) {
            EXPECT_EQ(1, null_map[i]) << "0000-02-29 must not materialize";
            ++rejected;
            continue;
        }
        ASSERT_EQ(0, null_map[i]) << "day " << day << " was rejected";
        EXPECT_EQ(civil_day_string(day) + " 00:00:00.000000", rendered(*column, i))
                << "day " << day;
    }
    EXPECT_EQ(1, rejected);
}

TEST_F(DataTypeDateTimeV2SerDeCalendarTest, RejectsValuesOutsideTheDateTimeRange) {
    const auto utc = cctz::utc_time_zone();
    const auto expect_rejected = [&](int64_t micros, bool adjusted_to_utc, const char* why) {
        MutableColumnPtr column;
        const auto status = materialize_datetime({micros}, adjusted_to_utc,
                                                 adjusted_to_utc ? &utc : nullptr, &column);
        EXPECT_FALSE(status.ok()) << why << " (micros=" << micros << ")";
        EXPECT_EQ(0, column->size()) << why;
    };

    for (bool adjusted : {false, true}) {
        expect_rejected(MIN_DORIS_TIMESTAMP_MICROS - 1, adjusted, "one micro before 0000-01-01");
        expect_rejected(MIN_DORIS_TIMESTAMP_MICROS - MICROS_PER_DAY, adjusted,
                        "one day before 0000-01-01");
        expect_rejected(MAX_DORIS_TIMESTAMP_MICROS + 1, adjusted,
                        "one micro after 9999-12-31 23:59:59.999999");
        expect_rejected(utc_micros(10000, 1, 1), adjusted, "year 10000");
    }
    // The proleptic-only leap day has no Doris representation even though it is inside the range.
    expect_rejected(-719469 * MICROS_PER_DAY, false, "0000-02-29 as a local timestamp");
    expect_rejected(-719469 * MICROS_PER_DAY, true, "0000-02-29 as an instant");
}

TEST_F(DataTypeDateTimeV2SerDeCalendarTest, MillisUnitKeepsYearZero) {
    // INT64 nanos cannot reach year zero at all (it only spans 1677..2262), so millis is the only
    // other unit that needs covering here.
    const std::vector<int64_t> millis {utc_micros(0, 1, 1, 12, 34, 56) / 1000,
                                       utc_micros(0, 2, 28) / 1000, utc_micros(0, 3, 1) / 1000};
    MutableColumnPtr column;
    ASSERT_TRUE(
            materialize_datetime(millis, false, nullptr, &column, nullptr, ParquetTimeUnit::MILLIS)
                    .ok());
    ASSERT_EQ(3, column->size());
    EXPECT_EQ("0000-01-01 12:34:56.000000", rendered(*column, 0));
    EXPECT_EQ("0000-02-28 00:00:00.000000", rendered(*column, 1));
    EXPECT_EQ("0000-03-01 00:00:00.000000", rendered(*column, 2));
}

TEST_F(DataTypeDateTimeV2SerDeCalendarTest, Int96KeepsYearZero) {
    // INT96 is always an instant. 0000-01-01 is Julian day 1721060.
    const std::vector<TestInt96> values {
            {45296LL * 1000000000LL, JULIAN_UNIX_EPOCH - 719528}, // 0000-01-01 12:34:56
            {0, JULIAN_UNIX_EPOCH - 719468},                      // 0000-03-01 00:00:00
    };
    VectorDecodeSource source;
    source.set_values(values);
    const auto utc = cctz::utc_time_zone();
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT96,
                                  .logical_type = ParquetLogicalType::TIMESTAMP,
                                  .timezone = &utc};
    ParquetMaterializationState state;
    DataTypeDateTimeV2 type(6);
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 2, state).ok());
    ASSERT_EQ(2, column->size());
    EXPECT_EQ("0000-01-01 12:34:56.000000", rendered(*column, 0));
    EXPECT_EQ("0000-03-01 00:00:00.000000", rendered(*column, 1));
}

// Dictionary-encoded pages convert each entry once and then fan the result out over the row
// indices, a different code path from the plain decoder above.
TEST_F(DataTypeDateTimeV2SerDeCalendarTest, DictionaryEncodedYearZeroMaterializes) {
    const std::vector<int64_t> dictionary {utc_micros(0, 1, 1, 12, 34, 56), utc_micros(0, 3, 1),
                                           utc_micros(2024, 1, 1, 12)};
    VectorDecodeSource source;
    source.set_dictionary(dictionary, {2, 0, 1, 0});
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                  .encoding = ParquetValueEncoding::DICTIONARY,
                                  .logical_type = ParquetLogicalType::TIMESTAMP,
                                  .time_unit = ParquetTimeUnit::MICROS};
    IColumn::Filter null_map(4, 0);
    ParquetMaterializationState state;
    state.conversion_failure_null_map = &null_map;
    DataTypeDateTimeV2 type(6);
    auto column = type.create_column();

    ASSERT_TRUE(
            type.get_serde()->read_column_from_parquet(*column, source, context, 4, state).ok());
    ASSERT_EQ(4, column->size());
    EXPECT_EQ(null_map, IColumn::Filter({0, 0, 0, 0}));
    EXPECT_EQ("2024-01-01 12:00:00.000000", rendered(*column, 0));
    EXPECT_EQ("0000-01-01 12:34:56.000000", rendered(*column, 1));
    EXPECT_EQ("0000-03-01 00:00:00.000000", rendered(*column, 2));
    EXPECT_EQ("0000-01-01 12:34:56.000000", rendered(*column, 3));
}

// The decoded-value path is a second copy of the same conversion, used when a reader hands Doris
// pre-decoded values instead of an encoded page. It shares the helpers, so it shares the bug.
TEST_F(DataTypeDateTimeV2SerDeCalendarTest, DecodedValuesKeepYearZero) {
    const std::vector<int64_t> values {utc_micros(0, 1, 1, 12, 34, 56), utc_micros(0, 1, 31),
                                       utc_micros(0, 3, 1)};
    NullMap conversion_failures(values.size(), 0);
    DecodedColumnView view {.value_kind = DecodedValueKind::INT64,
                            .time_unit = DecodedTimeUnit::MICROS,
                            .row_count = static_cast<int64_t>(values.size()),
                            .values = reinterpret_cast<const uint8_t*>(values.data()),
                            .enable_strict_mode = false,
                            .conversion_failure_null_map = &conversion_failures};
    DataTypeDateTimeV2 type(6);
    auto column = type.create_column();

    ASSERT_TRUE(type.get_serde()->read_column_from_decoded_values(*column, view).ok());
    ASSERT_EQ(values.size(), column->size());
    EXPECT_EQ(conversion_failures, NullMap({0, 0, 0}));
    EXPECT_EQ("0000-01-01 12:34:56.000000", rendered(*column, 0));
    EXPECT_EQ("0000-01-31 00:00:00.000000", rendered(*column, 1));
    EXPECT_EQ("0000-03-01 00:00:00.000000", rendered(*column, 2));
}

// TIMESTAMPTZ shares the same helper and the same storage minimum as DATETIMEV2, so the reader
// must not be narrower than that type either.
TEST_F(DataTypeDateTimeV2SerDeCalendarTest, TimestampTzKeepsYearZero) {
    const std::vector<int64_t> values {MIN_DORIS_TIMESTAMP_MICROS, utc_micros(0, 1, 1, 12, 34, 56),
                                       utc_micros(0, 3, 1), MAX_DORIS_TIMESTAMP_MICROS};
    VectorDecodeSource source;
    source.set_values(values);
    ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                  .logical_type = ParquetLogicalType::TIMESTAMP,
                                  .time_unit = ParquetTimeUnit::MICROS,
                                  .timestamp_is_adjusted_to_utc = true};
    ParquetMaterializationState state;
    DataTypeTimeStampTz type(6);
    auto column = type.create_column();

    ASSERT_TRUE(type.get_serde()
                        ->read_column_from_parquet(*column, source, context, values.size(), state)
                        .ok());
    const auto& data = assert_cast<const ColumnTimeStampTz&>(*column).get_data();
    ASSERT_EQ(values.size(), data.size());
    EXPECT_EQ(0, data[0].year());
    EXPECT_EQ(1, data[0].month());
    EXPECT_EQ(1, data[0].day());
    EXPECT_EQ(0, data[1].year());
    EXPECT_EQ(34, data[1].minute());
    EXPECT_EQ(0, data[2].year());
    EXPECT_EQ(3, data[2].month());
    EXPECT_EQ(9999, data[3].year());
    EXPECT_EQ(999999, data[3].microsecond());
}

TEST_F(DataTypeDateTimeV2SerDeCalendarTest, TimestampTzStillRejectsUnrepresentableValues) {
    const auto expect_rejected = [&](int64_t micros, const char* why) {
        VectorDecodeSource source;
        source.set_values(std::vector<int64_t> {micros});
        ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                      .logical_type = ParquetLogicalType::TIMESTAMP,
                                      .time_unit = ParquetTimeUnit::MICROS,
                                      .timestamp_is_adjusted_to_utc = true};
        ParquetMaterializationState state;
        DataTypeTimeStampTz type(6);
        auto column = type.create_column();
        EXPECT_FALSE(
                type.get_serde()->read_column_from_parquet(*column, source, context, 1, state).ok())
                << why;
        EXPECT_EQ(0, column->size()) << why;
    };

    expect_rejected(MIN_DORIS_TIMESTAMP_MICROS - 1, "one micro before 0000-01-01");
    expect_rejected(MAX_DORIS_TIMESTAMP_MICROS + 1, "one micro after 9999-12-31 23:59:59.999999");
    expect_rejected(utc_micros(10000, 1, 1), "year 10000");
}

// Predicate pushdown converts the values through a separate consumer before the filter runs, so a
// value the reader refuses is compared as a conversion failure instead of as itself and the row
// silently drops out of the result. Year zero has to reach the predicate as a real value.
TEST_F(DataTypeDateTimeV2SerDeCalendarTest, RawPredicateKeepsYearZero) {
    class CapturingConsumer final : public ParquetLogicalValueConsumer {
    public:
        Status consume(const uint8_t* values, size_t num_values, size_t value_width,
                       const uint8_t* conversion_nulls) override {
            width = value_width;
            bytes.assign(values, values + num_values * value_width);
            nulls.clear();
            nulls.resize_fill(num_values, 0);
            if (conversion_nulls != nullptr) {
                memcpy(nulls.data(), conversion_nulls, num_values);
            }
            return Status::OK();
        }

        std::vector<uint8_t> bytes;
        IColumn::Filter nulls;
        size_t width = 0;
    };

    const std::vector<int64_t> values {utc_micros(0, 1, 1, 12, 34, 56), utc_micros(0, 3, 1),
                                       utc_micros(2024, 1, 1, 12)};
    const auto utc = cctz::utc_time_zone();
    for (bool adjusted_to_utc : {false, true}) {
        VectorDecodeSource source;
        source.set_values(values);
        const ParquetDecodeContext context {.physical_type = ParquetPhysicalType::INT64,
                                            .logical_type = ParquetLogicalType::TIMESTAMP,
                                            .time_unit = ParquetTimeUnit::MICROS,
                                            .timestamp_is_adjusted_to_utc = adjusted_to_utc,
                                            .timezone = &utc};
        CapturingConsumer consumer;
        DataTypeDateTimeV2 type(6);
        ASSERT_TRUE(type.get_serde()
                            ->read_parquet_raw_predicate(source, context, values.size(), false,
                                                         consumer)
                            .ok())
                << "adjusted_to_utc=" << adjusted_to_utc;
        EXPECT_EQ(consumer.nulls, IColumn::Filter(values.size(), 0))
                << "adjusted_to_utc=" << adjusted_to_utc;
        ASSERT_EQ(sizeof(DateV2Value<DateTimeV2ValueType>), consumer.width);

        auto column = ColumnDateTimeV2::create();
        column->get_data().resize(values.size());
        memcpy(column->get_data().data(), consumer.bytes.data(), consumer.bytes.size());
        EXPECT_EQ("0000-01-01 12:34:56.000000", rendered(*column, 0));
        EXPECT_EQ("0000-03-01 00:00:00.000000", rendered(*column, 1));
        EXPECT_EQ("2024-01-01 12:00:00.000000", rendered(*column, 2));
    }
}

} // namespace doris
