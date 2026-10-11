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

#include <initializer_list>
#include <limits>
#include <vector>

#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_decimal.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/value/decimalv2_value.h"
#include "core/value/timestamp_ns_value.h"
#include "core/value/vdatetime_value.h"
#include "testutil/column_helper.h"
#include "util/raw_value.h"

namespace doris {

// Oracle tests for IColumn::update_identity_hash: the batch column interface must reproduce
// RawValue::identity_hash over the column's own get_data_at bytes for every supported storage
// layout (nullable and non-nullable, fixed and variable width), including null rows, which
// append the shared four zero canonical bytes.
class IdentityColumnHashTest : public ::testing::Test {
protected:
    // Scalar oracle over the same block: per-row get_data_at + RawValue::identity_hash.
    static std::vector<uint32_t> scalar_oracle(const Block& block, uint32_t mod,
                                               const PrimitiveType type) {
        const auto& column = block.get_by_position(0).column;
        std::vector<uint32_t> hashes(column->size(), 0);
        for (size_t i = 0; i < column->size(); i++) {
            auto val = column->get_data_at(i);
            hashes[i] = RawValue::identity_hash(val.data, val.size, type, hashes[i], mod);
        }
        return hashes;
    }

    static std::vector<uint32_t> batch_hash(const Block& block, uint32_t mod) {
        const auto& column = block.get_by_position(0).column;
        std::vector<uint32_t> hashes(column->size(), 0);
        column->update_identity_hash(hashes.data(), mod);
        return hashes;
    }
};

TEST_F(IdentityColumnHashTest, IntColumnMatchesScalarOracle) {
    for (uint32_t mod : {1U, 8U, 10U, 251U, 1024U}) {
        auto block = ColumnHelper::create_block<DataTypeInt32>(
                {3, 8, 100, 999, -1, -8, std::numeric_limits<int32_t>::min(),
                 std::numeric_limits<int32_t>::max()});
        auto expected = scalar_oracle(block, mod, TYPE_INT);
        auto actual = batch_hash(block, mod);
        ASSERT_EQ(expected, actual);
    }
}

TEST_F(IdentityColumnHashTest, NullableIntColumnMatchesScalarOracle) {
    for (uint32_t mod : {8U, 251U, 1024U}) {
        auto block =
                ColumnHelper::create_nullable_block<DataTypeInt32>({0, 1, 300, -7}, {1, 0, 0, 1});
        auto expected = scalar_oracle(block, mod, TYPE_INT);
        auto actual = batch_hash(block, mod);
        ASSERT_EQ(expected, actual);
    }
}

TEST_F(IdentityColumnHashTest, BigIntAndLargeIntMatchScalarOracle) {
    for (uint32_t mod : {8U, 251U, 1009U}) {
        auto block = ColumnHelper::create_block<DataTypeInt64>(
                {0, 1, -1, 0x123456789abcdefLL, std::numeric_limits<int64_t>::min()});
        auto expected = scalar_oracle(block, mod, TYPE_BIGINT);
        ASSERT_EQ(expected, batch_hash(block, mod));
    }
    for (uint32_t mod : {8U, 251U, 1009U}) {
        auto block = ColumnHelper::create_block<DataTypeInt128>(
                {int128_t(0), int128_t(1), int128_t(-1)});
        auto expected = scalar_oracle(block, mod, TYPE_LARGEINT);
        ASSERT_EQ(expected, batch_hash(block, mod));
    }
}

TEST_F(IdentityColumnHashTest, StringColumnMatchesScalarOracle) {
    for (uint32_t mod : {8U, 251U, 1024U}) {
        auto block = ColumnHelper::create_block<DataTypeString>(
                {"", "A", "BC", "identity", std::string(40, 'x')});
        auto expected = scalar_oracle(block, mod, TYPE_STRING);
        auto actual = batch_hash(block, mod);
        ASSERT_EQ(expected, actual);
    }
}

TEST_F(IdentityColumnHashTest, NullableStringColumnMatchesScalarOracle) {
    for (uint32_t mod : {8U, 251U, 1024U}) {
        auto block = ColumnHelper::create_nullable_block<DataTypeString>(
                {"", "A", "zz", "null-payload"}, {1, 0, 0, 1});
        auto expected = scalar_oracle(block, mod, TYPE_STRING);
        auto actual = batch_hash(block, mod);
        ASSERT_EQ(expected, actual);
    }
}

TEST_F(IdentityColumnHashTest, DecimalV3ColumnsMatchScalarOracle) {
    for (uint32_t mod : {8U, 251U, 1009U}) {
        // Decimal columns need an explicit scale, so build them directly instead of via
        // ColumnHelper::create_block (whose DataType default constructor has no precision).
        auto col32 = ColumnDecimal32::create(0, 0);
        const std::vector<int32_t> values32 = {0, 1, -123456};
        col32->insert_many_fix_len_data(reinterpret_cast<const char*>(values32.data()),
                                        values32.size());
        Block block32({ColumnWithTypeAndName(col32->get_ptr(),
                                             std::make_shared<DataTypeDecimal32>(9, 0), "d32")});
        auto expected = scalar_oracle(block32, mod, TYPE_DECIMAL32);
        ASSERT_EQ(expected, batch_hash(block32, mod));

        auto col64 = ColumnDecimal64::create(0, 0);
        const std::vector<int64_t> values64 = {0, 1, -123456789012345LL};
        col64->insert_many_fix_len_data(reinterpret_cast<const char*>(values64.data()),
                                        values64.size());
        Block block64({ColumnWithTypeAndName(col64->get_ptr(),
                                             std::make_shared<DataTypeDecimal64>(18, 0), "d64")});
        expected = scalar_oracle(block64, mod, TYPE_DECIMAL64);
        ASSERT_EQ(expected, batch_hash(block64, mod));
    }
}

TEST_F(IdentityColumnHashTest, DecimalV2ColumnMatchesScalarOracle) {
    for (uint32_t mod : {8U, 251U, 1009U}) {
        auto col = ColumnDecimal128V2::create(0, 9);
        const std::vector<DecimalV2Value> values = {DecimalV2Value(0, 0),
                                                    DecimalV2Value(123456789012LL, 456000000),
                                                    DecimalV2Value(-123456789012LL, 456000000)};
        for (const auto& value : values) {
            col->insert_value(value);
        }
        Block block({ColumnWithTypeAndName(col->get_ptr(),
                                           std::make_shared<DataTypeDecimalV2>(27, 9), "dv2")});
        auto expected = scalar_oracle(block, mod, TYPE_DECIMALV2);
        auto actual = batch_hash(block, mod);
        ASSERT_EQ(expected, actual);
    }
}

TEST_F(IdentityColumnHashTest, DateV2AndDateTimeV2MatchScalarOracle) {
    DateV2Value<DateV2ValueType> date;
    date.unchecked_set_time(2026, 1, 2, 0, 0, 0, 0);
    DateV2Value<DateTimeV2ValueType> datetime;
    datetime.unchecked_set_time(2026, 6, 30, 23, 59, 59, 123456);
    for (uint32_t mod : {8U, 251U, 1024U}) {
        auto block =
                ColumnHelper::create_block<DataTypeDateV2>({date, DateV2Value<DateV2ValueType>()});
        auto expected = scalar_oracle(block, mod, TYPE_DATEV2);
        ASSERT_EQ(expected, batch_hash(block, mod));

        auto block2 = ColumnHelper::create_block<DataTypeDateTimeV2>(
                {datetime, DateV2Value<DateTimeV2ValueType>()});
        expected = scalar_oracle(block2, mod, TYPE_DATETIMEV2);
        ASSERT_EQ(expected, batch_hash(block2, mod));
    }
}

TEST_F(IdentityColumnHashTest, LegacyDateAndDatetimeMatchScalarOracle) {
    auto date = VecDateTimeValue::create_from_olap_date(20260102);
    auto datetime = VecDateTimeValue::create_from_olap_datetime(20260102030405);
    for (uint32_t mod : {8U, 251U, 1024U}) {
        auto block = ColumnHelper::create_block<DataTypeDate>({date, VecDateTimeValue()});
        auto expected = scalar_oracle(block, mod, TYPE_DATE);
        ASSERT_EQ(expected, batch_hash(block, mod));

        auto block2 = ColumnHelper::create_block<DataTypeDateTime>({datetime, VecDateTimeValue()});
        expected = scalar_oracle(block2, mod, TYPE_DATETIME);
        ASSERT_EQ(expected, batch_hash(block2, mod));
    }
}

// A nullable legacy DATE/DATETIME distribution column with a non-zero incoming seed must hash a
// null row to the four zero canonical bytes alone, exactly like the scalar path over
// get_data_at == {nullptr, 0}. Guards the ColumnVector legacy-date branch's null_map handling.
TEST_F(IdentityColumnHashTest, NullableLegacyDateAndDatetimeMatchScalarOracle) {
    auto date = VecDateTimeValue::create_from_olap_date(20260102);
    auto datetime = VecDateTimeValue::create_from_olap_datetime(20260102030405);
    for (uint32_t mod : {7U, 251U, 1024U}) {
        auto block = ColumnHelper::create_nullable_block<DataTypeDate>({date, VecDateTimeValue()},
                                                                       {0, 1});
        ASSERT_EQ(scalar_oracle(block, mod, TYPE_DATE), batch_hash(block, mod));

        auto block2 = ColumnHelper::create_nullable_block<DataTypeDateTime>(
                {datetime, VecDateTimeValue(), datetime}, {1, 0, 1});
        ASSERT_EQ(scalar_oracle(block2, mod, TYPE_DATETIME), batch_hash(block2, mod));
    }
    // The same oracle with a non-zero incoming seed (as if a first distribution column had
    // already hashed): seed * 256^4 != seed for the null row, so any divergence shows up here.
    for (uint32_t seed : {3U, 5U, 1000000007U}) {
        auto block = ColumnHelper::create_nullable_block<DataTypeDate>({date, VecDateTimeValue()},
                                                                       {0, 1});
        const auto& column = block.get_by_position(0).column;
        for (uint32_t mod : {7U, 251U}) {
            std::vector<uint32_t> expected(column->size(), seed);
            for (size_t row = 0; row < column->size(); row++) {
                auto val = column->get_data_at(row);
                expected[row] =
                        RawValue::identity_hash(val.data, val.size, TYPE_DATE, expected[row], mod);
            }
            std::vector<uint32_t> actual(column->size(), seed);
            column->update_identity_hash(actual.data(), mod);
            ASSERT_EQ(expected, actual);
        }
    }
}

TEST_F(IdentityColumnHashTest, TimestampNsMatchesScalarOracle) {
    for (uint32_t mod : {8U, 257U, 1009U}) {
        auto block = ColumnHelper::create_block<DataTypeTimeStampNs>(
                {TimeStampNsValue(0), TimeStampNsValue(1), TimeStampNsValue(-1)});
        auto expected = scalar_oracle(block, mod, TYPE_TIMESTAMP_NS);
        ASSERT_EQ(expected, batch_hash(block, mod));
    }
}

TEST_F(IdentityColumnHashTest, UnsupportedColumnThrows) {
    // Array/Map/Struct cannot be distribution columns; the base interface must reject them.
    auto array_data_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    auto column = array_data_type->create_column();
    Block block({ColumnWithTypeAndName(column->get_ptr(), array_data_type, "array")});
    std::vector<uint32_t> hashes(1, 0);
    ASSERT_THROW(block.get_by_position(0).column->update_identity_hash(hashes.data(), 8),
                 doris::Exception);
}

} // namespace doris
