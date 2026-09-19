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

#include "exec/sink/writer/iceberg/partition_transformers.h"

#include <gtest/gtest.h>

#include <limits>

#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_varbinary.h"
#include "format/table/iceberg/partition_spec.h"

namespace doris {

class PartitionTransformersTest : public testing::Test {
public:
    PartitionTransformersTest() = default;
    virtual ~PartitionTransformersTest() = default;
};

TEST_F(PartitionTransformersTest, binary_transforms_preserve_raw_bytes) {
    auto type = std::make_shared<DataTypeVarbinary>();
    auto column = type->create_column();
    const std::vector<std::string> values = {std::string("\xC3\xA9\0\xFF", 4), "", "abc"};
    for (const auto& value : values) {
        column->insert_data(value.data(), value.size());
    }
    Block block({{column->get_ptr(), type, "binary_key"}});
    auto truncate = PartitionColumnTransforms::create(
            iceberg::PartitionField(1, 1000, "key_prefix", "truncate[1]"), type);
    auto truncated = truncate->apply(block, 0);
    // Binary truncation counts bytes, not UTF-8 code points, and keeps its physical type.
    EXPECT_EQ(TYPE_VARBINARY, truncated.type->get_primitive_type());
    EXPECT_EQ(std::string("\xC3", 1), truncated.column->get_data_at(0).to_string());
    EXPECT_EQ("", truncated.column->get_data_at(1).to_string());
    EXPECT_EQ("a", truncated.column->get_data_at(2).to_string());

    auto bucket = PartitionColumnTransforms::create(
            iceberg::PartitionField(1, 1001, "key_bucket", "bucket[16]"), type);
    auto bucketed = bucket->apply(block, 0);
    const auto& buckets = assert_cast<const ColumnInt32&>(*bucketed.column).get_data();
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(
                (HashUtil::murmur_hash3_32(values[i].data(), values[i].size(), 0) & INT32_MAX) % 16,
                buckets[i]);
    }
    IdentityPartitionColumnTransform identity(type);
    EXPECT_EQ("0xc3a900ff", identity.get_partition_value(type, values[0]));
    auto null_map = ColumnUInt8::create();
    null_map->get_data().assign({0, 1, 0});
    Block nullable_block(
            {{ColumnNullable::create(block.get_by_position(0).column, std::move(null_map)),
              make_nullable(type), "binary_key"}});
    for (auto* transform : {truncate.get(), bucket.get()}) {
        auto result = transform->apply(nullable_block, 0);
        EXPECT_TRUE(result.column->is_null_at(1));
        EXPECT_FALSE(result.column->is_null_at(0));
    }
}

TEST_F(PartitionTransformersTest, timestamp_transforms_use_utc_calendar_and_microseconds) {
    for (const auto& type : std::vector<DataTypePtr> {std::make_shared<DataTypeDateTimeV2>(6),
                                                      std::make_shared<DataTypeTimeStampTz>(6)}) {
        auto column = type->create_column();
        // The first two UTC values represent the repeated New York 01:30 with different offsets.
        const std::vector<std::array<int, 7>> fields = {{2021, 11, 7, 5, 30, 0, 123456},
                                                        {2021, 11, 7, 6, 30, 0, 123456},
                                                        {1969, 12, 31, 23, 59, 59, 999999}};
        const std::vector<Int64> micros = {1636263000123456, 1636266600123456, -1};
        for (const auto& f : fields) {
            DateV2Value<DateTimeV2ValueType> dt;
            ASSERT_TRUE(dt.check_range_and_set_time(f[0], f[1], f[2], f[3], f[4], f[5], f[6]));
            auto packed = dt.to_date_int_val();
            column->insert_data(reinterpret_cast<const char*>(&packed), sizeof(packed));
        }
        column->insert_default();
        auto null_map = ColumnUInt8::create();
        null_map->get_data().assign({0, 0, 0, 1});
        Block block({{ColumnNullable::create(std::move(column), std::move(null_map)),
                      make_nullable(type), "event_time"}});
        const std::vector<std::string> transforms = {"year", "month", "day", "hour", "bucket[16]"};
        const std::vector<std::vector<Int32>> expected = {
                {51, 51, -1}, {622, 622, -1}, {18938, 18938, -1}, {454517, 454518, -1}};
        for (size_t t = 0; t < transforms.size(); ++t) {
            SCOPED_TRACE(type->get_name() + " " + transforms[t]);
            auto transform = PartitionColumnTransforms::create(
                    iceberg::PartitionField(1, 1000, "event_partition", transforms[t]), type);
            auto result = transform->apply(block, 0);
            ASSERT_TRUE(result.column->is_null_at(3));
            const auto& values =
                    assert_cast<const ColumnInt32&>(
                            assert_cast<const ColumnNullable&>(*result.column).get_nested_column())
                            .get_data();
            for (size_t row = 0; row < micros.size(); ++row) {
                Int32 expected_value =
                        t < 4 ? expected[t][row]
                              : (HashUtil::murmur_hash3_32(&micros[row], sizeof(Int64), 0) &
                                 INT32_MAX) %
                                        16;
                EXPECT_EQ(expected_value, values[row]);
            }
            if (transforms[t] == "hour") {
                EXPECT_EQ("1969-12-31-23", transform->to_human_string(result.type, Int32(-1)));
            }
        }
    }
}

TEST_F(PartitionTransformersTest, date_partitions_before_epoch_use_calendar_ordinals) {
    auto type = std::make_shared<DataTypeDateV2>();
    auto column = ColumnDateV2::create();
    DateV2Value<DateV2ValueType> value;
    value.unchecked_set_time(1969, 12, 31, 0, 0, 0);
    column->insert_value(value.to_date_int_val());
    Block block({{std::move(column), type, "event_date"}});
    for (const auto& name : {"year", "month", "day"}) {
        auto transform = PartitionColumnTransforms::create(
                iceberg::PartitionField(1, 1000, "date_partition", name), type);
        auto result = transform->apply(block, 0);
        EXPECT_EQ(-1, assert_cast<const ColumnInt32&>(*result.column).get_data()[0]);
    }
}

TEST_F(PartitionTransformersTest, test_integer_truncate_transform) {
    const std::vector<int32_t> values({1, -1});
    auto column = ColumnInt32::create();
    column->insert_many_fix_len_data(reinterpret_cast<const char*>(values.data()), values.size());
    ColumnWithTypeAndName test_int(column->get_ptr(), std::make_shared<DataTypeInt32>(),
                                   "test_int");

    Block block({test_int});
    auto source_type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, false);
    IntegerTruncatePartitionColumnTransform transform(source_type, 10);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {0, -10};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
    }
}

TEST_F(PartitionTransformersTest, test_bigint_truncate_transform) {
    const std::vector<int64_t> values({1, -1});
    auto column = ColumnInt64::create();
    column->insert_many_fix_len_data(reinterpret_cast<const char*>(values.data()), values.size());
    ColumnWithTypeAndName test_bigint(column->get_ptr(), std::make_shared<DataTypeInt64>(),
                                      "test_bigint");

    Block block({test_bigint});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, false);
    BigintTruncatePartitionColumnTransform transform(source_type, 10);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt64*>(result.column.get())->get_data();
    std::vector<int64_t> expected_data = {0, -10};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
    }
}

TEST_F(PartitionTransformersTest, test_decimal32_truncate_transform) {
    const std::vector<int32_t> values({1065});
    auto column = ColumnDecimal32::create(0, 2);
    column->insert_many_fix_len_data(reinterpret_cast<const char*>(values.data()), values.size());
    ColumnWithTypeAndName test_decimal32(
            column->get_ptr(), std::make_shared<DataTypeDecimal32>(4, 2), "test_decimal32");

    Block block({test_decimal32});
    auto source_type = DataTypeFactory::instance().create_data_type(TYPE_DECIMAL32, false, 4, 2);
    DecimalTruncatePartitionColumnTransform<TYPE_DECIMAL32> transform(source_type, 50);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnDecimal32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {1050};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i].value);
    }
}

TEST_F(PartitionTransformersTest, test_string_truncate_transform) {
    const std::vector<StringRef> values({{"iceberg", sizeof("iceberg") - 1}});
    auto column = ColumnString::create();
    column->insert_many_strings(&values[0], values.size());
    ColumnWithTypeAndName test_string(column->get_ptr(), std::make_shared<DataTypeString>(),
                                      "test_string");

    Block block({test_string});
    auto source_type = DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
    StringTruncatePartitionColumnTransform transform(source_type, 3);

    auto result = transform.apply(block, 0);
    const auto result_column = assert_cast<const ColumnString*>(result.column.get());
    const char result_data[] = {'i', 'c', 'e'};
    std::vector<StringRef> expected_data = {
            {result_data, sizeof(result_data) / sizeof(result_data[0])}};
    EXPECT_EQ(expected_data.size(), result_column->size());
    for (size_t i = 0; i < result_column->size(); ++i) {
        EXPECT_EQ(expected_data[i], result_column->get_data_at(i));
    }
}

TEST_F(PartitionTransformersTest, test_floating_point_special_partition_value) {
    auto float_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_FLOAT, false);
    auto double_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DOUBLE, false);
    IdentityPartitionColumnTransform float_transform(float_type);
    IdentityPartitionColumnTransform double_transform(double_type);

    EXPECT_EQ("NaN", float_transform.get_partition_value(
                             float_type, std::numeric_limits<Float32>::quiet_NaN()));
    EXPECT_EQ("Infinity", float_transform.get_partition_value(
                                  float_type, std::numeric_limits<Float32>::infinity()));
    EXPECT_EQ("-Infinity", float_transform.get_partition_value(
                                   float_type, -std::numeric_limits<Float32>::infinity()));
    EXPECT_EQ("NaN", double_transform.get_partition_value(
                             double_type, std::numeric_limits<Float64>::quiet_NaN()));
    EXPECT_EQ("Infinity", double_transform.get_partition_value(
                                  double_type, std::numeric_limits<Float64>::infinity()));
    EXPECT_EQ("-Infinity", double_transform.get_partition_value(
                                   double_type, -std::numeric_limits<Float64>::infinity()));
}

TEST_F(PartitionTransformersTest, test_integer_bucket_transform) {
    const std::vector<int32_t> values({34, -123}); // 2017239379, -471378254
    auto column = ColumnInt32::create();
    column->insert_many_fix_len_data(reinterpret_cast<const char*>(values.data()), values.size());
    ColumnWithTypeAndName test_int(column->get_ptr(), std::make_shared<DataTypeInt32>(),
                                   "test_int");

    Block block({test_int});
    auto source_type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, false);
    IntBucketPartitionColumnTransform transform(source_type, 16);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {3, 2};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
    }
}

TEST_F(PartitionTransformersTest, test_bigint_bucket_transform) {
    const std::vector<int64_t> values({34, -123}); // 2017239379, -471378254
    auto column = ColumnInt64::create();
    column->insert_many_fix_len_data(reinterpret_cast<const char*>(values.data()), values.size());
    ColumnWithTypeAndName test_bigint(column->get_ptr(), std::make_shared<DataTypeInt64>(),
                                      "test_bigint");

    Block block({test_bigint});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BIGINT, false);
    BigintBucketPartitionColumnTransform transform(source_type, 16);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {3, 2};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
    }
}

TEST_F(PartitionTransformersTest, test_decimal32_bucket_transform) {
    const std::vector<int32_t> values({1420}); // -500754589
    auto column = ColumnDecimal32::create(0, 2);
    column->insert_many_fix_len_data(reinterpret_cast<const char*>(values.data()), values.size());
    ColumnWithTypeAndName test_decimal32(
            column->get_ptr(), std::make_shared<DataTypeDecimal32>(4, 2), "test_decimal32");

    Block block({test_decimal32});
    auto source_type = DataTypeFactory::instance().create_data_type(TYPE_DECIMAL32, false, 4, 2);
    DecimalBucketPartitionColumnTransform<TYPE_DECIMAL32> transform(source_type, 16);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {3};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
    }
}

TEST_F(PartitionTransformersTest, test_date_bucket_transform) {
    auto column = ColumnDateV2::create();
    auto& date_v2_data = column->get_data();
    DateV2Value<DateV2ValueType> value;
    value.unchecked_set_time(2017, 11, 16, 0, 0, 0, 0); // -653330422
    date_v2_data.push_back(*reinterpret_cast<UInt32*>(&value));
    ColumnWithTypeAndName test_date(column->get_ptr(), std::make_shared<DataTypeDateV2>(),
                                    "test_date");

    Block block({test_date});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATEV2, false);
    DateBucketPartitionColumnTransform transform(source_type, 16);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {10};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
    }
}

TEST_F(PartitionTransformersTest, test_timestamp_bucket_transform) {
    auto column = ColumnDateTimeV2::create();
    auto& datetime_v2_data = column->get_data();
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(2017, 11, 16, 22, 31, 8, 0); // -2047944441
    datetime_v2_data.push_back(*reinterpret_cast<UInt64*>(&value));
    ColumnWithTypeAndName test_timestamp(column->get_ptr(), std::make_shared<DataTypeDateTimeV2>(),
                                         "test_timestamp");

    Block block({test_timestamp});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIMEV2, false);
    TimestampBucketPartitionColumnTransform transform(source_type, 16);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {7};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
    }
}

TEST_F(PartitionTransformersTest, test_string_bucket_transform) {
    const std::vector<StringRef> values({{"iceberg", sizeof("iceberg") - 1}}); // 1210000089
    auto column = ColumnString::create();
    column->insert_many_strings(&values[0], values.size());
    ColumnWithTypeAndName test_string(column->get_ptr(), std::make_shared<DataTypeString>(),
                                      "test_string");

    Block block({test_string});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, false);
    StringBucketPartitionColumnTransform transform(source_type, 16);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {9};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
    }
}

TEST_F(PartitionTransformersTest, test_date_year_transform) {
    auto column = ColumnDateV2::create();
    auto& date_v2_data = column->get_data();
    DateV2Value<DateV2ValueType> value;
    value.unchecked_set_time(2017, 11, 16, 0, 0, 0, 0);
    date_v2_data.push_back(*reinterpret_cast<UInt32*>(&value));
    ColumnWithTypeAndName test_date(column->get_ptr(), std::make_shared<DataTypeDateV2>(),
                                    "test_date");

    Block block({test_date});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATEV2, false);
    DateYearPartitionColumnTransform transform(source_type);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {47};
    std::vector<std::string> expected_human_string = {"2017"};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
        EXPECT_EQ(expected_human_string[i],
                  transform.to_human_string(transform.get_result_type(), result_data[i]));
    }
}

TEST_F(PartitionTransformersTest, test_timestamp_year_transform) {
    auto column = ColumnDateTimeV2::create();
    auto& datetime_v2_data = column->get_data();
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(2017, 11, 16, 22, 31, 8, 0);
    datetime_v2_data.push_back(*reinterpret_cast<UInt64*>(&value));
    ColumnWithTypeAndName test_timestamp(column->get_ptr(), std::make_shared<DataTypeDateTimeV2>(),
                                         "test_timestamp");

    Block block({test_timestamp});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIMEV2, false);
    TimestampYearPartitionColumnTransform transform(source_type);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {47};
    std::vector<std::string> expected_human_string = {"2017"};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
        EXPECT_EQ(expected_human_string[i],
                  transform.to_human_string(transform.get_result_type(), result_data[i]));
    }
}

TEST_F(PartitionTransformersTest, test_date_month_transform) {
    auto column = ColumnDateV2::create();
    auto& date_v2_data = column->get_data();
    DateV2Value<DateV2ValueType> value;
    value.unchecked_set_time(2017, 11, 16, 0, 0, 0, 0);
    date_v2_data.push_back(*reinterpret_cast<UInt32*>(&value));
    ColumnWithTypeAndName test_date(column->get_ptr(), std::make_shared<DataTypeDateV2>(),
                                    "test_date");

    Block block({test_date});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATEV2, false);
    DateMonthPartitionColumnTransform transform(source_type);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {574};
    std::vector<std::string> expected_human_string = {"2017-11"};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
        EXPECT_EQ(expected_human_string[i],
                  transform.to_human_string(transform.get_result_type(), result_data[i]));
    }
}

TEST_F(PartitionTransformersTest, test_timestamp_month_transform) {
    auto column = ColumnDateTimeV2::create();
    auto& datetime_v2_data = column->get_data();
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(2017, 11, 16, 22, 31, 8, 0);
    datetime_v2_data.push_back(*reinterpret_cast<UInt64*>(&value));
    ColumnWithTypeAndName test_timestamp(column->get_ptr(), std::make_shared<DataTypeDateTimeV2>(),
                                         "test_timestamp");

    Block block({test_timestamp});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIMEV2, false);
    TimestampMonthPartitionColumnTransform transform(source_type);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {574};
    std::vector<std::string> expected_human_string = {"2017-11"};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
        EXPECT_EQ(expected_human_string[i],
                  transform.to_human_string(transform.get_result_type(), result_data[i]));
    }
}

TEST_F(PartitionTransformersTest, test_date_day_transform) {
    auto column = ColumnDateV2::create();
    auto& date_v2_data = column->get_data();
    DateV2Value<DateV2ValueType> value;
    value.unchecked_set_time(2017, 11, 16, 0, 0, 0, 0);
    date_v2_data.push_back(*reinterpret_cast<UInt32*>(&value));
    ColumnWithTypeAndName test_date(column->get_ptr(), std::make_shared<DataTypeDateV2>(),
                                    "test_date");

    Block block({test_date});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATEV2, false);
    DateDayPartitionColumnTransform transform(source_type);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {17486};
    std::vector<std::string> expected_human_string = {"2017-11-16"};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
        EXPECT_EQ(expected_human_string[i],
                  transform.to_human_string(transform.get_result_type(), result_data[i]));
    }
}

TEST_F(PartitionTransformersTest, test_timestamp_day_transform) {
    auto column = ColumnDateTimeV2::create();
    auto& datetime_v2_data = column->get_data();
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(2017, 11, 16, 22, 31, 8, 0);
    datetime_v2_data.push_back(*reinterpret_cast<UInt64*>(&value));
    ColumnWithTypeAndName test_timestamp(column->get_ptr(), std::make_shared<DataTypeDateTimeV2>(),
                                         "test_timestamp");

    Block block({test_timestamp});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIMEV2, false);
    TimestampDayPartitionColumnTransform transform(source_type);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {17486};
    std::vector<std::string> expected_human_string = {"2017-11-16"};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
        EXPECT_EQ(expected_human_string[i],
                  transform.to_human_string(transform.get_result_type(), result_data[i]));
    }
}

TEST_F(PartitionTransformersTest, test_timestamp_hour_transform) {
    auto column = ColumnDateTimeV2::create();
    auto& datetime_v2_data = column->get_data();
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(2017, 11, 16, 22, 31, 8, 0);
    datetime_v2_data.push_back(*reinterpret_cast<UInt64*>(&value));
    ColumnWithTypeAndName test_timestamp(column->get_ptr(), std::make_shared<DataTypeDateTimeV2>(),
                                         "test_timestamp");

    Block block({test_timestamp});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIMEV2, false);
    TimestampHourPartitionColumnTransform transform(source_type);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {419686};
    std::vector<std::string> expected_human_string = {"2017-11-16-22"};
    EXPECT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]);
        EXPECT_EQ(expected_human_string[i],
                  transform.to_human_string(transform.get_result_type(), result_data[i]));
    }
}

TEST_F(PartitionTransformersTest, test_void_transform) {
    const std::vector<int32_t> values({1, -1});
    auto column = ColumnInt32::create();
    column->insert_many_fix_len_data(reinterpret_cast<const char*>(values.data()), values.size());
    ColumnWithTypeAndName test_int(column->get_ptr(), std::make_shared<DataTypeInt32>(),
                                   "test_int");

    Block block({test_int});
    auto source_type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, false);
    VoidPartitionColumnTransform transform(source_type);

    auto result = transform.apply(block, 0);

    const auto& result_null_map_data =
            assert_cast<const ColumnNullable*>(result.column.get())->get_null_map_data();

    for (size_t i = 0; i < result_null_map_data.size(); ++i) {
        EXPECT_EQ(1, result_null_map_data[i]);
    }
}

TEST_F(PartitionTransformersTest, test_nullable_column_integer_truncate_transform) {
    const std::vector<int32_t> values({1, -1});
    auto column = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
    column->insert_data(nullptr, 0);
    column->insert_many_fix_len_data(reinterpret_cast<const char*>(values.data()), values.size());
    ColumnWithTypeAndName test_int(
            column->get_ptr(),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "test_int");

    Block block({test_int});
    auto source_type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, false);
    IntegerTruncatePartitionColumnTransform transform(source_type, 10);

    auto result = transform.apply(block, 0);

    std::vector<int32_t> expected_data = {0, -10};
    std::vector<std::string> expected_human_string = {"0", "-10"};
    const auto* result_column = assert_cast<const ColumnNullable*>(result.column.get());
    const auto& result_data =
            assert_cast<const ColumnInt32*>(result_column->get_nested_column_ptr().get())
                    ->get_data();
    const auto& null_map_column = result_column->get_null_map_column();

    EXPECT_EQ(Field::create_field<TYPE_BOOLEAN>(1), null_map_column[0]);
    EXPECT_EQ(Field::create_field<TYPE_BOOLEAN>(0), null_map_column[1]);
    EXPECT_EQ(Field::create_field<TYPE_BOOLEAN>(0), null_map_column[2]);

    for (size_t i = 0, j = 0; i < result_column->size(); ++i) {
        if (null_map_column[i] == Field::create_field<TYPE_BOOLEAN>(0)) {
            EXPECT_EQ(expected_data[j], result_data[i]);
            EXPECT_EQ(expected_human_string[j],
                      transform.to_human_string(transform.get_result_type(), result_data[i]));
            ++j;
        }
    }
}

TEST_F(PartitionTransformersTest, test_nullable_column_string_truncate_transform) {
    auto column = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    column->insert_data(nullptr, 0);
    column->insert_data("iceberg", sizeof("iceberg") - 1);
    column->insert_data("db", sizeof("db") - 1);
    ColumnWithTypeAndName test_string(
            column->get_ptr(),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "test_string");

    Block block({test_string});
    auto source_type = DataTypeFactory::instance().create_data_type(TYPE_STRING, true);
    StringTruncatePartitionColumnTransform transform(source_type, 3);

    auto result = transform.apply(block, 0);

    const auto* result_column = assert_cast<const ColumnNullable*>(result.column.get());
    const auto* result_strings =
            assert_cast<const ColumnString*>(result_column->get_nested_column_ptr().get());
    EXPECT_EQ(3, result_column->size());
    EXPECT_EQ(Field::create_field<TYPE_BOOLEAN>(1), result_column->get_null_map_column()[0]);
    EXPECT_EQ(Field::create_field<TYPE_BOOLEAN>(0), result_column->get_null_map_column()[1]);
    EXPECT_EQ(Field::create_field<TYPE_BOOLEAN>(0), result_column->get_null_map_column()[2]);
    EXPECT_EQ("ice", result_strings->get_data_at(1).to_string());
    EXPECT_EQ("db", result_strings->get_data_at(2).to_string());
}

} // namespace doris
