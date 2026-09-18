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
#include "core/data_type/data_type_decimal.h"

namespace doris {

class PartitionTransformersTest : public testing::Test {
public:
    PartitionTransformersTest() = default;
    virtual ~PartitionTransformersTest() = default;
};

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

// The expected values below were produced with the Apache Iceberg reference implementation
// (iceberg-api 1.10.1, the version fe/pom.xml depends on) via DateTimeUtil / Transforms /
// BucketUtil. They pin two spec requirements that Doris used to violate:
//   * the day ordinal is proleptic Gregorian, in which year 0 IS a leap year, so 0000-01-01 is
//     -719528 and not the -719527 that Doris's MySQL-calendar `daynr()` implies;
//   * `day` and `hour` floor towards negative infinity rather than truncating towards the epoch.
namespace {

ColumnWithTypeAndName make_date_column(const std::vector<std::tuple<int, int, int>>& dates,
                                       ColumnDateV2::MutablePtr& column) {
    auto& data = column->get_data();
    for (const auto& [y, m, d] : dates) {
        DateV2Value<DateV2ValueType> value;
        value.unchecked_set_time(y, m, d, 0, 0, 0, 0);
        data.push_back(*reinterpret_cast<UInt32*>(&value));
    }
    return {column->get_ptr(), std::make_shared<DataTypeDateV2>(), "test_date"};
}

// The microsecond field is part of the tuple on purpose: Iceberg's timestamp transforms are
// defined on the full microsecond value, so a test that only ever passes 0 cannot tell flooring
// from truncation, nor a bucket that keeps the sub-second part from one that drops it.
ColumnWithTypeAndName make_timestamp_column(
        const std::vector<std::tuple<int, int, int, int, int, int, int>>& timestamps,
        ColumnDateTimeV2::MutablePtr& column) {
    auto& data = column->get_data();
    for (const auto& [y, mo, d, h, mi, se, us] : timestamps) {
        DateV2Value<DateTimeV2ValueType> value;
        value.unchecked_set_time(y, mo, d, h, mi, se, us);
        data.push_back(*reinterpret_cast<UInt64*>(&value));
    }
    return {column->get_ptr(), std::make_shared<DataTypeDateTimeV2>(), "test_timestamp"};
}

} // namespace

TEST_F(PartitionTransformersTest, test_date_day_transform_proleptic_gregorian) {
    auto column = ColumnDateV2::create();
    auto test_date = make_date_column({{0, 1, 1},
                                       {0, 2, 28},
                                       {0, 3, 1},
                                       {1, 1, 1},
                                       {1969, 6, 15},
                                       {1969, 12, 31},
                                       {1970, 1, 1},
                                       {2017, 11, 16},
                                       {9999, 12, 31}},
                                      column);

    Block block({test_date});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATEV2, false);
    DateDayPartitionColumnTransform transform(source_type);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {-719528, -719470, -719468, -719162, -200,
                                          -1,      0,       17486,   2932896};
    std::vector<std::string> expected_human_string = {"0000-01-01", "0000-02-28", "0000-03-01",
                                                      "0001-01-01", "1969-06-15", "1969-12-31",
                                                      "1970-01-01", "2017-11-16", "9999-12-31"};
    ASSERT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]) << "row " << i;
        EXPECT_EQ(expected_human_string[i],
                  transform.to_human_string(transform.get_result_type(), result_data[i]));
    }
}

TEST_F(PartitionTransformersTest, test_timestamp_day_transform_floors_before_epoch) {
    auto column = ColumnDateTimeV2::create();
    auto test_timestamp = make_timestamp_column({{0, 1, 1, 0, 0, 0, 0},
                                                 {0, 1, 1, 12, 34, 56, 0},
                                                 {0, 2, 28, 0, 0, 0, 0},
                                                 {0, 2, 28, 23, 59, 59, 999999},
                                                 {1969, 12, 31, 0, 0, 0, 0},
                                                 {1969, 12, 31, 12, 0, 0, 0},
                                                 {1969, 12, 31, 23, 59, 59, 0},
                                                 {1969, 12, 31, 23, 59, 59, 999999},
                                                 {1970, 1, 1, 0, 0, 0, 0},
                                                 {2017, 11, 16, 22, 31, 8, 0}},
                                                column);

    Block block({test_timestamp});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIMEV2, false);
    TimestampDayPartitionColumnTransform transform(source_type);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    // Every 1969-12-31 timestamp must land on -1: Iceberg floors, it does not round towards the
    // epoch the way SQL DATEDIFF does. The last microsecond of a day belongs to that same day.
    std::vector<int32_t> expected_data = {-719528, -719528, -719470, -719470, -1,
                                          -1,      -1,      -1,      0,       17486};
    ASSERT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]) << "row " << i;
    }
}

TEST_F(PartitionTransformersTest, test_timestamp_hour_transform_floors_before_epoch) {
    auto column = ColumnDateTimeV2::create();
    auto test_timestamp = make_timestamp_column({{0, 1, 1, 0, 0, 0, 0},
                                                 {0, 1, 1, 12, 34, 56, 0},
                                                 {0, 2, 28, 0, 0, 0, 0},
                                                 {0, 2, 28, 23, 59, 59, 999999},
                                                 {1, 1, 1, 0, 0, 0, 0},
                                                 {1969, 6, 15, 10, 0, 0, 0},
                                                 {1969, 12, 31, 0, 0, 0, 0},
                                                 {1969, 12, 31, 12, 0, 0, 0},
                                                 {1969, 12, 31, 23, 30, 0, 0},
                                                 {1969, 12, 31, 23, 59, 59, 999999},
                                                 {1970, 1, 1, 0, 0, 0, 0},
                                                 {1970, 1, 1, 12, 0, 0, 0},
                                                 {2017, 11, 16, 22, 31, 8, 0},
                                                 {9999, 12, 31, 23, 59, 59, 999999}},
                                                column);

    Block block({test_timestamp});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIMEV2, false);
    TimestampHourPartitionColumnTransform transform(source_type);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    std::vector<int32_t> expected_data = {-17268672, -17268660, -17267280, -17267257, -17259888,
                                          -4790,     -24,       -12,       -1,        -1,
                                          0,         12,        419686,    70389527};
    // The partition path must floor as well: hour ordinal -1 is 1969-12-31-23.
    std::vector<std::string> expected_human_string = {
            "0000-01-01-00", "0000-01-01-12", "0000-02-28-00", "0000-02-28-23", "0001-01-01-00",
            "1969-06-15-10", "1969-12-31-00", "1969-12-31-12", "1969-12-31-23", "1969-12-31-23",
            "1970-01-01-00", "1970-01-01-12", "2017-11-16-22", "9999-12-31-23"};
    ASSERT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]) << "row " << i;
        EXPECT_EQ(expected_human_string[i],
                  transform.to_human_string(transform.get_result_type(), result_data[i]))
                << "row " << i;
    }
}

TEST_F(PartitionTransformersTest, test_date_bucket_transform_year_zero) {
    auto column = ColumnDateV2::create();
    auto test_date = make_date_column({{0, 1, 1}, {0, 2, 28}, {0, 3, 1}, {2017, 11, 16}}, column);

    Block block({test_date});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATEV2, false);
    DateBucketPartitionColumnTransform transform(source_type, 16);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    // Buckets for the proleptic day ordinals -719528, -719470, -719468 and 17486. Hashing the
    // Doris daynr instead would put 0000-01-01 in bucket 6.
    std::vector<int32_t> expected_data = {1, 0, 7, 10};
    ASSERT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]) << "row " << i;
    }
}

TEST_F(PartitionTransformersTest, test_date_year_month_transform_floors_before_epoch) {
    auto column = ColumnDateV2::create();
    auto test_date = make_date_column({{0, 1, 1},
                                       {0, 2, 28},
                                       {0, 3, 1},
                                       {1, 1, 1},
                                       {1899, 12, 31},
                                       {1969, 6, 15},
                                       {1969, 12, 31},
                                       {1970, 1, 1},
                                       {2024, 2, 29},
                                       {9999, 12, 31}},
                                      column);
    Block block({test_date});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATEV2, false);

    {
        DateYearPartitionColumnTransform transform(source_type);
        auto result = transform.apply(block, 0);
        const auto& data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
        // Whole calendar years from 1970, floored. Rounding towards zero would report 0 for
        // 1969-06-15 and -1969 for 0000-02-28.
        std::vector<int32_t> expected = {-1970, -1970, -1970, -1969, -71, -1, -1, 0, 54, 8029};
        // iceberg-api's TransformUtil.humanYear zero-pads to four digits, so the partition
        // directory of a year-zero row is `..._year=0000`, not `..._year=0`.
        std::vector<std::string> expected_human_string = {"0000", "0000", "0000", "0001", "1899",
                                                          "1969", "1969", "1970", "2024", "9999"};
        ASSERT_EQ(expected.size(), data.size());
        for (size_t i = 0; i < data.size(); ++i) {
            EXPECT_EQ(expected[i], data[i]) << "row " << i;
            EXPECT_EQ(expected_human_string[i],
                      transform.to_human_string(transform.get_result_type(), data[i]))
                    << "row " << i;
        }
    }
    {
        DateMonthPartitionColumnTransform transform(source_type);
        auto result = transform.apply(block, 0);
        const auto& data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
        std::vector<int32_t> expected = {-23640, -23639, -23638, -23628, -841,
                                         -7,     -1,     0,      649,    96359};
        ASSERT_EQ(expected.size(), data.size());
        for (size_t i = 0; i < data.size(); ++i) {
            EXPECT_EQ(expected[i], data[i]) << "row " << i;
        }
    }
}

TEST_F(PartitionTransformersTest, test_timestamp_year_month_transform_floors_before_epoch) {
    auto column = ColumnDateTimeV2::create();
    auto test_timestamp = make_timestamp_column({{0, 1, 1, 12, 34, 56, 0},
                                                 {0, 2, 28, 0, 0, 0, 0},
                                                 {1, 1, 1, 0, 0, 0, 0},
                                                 {1969, 6, 15, 10, 0, 0, 0},
                                                 {1969, 12, 31, 23, 59, 59, 999999},
                                                 {1970, 1, 1, 0, 0, 0, 0},
                                                 {2024, 1, 1, 12, 0, 0, 0},
                                                 {9999, 12, 31, 23, 59, 59, 999999}},
                                                column);
    Block block({test_timestamp});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIMEV2, false);

    {
        TimestampYearPartitionColumnTransform transform(source_type);
        auto result = transform.apply(block, 0);
        const auto& data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
        std::vector<int32_t> expected = {-1970, -1970, -1969, -1, -1, 0, 54, 8029};
        ASSERT_EQ(expected.size(), data.size());
        for (size_t i = 0; i < data.size(); ++i) {
            EXPECT_EQ(expected[i], data[i]) << "row " << i;
        }
    }
    {
        TimestampMonthPartitionColumnTransform transform(source_type);
        auto result = transform.apply(block, 0);
        const auto& data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
        std::vector<int32_t> expected = {-23640, -23639, -23628, -7, -1, 0, 648, 96359};
        ASSERT_EQ(expected.size(), data.size());
        for (size_t i = 0; i < data.size(); ++i) {
            EXPECT_EQ(expected[i], data[i]) << "row " << i;
        }
    }
}

// The exact row the iceberg write regression suite stores: before the fix, 1969-12-31 23:59:59 and
// 1970-01-01 00:00:00 collapsed into the same day and hour partition.
TEST_F(PartitionTransformersTest, test_epoch_boundary_rows_land_in_distinct_partitions) {
    auto column = ColumnDateTimeV2::create();
    auto test_timestamp = make_timestamp_column({{1969, 12, 31, 23, 59, 59, 999999},
                                                 {1970, 1, 1, 0, 0, 0, 0},
                                                 {2024, 2, 29, 12, 34, 56, 123456}},
                                                column);
    Block block({test_timestamp});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIMEV2, false);

    TimestampDayPartitionColumnTransform day_transform(source_type);
    // Keep the result alive: it owns the ColumnPtr the data reference points into.
    auto day_result = day_transform.apply(block, 0);
    const auto& days = assert_cast<const ColumnInt32*>(day_result.column.get())->get_data();
    EXPECT_EQ(-1, days[0]);
    EXPECT_EQ(0, days[1]);
    EXPECT_EQ(19782, days[2]);
    EXPECT_NE(days[0], days[1]) << "before-epoch and epoch rows must not share a day partition";
    EXPECT_EQ("1969-12-31",
              day_transform.to_human_string(day_transform.get_result_type(), days[0]));

    TimestampHourPartitionColumnTransform hour_transform(source_type);
    auto hour_result = hour_transform.apply(block, 0);
    const auto& hours = assert_cast<const ColumnInt32*>(hour_result.column.get())->get_data();
    EXPECT_EQ(-1, hours[0]);
    EXPECT_EQ(0, hours[1]);
    EXPECT_EQ(474780, hours[2]);
    EXPECT_NE(hours[0], hours[1]) << "before-epoch and epoch rows must not share an hour partition";
    EXPECT_EQ("1969-12-31-23",
              hour_transform.to_human_string(hour_transform.get_result_type(), hours[0]));
}

// Iceberg buckets a timestamp by hashing its full microsecond value (spec: Partition Transforms;
// iceberg-api 1.10.1 `Bucket.BucketLong` over `BucketUtil.hash(long)`). Doris used to hash whole
// seconds times a million, so a DATETIME(6) row landed in a different bucket than the same row
// written by Spark, and bucket pruning on it skipped the Doris-written file.
TEST_F(PartitionTransformersTest, test_timestamp_bucket_transform_keeps_microseconds) {
    auto column = ColumnDateTimeV2::create();
    auto test_timestamp = make_timestamp_column({{2024, 2, 29, 12, 34, 56, 123456},
                                                 {2024, 2, 29, 12, 34, 56, 0},
                                                 {1969, 12, 31, 23, 59, 59, 999999},
                                                 {1969, 12, 31, 23, 59, 59, 0},
                                                 {0, 1, 1, 12, 34, 56, 654321},
                                                 {1970, 1, 1, 0, 0, 0, 0}},
                                                column);

    Block block({test_timestamp});
    auto source_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_DATETIMEV2, false);
    TimestampBucketPartitionColumnTransform transform(source_type, 16);

    auto result = transform.apply(block, 0);

    const auto& result_data = assert_cast<const ColumnInt32*>(result.column.get())->get_data();
    // Buckets of the micros-since-epoch values 1709210096123456, 1709210096000000, -1, -1000000,
    // -62167173903345679 and 0. Truncating to whole seconds would report 12, 12, 15, 15, 8 and 12,
    // i.e. rows 0, 2 and 4 would collide with (or move onto) their truncated twins.
    std::vector<int32_t> expected_data = {8, 12, 8, 15, 12, 12};
    ASSERT_EQ(expected_data.size(), result_data.size());
    for (size_t i = 0; i < result_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], result_data[i]) << "row " << i;
    }
    EXPECT_NE(result_data[0], result_data[1])
            << "a sub-second timestamp must not share a bucket with its truncated value";
    EXPECT_NE(result_data[2], result_data[3])
            << "a sub-second timestamp must not share a bucket with its truncated value";
}

} // namespace doris
