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

#include "vec/sink/writer/iceberg/partition_transformers.h"

#include <gtest/gtest.h>

#include "vec/data_types/data_type_time_v2.h"

namespace doris::vectorized {

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
    TypeDescriptor source_type(PrimitiveType::TYPE_INT);
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
    TypeDescriptor source_type(PrimitiveType::TYPE_BIGINT);
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
    ColumnWithTypeAndName test_decimal32(column->get_ptr(),
                                         std::make_shared<DataTypeDecimal<Decimal32>>(4, 2),
                                         "test_decimal32");

    Block block({test_decimal32});
    TypeDescriptor source_type = TypeDescriptor::create_decimalv3_type(4, 2);
    DecimalTruncatePartitionColumnTransform<Decimal32> transform(source_type, 50);

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
    TypeDescriptor source_type = TypeDescriptor::create_string_type();
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

TEST_F(PartitionTransformersTest, test_integer_bucket_transform) {
    const std::vector<int32_t> values({34, -123}); // 2017239379, -471378254
    auto column = ColumnInt32::create();
    column->insert_many_fix_len_data(reinterpret_cast<const char*>(values.data()), values.size());
    ColumnWithTypeAndName test_int(column->get_ptr(), std::make_shared<DataTypeInt32>(),
                                   "test_int");

    Block block({test_int});
    TypeDescriptor source_type(PrimitiveType::TYPE_INT);
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
    TypeDescriptor source_type(PrimitiveType::TYPE_BIGINT);
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
    ColumnWithTypeAndName test_decimal32(column->get_ptr(),
                                         std::make_shared<DataTypeDecimal<Decimal32>>(4, 2),
                                         "test_decimal32");

    Block block({test_decimal32});
    TypeDescriptor source_type = TypeDescriptor::create_decimalv3_type(4, 2);
    DecimalBucketPartitionColumnTransform<Decimal32> transform(source_type, 16);

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
    date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&value));
    ColumnWithTypeAndName test_date(column->get_ptr(), std::make_shared<DataTypeDateV2>(),
                                    "test_date");

    Block block({test_date});
    TypeDescriptor source_type(PrimitiveType::TYPE_DATEV2);
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
    datetime_v2_data.push_back(*reinterpret_cast<vectorized::UInt64*>(&value));
    ColumnWithTypeAndName test_timestamp(column->get_ptr(), std::make_shared<DataTypeDateTimeV2>(),
                                         "test_timestamp");

    Block block({test_timestamp});
    TypeDescriptor source_type(PrimitiveType::TYPE_DATETIMEV2);
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
    TypeDescriptor source_type(PrimitiveType::TYPE_STRING);
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
    date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&value));
    ColumnWithTypeAndName test_date(column->get_ptr(), std::make_shared<DataTypeDateV2>(),
                                    "test_date");

    Block block({test_date});
    TypeDescriptor source_type(PrimitiveType::TYPE_DATEV2);
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
    datetime_v2_data.push_back(*reinterpret_cast<vectorized::UInt64*>(&value));
    ColumnWithTypeAndName test_timestamp(column->get_ptr(), std::make_shared<DataTypeDateTimeV2>(),
                                         "test_timestamp");

    Block block({test_timestamp});
    TypeDescriptor source_type(PrimitiveType::TYPE_DATETIMEV2);
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
    date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&value));
    ColumnWithTypeAndName test_date(column->get_ptr(), std::make_shared<DataTypeDateV2>(),
                                    "test_date");

    Block block({test_date});
    TypeDescriptor source_type(PrimitiveType::TYPE_DATEV2);
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
    datetime_v2_data.push_back(*reinterpret_cast<vectorized::UInt64*>(&value));
    ColumnWithTypeAndName test_timestamp(column->get_ptr(), std::make_shared<DataTypeDateTimeV2>(),
                                         "test_timestamp");

    Block block({test_timestamp});
    TypeDescriptor source_type(PrimitiveType::TYPE_DATETIMEV2);
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
    date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&value));
    ColumnWithTypeAndName test_date(column->get_ptr(), std::make_shared<DataTypeDateV2>(),
                                    "test_date");

    Block block({test_date});
    TypeDescriptor source_type(PrimitiveType::TYPE_DATEV2);
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
    datetime_v2_data.push_back(*reinterpret_cast<vectorized::UInt64*>(&value));
    ColumnWithTypeAndName test_timestamp(column->get_ptr(), std::make_shared<DataTypeDateTimeV2>(),
                                         "test_timestamp");

    Block block({test_timestamp});
    TypeDescriptor source_type(PrimitiveType::TYPE_DATETIMEV2);
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
    datetime_v2_data.push_back(*reinterpret_cast<vectorized::UInt64*>(&value));
    ColumnWithTypeAndName test_timestamp(column->get_ptr(), std::make_shared<DataTypeDateTimeV2>(),
                                         "test_timestamp");

    Block block({test_timestamp});
    TypeDescriptor source_type(PrimitiveType::TYPE_DATETIMEV2);
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
    TypeDescriptor source_type(PrimitiveType::TYPE_INT);
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
    TypeDescriptor source_type(PrimitiveType::TYPE_INT);
    IntegerTruncatePartitionColumnTransform transform(source_type, 10);

    auto result = transform.apply(block, 0);

    std::vector<int32_t> expected_data = {0, -10};
    std::vector<std::string> expected_human_string = {"0", "-10"};
    const auto* result_column = assert_cast<const ColumnNullable*>(result.column.get());
    const auto& result_data =
            assert_cast<const ColumnInt32*>(result_column->get_nested_column_ptr().get())
                    ->get_data();
    const auto& null_map_column = result_column->get_null_map_column();

    EXPECT_EQ(1, null_map_column[0]);
    EXPECT_EQ(0, null_map_column[1]);
    EXPECT_EQ(0, null_map_column[2]);

    for (size_t i = 0, j = 0; i < result_column->size(); ++i) {
        if (null_map_column[i] == 0) {
            EXPECT_EQ(expected_data[j], result_data[i]);
            EXPECT_EQ(expected_human_string[j],
                      transform.to_human_string(transform.get_result_type(), result_data[i]));
            ++j;
        }
    }
}

} // namespace doris::vectorized
