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

#include "exprs/aggregate/aggregate_function_min_max.h"

#include <gtest/gtest.h>

#include <string>

#include "agent/be_exec_version_manager.h"
#include "core/arena.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"

namespace doris {

class SingleValueDataStringTest : public testing::Test {
protected:
    void set_value(SingleValueDataString& data, const std::string& value) {
        auto column = ColumnString::create();
        column->insert_data(value.data(), value.size());
        data.set(*column, 0, arena);
    }

    std::string get_value(const SingleValueDataString& data) {
        auto column = ColumnString::create();
        data.insert_result_into(*column);
        return column->get_data_at(0).to_string();
    }

    Arena arena;
};

TEST_F(SingleValueDataStringTest, DefaultNotHas) {
    SingleValueDataString data;
    ASSERT_FALSE(data.has());
}

TEST_F(SingleValueDataStringTest, ResetWhenNoValue) {
    SingleValueDataString data;
    // reset on empty should not crash
    data.reset();
    ASSERT_FALSE(data.has());
}

TEST_F(SingleValueDataStringTest, SetSmallString) {
    SingleValueDataString data;
    std::string small = "hello";
    set_value(data, small);
    ASSERT_TRUE(data.has());
    ASSERT_EQ(get_value(data), small);
}

TEST_F(SingleValueDataStringTest, SetLargeString) {
    SingleValueDataString data;
    // Create a string larger than MAX_SMALL_STRING_SIZE
    std::string large(SingleValueDataString::MAX_SMALL_STRING_SIZE + 10, 'x');
    set_value(data, large);
    ASSERT_TRUE(data.has());
    ASSERT_EQ(get_value(data), large);
}

TEST_F(SingleValueDataStringTest, ResetAfterSet) {
    SingleValueDataString data;
    std::string s = "test";
    set_value(data, s);
    ASSERT_TRUE(data.has());
    data.reset();
    ASSERT_FALSE(data.has());
}

TEST_F(SingleValueDataStringTest, SetIfSmaller) {
    SingleValueDataString data;
    std::string a = "banana";
    std::string b = "apple";

    set_value(data, a);
    ASSERT_EQ(get_value(data), a);

    SingleValueDataString other;
    set_value(other, b);

    ASSERT_TRUE(data.set_if_smaller(other, arena));
    ASSERT_EQ(get_value(data), b);

    // "apple" is not less than "apple"
    ASSERT_FALSE(data.set_if_smaller(other, arena));
}

TEST_F(SingleValueDataStringTest, SetIfGreater) {
    SingleValueDataString data;
    std::string a = "apple";
    std::string b = "banana";

    set_value(data, a);

    SingleValueDataString other;
    set_value(other, b);

    ASSERT_TRUE(data.set_if_greater(other, arena));
    ASSERT_EQ(get_value(data), b);

    ASSERT_FALSE(data.set_if_greater(other, arena));
}

TEST_F(SingleValueDataStringTest, SetFromState) {
    SingleValueDataString data;
    SingleValueDataString src;
    std::string s = "first";
    set_value(src, s);

    data.set(src, arena);
    ASSERT_TRUE(data.has());
    ASSERT_EQ(get_value(data), s);

    SingleValueDataString other;
    std::string s2 = "second";
    set_value(other, s2);
    data.set(other, arena);
    ASSERT_EQ(get_value(data), s2);
}

TEST_F(SingleValueDataStringTest, WriteReadSmallString) {
    SingleValueDataString data;
    std::string s = "serialize_me";
    set_value(data, s);

    // Write
    auto col_write = ColumnString::create();
    BufferWritable writer(*col_write);
    auto data_type = std::make_shared<DataTypeString>();
    data.write(writer, data_type, -1);
    writer.commit();

    // Read
    auto ref = col_write->get_data_at(0);
    BufferReadable reader(ref);
    SingleValueDataString data2;
    data2.read(reader, data_type, -1, arena);

    ASSERT_TRUE(data2.has());
    ASSERT_EQ(get_value(data2), s);
}

TEST_F(SingleValueDataStringTest, WriteReadLargeString) {
    SingleValueDataString data;
    std::string s(SingleValueDataString::MAX_SMALL_STRING_SIZE + 20, 'L');
    set_value(data, s);

    auto col_write = ColumnString::create();
    BufferWritable writer(*col_write);
    auto data_type = std::make_shared<DataTypeString>();
    data.write(writer, data_type, -1);
    writer.commit();

    auto ref = col_write->get_data_at(0);
    BufferReadable reader(ref);
    SingleValueDataString data2;
    data2.read(reader, data_type, -1, arena);

    ASSERT_TRUE(data2.has());
    ASSERT_EQ(get_value(data2), s);
}

TEST_F(SingleValueDataStringTest, WriteReadNoValue) {
    SingleValueDataString data;

    auto col_write = ColumnString::create();
    BufferWritable writer(*col_write);
    auto data_type = std::make_shared<DataTypeString>();
    data.write(writer, data_type, -1);
    writer.commit();

    auto ref = col_write->get_data_at(0);
    BufferReadable reader(ref);
    SingleValueDataString data2;
    data2.read(reader, data_type, -1, arena);

    ASSERT_FALSE(data2.has());
}

TEST_F(SingleValueDataStringTest, InsertResultIntoWithValue) {
    SingleValueDataString data;
    std::string s = "result";
    set_value(data, s);

    auto col = ColumnString::create();
    data.insert_result_into(*col);
    ASSERT_EQ(col->size(), 1);
    auto ref = col->get_data_at(0);
    ASSERT_EQ(std::string(ref.data, ref.size), s);
}

TEST_F(SingleValueDataStringTest, InsertResultIntoWithoutValue) {
    SingleValueDataString data;
    auto col = ColumnString::create();
    data.insert_result_into(*col);
    ASSERT_EQ(col->size(), 1);
    // Default is empty string
    auto ref = col->get_data_at(0);
    ASSERT_EQ(ref.size, 0);
}

TEST_F(SingleValueDataStringTest, LargeStringRealloc) {
    SingleValueDataString data;
    // First large allocation
    std::string s1(SingleValueDataString::MAX_SMALL_STRING_SIZE + 10, 'A');
    set_value(data, s1);
    ASSERT_EQ(get_value(data), s1);

    // Second larger allocation triggers realloc
    std::string s2(SingleValueDataString::MAX_SMALL_STRING_SIZE + 200, 'B');
    set_value(data, s2);
    ASSERT_EQ(get_value(data), s2);
}

TEST_F(SingleValueDataStringTest, SizeStaticAssert) {
    static_assert(sizeof(SingleValueDataString) == SingleValueDataString::AUTOMATIC_STORAGE_SIZE);
}

TEST(SingleValueDataColumnTest, OwnsValueAndCreatesColumnLazily) {
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    SingleValueDataColumn data;
    EXPECT_FALSE(data.has());
    EXPECT_EQ(data.allocated_bytes(), 0);

    auto source = array_type->create_column();
    source->insert(Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2)}));
    Arena arena;
    data.set(*source, 0, arena);
    ASSERT_TRUE(data.has());

    source->clear();
    auto result = array_type->create_column();
    data.insert_result_into(*result);
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ((*result)[0],
              Field::create_field<TYPE_ARRAY>(
                      Array {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2)}));
}

TEST(SingleValueDataColumnTest, AllocationDoesNotScaleWithSourceRows) {
    constexpr size_t large_source_rows = 4096;
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    auto create_source = [&](size_t rows) {
        auto source = array_type->create_column();
        const auto value = Field::create_field<TYPE_ARRAY>(
                Array {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2)});
        for (size_t i = 0; i < rows; ++i) {
            source->insert(value);
        }
        return source;
    };

    auto single_row_source = create_source(1);
    auto large_source = create_source(large_source_rows);
    ASSERT_GT(large_source->allocated_bytes(), single_row_source->allocated_bytes());

    Arena arena;
    SingleValueDataColumn single_row_state;
    single_row_state.set(*single_row_source, 0, arena);
    SingleValueDataColumn large_source_state;
    large_source_state.set(*large_source, large_source_rows - 1, arena);

    EXPECT_EQ(large_source_state.allocated_bytes(), single_row_state.allocated_bytes());
    EXPECT_LT(large_source_state.allocated_bytes(), large_source->allocated_bytes());

    auto result = array_type->create_column();
    large_source_state.insert_result_into(*result);
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ((*result)[0], (*large_source)[large_source_rows - 1]);
}

TEST(SingleValueDataColumnTest, CompareCopyAndSerialize) {
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    const int be_exec_version = BeExecVersionManager::get_newest_version();
    auto source = array_type->create_column();
    source->insert(Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_INT>(2)}));
    source->insert(Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_INT>(1)}));

    Arena arena;
    SingleValueDataColumn data;
    EXPECT_TRUE(data.set_if_smaller(*source, 0, arena));
    EXPECT_TRUE(data.set_if_smaller(*source, 1, arena));
    EXPECT_TRUE(data.is_equal_to(*source, 1));

    SingleValueDataColumn copied;
    copied.set(data, arena);
    data.reset();
    ASSERT_TRUE(copied.has());

    auto serialized = ColumnString::create();
    BufferWritable writer(*serialized);
    copied.write(writer, array_type, be_exec_version);
    writer.commit();

    SingleValueDataColumn restored;
    BufferReadable reader(serialized->get_data_at(0));
    restored.read(reader, array_type, be_exec_version, arena);
    auto result = array_type->create_column();
    restored.insert_result_into(*result);
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ((*result)[0],
              Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_INT>(1)}));
}

} // namespace doris
