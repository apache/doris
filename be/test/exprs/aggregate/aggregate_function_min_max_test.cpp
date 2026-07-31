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

#include "core/arena.h"
#include "core/column/column_string.h"
#include "core/string_buffer.hpp"

namespace doris {

class SingleValueDataStringTest : public testing::Test {
protected:
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

TEST_F(SingleValueDataStringTest, SmallStringChangeImpl) {
    SingleValueDataString data;
    std::string small = "hello";
    data.change_impl(StringRef(small.data(), small.size()), arena);
    ASSERT_TRUE(data.has());
    auto ref = data.get_string_ref();
    ASSERT_EQ(ref.size, small.size());
    ASSERT_EQ(std::string(ref.data, ref.size), small);
}

TEST_F(SingleValueDataStringTest, LargeStringChangeImpl) {
    SingleValueDataString data;
    // Create a string larger than MAX_SMALL_STRING_SIZE
    std::string large(SingleValueDataString::MAX_SMALL_STRING_SIZE + 10, 'x');
    data.change_impl(StringRef(large.data(), large.size()), arena);
    ASSERT_TRUE(data.has());
    auto ref = data.get_string_ref();
    ASSERT_EQ(ref.size, large.size());
    ASSERT_EQ(std::string(ref.data, ref.size), large);
}

TEST_F(SingleValueDataStringTest, ResetAfterChange) {
    SingleValueDataString data;
    std::string s = "test";
    data.change_impl(StringRef(s.data(), s.size()), arena);
    ASSERT_TRUE(data.has());
    data.reset();
    ASSERT_FALSE(data.has());
}

TEST_F(SingleValueDataStringTest, ChangeIfLess) {
    SingleValueDataString data;
    std::string a = "banana";
    std::string b = "apple";

    data.change_impl(StringRef(a.data(), a.size()), arena);
    ASSERT_EQ(std::string(data.get_string_ref().data, data.get_string_ref().size), a);

    SingleValueDataString other;
    other.change_impl(StringRef(b.data(), b.size()), arena);

    ASSERT_TRUE(data.change_if_less(other, arena));
    ASSERT_EQ(std::string(data.get_string_ref().data, data.get_string_ref().size), b);

    // "apple" is not less than "apple"
    ASSERT_FALSE(data.change_if_less(other, arena));
}

TEST_F(SingleValueDataStringTest, ChangeIfGreater) {
    SingleValueDataString data;
    std::string a = "apple";
    std::string b = "banana";

    data.change_impl(StringRef(a.data(), a.size()), arena);

    SingleValueDataString other;
    other.change_impl(StringRef(b.data(), b.size()), arena);

    ASSERT_TRUE(data.change_if_greater(other, arena));
    ASSERT_EQ(std::string(data.get_string_ref().data, data.get_string_ref().size), b);

    ASSERT_FALSE(data.change_if_greater(other, arena));
}

TEST_F(SingleValueDataStringTest, ChangeFirstTime) {
    SingleValueDataString data;
    SingleValueDataString src;
    std::string s = "first";
    src.change_impl(StringRef(s.data(), s.size()), arena);

    data.change_first_time(src, arena);
    ASSERT_TRUE(data.has());
    ASSERT_EQ(std::string(data.get_string_ref().data, data.get_string_ref().size), s);

    // Second call should not change
    SingleValueDataString other;
    std::string s2 = "second";
    other.change_impl(StringRef(s2.data(), s2.size()), arena);
    data.change_first_time(other, arena);
    ASSERT_EQ(std::string(data.get_string_ref().data, data.get_string_ref().size), s);
}

TEST_F(SingleValueDataStringTest, WriteReadSmallString) {
    SingleValueDataString data;
    std::string s = "serialize_me";
    data.change_impl(StringRef(s.data(), s.size()), arena);

    // Write
    auto col_write = ColumnString::create();
    BufferWritable writer(*col_write);
    data.write(writer);
    writer.commit();

    // Read
    auto ref = col_write->get_data_at(0);
    BufferReadable reader(ref);
    SingleValueDataString data2;
    data2.read(reader, arena);

    ASSERT_TRUE(data2.has());
    ASSERT_EQ(std::string(data2.get_string_ref().data, data2.get_string_ref().size), s);
}

TEST_F(SingleValueDataStringTest, WriteReadLargeString) {
    SingleValueDataString data;
    std::string s(SingleValueDataString::MAX_SMALL_STRING_SIZE + 20, 'L');
    data.change_impl(StringRef(s.data(), s.size()), arena);

    auto col_write = ColumnString::create();
    BufferWritable writer(*col_write);
    data.write(writer);
    writer.commit();

    auto ref = col_write->get_data_at(0);
    BufferReadable reader(ref);
    SingleValueDataString data2;
    data2.read(reader, arena);

    ASSERT_TRUE(data2.has());
    ASSERT_EQ(std::string(data2.get_string_ref().data, data2.get_string_ref().size), s);
}

TEST_F(SingleValueDataStringTest, WriteReadNoValue) {
    SingleValueDataString data;

    auto col_write = ColumnString::create();
    BufferWritable writer(*col_write);
    data.write(writer);
    writer.commit();

    auto ref = col_write->get_data_at(0);
    BufferReadable reader(ref);
    SingleValueDataString data2;
    data2.read(reader, arena);

    ASSERT_FALSE(data2.has());
}

TEST_F(SingleValueDataStringTest, InsertResultIntoWithValue) {
    SingleValueDataString data;
    std::string s = "result";
    data.change_impl(StringRef(s.data(), s.size()), arena);

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
    data.change_impl(StringRef(s1.data(), s1.size()), arena);
    ASSERT_EQ(std::string(data.get_string_ref().data, data.get_string_ref().size), s1);

    // Second larger allocation triggers realloc
    std::string s2(SingleValueDataString::MAX_SMALL_STRING_SIZE + 200, 'B');
    data.change_impl(StringRef(s2.data(), s2.size()), arena);
    ASSERT_EQ(std::string(data.get_string_ref().data, data.get_string_ref().size), s2);
}

TEST_F(SingleValueDataStringTest, SizeStaticAssert) {
    static_assert(sizeof(SingleValueDataString) == SingleValueDataString::AUTOMATIC_STORAGE_SIZE);
}

} // namespace doris
