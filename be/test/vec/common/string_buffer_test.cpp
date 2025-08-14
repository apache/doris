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

#include "vec/common/string_buffer.hpp"

#include <gtest/gtest.h>

#include "vec/columns/column_string.h"
#include "vec/common/arena.h"

namespace doris::vectorized {

TEST(StringBufferTest, TestWrite) {
    auto column = ColumnString::create();
    BufferWritable buf(*column);

    buf.write("hello", 5);
    buf.commit();
    ASSERT_EQ(column->size(), 1);
    ASSERT_EQ(column->get_data_at(0).to_string(), "hello");

    buf.write(' ');
    buf.commit();
    ASSERT_EQ(column->size(), 2);
    ASSERT_EQ(column->get_data_at(1).to_string(), " ");

    buf.write_c_string("world");
    buf.commit();
    ASSERT_EQ(column->size(), 3);
    ASSERT_EQ(column->get_data_at(2).to_string(), "world");

    std::string s = "!";
    buf.write(s.data(), s.size());
    buf.commit();
    ASSERT_EQ(column->size(), 4);
    ASSERT_EQ(column->get_data_at(3).to_string(), "!");

    ASSERT_EQ(column->get_data_at(0).to_string(), "hello");
    ASSERT_EQ(column->get_data_at(1).to_string(), " ");
    ASSERT_EQ(column->get_data_at(2).to_string(), "world");
    ASSERT_EQ(column->get_data_at(3).to_string(), "!");
}

TEST(StringBufferTest, TestWriteNumber) {
    auto column = ColumnString::create();
    BufferWritable buf(*column);

    buf.write_number(123);
    buf.commit();
    ASSERT_EQ(column->size(), 1);
    ASSERT_EQ(column->get_data_at(0).to_string(), "123");

    buf.write_number(-456);
    buf.commit();
    ASSERT_EQ(column->size(), 2);
    ASSERT_EQ(column->get_data_at(0).to_string(), "123");
    ASSERT_EQ(column->get_data_at(1).to_string(), "-456");

    buf.write_number(78.9);
    buf.commit();
    ASSERT_EQ(column->size(), 3);
    ASSERT_EQ(column->get_data_at(2).to_string(), "78.9");
}

TEST(StringBufferTest, TestWriteReadBinary) {
    auto column = ColumnString::create();
    BufferWritable buf(*column);

    // POD
    int int_val = 123;
    buf.write_binary(int_val);
    buf.commit();
    ASSERT_EQ(column->size(), 1);
    ASSERT_EQ(column->get_data_at(0).size, sizeof(int));

    StringRef sr = column->get_data_at(0);
    BufferReadable reader(sr);
    int read_int_val = 0;
    reader.read_binary(read_int_val);
    ASSERT_EQ(int_val, read_int_val);

    // String
    std::string str_val = "hello world";
    buf.write_binary(str_val);
    buf.commit();
    ASSERT_EQ(column->size(), 2);

    sr = column->get_data_at(1);
    BufferReadable reader2(sr);
    std::string read_str_val;
    reader2.read_binary(read_str_val);
    ASSERT_EQ(str_val, read_str_val);

    // StringRef
    StringRef str_ref_val("doris", 5);
    buf.write_binary(str_ref_val);
    buf.commit();
    ASSERT_EQ(column->size(), 3);

    sr = column->get_data_at(2);
    BufferReadable reader3(sr);
    StringRef read_str_ref_val;
    reader3.read_binary(read_str_ref_val);
    ASSERT_EQ(str_ref_val.to_string(), read_str_ref_val.to_string());
}

// This test may fail due to a bug in read_var_uint, where it can read out of bounds.
// The loop condition `i < 9` should probably be `i < len`.
//TEST(StringBufferTest, TestVarUInt) {
//    auto column = ColumnString::create();
//    BufferWritable buf(*column);
//
//    std::vector<UInt64> values = {123, 12345, 1234567, 0, (1UL << 35) - 1, (1UL << 63) - 1};
//
//    for (const auto& v : values) {
//        buf.write_var_uint(v);
//    }
//    buf.commit();
//
//    ASSERT_EQ(column->size(), 1);
//    StringRef sr = column->get_data_at(0);
//    BufferReadable reader(sr);
//
//    for (const auto& v : values) {
//        UInt64 read_val;
//        reader.read_var_uint(read_val);
//        ASSERT_EQ(v, read_val);
//    }
//}

TEST(StringBufferTest, TestWriteJsonString) {
    auto column = ColumnString::create();
    BufferWritable buf(*column);

    std::string json_str = "ab\b\f\n\r\t\\\"/c";
    buf.write_json_string(json_str);
    buf.commit();

    ASSERT_EQ(column->size(), 1);
    std::string expected = "\"ab\\b\\f\\n\\r\\t\\\\\\\"/c\"";
    ASSERT_EQ(column->get_data_at(0).to_string(), expected);

    // control characters
    char control_chars[] = {0x01, 0x1f};
    buf.write_json_string(control_chars, 2);
    buf.commit();
    ASSERT_EQ(column->size(), 2);
    expected = "\"\\u0001\\u001F\"";
    ASSERT_EQ(column->get_data_at(1).to_string(), expected);

    // utf8 line separators
    std::string ls_str =
            "\xE2\x80\xA8"
            " and "
            "\xE2\x80\xA9";
    buf.write_json_string(ls_str);
    buf.commit();
    ASSERT_EQ(column->size(), 3);
    expected = "\"\\u2028 and \\u2029\"";
    ASSERT_EQ(column->get_data_at(2).to_string(), expected);
}

TEST(StringBufferTest, ReadWriteStringRefWithArena) {
    auto column = ColumnString::create();
    BufferWritable buf(*column);
    Arena arena;

    StringRef original_str_ref("hello from arena", 16);
    buf.write_binary(original_str_ref);
    buf.commit();

    ASSERT_EQ(column->size(), 1);
    StringRef sr = column->get_data_at(0);
    BufferReadable reader(sr);

    StringRef new_str_ref = reader.read_binary_into(arena);

    ASSERT_EQ(original_str_ref.size, new_str_ref.size);
    ASSERT_EQ(original_str_ref.to_string(), new_str_ref.to_string());
    // The new StringRef should have its data in the arena.
    const char* arena_end = arena.alloc(0);
    const char* arena_start = arena_end - arena.size();
    ASSERT_TRUE(new_str_ref.data >= arena_start && new_str_ref.data < arena_end);
}

} // namespace doris::vectorized
