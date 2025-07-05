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

#include "runtime/primitive_type.h"

namespace doris::vectorized {

TEST(StringBufferTest, TestWriteNumber) {
    ColumnString column_string;
    BufferWritable buffer(column_string);
    buffer.write_number(12345);
    buffer.write_number(true);
    buffer.write_number(3.14159);
    buffer.commit();

    EXPECT_EQ(column_string.size(), 1);
    auto str_ref = column_string.get_data_at(0);

    EXPECT_EQ(str_ref.to_string(), "12345true3.14159");
}

TEST(StringBufferTest, TestWriteBinary) {
    ColumnString column_string;
    BufferWritable buffer(column_string);

    {
        String str = "Hello, World!";
        write_binary(str, buffer);
    }
    {
        int64_t x = 123456789;
        write_binary(x, buffer);
    }

    buffer.commit();

    EXPECT_EQ(column_string.size(), 1);
    auto str_ref = column_string.get_data_at(0);

    BufferReadable readable(str_ref);

    {
        String read_str;
        read_binary(read_str, readable);
        EXPECT_EQ(read_str, "Hello, World!");
    }

    {
        int64_t read_x;
        read_binary(read_x, readable);
        EXPECT_EQ(read_x, 123456789);
    }
}

} // namespace doris::vectorized
