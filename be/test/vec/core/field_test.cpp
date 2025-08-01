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

#include "vec/core/field.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "gtest/gtest_pred_impl.h" // IWYU pragma: keep
#include "runtime/define_primitive_type.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
TEST(VFieldTest, field_string) {
    Field f;

    f = Field::create_field<TYPE_STRING>(String {"Hello, world (1)"});
    ASSERT_EQ(f.get<String>(), "Hello, world (1)");
    f = Field::create_field<TYPE_STRING>(String {"Hello, world (2)"});
    ASSERT_EQ(f.get<String>(), "Hello, world (2)");
    f = Field::create_field<TYPE_ARRAY>(
            Array {Field ::create_field<TYPE_STRING>(String {"Hello, world (3)"})});
    ASSERT_EQ(f.get<Array>()[0].get<String>(), "Hello, world (3)");
    f = Field::create_field<TYPE_STRING>(String {"Hello, world (4)"});
    ASSERT_EQ(f.get<String>(), "Hello, world (4)");
    f = Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_STRING>(String {"Hello, world (5)"})});
    ASSERT_EQ(f.get<Array>()[0].get<String>(), "Hello, world (5)");
    f = Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_STRING>(String {"Hello, world (6)"})});
    ASSERT_EQ(f.get<Array>()[0].get<String>(), "Hello, world (6)");
}

TEST(VFieldTest, jsonb_field_unique_ptr) {
    // Test default constructor
    JsonbField empty;
    ASSERT_EQ(empty.get_value(), nullptr);
    ASSERT_EQ(empty.get_size(), 0);

    // Test constructor with data
    const char* test_data = R"({ "key": "value" })";
    size_t test_size = strlen(test_data);
    JsonbField jf1(test_data, test_size);
    ASSERT_NE(jf1.get_value(), nullptr);
    ASSERT_EQ(jf1.get_size(), test_size);
    ASSERT_EQ(std::string(jf1.get_value(), jf1.get_size()), std::string(test_data));

    // Test copy constructor
    JsonbField jf2(jf1);
    ASSERT_NE(jf2.get_value(), nullptr);
    ASSERT_NE(jf2.get_value(), jf1.get_value()); // Different memory locations
    ASSERT_EQ(jf2.get_size(), jf1.get_size());
    ASSERT_EQ(std::string(jf2.get_value(), jf2.get_size()),
              std::string(jf1.get_value(), jf1.get_size()));

    // Test move constructor
    JsonbField jf3(std::move(jf2));
    ASSERT_NE(jf3.get_value(), nullptr);
    ASSERT_EQ(jf2.get_value(), nullptr); // jf2 should be empty after move
    ASSERT_EQ(jf2.get_size(), 0);        // jf2 size should be 0 after move
    ASSERT_EQ(jf3.get_size(), test_size);
    ASSERT_EQ(std::string(jf3.get_value(), jf3.get_size()), std::string(test_data));

    // Test copy assignment
    JsonbField jf4;
    jf4 = jf1;
    ASSERT_NE(jf4.get_value(), nullptr);
    ASSERT_NE(jf4.get_value(), jf1.get_value()); // Different memory locations
    ASSERT_EQ(jf4.get_size(), jf1.get_size());
    ASSERT_EQ(std::string(jf4.get_value(), jf4.get_size()),
              std::string(jf1.get_value(), jf1.get_size()));

    // Test move assignment
    JsonbField jf5;
    jf5 = std::move(jf4);
    ASSERT_NE(jf5.get_value(), nullptr);
    ASSERT_EQ(jf4.get_value(), nullptr); // jf4 should be empty after move
    ASSERT_EQ(jf4.get_size(), 0);        // jf4 size should be 0 after move
    ASSERT_EQ(jf5.get_size(), test_size);
    ASSERT_EQ(std::string(jf5.get_value(), jf5.get_size()), std::string(test_data));

    // Test JsonbField with Field
    Field field_jf = Field::create_field<TYPE_JSONB>(jf1);
    ASSERT_EQ(field_jf.get_type(), TYPE_JSONB);
    ASSERT_NE(field_jf.get<JsonbField>().get_value(), nullptr);
    ASSERT_EQ(field_jf.get<JsonbField>().get_size(), test_size);
    ASSERT_EQ(std::string(field_jf.get<JsonbField>().get_value(),
                          field_jf.get<JsonbField>().get_size()),
              std::string(test_data));
}

// Test for JsonbField I/O operations
TEST(VFieldTest, jsonb_field_io) {
    // Prepare a JsonbField
    const char* test_data = R"({ "key": "value" })";
    size_t test_size = strlen(test_data);
    JsonbField original(test_data, test_size);

    // TEST 1: write_json_binary - From JsonbField to buffer
    // Create a ColumnString to use with BufferWritable
    ColumnString column_str;

    // Write the JsonbField to the buffer
    {
        BufferWritable buf(column_str);
        buf.write_binary(StringRef {original.get_value(), original.get_size()});
        buf.commit(); // Important: commit the write operation
    }

    // Verify data was written
    ASSERT_GT(column_str.size(), 0);

    // Read the JsonbField back using BufferReadable
    {
        // Get the StringRef from ColumnString
        StringRef str_ref = column_str.get_data_at(0);

        // Create a BufferReadable from StringRef
        BufferReadable read_buf(str_ref);

        // Read the data back into a new JsonbField
        StringRef result;
        read_buf.read_binary(result);
        JsonbField read_field = JsonbField(result.data, result.size);

        // Verify the data
        ASSERT_NE(read_field.get_value(), nullptr);
        ASSERT_EQ(read_field.get_size(), original.get_size());
        ASSERT_EQ(std::string(read_field.get_value(), read_field.get_size()),
                  std::string(original.get_value(), original.get_size()));
    }

    // Test with JsonbField as a Field and serde it
    {
        ColumnString field_column;

        // ser
        {
            BufferWritable field_buf(field_column);
            field_buf.write_binary(StringRef {original.get_value(), original.get_size()});
            field_buf.commit();
        }

        // Verify field was written
        ASSERT_GT(field_column.size(), 0);

        // de
        {
            StringRef field_str_ref = field_column.get_data_at(0);
            BufferReadable read_field_buf(field_str_ref);

            // we can't use read_binary because of the JsonbField is not POD type
            StringRef result;
            read_field_buf.read_binary(result);
            JsonbField jsonb_from_field = JsonbField(result.data, result.size);
            Field f2 = Field::create_field<TYPE_JSONB>(jsonb_from_field);

            ASSERT_EQ(f2.get_type(), TYPE_JSONB);
            ASSERT_NE(f2.get<JsonbField>().get_value(), nullptr);
            ASSERT_EQ(
                    std::string(f2.get<JsonbField>().get_value(), f2.get<JsonbField>().get_size()),
                    std::string(test_data));
        }
    }
}

} // namespace doris::vectorized
