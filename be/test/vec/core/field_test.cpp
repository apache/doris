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

#include "gtest/gtest_pred_impl.h"
#include "vec/core/types.h"

namespace doris::vectorized {
TEST(VFieldTest, field_string) {
    Field f;

    f = Field {String {"Hello, world (1)"}};
    ASSERT_EQ(f.get<String>(), "Hello, world (1)");
    f = Field {String {"Hello, world (2)"}};
    ASSERT_EQ(f.get<String>(), "Hello, world (2)");
    f = Field {Array {Field {String {"Hello, world (3)"}}}};
    ASSERT_EQ(f.get<Array>()[0].get<String>(), "Hello, world (3)");
    f = String {"Hello, world (4)"};
    ASSERT_EQ(f.get<String>(), "Hello, world (4)");
    f = Array {Field {String {"Hello, world (5)"}}};
    ASSERT_EQ(f.get<Array>()[0].get<String>(), "Hello, world (5)");
    f = Array {Field(String {"Hello, world (6)"})};
    ASSERT_EQ(f.get<Array>()[0].get<String>(), "Hello, world (6)");
}

TEST(VFieldTest, field_jsonb) {
    const char* data = "hello";
    JsonbField jsonb(data, 5);
    ASSERT_EQ(jsonb.size, 5);
    ASSERT_NE(jsonb.data, data);

    JsonbField copyed_jsonb(jsonb);
    ASSERT_EQ(copyed_jsonb.size, 5);
    ASSERT_NE(copyed_jsonb.data, data);

    const char* moved_data = jsonb.data;
    JsonbField moved_jsonb(std::move(jsonb));
    ASSERT_EQ(moved_jsonb.size, 5);
    ASSERT_EQ(moved_jsonb.data, moved_data);
    ASSERT_EQ(jsonb.size, 0);
    ASSERT_EQ(jsonb.data, nullptr);

    jsonb = copyed_jsonb;
    ASSERT_EQ(jsonb.size, 5);
    ASSERT_NE(jsonb.data, copyed_jsonb.data);
    ASSERT_EQ(std::string_view(jsonb.data, jsonb.size), "hello");

    jsonb = std::move(moved_jsonb);
    ASSERT_EQ(jsonb.size, 5);
    ASSERT_EQ(jsonb.data, moved_data);
    ASSERT_EQ(moved_jsonb.size, 0);
    ASSERT_EQ(moved_jsonb.data, nullptr);
}

} // namespace doris::vectorized
