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

#include "core/column/predicate_column.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "common/status.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "core/types.h"
#include "testutil/column_helper.h"

namespace doris {

TEST(PredicateColumnTest, InsertDuplicateFieldsString) {
    auto column = PredicateColumnType<TYPE_STRING>::create();
    column->reserve(10);

    // Test with a simple string - insert 5 duplicates
    std::string test_str = "hello";
    Field field = Field::create_field<TYPE_STRING>(test_str);
    column->insert_duplicate_fields(field, 5);

    EXPECT_EQ(column->size(), 5);
    for (size_t i = 0; i < 5; i++) {
        StringRef ref = column->get_data()[i];
        EXPECT_EQ(ref.size, test_str.size());
        EXPECT_EQ(std::string(ref.data, ref.size), test_str);
    }

    // Insert another batch to verify memory doesn't overlap
    std::string str2 = "world";
    Field field2 = Field::create_field<TYPE_STRING>(str2);
    column->insert_duplicate_fields(field2, 3);

    EXPECT_EQ(column->size(), 8);
    // Verify first batch is still correct
    for (size_t i = 0; i < 5; i++) {
        StringRef ref = column->get_data()[i];
        EXPECT_EQ(std::string(ref.data, ref.size), test_str);
    }
    // Verify second batch
    for (size_t i = 5; i < 8; i++) {
        StringRef ref = column->get_data()[i];
        EXPECT_EQ(std::string(ref.data, ref.size), str2);
    }
}

TEST(PredicateColumnTest, InsertDuplicateFieldsInt) {
    auto column = PredicateColumnType<TYPE_INT>::create();
    column->reserve(10);

    Int32 val = 42;
    Field field = Field::create_field<TYPE_INT>(val);
    column->insert_duplicate_fields(field, 5);

    EXPECT_EQ(column->size(), 5);
    for (size_t i = 0; i < 5; i++) {
        EXPECT_EQ(column->get_data()[i], 42);
    }
}

TEST(PredicateColumnTest, InsertDuplicateFieldsLargeInt) {
    auto column = PredicateColumnType<TYPE_LARGEINT>::create();
    column->reserve(10);

    Int128 val = Int128(123456789012345LL) * Int128(1000000000LL);
    Field field = Field::create_field<TYPE_LARGEINT>(val);
    column->insert_duplicate_fields(field, 3);

    EXPECT_EQ(column->size(), 3);
    for (size_t i = 0; i < 3; i++) {
        EXPECT_EQ(column->get_data()[i], val);
    }
}

} // namespace doris