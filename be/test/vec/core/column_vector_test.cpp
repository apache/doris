
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

#include "vec/columns/column_vector.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "gtest/gtest_pred_impl.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(VColumnVectorTest, insert_date_column) {
    auto column = ColumnInt64::create();

    size_t rows = 4096;
    int64_t val = 0;
    for (size_t i = 0; i < rows; ++i) {
        column->insert_date_column(reinterpret_cast<char*>(&val), 1);
    }
    ASSERT_EQ(column->size(), rows);
}

TEST(VColumnVectorTest, insert_indices_from_uInt64_crash) {
    auto src = ColumnInt64::create();
    std::vector<int64_t> src_values = {10LL, 20LL, 30LL, 40LL};
    for (auto v : src_values) {
        src->insert(Field::create_field<PrimitiveType::TYPE_BIGINT>(v));
    }
    ASSERT_EQ(src->size(), 4);

    auto dest = ColumnInt64::create();
    std::vector<int64_t> dest_values = {1LL, 2LL};
    for (auto v : dest_values) {
        dest->insert(Field::create_field<PrimitiveType::TYPE_BIGINT>(v));
    }

    // Valid indices (should work)
    uint32_t valid_indices[] = {0, 2, 3};
    dest->insert_indices_from(*src, valid_indices, valid_indices + 3);
    ASSERT_EQ(dest->size(), 5);        // 2 original + 3 inserted
    ASSERT_EQ(dest->get_int(2), 10LL); // Check inserted value

    // Invalid indices to trigger crash
    uint32_t invalid_indices[] = {
            0, 2, 1000000000U, 2000000000U}; // Large indices to hit unmapped memory (src size = 4)
    try {
        dest->insert_indices_from(*src, invalid_indices, invalid_indices + 4);
        FAIL() << "Expected Exception for out-of-bounds indices";
    } catch (const Exception& e) {
        ASSERT_EQ(e.code(), ErrorCode::INTERNAL_ERROR);
        ASSERT_TRUE(
                std::string(e.to_string()).find("Index 1000000000 exceeds source column size 4") !=
                std::string::npos);
    }
}

} // namespace doris::vectorized
