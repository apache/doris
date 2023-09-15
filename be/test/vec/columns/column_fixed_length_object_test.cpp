
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

#include "vec/columns/column_fixed_length_object.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "vec/columns/column_vector.h"
#include "vec/common/sip_hash.h"

namespace doris::vectorized {

TEST(ColumnFixedLenghtObjectTest, InsertRangeFrom) {
    auto column1 = ColumnFixedLengthObject::create(sizeof(size_t));
    EXPECT_EQ(sizeof(size_t), column1->item_size());
    const size_t count = 1000;

    column1->resize(count);
    auto& data = column1->get_data();
    for (size_t i = 0; i < count; ++i) {
        *((size_t*)&data[i * sizeof(size_t)]) = i;
    }

    auto column2 = ColumnFixedLengthObject::create(sizeof(size_t));
    EXPECT_EQ(sizeof(size_t), column2->item_size());

    column2->insert_range_from(*column1, 100, 0);
    EXPECT_EQ(column2->size(), 0);

    column2->insert_range_from(*column1, 97, 100);
    EXPECT_EQ(column2->size(), 100);

    auto& data2 = column2->get_data();
    for (size_t i = 0; i < 100; ++i) {
        EXPECT_EQ(*((size_t*)&data2[i * sizeof(size_t)]), i + 97);
    }
}

TEST(ColumnFixedLenghtObjectTest, UpdateHashWithValue) {
    auto column1 = ColumnFixedLengthObject::create(sizeof(int64_t));
    EXPECT_EQ(sizeof(int64_t), column1->item_size());
    const size_t count = 1000;

    column1->resize(count);
    auto& data = column1->get_data();
    for (size_t i = 0; i != count; ++i) {
        *((int64_t*)&data[i * column1->item_size()]) = i;
    }

    SipHash hash1;
    for (size_t i = 0; i != count; ++i) {
        column1->update_hash_with_value(i, hash1);
    }

    auto column2 = ColumnVector<int64_t>::create();
    column2->resize(count);
    for (size_t i = 0; i != count; ++i) {
        column2->get_data()[i] = i;
    }

    SipHash hash2;
    for (size_t i = 0; i != count; ++i) {
        column2->update_hash_with_value(i, hash2);
    }

    EXPECT_EQ(hash1.get64(), hash2.get64());
}
} // namespace doris::vectorized
