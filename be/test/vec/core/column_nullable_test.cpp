
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

#include "vec/columns/column_nullable.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "vec/columns/column_vector.h"
#include "vec/common/sip_hash.h"

namespace doris::vectorized {

TEST(ColumnNullableTest, HashTest) {
    MutableColumnPtr tmp_column = ColumnVector<int>::create();
    auto val1 = 10;
    auto val2 = 20;
    tmp_column->insert_data((const char*)(&val1), 0);
    tmp_column->insert_data((const char*)(&val2), 0);
    ColumnPtr column = std::move(tmp_column);
    EXPECT_EQ(column->size(), 2);

    auto nullable_column = ColumnNullable::create(column, ColumnUInt8::create(column->size(), 0));
    SipHash hashes[2];
    column->update_hash_with_value(0, hashes[0]);
    nullable_column->update_hash_with_value(0, hashes[1]);
    EXPECT_EQ(hashes[0].get64(), hashes[1].get64());

    auto& null_map = ((ColumnNullable)(*nullable_column)).get_null_map_data();
    null_map[1] = true;
    column->update_hash_with_value(1, hashes[0]);
    nullable_column->update_hash_with_value(1, hashes[1]);
    EXPECT_NE(hashes[0].get64(), hashes[1].get64());
}

} // namespace doris::vectorized
