
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

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "vec/data_types/data_type_date.h"

namespace doris::vectorized {

TEST(VColumnVectorTest, insert_date_column) {
    auto column = ColumnVector<Int64>::create();

    size_t rows = 4096;
    int64_t val = 0;
    for (size_t i = 0; i < rows; ++i) {
        column->insert_date_column(reinterpret_cast<char*>(&val), 1);
    }
    ASSERT_EQ(column->size(), rows);
}

} // namespace doris::vectorized
