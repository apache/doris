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

#include "format_v2/jni/jdbc_reader.h"

#include <gtest/gtest.h>

#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"

namespace doris::format::jdbc {

TEST(JdbcJniReaderTest, NonNullableSpecialTypeRejectsCastNull) {
    auto data = ColumnString::create();
    data->insert_default();
    auto null_map = ColumnUInt8::create();
    null_map->get_data().push_back(1);
    auto result = ColumnNullable::create(std::move(data), std::move(null_map));

    EXPECT_TRUE(validate_non_nullable_special_type_result(*result, 1)
                        .is<ErrorCode::DATA_QUALITY_ERROR>());
}

TEST(JdbcJniReaderTest, NonNullableSpecialTypeAcceptsSuccessfulCast) {
    auto data = ColumnString::create();
    data->insert_data("ok", 2);
    auto null_map = ColumnUInt8::create();
    null_map->get_data().push_back(0);
    auto result = ColumnNullable::create(std::move(data), std::move(null_map));

    EXPECT_TRUE(validate_non_nullable_special_type_result(*result, 1).ok());
}

} // namespace doris::format::jdbc
