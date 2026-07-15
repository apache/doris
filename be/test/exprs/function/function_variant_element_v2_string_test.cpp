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

#include <gtest/gtest.h>

#include <array>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/function_variant_element_v2.h"

namespace doris {

TEST(VariantElementV2TypedTest, StringRootIsNotReparsedAsJson) {
    auto strings = ColumnString::create();
    strings->insert_data(R"({"a":1})", 7);
    auto source = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(strings), ColumnUInt8::create(1, 0)),
            std::make_shared<DataTypeString>());
    std::array segments {VariantElementV2PathSegment::object_key(StringRef("a"))};
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    ASSERT_TRUE(resolve_variant_element_v2_path(segments, &path).ok());

    ColumnPtr result;
    ASSERT_TRUE(extract_variant_element_v2(*source, *path, {}, &result).ok());
    const auto& nullable = assert_cast<const ColumnNullable&>(*result);
    ASSERT_EQ(nullable.size(), 1);
    EXPECT_TRUE(nullable.is_null_at(0));
    EXPECT_TRUE(source->is_typed());
}

} // namespace doris
