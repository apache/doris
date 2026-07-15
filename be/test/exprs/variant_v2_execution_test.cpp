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

#include "common/config.h"
#include "core/column/column_variant.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_variant.h"

namespace doris {

TEST(VariantV2ExecutionTest, BeConfigSelectsPhysicalColumn) {
    const bool previous = config::enable_variant_v2;
    DataTypeVariant type;

    config::enable_variant_v2 = false;
    EXPECT_NE(check_and_get_column<ColumnVariant>(*type.create_column()), nullptr);

    config::enable_variant_v2 = true;
    EXPECT_NE(check_and_get_column<ColumnVariantV2>(*type.create_column()), nullptr);
    config::enable_variant_v2 = previous;
}

} // namespace doris
