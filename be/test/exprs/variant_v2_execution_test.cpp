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

#include "core/column/column_variant.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"

namespace doris {

TEST(VariantV2ExecutionTest, ExecutionTypeSelectsPhysicalColumn) {
    DataTypeVariant legacy;
    DataTypeVariantV2 compute_v2;

    EXPECT_NE(check_and_get_column<ColumnVariant>(*legacy.create_column()), nullptr);
    EXPECT_NE(check_and_get_column<ColumnVariantV2>(*compute_v2.create_column()), nullptr);
}

} // namespace doris
