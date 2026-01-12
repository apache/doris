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

#include "vec/columns/column_viewer.h"

#include <gtest/gtest.h>

#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "testutil/column_helper.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

TEST(ColumnViewerTest, test) {
    {
        // vec
        auto col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});

        ColumnViewer<PrimitiveType::TYPE_INT> viewer {col};

        EXPECT_EQ(viewer.size(), 3);
        EXPECT_EQ(viewer.is_null(0), false);
        EXPECT_EQ(viewer.value(0), 1);
        EXPECT_EQ(viewer.is_null(1), false);
        EXPECT_EQ(viewer.value(1), 2);
        EXPECT_EQ(viewer.is_null(2), false);
        EXPECT_EQ(viewer.value(2), 3);
    }

    {
        // nullable vec

        auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3}, {0, 1, 0});
        ColumnViewer<PrimitiveType::TYPE_INT> viewer {col};
        EXPECT_EQ(viewer.size(), 3);
        EXPECT_EQ(viewer.is_null(0), false);
        EXPECT_EQ(viewer.value(0), 1);
        EXPECT_EQ(viewer.is_null(1), true);
        EXPECT_EQ(viewer.is_null(2), false);
        EXPECT_EQ(viewer.value(2), 3);
    }

    {
        // const vec

        auto col = ColumnHelper::create_column<DataTypeInt32>({1});
        ColumnPtr const_col = ColumnConst::create(col, 3);
        ColumnViewer<PrimitiveType::TYPE_INT> viewer {const_col};
        EXPECT_EQ(viewer.size(), 3);
        EXPECT_EQ(viewer.is_null(0), false);
        EXPECT_EQ(viewer.value(0), 1);
        EXPECT_EQ(viewer.is_null(1), false);
        EXPECT_EQ(viewer.value(1), 1);
        EXPECT_EQ(viewer.is_null(2), false);
        EXPECT_EQ(viewer.value(2), 1);
    }

    {
        // const nullable vec(not null)
        auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({1}, {0});
        ColumnPtr const_col = ColumnConst::create(col, 3);
        ColumnViewer<PrimitiveType::TYPE_INT> viewer {const_col};
        EXPECT_EQ(viewer.size(), 3);
        EXPECT_EQ(viewer.is_null(0), false);
        EXPECT_EQ(viewer.value(0), 1);
        EXPECT_EQ(viewer.is_null(1), false);
        EXPECT_EQ(viewer.value(1), 1);
        EXPECT_EQ(viewer.is_null(2), false);
        EXPECT_EQ(viewer.value(2), 1);
    }

    {
        // const nullable vec(is null)
        auto col = ColumnHelper::create_nullable_column<DataTypeInt32>({1}, {1});
        ColumnPtr const_col = ColumnConst::create(col, 3);
        ColumnViewer<PrimitiveType::TYPE_INT> viewer {const_col};
        EXPECT_EQ(viewer.size(), 3);
        EXPECT_EQ(viewer.is_null(0), true);
        EXPECT_EQ(viewer.is_null(1), true);
        EXPECT_EQ(viewer.is_null(2), true);
    }
}
} // namespace doris::vectorized