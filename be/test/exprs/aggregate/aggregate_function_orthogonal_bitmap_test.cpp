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

#include <cstdint>
#include <memory>
#include <string>

#include "agent/be_exec_version_manager.h"
#include "common/exception.h"
#include "core/column/column_complex.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/value/bitmap_value.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "testutil/column_helper.h"

namespace doris {

static void check_nullable_formula(const std::string& name, const DataTypePtr& result_type) {
    auto bitmap_type = std::make_shared<DataTypeBitMap>();
    auto string_type = std::make_shared<DataTypeString>();
    auto formula_type = make_nullable(string_type);
    auto bitmap = ColumnBitmap::create();
    bitmap->insert_value(BitmapValue {uint64_t(1)});
    auto key = ColumnHelper::create_column<DataTypeString>({"1"});
    auto null_formula = ColumnHelper::create_nullable_column<DataTypeString>({"1"}, {1});
    auto valid_formula = ColumnHelper::create_nullable_column<DataTypeString>({"1"}, {0});

    AggregateFunctionAttr attr;
    auto function = AggregateFunctionSimpleFactory::instance().get(
            name, {bitmap_type, string_type, formula_type}, result_type, false,
            BeExecVersionManager::get_newest_version(), attr);
    ASSERT_NE(function, nullptr);

    const IColumn* null_columns[] = {bitmap.get(), key.get(), null_formula.get()};
    EXPECT_THROW(function->check_input_columns_type(null_columns), Exception);

    const IColumn* valid_columns[] = {bitmap.get(), key.get(), valid_formula.get()};
    EXPECT_NO_THROW(function->check_input_columns_type(valid_columns));
}

TEST(AggregateFunctionOrthogonalBitmapTest, RejectNullableFormulaBeforeSkippingRows) {
    check_nullable_formula("orthogonal_bitmap_expr_calculate", std::make_shared<DataTypeBitMap>());
    check_nullable_formula("orthogonal_bitmap_expr_calculate_count",
                           std::make_shared<DataTypeInt64>());
}

} // namespace doris
