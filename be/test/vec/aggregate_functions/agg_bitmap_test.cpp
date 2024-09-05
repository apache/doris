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

#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "util/bitmap_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

const int agg_test_batch_size = 10;

namespace doris::vectorized {
// declare function
void register_aggregate_function_bitmap(AggregateFunctionSimpleFactory& factory);

TEST(AggBitmapTest, bitmap_union_test) {
    std::string function_name = "bitmap_union";
    auto data_type = std::make_shared<DataTypeBitMap>();
    // Prepare test data.
    auto column_bitmap = data_type->create_column();
    for (int i = 0; i < agg_test_batch_size; i++) {
        BitmapValue bitmap_value(i);
        assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value);
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_bitmap(factory);
    DataTypes data_types = {data_type};
    auto agg_function = factory.get(function_name, data_types, false, -1);
    agg_function->set_version(3);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_bitmap.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, nullptr);
    }

    // Check result.
    ColumnBitmap ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(ans.size(), 1);
    EXPECT_EQ(ans.get_element(0).cardinality(), agg_test_batch_size);
    agg_function->destroy(place);

    auto dst = agg_function->create_serialize_column();
    agg_function->streaming_agg_serialize_to_column(column, dst, agg_test_batch_size, nullptr);

    for (size_t i = 0; i != agg_test_batch_size; ++i) {
        EXPECT_EQ(std::to_string(i), assert_cast<ColumnBitmap&>(*dst).get_element(i).to_string());
    }
}

} // namespace doris::vectorized
