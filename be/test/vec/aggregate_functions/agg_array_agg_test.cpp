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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <ostream>
#include <string>

#include "agg_function_test.h"
#include "common/logging.h"
#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arena.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris {
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

struct AggregateFunctionArrayAggTest : public AggregateFunctiontest {};

TEST_F(AggregateFunctionArrayAggTest, test_array_agg_aint64) {
    create_agg("array_agg", false, {std::make_shared<DataTypeInt64>()});

    auto data_type = std::make_shared<DataTypeInt64>();
    auto array_data_type = std::make_shared<DataTypeArray>(data_type);

    auto off_column = ColumnVector<ColumnArray::Offset64>::create();
    auto data_column = data_type->create_column();
    std::vector<ColumnArray::Offset64> offs = {0, 3};
    std::vector<int64_t> vals = {1, 2, 3};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }
    auto array_column = ColumnArray::create(std::move(data_column), std::move(off_column));

    execute(Block({ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3})}),
            ColumnWithTypeAndName(std::move(array_column), array_data_type, "column"));
}

TEST_F(AggregateFunctionArrayAggTest, test_array_agg_aint64_nullable) {
    auto data_type = make_nullable(std::make_shared<DataTypeInt64>());
    create_agg("array_agg", false, {data_type});

    auto array_data_type = std::make_shared<DataTypeArray>(data_type);

    auto off_column = ColumnVector<ColumnArray::Offset64>::create();
    auto data_column = data_type->create_column();
    std::vector<ColumnArray::Offset64> offs = {0, 4};
    std::vector<int64_t> vals = {1, 2, 3};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    data_column->insert_default();
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }
    auto array_column = ColumnArray::create(data_column->clone(), std::move(off_column));

    execute(Block({ColumnWithTypeAndName(data_column->clone(), data_type, "")}),
            ColumnWithTypeAndName(std::move(array_column), array_data_type, "column"));
}

TEST_F(AggregateFunctionArrayAggTest, test_array_agg_astr_nullable) {
    auto data_type = make_nullable(std::make_shared<DataTypeString>());
    create_agg("array_agg", false, {data_type});

    auto array_data_type = std::make_shared<DataTypeArray>(data_type);

    auto off_column = ColumnVector<ColumnArray::Offset64>::create();
    auto data_column = data_type->create_column();
    std::vector<ColumnArray::Offset64> offs = {0, 4};
    std::vector<int64_t> vals = {1, 2, 3};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    data_column->insert_default();
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), sizeof(v));
    }
    auto array_column = ColumnArray::create(data_column->clone(), std::move(off_column));

    execute(Block({ColumnWithTypeAndName(data_column->clone(), data_type, "")}),
            ColumnWithTypeAndName(std::move(array_column), array_data_type, "column"));
}

} // namespace doris::vectorized
