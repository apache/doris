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

#include "pipeline/dependency.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"

namespace doris::pipeline {

class AggSharedStateTest : public testing::Test {
protected:
    void SetUp() override {
        _shared_state = std::make_shared<AggSharedState>();

        // Setup test data
        auto int_type = std::make_shared<vectorized::DataTypeInt32>();
        _shared_state->limit_columns.push_back(int_type->create_column());

        // Setup order directions (ascending)
        _shared_state->order_directions = {1};
        _shared_state->null_directions = {1};

        // Create test column
        _test_column = int_type->create_column();
        auto* col_data = reinterpret_cast<vectorized::ColumnInt32*>(_test_column.get());

        // Insert test values: 5, 3, 1, -2, -1, 0
        col_data->insert(vectorized::Field::create_field<TYPE_INT>(5));
        col_data->insert(vectorized::Field::create_field<TYPE_INT>(3));
        col_data->insert(vectorized::Field::create_field<TYPE_INT>(1));
        col_data->insert(vectorized::Field::create_field<TYPE_INT>(-1));
        col_data->insert(vectorized::Field::create_field<TYPE_INT>(0));
        col_data->insert(vectorized::Field::create_field<TYPE_INT>(2));

        _key_columns.push_back(_test_column.get());
        // prepare the heap data first [5, 3, 1, -2]
        for (int i = 0; i < 4; ++i) {
            for (int j = 0; j < _key_columns.size(); ++j) {
                _shared_state->limit_columns[j]->insert_from(*_key_columns[j], i);
            }
            // build agg limit heap
            _shared_state->limit_heap.emplace(
                    _shared_state->limit_columns[0]->size() - 1, _shared_state->limit_columns,
                    _shared_state->order_directions, _shared_state->null_directions);
        }
        // keep the top limit values, only 3 value in heap [-1, 3, 1]
        _shared_state->limit_heap.pop();
        _shared_state->limit_columns_min = _shared_state->limit_heap.top()._row_id;
    }

    std::shared_ptr<AggSharedState> _shared_state;
    vectorized::MutableColumnPtr _test_column;
    vectorized::ColumnRawPtrs _key_columns;
};

TEST_F(AggSharedStateTest, TestRefreshTopLimit) {
    // Test with limit = 3 (keep top 3 values)
    _shared_state->limit = 3;

    // Add values one by one and verify the minimum value is tracked correctly
    EXPECT_EQ(_shared_state->limit_columns_min, 1);

    _shared_state->refresh_top_limit(4, _key_columns);
    EXPECT_EQ(_shared_state->limit_columns_min, 2);

    _shared_state->refresh_top_limit(5, _key_columns);
    EXPECT_EQ(_shared_state->limit_columns_min, 2); // 1 should still be max

    auto heap_size = _shared_state->limit_heap.size();
    EXPECT_EQ(heap_size, 3);

    EXPECT_EQ(_shared_state->limit_heap.top()._row_id, 2); // 1 should be the top value
    _shared_state->limit_heap.pop();
    EXPECT_EQ(_shared_state->limit_heap.top()._row_id, 4); // 0 should be the top value
    _shared_state->limit_heap.pop();
    EXPECT_EQ(_shared_state->limit_heap.top()._row_id, 3); // -1 should be the top value
}

} // namespace doris::pipeline
