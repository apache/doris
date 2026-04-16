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

#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exec/common/groupby_agg_context.h"

namespace doris {

// Expose protected sort-limit state for unit testing.
class TestableGroupByAggContext : public GroupByAggContext {
public:
    TestableGroupByAggContext()
            : GroupByAggContext(/*key_data_types=*/ {}, /*agg_state_offsets=*/ {},
                                /*total_agg_state_size=*/0,
                                /*agg_state_alignment=*/1, /*is_first_phase=*/true) {}

    MutableColumns& limit_columns() { return _limit_columns; }
    int& limit_columns_min_ref() { return _limit_columns_min; }
    std::priority_queue<HeapLimitCursor>& limit_heap_ref() { return _limit_heap; }
};

class GroupByAggContextLimitTest : public testing::Test {
protected:
    void SetUp() override {
        _ctx = std::make_shared<TestableGroupByAggContext>();

        // Setup test data
        auto int_type = std::make_shared<DataTypeInt32>();
        _ctx->limit_columns().push_back(int_type->create_column());

        // Setup order directions (ascending)
        _ctx->order_directions = {1};
        _ctx->null_directions = {1};

        // Create test column
        _test_column = int_type->create_column();
        auto* col_data = reinterpret_cast<ColumnInt32*>(_test_column.get());

        // Insert test values: 5, 3, 1, -1, 0, 2
        col_data->insert(Field::create_field<TYPE_INT>(5));
        col_data->insert(Field::create_field<TYPE_INT>(3));
        col_data->insert(Field::create_field<TYPE_INT>(1));
        col_data->insert(Field::create_field<TYPE_INT>(-1));
        col_data->insert(Field::create_field<TYPE_INT>(0));
        col_data->insert(Field::create_field<TYPE_INT>(2));

        _key_columns.push_back(_test_column.get());
        // prepare the heap data first [5, 3, 1, -1]
        for (int i = 0; i < 4; ++i) {
            for (size_t j = 0; j < _key_columns.size(); ++j) {
                _ctx->limit_columns()[j]->insert_from(*_key_columns[j], i);
            }
            // build agg limit heap
            _ctx->limit_heap_ref().emplace(_ctx->limit_columns()[0]->size() - 1,
                                           _ctx->limit_columns(), _ctx->order_directions,
                                           _ctx->null_directions);
        }
        // keep the top limit values, only 3 values in heap [-1, 3, 1]
        _ctx->limit_heap_ref().pop();
        _ctx->limit_columns_min_ref() = _ctx->limit_heap_ref().top()._row_id;
    }

    std::shared_ptr<TestableGroupByAggContext> _ctx;
    MutableColumnPtr _test_column;
    ColumnRawPtrs _key_columns;
};

TEST_F(GroupByAggContextLimitTest, TestRefreshTopLimit) {
    // Test with limit = 3 (keep top 3 values)
    _ctx->limit = 3;

    // Add values one by one and verify the minimum value is tracked correctly
    EXPECT_EQ(_ctx->limit_columns_min_ref(), 1);

    _ctx->refresh_top_limit(4, _key_columns);
    EXPECT_EQ(_ctx->limit_columns_min_ref(), 2);

    _ctx->refresh_top_limit(5, _key_columns);
    EXPECT_EQ(_ctx->limit_columns_min_ref(), 2); // 1 should still be max

    auto heap_size = _ctx->limit_heap_ref().size();
    EXPECT_EQ(heap_size, 3);

    EXPECT_EQ(_ctx->limit_heap_ref().top()._row_id, 2); // 1 should be the top value
    _ctx->limit_heap_ref().pop();
    EXPECT_EQ(_ctx->limit_heap_ref().top()._row_id, 4); // 0 should be the top value
    _ctx->limit_heap_ref().pop();
    EXPECT_EQ(_ctx->limit_heap_ref().top()._row_id, 3); // -1 should be the top value
}

} // namespace doris
