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

#include "agent/be_exec_version_manager.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_string.h"
#include "exprs/aggregate/aggregate_function_state_merge.h"
#include "exprs/aggregate/aggregate_function_state_union.h"
#include "testutil/column_helper.h"

namespace doris {

class AggregateStateUnionTest : public testing::Test {
protected:
    static constexpr size_t GROUPS = 3;
    using Places = std::array<AggregateDataPtr, GROUPS>;

    static Places create_places(const AggregateFunctionPtr& function, Arena& arena, size_t offset) {
        Places places;
        for (auto& place : places) {
            place = arena.aligned_alloc(offset + function->size_of_data(),
                                        function->align_of_data());
            memset(place, 0x5a, offset);
            function->create(place + offset);
        }
        return places;
    }

    static void compare_results(const AggregateFunctionPtr& function, const Places& actual,
                                const Places& expected, size_t offset) {
        auto actual_result = function->get_return_type()->create_column();
        auto expected_result = function->get_return_type()->create_column();
        for (size_t group = 0; group < GROUPS; ++group) {
            function->insert_result_into(actual[group] + offset, *actual_result);
            function->insert_result_into(expected[group] + offset, *expected_result);
            EXPECT_EQ(actual_result->compare_at(group, group, *expected_result, 1), 0)
                    << "group=" << group;
            EXPECT_EQ(std::string(actual[group], offset), std::string(offset, 0x5a));
        }
    }

    static void check_batches(const AggregateFunctionPtr& function,
                              const AggregateFunctionPtr& nested, const IColumn& states,
                              bool selected) {
        SCOPED_TRACE(selected ? "selected" : "all rows");
        Arena arena;
        const size_t offset = 2 * function->align_of_data();
        auto actual = create_places(function, arena, offset);
        auto expected = create_places(function, arena, offset);
        DEFER({
            for (size_t group = 0; group < GROUPS; ++group) {
                function->destroy(actual[group] + offset);
                function->destroy(expected[group] + offset);
            }
        });
        // Repeated destinations, nonzero offsets, and a NULL-only group are intentional.
        const std::array<size_t, 6> groups = {0, 1, 0, 2, 0, 1};
        std::array<AggregateDataPtr, 6> rows;
        for (size_t row = 0; row < rows.size(); ++row) {
            rows[row] = selected && (row == 0 || row == 2) ? nullptr : actual[groups[row]];
        }
        const IColumn* columns[] = {&states};
        // Empty batches must not inspect row zero, including the Nullable fast path.
        auto empty = states.clone_empty();
        const IColumn* empty_columns[] = {empty.get()};
        function->add_batch(0, rows.data(), offset, empty_columns, arena, false);
        function->add_batch_selected(0, rows.data(), offset, empty_columns, arena);
        for (int batch = 0; batch < 3; ++batch) {
            if (selected) {
                function->add_batch_selected(states.size(), rows.data(), offset, columns, arena);
            } else {
                function->add_batch(states.size(), rows.data(), offset, columns, arena, false);
            }
            for (size_t row = 0; row < rows.size(); ++row) {
                if (rows[row]) {
                    nested->deserialize_and_merge_from_column_range(expected[groups[row]] + offset,
                                                                    states, row, row, arena);
                }
            }
            // Read results after the batch scratch arena has been destroyed.
            compare_results(nested, actual, expected, offset);
        }
        rows.fill(nullptr);
        function->add_batch_selected(states.size(), rows.data(), offset, columns, arena);
        compare_results(nested, actual, expected, offset);
    }

    static void check(const std::string& name, const DataTypePtr& argument, const ColumnPtr& input,
                      bool result_nullable = true) {
        auto type = std::make_shared<DataTypeAggState>(DataTypes {argument}, result_nullable, name,
                                                       BeExecVersionManager::get_newest_version());
        auto nested = type->get_nested_function();
        auto states = type->create_column();
        Arena arena;
        // Serialize independently built states, as produced by *_state or *_combine.
        for (size_t row = 0; row < input->size(); ++row) {
            auto* place = arena.aligned_alloc(nested->size_of_data(), nested->align_of_data());
            nested->create(place);
            DEFER(nested->destroy(place));
            const IColumn* columns[] = {input.get()};
            nested->add(place, columns, row, arena);
            auto serialized = nested->create_serialize_column();
            nested->serialize_without_key_to_column(place, *serialized);
            states->insert_range_from(*serialized, 0, 1);
        }
        for (bool merge : {false, true}) {
            SCOPED_TRACE(merge ? "merge" : "union");
            auto function =
                    merge ? AggregateStateMerge::create(nested, {type}, nested->get_return_type())
                          : AggregateStateUnion::create(nested, {type}, type);
            check_batches(function, nested, *states, false);
            check_batches(function, nested, *states, true);
        }
    }
};

TEST_F(AggregateStateUnionTest, Sum) {
    check("sum", std::make_shared<DataTypeInt64>(),
          ColumnHelper::create_column<DataTypeInt64>({2, 50, 7, 0, 11, 19}));
}

TEST_F(AggregateStateUnionTest, NullableSum) {
    check("sum", make_nullable(std::make_shared<DataTypeInt64>()),
          ColumnHelper::create_nullable_column<DataTypeInt64>({2, 50, 7, 0, 11, 19},
                                                              {0, 0, 0, 1, 0, 0}));
}

TEST_F(AggregateStateUnionTest, AllNullSum) {
    check("sum", make_nullable(std::make_shared<DataTypeInt64>()),
          ColumnHelper::create_nullable_column<DataTypeInt64>({0, 0, 0, 0, 0, 0},
                                                              {1, 1, 1, 1, 1, 1}));
}

TEST_F(AggregateStateUnionTest, DecimalSum) {
    auto type = std::make_shared<DataTypeDecimal128>(27, 9);
    auto input = type->create_column();
    auto& values = assert_cast<ColumnDecimal128V3&>(*input);
    for (Int128 value : {2, 50, 7, 0, 11, 19}) {
        values.insert_value(Decimal128V3(value * 1000000000));
    }
    check("sum", type, std::move(input));
}

TEST_F(AggregateStateUnionTest, StringArrayStateLifetime) {
    check("array_agg", std::make_shared<DataTypeString>(),
          ColumnHelper::create_column<DataTypeString>(
                  {std::string(4096, 'a'), "b", "c", "", std::string(8192, 'd'), "e"}),
          false);
}

TEST_F(AggregateStateUnionTest, NullableStringMinStateLifetime) {
    check("min", make_nullable(std::make_shared<DataTypeString>()),
          ColumnHelper::create_nullable_column<DataTypeString>(
                  {std::string(4096, 'a'), "z", "c", "", std::string(8192, 'b'), "e"},
                  {0, 0, 0, 1, 0, 0}));
}

} // namespace doris
