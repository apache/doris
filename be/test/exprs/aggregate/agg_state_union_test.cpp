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
#include "core/column/column_complex.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_string.h"
#include "exprs/aggregate/aggregate_function_state_combine.h"
#include "exprs/aggregate/aggregate_function_state_merge.h"
#include "exprs/aggregate/aggregate_function_state_union.h"
#include "testutil/column_helper.h"

namespace doris {

// Reuse the state lifecycle forwarding while inspecting the scratch passed by UNION/MERGE.
class ScratchCheckingAggregateFunction final : public AggregateStateUnion {
public:
    using AggregateStateUnion::AggregateStateUnion;

    bool needs_deserialize_and_merge_scratch() const override {
        return _function->needs_deserialize_and_merge_scratch();
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   size_t num_rows) const override {
        check_scratch(rhs);
        ++batch_calls;
        _function->deserialize_and_merge_vec(places, offset, rhs, column, arena, num_rows);
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, size_t num_rows) const override {
        check_scratch(rhs);
        ++selected_batch_calls;
        _function->deserialize_and_merge_vec_selected(places, offset, rhs, column, arena, num_rows);
    }

    mutable size_t batch_calls = 0;
    mutable size_t selected_batch_calls = 0;

private:
    void check_scratch(ConstAggregateDataPtr rhs) const {
        EXPECT_EQ(rhs != nullptr, needs_deserialize_and_merge_scratch());
        EXPECT_EQ(reinterpret_cast<uintptr_t>(rhs) % align_of_data(), 0);
    }
};

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
        check_function(nested, {input}, type);
    }

    static void check_function(const AggregateFunctionPtr& nested, const Columns& inputs,
                               const DataTypePtr& state_type) {
        auto states = nested->create_serialize_column();
        std::vector<const IColumn*> columns;
        for (const auto& input : inputs) {
            columns.push_back(input.get());
        }
        Arena arena;
        // Serialize independently built states, as produced by *_state or *_combine.
        for (size_t row = 0; row < inputs[0]->size(); ++row) {
            auto* place = arena.aligned_alloc(nested->size_of_data(), nested->align_of_data());
            nested->create(place);
            DEFER(nested->destroy(place));
            nested->add(place, columns.data(), row, arena);
            auto serialized = nested->create_serialize_column();
            nested->serialize_without_key_to_column(place, *serialized);
            states->insert_range_from(*serialized, 0, 1);
        }
        for (bool combine : {false, true}) {
            SCOPED_TRACE(combine ? "combine reader" : "direct reader");
            auto reader = combine ? AggregateStateCombine::create(
                                            nested, nested->get_argument_types(), state_type)
                                  : nested;
            auto checked = std::make_shared<ScratchCheckingAggregateFunction>(
                    reader, DataTypes {state_type}, state_type);
            EXPECT_EQ(checked->needs_deserialize_and_merge_scratch(),
                      nested->needs_deserialize_and_merge_scratch());
            for (bool merge : {false, true}) {
                SCOPED_TRACE(merge ? "merge" : "union");
                auto function =
                        merge ? AggregateStateMerge::create(checked, {state_type},
                                                            nested->get_return_type())
                              : AggregateStateUnion::create(checked, {state_type}, state_type);
                check_batches(function, nested, *states, false);
                check_batches(function, nested, *states, true);
            }
            EXPECT_GT(checked->batch_calls, 0);
            EXPECT_GT(checked->selected_batch_calls, 0);
        }
    }

    static void check_scratch_requirement(const std::string& name, const DataTypes& arguments,
                                          const Columns& inputs, bool needs_scratch,
                                          bool result_nullable = false, bool null_v2 = false) {
        SCOPED_TRACE(name);
        AggregateFunctionAttr attr;
        attr.enable_aggregate_function_null_v2 = null_v2;
        auto nested = AggregateFunctionSimpleFactory::instance().get(
                name, arguments, nullptr, result_nullable,
                BeExecVersionManager::get_newest_version(), attr);
        ASSERT_NE(nested, nullptr);
        nested->set_version(BeExecVersionManager::get_newest_version());
        ASSERT_EQ(nested->needs_deserialize_and_merge_scratch(), needs_scratch);
        check_function(nested, inputs, nested->get_serialized_type());
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

TEST_F(AggregateStateUnionTest, NativeNumericReadersNeedNoScratch) {
    auto type = std::make_shared<DataTypeInt64>();
    auto input = ColumnHelper::create_column<DataTypeInt64>({2, 50, 7, 0, 11, 19});
    for (const auto* name : {"sum", "avg", "count", "min", "max", "bitmap_agg"}) {
        check_scratch_requirement(name, {type}, {input}, false);
    }
    check_scratch_requirement("count", {make_nullable(type)},
                              {ColumnHelper::create_nullable_column<DataTypeInt64>(
                                      {2, 50, 7, 0, 11, 19}, {0, 0, 0, 1, 0, 0})},
                              false);
}

TEST_F(AggregateStateUnionTest, NativeComplexReadersNeedNoScratch) {
    auto type = std::make_shared<DataTypeString>();
    const std::vector<std::string> values = {std::string(4096, 'a'), "b", "c", "",
                                             std::string(8192, 'd'), "e"};
    auto input = ColumnHelper::create_column<DataTypeString>(values);
    // Nullable inputs select the native array reader; non-nullable inputs select collect_list.
    check_scratch_requirement(
            "array_agg", {make_nullable(type)},
            {ColumnHelper::create_nullable_column<DataTypeString>(values, {0, 0, 0, 1, 0, 0})},
            false);
    for (const auto* name : {"map_agg_v1", "map_agg_v2"}) {
        check_scratch_requirement(name, {type, type}, {input, input}, false);
    }
    auto bitmap_type = std::make_shared<DataTypeBitMap>();
    auto bitmaps = ColumnBitmap::create();
    for (UInt64 value : {2, 50, 7, 0, 11, 19}) {
        bitmaps->insert_value(BitmapValue(value));
    }
    check_scratch_requirement("bitmap_union", {bitmap_type}, {std::move(bitmaps)}, false);
}

TEST_F(AggregateStateUnionTest, GenericAndStringMinMaxNeedScratch) {
    auto type = std::make_shared<DataTypeString>();
    auto input = ColumnHelper::create_column<DataTypeString>(
            {std::string(4096, 'a'), "z", "c", "", std::string(8192, 'b'), "e"});
    for (const auto* name : {"min", "max"}) {
        check_scratch_requirement(name, {type}, {input}, true);
    }
    check_scratch_requirement("array_agg", {type}, {input}, true);
}

TEST_F(AggregateStateUnionTest, NullableReadersPreserveScratchRequirement) {
    auto type = make_nullable(std::make_shared<DataTypeInt64>());
    for (bool all_null : {false, true}) {
        auto input = ColumnHelper::create_nullable_column<DataTypeInt64>(
                {2, 50, 7, 0, 11, 19}, all_null ? std::vector<UInt8> {1, 1, 1, 1, 1, 1}
                                                : std::vector<UInt8> {0, 0, 0, 1, 0, 0});
        for (bool null_v2 : {false, true}) {
            for (bool result_nullable : {false, true}) {
                check_scratch_requirement("sum", {type}, {input}, !null_v2, result_nullable,
                                          null_v2);
            }
            // The generic serialized-string reader still needs scratch inside Nullable V2.
            check_scratch_requirement("multi_distinct_count", {type}, {input}, true, false,
                                      null_v2);
        }
    }
}

TEST_F(AggregateStateUnionTest, FixedLengthColumnCanStillNeedScratch) {
    // This reader reconstructs a hash-set state from each fixed-width serialized count.
    check_scratch_requirement(
            "multi_distinct_count_distribute_key", {std::make_shared<DataTypeInt64>()},
            {ColumnHelper::create_column<DataTypeInt64>({2, 50, 7, 0, 11, 19})}, true);
}

} // namespace doris
