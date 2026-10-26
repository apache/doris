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

#include <cstddef>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "core/column/column.h"
#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "core/types.h"
#include "core/value/bitmap_value.h"
#include "exprs/aggregate/agg_function_test.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "gtest/gtest_pred_impl.h"

const int agg_test_batch_size = 10;

namespace doris {
// declare function
void register_aggregate_function_bitmap(AggregateFunctionSimpleFactory& factory);

TEST(AggBitmapTest, bitmap_union_test) {
    Arena arena;
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
    auto agg_function = factory.get(function_name, data_types, nullptr, false, -1);
    agg_function->set_version(3);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_bitmap.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnBitmap ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(ans.size(), 1);
    EXPECT_EQ(ans.get_element(0).cardinality(), agg_test_batch_size);
    agg_function->destroy(place);

    auto dst = agg_function->create_serialize_column();
    agg_function->streaming_agg_serialize_to_column(column, dst, agg_test_batch_size, arena);

    for (size_t i = 0; i != agg_test_batch_size; ++i) {
        EXPECT_EQ(std::to_string(i), assert_cast<ColumnBitmap&>(*dst).get_element(i).to_string());
    }
}

TEST(AggBitmapTest, group_bitmap_xor_test) {
    Arena arena;
    std::string function_name = "group_bitmap_xor";
    auto data_type = std::make_shared<DataTypeBitMap>();
    // Prepare test data.
    auto column_bitmap = data_type->create_column();

    // Insert first bitmap: {4, 7, 8}
    std::vector<int> values_1 = {4, 7, 8};
    BitmapValue bitmap_value_1;
    for (int value : values_1) {
        bitmap_value_1.add(value);
    }
    assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value_1);

    // Insert second bitmap: {1, 3, 6, 15}
    std::vector<int> values_2 = {1, 3, 6, 15};
    BitmapValue bitmap_value_2;
    for (int value : values_2) {
        bitmap_value_2.add(value);
    }
    assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value_2);

    // Insert third bitmap: {4, 7}
    std::vector<int> values_3 = {4, 7};
    BitmapValue bitmap_value_3;
    for (int value : values_3) {
        bitmap_value_3.add(value);
    }
    assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value_3);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_bitmap(factory);
    DataTypes data_types = {data_type};
    auto agg_function = factory.get(function_name, data_types, nullptr, false, -1);
    agg_function->set_version(3);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_bitmap.get()};
    for (int i = 0; i < 3; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnBitmap ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(ans.size(), 1);
    EXPECT_EQ(ans.get_element(0).cardinality(), 5); // Expected intersection: {1, 3, 6, 8, 15}
    agg_function->destroy(place);

    auto dst = agg_function->create_serialize_column();
    BitmapValue intersection_result = ans.get_element(0);
    assert_cast<ColumnBitmap&>(*dst).insert_value(intersection_result);

    std::vector<int> expected_values = {1, 3, 6, 8, 15};
    BitmapValue expected_intersection;
    for (int value : expected_values) {
        expected_intersection.add(value);
    }

    EXPECT_EQ(expected_intersection.to_string(),
              assert_cast<ColumnBitmap&>(*dst).get_element(0).to_string());
}

TEST(AggBitmapTest, bitmap_intersect_test) {
    Arena arena;
    std::string function_name = "bitmap_intersect";
    auto data_type = std::make_shared<DataTypeBitMap>();
    // Prepare test data.
    auto column_bitmap = data_type->create_column();

    // Insert first bitmap: {0, 1, 2, 3, 4, 5}
    std::vector<int> values_1 = {0, 1, 2, 3, 4, 5};
    BitmapValue bitmap_value_1;
    for (int value : values_1) {
        bitmap_value_1.add(value);
    }
    assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value_1);

    // Insert second bitmap: {0, 1, 2, 3, 100}
    std::vector<int> values_2 = {0, 1, 2, 3, 100};
    BitmapValue bitmap_value_2;
    for (int value : values_2) {
        bitmap_value_2.add(value);
    }
    assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value_2);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_bitmap(factory);
    DataTypes data_types = {data_type};
    auto agg_function = factory.get(function_name, data_types, nullptr, false, -1);
    agg_function->set_version(3);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_bitmap.get()};
    for (int i = 0; i < 2; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnBitmap ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(ans.size(), 1);
    EXPECT_EQ(ans.get_element(0).cardinality(), 4); // Expected intersection: {0, 1, 2, 3}
    agg_function->destroy(place);

    auto dst = agg_function->create_serialize_column();
    BitmapValue intersection_result = ans.get_element(0);
    assert_cast<ColumnBitmap&>(*dst).insert_value(intersection_result);

    BitmapValue expected_intersection;
    for (int i = 0; i < 4; i++) {
        expected_intersection.add(i);
    }

    EXPECT_EQ(expected_intersection.to_string(),
              assert_cast<ColumnBitmap&>(*dst).get_element(0).to_string());
}

class BitmapIntersectPartialStateTest : public testing::TestWithParam<bool> {
protected:
    void SetUp() override {
        AggregateFunctionSimpleFactory factory;
        register_aggregate_function_bitmap(factory);
        auto type = make_nullable(std::make_shared<DataTypeBitMap>());
        AggregateFunctionAttr attr;
        attr.enable_aggregate_function_null_v2 = GetParam();
        function = factory.get("bitmap_intersect_v2", {type}, nullptr, false, -1, attr);
        input = type->create_column();
        auto& nullable = assert_cast<ColumnNullable&>(*input);
        auto& bitmaps = assert_cast<ColumnBitmap&>(nullable.get_nested_column());
        for (const auto& values : {std::vector<uint64_t> {1, 2}, std::vector<uint64_t> {2, 3}}) {
            BitmapValue bitmap;
            for (auto value : values) {
                bitmap.add(value);
            }
            bitmaps.insert_value(bitmap);
            nullable.get_null_map_data().push_back(0);
        }
        input->insert_default();  // NULL-only partial state.
        bitmaps.insert_default(); // A valid empty bitmap must still clear the intersection.
        nullable.get_null_map_data().push_back(0);
        columns[0] = input.get();
        for (size_t row = 0; row < input->size(); ++row) {
            auto* place = arena.alloc(function->size_of_data());
            function->create(place);
            function->add(place, columns, row, arena);
            partials.push_back(place);
        }
        result = arena.alloc(function->size_of_data());
        function->create(result);
    }

    void TearDown() override {
        for (auto* place : partials) {
            function->destroy(place);
        }
        function->destroy(result);
    }

    void check(const std::string& expected) {
        ColumnBitmap output;
        function->insert_result_into(result, output);
        EXPECT_EQ(output.get_element(0).to_string(), expected);
    }

    Arena arena;
    AggregateFunctionPtr function;
    MutableColumnPtr input;
    const IColumn* columns[1];
    std::vector<AggregateDataPtr> partials;
    AggregateDataPtr result;
};

TEST_P(BitmapIntersectPartialStateTest, DirectMerge) {
    // The NULL partial is an identity in either merge order.
    for (const auto& order : {std::vector<size_t> {0, 1, 2}, std::vector<size_t> {2, 0, 1}}) {
        function->reset(result);
        for (auto row : order) {
            function->merge(result, partials[row], arena);
        }
        check("2");
    }
    function->merge(result, partials[3], arena);
    check("");
    function->reset(result);
    function->merge(result, partials[2], arena);
    check("");
}

TEST_P(BitmapIntersectPartialStateTest, SerializedMerge) {
    auto serialized = function->create_serialize_column();
    function->serialize_to_column(partials, 0, serialized, partials.size());
    function->deserialize_and_merge_from_column_range(result, *serialized, 0, 2, arena);
    check("2");
    function->deserialize_and_merge_from_column_range(result, *serialized, 3, 3, arena);
    check("");
    function->add(result, columns, 0, arena);
    check("");

    // A NULL row remains uninitialized; a valid row restores the flag before add().
    function->reset(result);
    function->deserialize_from_column_row(result, *serialized, 2, arena);
    function->add(result, columns, 0, arena);
    check("1,2");
    function->reset(result);
    function->deserialize_from_column_row(result, *serialized, 0, arena);
    function->add(result, columns, 1, arena);
    check("2");

    auto* scratch = arena.alloc(function->size_of_data() * 3);
    std::vector<AggregateDataPtr> destinations(3, result);
    function->reset(result);
    function->deserialize_and_merge_vec(destinations.data(), 0, scratch, serialized.get(), arena,
                                        3);
    check("2");
    function->reset(result);
    function->add(result, columns, 0, arena);
    destinations[0] = nullptr;
    function->deserialize_and_merge_vec_selected(destinations.data(), 0, scratch, serialized.get(),
                                                 arena, 3);
    check("2");
}

TEST_P(BitmapIntersectPartialStateTest, WithoutKeyAndStreamingMerge) {
    auto without_key = function->create_serialize_column();
    for (size_t row : {0, 1, 2}) {
        function->serialize_without_key_to_column(partials[row], *without_key);
    }
    function->deserialize_and_merge_from_column(result, *without_key, arena);
    check("2");

    auto streaming = function->create_serialize_column();
    function->streaming_agg_serialize_to_column(columns, streaming, 3, arena);
    function->reset(result);
    function->deserialize_and_merge_from_column(result, *streaming, arena);
    check("2");

    auto null_input = input->clone_empty();
    null_input->insert_from(*input, 2);
    const IColumn* null_columns[] = {null_input.get()};
    auto null_stream = function->create_serialize_column();
    function->streaming_agg_serialize_to_column(null_columns, null_stream, 1, arena);
    if (GetParam()) {
        EXPECT_TRUE(assert_cast<const ColumnNullable&>(*null_stream).is_null_at(0));
    } else {
        EXPECT_EQ(null_stream->get_data_at(0).data[0], 0);
    }
    function->deserialize_and_merge_from_column(result, *null_stream, arena);
    check("2");

    auto with_empty = function->create_serialize_column();
    function->streaming_agg_serialize_to_column(columns, with_empty, 4, arena);
    function->reset(result);
    function->deserialize_and_merge_from_column(result, *with_empty, arena);
    check("");
    function->add(result, columns, 0, arena);
    check("");
}

TEST_P(BitmapIntersectPartialStateTest, IntermediateValidityAndNonNullableResult) {
    EXPECT_FALSE(function->get_return_type()->is_nullable());
    EXPECT_EQ(function->get_serialized_type()->get_name(),
              GetParam() ? "Nullable(BitMap)" : "String");
    auto serialized = function->create_serialize_column();
    function->serialize_to_column(partials, 0, serialized, partials.size());
    if (GetParam()) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*serialized);
        EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 0, 1, 0}));
        EXPECT_EQ(assert_cast<const ColumnBitmap&>(nullable.get_nested_column())
                          .get_element(3)
                          .cardinality(),
                  0);
    } else {
        EXPECT_EQ(serialized->get_data_at(2).data[0], 0);
        EXPECT_EQ(serialized->get_data_at(3).data[0], 1);
    }
}

TEST_P(BitmapIntersectPartialStateTest, ByteRoundTripAndContinue) {
    auto serialized = ColumnString::create();
    VectorBufferWriter writer(*serialized);
    for (auto* partial : partials) {
        function->serialize(partial, writer);
        writer.commit();
    }
    // deserialize() replaces a previously populated state, including its flag.
    const std::vector<std::string> expected {"2", "", "2,3", ""};
    for (size_t row : {0, 2, 3}) {
        function->reset(result);
        function->add(result, columns, 1, arena);
        BufferReadable reader(serialized->get_data_at(row));
        function->deserialize(result, reader, arena);
        function->add(result, columns, 1, arena);
        check(expected[row]);
    }
    // A NULL column row must also replace both the old value and old validity.
    auto native = function->create_serialize_column();
    function->serialize_to_column(partials, 0, native, partials.size());
    function->reset(result);
    function->add(result, columns, 0, arena);
    function->deserialize_from_column_row(result, *native, 2, arena);
    function->add(result, columns, 1, arena);
    check("2,3");

    AggregateFunctionGuard scratch(function.get());
    for (const auto& order : {std::vector<size_t> {0, 1, 2}, std::vector<size_t> {2, 0, 1}}) {
        function->reset(result);
        for (auto row : order) {
            BufferReadable reader(serialized->get_data_at(row));
            function->deserialize_and_merge(result, scratch.data(), reader, arena);
        }
        check("2");
        BufferReadable empty_reader(serialized->get_data_at(3));
        function->deserialize_and_merge(result, scratch.data(), empty_reader, arena);
        check("");
    }
}

TEST_P(BitmapIntersectPartialStateTest, EmptyBatchAndNoNullFastPath) {
    auto no_nulls = input->clone_empty();
    no_nulls->insert_range_from(*input, 0, 2);
    const IColumn* no_null_columns[] = {no_nulls.get()};
    function->add_batch_single_place(0, result, no_null_columns, arena);
    auto serialized = function->create_serialize_column();
    function->serialize_without_key_to_column(result, *serialized);
    if (GetParam()) {
        EXPECT_TRUE(assert_cast<const ColumnNullable&>(*serialized).is_null_at(0));
    } else {
        EXPECT_EQ(serialized->get_data_at(0).data[0], 0);
    }

    std::vector<AggregateDataPtr> destinations(2, result);
    function->add_batch(2, destinations.data(), 0, no_null_columns, arena, false);
    check("2");
    function->reset(result);
    function->add_batch_single_place(2, result, no_null_columns, arena);
    check("2");
    function->reset(result);
    function->add_batch_range(0, 1, result, no_null_columns, arena, false);
    check("2");
    function->reset(result);
    destinations[0] = nullptr;
    function->add_batch_selected(2, destinations.data(), 0, no_null_columns, arena);
    check("2,3");
}

TEST_P(BitmapIntersectPartialStateTest, SelectedMergeWithOffsetAndRepeatedDestination) {
    constexpr size_t offset = 16;
    auto* base = arena.alloc(function->size_of_data() + offset);
    auto* state = base + offset;
    function->create(state);
    auto serialized = function->create_serialize_column();
    function->serialize_to_column(partials, 0, serialized, partials.size());
    auto* scratch = arena.alloc(function->size_of_data() * partials.size());
    std::vector<AggregateDataPtr> destinations {base, base, base, nullptr};
    function->deserialize_and_merge_vec_selected(destinations.data(), offset, scratch,
                                                 serialized.get(), arena, partials.size());
    ColumnBitmap output;
    function->insert_result_into(state, output);
    EXPECT_EQ(output.get_element(0).to_string(), "2");
    function->destroy(state);
}

TEST(AggBitmapTest, BitmapIntersectNonNullableAndLegacyFormats) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_bitmap(factory);
    auto bitmap_type = std::make_shared<DataTypeBitMap>();
    for (bool null_v2 : {false, true}) {
        AggregateFunctionAttr attr;
        attr.enable_aggregate_function_null_v2 = null_v2;
        auto function = factory.get("bitmap_intersect_v2", {bitmap_type}, nullptr, false, -1, attr);
        EXPECT_EQ(function->get_serialized_type()->get_name(),
                  null_v2 ? "Nullable(BitMap)" : "String");
        EXPECT_FALSE(function->get_return_type()->is_nullable());
        AggregateFunctionGuard state(function.get());
        AggregateFunctionGuard empty_state(function.get());
        ColumnBitmap output;
        function->insert_result_into(state.data(), output);
        EXPECT_EQ(output.get_element(0).cardinality(), 0);

        Arena arena;
        auto input = ColumnBitmap::create();
        input->insert_value(BitmapValue(std::vector<uint64_t> {1, 2}));
        input->insert_value(BitmapValue(std::vector<uint64_t> {2, 3}));
        const IColumn* columns[] = {input.get()};
        function->check_input_columns_type(columns);
        function->add(state.data(), columns, 0, arena);
        function->merge(state.data(), empty_state.data(), arena);
        auto serialized_empty = function->create_serialize_column();
        function->serialize_without_key_to_column(empty_state.data(), *serialized_empty);
        function->deserialize_and_merge_from_column(state.data(), *serialized_empty, arena);
        output.clear();
        function->insert_result_into(state.data(), output);
        EXPECT_EQ(output.get_element(0).to_string(), "1,2");

        auto streaming = function->create_serialize_column();
        function->streaming_agg_serialize_to_column(columns, streaming, input->size(), arena);
        function->reset(state.data());
        function->deserialize_and_merge_from_column(state.data(), *streaming, arena);
        output.clear();
        function->insert_result_into(state.data(), output);
        EXPECT_EQ(output.get_element(0).to_string(), "2");

        auto legacy = factory.get("bitmap_intersect", {make_nullable(bitmap_type)}, nullptr, false,
                                  -1, attr);
        // The legacy decoder and other functions retain the session option's format.
        EXPECT_EQ(legacy->get_serialized_type()->get_name(), null_v2 ? "BitMap" : "String");
        auto union_function =
                factory.get("bitmap_union", {make_nullable(bitmap_type)}, nullptr, false, -1, attr);
        EXPECT_EQ(union_function->get_serialized_type()->get_name(), null_v2 ? "BitMap" : "String");
    }
}

INSTANTIATE_TEST_SUITE_P(NullWrappers, BitmapIntersectPartialStateTest, testing::Bool());

TEST(AggBitmapTest, bitmap_union_count_test) {
    Arena arena;
    std::string function_name = "bitmap_union_count";
    auto data_type = std::make_shared<DataTypeBitMap>();
    // Prepare test data.
    auto column_bitmap = data_type->create_column();

    std::vector<int> values = {1, 2, 3, 4, 5, 1, 2, 3, 100};
    auto values_size = values.size();
    for (int value : values) {
        BitmapValue bitmap_value(value);
        assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value);
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_bitmap(factory);
    auto agg_function = factory.get(function_name, {data_type}, nullptr, false, -1);
    agg_function->set_version(3);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_bitmap.get()};
    for (int i = 0; i < values_size; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnInt64 ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(ans.get_element(0), 6);
    agg_function->destroy(place);
}

template <PrimitiveType T>
void validate_bitmap_union_int_test() {
    Arena arena;
    std::string function_name = "bitmap_union_int";
    DataTypePtr data_type;
    if constexpr (T == TYPE_TINYINT) {
        data_type = std::make_shared<DataTypeInt8>();
    } else if constexpr (T == TYPE_SMALLINT) {
        data_type = std::make_shared<DataTypeInt16>();
    } else if constexpr (T == TYPE_INT) {
        data_type = std::make_shared<DataTypeInt32>();
    } else if constexpr (T == TYPE_BIGINT) {
        data_type = std::make_shared<DataTypeInt64>();
    } else {
        LOG(FATAL) << "unsupported type";
    }

    // Prepare test data.
    auto column_int = data_type->create_column();

    std::vector<typename PrimitiveTypeTraits<T>::CppType> values = {1, 2, 3, 4, 5, 1, 2, 3, 100};
    auto values_size = values.size();
    for (int value : values) {
        (*column_int).insert(Field::create_field<T>(value));
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_bitmap(factory);
    auto agg_function = factory.get(function_name, {data_type}, nullptr, false, -1);
    agg_function->set_version(3);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_int.get()};
    for (int i = 0; i < values_size; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnInt64 ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(ans.get_element(0), 6);
    agg_function->destroy(place);
}

TEST(AggBitmapTest, bitmap_union_int_test) {
    validate_bitmap_union_int_test<TYPE_TINYINT>();
    validate_bitmap_union_int_test<TYPE_SMALLINT>();
    validate_bitmap_union_int_test<TYPE_INT>();
    validate_bitmap_union_int_test<TYPE_BIGINT>();
}

class BitmapIntegerAggregateTest : public AggregateFunctiontest {
protected:
    template <PrimitiveType T>
    void check_input(const std::vector<typename PrimitiveTypeTraits<T>::CppType>& values,
                     const BitmapValue& expected, bool nullable) {
        DataTypePtr type = std::make_shared<typename PrimitiveTypeTraits<T>::DataType>();
        if (nullable) {
            type = make_nullable(type);
        }
        auto input = type->create_column();
        for (auto value : values) {
            input->insert(Field::create_field<T>(value));
        }
        if (nullable) {
            input->insert_default();
        }
        Block block({{std::move(input), type, "input"}});

        // The shared helper covers single-place batches, streaming aggregation,
        // reset, serialization and all column-based merge paths.
        create_agg("bitmap_agg", false, {type}, std::make_shared<DataTypeBitMap>());
        execute(block, ColumnHelper::create_column_with_name<DataTypeBitMap>({expected}));
        create_agg("bitmap_union_int", false, {type}, std::make_shared<DataTypeInt64>());
        execute(block, ColumnHelper::create_column_with_name<DataTypeInt64>(
                               {static_cast<Int64>(expected.cardinality())}));
    }

    template <PrimitiveType T>
    void check_negative_inputs() {
        using CppType = typename PrimitiveTypeTraits<T>::CppType;
        constexpr auto min = std::numeric_limits<CppType>::min();
        constexpr auto max = std::numeric_limits<CppType>::max();
        for (bool nullable : {false, true}) {
            SCOPED_TRACE(nullable);
            check_input<T>({-1}, BitmapValue(), nullable);
            check_input<T>({min, -2, -1, -1}, BitmapValue(), nullable);
            BitmapValue expected(std::vector<uint64_t> {0, 1, static_cast<uint64_t>(max)});
            check_input<T>({min, -2, -1, 0, 1, 1, max}, expected, nullable);
            check_input<T>({0, 1, 1, max}, expected, nullable);

            // Cross the small-set threshold and exercise repeated negative values.
            std::vector<CppType> dense_values;
            BitmapValue dense_expected;
            for (int i = 0; i < 100; ++i) {
                dense_values.push_back(-1);
                dense_values.push_back(i);
                dense_expected.add(i);
            }
            check_input<T>(dense_values, dense_expected, nullable);
        }
        // One NULL and no non-NULL values.
        check_input<T>({}, BitmapValue(), true);
    }
};

TEST_F(BitmapIntegerAggregateTest, IgnoreNegativeTinyInt) {
    check_negative_inputs<TYPE_TINYINT>();
}

TEST_F(BitmapIntegerAggregateTest, IgnoreNegativeSmallInt) {
    check_negative_inputs<TYPE_SMALLINT>();
}

TEST_F(BitmapIntegerAggregateTest, IgnoreNegativeInt) {
    check_negative_inputs<TYPE_INT>();
}

TEST_F(BitmapIntegerAggregateTest, IgnoreNegativeBigInt) {
    check_negative_inputs<TYPE_BIGINT>();
}

TEST_F(BitmapIntegerAggregateTest, PreserveUnsignedBitmapValues) {
    const BitmapValue bitmap(std::vector<uint64_t> {0, std::numeric_limits<uint64_t>::max()});
    for (bool nullable : {false, true}) {
        DataTypePtr type = std::make_shared<DataTypeBitMap>();
        if (nullable) {
            type = make_nullable(type);
        }
        auto input = type->create_column();
        input->insert(Field::create_field<TYPE_BITMAP>(bitmap));
        input->insert(Field::create_field<TYPE_BITMAP>(bitmap));
        if (nullable) {
            input->insert_default();
        }
        create_agg("bitmap_union_count", false, {type}, std::make_shared<DataTypeInt64>());
        execute(Block({{std::move(input), type, "input"}}),
                ColumnHelper::create_column_with_name<DataTypeInt64>({2}));
    }
}

} // namespace doris
