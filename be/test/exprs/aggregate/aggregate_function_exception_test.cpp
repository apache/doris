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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "core/arena.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_foreach.h"
#include "exprs/aggregate/aggregate_function_null.h"

namespace doris {

struct TrackingAggregateState {
    TrackingAggregateState() { ++construct_count; }
    ~TrackingAggregateState() { ++destroy_count; }

    static void reset_counters() {
        construct_count = 0;
        destroy_count = 0;
        merge_count = 0;
    }

    static int construct_count;
    static int destroy_count;
    static int merge_count;
};

int TrackingAggregateState::construct_count = 0;
int TrackingAggregateState::destroy_count = 0;
int TrackingAggregateState::merge_count = 0;

struct PairSumAggregateState {
    Int64 value = 0;
};

struct HeapStringAggregateState {
    ~HeapStringAggregateState() { live_bytes -= value.size(); }

    void assign(StringRef source) {
        live_bytes -= value.size();
        value.assign(source.data, source.size);
        live_bytes += value.size();
        peak_live_bytes = std::max(peak_live_bytes, live_bytes);
    }

    static void reset_counters() {
        live_bytes = 0;
        peak_live_bytes = 0;
    }

    static size_t live_bytes;
    static size_t peak_live_bytes;
    String value;
};

size_t HeapStringAggregateState::live_bytes = 0;
size_t HeapStringAggregateState::peak_live_bytes = 0;

class PairSumAggregateFunction final
        : public IAggregateFunctionDataHelper<PairSumAggregateState, PairSumAggregateFunction> {
public:
    explicit PairSumAggregateFunction(std::vector<size_t>* observed_column_sizes = nullptr)
            : IAggregateFunctionDataHelper<PairSumAggregateState, PairSumAggregateFunction>(
                      DataTypes {std::make_shared<DataTypeInt32>(),
                                 std::make_shared<DataTypeInt32>()}),
              observed_column_sizes(observed_column_sizes) {}

    String get_name() const override { return "pair_sum"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        if (observed_column_sizes != nullptr) {
            observed_column_sizes->push_back(columns[0]->size());
        }
        data(place).value += assert_cast<const ColumnInt32&>(*columns[0]).get_data()[row_num] +
                             assert_cast<const ColumnInt32&>(*columns[1]).get_data()[row_num];
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena&) const override {
        data(place).value += data(rhs).value;
    }

    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        buf.write_binary(data(place).value);
    }

    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena&) const override {
        buf.read_binary(data(place).value);
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).insert_value(data(place).value);
    }

private:
    std::vector<size_t>* observed_column_sizes;
};

class HeapStringAggregateFunction final
        : public IAggregateFunctionDataHelper<HeapStringAggregateState,
                                              HeapStringAggregateFunction> {
public:
    HeapStringAggregateFunction()
            : IAggregateFunctionDataHelper<HeapStringAggregateState, HeapStringAggregateFunction>(
                      DataTypes {std::make_shared<DataTypeString>(),
                                 std::make_shared<DataTypeInt32>()}) {}

    String get_name() const override { return "heap_string"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        data(place).assign(assert_cast<const ColumnString&>(*columns[0]).get_data_at(row_num));
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena&) const override {
        data(place).assign(StringRef(data(rhs).value));
    }

    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        buf.write_binary(data(place).value);
    }

    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena&) const override {
        String value;
        buf.read_binary(value);
        data(place).assign(StringRef(value));
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnString&>(to).insert_data(data(place).value.data(),
                                                   data(place).value.size());
    }
};

class ThrowOnDeserializeAggregateFunction final
        : public IAggregateFunctionDataHelper<TrackingAggregateState,
                                              ThrowOnDeserializeAggregateFunction> {
public:
    ThrowOnDeserializeAggregateFunction()
            : IAggregateFunctionDataHelper<TrackingAggregateState,
                                           ThrowOnDeserializeAggregateFunction>(
                      DataTypes {std::make_shared<DataTypeString>()}) {}

    String get_name() const override { return "throw_on_deserialize"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr, const IColumn**, ssize_t, Arena&) const override {}

    void merge(AggregateDataPtr, ConstAggregateDataPtr, Arena&) const override {}

    void serialize(ConstAggregateDataPtr, BufferWritable& buf) const override {
        String payload;
        buf.write_binary(payload);
    }

    void deserialize(AggregateDataPtr, BufferReadable& buf, Arena&) const override {
        String payload;
        buf.read_binary(payload);
        if (payload == "throw") {
            throw Exception(ErrorCode::INTERNAL_ERROR, "mock deserialize failure");
        }
    }

    void insert_result_into(ConstAggregateDataPtr, IColumn&) const override {}
};

class ThrowOnSecondMergeAggregateFunction final
        : public IAggregateFunctionDataHelper<TrackingAggregateState,
                                              ThrowOnSecondMergeAggregateFunction> {
public:
    ThrowOnSecondMergeAggregateFunction()
            : IAggregateFunctionDataHelper<TrackingAggregateState,
                                           ThrowOnSecondMergeAggregateFunction>(
                      DataTypes {std::make_shared<DataTypeString>()}) {}

    String get_name() const override { return "throw_on_second_merge"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }

    void add(AggregateDataPtr, const IColumn**, ssize_t, Arena&) const override {}

    void merge(AggregateDataPtr, ConstAggregateDataPtr, Arena&) const override {
        if (++TrackingAggregateState::merge_count == 2) {
            throw Exception(ErrorCode::MEM_ALLOC_FAILED, "mock merge allocation failure");
        }
    }

    void serialize(ConstAggregateDataPtr, BufferWritable&) const override {}

    void deserialize(AggregateDataPtr, BufferReadable&, Arena&) const override {}

    void insert_result_into(ConstAggregateDataPtr, IColumn&) const override {}
};

class AggregateFunctionExceptionTest : public testing::Test {
protected:
    void SetUp() override { TrackingAggregateState::reset_counters(); }

    MutableColumnPtr make_column(std::initializer_list<String> payloads) {
        auto column = ColumnString::create();
        VectorBufferWriter writer(*column);
        for (const auto& payload : payloads) {
            writer.write_binary(payload);
            writer.commit();
        }
        return column;
    }

    ThrowOnDeserializeAggregateFunction function;
    Arena arena;
};

TEST_F(AggregateFunctionExceptionTest, DeserializeVecDestroysCurrentStateOnFailure) {
    auto column = make_column({"ok", "throw"});
    std::vector<char> states(function.size_of_data() * 2);

    bool thrown = false;
    try {
        function.deserialize_vec(states.data(), static_cast<ColumnString*>(column.get()), arena, 2);
    } catch (const Exception&) {
        thrown = true;
    }

    EXPECT_TRUE(thrown);
    if (!thrown) {
        function.destroy_vec(states.data(), 2);
    }
    EXPECT_EQ(TrackingAggregateState::construct_count, 2);
    EXPECT_EQ(TrackingAggregateState::destroy_count, 2);
}

TEST_F(AggregateFunctionExceptionTest, DeserializeAndMergeVecDestroysRhsStateOnFailure) {
    auto column = make_column({"throw"});
    std::vector<char> place_storage(function.size_of_data());
    std::vector<char> rhs_storage(function.size_of_data());
    auto* place = place_storage.data();
    function.create(place);

    std::array<AggregateDataPtr, 1> places {place};
    const auto destroy_count_before_call = TrackingAggregateState::destroy_count;
    bool thrown = false;
    try {
        function.deserialize_and_merge_vec(places.data(), 0, rhs_storage.data(), column.get(),
                                           arena, 1);
    } catch (const Exception&) {
        thrown = true;
    }

    EXPECT_TRUE(thrown);
    EXPECT_EQ(TrackingAggregateState::destroy_count - destroy_count_before_call, 1);

    function.destroy(place);
    EXPECT_EQ(TrackingAggregateState::construct_count, TrackingAggregateState::destroy_count);
}

TEST_F(AggregateFunctionExceptionTest,
       DeserializeAndMergeVecSelectedDestroysAllCreatedRhsStatesOnFailure) {
    auto column = make_column({"skip", "throw"});
    std::vector<char> place_storage(function.size_of_data());
    std::vector<char> rhs_storage(function.size_of_data() * 2);
    auto* place = place_storage.data();
    function.create(place);

    std::array<AggregateDataPtr, 2> places {nullptr, place};
    const auto destroy_count_before_call = TrackingAggregateState::destroy_count;
    bool thrown = false;
    try {
        function.deserialize_and_merge_vec_selected(places.data(), 0, rhs_storage.data(),
                                                    column.get(), arena, 2);
    } catch (const Exception&) {
        thrown = true;
    }

    EXPECT_TRUE(thrown);
    EXPECT_EQ(TrackingAggregateState::destroy_count - destroy_count_before_call, 2);

    function.destroy(place);
    EXPECT_EQ(TrackingAggregateState::construct_count, TrackingAggregateState::destroy_count);
}

TEST_F(AggregateFunctionExceptionTest, ForEachGrowthPreservesOldStatesWhenMergeThrows) {
    auto nested_function = std::make_shared<ThrowOnSecondMergeAggregateFunction>();
    auto input_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    AggregateFunctionForEach foreach_function(nested_function, DataTypes {input_type});
    auto input_column = input_type->create_column();
    input_column->insert(
            Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_STRING>(String("a")),
                                                   Field::create_field<TYPE_STRING>(String("b"))}));
    input_column->insert(
            Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_STRING>(String("c")),
                                                   Field::create_field<TYPE_STRING>(String("d")),
                                                   Field::create_field<TYPE_STRING>(String("e"))}));
    const IColumn* columns[] = {input_column.get()};

    {
        AggregateFunctionGuard state(&foreach_function);
        foreach_function.add(state.data(), columns, 0, arena);

        try {
            foreach_function.add(state.data(), columns, 1, arena);
            FAIL() << "Expected doris::Exception";
        } catch (const doris::Exception& e) {
            EXPECT_EQ(e.code(), doris::ErrorCode::MEM_ALLOC_FAILED);
        }

        EXPECT_EQ(TrackingAggregateState::merge_count, 2);
        EXPECT_EQ(TrackingAggregateState::construct_count, 5);
        EXPECT_EQ(TrackingAggregateState::destroy_count, 3);
    }

    EXPECT_EQ(TrackingAggregateState::construct_count, TrackingAggregateState::destroy_count);
}

TEST_F(AggregateFunctionExceptionTest, ForEachReadsEachArgumentFromItsOwnRowOffset) {
    auto nested_function = std::make_shared<PairSumAggregateFunction>();
    auto input_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    AggregateFunctionForEach foreach_function(nested_function, DataTypes {input_type, input_type});

    auto compacted_data = ColumnInt32::create();
    compacted_data->get_data().assign({30, 40});
    auto compacted_offsets = ColumnArray::ColumnOffsets::create();
    compacted_offsets->get_data().assign({0, 2});
    auto compacted = ColumnArray::create(std::move(compacted_data), std::move(compacted_offsets));

    auto original_data = ColumnInt32::create();
    original_data->get_data().assign({10, 20, 300, 400});
    auto original_offsets = ColumnArray::ColumnOffsets::create();
    original_offsets->get_data().assign({2, 4});
    auto original = ColumnArray::create(std::move(original_data), std::move(original_offsets));
    const IColumn* columns[] = {compacted.get(), original.get()};

    AggregateFunctionGuard state(&foreach_function);
    ASSERT_NO_THROW(foreach_function.add(state.data(), columns, 1, arena));

    auto result = foreach_function.get_return_type()->create_column();
    foreach_function.insert_result_into(state.data(), *result);
    const auto& result_array = assert_cast<const ColumnArray&>(*result);
    const auto& result_data = assert_cast<const ColumnInt64&>(
            assert_cast<const ColumnNullable&>(result_array.get_data()).get_nested_column());
    EXPECT_EQ(result_data.get_data(), ColumnInt64::Container({330, 440}));
}

TEST_F(AggregateFunctionExceptionTest, ForEachNormalizesShiftedOffsetsOncePerBatch) {
    std::vector<size_t> observed_column_sizes;
    auto nested_function = std::make_shared<PairSumAggregateFunction>(&observed_column_sizes);
    auto input_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    AggregateFunctionForEach foreach_function(nested_function, DataTypes {input_type, input_type});

    auto compacted_data = ColumnInt32::create();
    compacted_data->get_data().assign({30, 40});
    auto compacted_offsets = ColumnArray::ColumnOffsets::create();
    compacted_offsets->get_data().assign({0, 1, 2});
    auto compacted = ColumnArray::create(std::move(compacted_data), std::move(compacted_offsets));

    auto original_data = ColumnInt32::create();
    original_data->get_data().assign({10, 20, 300, 400});
    auto original_offsets = ColumnArray::ColumnOffsets::create();
    original_offsets->get_data().assign({2, 3, 4});
    auto original = ColumnArray::create(std::move(original_data), std::move(original_offsets));
    const IColumn* columns[] = {compacted.get(), original.get()};

    AggregateFunctionGuard state(&foreach_function);
    std::array<AggregateDataPtr, 3> places {nullptr, state.data(), state.data()};
    foreach_function.add_batch_selected(places.size(), places.data(), 0, columns, arena);

    EXPECT_EQ(observed_column_sizes, std::vector<size_t>({2, 2}));
    auto result = foreach_function.get_return_type()->create_column();
    foreach_function.insert_result_into(state.data(), *result);
    const auto& result_array = assert_cast<const ColumnArray&>(*result);
    const auto& result_data = assert_cast<const ColumnInt64&>(
            assert_cast<const ColumnNullable&>(result_array.get_data()).get_nested_column());
    EXPECT_EQ(result_data.get_data(), ColumnInt64::Container({770}));
}

TEST_F(AggregateFunctionExceptionTest, NullableForEachNormalizesVisibleRowsOncePerBatch) {
    std::vector<size_t> observed_column_sizes;
    auto nested_function = std::make_shared<PairSumAggregateFunction>(&observed_column_sizes);
    auto input_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    auto* foreach_function =
            new AggregateFunctionForEach(nested_function, DataTypes {input_type, input_type});
    AggregateFunctionNullVariadicInline<AggregateFunctionForEach, true> nullable_foreach(
            foreach_function, DataTypes {make_nullable(input_type), make_nullable(input_type)},
            false);

    auto compacted_data = ColumnInt32::create();
    compacted_data->get_data().assign({30, 40});
    auto compacted_offsets = ColumnArray::ColumnOffsets::create();
    compacted_offsets->get_data().assign({0, 1, 2});
    auto compacted = ColumnArray::create(std::move(compacted_data), std::move(compacted_offsets));
    auto compacted_nullable =
            ColumnNullable::create(std::move(compacted), ColumnUInt8::create(3, 0));
    compacted_nullable->get_null_map_data()[0] = 1;

    auto original_data = ColumnInt32::create();
    original_data->get_data().assign({10, 20, 300, 400});
    auto original_offsets = ColumnArray::ColumnOffsets::create();
    original_offsets->get_data().assign({2, 3, 4});
    auto original = ColumnArray::create(std::move(original_data), std::move(original_offsets));
    auto original_nullable = ColumnNullable::create(std::move(original), ColumnUInt8::create(3, 0));
    original_nullable->get_null_map_data()[0] = 1;
    const IColumn* columns[] = {compacted_nullable.get(), original_nullable.get()};

    AggregateFunctionGuard state(&nullable_foreach);
    nullable_foreach.add_batch_single_place(3, state.data(), columns, arena);

    EXPECT_EQ(observed_column_sizes, std::vector<size_t>({2, 2}));
    auto result = nullable_foreach.get_return_type()->create_column();
    nullable_foreach.insert_result_into(state.data(), *result);
    const auto& nullable_result = assert_cast<const ColumnNullable&>(*result);
    ASSERT_EQ(nullable_result.get_null_map_data(), ColumnUInt8::Container({0}));
    const auto& result_array = assert_cast<const ColumnArray&>(nullable_result.get_nested_column());
    const auto& result_data = assert_cast<const ColumnInt64&>(
            assert_cast<const ColumnNullable&>(result_array.get_data()).get_nested_column());
    EXPECT_EQ(result_data.get_data(), ColumnInt64::Container({770}));
}

TEST_F(AggregateFunctionExceptionTest, NullableForEachStreamingNormalizesVisibleRowsOncePerBatch) {
    std::vector<size_t> observed_column_sizes;
    auto nested_function = std::make_shared<PairSumAggregateFunction>(&observed_column_sizes);
    auto input_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    auto* foreach_function =
            new AggregateFunctionForEach(nested_function, DataTypes {input_type, input_type});
    AggregateFunctionNullVariadicInline<AggregateFunctionForEach, true> nullable_foreach(
            foreach_function, DataTypes {make_nullable(input_type), make_nullable(input_type)},
            false);

    auto compacted_data = ColumnInt32::create();
    compacted_data->get_data().assign({30, 40});
    auto compacted_offsets = ColumnArray::ColumnOffsets::create();
    compacted_offsets->get_data().assign({0, 1, 2});
    auto compacted = ColumnArray::create(std::move(compacted_data), std::move(compacted_offsets));
    auto compacted_nullable =
            ColumnNullable::create(std::move(compacted), ColumnUInt8::create(3, 0));
    compacted_nullable->get_null_map_data()[0] = 1;

    auto original_data = ColumnInt32::create();
    original_data->get_data().assign({10, 20, 300, 400});
    auto original_offsets = ColumnArray::ColumnOffsets::create();
    original_offsets->get_data().assign({2, 3, 4});
    auto original = ColumnArray::create(std::move(original_data), std::move(original_offsets));
    auto original_nullable = ColumnNullable::create(std::move(original), ColumnUInt8::create(3, 0));
    original_nullable->get_null_map_data()[0] = 1;
    const IColumn* columns[] = {compacted_nullable.get(), original_nullable.get()};
    MutableColumnPtr serialized = ColumnString::create();

    nullable_foreach.streaming_agg_serialize_to_column(columns, serialized, 3, arena);

    EXPECT_EQ(observed_column_sizes, std::vector<size_t>({2, 2}));
    EXPECT_EQ(serialized->size(), 3);

    auto result = nullable_foreach.get_return_type()->create_column();
    const auto& serialized_column = assert_cast<const ColumnString&>(*serialized);
    for (size_t row = 0; row < serialized_column.size(); ++row) {
        AggregateFunctionGuard state(&nullable_foreach);
        VectorBufferReader reader(serialized_column.get_data_at(row));
        nullable_foreach.deserialize(state.data(), reader, arena);
        nullable_foreach.insert_result_into(state.data(), *result);
    }
    const auto& nullable_result = assert_cast<const ColumnNullable&>(*result);
    EXPECT_EQ(nullable_result.get_null_map_data(), ColumnUInt8::Container({1, 0, 0}));
    const auto& result_array = assert_cast<const ColumnArray&>(nullable_result.get_nested_column());
    const auto& result_data = assert_cast<const ColumnInt64&>(
            assert_cast<const ColumnNullable&>(result_array.get_data()).get_nested_column());
    EXPECT_EQ(result_data.get_data(), ColumnInt64::Container({330, 440}));
}

TEST_F(AggregateFunctionExceptionTest, NullableForEachStreamingReleasesHeapStatePerRow) {
    constexpr size_t row_count = 128;
    constexpr size_t array_size = 8;
    constexpr size_t payload_size = 4096;

    HeapStringAggregateState::reset_counters();
    auto nested_function = std::make_shared<HeapStringAggregateFunction>();
    auto string_array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    auto int_array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    auto* foreach_function = new AggregateFunctionForEach(
            nested_function, DataTypes {string_array_type, int_array_type});
    AggregateFunctionNullVariadicInline<AggregateFunctionForEach, true> nullable_foreach(
            foreach_function,
            DataTypes {make_nullable(string_array_type), make_nullable(int_array_type)}, false);

    const String payload(payload_size, 'x');
    auto string_data = ColumnString::create();
    auto key_data = ColumnInt32::create();
    auto string_offsets = ColumnArray::ColumnOffsets::create();
    auto key_offsets = ColumnArray::ColumnOffsets::create();
    for (size_t row = 0; row < row_count; ++row) {
        for (size_t element = 0; element < array_size; ++element) {
            string_data->insert_data(payload.data(), payload.size());
            key_data->insert_value(static_cast<Int32>(element));
        }
        string_offsets->get_data().push_back((row + 1) * array_size);
        key_offsets->get_data().push_back((row + 1) * array_size);
    }
    auto strings = ColumnNullable::create(
            ColumnArray::create(std::move(string_data), std::move(string_offsets)),
            ColumnUInt8::create(row_count, 0));
    auto keys =
            ColumnNullable::create(ColumnArray::create(std::move(key_data), std::move(key_offsets)),
                                   ColumnUInt8::create(row_count, 0));
    const IColumn* columns[] = {strings.get(), keys.get()};
    MutableColumnPtr serialized = ColumnString::create();

    nullable_foreach.streaming_agg_serialize_to_column(columns, serialized, row_count, arena);

    EXPECT_EQ(serialized->size(), row_count);
    EXPECT_LE(HeapStringAggregateState::peak_live_bytes, array_size * payload_size);
    EXPECT_EQ(HeapStringAggregateState::live_bytes, 0);
}

} // namespace doris
