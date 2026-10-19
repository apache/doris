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
#include <array>
#include <cstring>

#include "agent/be_exec_version_manager.h"
#include "core/arena.h"
#include "core/block/block.h"
#include "core/column/column_complex.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"
#include "core/value/bitmap_value.h"
#include "exprs/aggregate/aggregate_function_null_v2.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/aggregate_function_state_union.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris {
namespace {

AggregateFunctionPtr nullable_function(const std::string& name, const DataTypes& types,
                                       bool result_nullable = true) {
    auto function = AggregateFunctionSimpleFactory::instance().get(
            name, types, nullptr, result_nullable, BeExecVersionManager::get_newest_version());
    EXPECT_NE(function, nullptr);
    return function;
}

MutableColumnPtr nullable_ints(std::initializer_list<Int64> values,
                               std::initializer_list<UInt8> nulls) {
    auto data = ColumnInt64::create();
    data->get_data().assign(values);
    auto null_map = ColumnUInt8::create();
    null_map->get_data().assign(nulls);
    return ColumnNullable::create(std::move(data), std::move(null_map));
}

void add_column(const AggregateFunctionPtr& function, AggregateDataPtr place, const IColumn& column,
                Arena& arena) {
    const IColumn* columns[] = {&column};
    function->add_batch_single_place(column.size(), place, columns, arena);
}

// Compare scalar accumulation with streaming -> batch merge and the single-row reader.
// Selected-out rows deliberately contain no contribution and must never be decoded.
void check_streaming(const AggregateFunctionPtr& function, const Columns& inputs,
                     bool poison_native_nulls = false) {
    Arena arena;
    std::vector<const IColumn*> columns;
    for (const auto& input : inputs) {
        columns.push_back(input.get());
    }
    const size_t rows = inputs[0]->size();
    function->check_input_columns_type(columns.data());
    auto serialized = function->create_serialize_column();
    function->streaming_agg_serialize_to_column(columns.data(), serialized, rows, arena);
    ASSERT_EQ(serialized->size(), rows);
    ASSERT_TRUE(function->get_serialized_type()->check_column(*serialized).ok());
    if (poison_native_nulls) {
        auto& nullable = assert_cast<ColumnNullable&>(*serialized);
        auto& payload = assert_cast<ColumnFixedLengthObject&>(nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            if (nullable.is_null_at(row)) {
                // A NULL mark makes the payload irrelevant, even for native numeric readers.
                std::memset(payload.get_data().data() + row * payload.item_size(), 0x7f,
                            payload.item_size());
            }
        }
    }
    for (bool selected : {false, true}) {
        AggregateFunctionGuard expected(function.get());
        AggregateFunctionGuard actual(function.get());
        std::vector<AggregateDataPtr> places(rows, actual.data());
        for (size_t i = 0; i < rows; ++i) {
            if (selected && i % 3 == 0) {
                places[i] = nullptr;
            } else {
                function->add(expected.data(), columns.data(), i, arena);
            }
        }
        Arena scratch_arena;
        AggregateDataPtr scratch = nullptr;
        if (function->needs_deserialize_and_merge_scratch()) {
            scratch = scratch_arena.aligned_alloc(rows * function->size_of_data(),
                                                  function->align_of_data());
        }
        if (selected) {
            function->deserialize_and_merge_vec_selected(places.data(), 0, scratch,
                                                         serialized.get(), arena, rows);
        } else {
            function->deserialize_and_merge_vec(places.data(), 0, scratch, serialized.get(), arena,
                                                rows);
        }
        auto expected_result = function->get_return_type()->create_column();
        auto actual_result = expected_result->clone_empty();
        function->insert_result_into(expected.data(), *expected_result);
        function->insert_result_into(actual.data(), *actual_result);
        EXPECT_EQ(actual_result->compare_at(0, 0, *expected_result, 1), 0);
    }
    for (size_t i = 0; i < rows; ++i) {
        AggregateFunctionGuard actual(function.get());
        AggregateFunctionGuard expected(function.get());
        function->deserialize_from_column_row(actual.data(), *serialized, i, arena);
        function->add(expected.data(), columns.data(), i, arena);
        auto expected_result = function->get_return_type()->create_column();
        auto actual_result = expected_result->clone_empty();
        function->insert_result_into(expected.data(), *expected_result);
        function->insert_result_into(actual.data(), *actual_result);
        EXPECT_EQ(actual_result->compare_at(0, 0, *expected_result, 1), 0) << "row=" << i;
    }
}

struct alignas(64) StreamingArenaProbeData {
    StringRef value;
};

class StreamingArenaProbe final
        : public IAggregateFunctionDataHelper<StreamingArenaProbeData, StreamingArenaProbe> {
    using Base = IAggregateFunctionDataHelper<StreamingArenaProbeData, StreamingArenaProbe>;

public:
    StreamingArenaProbe() : Base({std::make_shared<DataTypeString>()}) {}
    String get_name() const override { return "streaming_arena_probe"; }
    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }
    void create(AggregateDataPtr place) const override {
        EXPECT_EQ(reinterpret_cast<uintptr_t>(place) % alignof(StreamingArenaProbeData), 0);
        ++creates;
        Base::create(place);
    }
    void destroy(AggregateDataPtr place) const noexcept override {
        ++destroys;
        Base::destroy(place);
    }
    void reset(AggregateDataPtr place) const override { data(place).value = {}; }
    void add(AggregateDataPtr place, const IColumn** columns, ssize_t row,
             Arena& arena) const override {
        auto value = assert_cast<const ColumnString&>(*columns[0]).get_data_at(row);
        auto* copied = arena.alloc(value.size);
        std::memcpy(copied, value.data, value.size);
        data(place).value = {copied, value.size};
        max_arena_used = std::max(max_arena_used, arena.used_size());
    }
    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena&) const override {
        data(place).value = data(rhs).value;
    }
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        buf.write_binary(data(place).value);
    }
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena&) const override {
        buf.read_binary(data(place).value);
    }
    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        const auto value = data(place).value;
        assert_cast<ColumnString&>(to).insert_data(value.data, value.size);
    }

    mutable size_t creates = 0;
    mutable size_t destroys = 0;
    mutable size_t max_arena_used = 0;
};

void check_streaming_arena_mode(int mode) {
    constexpr size_t rows = 64;
    constexpr size_t width = 8192;
    auto values = ColumnString::create();
    for (size_t row = 0; row < rows; ++row) {
        values->insert_value(std::string(width, static_cast<char>('a' + row % 26)));
    }
    auto* probe = new StreamingArenaProbe;
    AggregateFunctionPtr function;
    ColumnPtr values_column = std::move(values);
    ColumnPtr input = values_column;
    if (mode == 0) {
        function.reset(probe);
    } else {
        auto nulls = ColumnUInt8::create(rows, 0);
        for (size_t row = 0; row < rows; ++row) {
            nulls->get_data()[row] = mode == 2 && row % 3 == 0;
        }
        input = ColumnNullable::create(values_column, std::move(nulls));
        function = std::make_shared<AggregateFunctionNullUnaryInlineV2<StreamingArenaProbe, true>>(
                probe, DataTypes {make_nullable(std::make_shared<DataTypeString>())}, false);
    }
    Arena caller_arena;
    const IColumn* columns[] = {input.get()};
    auto serialized = function->create_serialize_column();
    function->streaming_agg_serialize_to_column(columns, serialized, rows, caller_arena);
    EXPECT_EQ(caller_arena.used_size(), 0);
    EXPECT_LE(probe->max_arena_used, width);
    EXPECT_EQ(probe->creates, mode == 2 ? 42 : rows);
    EXPECT_EQ(probe->destroys, probe->creates);
    const IColumn* payload = serialized.get();
    if (mode != 0) {
        payload = &assert_cast<const ColumnNullable&>(*serialized).get_nested_column();
    }
    for (size_t row = 0; row < rows; ++row) {
        if (mode == 2 && row % 3 == 0) {
            EXPECT_TRUE(serialized->is_null_at(row));
            continue;
        }
        // Read after the temporary arenas were destroyed: serialized rows own the bytes.
        VectorBufferReader reader(assert_cast<const ColumnString&>(*payload).get_data_at(row));
        StringRef decoded;
        reader.read_binary(decoded);
        EXPECT_EQ(decoded, values_column->get_data_at(row));
    }
}

TEST(NullableAggregationV2Test, StreamingStatesAreAlignedAndTemporaryPayloadsAreRowLocal) {
    // Bare generic, nullable/no NULL, and nullable/sparse NULL take different streaming paths.
    for (int mode : {0, 1, 2}) {
        check_streaming_arena_mode(mode);
    }
}

TEST(NullableAggregationV2Test, GenericRangeReadersAndStateGuardsHonorAlignment) {
    auto function = std::make_shared<StreamingArenaProbe>();
    auto values = ColumnString::create();
    values->insert_value(std::string(8192, 'a'));
    values->insert_value("last");
    const IColumn* columns[] = {values.get()};
    Arena arena;
    auto serialized = function->create_serialize_column();
    function->streaming_agg_serialize_to_column(columns, serialized, 2, arena);
    {
        AggregateFunctionGuard state(function.get());
        const auto before_creates = function->creates;
        const auto before_destroys = function->destroys;
        function->deserialize_and_merge_from_column_range(state.data(), *serialized, 0, 1, arena);
        EXPECT_EQ(function->creates - before_creates, 2);
        EXPECT_EQ(function->destroys - before_destroys, 2);
        auto result = function->get_return_type()->create_column();
        function->insert_result_into(state.data(), *result);
        EXPECT_EQ(result->get_data_at(0), StringRef("last", 4));
    }
    EXPECT_EQ(function->creates, function->destroys);
}

TEST(NullableAggregationV2Test, AllNullStreamingDoesNotConstructTemporaryStates) {
    auto* probe = new StreamingArenaProbe;
    auto function = std::make_shared<AggregateFunctionNullUnaryInlineV2<StreamingArenaProbe, true>>(
            probe, DataTypes {make_nullable(std::make_shared<DataTypeString>())}, false);
    auto values = ColumnString::create();
    values->insert_value("unread");
    auto input = ColumnNullable::create(std::move(values), ColumnUInt8::create(1, 1));
    const IColumn* columns[] = {input.get()};
    Arena arena;
    auto serialized = function->create_serialize_column();
    function->streaming_agg_serialize_to_column(columns, serialized, 1, arena);
    EXPECT_TRUE(serialized->is_null_at(0));
    EXPECT_EQ(probe->creates, 0);
    EXPECT_EQ(probe->destroys, 0);
    EXPECT_EQ(probe->max_arena_used, 0);
    EXPECT_EQ(arena.used_size(), 0);
}

TEST(NullableAggregationV2Test, NativeUnaryAndStableStateCodecs) {
    auto type = make_nullable(std::make_shared<DataTypeInt64>());
    for (const auto* name :
         {"sum", "avg", "min", "max", "multi_distinct_sum", "multi_distinct_count"}) {
        SCOPED_TRACE(name);
        const bool result_nullable = std::string_view(name) != "multi_distinct_count";
        auto function = nullable_function(name, {type}, result_nullable);
        for (const auto& nulls : {std::initializer_list<UInt8> {0, 0, 0, 0, 0, 0},
                                  {1, 0, 0, 0, 0, 1},
                                  {1, 1, 1, 0, 1, 1},
                                  {1, 1, 1, 1, 1, 1}}) {
            auto input = nullable_ints({999, 10, 20, 20, -10, 999}, nulls);
            check_streaming(function, {std::move(input)},
                            std::string_view(name) == "sum" || std::string_view(name) == "avg");
        }
        check_streaming(make_agg_state_compatible_function(function),
                        {nullable_ints({99, 10, 20, 0, 20, 99}, {1, 0, 0, 1, 0, 1})});
    }
}

TEST(NullableAggregationV2Test, VariadicNullableArgumentsAtEveryPosition) {
    auto type = std::make_shared<DataTypeFloat64>();
    for (bool nullable_first : {false, true}) {
        for (bool nullable_second : {false, true}) {
            if (!nullable_first && !nullable_second) {
                continue;
            }
            auto function = nullable_function("avg_weighted",
                                              {nullable_first ? make_nullable(type) : type,
                                               nullable_second ? make_nullable(type) : type});
            EXPECT_NE(function->get_name().find("NullableV2"), std::string::npos);
            EXPECT_TRUE(function->get_serialized_type()->is_nullable());
            auto first = ColumnFloat64::create();
            first->get_data().assign({10, 999, 20, 30, 999, 999});
            auto second = ColumnFloat64::create();
            second->get_data().assign({1, 2, 999, 3, 999, 999});
            ColumnPtr x = std::move(first);
            ColumnPtr w = std::move(second);
            if (nullable_first) {
                auto nulls = ColumnUInt8::create();
                nulls->get_data().assign({0, 1, 0, 0, 1, 1});
                x = ColumnNullable::create(x, std::move(nulls));
            }
            if (nullable_second) {
                auto nulls = ColumnUInt8::create();
                nulls->get_data().assign({0, 0, 1, 0, 1, 1});
                w = ColumnNullable::create(w, std::move(nulls));
            }
            check_streaming(function, {x, w});
            check_streaming(make_agg_state_compatible_function(function), {x, w});
        }
    }
}

void check_legacy_incremental_window(const char* name) {
    auto type = make_nullable(std::make_shared<DataTypeInt64>());
    auto input = nullable_ints({0, 0, 3, 5}, {1, 1, 0, 0});
    const IColumn* columns[] = {input.get()};
    AggregateFunctionAttr attr;
    attr.is_window_function = true;
    auto function =
            AggregateFunctionSimpleFactory::instance().get(name, {type}, nullptr, true, 19, attr);
    auto codec = make_nullable_wire_compatible_function(function, 19, false);
    EXPECT_TRUE(codec->supported_incremental_mode());
    EXPECT_EQ(codec->result_column_could_resize(), function->result_column_could_resize());
    EXPECT_TRUE(codec->needs_deserialize_and_merge_scratch());
    AggregateFunctionGuard state(codec.get());
    Arena arena;
    auto result = codec->get_return_type()->create_column();
    UInt8 use_null_result = 0;
    UInt8 could_use_previous = 0;
    for (int64_t row = 0; row < 4; ++row) {
        codec->execute_function_with_incremental(
                0, 4, std::max<int64_t>(0, row - 1), row + 1, state.data(), columns, arena,
                row >= 2 && input->is_null_at(row - 2), input->is_null_at(row), true,
                &use_null_result, &could_use_previous);
        codec->insert_result_into(state.data(), *result);
        could_use_previous = 1;
    }
    EXPECT_TRUE(result->is_null_at(0));
    EXPECT_TRUE(result->is_null_at(1));
    const auto& nested = assert_cast<const ColumnNullable&>(*result).get_nested_column();
    if (std::string_view(name) == "sum") {
        const auto& values = assert_cast<const ColumnInt64&>(nested).get_data();
        EXPECT_EQ(values[2], 3);
        EXPECT_EQ(values[3], 8);
    } else {
        const auto& values = assert_cast<const ColumnFloat64&>(nested).get_data();
        EXPECT_DOUBLE_EQ(values[2], 3);
        EXPECT_DOUBLE_EQ(values[3], 4);
    }
}

TEST(NullableAggregationV2Test, LegacyWireCodecKeepsIncrementalWindowExecution) {
    check_legacy_incremental_window("sum");
    check_legacy_incremental_window("avg");
}

TEST(NullableAggregationV2Test, WideStringReadersSkipNullPartialsBeforeDecoding) {
    auto type = make_nullable(std::make_shared<DataTypeString>());
    for (const auto* name : {"min", "max"}) {
        auto strings = ColumnString::create();
        for (const auto& value : {std::string(8192, 'x'), std::string(), std::string(4096, 'a'),
                                  std::string(), std::string(8192, 'z'), std::string()}) {
            strings->insert_data(value.data(), value.size());
        }
        auto nulls = ColumnUInt8::create();
        nulls->get_data().assign({0, 1, 0, 1, 0, 1});
        ColumnPtr input = ColumnNullable::create(std::move(strings), std::move(nulls));
        auto function = nullable_function(name, {type});
        check_streaming(function, {input});
        check_streaming(make_agg_state_compatible_function(function), {input});
    }
}

TEST(NullableAggregationV2Test, BitmapIntersectionIgnoresNullOnlyPartialStates) {
    // The NULL-only middle partial must not turn {1,2} INTERSECT {2,3} into empty.
    auto type = make_nullable(std::make_shared<DataTypeBitMap>());
    auto function = nullable_function("bitmap_intersect", {type});
    auto bitmaps = ColumnBitmap::create();
    for (const auto& values : {std::initializer_list<UInt64> {1, 2}, {}, {2, 3}, {}, {2}, {}}) {
        BitmapValue bitmap;
        for (auto value : values) {
            bitmap.add(value);
        }
        bitmaps->insert_value(bitmap);
    }
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({0, 1, 0, 1, 0, 1});
    ColumnPtr input = ColumnNullable::create(std::move(bitmaps), std::move(nulls));
    check_streaming(function, {input});
    check_streaming(make_agg_state_compatible_function(function), {input});
    Arena arena;
    AggregateFunctionGuard state(function.get());
    add_column(function, state.data(), *input, arena);
    auto output = function->get_return_type()->create_column();
    function->insert_result_into(state.data(), *output);
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    ASSERT_FALSE(nullable.is_null_at(0));
    const auto& result = assert_cast<const ColumnBitmap&>(nullable.get_nested_column());
    EXPECT_EQ(result.get_data()[0].cardinality(), 1);
    EXPECT_TRUE(result.get_data()[0].contains(2));
}

TEST(NullableAggregationV2Test, NonNullableBitmapResultStillMarksEmptyPartials) {
    auto type = make_nullable(std::make_shared<DataTypeBitMap>());
    auto function = nullable_function("bitmap_intersect", {type}, false);
    auto bitmaps = ColumnBitmap::create();
    for (const auto* text : {"1,2", "", "2,3", "", "2", ""}) {
        BitmapValue value;
        if (std::string_view(text) == "1,2") {
            value.add(1);
            value.add(2);
        } else if (std::string_view(text) == "2,3") {
            value.add(2);
            value.add(3);
        } else if (std::string_view(text) == "2") {
            value.add(2);
        }
        bitmaps->insert_value(value);
    }
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({0, 1, 0, 1, 0, 1});
    ColumnPtr input = ColumnNullable::create(std::move(bitmaps), std::move(nulls));
    EXPECT_FALSE(function->get_return_type()->is_nullable());
    EXPECT_TRUE(function->get_serialized_type()->is_nullable());
    check_streaming(function, {input});
    check_streaming(make_agg_state_compatible_function(function), {input});
}

TEST(NullableAggregationV2Test, BitmapStateMarkerSurvivesOldSchemaCompactionCodec) {
    auto type = std::make_shared<DataTypeAggState>(
            DataTypes {make_nullable(std::make_shared<DataTypeBitMap>())}, false,
            "bitmap_intersect", 19);
    auto function = type->get_nested_function();
    auto serialized = type->create_column();
    // New no-input state versus the legal historical encoding of an actual empty bitmap.
    const std::array<char, 1> no_input {0};
    const std::array<char, 3> empty_bitmap {1, 1, 0};
    auto& strings = assert_cast<ColumnString&>(*serialized);
    strings.insert_data(no_input.data(), no_input.size());
    strings.insert_data(empty_bitmap.data(), empty_bitmap.size());
    Arena arena;
    for (size_t row = 0; row < 2; ++row) {
        AggregateFunctionGuard state(function.get());
        AggregateFunctionGuard partial(function.get());
        function->deserialize_from_column_row(partial.data(), *serialized, row, arena);
        auto reencoded = type->create_column();
        function->serialize_without_key_to_column(partial.data(), *reencoded);
        EXPECT_EQ(reencoded->get_data_at(0), serialized->get_data_at(row));
        auto values = ColumnBitmap::create();
        BitmapValue bitmap;
        bitmap.add(1);
        bitmap.add(2);
        values->insert_value(bitmap);
        auto nulls = ColumnUInt8::create(1, 0);
        auto input = ColumnNullable::create(std::move(values), std::move(nulls));
        const IColumn* columns[] = {input.get()};
        function->add(state.data(), columns, 0, arena);
        function->merge(state.data(), partial.data(), arena);
        auto result = function->get_return_type()->create_column();
        function->insert_result_into(state.data(), *result);
        const auto& bitmap_result = assert_cast<const ColumnBitmap&>(*result).get_data()[0];
        EXPECT_EQ(bitmap_result.cardinality(), row == 0 ? 2 : 0);
    }
}

TEST(NullableAggregationV2Test, EmptyBatchAndEntirelySkippedStringStates) {
    auto type = make_nullable(std::make_shared<DataTypeString>());
    auto function = nullable_function("min", {type});
    auto column = type->create_column();
    const IColumn* columns[] = {column.get()};
    Arena arena;
    AggregateFunctionGuard state(function.get());
    function->add_batch_single_place(0, state.data(), columns, arena);
    auto serialized = function->create_serialize_column();
    function->streaming_agg_serialize_to_column(columns, serialized, 0, arena);
    function->deserialize_and_merge_vec(nullptr, 0, nullptr, serialized.get(), arena, 0);
    // Invalid nested string payloads are never read when all destinations are skipped.
    serialized->insert_default();
    serialized->insert_default();
    std::array<AggregateDataPtr, 2> skipped {nullptr, nullptr};
    function->deserialize_and_merge_vec_selected(skipped.data(), 0, nullptr, serialized.get(),
                                                 arena, skipped.size());
    auto output = function->get_return_type()->create_column();
    function->insert_result_into(state.data(), *output);
    EXPECT_TRUE(output->is_null_at(0));
}

TEST(NullableAggregationV2Test, PublishedNullableSumBytesRemainReadableAndWritable) {
    auto input_type = make_nullable(std::make_shared<DataTypeInt64>());
    // Published V1 buffer format: bool(has_value)=1 followed by the little-endian Int64 sum.
    // These golden bytes are independent of the new serializer and contain a valid old state.
    constexpr char ten[] = {1, 10, 0, 0, 0, 0, 0, 0, 0};
    constexpr char empty[] = {0};
    for (int version : {8, 10, 19, BeExecVersionManager::get_newest_version()}) {
        auto type =
                std::make_shared<DataTypeAggState>(DataTypes {input_type}, true, "sum", version);
        auto function = type->get_nested_function();
        EXPECT_EQ(type->get_serialized_type()->get_primitive_type(), TYPE_STRING);
        auto old_states = type->create_column();
        old_states->insert_data(ten, sizeof(ten));
        old_states->insert_data(empty, sizeof(empty));
        Arena arena;
        AggregateFunctionGuard merged(function.get());
        function->deserialize_and_merge_from_column(merged.data(), *old_states, arena);
        auto input = nullable_ints({20, 999}, {0, 1});
        add_column(function, merged.data(), *input, arena);
        auto output = type->create_column();
        function->serialize_without_key_to_column(merged.data(), *output);
        constexpr char thirty[] = {1, 30, 0, 0, 0, 0, 0, 0, 0};
        EXPECT_EQ(output->get_data_at(0).to_string(), std::string(thirty, sizeof(thirty)));
        PColumnMeta meta;
        type->to_pb_column_meta(&meta);
        auto rebuilt = DataTypeFactory::instance().create_data_type(meta);
        EXPECT_TRUE(rebuilt->check_column(*output).ok());
        auto serialized_bytes = type->get_uncompressed_serialized_bytes(*old_states, version);
        std::string wire(serialized_bytes, '\0');
        type->serialize(*old_states, wire.data(), version);
        auto restored = rebuilt->create_column();
        rebuilt->deserialize(wire.data(), &restored, version);
        EXPECT_EQ(restored->get_data_at(0).to_string(), std::string(ten, sizeof(ten)));
        EXPECT_EQ(restored->get_data_at(1).to_string(), std::string(empty, sizeof(empty)));

        Block block({{old_states->get_ptr(), type, "state"}});
        PBlock wire_block;
        size_t uncompressed_bytes = 0;
        size_t compressed_bytes = 0;
        int64_t compress_time = 0;
        ASSERT_TRUE(block.serialize(version, &wire_block, &uncompressed_bytes, &compressed_bytes,
                                    &compress_time, segment_v2::CompressionTypePB::SNAPPY)
                            .ok());
        Block received;
        size_t decompressed_bytes = 0;
        int64_t decompress_time = 0;
        ASSERT_TRUE(received.deserialize(wire_block, &decompressed_bytes, &decompress_time).ok());
        EXPECT_EQ(received.get_by_position(0).column->get_data_at(0).to_string(),
                  std::string(ten, sizeof(ten)));
    }
}

TEST(NullableAggregationV2Test, OuterSqlNullAndEmptyStateAreDistinct) {
    auto type = std::make_shared<DataTypeAggState>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt64>())}, true, "sum",
            BeExecVersionManager::get_newest_version());
    auto nested = type->get_nested_function();
    auto column = type->create_column();
    constexpr char ten[] = {1, 10, 0, 0, 0, 0, 0, 0, 0};
    constexpr char empty[] = {0};
    column->insert_data(ten, sizeof(ten));
    column->insert_default();                  // SQL NULL has no readable serialized state.
    column->insert_data(empty, sizeof(empty)); // A non-NULL state with an internal NULL result.
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({0, 1, 0});
    auto outer = ColumnNullable::create(std::move(column), std::move(nulls));
    const IColumn* columns[] = {outer.get()};
    auto function = AggregateStateUnion::create(nested, {make_nullable(type)}, type);
    Arena arena;
    for (bool batch : {false, true}) {
        AggregateFunctionGuard state(function.get());
        if (batch) {
            std::array<AggregateDataPtr, 3> places {state.data(), state.data(), state.data()};
            function->add_batch(places.size(), places.data(), 0, columns, arena, false);
        } else {
            function->add_batch_single_place(outer->size(), state.data(), columns, arena);
        }
        auto output = nested->get_return_type()->create_column();
        nested->insert_result_into(state.data(), *output);
        ASSERT_FALSE(output->is_null_at(0));
        EXPECT_EQ(assert_cast<const ColumnInt64&>(
                          assert_cast<const ColumnNullable&>(*output).get_nested_column())
                          .get_data()[0],
                  10);
    }
}

TEST(NullableAggregationV2Test, VersionedWireCodecDoesNotSelectAnExecutionImplementation) {
    auto type = make_nullable(std::make_shared<DataTypeFloat64>());
    for (int version : {19, USE_UNIFIED_NULLABLE_AGGREGATION_VERSION}) {
        for (bool legacy_unary_v2 : {false, true}) {
            auto unary = make_nullable_wire_compatible_function(nullable_function("sum", {type}),
                                                                version, legacy_unary_v2);
            auto variadic = make_nullable_wire_compatible_function(
                    nullable_function("avg_weighted", {type, type}), version, legacy_unary_v2);
            EXPECT_NE(unary->get_name().find("NullableV2"), std::string::npos);
            EXPECT_NE(variadic->get_name().find("NullableV2"), std::string::npos);
            EXPECT_EQ(unary->get_serialized_type()->is_nullable(),
                      version >= USE_UNIFIED_NULLABLE_AGGREGATION_VERSION || legacy_unary_v2);
            EXPECT_EQ(variadic->get_serialized_type()->is_nullable(),
                      version >= USE_UNIFIED_NULLABLE_AGGREGATION_VERSION);
        }
    }
}

TEST(NullableAggregationV2Test, StoredNullableStatesUseSupportedColumnConverters) {
    for (const auto* name : {"sum", "count"}) {
        ColumnPB meta;
        meta.set_unique_id(0);
        meta.set_name("state");
        meta.set_type("AGG_STATE");
        meta.set_aggregation(name);
        meta.set_is_nullable(false);
        meta.set_result_is_nullable(std::string_view(name) == "sum");
        meta.set_be_exec_version(19);
        auto* child = meta.add_children_columns();
        child->set_unique_id(1);
        child->set_name("argument");
        child->set_type("BIGINT");
        child->set_is_nullable(true);
        TabletColumn tablet_column(meta);
        auto type = DataTypeFactory::instance().create_data_type(tablet_column);
        const auto& state_type = assert_cast<const DataTypeAggState&>(*type);
        auto function = state_type.get_nested_function();
        auto states = type->create_column();
        auto input = nullable_ints({10, 999}, {0, 1});
        const IColumn* columns[] = {input.get()};
        Arena arena;
        function->streaming_agg_serialize_to_column(columns, states, 2, arena);
        OlapBlockDataConvertor convertor;
        convertor.add_column_data_convertor(tablet_column);
        ASSERT_TRUE(convertor
                            .set_source_content_with_specifid_column(
                                    {states->get_ptr(), type, "state"}, 0, 2, 0)
                            .ok());
        auto [status, accessor] = convertor.convert_column_data(0);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_NE(accessor, nullptr);
        // SUM stays String at this boundary; COUNT retains its published non-nullable
        // fixed state. Nullable(FixedLengthObject) never reaches the legacy storage converter.
        const auto* slices = static_cast<const Slice*>(accessor->get_data());
        ASSERT_NE(slices, nullptr);
        EXPECT_EQ(slices[0].size, std::string_view(name) == "sum" ? 9 : 8);
        EXPECT_EQ(slices[1].size, std::string_view(name) == "sum" ? 1 : 8);
    }
}

} // namespace
} // namespace doris
