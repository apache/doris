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
#include <memory>

#include "agent/be_exec_version_manager.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_variant.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/aggregate_function_uniq.h"

namespace doris {
namespace {

class AggregateState {
public:
    explicit AggregateState(const IAggregateFunction& function) : _function(function) {
        place = reinterpret_cast<AggregateDataPtr>(
                arena.aligned_alloc(function.size_of_data(), function.align_of_data()));
        function.create(place);
    }

    ~AggregateState() { _function.destroy(place); }

    AggregateState(const AggregateState&) = delete;
    AggregateState& operator=(const AggregateState&) = delete;

    Arena arena;
    AggregateDataPtr place = nullptr;

private:
    const IAggregateFunction& _function;
};

ColumnVariantV2::MutablePtr encoded_values() {
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = 7});
    {
        auto row = builder.begin_row();
        row.add_int(1);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_decimal(10, 1);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_null();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(StringRef("z"));
        row.add_int(2);
        object.add_key(StringRef("a"));
        row.add_int(1);
        object.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(StringRef("a"));
        row.add_int(1);
        object.add_key(StringRef("z"));
        row.add_int(2);
        object.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_int(1);
        row.add_int(2);
        array.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_int(2);
        row.add_int(1);
        array.finish();
        row.finish();
    }

    VariantBatchBuilder block = builder.finish_batch();
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(block);
    return column;
}

ColumnVariantV2::MutablePtr typed_int_values() {
    auto values = ColumnInt64::create();
    for (int64_t value : {1, 1, 2, 0}) {
        values->insert_value(value);
    }
    auto nulls = ColumnUInt8::create();
    for (uint8_t value : {0, 0, 0, 1}) {
        nulls->insert_value(value);
    }
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(nulls)),
            std::make_shared<DataTypeInt64>());
}

ColumnVariantV2::MutablePtr typed_decimal_values() {
    auto values = ColumnDecimal128V3::create(0, 2);
    values->insert_value(Decimal128V3 {100});
    values->insert_value(Decimal128V3 {200});
    auto nulls = ColumnUInt8::create(2, 0);
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(nulls)),
            std::make_shared<DataTypeDecimal128>(38, 2));
}

void add_column(const IAggregateFunction& function, AggregateState& state, const IColumn& column) {
    const IColumn* columns[] = {&column};
    for (size_t row = 0; row < column.size(); ++row) {
        function.add(state.place, columns, row, state.arena);
    }
}

int64_t result(const IAggregateFunction& function, ConstAggregateDataPtr place) {
    auto output = ColumnInt64::create();
    function.insert_result_into(place, *output);
    return output->get_data().front();
}

TEST(AggregateFunctionUniqVariantTest, CanonicalExactStateCoversEncodedAndTypedValues) {
    AggregateFunctionUniqVariant function({std::make_shared<DataTypeVariant>()});
    auto encoded = encoded_values();
    AggregateState destination(function);
    add_column(function, destination, *encoded);
    EXPECT_EQ(result(function, destination.place), 5);

    const size_t used_after_unique_values = destination.arena.used_size();
    const IColumn* columns[] = {encoded.get()};
    function.add(destination.place, columns, 0, destination.arena);
    EXPECT_EQ(destination.arena.used_size(), used_after_unique_values);

    {
        AggregateState typed(function);
        auto values = typed_int_values();
        add_column(function, typed, *values);
        EXPECT_EQ(result(function, typed.place), 3);
        function.merge(destination.place, typed.place, destination.arena);
    }
    EXPECT_EQ(result(function, destination.place), 6);

    {
        AggregateState typed(function);
        auto values = typed_decimal_values();
        add_column(function, typed, *values);
        function.merge(destination.place, typed.place, destination.arena);
    }
    EXPECT_EQ(result(function, destination.place), 6);

    function.reset(destination.place);
    EXPECT_EQ(result(function, destination.place), 0);
}

TEST(AggregateFunctionUniqVariantTest, SerializedStateOwnsOnlyCanonicalKeys) {
    AggregateFunctionUniqVariant function({std::make_shared<DataTypeVariant>()});
    AggregateState source(function);
    auto encoded = encoded_values();
    add_column(function, source, *encoded);

    ColumnString serialized;
    VectorBufferWriter writer(serialized);
    function.serialize(source.place, writer);
    writer.commit();

    AggregateState restored(function);
    VectorBufferReader first_reader(serialized.get_data_at(0));
    function.deserialize(restored.place, first_reader, restored.arena);
    EXPECT_EQ(result(function, restored.place), 5);

    const size_t used_after_first_deserialize = restored.arena.used_size();
    VectorBufferReader second_reader(serialized.get_data_at(0));
    function.deserialize(restored.place, second_reader, restored.arena);
    EXPECT_EQ(result(function, restored.place), 5);
    EXPECT_EQ(restored.arena.used_size(), used_after_first_deserialize);
}

TEST(AggregateFunctionUniqVariantTest, DeserializeAndMergeCopiesNewKeysIntoDestinationArena) {
    AggregateFunctionUniqVariant function({std::make_shared<DataTypeVariant>()});
    AggregateState destination(function);
    auto typed = typed_int_values();
    add_column(function, destination, *typed);
    EXPECT_EQ(result(function, destination.place), 3);

    AggregateState scratch(function);
    {
        AggregateState source(function);
        auto encoded = encoded_values();
        add_column(function, source, *encoded);

        ColumnString serialized;
        VectorBufferWriter writer(serialized);
        function.serialize(source.place, writer);
        writer.commit();

        VectorBufferReader reader(serialized.get_data_at(0));
        function.deserialize_and_merge(destination.place, scratch.place, reader, destination.arena);
    }
    EXPECT_EQ(result(function, destination.place), 6);
}

TEST(AggregateFunctionUniqVariantTest, NullableFactorySkipsSqlNullButCountsVariantNull) {
    const DataTypePtr variant_type = std::make_shared<DataTypeVariant>();
    const DataTypePtr nullable_type = make_nullable(variant_type);
    AggregateFunctionPtr function = AggregateFunctionSimpleFactory::instance().get(
            "multi_distinct_count", {nullable_type}, std::make_shared<DataTypeInt64>(), false,
            BeExecVersionManager::get_newest_version());
    ASSERT_NE(function, nullptr);

    auto nested = encoded_values();
    auto outer_nulls = ColumnUInt8::create(nested->size(), 0);
    outer_nulls->get_data()[5] = 1;
    ColumnPtr nullable = ColumnNullable::create(std::move(nested), std::move(outer_nulls));

    AggregateState state(*function);
    add_column(*function, state, *nullable);
    EXPECT_EQ(result(*function, state.place), 4);
}

} // namespace
} // namespace doris
