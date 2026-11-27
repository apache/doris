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
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/column/column_array.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_hll.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_quantilestate.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_uuid.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type/data_type_variant.h"
#include "exprs/aggregate/aggregate_function_collect.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/aggregate_function_state_combine.h"
#include "exprs/aggregate/aggregate_function_state_merge.h"
#include "exprs/aggregate/aggregate_function_state_union.h"
#include "testutil/column_helper.h"

namespace doris {
namespace {

constexpr int NATIVE_VERSION = SUPPORT_COLLECT_LIST_NATIVE_SERIALIZATION_VERSION;
constexpr int LEGACY_VERSION = NATIVE_VERSION - 1;

Field array_field(std::initializer_list<Field> values) {
    return Field::create_field<TYPE_ARRAY>(Array(values));
}

Field string_field(const String& value) {
    return Field::create_field<TYPE_STRING>(value);
}

DataTypePtr string_array_type() {
    return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>()));
}

// Frozen pre-version-20 protocol, independent of the production list state's read/write.
void old_write(const IDataType& type, const IColumn& column, Int64 limit, BufferWritable& out) {
    out.write_binary(column.size());
    auto serde = type.get_serde();
    DataTypeSerDe::FormatOptions options;
    auto timezone = cctz::utc_time_zone();
    options.timezone = &timezone;
    auto text = ColumnString::create();
    VectorBufferWriter writer(*text);
    for (size_t row = 0; row < column.size(); ++row) {
        text->clear();
        ASSERT_TRUE(serde->serialize_one_cell_to_json(column, row, writer, options).ok());
        writer.commit();
        out.write_binary(text->get_data_at(0));
    }
    write_var_int(limit, out);
}

void old_read(const IDataType& type, IColumn& column, Int64& limit, BufferReadable& in) {
    size_t rows = 0;
    in.read_binary(rows);
    column.clear();
    column.reserve(rows);
    auto serde = type.get_serde();
    DataTypeSerDe::FormatOptions options;
    auto timezone = cctz::utc_time_zone();
    options.timezone = &timezone;
    for (size_t row = 0; row < rows; ++row) {
        StringRef text;
        in.read_binary(text);
        Slice slice(text.data, text.size);
        ASSERT_TRUE(serde->deserialize_one_cell_from_json(column, slice, options).ok());
    }
    read_var_int(limit, in);
}

void add_rows(const IAggregateFunction& function, AggregateDataPtr place, const IColumn& column,
              Arena& arena, int limit = -1) {
    auto limits = ColumnInt32::create(column.size(), limit);
    const IColumn* columns[] = {&column, limits.get()};
    for (size_t row = 0; row < column.size(); ++row) {
        function.add(place, columns, row, arena);
    }
}

void expect_result(const IAggregateFunction& function, ConstAggregateDataPtr place,
                   const Field& expected) {
    auto result = function.get_return_type()->create_column();
    function.insert_result_into(place, *result);
    auto expected_column = function.get_return_type()->create_column();
    expected_column->insert(expected);
    EXPECT_TRUE(ColumnHelper::column_equal(std::move(result), std::move(expected_column)));
}

void round_trip(const std::string& name, const DataTypePtr& type, const Array& values,
                const Field& expected, int version = NATIVE_VERSION,
                const AggregateFunctionAttr& attr = {}) {
    auto function = AggregateFunctionSimpleFactory::instance().get(name, {type}, type, false,
                                                                   version, attr);
    ASSERT_NE(function, nullptr);
    function->set_version(version);
    auto input = type->create_column();
    for (const auto& value : values) {
        input->insert(value);
    }
    Arena arena;
    AggregateFunctionGuard source(function.get());
    AggregateFunctionGuard restored(function.get());
    add_rows(*function, source.data(), *input, arena);
    auto wire = ColumnString::create();
    BufferWritable writer(*wire);
    function->serialize(source.data(), writer);
    writer.commit();
    auto bytes = wire->get_data_at(0);
    BufferReadable reader(bytes);
    function->deserialize(restored.data(), reader, arena);
    EXPECT_FALSE(reader.has_remaining());
    expect_result(*function, restored.data(), expected);
}

TEST(AggregateCollectSerializationTest, LegacyBytesAndBothDirections) {
    const auto integer = Field::create_field<TYPE_INT>(7);
    const auto array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
    const auto map_type =
            std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()),
                                          make_nullable(std::make_shared<DataTypeInt32>()));
    const auto struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt32>()),
                       make_nullable(std::make_shared<DataTypeString>())});
    const std::vector<std::pair<DataTypePtr, Field>> cases = {
            {array_type, array_field({integer, Field()})},
            {map_type, Field::create_field<TYPE_MAP>(
                               Map {array_field({string_field("key")}), array_field({integer})})},
            {struct_type, Field::create_field<TYPE_STRUCT>(Struct {integer, Field()})}};
    for (const auto& [type, value] : cases) {
        for (const int limit : {-1, 0, 1, 3}) {
            SCOPED_TRACE(type->get_name() + " limit=" + std::to_string(limit));
            DataTypes arguments {type};
            if (limit >= 0) {
                arguments.push_back(std::make_shared<DataTypeInt32>());
            }
            auto function = AggregateFunctionSimpleFactory::instance().get(
                    "collect_list", arguments, nullptr, false, LEGACY_VERSION);
            ASSERT_NE(function, nullptr);
            function->set_version(LEGACY_VERSION);
            auto input = type->create_column();
            input->insert(value);
            input->insert(value);
            const auto count = limit < 0 ? 2 : std::min(limit, 2);
            auto selected = type->create_column();
            selected->insert_range_from(*input, 0, count);
            auto reference = ColumnString::create();
            BufferWritable old_writer(*reference);
            old_write(*type, *selected, limit, old_writer);
            old_writer.commit();
            Arena arena;
            AggregateFunctionGuard source(function.get());
            AggregateFunctionGuard restored(function.get());
            add_rows(*function, source.data(), *input, arena, limit);
            auto actual = ColumnString::create();
            BufferWritable writer(*actual);
            function->serialize(source.data(), writer);
            writer.commit();
            EXPECT_EQ(actual->get_data_at(0), reference->get_data_at(0));
            auto bytes = reference->get_data_at(0);
            BufferReadable reader(bytes);
            function->deserialize(restored.data(), reader, arena);
            EXPECT_FALSE(reader.has_remaining());
            expect_result(*function, restored.data(),
                          Field::create_field<TYPE_ARRAY>(Array(count, value)));
            auto old_column = type->create_column();
            Int64 old_limit = -2;
            auto actual_bytes = actual->get_data_at(0);
            BufferReadable old_reader(actual_bytes);
            old_read(*type, *old_column, old_limit, old_reader);
            EXPECT_EQ(old_limit, limit);
            EXPECT_FALSE(old_reader.has_remaining());
            EXPECT_TRUE(ColumnHelper::column_equal(std::move(old_column), std::move(selected)));
        }
    }
}

TEST(AggregateCollectSerializationTest, NativeNestedValuesAliasesAndNullableWrappers) {
    const auto type = string_array_type();
    const auto value = array_field({string_field("a\",\"b"), string_field("\\\""),
                                    string_field(String("a\0b", 3)), Field(), string_field("")});
    const Array values {value, array_field({}), value};
    const auto expected = Field::create_field<TYPE_ARRAY>(values);
    for (const auto* name : {"collect_list", "group_array", "array_agg"}) {
        SCOPED_TRACE(name);
        round_trip(name, type, values, expected);
        round_trip(name, type, {}, array_field({}));
        for (bool null_v2 : {false, true}) {
            AggregateFunctionAttr attr;
            attr.enable_aggregate_function_null_v2 = null_v2;
            Array nullable_values {Field(), value, Field(), array_field({})};
            const auto nullable_expected =
                    std::string(name) == "array_agg"
                            ? Field::create_field<TYPE_ARRAY>(nullable_values)
                            : array_field({value, array_field({})});
            round_trip(name, make_nullable(type), nullable_values, nullable_expected,
                       NATIVE_VERSION, attr);
            round_trip(name, make_nullable(type), {Field(), Field()},
                       std::string(name) == "array_agg" ? array_field({Field(), Field()})
                                                        : array_field({}),
                       NATIVE_VERSION, attr);
        }
    }
    const auto map_type = std::make_shared<DataTypeMap>(
            make_nullable(std::make_shared<DataTypeString>()), make_nullable(type));
    const auto map_value = Field::create_field<TYPE_MAP>(
            Map {array_field({string_field("a\",\"b"), string_field("null")}),
                 array_field({value, Field()})});
    round_trip("collect_list", map_type, {map_value}, array_field({map_value}));
    const auto struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(map_type), make_nullable(type)});
    const auto struct_value = Field::create_field<TYPE_STRUCT>(Struct {map_value, value});
    round_trip("collect_list", struct_type, {struct_value}, array_field({struct_value}));

    // These explicitly dispatched types also select the versioned column-based specialization.
    auto uuid = Field::create_field<TYPE_UUID>(UUIDValue::from_parts(123, 456));
    round_trip("collect_list", std::make_shared<DataTypeUUID>(), {uuid}, array_field({uuid}));
    auto time = Field::create_field<TYPE_TIMEV2>(123456789.0);
    round_trip("collect_list", std::make_shared<DataTypeTimeV2>(6), {time}, array_field({time}));
}

TEST(AggregateCollectSerializationTest, ExactFramesPrefixesBatchMergeAndReuse) {
    auto type = std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
    Array integers;
    for (int i = 0; i < 65; ++i) {
        integers.push_back(Field::create_field<TYPE_INT>(i));
    }
    auto value = Field::create_field<TYPE_ARRAY>(integers);
    auto input = type->create_column();
    input->insert(value);
    input->insert(array_field({}));
    for (const int version : {LEGACY_VERSION, NATIVE_VERSION}) {
        auto function = AggregateFunctionSimpleFactory::instance().get("collect_list", {type},
                                                                       nullptr, false, version);
        ASSERT_NE(function, nullptr);
        function->set_version(version);
        Arena arena;
        AggregateFunctionGuard source(function.get());
        AggregateFunctionGuard restored(function.get());
        add_rows(*function, source.data(), *input, arena);
        auto wire = ColumnString::create();
        BufferWritable writer(*wire);
        for (int i = 0; i < 2; ++i) {
            writer.write_binary(UInt8(0x5a));
            function->serialize(source.data(), writer);
            writer.write_binary(UInt8(0xa5));
            writer.commit();
        }
        EXPECT_EQ(wire->get_data_at(0), wire->get_data_at(1));
        for (size_t row = 0; row < wire->size(); ++row) {
            auto bytes = wire->get_data_at(row);
            BufferReadable reader(bytes);
            UInt8 prefix = 0;
            reader.read_binary(prefix);
            EXPECT_EQ(prefix, 0x5a);
            // Repeated deserialize must replace, not append to, the populated state.
            function->deserialize(restored.data(), reader, arena);
            UInt8 suffix = 0;
            reader.read_binary(suffix);
            EXPECT_EQ(suffix, 0xa5);
            EXPECT_FALSE(reader.has_remaining());
            expect_result(*function, restored.data(), array_field({value, array_field({})}));
        }
        if (version == NATIVE_VERSION) {
            auto bytes = wire->get_data_at(0);
            BufferReadable reader(bytes);
            UInt8 prefix = 0;
            reader.read_binary(prefix);
            UInt64 serialized_bytes = 0;
            reader.read_binary(serialized_bytes);
            auto decoded = type->create_column();
            const auto* begin = reader.data();
            const auto* end = type->deserialize(begin, &decoded, version);
            EXPECT_EQ(end, begin + serialized_bytes);
            reader.add_offset(serialized_bytes);
            Int64 limit = 0;
            read_var_int(limit, reader);
            UInt8 suffix = 0;
            reader.read_binary(suffix);
            EXPECT_EQ(suffix, 0xa5);
            EXPECT_FALSE(reader.has_remaining());
            EXPECT_LT(serialized_bytes, type->get_uncompressed_serialized_bytes(*input, version));
            EXPECT_TRUE(ColumnHelper::column_equal(std::move(decoded), input->clone()));
        }
        auto batch = function->create_serialize_column();
        function->serialize_to_column({source.data(), source.data()}, 0, batch, 2);
        function->reset(restored.data());
        function->deserialize_from_column_row(restored.data(), *batch, 1, arena);
        expect_result(*function, restored.data(), array_field({value, array_field({})}));
        auto single = function->create_serialize_column();
        function->serialize_without_key_to_column(source.data(), *single);
        EXPECT_EQ(batch->get_data_at(0), single->get_data_at(0));
        function->reset(restored.data());
        auto streaming = function->create_serialize_column();
        const IColumn* columns[] = {input.get()};
        function->streaming_agg_serialize_to_column(columns, streaming, input->size(), arena);
        function->deserialize_and_merge_from_column_range(restored.data(), *streaming, 0, 1, arena);
        expect_result(*function, restored.data(), array_field({value, array_field({})}));
    }
}

TEST(AggregateCollectSerializationTest, OtherColumnBasedTypesUseNativeSerialization) {
    // The INVALID_TYPE fallback is also reachable for these non-scalar families. Compare
    // native bytes because opaque object columns need not support comparison/display.
    for (const auto& type :
         DataTypes {std::make_shared<DataTypeJsonb>(), std::make_shared<DataTypeVariant>(0, false),
                    std::make_shared<DataTypeBitMap>(), std::make_shared<DataTypeHLL>(),
                    std::make_shared<DataTypeQuantileState>(),
                    std::make_shared<DataTypeVarbinary>()}) {
        SCOPED_TRACE(type->get_name());
        auto function = AggregateFunctionSimpleFactory::instance().get(
                "collect_list", {type}, nullptr, false, NATIVE_VERSION);
        ASSERT_NE(function, nullptr);
        function->set_version(NATIVE_VERSION);
        auto input = type->create_column();
        input->insert_default();
        input->insert_default();
        Arena arena;
        AggregateFunctionGuard source(function.get());
        AggregateFunctionGuard restored(function.get());
        add_rows(*function, source.data(), *input, arena);
        auto wire = function->create_serialize_column();
        function->serialize_without_key_to_column(source.data(), *wire);
        function->deserialize_from_column_row(restored.data(), *wire, 0, arena);
        auto result = function->get_return_type()->create_column();
        function->insert_result_into(restored.data(), *result);
        const auto& array = assert_cast<const ColumnArray&>(*result);
        ASSERT_EQ(array.size(), 1);
        EXPECT_EQ(array.get_offsets()[0], 2);
        auto actual_data = remove_nullable(array.get_data().clone());
        auto serialize_column = [&](const IColumn& column) {
            std::string bytes(type->get_uncompressed_serialized_bytes(column, NATIVE_VERSION),
                              '\0');
            auto* end = type->serialize(column, bytes.data(), NATIVE_VERSION);
            bytes.resize(end - bytes.data());
            return bytes;
        };
        EXPECT_EQ(serialize_column(*actual_data), serialize_column(*input));
    }
}

TEST(AggregateCollectSerializationTest, InterleavedQueryVersionsRemainIndependent) {
    const auto type = string_array_type();
    const auto value = array_field({string_field("safe")});
    auto input = type->create_column();
    input->insert(value);
    auto old_function = AggregateFunctionSimpleFactory::instance().get(
            "collect_list", {type}, nullptr, false, LEGACY_VERSION);
    auto new_function = AggregateFunctionSimpleFactory::instance().get(
            "collect_list", {type}, nullptr, false, NATIVE_VERSION);
    ASSERT_NE(old_function, nullptr);
    ASSERT_NE(new_function, nullptr);
    old_function->set_version(LEGACY_VERSION);
    new_function->set_version(NATIVE_VERSION);
    Arena arena;
    AggregateFunctionGuard old_state(old_function.get());
    AggregateFunctionGuard new_state(new_function.get());
    add_rows(*old_function, old_state.data(), *input, arena);
    add_rows(*new_function, new_state.data(), *input, arena);
    auto wire = ColumnString::create();
    BufferWritable writer(*wire);
    for (const auto& function : {old_function, new_function, old_function, new_function}) {
        function->serialize(function == old_function ? old_state.data() : new_state.data(), writer);
        writer.commit();
    }
    EXPECT_EQ(wire->get_data_at(0), wire->get_data_at(2));
    EXPECT_EQ(wire->get_data_at(1), wire->get_data_at(3));
    EXPECT_NE(wire->get_data_at(0), wire->get_data_at(1));
}

TEST(AggregateCollectSerializationTest, NativeLimitSurvivesPartialMergeAndReset) {
    auto type = string_array_type();
    const auto value = array_field({string_field("a\",\"b")});
    auto input = type->create_column();
    input->insert(value);
    input->insert(value);
    for (int limit : {0, 1, 3}) {
        auto function = AggregateFunctionSimpleFactory::instance().get(
                "collect_list", {type, std::make_shared<DataTypeInt32>()}, nullptr, false,
                NATIVE_VERSION);
        ASSERT_NE(function, nullptr);
        function->set_version(NATIVE_VERSION);
        Arena arena;
        AggregateFunctionGuard source(function.get());
        AggregateFunctionGuard restored(function.get());
        AggregateFunctionGuard merged(function.get());
        add_rows(*function, source.data(), *input, arena, limit);
        auto wire = function->create_serialize_column();
        function->serialize_without_key_to_column(source.data(), *wire);
        function->deserialize_from_column_row(restored.data(), *wire, 0, arena);
        function->merge(merged.data(), restored.data(), arena);
        function->merge(merged.data(), restored.data(), arena);
        expect_result(*function, merged.data(),
                      Field::create_field<TYPE_ARRAY>(Array(limit, value)));
        function->reset(restored.data());
        add_rows(*function, restored.data(), *input, arena, 1);
        expect_result(*function, restored.data(), array_field({value}));
    }
}

TEST(AggregateCollectSerializationTest, ScalarStringAndSetBytesDoNotChange) {
    for (const auto& type :
         DataTypes {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()}) {
        auto input = type->create_column();
        input->insert_default();
        input->insert_default();
        for (const auto* name : {"collect_list", "collect_set"}) {
            auto wire = ColumnString::create();
            BufferWritable writer(*wire);
            for (int version : {LEGACY_VERSION, NATIVE_VERSION}) {
                auto function = AggregateFunctionSimpleFactory::instance().get(
                        name, {type}, nullptr, false, version);
                ASSERT_NE(function, nullptr);
                function->set_version(version);
                Arena arena;
                AggregateFunctionGuard source(function.get());
                add_rows(*function, source.data(), *input, arena);
                function->serialize(source.data(), writer);
                writer.commit();
            }
            EXPECT_EQ(wire->get_data_at(0), wire->get_data_at(1));
        }
    }
}

TEST(AggregateCollectSerializationTest, ForeachForwardsQueryVersion) {
    const auto value = array_field({string_field("a\",\"b")});
    const auto type = std::make_shared<DataTypeArray>(make_nullable(string_array_type()));
    round_trip("collect_list_foreach", type, {array_field({value}), array_field({value, Field()})},
               array_field({array_field({value, value}), array_field({})}), NATIVE_VERSION,
               {.is_foreach = true, .column_names = {}});
}

TEST(AggregateCollectSerializationTest, QueryLocalStateCombinatorsForwardVersion) {
    auto type = string_array_type();
    auto state_type = std::make_shared<DataTypeAggState>(DataTypes {type}, false, "collect_list",
                                                         NATIVE_VERSION);
    auto nested = state_type->get_nested_function();
    auto combine = AggregateStateCombine::create(nested, {type}, state_type);
    auto union_function = AggregateStateUnion::create(nested, {state_type}, state_type);
    auto merge = AggregateStateMerge::create(nested, {state_type}, nested->get_return_type());
    combine->set_version(NATIVE_VERSION);
    union_function->set_version(NATIVE_VERSION);
    merge->set_version(NATIVE_VERSION);
    const auto value = array_field({string_field("a\",\"b")});
    auto input = type->create_column();
    input->insert(value);
    Arena arena;
    AggregateFunctionGuard combined(combine.get());
    AggregateFunctionGuard united(union_function.get());
    AggregateFunctionGuard merged(merge.get());
    add_rows(*combine, combined.data(), *input, arena);
    auto states = state_type->create_column();
    combine->insert_result_into(combined.data(), *states);
    add_rows(*union_function, united.data(), *states, arena);
    auto union_states = state_type->create_column();
    union_function->insert_result_into(united.data(), *union_states);
    add_rows(*merge, merged.data(), *union_states, arena);
    expect_result(*merge, merged.data(), array_field({value}));
}

class ThrowOnSerializeDataType final : public DataTypeString {
public:
    char* serialize(const IColumn&, char* data, int) const override {
        *data = 1;
        throw Exception(ErrorCode::MEM_ALLOC_FAILED, "mock serialize allocation failure");
    }
};

TEST(AggregateCollectSerializationTest, FailedNativeWritePreservesPreviousStateAndPrefix) {
    auto type = std::make_shared<ThrowOnSerializeDataType>();
    AggregateFunctionCollectListData<INVALID_TYPE, false> data({type});
    auto wire = ColumnString::create();
    BufferWritable writer(*wire);
    writer.write_c_string("previous");
    writer.commit();
    writer.write_c_string("prefix");
    EXPECT_THROW(data.write(writer, *type, NATIVE_VERSION), Exception);
    writer.commit();
    EXPECT_EQ(wire->get_data_at(0).to_string(), "previous");
    EXPECT_EQ(wire->get_data_at(1).to_string(), "prefix");
}

} // namespace
} // namespace doris
