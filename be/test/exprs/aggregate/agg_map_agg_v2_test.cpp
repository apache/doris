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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/exception.h"
#include "core/column/column_map.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "core/pod_array.h"
#include "exprs/aggregate/aggregate_function_map_v2.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/aggregate_function_state_merge.h"
#include "exprs/aggregate/aggregate_function_state_union.h"

namespace doris {
namespace {

void check_state_union_and_merge_round_trip(const DataTypePtr& key_type,
                                            const std::vector<Field>& keys) {
    const auto be_exec_version = BeExecVersionManager::get_newest_version();
    const DataTypes argument_types {make_nullable(key_type),
                                    make_nullable(std::make_shared<DataTypeInt32>())};
    auto state_type = std::make_shared<DataTypeAggState>(argument_types, false, "map_agg_v2",
                                                         be_exec_version);
    auto nested_function = state_type->get_nested_function();
    ASSERT_NE(nested_function, nullptr);

    auto key_column = argument_types[0]->create_column();
    auto value_column = argument_types[1]->create_column();
    for (size_t i = 0; i < keys.size(); ++i) {
        key_column->insert(keys[i]);
        value_column->insert(Field::create_field<TYPE_INT>(static_cast<Int32>(i)));
    }
    const IColumn* input_columns[] = {key_column.get(), value_column.get()};

    Arena arena;
    auto state_column = state_type->create_column();
    nested_function->streaming_agg_serialize_to_column(input_columns, state_column, keys.size(),
                                                       arena);
    ASSERT_EQ(state_column->size(), keys.size());

    auto union_function =
            AggregateStateUnion::create(nested_function, DataTypes {state_type}, state_type);
    union_function->set_version(be_exec_version);
    AggregateFunctionGuard union_state(union_function.get());
    const IColumn* state_columns[] = {state_column.get()};
    union_function->add_batch_single_place(state_column->size(), union_state.data(), state_columns,
                                           arena);

    auto serialized_union = union_function->create_serialize_column();
    std::vector<AggregateDataPtr> union_places {union_state.data()};
    union_function->serialize_to_column(union_places, 0, serialized_union, 1);
    ASSERT_EQ(serialized_union->size(), 1);

    AggregateFunctionGuard round_tripped_union(union_function.get());
    union_function->deserialize_and_merge_from_column_range(round_tripped_union.data(),
                                                            *serialized_union, 0, 0, arena);
    auto union_result = state_type->create_column();
    union_function->insert_result_into(round_tripped_union.data(), *union_result);
    ASSERT_EQ(union_result->size(), 1);

    auto merge_function = AggregateStateMerge::create(nested_function, DataTypes {state_type},
                                                      nested_function->get_return_type());
    merge_function->set_version(be_exec_version);
    AggregateFunctionGuard merged_state(merge_function.get());
    const IColumn* union_result_columns[] = {union_result.get()};
    merge_function->add_batch_single_place(union_result->size(), merged_state.data(),
                                           union_result_columns, arena);

    auto result = nested_function->get_return_type()->create_column();
    merge_function->insert_result_into(merged_state.data(), *result);
    const auto& result_map = assert_cast<const ColumnMap&>(*result);
    ASSERT_EQ(result_map.size(), 1);
    EXPECT_EQ(result_map.get_keys().size(), keys.size());
}

template <bool use_exact_key_frame>
std::pair<size_t, size_t> serialize_and_deserialize_state(const DataTypePtr& key_type,
                                                          const std::vector<Field>& keys,
                                                          int be_exec_version) {
    const DataTypes argument_types {make_nullable(key_type),
                                    make_nullable(std::make_shared<DataTypeInt32>())};
    AggregateFunctionMapAggDataV2<use_exact_key_frame> state(argument_types, be_exec_version);
    for (size_t i = 0; i < keys.size(); ++i) {
        state.add_single(keys[i], Field::create_field<TYPE_INT>(static_cast<Int32>(i)));
    }

    auto serialized_state = ColumnString::create();
    BufferWritable writer(*serialized_state);
    state.write(writer);
    writer.commit();

    PaddedPODArray<UInt8> key_frame;
    PaddedPODArray<UInt8> value_frame;
    BufferReadable frame_reader(serialized_state->get_data_at(0));
    frame_reader.read_binary(key_frame);
    frame_reader.read_binary(value_frame);

    AggregateFunctionMapAggDataV2<use_exact_key_frame> restored_state(argument_types,
                                                                      be_exec_version);
    BufferReadable state_reader(serialized_state->get_data_at(0));
    restored_state.read(state_reader);

    auto result_type = std::make_shared<DataTypeMap>(argument_types[0], argument_types[1]);
    auto result = result_type->create_column();
    restored_state.insert_result_into(*result);
    const auto& result_map = assert_cast<const ColumnMap&>(*result);
    EXPECT_EQ(result_map.get_keys().size(), keys.size());

    return {key_frame.size(), value_frame.size()};
}

void serialize_foreach_v2_key_frame(int be_exec_version, size_t& key_frame_size) {
    const auto key_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto value_type = make_nullable(std::make_shared<DataTypeInt32>());
    const DataTypes argument_types {std::make_shared<DataTypeArray>(key_type),
                                    std::make_shared<DataTypeArray>(value_type)};
    const auto map_type = std::make_shared<DataTypeMap>(key_type, value_type);
    const auto result_type = std::make_shared<DataTypeArray>(make_nullable(map_type));
    auto function = AggregateFunctionSimpleFactory::instance().get(
            "map_agg_v2_foreachv2", argument_types, result_type, false, be_exec_version);
    ASSERT_NE(function, nullptr);
    function->set_version(be_exec_version);

    constexpr auto key_count = SERIALIZED_MEM_SIZE_LIMIT / sizeof(Int32) + 1;
    auto key_column = argument_types[0]->create_column();
    auto value_column = argument_types[1]->create_column();
    for (size_t i = 0; i < key_count; ++i) {
        key_column->insert(Field::create_field<TYPE_ARRAY>(
                Array {Field::create_field<TYPE_INT>(static_cast<Int32>(i + 1))}));
        value_column->insert(Field::create_field<TYPE_ARRAY>(
                Array {Field::create_field<TYPE_INT>(static_cast<Int32>(i))}));
    }
    const IColumn* input_columns[] = {key_column.get(), value_column.get()};

    Arena arena;
    AggregateFunctionGuard state(function.get());
    for (size_t i = 0; i < key_count; ++i) {
        function->add(state.data(), input_columns, static_cast<ssize_t>(i), arena);
    }

    auto serialized_state = ColumnString::create();
    BufferWritable writer(*serialized_state);
    function->serialize(state.data(), writer);
    writer.commit();

    BufferReadable reader(serialized_state->get_data_at(0));
    size_t dynamic_array_size = 0;
    reader.read_binary(dynamic_array_size);
    EXPECT_EQ(dynamic_array_size, 1);
    PaddedPODArray<UInt8> key_frame;
    PaddedPODArray<UInt8> value_frame;
    reader.read_binary(key_frame);
    reader.read_binary(value_frame);
    EXPECT_FALSE(reader.has_remaining());
    key_frame_size = key_frame.size();
}

} // namespace

TEST(AggregateFunctionMapAggV2Test, SupportsIpKeyTypes) {
    const DataTypes key_types {std::make_shared<DataTypeIPv4>(), std::make_shared<DataTypeIPv6>()};

    for (const auto& key_type : key_types) {
        const DataTypes argument_types {make_nullable(key_type),
                                        make_nullable(std::make_shared<DataTypeInt32>())};
        auto function = AggregateFunctionSimpleFactory::instance().get(
                "map_agg_v2", argument_types, nullptr, false,
                BeExecVersionManager::get_newest_version());

        ASSERT_NE(function, nullptr) << key_type->get_name();

        auto legacy_function = AggregateFunctionSimpleFactory::instance().get(
                "map_agg_v2", argument_types, nullptr, false,
                SUPPORT_MAP_AGG_V2_EXACT_FRAME_VERSION - 1);
        EXPECT_EQ(legacy_function, nullptr) << key_type->get_name();
    }
}

TEST(AggregateFunctionMapAggV2Test, IpStateUnionAndMergeRoundTrip) {
    constexpr auto ipv4_key_count = SERIALIZED_MEM_SIZE_LIMIT / sizeof(IPv4) + 1;
    std::vector<Field> ipv4_keys;
    ipv4_keys.reserve(ipv4_key_count);
    for (size_t i = 0; i < ipv4_key_count; ++i) {
        ipv4_keys.push_back(Field::create_field<TYPE_IPV4>(IPv4(i + 1)));
    }
    check_state_union_and_merge_round_trip(std::make_shared<DataTypeIPv4>(), ipv4_keys);

    constexpr auto ipv6_key_count = SERIALIZED_MEM_SIZE_LIMIT / sizeof(IPv6) + 1;
    std::vector<Field> ipv6_keys;
    ipv6_keys.reserve(ipv6_key_count);
    for (size_t i = 0; i < ipv6_key_count; ++i) {
        ipv6_keys.push_back(Field::create_field<TYPE_IPV6>(IPv6(i + 1)));
    }
    check_state_union_and_merge_round_trip(std::make_shared<DataTypeIPv6>(), ipv6_keys);
}

TEST(AggregateFunctionMapAggV2Test, ExactAndLegacyStateFrames) {
    constexpr auto key_count = SERIALIZED_MEM_SIZE_LIMIT / sizeof(Int32) + 1;
    std::vector<Field> keys;
    keys.reserve(key_count);
    for (size_t i = 0; i < key_count; ++i) {
        keys.push_back(Field::create_field<TYPE_INT>(static_cast<Int32>(i + 1)));
    }

    const auto exact_frame_sizes = serialize_and_deserialize_state<true>(
            std::make_shared<DataTypeInt32>(), keys, SUPPORT_MAP_AGG_V2_EXACT_FRAME_VERSION);
    const auto legacy_frame_sizes = serialize_and_deserialize_state<false>(
            std::make_shared<DataTypeInt32>(), keys, SUPPORT_MAP_AGG_V2_EXACT_FRAME_VERSION - 1);

    EXPECT_LT(exact_frame_sizes.first, legacy_frame_sizes.first);
    EXPECT_EQ(exact_frame_sizes.second, legacy_frame_sizes.second);
}

TEST(AggregateFunctionMapAggV2Test, PersistedStateCompatibleAcrossFrameVersion) {
    constexpr auto legacy_version = SUPPORT_MAP_AGG_V2_EXACT_FRAME_VERSION - 1;
    const DataTypes argument_types {make_nullable(std::make_shared<DataTypeInt32>()),
                                    make_nullable(std::make_shared<DataTypeInt32>())};
    auto legacy_state_type =
            std::make_shared<DataTypeAggState>(argument_types, false, "map_agg_v2", legacy_version);
    auto current_state_type = std::make_shared<DataTypeAggState>(
            argument_types, false, "map_agg_v2", SUPPORT_MAP_AGG_V2_EXACT_FRAME_VERSION);

    EXPECT_TRUE(legacy_state_type->get_serialized_type()->equals(
            *current_state_type->get_serialized_type()));
    EXPECT_NO_THROW(legacy_state_type->check_function_compatibility(
            SUPPORT_MAP_AGG_V2_EXACT_FRAME_VERSION));
}

TEST(AggregateFunctionMapAggV2Test, ForeachV2UsesNegotiatedFrameVersion) {
    size_t exact_frame_size = 0;
    size_t legacy_frame_size = 0;
    serialize_foreach_v2_key_frame(SUPPORT_MAP_AGG_V2_EXACT_FRAME_VERSION, exact_frame_size);
    serialize_foreach_v2_key_frame(SUPPORT_MAP_AGG_V2_EXACT_FRAME_VERSION - 1, legacy_frame_size);

    EXPECT_LT(exact_frame_size, legacy_frame_size);
}

TEST(AggregateFunctionMapAggV2Test, NestedFunctionsPreserveLegacyWrapperAvailability) {
    constexpr auto legacy_version = SUPPORT_MAP_AGG_V2_EXACT_FRAME_VERSION - 1;
    const auto double_type = std::make_shared<DataTypeFloat64>();
    const auto array_type = std::make_shared<DataTypeArray>(double_type);
    const auto result_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>()));
    const std::vector<std::pair<std::string, size_t>> test_cases {
            {"stddev_samp_foreachv2", 1},
            {"variance_samp_foreachv2", 1},
            {"covar_samp_foreachv2", 2},
    };

    for (const auto& [name, argument_count] : test_cases) {
        SCOPED_TRACE(name);
        DataTypes argument_types(argument_count, array_type);
        AggregateFunctionPtr function;
        EXPECT_NO_THROW(function = AggregateFunctionSimpleFactory::instance().get(
                                name, argument_types, result_type, false, legacy_version));
        EXPECT_NE(function, nullptr);
    }

    EXPECT_THROW(
            AggregateFunctionSimpleFactory::instance().get("stddev_samp", DataTypes {double_type},
                                                           double_type, false, legacy_version),
            Exception);
}

} // namespace doris
