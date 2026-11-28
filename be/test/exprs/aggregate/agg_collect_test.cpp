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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "core/arena.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"
#include "core/types.h"
#include "exprs/aggregate/agg_function_test.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_collect.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {
class IColumn;
} // namespace doris

namespace doris {

void register_aggregate_function_collect_list(AggregateFunctionSimpleFactory& factory);

class VAggCollectTest : public testing::Test {
public:
    void SetUp() override {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        register_aggregate_function_collect_list(factory);
    }

    void TearDown() override {}

    bool is_distinct(const std::string& fn_name) { return fn_name == "collect_set"; }

    template <typename DataType>
    void agg_collect_add_elements(AggregateFunctionPtr agg_function, AggregateDataPtr place,
                                  size_t input_nums, bool support_complex = false) {
        using FieldType = typename DataType::FieldType;
        MutableColumnPtr input_col;
        if (support_complex) {
            auto type =
                    std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataType>()));
            input_col = type->create_column();
        } else {
            auto type = std::make_shared<DataType>();
            input_col = type->create_column();
        }
        for (size_t i = 0; i < input_nums; ++i) {
            for (size_t j = 0; j < _repeated_times; ++j) {
                if (support_complex) {
                    if constexpr (std::is_same_v<DataType, DataTypeString>) {
                        Array vec1 = {Field::create_field<TYPE_STRING>(
                                              String("item0" + std::to_string(i))),
                                      Field::create_field<TYPE_STRING>(
                                              String("item1" + std::to_string(i)))};
                        input_col->insert(Field::create_field<TYPE_ARRAY>(vec1));
                    } else {
                        input_col->insert_default();
                    }
                    continue;
                }
                if constexpr (std::is_same_v<DataType, DataTypeString>) {
                    auto item = std::string("item") + std::to_string(i);
                    input_col->insert_data(item.c_str(), item.size());
                } else if constexpr (std::is_same_v<DataType, DataTypeDateV2>) {
                    auto item = static_cast<uint32_t>(i);
                    input_col->insert_data(reinterpret_cast<const char*>(&item), 0);
                } else if constexpr (std::is_same_v<DataType, DataTypeDateTimeV2>) {
                    auto item = static_cast<uint64_t>(i);
                    input_col->insert_data(reinterpret_cast<const char*>(&item), 0);
                } else {
                    auto item = FieldType(static_cast<uint64_t>(i));
                    input_col->insert_data(reinterpret_cast<const char*>(&item), 0);
                }
            }
        }
        EXPECT_EQ(input_col->size(), input_nums * _repeated_times);

        const IColumn* column[1] = {input_col.get()};
        for (int i = 0; i < input_col->size(); i++) {
            agg_function->add(place, column, i, _agg_arena_pool);
        }
    }

    template <typename DataType>
    void test_agg_collect(const std::string& fn_name, size_t input_nums = 0,
                          bool support_complex = false) {
        DataTypes data_types = {(DataTypePtr)std::make_shared<DataType>()};
        if (support_complex) {
            data_types = {
                    (DataTypePtr)std::make_shared<DataTypeArray>(make_nullable(data_types[0]))};
        }
        LOG(INFO) << "test_agg_collect for " << fn_name << "(" << data_types[0]->get_name() << ")";
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        auto agg_function = factory.get(fn_name, data_types, nullptr, false, -1);
        EXPECT_NE(agg_function, nullptr);

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        agg_collect_add_elements<DataType>(agg_function, place, input_nums, support_complex);

        ColumnString buf;
        VectorBufferWriter buf_writer(buf);
        agg_function->serialize(place, buf_writer);
        buf_writer.commit();
        VectorBufferReader buf_reader(buf.get_data_at(0));
        agg_function->deserialize(place, buf_reader, _agg_arena_pool);

        std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
        AggregateDataPtr place2 = memory2.get();
        agg_function->create(place2);

        agg_collect_add_elements<DataType>(agg_function, place2, input_nums, support_complex);

        agg_function->merge(place, place2, _agg_arena_pool);
        auto column_result =
                ColumnArray::create(std::move(make_nullable(data_types[0]->create_column())));
        agg_function->insert_result_into(place, column_result->assert_mutable_ref());
        EXPECT_EQ(column_result->size(), 1);
        EXPECT_EQ(column_result->get_offsets()[0],
                  is_distinct(fn_name) ? input_nums : 2 * input_nums * _repeated_times);

        auto column_result2 =
                ColumnArray::create(std::move(make_nullable(data_types[0]->create_column())));
        agg_function->insert_result_into(place2, column_result2->assert_mutable_ref());
        EXPECT_EQ(column_result2->size(), 1);
        EXPECT_EQ(column_result2->get_offsets()[0],
                  is_distinct(fn_name) ? input_nums : input_nums * _repeated_times);

        agg_function->destroy(place);
        agg_function->destroy(place2);
    }

private:
    const size_t _repeated_times = 2;
    Arena _agg_arena_pool;
};

TEST_F(VAggCollectTest, test_empty) {
    test_agg_collect<DataTypeInt8>("collect_list");
    test_agg_collect<DataTypeInt8>("collect_set");
    test_agg_collect<DataTypeInt16>("collect_list");
    test_agg_collect<DataTypeInt16>("collect_set");
    test_agg_collect<DataTypeInt32>("collect_list");
    test_agg_collect<DataTypeInt32>("collect_set");
    test_agg_collect<DataTypeInt64>("collect_list");
    test_agg_collect<DataTypeInt64>("collect_set");
    test_agg_collect<DataTypeInt128>("collect_list");
    test_agg_collect<DataTypeInt128>("collect_set");

    test_agg_collect<DataTypeDecimalV2>("collect_list");
    test_agg_collect<DataTypeDecimalV2>("collect_set");

    test_agg_collect<DataTypeDateV2>("collect_list");
    test_agg_collect<DataTypeDateV2>("collect_set");

    test_agg_collect<DataTypeString>("collect_list");
    test_agg_collect<DataTypeString>("collect_set");
}

TEST_F(VAggCollectTest, test_with_data) {
    test_agg_collect<DataTypeInt32>("collect_list", 7);
    test_agg_collect<DataTypeInt32>("collect_set", 9);
    test_agg_collect<DataTypeInt128>("collect_list", 20);
    test_agg_collect<DataTypeInt128>("collect_set", 30);

    test_agg_collect<DataTypeDecimalV2>("collect_list", 10);
    test_agg_collect<DataTypeDecimalV2>("collect_set", 11);

    test_agg_collect<DataTypeDateTimeV2>("collect_list", 5);
    test_agg_collect<DataTypeDateTimeV2>("collect_set", 6);

    test_agg_collect<DataTypeString>("collect_list", 10);
    test_agg_collect<DataTypeString>("collect_set", 5);
}

TEST_F(VAggCollectTest, test_complex_data_type) {
    test_agg_collect<DataTypeInt8>("collect_list", 7, true);
    test_agg_collect<DataTypeInt128>("array_agg", 9, true);

    test_agg_collect<DataTypeDateTimeV2>("collect_list", 5, true);
    test_agg_collect<DataTypeDateTimeV2>("array_agg", 6, true);

    test_agg_collect<DataTypeString>("collect_list", 10, true);
    test_agg_collect<DataTypeString>("array_agg", 5, true);
}

TEST_F(VAggCollectTest, test_merge_preserve_initialized_max_size) {
    {
        const DataTypes argument_types {std::make_shared<DataTypeInt32>()};
        AggregateFunctionCollectListData<TYPE_INT, true> lhs(argument_types);
        AggregateFunctionCollectListData<TYPE_INT, true> rhs(argument_types);
        lhs.max_size = 2;
        lhs.data.push_back(1);
        lhs.data.push_back(2);
        rhs.data.push_back(3);
        rhs.data.push_back(4);

        lhs.merge(rhs);

        EXPECT_EQ(lhs.max_size, 2);
        EXPECT_EQ(lhs.size(), 2);
    }

    {
        const DataTypes argument_types {std::make_shared<DataTypeString>()};
        AggregateFunctionCollectSetData<TYPE_STRING, true> lhs(argument_types);
        AggregateFunctionCollectSetData<TYPE_STRING, true> rhs(argument_types);
        lhs.max_size = 1;
        lhs.data_set.insert(StringRef("lhs", sizeof("lhs") - 1));
        rhs.data_set.insert(StringRef("rhs", sizeof("rhs") - 1));
        Arena arena;

        lhs.merge(rhs, arena);

        EXPECT_EQ(lhs.max_size, 1);
        EXPECT_EQ(lhs.size(), 1);
    }

    {
        const DataTypes argument_types {std::make_shared<DataTypeString>()};
        AggregateFunctionCollectListData<TYPE_STRING, true> lhs(argument_types);
        AggregateFunctionCollectListData<TYPE_STRING, true> rhs(argument_types);
        lhs.max_size = 1;
        lhs.data->insert_data("lhs", sizeof("lhs") - 1);
        rhs.data->insert_data("rhs", sizeof("rhs") - 1);

        lhs.merge(rhs);

        EXPECT_EQ(lhs.max_size, 1);
        EXPECT_EQ(lhs.size(), 1);
    }

    {
        const DataTypePtr nested_type = std::make_shared<DataTypeInt32>();
        const DataTypes argument_types {
                std::make_shared<DataTypeArray>(make_nullable(nested_type))};
        AggregateFunctionCollectListData<TYPE_ARRAY, true> lhs(argument_types);
        AggregateFunctionCollectListData<TYPE_ARRAY, true> rhs(argument_types);
        lhs.max_size = 1;
        lhs.column_data->insert_default();
        rhs.column_data->insert_default();

        lhs.merge(rhs);

        EXPECT_EQ(lhs.max_size, 1);
        EXPECT_EQ(lhs.size(), 1);
    }
}

struct AggregateFunctionCollectTest : public AggregateFunctiontest {};

TEST_F(AggregateFunctionCollectTest, test_collect_list_aint64) {
    create_agg("collect_list", false, {std::make_shared<DataTypeInt64>()},
               std::make_shared<DataTypeInt64>());

    auto data_type = std::make_shared<DataTypeInt64>();
    auto array_data_type = std::make_shared<DataTypeArray>(make_nullable(data_type));

    auto off_column = ColumnOffset64::create();
    auto data_column = ColumnInt64::create();
    std::vector<ColumnArray::Offset64> offs = {0, 3};
    std::vector<int64_t> vals = {1, 2, 3};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }
    auto array_column =
            ColumnArray::create(make_nullable(data_column->clone()), std::move(off_column));

    execute(Block({ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3})}),
            ColumnWithTypeAndName(std::move(array_column), array_data_type, "column"));
}

TEST_F(AggregateFunctionCollectTest, test_collect_list_aint64_with_max_size) {
    create_agg("collect_list", false,
               {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt32>()},
               std::make_shared<DataTypeInt64>());

    auto data_type = std::make_shared<DataTypeInt64>();
    auto array_data_type = std::make_shared<DataTypeArray>(make_nullable(data_type));

    auto off_column = ColumnOffset64::create();
    auto data_column = ColumnInt64::create();
    std::vector<ColumnArray::Offset64> offs = {0, 3};
    std::vector<int64_t> vals = {1, 2, 3};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }
    auto array_column =
            ColumnArray::create(make_nullable(data_column->clone()), std::move(off_column));

    execute(Block({ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4}),
                   ColumnHelper::create_column_with_name<DataTypeInt32>({3, 3, 3, 3})}),
            ColumnWithTypeAndName(std::move(array_column), array_data_type, "column"));
}

TEST_F(AggregateFunctionCollectTest, test_collect_set_aint64) {
    create_agg("collect_set", false, {std::make_shared<DataTypeInt64>()},
               std::make_shared<DataTypeInt64>());

    auto data_type = std::make_shared<DataTypeInt64>();
    auto array_data_type = std::make_shared<DataTypeArray>(make_nullable(data_type));

    auto off_column = ColumnOffset64::create();
    auto data_column = ColumnInt64::create();
    std::vector<ColumnArray::Offset64> offs = {0, 3};
    std::vector<int64_t> vals = {2, 1, 3};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }
    auto array_column =
            ColumnArray::create(make_nullable(data_column->clone()), std::move(off_column));

    execute(Block({ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3})}),
            ColumnWithTypeAndName(std::move(array_column), array_data_type, "column"));
}

TEST_F(AggregateFunctionCollectTest, test_collect_set_aint64_with_max_size) {
    create_agg("collect_set", false,
               {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt32>()},
               std::make_shared<DataTypeInt64>());

    auto data_type = std::make_shared<DataTypeInt64>();
    auto array_data_type = std::make_shared<DataTypeArray>(make_nullable(data_type));

    auto off_column = ColumnOffset64::create();
    auto data_column = ColumnInt64::create();
    std::vector<ColumnArray::Offset64> offs = {0, 3};
    std::vector<int64_t> vals = {2, 1, 3};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }
    auto array_column =
            ColumnArray::create(make_nullable(data_column->clone()), std::move(off_column));

    execute(Block({ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3, 4, 3}),
                   ColumnHelper::create_column_with_name<DataTypeInt32>({3, 3, 3, 3, 3})}),
            ColumnWithTypeAndName(std::move(array_column), array_data_type, "column"));
}

namespace {

constexpr size_t kValueSize = 201;

// `count` distinct strings of kValueSize bytes each.
std::vector<std::string> distinct_values(size_t count) {
    std::vector<std::string> values;
    for (size_t i = 0; i < count; ++i) {
        auto suffix = std::to_string(1000 + i);
        values.push_back(std::string(kValueSize - suffix.size(), 'x') + suffix);
    }
    return values;
}

// `rows` rows that cycle over `values`.
ColumnString::MutablePtr cycling_column(const std::vector<std::string>& values, size_t rows) {
    auto column = ColumnString::create();
    for (size_t row = 0; row < rows; ++row) {
        const auto& value = values[row % values.size()];
        column->insert_data(value.data(), value.size());
    }
    return column;
}

template <typename Set>
std::vector<std::string> sorted_keys(const Set& set) {
    std::vector<std::string> keys;
    for (const auto& key : set) {
        keys.push_back(key.to_string());
    }
    std::ranges::sort(keys);
    return keys;
}

std::vector<std::string> sorted(std::vector<std::string> values) {
    std::ranges::sort(values);
    return values;
}

} // namespace

// The arena frees nothing before it is destroyed, so its used size is what the state keeps:
// one copy per distinct value, not one per input row.
TEST(CollectSetStringArenaTest, AddCopiesEachDistinctValueOnce) {
    auto values = distinct_values(10);
    values.emplace_back();
    auto column = cycling_column(values, 20000);
    const DataTypes argument_types {std::make_shared<DataTypeString>()};
    AggregateFunctionCollectSetData<TYPE_STRING, false> data(argument_types);
    Arena arena;

    for (size_t row = 0; row < column->size(); ++row) {
        data.add(*column, row, arena);
    }

    // Copying before the lookup kept a copy of every row: 18,182 non-empty rows * 201 bytes.
    EXPECT_EQ(arena.used_size(), 10 * kValueSize);
    std::ranges::fill(column->get_chars(), '?');
    EXPECT_EQ(sorted_keys(data.data_set), sorted(values));
    for (const auto& key : data.data_set) {
        EXPECT_TRUE(arena.contains(key.data, key.size));
    }
}

TEST(CollectSetStringArenaTest, AddWithLimitCopiesEachDistinctValueOnce) {
    const DataTypes argument_types {std::make_shared<DataTypeString>(),
                                    std::make_shared<DataTypeInt32>()};
    auto function = AggregateFunctionSimpleFactory::instance().get("collect_set", argument_types,
                                                                   nullptr, false, -1);
    ASSERT_NE(function, nullptr);
    const auto values = distinct_values(10);
    auto column = cycling_column(values, 20000);
    auto limit = ColumnInt32::create(column->size(), 100);
    const IColumn* columns[] = {column.get(), limit.get()};
    Arena arena;
    AggregateFunctionGuard state(function.get());

    function->add_batch_single_place(column->size(), state.data(), columns, arena);

    // The limit counts distinct values only, so it never stopped the copies of duplicates:
    // copying before the lookup kept 20,000 * 201 bytes here.
    EXPECT_EQ(arena.used_size(), 10 * kValueSize);
}

TEST(CollectSetStringArenaTest, MergeCopiesOnlyNewValues) {
    const DataTypes argument_types {std::make_shared<DataTypeString>()};
    const auto values = distinct_values(10);
    AggregateFunctionCollectSetData<TYPE_STRING, false> destination(argument_types);
    Arena arena;
    {
        AggregateFunctionCollectSetData<TYPE_STRING, false> source(argument_types);
        Arena source_arena;
        auto column = cycling_column(values, values.size());
        for (size_t row = 0; row < column->size(); ++row) {
            source.add(*column, row, source_arena);
        }
        for (int repeat = 0; repeat < 1000; ++repeat) {
            destination.merge(source, arena);
        }
    }

    // Copying before the lookup used 1,000 * 10 * 201 bytes here.
    EXPECT_EQ(arena.used_size(), 10 * kValueSize);
    EXPECT_EQ(sorted_keys(destination.data_set), sorted(values));
}

TEST(CollectSetStringArenaTest, MergeWithLimitCopiesOnlyNewValuesAndKeepsTheLimit) {
    const DataTypes argument_types {std::make_shared<DataTypeString>()};
    const auto values = distinct_values(10);
    AggregateFunctionCollectSetData<TYPE_STRING, true> source(argument_types);
    Arena source_arena;
    auto column = cycling_column(values, values.size());
    for (size_t row = 0; row < column->size(); ++row) {
        source.add(*column, row, source_arena);
    }

    for (Int64 max_size : {3, 100}) {
        AggregateFunctionCollectSetData<TYPE_STRING, true> destination(argument_types);
        destination.max_size = max_size;
        Arena arena;
        for (int repeat = 0; repeat < 100; ++repeat) {
            destination.merge(source, arena);
        }

        const auto kept = std::min(static_cast<size_t>(max_size), values.size());
        EXPECT_EQ(destination.size(), kept);
        EXPECT_EQ(arena.used_size(), kept * kValueSize);
    }
}

TEST(CollectSetStringArenaTest, DeserializeAndMergeCopiesOnlyNewValues) {
    const DataTypes argument_types {std::make_shared<DataTypeString>()};
    auto function = AggregateFunctionSimpleFactory::instance().get("collect_set", argument_types,
                                                                   nullptr, false, -1);
    ASSERT_NE(function, nullptr);
    const auto values = distinct_values(10);
    auto serialized = function->get_serialized_type()->create_column();
    {
        Arena source_arena;
        AggregateFunctionGuard source(function.get());
        auto column = cycling_column(values, 1000);
        const IColumn* columns[] = {column.get()};
        function->add_batch_single_place(column->size(), source.data(), columns, source_arena);
        for (int repeat = 0; repeat < 100; ++repeat) {
            function->serialize_without_key_to_column(source.data(), *serialized);
        }
    }

    Arena arena;
    AggregateFunctionGuard destination(function.get());
    function->deserialize_and_merge_from_column(destination.data(), *serialized, arena);

    // 100 serialized copies of the same 10 values; copying before the lookup kept all of them.
    EXPECT_EQ(arena.used_size(), 10 * kValueSize);
    auto& serialized_chars = assert_cast<ColumnString&>(*serialized).get_chars();
    std::ranges::fill(serialized_chars, '?');
    auto result = ColumnArray::create(make_nullable(ColumnString::create()));
    function->insert_result_into(destination.data(), *result);
    const auto& strings =
            assert_cast<const ColumnNullable&>(result->get_data()).get_nested_column();
    std::vector<std::string> actual;
    for (size_t i = 0; i < strings.size(); ++i) {
        actual.push_back(strings.get_data_at(i).to_string());
    }
    EXPECT_EQ(sorted(actual), sorted(values));
}

} // namespace doris
