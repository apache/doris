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
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <ostream>
#include <string>

#include "agg_function_test.h"
#include "common/logging.h"
#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arena.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris {
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

void register_aggregate_function_collect_list(AggregateFunctionSimpleFactory& factory);

class VAggCollectTest : public testing::Test {
public:
    void SetUp() {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        register_aggregate_function_collect_list(factory);
    }

    void TearDown() {}

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
        auto agg_function = factory.get(fn_name, data_types, false, -1);
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
        agg_function->insert_result_into(place, column_result->assume_mutable_ref());
        EXPECT_EQ(column_result->size(), 1);
        EXPECT_EQ(column_result->get_offsets()[0],
                  is_distinct(fn_name) ? input_nums : 2 * input_nums * _repeated_times);

        auto column_result2 =
                ColumnArray::create(std::move(make_nullable(data_types[0]->create_column())));
        agg_function->insert_result_into(place2, column_result2->assume_mutable_ref());
        EXPECT_EQ(column_result2->size(), 1);
        EXPECT_EQ(column_result2->get_offsets()[0],
                  is_distinct(fn_name) ? input_nums : input_nums * _repeated_times);

        agg_function->destroy(place);
        agg_function->destroy(place2);
    }

private:
    const size_t _repeated_times = 2;
    vectorized::Arena _agg_arena_pool;
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

    test_agg_collect<DataTypeDate>("collect_list");
    test_agg_collect<DataTypeDate>("collect_set");

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

    test_agg_collect<DataTypeDateTime>("collect_list", 5);
    test_agg_collect<DataTypeDateTime>("collect_set", 6);

    test_agg_collect<DataTypeString>("collect_list", 10);
    test_agg_collect<DataTypeString>("collect_set", 5);
}

TEST_F(VAggCollectTest, test_complex_data_type) {
    test_agg_collect<DataTypeInt8>("collect_list", 7, true);
    test_agg_collect<DataTypeInt128>("array_agg", 9, true);

    test_agg_collect<DataTypeDateTime>("collect_list", 5, true);
    test_agg_collect<DataTypeDateTime>("array_agg", 6, true);

    test_agg_collect<DataTypeString>("collect_list", 10, true);
    test_agg_collect<DataTypeString>("array_agg", 5, true);
}

struct AggregateFunctionCollectTest : public AggregateFunctiontest {};

TEST_F(AggregateFunctionCollectTest, test_collect_list_aint64) {
    create_agg("collect_list", false, {std::make_shared<DataTypeInt64>()});

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
               {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt32>()});

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
    create_agg("collect_set", false, {std::make_shared<DataTypeInt64>()});

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
               {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt32>()});

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

} // namespace doris::vectorized
