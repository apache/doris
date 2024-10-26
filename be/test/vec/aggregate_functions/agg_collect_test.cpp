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

#include "common/logging.h"
#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/common/arena.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
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
                                  size_t input_nums) {
        using FieldType = typename DataType::FieldType;
        auto type = std::make_shared<DataType>();
        auto input_col = type->create_column();
        for (size_t i = 0; i < input_nums; ++i) {
            for (size_t j = 0; j < _repeated_times; ++j) {
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
            agg_function->add(place, column, i, &_agg_arena_pool);
        }
    }

    template <typename DataType>
    void test_agg_collect(const std::string& fn_name, size_t input_nums = 0) {
        DataTypes data_types = {(DataTypePtr)std::make_shared<DataType>()};
        LOG(INFO) << "test_agg_collect for " << fn_name << "(" << data_types[0]->get_name() << ")";
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        auto agg_function = factory.get(fn_name, data_types, false, -1);
        EXPECT_NE(agg_function, nullptr);

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        agg_collect_add_elements<DataType>(agg_function, place, input_nums);

        ColumnString buf;
        VectorBufferWriter buf_writer(buf);
        agg_function->serialize(place, buf_writer);
        buf_writer.commit();
        VectorBufferReader buf_reader(buf.get_data_at(0));
        agg_function->deserialize(place, buf_reader, &_agg_arena_pool);

        std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
        AggregateDataPtr place2 = memory2.get();
        agg_function->create(place2);

        agg_collect_add_elements<DataType>(agg_function, place2, input_nums);

        agg_function->merge(place, place2, &_agg_arena_pool);
        auto column_result = ColumnArray::create(data_types[0]->create_column());
        agg_function->insert_result_into(place, *column_result);
        EXPECT_EQ(column_result->size(), 1);
        EXPECT_EQ(column_result->get_offsets()[0],
                  is_distinct(fn_name) ? input_nums : 2 * input_nums * _repeated_times);

        auto column_result2 = ColumnArray::create(data_types[0]->create_column());
        agg_function->insert_result_into(place2, *column_result2);
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

    test_agg_collect<DataTypeDecimal<Decimal128V2>>("collect_list");
    test_agg_collect<DataTypeDecimal<Decimal128V2>>("collect_set");

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

    test_agg_collect<DataTypeDecimal<Decimal128V2>>("collect_list", 10);
    test_agg_collect<DataTypeDecimal<Decimal128V2>>("collect_set", 11);

    test_agg_collect<DataTypeDateTime>("collect_list", 5);
    test_agg_collect<DataTypeDateTime>("collect_set", 6);

    test_agg_collect<DataTypeString>("collect_list", 10);
    test_agg_collect<DataTypeString>("collect_set", 5);
}

} // namespace doris::vectorized
