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

#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/common/arena.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

void register_aggregate_function_linear_histogram(AggregateFunctionSimpleFactory& factory);

class AggLinearHistogramTest : public testing::Test {
    const static size_t INTERVAL = 10;
public:
    void SetUp() override {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        register_aggregate_function_linear_histogram(factory);
    }

    void TearDown() override {}

    template <typename DataType>
    void agg_linear_histogram_add_elements(AggregateFunctionPtr agg_function, AggregateDataPtr place, size_t input_rows) {
        using FieldType = typename DataType::FieldType;
        auto type = std::make_shared<DataType>();

        MutableColumns columns(2);
        columns[0] = type->create_column();
        columns[1] = type->create_column();

        for (size_t i = 0; i < input_rows; ++i) {
            auto item0 = FieldType(static_cast<uint64_t>(i));
            columns[0]->insert_data(reinterpret_cast<const char*>(&item0), 0);

            auto item1 = FieldType(static_cast<uint64_t>(INTERVAL));
            columns[1]->insert_data(reinterpret_cast<const char*>(&item1), 0);
        }

        EXPECT_EQ(columns[0]->size(), input_rows);

        const IColumn* column[2] = {columns[0].get(), columns[1].get()};
        for (int i = 0; i < input_rows; i++) {
            agg_function->add(place, column, i, &_agg_arena_pool);
        }
    }

    template <typename DataType>
    void test_agg_linear_histogram(size_t input_rows) {
        DataTypes data_types = {(DataTypePtr)std::make_shared<DataType>()};

        GTEST_LOG_(INFO) << "test_agg_linear_histogram for type"
                  << "(" << data_types[0]->get_name() << ")";

        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        auto agg_function = factory.get("linear_histogram", data_types);
        EXPECT_NE(agg_function, nullptr);

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        agg_linear_histogram_add_elements<DataType>(agg_function, place, input_rows);

        ColumnString buf;
        VectorBufferWriter buf_writer(buf);
        agg_function->serialize(place, buf_writer);
        buf_writer.commit();
        VectorBufferReader buf_reader(buf.get_data_at(0));
        agg_function->deserialize(place, buf_reader, &_agg_arena_pool);

        std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
        AggregateDataPtr place2 = memory2.get();
        agg_function->create(place2);
        agg_linear_histogram_add_elements<DataType>(agg_function, place2, input_rows);
        agg_function->merge(place, place2, &_agg_arena_pool);

        auto column_result1 = ColumnString::create();
        agg_function->insert_result_into(place, *column_result1);
        EXPECT_EQ(column_result1->size(), 1);
        EXPECT_TRUE(column_result1->get_offsets()[0] >= 1);

        auto column_result2 = ColumnString::create();
        agg_function->insert_result_into(place2, *column_result2);
        EXPECT_EQ(column_result2->size(), 1);
        EXPECT_TRUE(column_result2->get_offsets()[0] >= 1);

        GTEST_LOG_(INFO) << column_result1->get_data_at(0).to_string();
        GTEST_LOG_(INFO) << column_result2->get_data_at(0).to_string();
    }

private:
    vectorized::Arena _agg_arena_pool;
};

TEST_F(AggLinearHistogramTest, test_with_data) {
    // rows 100
    test_agg_linear_histogram<DataTypeInt8>(100);
    test_agg_linear_histogram<DataTypeInt16>(100);
    test_agg_linear_histogram<DataTypeInt32>(100);
    test_agg_linear_histogram<DataTypeInt64>(100);
    test_agg_linear_histogram<DataTypeInt128>(100);
    test_agg_linear_histogram<DataTypeFloat32>(100);
    test_agg_linear_histogram<DataTypeFloat64>(100);
}

} // namespace doris::vectorized
