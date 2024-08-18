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
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

void register_aggregate_function_linear_histogram(AggregateFunctionSimpleFactory& factory);

class AggLinearHistogramTest : public testing::Test {
public:
    void SetUp() override {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        register_aggregate_function_linear_histogram(factory);
    }

    void TearDown() override {}

    template <typename DataType>
    void agg_linear_histogram_add_elements(AggregateFunctionPtr agg_function, AggregateDataPtr place, 
                                            size_t input_rows, double interval, double offset) {
        using FieldType = typename DataType::FieldType;

        std::shared_ptr<DataType> type;
        if constexpr (std::is_same_v<DataType, DataTypeDecimal<Decimal32>>) {
            type = std::make_shared<DataType>(9, 2);
        } else if constexpr (std::is_same_v<DataType, DataTypeDecimal<Decimal64>>) {
            type = std::make_shared<DataType>(18, 2);
        } else {
            type = std::make_shared<DataType>();
        }

        MutableColumns columns(3);
        columns[0] = type->create_column();
        columns[1] = ColumnFloat64::create();
        columns[2] = ColumnFloat64::create();

        for (size_t i = 0; i < input_rows; ++i) {
            auto item0 = FieldType(static_cast<uint64_t>(i));
            columns[0]->insert_data(reinterpret_cast<const char*>(&item0), 0);
            columns[1]->insert_data(reinterpret_cast<const char*>(&interval), 0);
            if (offset != 0) {
                columns[2]->insert_data(reinterpret_cast<const char*>(&offset), 0);
            }
        }

        EXPECT_EQ(columns[0]->size(), input_rows);

        if (offset != 0) {
            const IColumn* column[3] = {columns[0].get(), columns[1].get(), columns[2].get()};
            for (int i = 0; i < input_rows; i++) {
                agg_function->add(place, column, i, &_agg_arena_pool);
            }
        } else {
            const IColumn* column[2] = {columns[0].get(), columns[1].get()};
            for (int i = 0; i < input_rows; i++) {
                agg_function->add(place, column, i, &_agg_arena_pool);
            }
        }
    }

    template <typename DataType>
    void test_agg_linear_histogram(size_t input_rows, double interval, double offset) {
        std::shared_ptr<DataType> type;
        if constexpr (std::is_same_v<DataType, DataTypeDecimal<Decimal32>>) {
            type = std::make_shared<DataType>(9, 2);
        } else if constexpr (std::is_same_v<DataType, DataTypeDecimal<Decimal64>>) {
            type = std::make_shared<DataType>(18, 2);
        } else {
            type = std::make_shared<DataType>();
        }
        DataTypes data_types1 = {(DataTypePtr)type, 
                                 std::make_shared<DataTypeFloat64>()};
        DataTypes data_types2 = {(DataTypePtr)type,
                                 std::make_shared<DataTypeFloat64>(), 
                                 std::make_shared<DataTypeFloat64>()};

        auto data_types = (offset == 0) ? data_types1 : data_types2;

        GTEST_LOG_(INFO) << "test_agg_linear_histogram for type"
                  << "(" << data_types[0]->get_name() << ")";

        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        auto agg_function = factory.get("linear_histogram", data_types);
        EXPECT_NE(agg_function, nullptr);

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        agg_linear_histogram_add_elements<DataType>(agg_function, place, input_rows, interval, offset);

        ColumnString buf;
        VectorBufferWriter buf_writer(buf);
        agg_function->serialize(place, buf_writer);
        buf_writer.commit();
        VectorBufferReader buf_reader(buf.get_data_at(0));
        agg_function->deserialize(place, buf_reader, &_agg_arena_pool);

        std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
        AggregateDataPtr place2 = memory2.get();
        agg_function->create(place2);
        agg_linear_histogram_add_elements<DataType>(agg_function, place2, input_rows, interval, offset);
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
    GTEST_LOG_(INFO) << "no offset";
    test_agg_linear_histogram<DataTypeInt8>(100, 10, 0);
    test_agg_linear_histogram<DataTypeInt16>(100, 10, 0);
    test_agg_linear_histogram<DataTypeInt32>(100, 10, 0);
    test_agg_linear_histogram<DataTypeInt64>(100, 10, 0);
    test_agg_linear_histogram<DataTypeInt128>(100, 10, 0);
    test_agg_linear_histogram<DataTypeFloat32>(5, 0.5, 0);
    test_agg_linear_histogram<DataTypeFloat64>(5, 0.5, 0);

    test_agg_linear_histogram<DataTypeDecimal<Decimal32>>(5, 0.5, 0);
    test_agg_linear_histogram<DataTypeDecimal<Decimal64>>(5, 0.5, 0);
    test_agg_linear_histogram<DataTypeDecimal<Decimal128V2>>(5, 0.5, 0);
    test_agg_linear_histogram<DataTypeDecimal<Decimal128V3>>(5, 0.5, 0);
    test_agg_linear_histogram<DataTypeDecimal<Decimal256>>(5, 0.5, 0);

    GTEST_LOG_(INFO) << "has offset";
    test_agg_linear_histogram<DataTypeInt8>(100, 10, 10);
    test_agg_linear_histogram<DataTypeInt16>(100, 10, 10);
    test_agg_linear_histogram<DataTypeInt32>(100, 10, 10);
    test_agg_linear_histogram<DataTypeInt64>(100, 10, 10);
    test_agg_linear_histogram<DataTypeInt128>(100, 10, 10);
    test_agg_linear_histogram<DataTypeFloat32>(5, 0.5, 0.5);
    test_agg_linear_histogram<DataTypeFloat64>(5, 0.5, 0.5);

    test_agg_linear_histogram<DataTypeDecimal<Decimal32>>(5, 0.5, 0.5);
    test_agg_linear_histogram<DataTypeDecimal<Decimal64>>(5, 0.5, 0.5);
    test_agg_linear_histogram<DataTypeDecimal<Decimal128V2>>(5, 0.5, 0.5);
    test_agg_linear_histogram<DataTypeDecimal<Decimal128V3>>(5, 0.5, 0.5);
    test_agg_linear_histogram<DataTypeDecimal<Decimal256>>(5, 0.5, 0.5);
}

} // namespace doris::vectorized
