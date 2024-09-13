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
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <ostream>
#include <string>

#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arena.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time_v2.h"

namespace doris::vectorized {

void register_aggregate_function_histogram(AggregateFunctionSimpleFactory& factory);

class VAggHistogramTest : public testing::Test {
public:
    void SetUp() override {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        register_aggregate_function_histogram(factory);
    }

    void TearDown() override {}

    template <typename DataType>
    void agg_histogram_add_elements(AggregateFunctionPtr agg_function, AggregateDataPtr place,
                                    size_t input_rows, size_t max_num_buckets) {
        using FieldType = typename DataType::FieldType;
        auto type = std::make_shared<DataType>();

        if (max_num_buckets == 0) {
            auto input_col = type->create_column();
            for (size_t i = 0; i < input_rows; ++i) {
                if constexpr (std::is_same_v<DataType, DataTypeString>) {
                    auto item = std::string("item") + std::to_string(i);
                    input_col->insert_data(item.c_str(), item.size());
                } else {
                    auto item = FieldType(static_cast<uint64_t>(i));
                    input_col->insert_data(reinterpret_cast<const char*>(&item), 0);
                }
            }

            EXPECT_EQ(input_col->size(), input_rows);

            const IColumn* column[1] = {input_col.get()};
            for (int i = 0; i < input_col->size(); i++) {
                agg_function->add(place, column, i, &_agg_arena_pool);
            }

            return;
        }

        MutableColumns columns(2);
        columns[0] = type->create_column();
        columns[1] = ColumnInt32::create();

        for (size_t i = 0; i < input_rows; ++i) {
            if constexpr (std::is_same_v<DataType, DataTypeString>) {
                auto item = std::string("item") + std::to_string(i);
                columns[0]->insert_data(item.c_str(), item.size());
            } else {
                auto item = FieldType(static_cast<uint64_t>(i));
                columns[0]->insert_data(reinterpret_cast<const char*>(&item), 0);
            }
            columns[1]->insert_data(reinterpret_cast<char*>(&max_num_buckets),
                                    sizeof(max_num_buckets));
        }

        EXPECT_EQ(columns[0]->size(), input_rows);

        const IColumn* column[2] = {columns[0].get(), columns[1].get()};
        for (int i = 0; i < input_rows; i++) {
            agg_function->add(place, column, i, &_agg_arena_pool);
        }
    }

    template <typename DataType>
    void test_agg_histogram(size_t input_rows = 0, size_t max_num_buckets = 0) {
        DataTypes data_types1 = {(DataTypePtr)std::make_shared<DataType>()};
        DataTypes data_types2 = {(DataTypePtr)std::make_shared<DataType>(),
                                 std::make_shared<DataTypeInt32>()};

        auto data_types = (max_num_buckets == 0) ? data_types1 : data_types2;
        LOG(INFO) << "test_agg_histogram for type"
                  << "(" << data_types[0]->get_name() << ")";

        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        auto agg_function = factory.get("histogram", data_types, false, -1);
        EXPECT_NE(agg_function, nullptr);

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        agg_histogram_add_elements<DataType>(agg_function, place, input_rows, max_num_buckets);

        ColumnString buf;
        VectorBufferWriter buf_writer(buf);
        agg_function->serialize(place, buf_writer);
        buf_writer.commit();
        VectorBufferReader buf_reader(buf.get_data_at(0));
        agg_function->deserialize(place, buf_reader, &_agg_arena_pool);

        std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
        AggregateDataPtr place2 = memory2.get();
        agg_function->create(place2);
        agg_histogram_add_elements<DataType>(agg_function, place2, input_rows, max_num_buckets);
        agg_function->merge(place, place2, &_agg_arena_pool);

        auto column_result1 = ColumnString::create();
        agg_function->insert_result_into(place, *column_result1);
        EXPECT_EQ(column_result1->size(), 1);
        EXPECT_TRUE(column_result1->get_offsets()[0] >= 1);

        auto column_result2 = ColumnString::create();
        agg_function->insert_result_into(place2, *column_result2);
        EXPECT_EQ(column_result2->size(), 1);
        EXPECT_TRUE(column_result2->get_offsets()[0] >= 1);

        LOG(INFO) << column_result1->get_data_at(0).to_string();
        LOG(INFO) << column_result2->get_data_at(0).to_string();

        // test empty data
        if (input_rows == 0 && max_num_buckets == 0) {
            std::string expect_empty_result = "{\"num_buckets\":0,\"buckets\":[]}";
            std::string empty_result1 = column_result1->get_data_at(0).to_string();
            std::string empty_result2 = column_result2->get_data_at(0).to_string();
            EXPECT_EQ(empty_result1, expect_empty_result);
            EXPECT_EQ(empty_result2, expect_empty_result);
        }

        // test with data
        if (input_rows == 1000 && max_num_buckets == 5) {
            if constexpr (std::is_same_v<DataType, DataTypeInt32>) {
                std::string expect_result1 =
                        "{\"num_buckets\":5,\"buckets\":["
                        "{\"lower\":\"0\",\"upper\":\"189\",\"count\":200,\"pre_sum\":0,\"ndv\":"
                        "151},"
                        "{\"lower\":\"190\",\"upper\":\"380\",\"count\":200,\"pre_sum\":200,"
                        "\"ndv\":149},"
                        "{\"lower\":\"382\",\"upper\":\"582\",\"count\":200,\"pre_sum\":400,"
                        "\"ndv\":150},"
                        "{\"lower\":\"586\",\"upper\":\"796\",\"count\":200,\"pre_sum\":600,"
                        "\"ndv\":157},"
                        "{\"lower\":\"797\",\"upper\":\"999\",\"count\":200,\"pre_sum\":800,"
                        "\"ndv\":147}]}";
                std::string expect_result2 =
                        "{\"num_buckets\":5,\"buckets\":["
                        "{\"lower\":\"0\",\"upper\":\"207\",\"count\":100,\"pre_sum\":0,\"ndv\":"
                        "100},"
                        "{\"lower\":\"209\",\"upper\":\"410\",\"count\":100,\"pre_sum\":100,"
                        "\"ndv\":100},"
                        "{\"lower\":\"412\",\"upper\":\"599\",\"count\":100,\"pre_sum\":200,"
                        "\"ndv\":100},"
                        "{\"lower\":\"600\",\"upper\":\"797\",\"count\":100,\"pre_sum\":300,"
                        "\"ndv\":100},"
                        "{\"lower\":\"799\",\"upper\":\"998\",\"count\":100,\"pre_sum\":400,"
                        "\"ndv\":100}]}";
                std::string result1 = column_result1->get_data_at(0).to_string();
                std::string result2 = column_result2->get_data_at(0).to_string();
                EXPECT_EQ(result1, expect_result1);
                EXPECT_EQ(result2, expect_result2);
            }
        }

        agg_function->destroy(place);
        agg_function->destroy(place2);
    }

private:
    vectorized::Arena _agg_arena_pool;
};

TEST_F(VAggHistogramTest, test_empty) {
    test_agg_histogram<DataTypeInt8>();
    test_agg_histogram<DataTypeInt16>();
    test_agg_histogram<DataTypeInt32>();
    test_agg_histogram<DataTypeInt64>();
    test_agg_histogram<DataTypeInt128>();

    test_agg_histogram<DataTypeFloat32>();
    test_agg_histogram<DataTypeFloat64>();

    test_agg_histogram<DataTypeDate>();
    test_agg_histogram<DataTypeDateTime>();
    test_agg_histogram<DataTypeString>();
    test_agg_histogram<DataTypeDecimal<Decimal128V2>>();
}

TEST_F(VAggHistogramTest, test_with_data) {
    // rows 1000, max bucket size 5
    test_agg_histogram<DataTypeString>(1000, 5);
    test_agg_histogram<DataTypeInt8>(100, 5);
    test_agg_histogram<DataTypeInt16>(100, 5);
    test_agg_histogram<DataTypeInt32>(100, 5);
    test_agg_histogram<DataTypeInt64>(100, 5);
    test_agg_histogram<DataTypeInt128>(100, 5);
    test_agg_histogram<DataTypeFloat32>(100, 5);
    test_agg_histogram<DataTypeFloat64>(100, 5);

    test_agg_histogram<DataTypeDate>(100, 5);
    test_agg_histogram<DataTypeDateV2>(100, 5);

    test_agg_histogram<DataTypeDateTime>(100, 5);
    test_agg_histogram<DataTypeDateTimeV2>(100, 5);

    test_agg_histogram<DataTypeDecimal<Decimal128V2>>(100, 5);
}

} // namespace doris::vectorized
