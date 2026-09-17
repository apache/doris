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

#include <cmath>
#include <vector>

#include "core/arena.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {

void register_aggregate_function_linear_histogram(AggregateFunctionSimpleFactory& factory);

std::string s1 =
        "{\"num_buckets\":10,\"buckets\":["
        "{\"lower\":0.0,\"upper\":10.0,\"count\":20,\"acc_count\":20},"
        "{\"lower\":10.0,\"upper\":20.0,\"count\":20,\"acc_count\":40},"
        "{\"lower\":20.0,\"upper\":30.0,\"count\":20,\"acc_count\":60},"
        "{\"lower\":30.0,\"upper\":40.0,\"count\":20,\"acc_count\":80},"
        "{\"lower\":40.0,\"upper\":50.0,\"count\":20,\"acc_count\":100},"
        "{\"lower\":50.0,\"upper\":60.0,\"count\":20,\"acc_count\":120},"
        "{\"lower\":60.0,\"upper\":70.0,\"count\":20,\"acc_count\":140},"
        "{\"lower\":70.0,\"upper\":80.0,\"count\":20,\"acc_count\":160},"
        "{\"lower\":80.0,\"upper\":90.0,\"count\":20,\"acc_count\":180},"
        "{\"lower\":90.0,\"upper\":100.0,\"count\":20,\"acc_count\":200}"
        "]}";
std::string s2 =
        "{\"num_buckets\":10,\"buckets\":["
        "{\"lower\":0.0,\"upper\":10.0,\"count\":10,\"acc_count\":10},"
        "{\"lower\":10.0,\"upper\":20.0,\"count\":10,\"acc_count\":20},"
        "{\"lower\":20.0,\"upper\":30.0,\"count\":10,\"acc_count\":30},"
        "{\"lower\":30.0,\"upper\":40.0,\"count\":10,\"acc_count\":40},"
        "{\"lower\":40.0,\"upper\":50.0,\"count\":10,\"acc_count\":50},"
        "{\"lower\":50.0,\"upper\":60.0,\"count\":10,\"acc_count\":60},"
        "{\"lower\":60.0,\"upper\":70.0,\"count\":10,\"acc_count\":70},"
        "{\"lower\":70.0,\"upper\":80.0,\"count\":10,\"acc_count\":80},"
        "{\"lower\":80.0,\"upper\":90.0,\"count\":10,\"acc_count\":90},"
        "{\"lower\":90.0,\"upper\":100.0,\"count\":10,\"acc_count\":100}"
        "]}";
std::string s3 =
        "{\"num_buckets\":11,\"buckets\":["
        "{\"lower\":-5.0,\"upper\":5.0,\"count\":10,\"acc_count\":10},"
        "{\"lower\":5.0,\"upper\":15.0,\"count\":20,\"acc_count\":30},"
        "{\"lower\":15.0,\"upper\":25.0,\"count\":20,\"acc_count\":50},"
        "{\"lower\":25.0,\"upper\":35.0,\"count\":20,\"acc_count\":70},"
        "{\"lower\":35.0,\"upper\":45.0,\"count\":20,\"acc_count\":90},"
        "{\"lower\":45.0,\"upper\":55.0,\"count\":20,\"acc_count\":110},"
        "{\"lower\":55.0,\"upper\":65.0,\"count\":20,\"acc_count\":130},"
        "{\"lower\":65.0,\"upper\":75.0,\"count\":20,\"acc_count\":150},"
        "{\"lower\":75.0,\"upper\":85.0,\"count\":20,\"acc_count\":170},"
        "{\"lower\":85.0,\"upper\":95.0,\"count\":20,\"acc_count\":190},"
        "{\"lower\":95.0,\"upper\":105.0,\"count\":10,\"acc_count\":200}"
        "]}";
std::string s4 =
        "{\"num_buckets\":11,\"buckets\":["
        "{\"lower\":-5.0,\"upper\":5.0,\"count\":5,\"acc_count\":5},"
        "{\"lower\":5.0,\"upper\":15.0,\"count\":10,\"acc_count\":15},"
        "{\"lower\":15.0,\"upper\":25.0,\"count\":10,\"acc_count\":25},"
        "{\"lower\":25.0,\"upper\":35.0,\"count\":10,\"acc_count\":35},"
        "{\"lower\":35.0,\"upper\":45.0,\"count\":10,\"acc_count\":45},"
        "{\"lower\":45.0,\"upper\":55.0,\"count\":10,\"acc_count\":55},"
        "{\"lower\":55.0,\"upper\":65.0,\"count\":10,\"acc_count\":65},"
        "{\"lower\":65.0,\"upper\":75.0,\"count\":10,\"acc_count\":75},"
        "{\"lower\":75.0,\"upper\":85.0,\"count\":10,\"acc_count\":85},"
        "{\"lower\":85.0,\"upper\":95.0,\"count\":10,\"acc_count\":95},"
        "{\"lower\":95.0,\"upper\":105.0,\"count\":5,\"acc_count\":100}"
        "]}";
std::string s5 =
        "{\"num_buckets\":9,\"buckets\":["
        "{\"lower\":0.0,\"upper\":0.5,\"count\":2,\"acc_count\":2},"
        "{\"lower\":0.5,\"upper\":1.0,\"count\":0,\"acc_count\":2},"
        "{\"lower\":1.0,\"upper\":1.5,\"count\":2,\"acc_count\":4},"
        "{\"lower\":1.5,\"upper\":2.0,\"count\":0,\"acc_count\":4},"
        "{\"lower\":2.0,\"upper\":2.5,\"count\":2,\"acc_count\":6},"
        "{\"lower\":2.5,\"upper\":3.0,\"count\":0,\"acc_count\":6},"
        "{\"lower\":3.0,\"upper\":3.5,\"count\":2,\"acc_count\":8},"
        "{\"lower\":3.5,\"upper\":4.0,\"count\":0,\"acc_count\":8},"
        "{\"lower\":4.0,\"upper\":4.5,\"count\":2,\"acc_count\":10}"
        "]}";
std::string s6 =
        "{\"num_buckets\":9,\"buckets\":["
        "{\"lower\":0.0,\"upper\":0.5,\"count\":1,\"acc_count\":1},"
        "{\"lower\":0.5,\"upper\":1.0,\"count\":0,\"acc_count\":1},"
        "{\"lower\":1.0,\"upper\":1.5,\"count\":1,\"acc_count\":2},"
        "{\"lower\":1.5,\"upper\":2.0,\"count\":0,\"acc_count\":2},"
        "{\"lower\":2.0,\"upper\":2.5,\"count\":1,\"acc_count\":3},"
        "{\"lower\":2.5,\"upper\":3.0,\"count\":0,\"acc_count\":3},"
        "{\"lower\":3.0,\"upper\":3.5,\"count\":1,\"acc_count\":4},"
        "{\"lower\":3.5,\"upper\":4.0,\"count\":0,\"acc_count\":4},"
        "{\"lower\":4.0,\"upper\":4.5,\"count\":1,\"acc_count\":5}"
        "]}";
std::string s7 =
        "{\"num_buckets\":9,\"buckets\":["
        "{\"lower\":-0.25,\"upper\":0.25,\"count\":2,\"acc_count\":2},"
        "{\"lower\":0.25,\"upper\":0.75,\"count\":0,\"acc_count\":2},"
        "{\"lower\":0.75,\"upper\":1.25,\"count\":2,\"acc_count\":4},"
        "{\"lower\":1.25,\"upper\":1.75,\"count\":0,\"acc_count\":4},"
        "{\"lower\":1.75,\"upper\":2.25,\"count\":2,\"acc_count\":6},"
        "{\"lower\":2.25,\"upper\":2.75,\"count\":0,\"acc_count\":6},"
        "{\"lower\":2.75,\"upper\":3.25,\"count\":2,\"acc_count\":8},"
        "{\"lower\":3.25,\"upper\":3.75,\"count\":0,\"acc_count\":8},"
        "{\"lower\":3.75,\"upper\":4.25,\"count\":2,\"acc_count\":10}"
        "]}";
std::string s8 =
        "{\"num_buckets\":9,\"buckets\":["
        "{\"lower\":-0.25,\"upper\":0.25,\"count\":1,\"acc_count\":1},"
        "{\"lower\":0.25,\"upper\":0.75,\"count\":0,\"acc_count\":1},"
        "{\"lower\":0.75,\"upper\":1.25,\"count\":1,\"acc_count\":2},"
        "{\"lower\":1.25,\"upper\":1.75,\"count\":0,\"acc_count\":2},"
        "{\"lower\":1.75,\"upper\":2.25,\"count\":1,\"acc_count\":3},"
        "{\"lower\":2.25,\"upper\":2.75,\"count\":0,\"acc_count\":3},"
        "{\"lower\":2.75,\"upper\":3.25,\"count\":1,\"acc_count\":4},"
        "{\"lower\":3.25,\"upper\":3.75,\"count\":0,\"acc_count\":4},"
        "{\"lower\":3.75,\"upper\":4.25,\"count\":1,\"acc_count\":5}"
        "]}";

class AggLinearHistogramTest : public testing::Test {
public:
    void SetUp() override {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        register_aggregate_function_linear_histogram(factory);
    }

    void TearDown() override {}

    template <typename DataType>
    void agg_linear_histogram_add_elements(AggregateFunctionPtr agg_function,
                                           AggregateDataPtr place, size_t input_rows,
                                           double interval, double offset) {
        using FieldType = typename DataType::FieldType;

        std::shared_ptr<DataType> type;
        if constexpr (IsDecimalNumber<FieldType>) {
            type = std::make_shared<DataType>(9, 0);
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
                agg_function->add(place, column, i, _agg_arena_pool);
            }
        } else {
            const IColumn* column[2] = {columns[0].get(), columns[1].get()};
            for (int i = 0; i < input_rows; i++) {
                agg_function->add(place, column, i, _agg_arena_pool);
            }
        }
    }

    template <typename DataType>
    void test_agg_linear_histogram(size_t input_rows, double interval, double offset) {
        using FieldType = typename DataType::FieldType;

        std::shared_ptr<DataType> type;
        if constexpr (IsDecimalNumber<FieldType>) {
            type = std::make_shared<DataType>(9, 0);
        } else {
            type = std::make_shared<DataType>();
        }
        DataTypes data_types1 = {(DataTypePtr)type, std::make_shared<DataTypeFloat64>()};
        DataTypes data_types2 = {(DataTypePtr)type, std::make_shared<DataTypeFloat64>(),
                                 std::make_shared<DataTypeFloat64>()};

        auto data_types = (offset == 0) ? data_types1 : data_types2;

        GTEST_LOG_(INFO) << "test_agg_linear_histogram for type"
                         << "(" << data_types[0]->get_name() << ")";

        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        auto agg_function = factory.get("linear_histogram", data_types, nullptr, false, -1,
                                        {.column_names = {""}});
        EXPECT_NE(agg_function, nullptr);

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        agg_linear_histogram_add_elements<DataType>(agg_function, place, input_rows, interval,
                                                    offset);

        ColumnString buf;
        VectorBufferWriter buf_writer(buf);
        agg_function->serialize(place, buf_writer);
        buf_writer.commit();
        VectorBufferReader buf_reader(buf.get_data_at(0));
        agg_function->deserialize(place, buf_reader, _agg_arena_pool);

        std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
        AggregateDataPtr place2 = memory2.get();
        agg_function->create(place2);
        agg_linear_histogram_add_elements<DataType>(agg_function, place2, input_rows, interval,
                                                    offset);
        agg_function->merge(place, place2, _agg_arena_pool);

        auto column_result1 = ColumnString::create();
        agg_function->insert_result_into(place, *column_result1);
        EXPECT_EQ(column_result1->size(), 1);
        EXPECT_TRUE(column_result1->get_offsets()[0] >= 1);

        auto column_result2 = ColumnString::create();
        agg_function->insert_result_into(place2, *column_result2);
        EXPECT_EQ(column_result2->size(), 1);
        EXPECT_TRUE(column_result2->get_offsets()[0] >= 1);

        std::string result1 = column_result1->get_data_at(0).to_string();
        std::string result2 = column_result2->get_data_at(0).to_string();

        expect_eq(input_rows, interval, offset, result1, result2);

        agg_function->destroy(place);
        agg_function->destroy(place2);
    }

    void expect_eq(size_t input_rows, double interval, double offset, std::string& result1,
                   std::string& result2) {
        // test empty data
        if (input_rows == 0) {
            std::string expect_empty_result = "{\"num_buckets\":0,\"buckets\":[]}";
            EXPECT_EQ(result1, expect_empty_result);
            EXPECT_EQ(result2, expect_empty_result);
        }

        // test with data
        if (input_rows == 100 && interval == 10 && offset == 0) {
            std::string expect_result1 = s1;
            std::string expect_result2 = s2;
            EXPECT_EQ(result1, expect_result1);
            EXPECT_EQ(result2, expect_result2);
        }

        if (input_rows == 100 && interval == 10 && offset == 5) {
            std::string expect_result1 = s3;
            std::string expect_result2 = s4;
            EXPECT_EQ(result1, expect_result1);
            EXPECT_EQ(result2, expect_result2);
        }

        if (input_rows == 5 && interval == 0.5 && offset == 0) {
            std::string expect_result1 = s5;
            std::string expect_result2 = s6;
            EXPECT_EQ(result1, expect_result1);
            EXPECT_EQ(result2, expect_result2);
        }

        if (input_rows == 5 && interval == 0.5 && offset == 0.25) {
            std::string expect_result1 = s7;
            std::string expect_result2 = s8;
            EXPECT_EQ(result1, expect_result1);
            EXPECT_EQ(result2, expect_result2);
        }
    }

    std::string run_float64_linear_histogram(const std::vector<double>& values, double interval,
                                             double offset = 0) {
        DataTypes data_types = {std::make_shared<DataTypeFloat64>(),
                                std::make_shared<DataTypeFloat64>()};
        if (offset != 0) {
            data_types.push_back(std::make_shared<DataTypeFloat64>());
        }

        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        auto agg_function = factory.get("linear_histogram", data_types, nullptr, false, -1,
                                        {.column_names = {""}});
        EXPECT_NE(agg_function, nullptr);

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        auto value_column = ColumnFloat64::create();
        auto interval_column = ColumnFloat64::create();
        auto offset_column = ColumnFloat64::create();
        for (double value : values) {
            value_column->insert_data(reinterpret_cast<const char*>(&value), 0);
            interval_column->insert_data(reinterpret_cast<const char*>(&interval), 0);
            if (offset != 0) {
                offset_column->insert_data(reinterpret_cast<const char*>(&offset), 0);
            }
        }

        if (offset != 0) {
            const IColumn* columns[3] = {value_column.get(), interval_column.get(),
                                         offset_column.get()};
            for (int row = 0; row < value_column->size(); ++row) {
                agg_function->add(place, columns, row, _agg_arena_pool);
            }
        } else {
            const IColumn* columns[2] = {value_column.get(), interval_column.get()};
            for (int row = 0; row < value_column->size(); ++row) {
                agg_function->add(place, columns, row, _agg_arena_pool);
            }
        }

        auto result_column = ColumnString::create();
        agg_function->insert_result_into(place, *result_column);
        std::string result = result_column->get_data_at(0).to_string();

        agg_function->destroy(place);
        return result;
    }

private:
    Arena _agg_arena_pool;
};

TEST_F(AggLinearHistogramTest, test_empty) {
    test_agg_linear_histogram<DataTypeInt8>(0, 10, 0);
    test_agg_linear_histogram<DataTypeInt16>(0, 10, 0);
    test_agg_linear_histogram<DataTypeInt32>(0, 10, 0);
    test_agg_linear_histogram<DataTypeInt64>(0, 10, 0);
    test_agg_linear_histogram<DataTypeInt128>(0, 10, 0);
    test_agg_linear_histogram<DataTypeFloat32>(0, 0.5, 0);
    test_agg_linear_histogram<DataTypeFloat64>(0, 0.5, 0);

    test_agg_linear_histogram<DataTypeDecimal32>(0, 0.5, 0);
    test_agg_linear_histogram<DataTypeDecimal64>(0, 0.5, 0);
    test_agg_linear_histogram<DataTypeDecimal128>(0, 0.5, 0);
    test_agg_linear_histogram<DataTypeDecimal256>(0, 0.5, 0);
}

TEST_F(AggLinearHistogramTest, test_with_data) {
    GTEST_LOG_(INFO) << "no offset";
    test_agg_linear_histogram<DataTypeInt8>(100, 10, 0);
    test_agg_linear_histogram<DataTypeInt16>(100, 10, 0);
    test_agg_linear_histogram<DataTypeInt32>(100, 10, 0);
    test_agg_linear_histogram<DataTypeInt64>(100, 10, 0);
    test_agg_linear_histogram<DataTypeInt128>(100, 10, 0);
    test_agg_linear_histogram<DataTypeFloat32>(5, 0.5, 0);
    test_agg_linear_histogram<DataTypeFloat64>(5, 0.5, 0);

    test_agg_linear_histogram<DataTypeDecimal32>(5, 0.5, 0);
    test_agg_linear_histogram<DataTypeDecimal64>(5, 0.5, 0);
    test_agg_linear_histogram<DataTypeDecimal128>(5, 0.5, 0);
    test_agg_linear_histogram<DataTypeDecimal256>(5, 0.5, 0);

    GTEST_LOG_(INFO) << "has offset";
    test_agg_linear_histogram<DataTypeInt8>(100, 10, 5);
    test_agg_linear_histogram<DataTypeInt16>(100, 10, 5);
    test_agg_linear_histogram<DataTypeInt32>(100, 10, 5);
    test_agg_linear_histogram<DataTypeInt64>(100, 10, 5);
    test_agg_linear_histogram<DataTypeInt128>(100, 10, 5);
    test_agg_linear_histogram<DataTypeFloat32>(5, 0.5, 0.25);
    test_agg_linear_histogram<DataTypeFloat64>(5, 0.5, 0.25);

    test_agg_linear_histogram<DataTypeDecimal32>(5, 0.5, 0.25);
    test_agg_linear_histogram<DataTypeDecimal64>(5, 0.5, 0.25);
    test_agg_linear_histogram<DataTypeDecimal128>(5, 0.5, 0.25);
    test_agg_linear_histogram<DataTypeDecimal256>(5, 0.5, 0.25);
}

TEST_F(AggLinearHistogramTest, test_decimal_interval_boundary) {
    EXPECT_EQ(run_float64_linear_histogram({0.1, 0.2, 0.3, 0.4}, 0.1),
              "{\"num_buckets\":4,\"buckets\":["
              "{\"lower\":0.1,\"upper\":0.2,\"count\":1,\"acc_count\":1},"
              "{\"lower\":0.2,\"upper\":0.3,\"count\":1,\"acc_count\":2},"
              "{\"lower\":0.3,\"upper\":0.4,\"count\":1,\"acc_count\":3},"
              "{\"lower\":0.4,\"upper\":0.5,\"count\":1,\"acc_count\":4}"
              "]}");
}

TEST_F(AggLinearHistogramTest, test_decimal_interval_raw_boundary_snaps) {
    EXPECT_EQ(run_float64_linear_histogram({0.3, 0.1 + 0.2}, 0.1),
              "{\"num_buckets\":1,\"buckets\":["
              "{\"lower\":0.3,\"upper\":0.4,\"count\":2,\"acc_count\":2}"
              "]}");
}

TEST_F(AggLinearHistogramTest, test_decimal_interval_does_not_snap_previous_value) {
    const double value_before_boundary = std::nextafter(0.3, 0.0);
    EXPECT_EQ(run_float64_linear_histogram({value_before_boundary, 0.3}, 0.1),
              "{\"num_buckets\":2,\"buckets\":["
              "{\"lower\":0.2,\"upper\":0.3,\"count\":1,\"acc_count\":1},"
              "{\"lower\":0.3,\"upper\":0.4,\"count\":1,\"acc_count\":2}"
              "]}");
}

TEST_F(AggLinearHistogramTest, test_near_decimal_interval_does_not_snap_boundary_value) {
    EXPECT_EQ(run_float64_linear_histogram({0.1}, 0.10000000000000002),
              "{\"num_buckets\":1,\"buckets\":["
              "{\"lower\":0.0,\"upper\":0.10000000000000002,\"count\":1,\"acc_count\":1}"
              "]}");
}

TEST_F(AggLinearHistogramTest, test_decimal_interval_boundary_with_offset) {
    EXPECT_EQ(run_float64_linear_histogram({0.15, 0.25, 0.35}, 0.1, 0.05),
              "{\"num_buckets\":3,\"buckets\":["
              "{\"lower\":0.15,\"upper\":0.25,\"count\":1,\"acc_count\":1},"
              "{\"lower\":0.25,\"upper\":0.35,\"count\":1,\"acc_count\":2},"
              "{\"lower\":0.35,\"upper\":0.45,\"count\":1,\"acc_count\":3}"
              "]}");
}

TEST_F(AggLinearHistogramTest, test_negative_decimal_interval_boundary) {
    EXPECT_EQ(run_float64_linear_histogram({-0.3, -0.2, -0.1}, 0.1),
              "{\"num_buckets\":3,\"buckets\":["
              "{\"lower\":-0.3,\"upper\":-0.2,\"count\":1,\"acc_count\":1},"
              "{\"lower\":-0.2,\"upper\":-0.1,\"count\":1,\"acc_count\":2},"
              "{\"lower\":-0.1,\"upper\":0.0,\"count\":1,\"acc_count\":3}"
              "]}");
}

TEST_F(AggLinearHistogramTest, test_negative_decimal_interval_does_not_snap_previous_value) {
    const double value_before_boundary =
            std::nextafter(-0.3, -std::numeric_limits<double>::infinity());
    EXPECT_EQ(run_float64_linear_histogram({value_before_boundary, -0.3}, 0.1),
              "{\"num_buckets\":2,\"buckets\":["
              "{\"lower\":-0.4,\"upper\":-0.3,\"count\":1,\"acc_count\":1},"
              "{\"lower\":-0.3,\"upper\":-0.2,\"count\":1,\"acc_count\":2}"
              "]}");
}

TEST_F(AggLinearHistogramTest, test_exact_integer_interval_does_not_snap_previous_value) {
    const double value_before_boundary = std::nextafter(1.0, 0.0);
    EXPECT_EQ(run_float64_linear_histogram({value_before_boundary, 1.0}, 1.0),
              "{\"num_buckets\":2,\"buckets\":["
              "{\"lower\":0.0,\"upper\":1.0,\"count\":1,\"acc_count\":1},"
              "{\"lower\":1.0,\"upper\":2.0,\"count\":1,\"acc_count\":2}"
              "]}");
}

TEST_F(AggLinearHistogramTest, test_large_value_near_boundary_does_not_snap) {
    EXPECT_EQ(run_float64_linear_histogram({999999999.999999, 1000000000.0}, 1.0),
              "{\"num_buckets\":2,\"buckets\":["
              "{\"lower\":999999999.0,\"upper\":1000000000.0,\"count\":1,\"acc_count\":1},"
              "{\"lower\":1000000000.0,\"upper\":1000000001.0,\"count\":1,\"acc_count\":2}"
              "]}");
}

TEST_F(AggLinearHistogramTest, test_large_value_near_bucket_limit_does_not_snap) {
    EXPECT_EQ(run_float64_linear_histogram({2147483646.999999}, 1.0),
              "{\"num_buckets\":1,\"buckets\":["
              "{\"lower\":2147483646.0,\"upper\":2147483647.0,\"count\":1,\"acc_count\":1}"
              "]}");
}

TEST_F(AggLinearHistogramTest, test_small_nonzero_interval_keeps_raw_boundaries) {
    EXPECT_EQ(run_float64_linear_histogram({1e-16, 2e-16}, 1e-16),
              "{\"num_buckets\":2,\"buckets\":["
              "{\"lower\":1e-16,\"upper\":2e-16,\"count\":1,\"acc_count\":1},"
              "{\"lower\":2e-16,\"upper\":3e-16,\"count\":1,\"acc_count\":2}"
              "]}");
}

} // namespace doris
