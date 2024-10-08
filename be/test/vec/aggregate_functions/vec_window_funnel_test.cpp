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

#include <memory>
#include <ostream>

#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

void register_aggregate_function_window_funnel(AggregateFunctionSimpleFactory& factory);

class VWindowFunnelTest : public testing::Test {
public:
    AggregateFunctionPtr agg_function;

    VWindowFunnelTest() {}

    void SetUp() {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        DataTypes data_types = {
                std::make_shared<DataTypeInt64>(),    std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeUInt8>(),
                std::make_shared<DataTypeUInt8>(),    std::make_shared<DataTypeUInt8>(),
                std::make_shared<DataTypeUInt8>()};
        agg_function = factory.get("window_funnel", data_types, false, -1);
        EXPECT_NE(agg_function, nullptr);
    }

    void TearDown() {}
};

TEST_F(VWindowFunnelTest, testEmpty) {
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    ColumnString buf;
    VectorBufferWriter buf_writer(buf);
    agg_function->serialize(place, buf_writer);
    buf_writer.commit();
    LOG(INFO) << "buf size : " << buf.size();
    VectorBufferReader buf_reader(buf.get_data_at(0));
    agg_function->deserialize(place, buf_reader, nullptr);

    std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
    AggregateDataPtr place2 = memory2.get();
    agg_function->create(place2);

    agg_function->merge(place, place2, nullptr);
    ColumnVector<Int32> column_result;
    agg_function->insert_result_into(place, column_result);
    EXPECT_EQ(column_result.get_data()[0], 0);

    ColumnVector<Int32> column_result2;
    agg_function->insert_result_into(place2, column_result2);
    EXPECT_EQ(column_result2.get_data()[0], 0);

    agg_function->destroy(place);
    agg_function->destroy(place2);
}

TEST_F(VWindowFunnelTest, testSerialize) {
    const int NUM_CONDS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_mode->insert("mode");
    }

    auto column_timestamp = ColumnVector<Int64>::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 2, 28, 0, 0, i);
        column_timestamp->insert_data((char*)&time_value, 0);
    }
    auto column_event1 = ColumnVector<UInt8>::create();
    column_event1->insert(1);
    column_event1->insert(0);
    column_event1->insert(0);
    column_event1->insert(0);

    auto column_event2 = ColumnVector<UInt8>::create();
    column_event2->insert(0);
    column_event2->insert(1);
    column_event2->insert(0);
    column_event2->insert(0);

    auto column_event3 = ColumnVector<UInt8>::create();
    column_event3->insert(0);
    column_event3->insert(0);
    column_event3->insert(1);
    column_event3->insert(0);

    auto column_event4 = ColumnVector<UInt8>::create();
    column_event4->insert(0);
    column_event4->insert(0);
    column_event4->insert(0);
    column_event4->insert(1);

    auto column_window = ColumnVector<Int64>::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_window->insert(2);
    }

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    const IColumn* column[7] = {column_window.get(), column_mode.get(),   column_timestamp.get(),
                                column_event1.get(), column_event2.get(), column_event3.get(),
                                column_event4.get()};
    for (int i = 0; i < NUM_CONDS; i++) {
        agg_function->add(place, column, i, nullptr);
    }

    ColumnVector<Int32> column_result;
    agg_function->insert_result_into(place, column_result);
    EXPECT_EQ(column_result.get_data()[0], 3);

    ColumnString buf;
    VectorBufferWriter buf_writer(buf);
    agg_function->serialize(place, buf_writer);
    buf_writer.commit();
    agg_function->destroy(place);

    std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
    AggregateDataPtr place2 = memory2.get();
    agg_function->create(place2);

    VectorBufferReader buf_reader(buf.get_data_at(0));
    agg_function->deserialize(place2, buf_reader, nullptr);

    ColumnVector<Int32> column_result2;
    agg_function->insert_result_into(place2, column_result2);
    EXPECT_EQ(column_result2.get_data()[0], 3);
    agg_function->destroy(place2);
}

TEST_F(VWindowFunnelTest, testMax4SortedNoMerge) {
    const int NUM_CONDS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_mode->insert("mode");
    }
    auto column_timestamp = ColumnVector<Int64>::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 2, 28, 0, 0, i);
        column_timestamp->insert_data((char*)&time_value, 0);
    }
    auto column_event1 = ColumnVector<UInt8>::create();
    column_event1->insert(1);
    column_event1->insert(0);
    column_event1->insert(0);
    column_event1->insert(0);

    auto column_event2 = ColumnVector<UInt8>::create();
    column_event2->insert(0);
    column_event2->insert(1);
    column_event2->insert(0);
    column_event2->insert(0);

    auto column_event3 = ColumnVector<UInt8>::create();
    column_event3->insert(0);
    column_event3->insert(0);
    column_event3->insert(1);
    column_event3->insert(0);

    auto column_event4 = ColumnVector<UInt8>::create();
    column_event4->insert(0);
    column_event4->insert(0);
    column_event4->insert(0);
    column_event4->insert(1);

    for (int win = -1; win < NUM_CONDS + 1; win++) {
        auto column_window = ColumnVector<Int64>::create();
        for (int i = 0; i < NUM_CONDS; i++) {
            column_window->insert(win);
        }

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        const IColumn* column[7] = {column_window.get(),    column_mode.get(),
                                    column_timestamp.get(), column_event1.get(),
                                    column_event2.get(),    column_event3.get(),
                                    column_event4.get()};
        for (int i = 0; i < NUM_CONDS; i++) {
            agg_function->add(place, column, i, nullptr);
        }

        ColumnVector<Int32> column_result;
        agg_function->insert_result_into(place, column_result);
        EXPECT_EQ(column_result.get_data()[0],
                  win < 0 ? 1 : (win < NUM_CONDS ? win + 1 : NUM_CONDS));
        agg_function->destroy(place);
    }
}

TEST_F(VWindowFunnelTest, testMax4SortedMerge) {
    const int NUM_CONDS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_mode->insert("mode");
    }
    auto column_timestamp = ColumnVector<Int64>::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 2, 28, 0, 0, i);
        column_timestamp->insert_data((char*)&time_value, 0);
    }
    auto column_event1 = ColumnVector<UInt8>::create();
    column_event1->insert(1);
    column_event1->insert(0);
    column_event1->insert(0);
    column_event1->insert(0);

    auto column_event2 = ColumnVector<UInt8>::create();
    column_event2->insert(0);
    column_event2->insert(1);
    column_event2->insert(0);
    column_event2->insert(0);

    auto column_event3 = ColumnVector<UInt8>::create();
    column_event3->insert(0);
    column_event3->insert(0);
    column_event3->insert(1);
    column_event3->insert(0);

    auto column_event4 = ColumnVector<UInt8>::create();
    column_event4->insert(0);
    column_event4->insert(0);
    column_event4->insert(0);
    column_event4->insert(1);

    for (int win = -1; win < NUM_CONDS + 1; win++) {
        auto column_window = ColumnVector<Int64>::create();
        for (int i = 0; i < NUM_CONDS; i++) {
            column_window->insert(win);
        }

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        const IColumn* column[7] = {column_window.get(),    column_mode.get(),
                                    column_timestamp.get(), column_event1.get(),
                                    column_event2.get(),    column_event3.get(),
                                    column_event4.get()};
        for (int i = 0; i < NUM_CONDS; i++) {
            agg_function->add(place, column, i, nullptr);
        }

        std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
        AggregateDataPtr place2 = memory2.get();
        agg_function->create(place2);

        agg_function->merge(place2, place, nullptr);
        ColumnVector<Int32> column_result;
        agg_function->insert_result_into(place2, column_result);
        EXPECT_EQ(column_result.get_data()[0],
                  win < 0 ? 1 : (win < NUM_CONDS ? win + 1 : NUM_CONDS));
        agg_function->destroy(place);
        agg_function->destroy(place2);
    }
}

TEST_F(VWindowFunnelTest, testMax4ReverseSortedNoMerge) {
    const int NUM_CONDS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_mode->insert("mode");
    }
    auto column_timestamp = ColumnVector<Int64>::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 2, 28, 0, 0, NUM_CONDS - i);
        column_timestamp->insert_data((char*)&time_value, 0);
    }
    auto column_event1 = ColumnVector<UInt8>::create();
    column_event1->insert(0);
    column_event1->insert(0);
    column_event1->insert(0);
    column_event1->insert(1);

    auto column_event2 = ColumnVector<UInt8>::create();
    column_event2->insert(0);
    column_event2->insert(0);
    column_event2->insert(1);
    column_event2->insert(0);

    auto column_event3 = ColumnVector<UInt8>::create();
    column_event3->insert(0);
    column_event3->insert(1);
    column_event3->insert(0);
    column_event3->insert(0);

    auto column_event4 = ColumnVector<UInt8>::create();
    column_event4->insert(1);
    column_event4->insert(0);
    column_event4->insert(0);
    column_event4->insert(0);

    for (int win = -1; win < NUM_CONDS + 1; win++) {
        auto column_window = ColumnVector<Int64>::create();
        for (int i = 0; i < NUM_CONDS; i++) {
            column_window->insert(win);
        }

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        const IColumn* column[7] = {column_window.get(),    column_mode.get(),
                                    column_timestamp.get(), column_event1.get(),
                                    column_event2.get(),    column_event3.get(),
                                    column_event4.get()};
        for (int i = 0; i < NUM_CONDS; i++) {
            agg_function->add(place, column, i, nullptr);
        }

        LOG(INFO) << "win " << win;
        ColumnVector<Int32> column_result;
        agg_function->insert_result_into(place, column_result);
        EXPECT_EQ(column_result.get_data()[0],
                  win < 0 ? 1 : (win < NUM_CONDS ? win + 1 : NUM_CONDS));
        agg_function->destroy(place);
    }
}

TEST_F(VWindowFunnelTest, testMax4ReverseSortedMerge) {
    const int NUM_CONDS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_mode->insert("mode");
    }
    auto column_timestamp = ColumnVector<Int64>::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 2, 28, 0, 0, NUM_CONDS - i);
        column_timestamp->insert_data((char*)&time_value, 0);
    }
    auto column_event1 = ColumnVector<UInt8>::create();
    column_event1->insert(0);
    column_event1->insert(0);
    column_event1->insert(0);
    column_event1->insert(1);

    auto column_event2 = ColumnVector<UInt8>::create();
    column_event2->insert(0);
    column_event2->insert(0);
    column_event2->insert(1);
    column_event2->insert(0);

    auto column_event3 = ColumnVector<UInt8>::create();
    column_event3->insert(0);
    column_event3->insert(1);
    column_event3->insert(0);
    column_event3->insert(0);

    auto column_event4 = ColumnVector<UInt8>::create();
    column_event4->insert(1);
    column_event4->insert(0);
    column_event4->insert(0);
    column_event4->insert(0);

    for (int win = -1; win < NUM_CONDS + 1; win++) {
        auto column_window = ColumnVector<Int64>::create();
        for (int i = 0; i < NUM_CONDS; i++) {
            column_window->insert(win);
        }

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        const IColumn* column[7] = {column_window.get(),    column_mode.get(),
                                    column_timestamp.get(), column_event1.get(),
                                    column_event2.get(),    column_event3.get(),
                                    column_event4.get()};
        for (int i = 0; i < NUM_CONDS; i++) {
            agg_function->add(place, column, i, nullptr);
        }

        std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
        AggregateDataPtr place2 = memory2.get();
        agg_function->create(place2);

        agg_function->merge(place2, place, NULL);
        ColumnVector<Int32> column_result;
        agg_function->insert_result_into(place2, column_result);
        EXPECT_EQ(column_result.get_data()[0],
                  win < 0 ? 1 : (win < NUM_CONDS ? win + 1 : NUM_CONDS));
        agg_function->destroy(place);
        agg_function->destroy(place2);
    }
}

} // namespace doris::vectorized
