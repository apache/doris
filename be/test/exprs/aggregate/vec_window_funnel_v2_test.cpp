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

#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"
#include "core/value/vdatetime_value.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {
class IColumn;
} // namespace doris

namespace doris {

void register_aggregate_function_window_funnel_v2(AggregateFunctionSimpleFactory& factory);

class VWindowFunnelV2Test : public testing::Test {
public:
    AggregateFunctionPtr agg_function;

    VWindowFunnelV2Test() {}

    void SetUp() {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        DataTypes data_types = {
                std::make_shared<DataTypeInt64>(),      std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeUInt8>(),
                std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>(),
                std::make_shared<DataTypeUInt8>()};
        agg_function = factory.get("window_funnel_v2", data_types, nullptr, false,
                                   BeExecVersionManager::get_newest_version());
        EXPECT_NE(agg_function, nullptr);
    }

    void TearDown() {}

    Arena arena;
};

TEST_F(VWindowFunnelV2Test, testEmpty) {
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    ColumnString buf;
    VectorBufferWriter buf_writer(buf);
    agg_function->serialize(place, buf_writer);
    buf_writer.commit();
    LOG(INFO) << "buf size : " << buf.size();
    VectorBufferReader buf_reader(buf.get_data_at(0));
    agg_function->deserialize(place, buf_reader, arena);

    std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
    AggregateDataPtr place2 = memory2.get();
    agg_function->create(place2);

    agg_function->merge(place, place2, arena);
    ColumnInt32 column_result;
    agg_function->insert_result_into(place, column_result);
    EXPECT_EQ(column_result.get_data()[0], 0);

    ColumnInt32 column_result2;
    agg_function->insert_result_into(place2, column_result2);
    EXPECT_EQ(column_result2.get_data()[0], 0);

    agg_function->destroy(place);
    agg_function->destroy(place2);
}

TEST_F(VWindowFunnelV2Test, testSerialize) {
    const int NUM_CONDS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("default"));
    }

    auto column_timestamp = ColumnDateTimeV2::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 2, 28, 0, 0, i);
        auto dtv2 = time_value.to_datetime_v2();
        column_timestamp->insert_data((char*)&dtv2, 0);
    }
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event4 = ColumnUInt8::create();
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(2));
    }

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    const IColumn* column[7] = {column_window.get(), column_mode.get(),   column_timestamp.get(),
                                column_event1.get(), column_event2.get(), column_event3.get(),
                                column_event4.get()};
    for (int i = 0; i < NUM_CONDS; i++) {
        agg_function->add(place, column, i, arena);
    }

    ColumnInt32 column_result;
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
    agg_function->deserialize(place2, buf_reader, arena);

    ColumnInt32 column_result2;
    agg_function->insert_result_into(place2, column_result2);
    EXPECT_EQ(column_result2.get_data()[0], 3);
    agg_function->destroy(place2);
}

TEST_F(VWindowFunnelV2Test, testDefaultSortedNoMerge) {
    const int NUM_CONDS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("default"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 2, 28, 0, 0, i);
        auto dtv2 = time_value.to_datetime_v2();
        column_timestamp->insert_data((char*)&dtv2, 0);
    }
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event4 = ColumnUInt8::create();
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(1));

    for (int win = 0; win < NUM_CONDS + 1; win++) {
        auto column_window = ColumnInt64::create();
        for (int i = 0; i < NUM_CONDS; i++) {
            column_window->insert(Field::create_field<TYPE_BIGINT>(win));
        }

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        const IColumn* column[7] = {column_window.get(),    column_mode.get(),
                                    column_timestamp.get(), column_event1.get(),
                                    column_event2.get(),    column_event3.get(),
                                    column_event4.get()};
        for (int i = 0; i < NUM_CONDS; i++) {
            agg_function->add(place, column, i, arena);
        }

        ColumnInt32 column_result;
        agg_function->insert_result_into(place, column_result);
        EXPECT_EQ(column_result.get_data()[0],
                  win < 0 ? 1 : (win < NUM_CONDS ? win + 1 : NUM_CONDS));
        agg_function->destroy(place);
    }
}

TEST_F(VWindowFunnelV2Test, testDefaultSortedMerge) {
    const int NUM_CONDS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("default"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 2, 28, 0, 0, i);
        auto dtv2 = time_value.to_datetime_v2();
        column_timestamp->insert_data((char*)&dtv2, 0);
    }
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event4 = ColumnUInt8::create();
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(1));

    for (int win = 0; win < NUM_CONDS + 1; win++) {
        auto column_window = ColumnInt64::create();
        for (int i = 0; i < NUM_CONDS; i++) {
            column_window->insert(Field::create_field<TYPE_BIGINT>(win));
        }

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        const IColumn* column[7] = {column_window.get(),    column_mode.get(),
                                    column_timestamp.get(), column_event1.get(),
                                    column_event2.get(),    column_event3.get(),
                                    column_event4.get()};
        for (int i = 0; i < NUM_CONDS; i++) {
            agg_function->add(place, column, i, arena);
        }

        std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
        AggregateDataPtr place2 = memory2.get();
        agg_function->create(place2);

        agg_function->merge(place2, place, arena);
        ColumnInt32 column_result;
        agg_function->insert_result_into(place2, column_result);
        EXPECT_EQ(column_result.get_data()[0],
                  win < 0 ? 1 : (win < NUM_CONDS ? win + 1 : NUM_CONDS));
        agg_function->destroy(place);
        agg_function->destroy(place2);
    }
}

TEST_F(VWindowFunnelV2Test, testDefaultReverseSortedNoMerge) {
    const int NUM_CONDS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("default"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 2, 28, 0, 0, NUM_CONDS - i);
        auto dtv2 = time_value.to_datetime_v2();
        column_timestamp->insert_data((char*)&dtv2, 0);
    }
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event4 = ColumnUInt8::create();
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));

    for (int win = 0; win < NUM_CONDS + 1; win++) {
        auto column_window = ColumnInt64::create();
        for (int i = 0; i < NUM_CONDS; i++) {
            column_window->insert(Field::create_field<TYPE_BIGINT>(win));
        }

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        const IColumn* column[7] = {column_window.get(),    column_mode.get(),
                                    column_timestamp.get(), column_event1.get(),
                                    column_event2.get(),    column_event3.get(),
                                    column_event4.get()};
        for (int i = 0; i < NUM_CONDS; i++) {
            agg_function->add(place, column, i, arena);
        }

        LOG(INFO) << "win " << win;
        ColumnInt32 column_result;
        agg_function->insert_result_into(place, column_result);
        EXPECT_EQ(column_result.get_data()[0],
                  win < 0 ? 1 : (win < NUM_CONDS ? win + 1 : NUM_CONDS));
        agg_function->destroy(place);
    }
}

TEST_F(VWindowFunnelV2Test, testDefaultReverseSortedMerge) {
    const int NUM_CONDS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("default"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    for (int i = 0; i < NUM_CONDS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 2, 28, 0, 0, NUM_CONDS - i);
        auto dtv2 = time_value.to_datetime_v2();
        column_timestamp->insert_data((char*)&dtv2, 0);
    }
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event4 = ColumnUInt8::create();
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));

    for (int win = 0; win < NUM_CONDS + 1; win++) {
        auto column_window = ColumnInt64::create();
        for (int i = 0; i < NUM_CONDS; i++) {
            column_window->insert(Field::create_field<TYPE_BIGINT>(win));
        }

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        const IColumn* column[7] = {column_window.get(),    column_mode.get(),
                                    column_timestamp.get(), column_event1.get(),
                                    column_event2.get(),    column_event3.get(),
                                    column_event4.get()};
        for (int i = 0; i < NUM_CONDS; i++) {
            agg_function->add(place, column, i, arena);
        }

        std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
        AggregateDataPtr place2 = memory2.get();
        agg_function->create(place2);

        agg_function->merge(place2, place, arena);
        ColumnInt32 column_result;
        agg_function->insert_result_into(place2, column_result);
        EXPECT_EQ(column_result.get_data()[0],
                  win < 0 ? 1 : (win < NUM_CONDS ? win + 1 : NUM_CONDS));
        agg_function->destroy(place);
        agg_function->destroy(place2);
    }
}

// Test that V2 only stores matched events (unmatched rows are not stored)
// This verifies the core memory optimization.
TEST_F(VWindowFunnelV2Test, testOnlyMatchedEventsStored) {
    const int NUM_ROWS = 6;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("default"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        VecDateTimeValue time_value;
        time_value.unchecked_set_time(2022, 2, 28, 0, 0, i);
        auto dtv2 = time_value.to_datetime_v2();
        column_timestamp->insert_data((char*)&dtv2, 0);
    }
    // 4 events, but rows 2 and 4 (0-indexed) match nothing
    // Row 0: event1=true
    // Row 1: event2=true
    // Row 2: all false (no match)
    // Row 3: event3=true
    // Row 4: all false (no match)
    // Row 5: event4=true
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event4 = ColumnUInt8::create();
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(10));
    }

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    const IColumn* column[7] = {column_window.get(), column_mode.get(),   column_timestamp.get(),
                                column_event1.get(), column_event2.get(), column_event3.get(),
                                column_event4.get()};
    for (int i = 0; i < NUM_ROWS; i++) {
        agg_function->add(place, column, i, arena);
    }

    ColumnInt32 column_result;
    agg_function->insert_result_into(place, column_result);
    // All 4 events matched in order within the window
    EXPECT_EQ(column_result.get_data()[0], 4);
    agg_function->destroy(place);
}

// Test INCREASE mode: timestamps must be strictly increasing
TEST_F(VWindowFunnelV2Test, testIncreaseMode) {
    const int NUM_ROWS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("increase"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    // Events 2 and 3 have the same timestamp
    VecDateTimeValue tv0, tv1, tv2, tv3;
    tv0.unchecked_set_time(2022, 2, 28, 0, 0, 0);
    tv1.unchecked_set_time(2022, 2, 28, 0, 0, 1);
    tv2.unchecked_set_time(2022, 2, 28, 0, 0, 1); // same as tv1
    tv3.unchecked_set_time(2022, 2, 28, 0, 0, 3);
    auto dtv2_0 = tv0.to_datetime_v2();
    auto dtv2_1 = tv1.to_datetime_v2();
    auto dtv2_2 = tv2.to_datetime_v2();
    auto dtv2_3 = tv3.to_datetime_v2();
    column_timestamp->insert_data((char*)&dtv2_0, 0);
    column_timestamp->insert_data((char*)&dtv2_1, 0);
    column_timestamp->insert_data((char*)&dtv2_2, 0);
    column_timestamp->insert_data((char*)&dtv2_3, 0);

    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event4 = ColumnUInt8::create();
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(10));
    }

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    const IColumn* column[7] = {column_window.get(), column_mode.get(),   column_timestamp.get(),
                                column_event1.get(), column_event2.get(), column_event3.get(),
                                column_event4.get()};
    for (int i = 0; i < NUM_ROWS; i++) {
        agg_function->add(place, column, i, arena);
    }

    ColumnInt32 column_result;
    agg_function->insert_result_into(place, column_result);
    // Event 2 and 3 have same timestamp, so increase mode breaks at event 3
    // Chain: event1(t=0) -> event2(t=1), event3 has same ts as event2, so fails
    // Result: 2
    EXPECT_EQ(column_result.get_data()[0], 2);
    agg_function->destroy(place);
}

// Test DEDUPLICATION mode: duplicate events break the chain
TEST_F(VWindowFunnelV2Test, testDeduplicationMode) {
    const int NUM_ROWS = 5;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("deduplication"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    VecDateTimeValue tv0, tv1, tv2, tv3, tv4;
    tv0.unchecked_set_time(2022, 2, 28, 0, 0, 0);
    tv1.unchecked_set_time(2022, 2, 28, 0, 0, 1);
    tv2.unchecked_set_time(2022, 2, 28, 0, 0, 2);
    tv3.unchecked_set_time(2022, 2, 28, 0, 0, 3);
    tv4.unchecked_set_time(2022, 2, 28, 0, 0, 4);
    auto dtv2_0 = tv0.to_datetime_v2();
    auto dtv2_1 = tv1.to_datetime_v2();
    auto dtv2_2 = tv2.to_datetime_v2();
    auto dtv2_3 = tv3.to_datetime_v2();
    auto dtv2_4 = tv4.to_datetime_v2();
    column_timestamp->insert_data((char*)&dtv2_0, 0);
    column_timestamp->insert_data((char*)&dtv2_1, 0);
    column_timestamp->insert_data((char*)&dtv2_2, 0);
    column_timestamp->insert_data((char*)&dtv2_3, 0);
    column_timestamp->insert_data((char*)&dtv2_4, 0);

    // Events: event1, event2, event1(dup!), event3, event4
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1)); // duplicate
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event4 = ColumnUInt8::create();
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(10));
    }

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    const IColumn* column[7] = {column_window.get(), column_mode.get(),   column_timestamp.get(),
                                column_event1.get(), column_event2.get(), column_event3.get(),
                                column_event4.get()};
    for (int i = 0; i < NUM_ROWS; i++) {
        agg_function->add(place, column, i, arena);
    }

    ColumnInt32 column_result;
    agg_function->insert_result_into(place, column_result);
    // Chain: event1(t=0) -> event2(t=1), then event1 dup at t=2 breaks chain (max_level=2)
    // New chain: event1(t=2) -> event3(t=3) -> event4(t=4): level=3 but event2 not matched
    // Actually event1(t=2) starts new chain, event3(t=3) needs event2 first, so new chain = 1
    // max(2, 1) = 2
    EXPECT_EQ(column_result.get_data()[0], 2);
    agg_function->destroy(place);
}

// Test FIXED mode (StarRocks-style): event level must not jump
TEST_F(VWindowFunnelV2Test, testFixedMode) {
    const int NUM_ROWS = 5;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("fixed"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    VecDateTimeValue tv0, tv1, tv2, tv3, tv4;
    tv0.unchecked_set_time(2022, 2, 28, 0, 0, 0);
    tv1.unchecked_set_time(2022, 2, 28, 0, 0, 1);
    tv2.unchecked_set_time(2022, 2, 28, 0, 0, 2);
    tv3.unchecked_set_time(2022, 2, 28, 0, 0, 3);
    tv4.unchecked_set_time(2022, 2, 28, 0, 0, 4);
    auto dtv2_0 = tv0.to_datetime_v2();
    auto dtv2_1 = tv1.to_datetime_v2();
    auto dtv2_2 = tv2.to_datetime_v2();
    auto dtv2_3 = tv3.to_datetime_v2();
    auto dtv2_4 = tv4.to_datetime_v2();
    column_timestamp->insert_data((char*)&dtv2_0, 0);
    column_timestamp->insert_data((char*)&dtv2_1, 0);
    column_timestamp->insert_data((char*)&dtv2_2, 0);
    column_timestamp->insert_data((char*)&dtv2_3, 0);
    column_timestamp->insert_data((char*)&dtv2_4, 0);

    // Events: event1, event2, event4(jump! skips event3), event3, event4
    // In V2 fixed mode (StarRocks-style), event4 at t=2 has no predecessor (event3 not matched),
    // so the chain breaks.
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event4 = ColumnUInt8::create();
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(1)); // jump
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(10));
    }

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    const IColumn* column[7] = {column_window.get(), column_mode.get(),   column_timestamp.get(),
                                column_event1.get(), column_event2.get(), column_event3.get(),
                                column_event4.get()};
    for (int i = 0; i < NUM_ROWS; i++) {
        agg_function->add(place, column, i, arena);
    }

    ColumnInt32 column_result;
    agg_function->insert_result_into(place, column_result);
    // Chain: event1(t=0) -> event2(t=1), then event4(t=2) jumps (no event3 predecessor),
    // chain breaks, max_level=2.
    // No further complete chain starts since event3 and event4 happen after.
    EXPECT_EQ(column_result.get_data()[0], 2);
    agg_function->destroy(place);
}

// Test that same-row multi-condition matching does NOT advance the chain
// through multiple levels. This tests the continuation flag logic.
// Scenario: window_funnel(86400, 'default', ts, xwhat=1, xwhat!=2, xwhat=3)
// Row 0 (xwhat=1): matches cond0=T (xwhat=1), cond1=T (1!=2), cond2=F → 2 conditions on same row
// Row 1 (xwhat=2): matches nothing (cond1: 2!=2=F)
// Row 2 (xwhat=3): matches cond1=T (3!=2), cond2=T (xwhat=3) → 2 conditions on same row
// Correct result: 2 (cond0 from row0, cond1 from row2). NOT 3.
// Without the continuation flag, row0 would advance through both cond0 and cond1,
// then row2 would match cond2 → result 3 (wrong).
TEST_F(VWindowFunnelV2Test, testSameRowMultiConditionDefault) {
    // 3 conditions (instead of 4), so we need a different setup
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    DataTypes data_types_3 = {
            std::make_shared<DataTypeInt64>(),      std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>()};
    auto agg_func_3 = factory.get("window_funnel_v2", data_types_3, nullptr, false,
                                  BeExecVersionManager::get_newest_version());
    ASSERT_NE(agg_func_3, nullptr);

    const int NUM_ROWS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("default"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    // Row 0: ts=10:41:00  (xwhat=1)
    // Row 1: ts=13:28:02  (xwhat=2)
    // Row 2: ts=16:15:01  (xwhat=3)
    // Row 3: ts=19:05:04  (xwhat=4)
    VecDateTimeValue tv0, tv1, tv2, tv3;
    tv0.unchecked_set_time(2022, 3, 12, 10, 41, 0);
    tv1.unchecked_set_time(2022, 3, 12, 13, 28, 2);
    tv2.unchecked_set_time(2022, 3, 12, 16, 15, 1);
    tv3.unchecked_set_time(2022, 3, 12, 19, 5, 4);
    auto dtv2_0 = tv0.to_datetime_v2();
    auto dtv2_1 = tv1.to_datetime_v2();
    auto dtv2_2 = tv2.to_datetime_v2();
    auto dtv2_3 = tv3.to_datetime_v2();
    column_timestamp->insert_data((char*)&dtv2_0, 0);
    column_timestamp->insert_data((char*)&dtv2_1, 0);
    column_timestamp->insert_data((char*)&dtv2_2, 0);
    column_timestamp->insert_data((char*)&dtv2_3, 0);

    // cond0: xwhat=1 (only row0 matches)
    auto column_cond0 = ColumnUInt8::create();
    column_cond0->insert(Field::create_field<TYPE_BOOLEAN>(1)); // row0: xwhat=1 → T
    column_cond0->insert(Field::create_field<TYPE_BOOLEAN>(0)); // row1: xwhat=2 → F
    column_cond0->insert(Field::create_field<TYPE_BOOLEAN>(0)); // row2: xwhat=3 → F
    column_cond0->insert(Field::create_field<TYPE_BOOLEAN>(0)); // row3: xwhat=4 → F

    // cond1: xwhat!=2 (rows 0,2,3 match)
    auto column_cond1 = ColumnUInt8::create();
    column_cond1->insert(Field::create_field<TYPE_BOOLEAN>(1)); // row0: 1!=2 → T
    column_cond1->insert(Field::create_field<TYPE_BOOLEAN>(0)); // row1: 2!=2 → F
    column_cond1->insert(Field::create_field<TYPE_BOOLEAN>(1)); // row2: 3!=2 → T
    column_cond1->insert(Field::create_field<TYPE_BOOLEAN>(1)); // row3: 4!=2 → T

    // cond2: xwhat=3 (only row2 matches)
    auto column_cond2 = ColumnUInt8::create();
    column_cond2->insert(Field::create_field<TYPE_BOOLEAN>(0)); // row0: F
    column_cond2->insert(Field::create_field<TYPE_BOOLEAN>(0)); // row1: F
    column_cond2->insert(Field::create_field<TYPE_BOOLEAN>(1)); // row2: T
    column_cond2->insert(Field::create_field<TYPE_BOOLEAN>(0)); // row3: F

    // window = 86400 seconds (24 hours)
    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(86400));
    }

    std::unique_ptr<char[]> memory(new char[agg_func_3->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_func_3->create(place);
    const IColumn* column[6] = {column_window.get(), column_mode.get(),  column_timestamp.get(),
                                column_cond0.get(),  column_cond1.get(), column_cond2.get()};
    for (int i = 0; i < NUM_ROWS; i++) {
        agg_func_3->add(place, column, i, arena);
    }

    ColumnInt32 column_result;
    agg_func_3->insert_result_into(place, column_result);
    // Without continuation flag: row0 matches cond0+cond1 (same row advances both),
    // row2 matches cond2 → result=3 (WRONG)
    // With continuation flag: row0 sets cond0 only (cond1 is same-row continuation),
    // row2's cond1 extends chain (different row), but cond2 from row2 is same-row → stops at 2
    EXPECT_EQ(column_result.get_data()[0], 2);
    agg_func_3->destroy(place);
}

// Test same-row multi-condition with ALL conditions matching the same event name
// window_funnel(big_window, 'default', ts, event='登录', event='登录', event='登录', ...)
// A single row matching all 4 conditions should only count as level 1 (not 4).
TEST_F(VWindowFunnelV2Test, testSameRowAllConditionsMatch) {
    auto column_mode = ColumnString::create();
    column_mode->insert(Field::create_field<TYPE_STRING>("default"));

    auto column_timestamp = ColumnDateTimeV2::create();
    VecDateTimeValue tv0;
    tv0.unchecked_set_time(2022, 2, 28, 0, 0, 0);
    auto dtv2_0 = tv0.to_datetime_v2();
    column_timestamp->insert_data((char*)&dtv2_0, 0);

    // All 4 conditions match the single row
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));
    auto column_event4 = ColumnUInt8::create();
    column_event4->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_window = ColumnInt64::create();
    column_window->insert(Field::create_field<TYPE_BIGINT>(86400));

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    const IColumn* column[7] = {column_window.get(), column_mode.get(),   column_timestamp.get(),
                                column_event1.get(), column_event2.get(), column_event3.get(),
                                column_event4.get()};
    agg_function->add(place, column, 0, arena);

    ColumnInt32 column_result;
    agg_function->insert_result_into(place, column_result);
    // Only 1 row matching all conditions → can only reach level 1
    // because each funnel step must come from a different row
    EXPECT_EQ(column_result.get_data()[0], 1);
    agg_function->destroy(place);
}

// Test INCREASE mode: event-0 re-occurrence should not break an already-valid chain.
// Counterexample from code review:
//   3 conditions, window=100s, INCREASE mode
//   Row A (t=0s): event1 only
//   Row B (t=50s): event1 only  (new event-0 overwrites old)
//   Row C (t=50s): event2 only
//   Row D (t=60s): event3 only
// Correct result: 3 (chain starting from t=0: e1@0 -> e2@50 -> e3@60)
// Bug (before fix): returned 1 because overwriting events_timestamp[0] with t=50
// caused the INCREASE check (50 < 50) to fail for e2.
TEST_F(VWindowFunnelV2Test, testIncreaseModeEvent0Overwrite) {
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    DataTypes data_types_3 = {
            std::make_shared<DataTypeInt64>(),      std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>()};
    auto agg_func_3 = factory.get("window_funnel_v2", data_types_3, nullptr, false,
                                  BeExecVersionManager::get_newest_version());
    ASSERT_NE(agg_func_3, nullptr);

    const int NUM_ROWS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("increase"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    // Row 0: t=0s, Row 1: t=50s, Row 2: t=50s, Row 3: t=60s
    VecDateTimeValue tv0, tv1, tv2, tv3;
    tv0.unchecked_set_time(2022, 2, 28, 0, 0, 0);
    tv1.unchecked_set_time(2022, 2, 28, 0, 0, 50);
    tv2.unchecked_set_time(2022, 2, 28, 0, 0, 50);
    tv3.unchecked_set_time(2022, 2, 28, 0, 1, 0);
    auto dtv2_0 = tv0.to_datetime_v2();
    auto dtv2_1 = tv1.to_datetime_v2();
    auto dtv2_2 = tv2.to_datetime_v2();
    auto dtv2_3 = tv3.to_datetime_v2();
    column_timestamp->insert_data((char*)&dtv2_0, 0);
    column_timestamp->insert_data((char*)&dtv2_1, 0);
    column_timestamp->insert_data((char*)&dtv2_2, 0);
    column_timestamp->insert_data((char*)&dtv2_3, 0);

    // Row 0: event1=T, event2=F, event3=F
    // Row 1: event1=T, event2=F, event3=F  (duplicate event-0)
    // Row 2: event1=F, event2=T, event3=F
    // Row 3: event1=F, event2=F, event3=T
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(100));
    }

    std::unique_ptr<char[]> memory(new char[agg_func_3->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_func_3->create(place);
    const IColumn* column[6] = {column_window.get(), column_mode.get(),   column_timestamp.get(),
                                column_event1.get(), column_event2.get(), column_event3.get()};
    for (int i = 0; i < NUM_ROWS; i++) {
        agg_func_3->add(place, column, i, arena);
    }

    ColumnInt32 column_result;
    agg_func_3->insert_result_into(place, column_result);
    // Chain from t=0: e1@0 -> e2@50 (50>0 ✓) -> e3@60 (60>50 ✓) = 3
    EXPECT_EQ(column_result.get_data()[0], 3);
    agg_func_3->destroy(place);
}

// Test INCREASE mode: later event-0 starts a better chain when early chain cannot complete.
// 3 conditions, window=5s, INCREASE mode
//   Row 0 (t=0s): event1
//   Row 1 (t=50s): event1  (new start — old chain can't reach e2 within 5s)
//   Row 2 (t=51s): event2
//   Row 3 (t=52s): event3
// Correct result: 3 (chain starting from t=50: e1@50 -> e2@51 -> e3@52)
TEST_F(VWindowFunnelV2Test, testIncreaseModeNewChainBetter) {
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    DataTypes data_types_3 = {
            std::make_shared<DataTypeInt64>(),      std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>()};
    auto agg_func_3 = factory.get("window_funnel_v2", data_types_3, nullptr, false,
                                  BeExecVersionManager::get_newest_version());
    ASSERT_NE(agg_func_3, nullptr);

    const int NUM_ROWS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("increase"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    VecDateTimeValue tv0, tv1, tv2, tv3;
    tv0.unchecked_set_time(2022, 2, 28, 0, 0, 0);
    tv1.unchecked_set_time(2022, 2, 28, 0, 0, 50);
    tv2.unchecked_set_time(2022, 2, 28, 0, 0, 51);
    tv3.unchecked_set_time(2022, 2, 28, 0, 0, 52);
    auto dtv2_0 = tv0.to_datetime_v2();
    auto dtv2_1 = tv1.to_datetime_v2();
    auto dtv2_2 = tv2.to_datetime_v2();
    auto dtv2_3 = tv3.to_datetime_v2();
    column_timestamp->insert_data((char*)&dtv2_0, 0);
    column_timestamp->insert_data((char*)&dtv2_1, 0);
    column_timestamp->insert_data((char*)&dtv2_2, 0);
    column_timestamp->insert_data((char*)&dtv2_3, 0);

    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(5));
    }

    std::unique_ptr<char[]> memory(new char[agg_func_3->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_func_3->create(place);
    const IColumn* column[6] = {column_window.get(), column_mode.get(),   column_timestamp.get(),
                                column_event1.get(), column_event2.get(), column_event3.get()};
    for (int i = 0; i < NUM_ROWS; i++) {
        agg_func_3->add(place, column, i, arena);
    }

    ColumnInt32 column_result;
    agg_func_3->insert_result_into(place, column_result);
    // Old chain from t=0 reaches only level 1 (e2@51 is 51s away, outside 5s window)
    // New chain from t=50: e1@50 -> e2@51 (51>50 ✓, within 5s) -> e3@52 (52>51 ✓) = 3
    EXPECT_EQ(column_result.get_data()[0], 3);
    agg_func_3->destroy(place);
}

// Test INCREASE mode: old chain is better than new chain (max_level preserved).
// 3 conditions, window=100s, INCREASE mode
//   Row 0 (t=0s): event1
//   Row 1 (t=10s): event2
//   Row 2 (t=50s): event1  (restarts chain, old chain had level=2)
//   No more events after the restart — new chain can only reach level 1.
// Correct result: 2 (old chain: e1@0 -> e2@10)
TEST_F(VWindowFunnelV2Test, testIncreaseModeOldChainBetter) {
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    DataTypes data_types_3 = {
            std::make_shared<DataTypeInt64>(),      std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>()};
    auto agg_func_3 = factory.get("window_funnel_v2", data_types_3, nullptr, false,
                                  BeExecVersionManager::get_newest_version());
    ASSERT_NE(agg_func_3, nullptr);

    const int NUM_ROWS = 3;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("increase"));
    }
    {
        auto column_timestamp = ColumnDateTimeV2::create();
        VecDateTimeValue tv0, tv1, tv2;
        tv0.unchecked_set_time(2022, 2, 28, 0, 0, 0);
        tv1.unchecked_set_time(2022, 2, 28, 0, 0, 10);
        tv2.unchecked_set_time(2022, 2, 28, 0, 0, 50);
        auto dtv2_0 = tv0.to_datetime_v2();
        auto dtv2_1 = tv1.to_datetime_v2();
        auto dtv2_2 = tv2.to_datetime_v2();
        column_timestamp->insert_data((char*)&dtv2_0, 0);
        column_timestamp->insert_data((char*)&dtv2_1, 0);
        column_timestamp->insert_data((char*)&dtv2_2, 0);

        // Row 0: e1=T, Row 1: e2=T, Row 2: e1=T (restart)
        auto column_event1 = ColumnUInt8::create();
        column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
        column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
        column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));

        auto column_event2 = ColumnUInt8::create();
        column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
        column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
        column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

        auto column_event3 = ColumnUInt8::create();
        column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
        column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
        column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));

        auto column_window = ColumnInt64::create();
        for (int i = 0; i < NUM_ROWS; i++) {
            column_window->insert(Field::create_field<TYPE_BIGINT>(100));
        }

        std::unique_ptr<char[]> memory(new char[agg_func_3->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_func_3->create(place);
        const IColumn* column[6] = {column_window.get(),    column_mode.get(),
                                    column_timestamp.get(), column_event1.get(),
                                    column_event2.get(),    column_event3.get()};
        for (int i = 0; i < NUM_ROWS; i++) {
            agg_func_3->add(place, column, i, arena);
        }

        ColumnInt32 column_result;
        agg_func_3->insert_result_into(place, column_result);
        // Old chain: e1@0 -> e2@10 = level 2
        // New chain from t=50: only e1@50, no further events = level 1
        // max(2, 1) = 2
        EXPECT_EQ(column_result.get_data()[0], 2);
        agg_func_3->destroy(place);
    }
    {
        auto column_timestamp = ColumnDateTimeV2::create();
        VecDateTimeValue tv0, tv1, tv2, tv3;
        tv0.unchecked_set_time(2022, 2, 28, 0, 0, 0);
        tv1.unchecked_set_time(2022, 2, 28, 0, 0, 10);
        tv2.unchecked_set_time(2022, 2, 28, 0, 0, 50);
        tv3.unchecked_set_time(2022, 2, 28, 0, 0, 50);
        auto dtv2_0 = tv0.to_datetime_v2();
        auto dtv2_1 = tv1.to_datetime_v2();
        auto dtv2_2 = tv2.to_datetime_v2();
        auto dtv2_3 = tv3.to_datetime_v2();
        column_timestamp->insert_data((char*)&dtv2_0, 0);
        column_timestamp->insert_data((char*)&dtv2_1, 0);
        column_timestamp->insert_data((char*)&dtv2_2, 0);
        column_timestamp->insert_data((char*)&dtv2_3, 0);

        auto column_event1 = ColumnUInt8::create();
        column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
        column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
        column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
        column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

        auto column_event2 = ColumnUInt8::create();
        column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
        column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
        column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
        column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

        auto column_event3 = ColumnUInt8::create();
        column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
        column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
        column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
        column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));

        auto column_window = ColumnInt64::create();
        for (int i = 0; i < NUM_ROWS; i++) {
            column_window->insert(Field::create_field<TYPE_BIGINT>(100));
        }

        std::unique_ptr<char[]> memory(new char[agg_func_3->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_func_3->create(place);
        const IColumn* column[6] = {column_window.get(),    column_mode.get(),
                                    column_timestamp.get(), column_event1.get(),
                                    column_event2.get(),    column_event3.get()};
        for (int i = 0; i < NUM_ROWS; i++) {
            agg_func_3->add(place, column, i, arena);
        }

        ColumnInt32 column_result;
        agg_func_3->insert_result_into(place, column_result);
        // Old chain: e1@0 -> e2@10 = level 2
        // New chain from t=50: only e1@50, e3@50 can't advance (no e2 matched)
        // max(2, 1) = 2
        EXPECT_EQ(column_result.get_data()[0], 2);
        agg_func_3->destroy(place);
    }
}

} // namespace doris
