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

// Test DEDUPLICATION mode: same-row multi-event matching should NOT break the chain.
// This is the exact scenario from the bug report where V1 returns 3 but V2 returned 2.
//
// 3 conditions, window=1 day (86400s), DEDUPLICATION mode
//   Row 0 (t=10:00): event1=T, event2=T, event3=F  → matches E0+E1 on same row
//   Row 1 (t=11:00): event1=T, event2=T, event3=F  → matches E0+E1 on same row
//   Row 2 (t=12:00): event1=T, event2=F, event3=T  → matches E0+E2 on same row
//
// V2 stores events in reverse order per row: [E1, E0] for each multi-match row.
// After sort, events_list for row0: (t=10:00, E1-cont), (t=10:00, E0)
// Bug: when processing E0 from row0 after E1 was already used to advance the chain,
// old V2 would break the chain. With the fix, E0 is recognized as same-row and skipped.
//
// Expected chain: E0@row0 → E1@row1 → E2@row2 = 3
TEST_F(VWindowFunnelV2Test, testDeduplicationSameRowMultiEvent) {
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
        column_mode->insert(Field::create_field<TYPE_STRING>("deduplication"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    VecDateTimeValue tv0, tv1, tv2;
    tv0.unchecked_set_time(2022, 3, 12, 10, 0, 0);
    tv1.unchecked_set_time(2022, 3, 12, 11, 0, 0);
    tv2.unchecked_set_time(2022, 3, 12, 12, 0, 0);
    auto dtv2_0 = tv0.to_datetime_v2();
    auto dtv2_1 = tv1.to_datetime_v2();
    auto dtv2_2 = tv2.to_datetime_v2();
    column_timestamp->insert_data((char*)&dtv2_0, 0);
    column_timestamp->insert_data((char*)&dtv2_1, 0);
    column_timestamp->insert_data((char*)&dtv2_2, 0);

    // Row 0: event1=T, event2=T, event3=F  (matches E0 and E1)
    // Row 1: event1=T, event2=T, event3=F  (matches E0 and E1)
    // Row 2: event1=T, event2=F, event3=T  (matches E0 and E2)
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(86400));
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
    // Chain: E0@row0 -> E1@row1 -> E2@row2 = 3
    // Before fix: V2 returned 2 because same-row E0 broke the chain after E1 was used.
    EXPECT_EQ(column_result.get_data()[0], 3);
    agg_func_3->destroy(place);
}

// Test DEDUPLICATION mode: a true duplicate on a DIFFERENT row should still break the chain.
// This ensures the fix doesn't over-suppress chain breaks.
//
// 3 conditions, window=86400s, DEDUPLICATION mode
//   Row 0 (t=10:00): event1=T only
//   Row 1 (t=11:00): event2=T only
//   Row 2 (t=12:00): event1=T only  ← true duplicate E0 from different row → breaks chain
//   Row 3 (t=13:00): event3=T only
//
// Expected: 2 (chain E0@row0 → E1@row1, then E0@row2 breaks it; new chain E0@row2 alone = 1)
TEST_F(VWindowFunnelV2Test, testDeduplicationTrueDuplicateStillBreaks) {
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
        column_mode->insert(Field::create_field<TYPE_STRING>("deduplication"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    VecDateTimeValue tv0, tv1, tv2, tv3;
    tv0.unchecked_set_time(2022, 3, 12, 10, 0, 0);
    tv1.unchecked_set_time(2022, 3, 12, 11, 0, 0);
    tv2.unchecked_set_time(2022, 3, 12, 12, 0, 0);
    tv3.unchecked_set_time(2022, 3, 12, 13, 0, 0);
    auto dtv2_0 = tv0.to_datetime_v2();
    auto dtv2_1 = tv1.to_datetime_v2();
    auto dtv2_2 = tv2.to_datetime_v2();
    auto dtv2_3 = tv3.to_datetime_v2();
    column_timestamp->insert_data((char*)&dtv2_0, 0);
    column_timestamp->insert_data((char*)&dtv2_1, 0);
    column_timestamp->insert_data((char*)&dtv2_2, 0);
    column_timestamp->insert_data((char*)&dtv2_3, 0);

    // Row 0: event1=T only
    // Row 1: event2=T only
    // Row 2: event1=T only (true duplicate from different row)
    // Row 3: event3=T only
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
        column_window->insert(Field::create_field<TYPE_BIGINT>(86400));
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
    // Chain: E0@row0 -> E1@row1 (level=2), then E0@row2 is a TRUE duplicate from a
    // different row → breaks chain. New chain: E0@row2, no E1 → level=1.
    // max(2, 1) = 2
    EXPECT_EQ(column_result.get_data()[0], 2);
    agg_func_3->destroy(place);
}

// Dedup mode: same-row E0 skip when chain is advanced by same-row events.
// When a row matches all 3 conditions and its E2 advances the chain, E0 from
// the same row should be skipped (not restart the chain) because the row already
// contributed to chain advancement.
TEST_F(VWindowFunnelV2Test, testDeduplicationE0PendingRestart) {
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    DataTypes data_types_3 = {
            std::make_shared<DataTypeInt64>(),      std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>()};
    auto agg_func_3 = factory.get("window_funnel_v2", data_types_3, nullptr, false,
                                  BeExecVersionManager::get_newest_version());
    ASSERT_NE(agg_func_3, nullptr);

    // Scenario: 3 conditions, c1/c2/c3.
    // Row 0 (t=10:00): c1=T only            → E0@R0, chain starts level=0
    // Row 1 (t=11:00): c2=T only            → E1@R1, chain advances level=1
    // Row 2 (t=12:00): c1=T, c2=T, c3=T    → E2(cont=0), E1(cont=1), E0(cont=1)
    //   E2: predecessor E1 matched, not same row → promote level=2
    //   E1(cont=1): duplicate, same row as E2@R2 → skip
    //   E0(cont=1): same row as chain (E2@R2 at level 2) → skip
    // Row 3 (t=13:00): c3=T only            → E2: duplicate level=2, different row → terminates chain
    // Result: max(2+1, 0+1) = 3
    const int NUM_ROWS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("deduplication"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    VecDateTimeValue tv0, tv1, tv2, tv3;
    tv0.unchecked_set_time(2022, 3, 12, 10, 0, 0);
    tv1.unchecked_set_time(2022, 3, 12, 11, 0, 0);
    tv2.unchecked_set_time(2022, 3, 12, 12, 0, 0);
    tv3.unchecked_set_time(2022, 3, 12, 13, 0, 0);
    auto dtv2_0 = tv0.to_datetime_v2();
    auto dtv2_1 = tv1.to_datetime_v2();
    auto dtv2_2 = tv2.to_datetime_v2();
    auto dtv2_3 = tv3.to_datetime_v2();
    column_timestamp->insert_data((char*)&dtv2_0, 0);
    column_timestamp->insert_data((char*)&dtv2_1, 0);
    column_timestamp->insert_data((char*)&dtv2_2, 0);
    column_timestamp->insert_data((char*)&dtv2_3, 0);

    // Row 0: c1=T, c2=F, c3=F
    // Row 1: c1=F, c2=T, c3=F
    // Row 2: c1=T, c2=T, c3=T
    // Row 3: c1=F, c2=F, c3=T
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(86400));
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
    // Chain reaches level 2 (3 steps matched: E0→E1→E2) before E2@R3 duplicate terminates it.
    EXPECT_EQ(column_result.get_data()[0], 3);
    agg_func_3->destroy(place);
}

// Dedup mode: E0 restart after chain terminated by same-row duplicate.
// When a row matches c1+c2, and c2 is a duplicate that terminates the chain,
// the E0 from the same row should start a new chain because _eliminate_chain()
// clears events_timestamp, so _is_same_row_as_chain won't trigger.
TEST_F(VWindowFunnelV2Test, testDeduplicationE0PendingRestartApplied) {
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    DataTypes data_types_3 = {
            std::make_shared<DataTypeInt64>(),      std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>()};
    auto agg_func_3 = factory.get("window_funnel_v2", data_types_3, nullptr, false,
                                  BeExecVersionManager::get_newest_version());
    ASSERT_NE(agg_func_3, nullptr);

    // Scenario: Row matches c1+c2 where c2 is a duplicate → chain terminated by duplicate.
    // The pending E0 from same row should restart.
    // Row 0 (t=10:00): c1=T only → chain level=0
    // Row 1 (t=11:00): c2=T only → chain level=1
    // Row 2 (t=12:00): c1=T, c2=T → E1(cont=0), E0(cont=1)
    //   E1(cont=0): duplicate! R2 ≠ R0, R2 ≠ R1 → NOT same row as chain → terminate.
    //   E0(cont=1): _is_same_row_as_chain → but chain just eliminated! events_timestamp[0] cleared.
    //     Actually after eliminate, events_timestamp[0] has no value, so the _is_same_row_as_chain
    //     check at E0 won't enter the if branch. E0 directly starts new chain.
    // Row 3 (t=13:00): c2=T only → chain level=1
    // Row 4 (t=14:00): c3=T only → chain level=2
    // Result: max(1+1, 2+1) = 3
    const int NUM_ROWS = 5;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("deduplication"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    VecDateTimeValue tv0, tv1, tv2, tv3, tv4;
    tv0.unchecked_set_time(2022, 3, 12, 10, 0, 0);
    tv1.unchecked_set_time(2022, 3, 12, 11, 0, 0);
    tv2.unchecked_set_time(2022, 3, 12, 12, 0, 0);
    tv3.unchecked_set_time(2022, 3, 12, 13, 0, 0);
    tv4.unchecked_set_time(2022, 3, 12, 14, 0, 0);
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

    // Row 0: c1=T, c2=F, c3=F
    // Row 1: c1=F, c2=T, c3=F
    // Row 2: c1=T, c2=T, c3=F
    // Row 3: c1=F, c2=T, c3=F
    // Row 4: c1=F, c2=F, c3=T
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(86400));
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
    // First chain: E0@R0→E1@R1 (level=2). E1@R2 duplicate terminates.
    // E0@R2 restarts. New chain: E0@R2→E1@R3→E2@R4 (level=3).
    // max(2, 3) = 3.
    EXPECT_EQ(column_result.get_data()[0], 3);
    agg_func_3->destroy(place);
}

// Fixed mode: same-row multi-condition level jump fix.
// When a row matches both a high-index condition (c3) and a low-index condition (c2),
// the high-index event was processed first and triggered a spurious level jump before
// the low-index event could advance the chain.
TEST_F(VWindowFunnelV2Test, testFixedSameRowMultiCondLevelJump) {
    // Use 3 conditions: c1=event1, c2=event2, c3=event3
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    DataTypes data_types_3 = {
            std::make_shared<DataTypeInt64>(),      std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>()};
    auto agg_func_3 = factory.get("window_funnel_v2", data_types_3, nullptr, false,
                                  BeExecVersionManager::get_newest_version());
    ASSERT_NE(agg_func_3, nullptr);

    const int NUM_ROWS = 2;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("fixed"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    VecDateTimeValue tv0, tv1;
    tv0.unchecked_set_time(2020, 1, 1, 0, 0, 0);
    tv1.unchecked_set_time(2020, 1, 2, 0, 0, 0);
    auto dtv2_0 = tv0.to_datetime_v2();
    auto dtv2_1 = tv1.to_datetime_v2();
    column_timestamp->insert_data((char*)&dtv2_0, 0);
    column_timestamp->insert_data((char*)&dtv2_1, 0);

    // Row 0: c1=T, c2=T, c3=T (all match — starts chain via c1)
    // Row 1: c1=F, c2=T, c3=T (c3 would trigger level jump before c2 can advance)
    auto column_event1 = ColumnUInt8::create();
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    auto column_event2 = ColumnUInt8::create();
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event2->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_event3 = ColumnUInt8::create();
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_event3->insert(Field::create_field<TYPE_BOOLEAN>(1));

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(86400));
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
    // Before fix: c3 from row 1 triggered level jump (c2 predecessor not matched) → result 1.
    // After fix: same-row look-ahead finds c2 can advance → skip level jump → result 2.
    EXPECT_EQ(column_result.get_data()[0], 2);
    agg_func_3->destroy(place);
}

// Fixed mode: same-row duplicate should NOT suppress a level-jump break.
// Counterexample from review: E0@r0, E1@r1, (E3,E1)@r2, E2@r3, E3@r4.
// E1@r2 is a duplicate of an already-matched level (curr_level=2, sr_idx=1 ≤ curr_level),
// so it must not be treated as an advancement. The chain should break at E3@r2.
TEST_F(VWindowFunnelV2Test, testFixedSameRowDuplicateDoesNotSuppressJump) {
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    // 4 conditions: window, mode, timestamp, c1, c2, c3, c4
    DataTypes data_types_4 = {
            std::make_shared<DataTypeInt64>(),      std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>()};
    auto agg_func = factory.get("window_funnel_v2", data_types_4, nullptr, false,
                                BeExecVersionManager::get_newest_version());
    ASSERT_NE(agg_func, nullptr);

    const int NUM_ROWS = 5;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("fixed"));
    }

    // Timestamps: r0=t0, r1=t1, r2=t2, r3=t3, r4=t4 (all within window)
    auto column_timestamp = ColumnDateTimeV2::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        VecDateTimeValue tv;
        tv.unchecked_set_time(2020, 1, 1 + i, 0, 0, 0);
        auto dtv2 = tv.to_datetime_v2();
        column_timestamp->insert_data((char*)&dtv2, 0);
    }

    // r0: c1=T, c2=F, c3=F, c4=F → E0
    // r1: c1=F, c2=T, c3=F, c4=F → E1
    // r2: c1=F, c2=T, c3=F, c4=T → E1(dup)+E3(jump)
    // r3: c1=F, c2=F, c3=T, c4=F → E2
    // r4: c1=F, c2=F, c3=F, c4=T → E3
    auto column_c1 = ColumnUInt8::create();
    for (int v : {1, 0, 0, 0, 0}) column_c1->insert(Field::create_field<TYPE_BOOLEAN>(v));

    auto column_c2 = ColumnUInt8::create();
    for (int v : {0, 1, 1, 0, 0}) column_c2->insert(Field::create_field<TYPE_BOOLEAN>(v));

    auto column_c3 = ColumnUInt8::create();
    for (int v : {0, 0, 0, 1, 0}) column_c3->insert(Field::create_field<TYPE_BOOLEAN>(v));

    auto column_c4 = ColumnUInt8::create();
    for (int v : {0, 0, 1, 0, 1}) column_c4->insert(Field::create_field<TYPE_BOOLEAN>(v));

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(86400 * 10));
    }

    std::unique_ptr<char[]> memory(new char[agg_func->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_func->create(place);
    const IColumn* column[7] = {column_window.get(), column_mode.get(), column_timestamp.get(),
                                column_c1.get(),     column_c2.get(),   column_c3.get(),
                                column_c4.get()};
    for (int i = 0; i < NUM_ROWS; i++) {
        agg_func->add(place, column, i, arena);
    }

    ColumnInt32 column_result;
    agg_func->insert_result_into(place, column_result);
    // Chain: E0@r0 → E1@r1 → E3@r2 triggers level jump. E1@r2 is a duplicate
    // (sr_idx=1 ≤ curr_level=2), so it must NOT suppress the break. Result = 2.
    EXPECT_EQ(column_result.get_data()[0], 2);
    agg_func->destroy(place);
}

// Fixed mode: when a row matches multiple conditions including E0 (c1),
// the E0 continuation should NOT restart the chain if a same-row event
// already advanced it. This emulates V1's row-based semantics where each
// row is tested against exactly one expected condition.
TEST_F(VWindowFunnelV2Test, testFixedContinuationE0DoesNotRestartChain) {
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    DataTypes data_types_3 = {
            std::make_shared<DataTypeInt64>(),      std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>()};
    auto agg_func_3 = factory.get("window_funnel_v2", data_types_3, nullptr, false,
                                  BeExecVersionManager::get_newest_version());
    ASSERT_NE(agg_func_3, nullptr);

    // 3 rows, 3 conditions (c1, c2, c3):
    //   r0: c1=T, c2=F, c3=F   -> E0 starts chain
    //   r1: c1=T, c2=T, c3=F   -> c2 advances chain, c1(cont) should be skipped
    //   r2: c1=F, c2=F, c3=T   -> c3 advances chain to level 2
    // Expected result: 3 (c1@r0 -> c2@r1 -> c3@r2)
    // Without E0-skip fix, c1@r1(cont) would restart chain, and result would be 2.
    const int NUM_ROWS = 3;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("fixed"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    VecDateTimeValue tv;
    for (int i = 0; i < NUM_ROWS; i++) {
        tv.unchecked_set_time(2020, 1, i + 1, 0, 0, 0);
        auto dtv2 = tv.to_datetime_v2();
        column_timestamp->insert_data((char*)&dtv2, 0);
    }

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(86400 * 10));
    }

    // c1: r0=T, r1=T, r2=F
    auto column_c1 = ColumnUInt8::create();
    column_c1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_c1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_c1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    // c2: r0=F, r1=T, r2=F
    auto column_c2 = ColumnUInt8::create();
    column_c2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_c2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_c2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    // c3: r0=F, r1=F, r2=T
    auto column_c3 = ColumnUInt8::create();
    column_c3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_c3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_c3->insert(Field::create_field<TYPE_BOOLEAN>(1));

    std::unique_ptr<char[]> memory(new char[agg_func_3->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_func_3->create(place);
    const IColumn* columns[6] = {column_window.get(), column_mode.get(), column_timestamp.get(),
                                 column_c1.get(),     column_c2.get(),   column_c3.get()};
    for (int i = 0; i < NUM_ROWS; i++) {
        agg_func_3->add(place, columns, i, arena);
    }

    ColumnInt32 column_result;
    agg_func_3->insert_result_into(place, column_result);
    EXPECT_EQ(column_result.get_data()[0], 3);
    agg_func_3->destroy(place);
}

// Dedup mode multi-pass: V1 reaches level 4 by starting from a later E0 and skipping
// over rows that match higher-level conditions. The old single-pass V2 broke at level 3
// because it greedily promoted from multi-condition rows, creating premature duplicates.
// 7 conditions, 15 valid rows (after null filtering). Window ~35 years (effectively unlimited).
// V1 chain: c1@R5(pk=8) → c2@R9(pk=6) → c3@R10(pk=12) → c4@R12(pk=5), then c1 dup at R13 → level=4.
TEST_F(VWindowFunnelV2Test, testDeduplicationMultiPassGapCheck) {
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    // 7 conditions
    DataTypes data_types_7 = {
            std::make_shared<DataTypeInt64>(),      std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>()};
    auto agg_func = factory.get("window_funnel_v2", data_types_7, nullptr, false,
                                BeExecVersionManager::get_newest_version());
    ASSERT_NE(agg_func, nullptr);

    // 15 rows sorted by timestamp (null rows excluded at SQL level).
    // Conditions: c1=val<4, c2=val<=2, c3=val!=6, c4=val<=4, c5=date='2023-12-13',
    //             c6=val<=5, c7=val<=8
    struct RowData {
        int year, month, day;
        int val;
        bool date_is_2023_12_13;
    };
    // clang-format off
    RowData rows[] = {
        {2000, 3, 19, 3, false},  // pk=10
        {2000, 4,  3, 8, false},  // pk=9
        {2001,10, 12, 3, false},  // pk=2
        {2002,11, 15, 8, false},  // pk=0
        {2003, 9, 10, 0, false},  // pk=19
        {2006, 6,  6, 0, false},  // pk=8  ← V1 best E0 start
        {2008, 7,  3, 8, false},  // pk=3
        {2009, 3,  3, 9, false},  // pk=11
        {2009, 5, 16, 8, true},   // pk=16
        {2009,11,  3, 0, true},   // pk=6  ← all 7 conditions match
        {2011, 6, 25, 7, true},   // pk=12
        {2014, 5, 16, 6, false},  // pk=14
        {2014,11,  2, 0, false},  // pk=5
        {2016, 1,  8, 3, false},  // pk=13
        {2017, 1,  3, 8, true},   // pk=17
    };
    // clang-format on
    const int NUM_ROWS = 15;

    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("deduplication"));
    }

    auto column_timestamp = ColumnDateTimeV2::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        VecDateTimeValue tv;
        tv.unchecked_set_time(rows[i].year, rows[i].month, rows[i].day, 0, 0, 0);
        auto dtv2 = tv.to_datetime_v2();
        column_timestamp->insert_data((char*)&dtv2, 0);
    }

    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(1119906808));
    }

    // c1: val < 4
    auto col_c1 = ColumnUInt8::create();
    for (int i = 0; i < NUM_ROWS; i++)
        col_c1->insert(Field::create_field<TYPE_BOOLEAN>(rows[i].val < 4 ? 1 : 0));
    // c2: val <= 2
    auto col_c2 = ColumnUInt8::create();
    for (int i = 0; i < NUM_ROWS; i++)
        col_c2->insert(Field::create_field<TYPE_BOOLEAN>(rows[i].val <= 2 ? 1 : 0));
    // c3: val != 6
    auto col_c3 = ColumnUInt8::create();
    for (int i = 0; i < NUM_ROWS; i++)
        col_c3->insert(Field::create_field<TYPE_BOOLEAN>(rows[i].val != 6 ? 1 : 0));
    // c4: val <= 4
    auto col_c4 = ColumnUInt8::create();
    for (int i = 0; i < NUM_ROWS; i++)
        col_c4->insert(Field::create_field<TYPE_BOOLEAN>(rows[i].val <= 4 ? 1 : 0));
    // c5: date = '2023-12-13'
    auto col_c5 = ColumnUInt8::create();
    for (int i = 0; i < NUM_ROWS; i++)
        col_c5->insert(Field::create_field<TYPE_BOOLEAN>(rows[i].date_is_2023_12_13 ? 1 : 0));
    // c6: val <= 5
    auto col_c6 = ColumnUInt8::create();
    for (int i = 0; i < NUM_ROWS; i++)
        col_c6->insert(Field::create_field<TYPE_BOOLEAN>(rows[i].val <= 5 ? 1 : 0));
    // c7: val <= 8
    auto col_c7 = ColumnUInt8::create();
    for (int i = 0; i < NUM_ROWS; i++)
        col_c7->insert(Field::create_field<TYPE_BOOLEAN>(rows[i].val <= 8 ? 1 : 0));

    std::unique_ptr<char[]> memory(new char[agg_func->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_func->create(place);
    const IColumn* column[10] = {column_window.get(), column_mode.get(), column_timestamp.get(),
                                 col_c1.get(),        col_c2.get(),      col_c3.get(),
                                 col_c4.get(),        col_c5.get(),      col_c6.get(),
                                 col_c7.get()};
    for (int i = 0; i < NUM_ROWS; i++) {
        agg_func->add(place, column, i, arena);
    }

    ColumnInt32 column_result;
    agg_func->insert_result_into(place, column_result);
    // V1 = 4: c1@R5(pk=8) → c2@R9(pk=6) → c3@R10(pk=12) → c4@R12(pk=5)
    // c1 dup at R13(pk=13) breaks the chain before c5 can be matched.
    EXPECT_EQ(column_result.get_data()[0], 4);
    agg_func->destroy(place);
}

// FIXED mode "relevant-but-wrong row" break: when the next row after a matched
// condition matches some condition but NOT the expected next one, the chain must
// break. V1 checks consecutive rows, so a c2-only row after c1→c2 breaks when
// expecting c3. Before the fix, V2 silently re-promoted c2 and kept searching.
TEST_F(VWindowFunnelV2Test, testFixedRelevantButWrongRowBreaksChain) {
    AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
    DataTypes data_types_3 = {
            std::make_shared<DataTypeInt64>(),      std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeDateTimeV2>(), std::make_shared<DataTypeUInt8>(),
            std::make_shared<DataTypeUInt8>(),      std::make_shared<DataTypeUInt8>()};
    auto agg_func_3 = factory.get("window_funnel_v2", data_types_3, nullptr, false,
                                  BeExecVersionManager::get_newest_version());
    ASSERT_NE(agg_func_3, nullptr);

    // 4 rows, 3 conditions (c1, c2, c3):
    //   r0: c1=T, c2=F, c3=F  → E0, chain level 0
    //   r1: c1=F, c2=T, c3=F  → c2 advances chain to level 1
    //   r2: c1=F, c2=T, c3=F  → c2 re-matches level 1 (expected c3). Chain BREAKS.
    //   r3: c1=F, c2=F, c3=T  → c3 could advance but chain is already broken
    // Expected: 2 (c1@r0 → c2@r1, break at r2)
    const int NUM_ROWS = 4;
    auto column_mode = ColumnString::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_mode->insert(Field::create_field<TYPE_STRING>("fixed"));
    }
    auto column_timestamp = ColumnDateTimeV2::create();
    VecDateTimeValue tv;
    for (int i = 0; i < NUM_ROWS; i++) {
        tv.unchecked_set_time(2020, 1, i + 1, 0, 0, 0);
        auto dtv2 = tv.to_datetime_v2();
        column_timestamp->insert_data((char*)&dtv2, 0);
    }
    auto column_window = ColumnInt64::create();
    for (int i = 0; i < NUM_ROWS; i++) {
        column_window->insert(Field::create_field<TYPE_BIGINT>(86400 * 10));
    }

    // c1: r0=T, r1=F, r2=F, r3=F
    auto column_c1 = ColumnUInt8::create();
    column_c1->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_c1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_c1->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_c1->insert(Field::create_field<TYPE_BOOLEAN>(0));

    // c2: r0=F, r1=T, r2=T, r3=F
    auto column_c2 = ColumnUInt8::create();
    column_c2->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_c2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_c2->insert(Field::create_field<TYPE_BOOLEAN>(1));
    column_c2->insert(Field::create_field<TYPE_BOOLEAN>(0));

    // c3: r0=F, r1=F, r2=F, r3=T
    auto column_c3 = ColumnUInt8::create();
    column_c3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_c3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_c3->insert(Field::create_field<TYPE_BOOLEAN>(0));
    column_c3->insert(Field::create_field<TYPE_BOOLEAN>(1));

    std::unique_ptr<char[]> memory(new char[agg_func_3->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_func_3->create(place);
    const IColumn* columns[6] = {column_window.get(), column_mode.get(), column_timestamp.get(),
                                 column_c1.get(),     column_c2.get(),   column_c3.get()};
    for (int i = 0; i < NUM_ROWS; i++) {
        agg_func_3->add(place, columns, i, arena);
    }

    ColumnInt32 column_result;
    agg_func_3->insert_result_into(place, column_result);
    EXPECT_EQ(column_result.get_data()[0], 2);
    agg_func_3->destroy(place);
}

} // namespace doris
