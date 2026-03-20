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

} // namespace doris
