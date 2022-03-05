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

#include <gtest/gtest.h>

#include "common/logging.h"
#include "exprs/aggregate_functions.h"
#include "runtime/datetime_value.h"
#include "testutil/function_utils.h"

namespace doris {

class WindowFunnelTest : public testing::Test {
public:
    WindowFunnelTest() {}
};

TEST_F(WindowFunnelTest, testMax4SortedNoMerge) {
    FunctionUtils* futil = new FunctionUtils();
    doris_udf::FunctionContext* context = futil->get_fn_ctx();

    const int NUM_CONDS = 4;
    for (int i = -1; i < NUM_CONDS + 4; i++) {
        StringVal stringVal1;
        BigIntVal window(i);
        StringVal mode("default");
        std::vector<doris_udf::AnyVal*> constant_args;
        constant_args.emplace_back(&window);
        constant_args.emplace_back(&mode);
        context->impl()->set_constant_args(std::move(constant_args));

        AggregateFunctions::window_funnel_init(context, &stringVal1);

        DateTimeVal timestamp;
        DateTimeValue time_value;
        time_value.set_time(2020, 2, 28, 0, 0, 1, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds[NUM_CONDS] = {true, false, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 2, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds1[NUM_CONDS] = {false, true, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds1, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 3, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds2[NUM_CONDS] = {false, false, true, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds2, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 4, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds3[NUM_CONDS] = {false, false, false, true};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds3, &stringVal1);

        IntVal v = AggregateFunctions::window_funnel_finalize(context, stringVal1);
        LOG(INFO) << "event num: " << NUM_CONDS << " window: " << window.val;
        ASSERT_EQ(v.val, i < 0 ? 1 : (i < NUM_CONDS ? i + 1 : NUM_CONDS));
    }
    delete futil;
}

TEST_F(WindowFunnelTest, testMax4SortedMerge) {
    FunctionUtils* futil = new FunctionUtils();
    doris_udf::FunctionContext* context = futil->get_fn_ctx();

    const int NUM_CONDS = 4;
    for (int i = -1; i < NUM_CONDS + 4; i++) {
        StringVal stringVal1;
        BigIntVal window(i);
        StringVal mode("default");
        std::vector<doris_udf::AnyVal*> constant_args;
        constant_args.emplace_back(&window);
        constant_args.emplace_back(&mode);
        context->impl()->set_constant_args(std::move(constant_args));

        AggregateFunctions::window_funnel_init(context, &stringVal1);

        DateTimeVal timestamp;
        DateTimeValue time_value;
        time_value.set_time(2020, 2, 28, 0, 0, 1, 0);
        time_value.to_datetime_val(&timestamp);

        BooleanVal conds[NUM_CONDS] = {true, false, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 2, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds1[NUM_CONDS] = {false, true, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds1, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 3, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds2[NUM_CONDS] = {false, false, true, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds2, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 4, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds3[NUM_CONDS] = {false, false, false, true};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds3, &stringVal1);

        StringVal s = AggregateFunctions::window_funnel_serialize(context, stringVal1);

        StringVal stringVal2;
        AggregateFunctions::window_funnel_init(context, &stringVal2);
        AggregateFunctions::window_funnel_merge(context, s, &stringVal2);
        IntVal v = AggregateFunctions::window_funnel_finalize(context, stringVal2);
        LOG(INFO) << "event num: " << NUM_CONDS << " window: " << window.val;
        ASSERT_EQ(v.val, i < 0 ? 1 : (i < NUM_CONDS ? i + 1 : NUM_CONDS));
    }
    delete futil;
}

TEST_F(WindowFunnelTest, testMax4ReverseSortedNoMerge) {
    FunctionUtils* futil = new FunctionUtils();
    doris_udf::FunctionContext* context = futil->get_fn_ctx();

    const int NUM_CONDS = 4;
    for (int i = -1; i < NUM_CONDS + 4; i++) {
        StringVal stringVal1;
        BigIntVal window(i);
        StringVal mode("default");
        std::vector<doris_udf::AnyVal*> constant_args;
        constant_args.emplace_back(&window);
        constant_args.emplace_back(&mode);
        context->impl()->set_constant_args(std::move(constant_args));

        AggregateFunctions::window_funnel_init(context, &stringVal1);

        DateTimeVal timestamp;
        DateTimeValue time_value;
        time_value.set_time(2020, 2, 28, 0, 0, 3, 0);
        time_value.to_datetime_val(&timestamp);

        BooleanVal conds[NUM_CONDS] = {true, false, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 2, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds1[NUM_CONDS] = {false, true, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds1, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 1, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds2[NUM_CONDS] = {false, false, true, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds2, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 0, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds3[NUM_CONDS] = {false, false, false, true};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds3, &stringVal1);

        IntVal v = AggregateFunctions::window_funnel_finalize(context, stringVal1);
        LOG(INFO) << "event num: " << NUM_CONDS << " window: " << window.val;
        ASSERT_EQ(v.val, 1);
    }
    delete futil;
}

TEST_F(WindowFunnelTest, testMax4ReverseSortedMerge) {
    FunctionUtils* futil = new FunctionUtils();
    doris_udf::FunctionContext* context = futil->get_fn_ctx();

    const int NUM_CONDS = 4;
    for (int i = -1; i < NUM_CONDS + 4; i++) {
        StringVal stringVal1;
        BigIntVal window(i);
        StringVal mode("default");
        std::vector<doris_udf::AnyVal*> constant_args;
        constant_args.emplace_back(&window);
        constant_args.emplace_back(&mode);
        context->impl()->set_constant_args(std::move(constant_args));

        AggregateFunctions::window_funnel_init(context, &stringVal1);

        DateTimeVal timestamp;
        DateTimeValue time_value;
        time_value.set_time(2020, 2, 28, 0, 0, 3, 0);
        time_value.to_datetime_val(&timestamp);

        BooleanVal conds[NUM_CONDS] = {true, false, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 2, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds1[NUM_CONDS] = {false, true, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds1, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 1, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds2[NUM_CONDS] = {false, false, true, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds2, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 0, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds3[NUM_CONDS] = {false, false, false, true};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds3, &stringVal1);

        StringVal s = AggregateFunctions::window_funnel_serialize(context, stringVal1);

        StringVal stringVal2;
        AggregateFunctions::window_funnel_init(context, &stringVal2);
        AggregateFunctions::window_funnel_merge(context, s, &stringVal2);
        IntVal v = AggregateFunctions::window_funnel_finalize(context, stringVal2);
        LOG(INFO) << "event num: " << NUM_CONDS << " window: " << window.val;
        ASSERT_EQ(v.val, 1);
    }
    delete futil;
}

TEST_F(WindowFunnelTest, testMax4DuplicateSortedNoMerge) {
    FunctionUtils* futil = new FunctionUtils();
    doris_udf::FunctionContext* context = futil->get_fn_ctx();

    const int NUM_CONDS = 4;
    for (int i = -1; i < NUM_CONDS + 4; i++) {
        StringVal stringVal1;
        BigIntVal window(i);
        StringVal mode("default");
        std::vector<doris_udf::AnyVal*> constant_args;
        constant_args.emplace_back(&window);
        constant_args.emplace_back(&mode);
        context->impl()->set_constant_args(std::move(constant_args));

        AggregateFunctions::window_funnel_init(context, &stringVal1);

        DateTimeVal timestamp;
        DateTimeValue time_value;
        time_value.set_time(2020, 2, 28, 0, 0, 0, 0);
        time_value.to_datetime_val(&timestamp);

        BooleanVal conds[NUM_CONDS] = {true, false, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 1, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds1[NUM_CONDS] = {false, true, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds1, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 2, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds2[NUM_CONDS] = {true, false, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds2, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 3, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds3[NUM_CONDS] = {false, false, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds3, &stringVal1);

        IntVal v = AggregateFunctions::window_funnel_finalize(context, stringVal1);
        LOG(INFO) << "event num: " << NUM_CONDS << " window: " << window.val;
        ASSERT_EQ(v.val, i < 0 ? 1 : (i < 2 ? i + 1 : 2));
    }
    delete futil;
}

TEST_F(WindowFunnelTest, testMax4DuplicateSortedMerge) {
    FunctionUtils* futil = new FunctionUtils();
    doris_udf::FunctionContext* context = futil->get_fn_ctx();

    const int NUM_CONDS = 4;
    for (int i = -1; i < NUM_CONDS + 4; i++) {
        StringVal stringVal1;
        BigIntVal window(i);
        StringVal mode("default");
        std::vector<doris_udf::AnyVal*> constant_args;
        constant_args.emplace_back(&window);
        constant_args.emplace_back(&mode);
        context->impl()->set_constant_args(std::move(constant_args));

        AggregateFunctions::window_funnel_init(context, &stringVal1);

        DateTimeVal timestamp;
        DateTimeValue time_value;
        time_value.set_time(2020, 2, 28, 0, 0, 0, 0);
        time_value.to_datetime_val(&timestamp);

        BooleanVal conds[NUM_CONDS] = {true, false, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 1, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds1[NUM_CONDS] = {false, true, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds1, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 2, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds2[NUM_CONDS] = {true, false, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds2, &stringVal1);

        time_value.set_time(2020, 2, 28, 0, 0, 3, 0);
        time_value.to_datetime_val(&timestamp);
        BooleanVal conds3[NUM_CONDS] = {false, false, false, false};
        AggregateFunctions::window_funnel_update(context, window, mode, timestamp, NUM_CONDS,
                                          conds3, &stringVal1);

        StringVal s = AggregateFunctions::window_funnel_serialize(context, stringVal1);

        StringVal stringVal2;
        AggregateFunctions::window_funnel_init(context, &stringVal2);
        AggregateFunctions::window_funnel_merge(context, s, &stringVal2);
        IntVal v = AggregateFunctions::window_funnel_finalize(context, stringVal2);
        LOG(INFO) << "event num: " << NUM_CONDS << " window: " << window.val;
        ASSERT_EQ(v.val, i < 0 ? 1 : (i < 2 ? i + 1 : 2));
    }
    delete futil;
}

TEST_F(WindowFunnelTest, testNoMatchedEvent) {
    FunctionUtils* futil = new FunctionUtils();
    doris_udf::FunctionContext* context = futil->get_fn_ctx();

    StringVal stringVal1;
    BigIntVal window(0);
    StringVal mode("default");
    std::vector<doris_udf::AnyVal*> constant_args;
    constant_args.emplace_back(&window);
    constant_args.emplace_back(&mode);
    context->impl()->set_constant_args(std::move(constant_args));

    AggregateFunctions::window_funnel_init(context, &stringVal1);

    DateTimeVal timestamp;
    DateTimeValue time_value;
    time_value.set_time(2020, 2, 28, 0, 0, 0, 0);
    time_value.to_datetime_val(&timestamp);

    BooleanVal conds[4] = {false, false, false, false};
    AggregateFunctions::window_funnel_update(context, window, mode, timestamp, 4,
                                      conds, &stringVal1);

    IntVal v = AggregateFunctions::window_funnel_finalize(context, stringVal1);
    ASSERT_EQ(v.val, 0);
    delete futil;
}

TEST_F(WindowFunnelTest, testNoEvent) {
    FunctionUtils* futil = new FunctionUtils();
    doris_udf::FunctionContext* context = futil->get_fn_ctx();

    StringVal stringVal1;
    BigIntVal window(0);
    StringVal mode("default");
    std::vector<doris_udf::AnyVal*> constant_args;
    constant_args.emplace_back(&window);
    constant_args.emplace_back(&mode);
    context->impl()->set_constant_args(std::move(constant_args));

    AggregateFunctions::window_funnel_init(context, &stringVal1);

    IntVal v = AggregateFunctions::window_funnel_finalize(context, stringVal1);
    ASSERT_EQ(v.val, 0);

    StringVal stringVal2;
    AggregateFunctions::window_funnel_init(context, &stringVal2);

    v = AggregateFunctions::window_funnel_finalize(context, stringVal2);
    ASSERT_EQ(v.val, 0);

    delete futil;
}

TEST_F(WindowFunnelTest, testInputNull) {
    FunctionUtils* futil = new FunctionUtils();
    doris_udf::FunctionContext* context = futil->get_fn_ctx();

    BigIntVal window(0);
    StringVal mode("default");
    std::vector<doris_udf::AnyVal*> constant_args;
    constant_args.emplace_back(&window);
    constant_args.emplace_back(&mode);
    context->impl()->set_constant_args(std::move(constant_args));

    StringVal stringVal1;
    AggregateFunctions::window_funnel_init(context, &stringVal1);

    DateTimeVal timestamp = DateTimeVal::null();
    BooleanVal conds[4] = {false, false, false, false};
    AggregateFunctions::window_funnel_update(context, window, mode, timestamp, 4,
                                      conds, &stringVal1);


    IntVal v = AggregateFunctions::window_funnel_finalize(context, stringVal1);
    ASSERT_EQ(v.val, 0);

    delete futil;
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
