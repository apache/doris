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

#include <cmath>
#include <memory>
#include <string>

#include "core/arena.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

void register_aggregate_function_uniq_theta(AggregateFunctionSimpleFactory& factory);

class VAggUniqThetaTest : public testing::Test {
public:
    void SetUp() override {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        register_aggregate_function_uniq_theta(factory);
    }
    void TearDown() override {}

    AggregateFunctionPtr get_int64_fn() {
        DataTypes types = {std::make_shared<DataTypeInt64>()};
        auto fn = AggregateFunctionSimpleFactory::instance().get("uniq_theta", types, nullptr,
                                                                 false, -1);
        EXPECT_NE(fn, nullptr);
        return fn;
    }

    // Build a place holding the int64 values [begin, end).
    struct Place {
        AggregateFunctionPtr fn;
        std::unique_ptr<char[]> mem;
        AggregateDataPtr ptr = nullptr;
        Place(AggregateFunctionPtr f) : fn(std::move(f)) {
            mem.reset(new char[fn->size_of_data()]);
            ptr = mem.get();
            fn->create(ptr);
        }
        ~Place() { fn->destroy(ptr); }
    };

    void add_int64_range(const AggregateFunctionPtr& fn, AggregateDataPtr place, int64_t begin,
                         int64_t end) {
        auto col = ColumnInt64::create();
        for (int64_t v = begin; v < end; ++v) {
            col->insert_value(v);
        }
        const IColumn* cols[1] = {col.get()};
        for (size_t i = 0; i < col->size(); ++i) {
            fn->add(place, cols, i, _arena);
        }
    }

    int64_t result_of(const AggregateFunctionPtr& fn, AggregateDataPtr place) {
        auto out = ColumnInt64::create();
        fn->insert_result_into(place, *out);
        EXPECT_EQ(out->size(), 1);
        return out->get_data()[0];
    }

    Arena _arena;
};

TEST_F(VAggUniqThetaTest, empty_state) {
    auto fn = get_int64_fn();
    Place p(fn);
    EXPECT_EQ(result_of(fn, p.ptr), 0);
}

TEST_F(VAggUniqThetaTest, small_exact) {
    auto fn = get_int64_fn();
    Place p(fn);
    add_int64_range(fn, p.ptr, 1, 6); // 1,2,3,4,5
    EXPECT_EQ(result_of(fn, p.ptr), 5);
}

TEST_F(VAggUniqThetaTest, duplicates_deduped) {
    auto fn = get_int64_fn();
    Place p(fn);
    auto col = ColumnInt64::create();
    for (int64_t v : {1, 1, 2, 2, 3}) {
        col->insert_value(v);
    }
    const IColumn* cols[1] = {col.get()};
    for (size_t i = 0; i < col->size(); ++i) {
        fn->add(p.ptr, cols, i, _arena);
    }
    EXPECT_EQ(result_of(fn, p.ptr), 3);
}

TEST_F(VAggUniqThetaTest, large_cardinality_within_error_bound) {
    auto fn = get_int64_fn();
    Place p(fn);
    const int64_t n = 100000;
    add_int64_range(fn, p.ptr, 0, n);
    int64_t est = result_of(fn, p.ptr);
    // Theta sketch relative error ~3.125% at 95% confidence; allow a small margin.
    EXPECT_LE(std::abs(est - n), static_cast<int64_t>(n * 0.04));
}

TEST_F(VAggUniqThetaTest, serialize_deserialize_roundtrip) {
    auto fn = get_int64_fn();
    Place p(fn);
    add_int64_range(fn, p.ptr, 0, 10);
    int64_t before = result_of(fn, p.ptr);

    ColumnString buf;
    VectorBufferWriter writer(buf);
    fn->serialize(p.ptr, writer);
    writer.commit();

    Place p2(fn);
    VectorBufferReader reader(buf.get_data_at(0));
    fn->deserialize(p2.ptr, reader, _arena);
    EXPECT_EQ(result_of(fn, p2.ptr), before);
}

TEST_F(VAggUniqThetaTest, merge_disjoint) {
    auto fn = get_int64_fn();
    Place a(fn);
    Place b(fn);
    add_int64_range(fn, a.ptr, 1, 4); // 1,2,3
    add_int64_range(fn, b.ptr, 4, 7); // 4,5,6
    fn->merge(a.ptr, b.ptr, _arena);
    EXPECT_EQ(result_of(fn, a.ptr), 6);
}

TEST_F(VAggUniqThetaTest, merge_overlapping) {
    auto fn = get_int64_fn();
    Place a(fn);
    Place b(fn);
    add_int64_range(fn, a.ptr, 1, 5); // 1,2,3,4
    add_int64_range(fn, b.ptr, 3, 7); // 3,4,5,6
    fn->merge(a.ptr, b.ptr, _arena);
    EXPECT_EQ(result_of(fn, a.ptr), 6);
}

TEST_F(VAggUniqThetaTest, add_after_merge_not_lost) {
    auto fn = get_int64_fn();
    Place a(fn);
    Place b(fn);
    add_int64_range(fn, a.ptr, 1, 4); // 1,2,3
    add_int64_range(fn, b.ptr, 4, 7); // 4,5,6 -> forces sk_union on a
    fn->merge(a.ptr, b.ptr, _arena);
    add_int64_range(fn, a.ptr, 7, 9); // 7,8 added after union exists
    EXPECT_EQ(result_of(fn, a.ptr), 8);
}

TEST_F(VAggUniqThetaTest, string_type) {
    DataTypes types = {std::make_shared<DataTypeString>()};
    auto fn = AggregateFunctionSimpleFactory::instance().get("uniq_theta", types, nullptr, false,
                                                             -1);
    ASSERT_NE(fn, nullptr);
    std::unique_ptr<char[]> mem(new char[fn->size_of_data()]);
    AggregateDataPtr place = mem.get();
    fn->create(place);

    auto col = ColumnString::create();
    for (const auto* s : {"a", "b", "c", "a", "d"}) {
        col->insert_data(s, strlen(s));
    }
    const IColumn* cols[1] = {col.get()};
    for (size_t i = 0; i < col->size(); ++i) {
        fn->add(place, cols, i, _arena);
    }
    auto out = ColumnInt64::create();
    fn->insert_result_into(place, *out);
    EXPECT_EQ(out->get_data()[0], 4);
    fn->destroy(place);
}

TEST_F(VAggUniqThetaTest, streaming_state_roundtrip) {
    // Exercises the _state path: one serialized sketch per row, then merge them all.
    auto fn = get_int64_fn();
    auto col = ColumnInt64::create();
    for (int64_t v : {10, 20, 20, 30, 40}) {
        col->insert_value(v);
    }
    const IColumn* cols[1] = {col.get()};
    MutableColumnPtr dst = ColumnString::create();
    fn->streaming_agg_serialize_to_column(cols, dst, col->size(), _arena);
    EXPECT_EQ(dst->size(), col->size());

    Place merged(fn);
    fn->deserialize_and_merge_from_column(merged.ptr, *dst, _arena);
    EXPECT_EQ(result_of(fn, merged.ptr), 4);
}

} // namespace doris
