
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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>
#include <ostream>

#include "common/logging.h"
#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"

namespace doris {
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

void register_aggregate_function_retention(AggregateFunctionSimpleFactory& factory);

class VRetentionTest : public testing::Test {
public:
    AggregateFunctionPtr agg_function;

    VRetentionTest() {}

    void SetUp() {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        DataTypes data_types = {
                std::make_shared<DataTypeUInt8>(),
                std::make_shared<DataTypeUInt8>(),
                std::make_shared<DataTypeUInt8>(),
        };
        agg_function = factory.get("retention", data_types, false, -1);
        EXPECT_NE(agg_function, nullptr);
    }

    void TearDown() {}
};

TEST_F(VRetentionTest, testEmpty) {
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
    auto column_result =
            ColumnArray::create(((DataTypePtr)std::make_shared<DataTypeUInt8>())->create_column());
    agg_function->insert_result_into(place, *column_result);
    auto& result = assert_cast<ColumnUInt8&>(assert_cast<ColumnArray&>(*column_result).get_data())
                           .get_data();
    for (int i = 0; i < result.size(); i++) {
        EXPECT_EQ(result[i], 0);
    }

    auto column_result2 =
            ColumnArray::create(((DataTypePtr)std::make_shared<DataTypeUInt8>())->create_column());
    agg_function->insert_result_into(place2, *column_result2);
    auto& result2 = assert_cast<ColumnUInt8&>(assert_cast<ColumnArray&>(*column_result2).get_data())
                            .get_data();
    for (int i = 0; i < result2.size(); i++) {
        EXPECT_EQ(result2[i], 0);
    }

    EXPECT_EQ(column_result2->get_offsets()[-1], 0);
    EXPECT_EQ(column_result2->get_offsets()[0], 3);
    EXPECT_EQ(column_result2->get_offsets().size(), 1);
    agg_function->destroy(place);
    agg_function->destroy(place2);
}

TEST_F(VRetentionTest, testSample) {
    const int batch_size = 4;

    auto column_event1 = ColumnVector<UInt8>::create();
    column_event1->insert(0);
    column_event1->insert(1);
    column_event1->insert(0);
    column_event1->insert(0);

    auto column_event2 = ColumnVector<UInt8>::create();
    column_event2->insert(0);
    column_event2->insert(0);
    column_event2->insert(1);
    column_event2->insert(0);

    auto column_event3 = ColumnVector<UInt8>::create();
    column_event3->insert(0);
    column_event3->insert(0);
    column_event3->insert(0);
    column_event3->insert(1);

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    const IColumn* column[3] = {column_event1.get(), column_event2.get(), column_event3.get()};
    for (int i = 0; i < batch_size; i++) {
        agg_function->add(place, column, i, nullptr);
    }

    std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
    AggregateDataPtr place2 = memory2.get();
    agg_function->create(place2);

    agg_function->merge(place2, place, nullptr);

    auto column_result2 =
            ColumnArray::create(((DataTypePtr)std::make_shared<DataTypeUInt8>())->create_column());
    agg_function->insert_result_into(place2, *column_result2);
    auto& result2 = assert_cast<ColumnUInt8&>(assert_cast<ColumnArray&>(*column_result2).get_data())
                            .get_data();
    for (int i = 0; i < result2.size(); i++) {
        EXPECT_EQ(result2[i], 1);
    }

    EXPECT_EQ(column_result2->get_offsets()[-1], 0);
    EXPECT_EQ(column_result2->get_offsets()[0], 3);
    EXPECT_EQ(column_result2->get_offsets().size(), 1);
    agg_function->destroy(place2);
}

TEST_F(VRetentionTest, testNoMerge) {
    const int batch_size = 4;

    auto column_event1 = ColumnVector<UInt8>::create();
    column_event1->insert(0);
    column_event1->insert(1);
    column_event1->insert(0);
    column_event1->insert(0);

    auto column_event2 = ColumnVector<UInt8>::create();
    column_event2->insert(0);
    column_event2->insert(0);
    column_event2->insert(1);
    column_event2->insert(0);

    auto column_event3 = ColumnVector<UInt8>::create();
    column_event3->insert(0);
    column_event3->insert(0);
    column_event3->insert(0);
    column_event3->insert(1);

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    const IColumn* column[3] = {column_event1.get(), column_event2.get(), column_event3.get()};
    for (int i = 0; i < batch_size; i++) {
        agg_function->add(place, column, i, nullptr);
    }

    auto column_result =
            ColumnArray::create(((DataTypePtr)std::make_shared<DataTypeUInt8>())->create_column());
    agg_function->insert_result_into(place, *column_result);
    auto& result = assert_cast<ColumnUInt8&>(assert_cast<ColumnArray&>(*column_result).get_data())
                           .get_data();
    for (int i = 0; i < result.size(); i++) {
        EXPECT_EQ(result[i], 1);
    }
    EXPECT_EQ(column_result->get_offsets()[-1], 0);
    EXPECT_EQ(column_result->get_offsets()[0], 3);
    EXPECT_EQ(column_result->get_offsets().size(), 1);
    agg_function->destroy(place);
}

TEST_F(VRetentionTest, testSerialize) {
    const int batch_size = 2;

    auto column_event1 = ColumnVector<UInt8>::create();
    column_event1->insert(0);
    column_event1->insert(1);

    auto column_event2 = ColumnVector<UInt8>::create();
    column_event2->insert(0);
    column_event2->insert(0);

    auto column_event3 = ColumnVector<UInt8>::create();
    column_event3->insert(0);
    column_event3->insert(0);

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    const IColumn* column[3] = {column_event1.get(), column_event2.get(), column_event3.get()};
    for (int i = 0; i < batch_size; i++) {
        agg_function->add(place, column, i, nullptr);
    }

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

    auto column_result =
            ColumnArray::create(((DataTypePtr)std::make_shared<DataTypeUInt8>())->create_column());
    agg_function->insert_result_into(place2, *column_result);
    auto& result = assert_cast<ColumnUInt8&>(assert_cast<ColumnArray&>(*column_result).get_data())
                           .get_data();
    for (int i = 0; i < result.size(); i++) {
        if (i == 0) {
            EXPECT_EQ(result[i], 1);
        } else {
            EXPECT_EQ(result[i], 0);
        }
    }

    auto column_event4 = ColumnVector<UInt8>::create();
    column_event4->insert(0);
    column_event4->insert(0);

    auto column_event5 = ColumnVector<UInt8>::create();
    column_event5->insert(0);
    column_event5->insert(1);

    auto column_event6 = ColumnVector<UInt8>::create();
    column_event6->insert(0);
    column_event6->insert(0);

    std::unique_ptr<char[]> memory3(new char[agg_function->size_of_data()]);
    AggregateDataPtr place3 = memory3.get();
    agg_function->create(place3);
    const IColumn* column2[3] = {column_event4.get(), column_event5.get(), column_event6.get()};
    for (int i = 0; i < batch_size; i++) {
        agg_function->add(place3, column2, i, nullptr);
    }

    agg_function->merge(place2, place3, nullptr);

    auto column_result2 =
            ColumnArray::create(((DataTypePtr)std::make_shared<DataTypeUInt8>())->create_column());
    agg_function->insert_result_into(place2, *column_result2);
    auto& result2 = assert_cast<ColumnUInt8&>(assert_cast<ColumnArray&>(*column_result2).get_data())
                            .get_data();
    for (int i = 0; i < result2.size(); i++) {
        if (i == result2.size() - 1) {
            EXPECT_EQ(result2[i], 0);
        } else {
            EXPECT_EQ(result2[i], 1);
        }
    }

    EXPECT_EQ(column_result2->get_offsets()[-1], 0);
    EXPECT_EQ(column_result2->get_offsets()[0], 3);
    EXPECT_EQ(column_result2->get_offsets().size(), 1);

    agg_function->destroy(place2);
    agg_function->destroy(place3);
}
} // namespace doris::vectorized
