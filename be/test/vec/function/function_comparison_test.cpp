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

#include <iostream>
#include <string>

#include "exec/schema_scanner.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {

TEST(ComparisonTest, ComparisonFunctionTest) {
    SchemaScanner::ColumnDesc column_descs[] = {{"k1", TYPE_SMALLINT, sizeof(int16_t), false},
                                                {"k2", TYPE_INT, sizeof(int32_t), false},
                                                {"k3", TYPE_DOUBLE, sizeof(double), false}};
    SchemaScanner schema_scanner(column_descs, 3);
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);

    auto tuple_desc = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc());
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr = MemTracker::CreateTracker(-1, "BlockTest", nullptr, false);
    RowBatch row_batch(row_desc, 1024, tracker_ptr.get());

    int16_t k1 = -100;
    int32_t k2 = 100;
    double k3 = 7.7;

    for (int i = 0; i < 1024; ++i, k1++, k2--, k3 += 0.1) {
        auto idx = row_batch.add_row();
        TupleRow* tuple_row = row_batch.get_row(idx);

        auto tuple = (Tuple*)(row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size()));
        auto slot_desc = tuple_desc->slots()[0];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k1, column_descs[0].size);
        slot_desc = tuple_desc->slots()[1];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k2, column_descs[1].size);
        slot_desc = tuple_desc->slots()[2];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k3, column_descs[2].size);

        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }

    vectorized::Block block = row_batch.convert_to_vec_block();
    // 1. compute the k1 > k2
    vectorized::ColumnNumbers arguments;
    arguments.emplace_back(block.get_position_by_name("k1"));
    arguments.emplace_back(block.get_position_by_name("k2"));

    size_t num_columns_without_result = block.columns();
    block.insert({nullptr, std::make_shared<vectorized::DataTypeUInt8>(), "k1 > k2"});

    vectorized::ColumnsWithTypeAndName ctn = {block.get_by_position(arguments[0]),
                                              block.get_by_position(arguments[1])};

    auto greater_function_ptr = vectorized::SimpleFunctionFactory::instance().get_function(
            "gt", ctn, std::make_shared<vectorized::DataTypeUInt8>());
    greater_function_ptr->execute(nullptr, block, arguments, num_columns_without_result, 1024,
                                  false);

    k1 = -100;
    k2 = 100;
    for (int i = 0; i < 1024; ++i, k1++, k2--) {
        vectorized::ColumnPtr column = block.get_columns()[3];
        ASSERT_EQ(column->get_bool(i), k1 > k2);
    }

    // 2. compute the k2 <= k3
    num_columns_without_result = block.columns();
    block.insert({nullptr, std::make_shared<vectorized::DataTypeUInt8>(), "k2 <= k3"});

    auto less_or_equals_function_ptr = vectorized::SimpleFunctionFactory::instance().get_function(
            "le", ctn, std::make_shared<vectorized::DataTypeUInt8>());

    arguments[0] = 1;
    arguments[1] = 2;
    less_or_equals_function_ptr->execute(nullptr, block, arguments, num_columns_without_result,
                                         1024, false);

    k2 = 100;
    k3 = 7.7;
    for (int i = 0; i < 1024; ++i, k3 += 0.1, k2--) {
        vectorized::ColumnPtr column = block.get_columns()[4];
        ASSERT_EQ(column->get_bool(i), k2 <= k3);
    }

    num_columns_without_result = block.columns();
    block.insert({nullptr, std::make_shared<vectorized::DataTypeUInt8>(), "k1 > k2 and k2 <= k3"});
    arguments[0] = 3;
    arguments[1] = 4;

    vectorized::ColumnsWithTypeAndName ctn2 = {block.get_by_position(arguments[0]),
                                               block.get_by_position(arguments[1])};
    auto and_function_ptr = vectorized::SimpleFunctionFactory::instance().get_function(
            "and", ctn2, std::make_shared<vectorized::DataTypeUInt8>());
    and_function_ptr->execute(nullptr, block, arguments, num_columns_without_result, 1024, false);

    k1 = -100;
    k2 = 100;
    k3 = 7.7;
    for (int i = 0; i < 1024; ++i, k1++, k3 += 0.1, k2--) {
        vectorized::ColumnPtr column = block.get_columns()[5];
        ASSERT_EQ(column->get_bool(i), k1 > k2 and k2 <= k3);
    }

    num_columns_without_result = block.columns();
    block.insert({nullptr, std::make_shared<vectorized::DataTypeUInt8>(), "k1 > k2 or k2 <= k3"});
    arguments[0] = 3;
    arguments[1] = 4;

    // vectorized::ColumnsWithTypeAndName ctn2 = { block.get_by_position(arguments[0]), block.get_by_position(arguments[1]) };
    auto or_function_ptr = vectorized::SimpleFunctionFactory::instance().get_function(
            "or", ctn2, std::make_shared<vectorized::DataTypeUInt8>());
    or_function_ptr->execute(nullptr, block, arguments, num_columns_without_result, 1024, false);

    k1 = -100;
    k2 = 100;
    k3 = 7.7;
    for (int i = 0; i < 1024; ++i, k1++, k3 += 0.1, k2--) {
        vectorized::ColumnPtr column = block.get_columns()[6];
        ASSERT_EQ(column->get_bool(i), k1 > k2 or k2 <= k3);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
