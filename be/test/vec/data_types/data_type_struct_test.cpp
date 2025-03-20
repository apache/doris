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

#include "vec/data_types/data_type_struct.h"

#include <execinfo.h> // for backtrace on Linux
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <iostream>

#include "vec/columns/column.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/function/function_test_util.h"

namespace doris::vectorized {

// TODO `DataTypeStructSerDe::deserialize_one_cell_from_json` has a bug,
// `SerdeArrowTest` cannot test Struct type nested Array and Map and Struct,
// so manually construct data to test them.
// Expect to delete this TEST after `deserialize_one_cell_from_json` is fixed.
TEST(DataTypeStructTest, SerdeNestedTypeArrowTest) {
    auto block = std::make_shared<Block>();
    {
        std::string col_name = "struct_nesting_array_map_struct";
        DataTypePtr f1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr f2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        DataTypePtr f3 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr f4 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr f5 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
        DataTypePtr f6 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
        DataTypePtr dt1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(f1));
        DataTypePtr dt2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(f2, f3));
        DataTypePtr dt3 = std::make_shared<DataTypeNullable>(
                std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {f4, f5, f6}));
        DataTypePtr st = std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {dt1, dt2, dt3});

        // nested Array
        Array a1, a2;
        a1.push_back(Field("array"));
        a1.push_back(Null());
        a2.push_back(Field("lucky array"));
        a2.push_back(Field("cute array"));

        // nested Map
        Array k1, k2, v1, v2;
        k1.push_back(1);
        k1.push_back(2);
        k2.push_back(11);
        k2.push_back(22);
        v1.push_back(Field("map"));
        v1.push_back(Null());
        v2.push_back(Field("clever map"));
        v2.push_back(Field("hello map"));

        Map m1, m2;
        m1.push_back(k1);
        m1.push_back(v1);
        m2.push_back(k2);
        m2.push_back(v2);

        // nested Struct
        Tuple t1, t2;
        t1.push_back(Field("clever"));
        t1.push_back(__int128_t(37));
        t1.push_back(true);
        t2.push_back("null");
        t2.push_back(__int128_t(26));
        t2.push_back(false);

        // Struct
        Tuple tt1, tt2;
        tt1.push_back(a1);
        tt1.push_back(m1);
        tt1.push_back(t1);
        tt2.push_back(a2);
        tt2.push_back(m2);
        tt2.push_back(t2);

        MutableColumnPtr struct_column = st->create_column();
        struct_column->reserve(2);
        struct_column->insert(tt1);
        struct_column->insert(tt2);
        vectorized::ColumnWithTypeAndName type_and_name(struct_column->get_ptr(), st, col_name);
        block->insert(type_and_name);
    }
    std::shared_ptr<arrow::RecordBatch> record_batch =
            CommonDataTypeSerdeTest::serialize_arrow(block);
    auto assert_block = std::make_shared<Block>(block->clone_empty());
    CommonDataTypeSerdeTest::deserialize_arrow(assert_block, record_batch);
    CommonDataTypeSerdeTest::compare_two_blocks(block, assert_block);
}

} // namespace doris::vectorized
