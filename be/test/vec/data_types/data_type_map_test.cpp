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

#include "vec/data_types/data_type_map.h"

#include <execinfo.h> // for backtrace on Linux
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <iostream>

#include "common/exception.h"
#include "vec/columns/column.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_struct.h"
#include "vec/function/function_test_util.h"

namespace doris::vectorized {

// TODO `DataTypeMapSerDe::deserialize_one_cell_from_json` has a bug,
// `SerdeArrowTest` cannot test Map type nested Array and Struct and Map,
// so manually construct data to test them.
// Expect to delete this TEST after `deserialize_one_cell_from_json` is fixed.
TEST(DataTypeMapTest, SerdeNestedTypeArrowTest) {
    auto block = std::make_shared<Block>();
    {
        std::string col_name = "map_nesting_array";
        DataTypePtr f1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr f2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        DataTypePtr dt1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(f1));
        DataTypePtr dt2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(f2));
        DataTypePtr ma = std::make_shared<DataTypeMap>(dt1, dt2);

        Array a1, a2, a3, a4;
        a1.push_back(Field("cute"));
        a1.push_back(Null());
        a2.push_back(Field("clever"));
        a1.push_back(Field("hello"));
        a3.push_back(1);
        a3.push_back(2);
        a4.push_back(11);
        a4.push_back(22);

        Array k1, v1;
        k1.push_back(a1);
        k1.push_back(a2);
        v1.push_back(a3);
        v1.push_back(a4);

        Map m1;
        m1.push_back(k1);
        m1.push_back(v1);

        MutableColumnPtr map_column = ma->create_column();
        map_column->reserve(1);
        map_column->insert(m1);
        vectorized::ColumnWithTypeAndName type_and_name(map_column->get_ptr(), ma, col_name);
        block->insert(type_and_name);
    }
    {
        std::string col_name = "map_nesting_struct";
        DataTypePtr f1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr f2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
        DataTypePtr f3 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
        DataTypePtr f4 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr dt1 = std::make_shared<DataTypeNullable>(
                std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {f1, f2, f3}));
        DataTypePtr dt2 = std::make_shared<DataTypeNullable>(
                std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {f4}));
        DataTypePtr ma = std::make_shared<DataTypeMap>(dt1, dt2);

        Tuple t1, t2, t3, t4;
        t1.push_back(Field("clever"));
        t1.push_back(__int128_t(37));
        t1.push_back(true);
        t2.push_back("null");
        t2.push_back(__int128_t(26));
        t2.push_back(false);
        t3.push_back(Field("cute"));
        t4.push_back("null");

        Array k1, v1;
        k1.push_back(t1);
        k1.push_back(t2);
        v1.push_back(t3);
        v1.push_back(t4);

        Map m1;
        m1.push_back(k1);
        m1.push_back(v1);

        MutableColumnPtr map_column = ma->create_column();
        map_column->reserve(1);
        map_column->insert(m1);
        vectorized::ColumnWithTypeAndName type_and_name(map_column->get_ptr(), ma, col_name);
        block->insert(type_and_name);
    }
    {
        std::string col_name = "map_nesting_map";
        DataTypePtr f1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        DataTypePtr f2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr f3 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
        DataTypePtr f4 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
        DataTypePtr dt1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(f1, f2));
        DataTypePtr dt2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(f3, f4));
        DataTypePtr ma = std::make_shared<DataTypeMap>(dt1, dt2);

        Array k1, k2, k3, k4, v1, v2, v3, v4;
        k1.push_back(1);
        k1.push_back(2);
        k2.push_back(11);
        k2.push_back(22);
        v1.push_back(Field("map"));
        v1.push_back(Null());
        v2.push_back(Field("clever map"));
        v2.push_back(Field("hello map"));
        k3.push_back(__int128_t(37));
        k3.push_back(__int128_t(26));
        k4.push_back(__int128_t(1111));
        k4.push_back(__int128_t(432535423));
        v3.push_back(true);
        v3.push_back(false);
        v4.push_back(false);
        v4.push_back(true);

        Map m11, m12, m21, m22;
        m11.push_back(k1);
        m11.push_back(v1);
        m12.push_back(k2);
        m12.push_back(v2);
        m21.push_back(k3);
        m21.push_back(v3);
        m22.push_back(k4);
        m22.push_back(v4);

        Array kk1, vv1;
        kk1.push_back(m11);
        kk1.push_back(m12);
        vv1.push_back(m21);
        vv1.push_back(m22);

        Map m1;
        m1.push_back(kk1);
        m1.push_back(vv1);

        MutableColumnPtr map_column = ma->create_column();
        map_column->reserve(1);
        map_column->insert(m1);
        vectorized::ColumnWithTypeAndName type_and_name(map_column->get_ptr(), ma, col_name);
        block->insert(type_and_name);
    }
    std::shared_ptr<arrow::RecordBatch> record_batch =
            CommonDataTypeSerdeTest::serialize_arrow(block);
    auto assert_block = std::make_shared<Block>(block->clone_empty());
    CommonDataTypeSerdeTest::deserialize_arrow(assert_block, record_batch);
    CommonDataTypeSerdeTest::compare_two_blocks(block, assert_block);
}

} // namespace doris::vectorized
