
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

#include "gtest/gtest_pred_impl.h"
#include "util/runtime_profile.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized {

DataTypes create_scala_data_types() {
    DataTypePtr dt = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>());
    DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDate>());
    DataTypePtr dc = std::make_shared<DataTypeNullable>(vectorized::create_decimal(10, 2, false));
    DataTypePtr dcv2 = std::make_shared<DataTypeNullable>(
            std::make_shared<DataTypeDecimal<vectorized::Decimal128V2>>(27, 9));
    DataTypePtr n3 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
    DataTypePtr n1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    DataTypePtr s1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());

    DataTypes dataTypes;
    dataTypes.push_back(dt);
    dataTypes.push_back(d);
    dataTypes.push_back(dc);
    dataTypes.push_back(dcv2);
    dataTypes.push_back(n3);
    dataTypes.push_back(n1);
    dataTypes.push_back(s1);

    return dataTypes;
}

TEST(HashFuncTest, ArraySimpleBenchmarkTest) {
    DataTypes dataTypes = create_scala_data_types();

    DataTypePtr d = std::make_shared<DataTypeInt64>();
    DataTypePtr array_ptr = std::make_shared<DataTypeArray>(d);
    MutableColumnPtr array_mutable_col = array_ptr->create_column();

    int r_num = 50;
    for (int r = 0; r < r_num; ++r) {
        Array a;
        for (int i = 0; i < 10000; ++i) {
            a.push_back(Int64(i));
        }
        array_mutable_col->insert(a);
    }
    std::vector<uint32_t> crc_hash_vals(r_num);
    int64_t time_t = 0;
    {
        SCOPED_RAW_TIMER(&time_t);
        EXPECT_NO_FATAL_FAILURE(array_mutable_col->update_crcs_with_value(
                crc_hash_vals.data(), PrimitiveType::TYPE_ARRAY, r_num));
    }
    std::cout << time_t << "ns" << std::endl;
}

TEST(HashFuncTest, ArrayNestedArrayTest) {
    DataTypes dataTypes = create_scala_data_types();

    DataTypePtr d = std::make_shared<DataTypeInt64>();
    MutableColumnPtr scala_mutable_col = d->create_column();
    DataTypePtr nested_array_ptr = std::make_shared<DataTypeArray>(d);
    DataTypePtr array_ptr = std::make_shared<DataTypeArray>(nested_array_ptr);
    MutableColumnPtr array_mutable_col = array_ptr->create_column();

    Array a, a1, a2, a3, nested, nested1;
    nested.push_back(Int64(1));
    nested1.push_back(Int64(2));

    // a: [[1], [2]]
    a.push_back(nested);
    a.push_back(nested1);
    // a1: [[2], [1]]
    a1.push_back(nested1);
    a1.push_back(nested);

    // a2: [[], [1]]
    a2.push_back(Array());
    a2.push_back(nested);
    // a3: [[1], []]
    a3.push_back(nested);
    a3.push_back(Array());

    array_mutable_col->insert(a);
    array_mutable_col->insert(a1);
    array_mutable_col->insert(a2);
    array_mutable_col->insert(a3);

    auto nested_col =
            reinterpret_cast<vectorized::ColumnArray*>(array_mutable_col.get())->get_data_ptr();
    EXPECT_EQ(nested_col->size(), 8);

    std::vector<uint64_t> xx_hash_vals(4);
    std::vector<uint32_t> crc_hash_vals(4);
    auto* __restrict xx_hashes = xx_hash_vals.data();
    auto* __restrict crc_hashes = crc_hash_vals.data();

    // xxHash
    EXPECT_NO_FATAL_FAILURE(array_mutable_col->update_hashes_with_value(xx_hashes));
    EXPECT_TRUE(xx_hashes[0] != xx_hashes[1]);
    EXPECT_TRUE(xx_hashes[2] != xx_hashes[3]);
    // crcHash
    EXPECT_NO_FATAL_FAILURE(
            array_mutable_col->update_crcs_with_value(crc_hashes, PrimitiveType::TYPE_ARRAY, 4));
    EXPECT_TRUE(crc_hashes[0] != crc_hashes[1]);
    EXPECT_TRUE(crc_hashes[2] != crc_hashes[3]);
}

} // namespace doris::vectorized
