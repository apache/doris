
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
            std::make_shared<DataTypeDecimal<vectorized::Decimal128>>(27, 9));
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

TEST(HashFuncTest, ArrayTypeTest) {
    DataTypes dataTypes = create_scala_data_types();

    std::vector<uint64_t> sip_hash_vals(1);
    std::vector<uint64_t> xx_hash_vals(1);
    std::vector<uint64_t> crc_hash_vals(1);
    auto* __restrict sip_hashes = sip_hash_vals.data();
    auto* __restrict xx_hashes = xx_hash_vals.data();
    auto* __restrict crc_hashes = crc_hash_vals.data();

    for (auto d : dataTypes) {
        DataTypePtr a = std::make_shared<DataTypeArray>(d);
        ColumnPtr col_a = a->create_column_const_with_default_value(1);
        // sipHash
        std::vector<SipHash> siphashs(1);
        col_a->update_hashes_with_value(siphashs);
        EXPECT_NO_FATAL_FAILURE(col_a->update_hashes_with_value(siphashs));
        sip_hashes[0] = siphashs[0].get64();
        std::cout << sip_hashes[0] << std::endl;
        // xxHash
        EXPECT_NO_FATAL_FAILURE(col_a->update_hashes_with_value(xx_hashes));
        std::cout << xx_hashes[0] << std::endl;
        // crcHash
        EXPECT_NO_FATAL_FAILURE(
                col_a->update_crcs_with_value(crc_hash_vals, PrimitiveType::TYPE_ARRAY));
        std::cout << crc_hashes[0] << std::endl;
    }
}

TEST(HashFuncTest, MapTypeTest) {
    DataTypes dataTypes = create_scala_data_types();

    std::vector<uint64_t> sip_hash_vals(1);
    std::vector<uint64_t> xx_hash_vals(1);
    std::vector<uint64_t> crc_hash_vals(1);
    auto* __restrict sip_hashes = sip_hash_vals.data();
    auto* __restrict xx_hashes = xx_hash_vals.data();
    auto* __restrict crc_hashes = crc_hash_vals.data();
    // data_type_map
    for (int i = 0; i < dataTypes.size() - 1; ++i) {
        DataTypePtr a = std::make_shared<DataTypeMap>(dataTypes[i], dataTypes[i + 1]);
        ColumnPtr col_a = a->create_column_const_with_default_value(1);
        // sipHash
        std::vector<SipHash> siphashs(1);
        EXPECT_NO_FATAL_FAILURE(unpack_if_const(col_a).first->update_hashes_with_value(siphashs));
        sip_hashes[0] = siphashs[0].get64();
        std::cout << sip_hashes[0] << std::endl;
        // xxHash
        EXPECT_NO_FATAL_FAILURE(unpack_if_const(col_a).first->update_hashes_with_value(xx_hashes));
        std::cout << xx_hashes[0] << std::endl;
        // crcHash
        EXPECT_NO_FATAL_FAILURE(unpack_if_const(col_a).first->update_crcs_with_value(
                crc_hash_vals, PrimitiveType::TYPE_MAP));
        std::cout << crc_hashes[0] << std::endl;
    }
}

TEST(HashFuncTest, StructTypeTest) {
    DataTypes dataTypes = create_scala_data_types();

    std::vector<uint64_t> sip_hash_vals(1);
    std::vector<uint64_t> xx_hash_vals(1);
    std::vector<uint64_t> crc_hash_vals(1);
    auto* __restrict sip_hashes = sip_hash_vals.data();
    auto* __restrict xx_hashes = xx_hash_vals.data();
    auto* __restrict crc_hashes = crc_hash_vals.data();

    // data_type_struct
    DataTypePtr a = std::make_shared<DataTypeStruct>(dataTypes);
    ColumnPtr col_a = a->create_column_const_with_default_value(1);
    // sipHash
    std::vector<SipHash> siphashs(1);
    EXPECT_NO_FATAL_FAILURE(unpack_if_const(col_a).first->update_hashes_with_value(siphashs));
    sip_hashes[0] = siphashs[0].get64();
    std::cout << sip_hashes[0] << std::endl;
    // xxHash
    EXPECT_NO_FATAL_FAILURE(unpack_if_const(col_a).first->update_hashes_with_value(xx_hashes));
    std::cout << xx_hashes[0] << std::endl;
    // crcHash
    EXPECT_NO_FATAL_FAILURE(unpack_if_const(col_a).first->update_crcs_with_value(
            crc_hash_vals, PrimitiveType::TYPE_STRUCT));
    std::cout << crc_hashes[0] << std::endl;
}

} // namespace doris::vectorized
