
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

#include <gen_cpp/types.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/hll.h"
#include "util/bitmap_value.h"
#include "util/quantile_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_quantilestate.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {

inline void column_to_pb(const DataTypePtr data_type, const IColumn& col, PValues* result) {
    const DataTypeSerDeSPtr serde = data_type->get_serde();
    Status st = serde->write_column_to_pb(col, *result, 0, col.size());
    if (!st.ok()) {
        std::cerr << "column_to_pb error, maybe not impl it: " << st.msg() << " "
                  << data_type->get_name() << std::endl;
    }
}

inline bool pb_to_column(const DataTypePtr data_type, PValues& result, IColumn& col) {
    auto serde = data_type->get_serde();
    Status st = serde->read_column_from_pb(col, result);
    if (!st.ok()) {
        std::cerr << "pb_to_column error, maybe not impl it: " << st.msg() << " "
                  << data_type->get_name() << std::endl;
        return false;
    }
    return true;
}

inline void check_pb_col(const DataTypePtr data_type, const IColumn& input_column) {
    PValues pv = PValues();
    column_to_pb(data_type, input_column, &pv);
    int s1 = pv.bytes_value_size();
    auto except_column = data_type->create_column();
    bool success_deserialized = pb_to_column(data_type, pv, *except_column);

    PValues as_pv = PValues();
    column_to_pb(data_type, *except_column, &as_pv);
    int s2 = as_pv.bytes_value_size();
    EXPECT_EQ(s1, s2);

    Block block_out, block_input;
    block_input.insert({input_column.get_ptr(), data_type, ""});
    std::string input_str = block_input.dump_data();
    block_out.insert({std::move(except_column), data_type, ""});
    std::string output_str = block_out.dump_data();
    //input column data should same as output column data of deserialize
    if (success_deserialized) {
        EXPECT_EQ(input_str, output_str);
    } else {
        EXPECT_TRUE(false);
    }
}
TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTest2) {
    std::cout << "==== double === " << std::endl;
    // double
    {
        auto vec = vectorized::ColumnFloat64::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 10; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeFloat64>());
        check_pb_col(data_type, *vec.get());
    }
}

TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTest3) {
    std::cout << "==== nullable_int32 === " << std::endl;
    // nullable_int
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto null_map = vectorized::ColumnVector<UInt8>::create();
        auto& data = vec->get_data();
        auto& null_map_data = null_map->get_data();

        for (int i = 0; i < 10; ++i) {
            data.push_back(i);
            null_map_data.push_back(i % 2);
        }
        auto nullable_column =
                vectorized::ColumnNullable::create(std::move(vec), std::move(null_map));
        vectorized::DataTypePtr data_type = std::make_shared<vectorized::DataTypeInt32>();
        vectorized::DataTypePtr nullable_type = make_nullable(data_type);

        check_pb_col(nullable_type, *nullable_column.get());
    }
}

TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTest4) {
    std::cout << "==== array<int32> === " << std::endl;
    // array<int32>
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto null_map = vectorized::ColumnVector<UInt8>::create();
        auto& data = vec->get_data();
        auto& null_map_data = null_map->get_data();
        int rows = 10;
        for (int i = 0; i < rows; ++i) {
            data.push_back(i);
            null_map_data.push_back(i % 2);
        }
        auto nullable_column =
                vectorized::ColumnNullable::create(std::move(vec), std::move(null_map));
        auto offset_column = vectorized::ColumnArray::ColumnOffsets::create();
        offset_column->get_data().push_back(3);
        offset_column->get_data().push_back(rows);
        /*
        +-------------------------------+
        |                   [0, NULL, 2]  |
        |[NULL, 4, NULL, 6, NULL, 8, NULL]|
        +-------------------------------+
        */
        auto array_column = vectorized::ColumnArray::create(std::move(nullable_column),
                                                            std::move(offset_column));

        vectorized::DataTypePtr data_type = std::make_shared<vectorized::DataTypeInt32>();
        vectorized::DataTypePtr nullable_type = make_nullable(data_type);
        vectorized::DataTypePtr array_type =
                std::make_shared<vectorized::DataTypeArray>(nullable_type);
        check_pb_col(array_type, *array_column.get());
    }
}

TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTest5) {
    std::cout << "==== array<array<int32>> === " << std::endl;
    // array<array<int32>>
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto null_map = vectorized::ColumnVector<UInt8>::create();
        auto& data = vec->get_data();
        auto& null_map_data = null_map->get_data();
        int rows = 10;
        for (int i = 0; i < rows; ++i) {
            data.push_back(i);
            null_map_data.push_back(i % 2);
        }
        auto nullable_column =
                vectorized::ColumnNullable::create(std::move(vec), std::move(null_map));
        auto offset_column = vectorized::ColumnArray::ColumnOffsets::create();
        offset_column->get_data().push_back(rows);
        //[0,1,2,3,.....9]
        auto array_column = vectorized::ColumnArray::create(std::move(nullable_column),
                                                            std::move(offset_column));
        vectorized::DataTypePtr data_type = std::make_shared<vectorized::DataTypeInt32>();
        vectorized::DataTypePtr nullable_type = make_nullable(data_type);
        vectorized::DataTypePtr array_type =
                std::make_shared<vectorized::DataTypeArray>(nullable_type);

        auto out_offset_column = vectorized::ColumnArray::ColumnOffsets::create();
        out_offset_column->get_data().push_back(1);
        vectorized::DataTypePtr out_array_type =
                std::make_shared<vectorized::DataTypeArray>(make_nullable(array_type));
        //[[0,1,2,3,.....9]]
        auto out_array_column = vectorized::ColumnArray::create(
                (make_nullable(std::move(array_column))), std::move(out_offset_column));
        check_pb_col(out_array_type, *out_array_column.get());
    }
}

TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTest6) {
    std::cout << "==== array<array<int32>> === " << std::endl;
    // array<array<int32>>
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto null_map = vectorized::ColumnVector<UInt8>::create();
        auto& data = vec->get_data();
        auto& null_map_data = null_map->get_data();
        int rows = 10;
        for (int i = 0; i < rows; ++i) {
            data.push_back(i);
            null_map_data.push_back(i % 2);
        }
        auto nullable_column =
                vectorized::ColumnNullable::create(std::move(vec), std::move(null_map));
        auto offset_column = vectorized::ColumnArray::ColumnOffsets::create();
        offset_column->get_data().push_back(4);
        offset_column->get_data().push_back(rows);
        /*
        +-------------------------------+
        |           [0, NULL, 2, NULL]  |
        |[ 4, NULL, 6, NULL, 8, NULL]   |
        +-------------------------------+
        */
        auto array_column = vectorized::ColumnArray::create(std::move(nullable_column),
                                                            std::move(offset_column));
        vectorized::DataTypePtr data_type = std::make_shared<vectorized::DataTypeInt32>();
        vectorized::DataTypePtr nullable_type = make_nullable(data_type);
        vectorized::DataTypePtr array_type =
                std::make_shared<vectorized::DataTypeArray>(nullable_type);
        auto out_offset_column = vectorized::ColumnArray::ColumnOffsets::create();
        //[[0, NULL, 2, NULL], [4, NULL, 6, NULL, 8, NULL]]
        out_offset_column->get_data().push_back(2);
        vectorized::DataTypePtr out_array_type =
                std::make_shared<vectorized::DataTypeArray>(make_nullable(array_type));
        auto null_array_column = make_nullable(std::move(array_column));

        auto out_array_column =
                vectorized::ColumnArray::create(null_array_column, std::move(out_offset_column));
        check_pb_col(out_array_type, *out_array_column.get());
    }
}

TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTest7) {
    std::cout << "==== array<array<int32>> === " << std::endl;
    // array<array<int32>>
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto null_map = vectorized::ColumnVector<UInt8>::create();
        auto& data = vec->get_data();
        auto& null_map_data = null_map->get_data();
        int rows = 10;
        for (int i = 0; i < rows; ++i) {
            data.push_back(i);
            null_map_data.push_back(i % 2);
        }
        auto nullable_column =
                vectorized::ColumnNullable::create(std::move(vec), std::move(null_map));
        auto offset_column = vectorized::ColumnArray::ColumnOffsets::create();
        offset_column->get_data().push_back(4);
        offset_column->get_data().push_back(rows);
        /*
        +-------------------------------+
        |           [0, NULL, 2, NULL]  |
        |[ 4, NULL, 6, NULL, 8, NULL]   |
        +-------------------------------+
        */
        auto array_column = vectorized::ColumnArray::create(std::move(nullable_column),
                                                            std::move(offset_column));
        vectorized::DataTypePtr data_type = std::make_shared<vectorized::DataTypeInt32>();
        vectorized::DataTypePtr nullable_type = make_nullable(data_type);
        vectorized::DataTypePtr array_type =
                std::make_shared<vectorized::DataTypeArray>(nullable_type);
        auto out_offset_column = vectorized::ColumnArray::ColumnOffsets::create();
        /* asd Array(Nullable(Array(Nullable(Int32)))) Array(size = 2, UInt64(size = 2), Nullable(size = 2, Array(size = 2, UInt64(size = 2), Nullable(size = 10, Int32(size = 10), UInt8(size = 10))), UInt8(size = 2)))
        +--------------------------------------------+
        |asd(Array(Nullable(Array(Nullable(Int32)))))|
        +--------------------------------------------+
        |                        [[0, NULL, 2, NULL]]|
        |               [[4, NULL, 6, NULL, 8, NULL]]|
        +--------------------------------------------+
        */
        out_offset_column->get_data().push_back(1);
        out_offset_column->get_data().push_back(2);
        vectorized::DataTypePtr out_array_type =
                std::make_shared<vectorized::DataTypeArray>(make_nullable(array_type));
        auto null_array_column = make_nullable(std::move(array_column));

        auto out_array_column =
                vectorized::ColumnArray::create(null_array_column, std::move(out_offset_column));
        check_pb_col(out_array_type, *out_array_column.get());
    }
}

inline void serialize_and_deserialize_pb_test() {
    std::cout << "==== int32 === " << std::endl;
    // int
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        check_pb_col(data_type, *vec.get());
    }
    std::cout << "==== string === " << std::endl;
    // string
    {
        auto strcol = vectorized::ColumnString::create();
        for (int i = 0; i < 1024; ++i) {
            std::string is = std::to_string(i);
            strcol->insert_data(is.c_str(), is.size());
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
        check_pb_col(data_type, *strcol.get());
    }
    std::cout << "==== decimal === " << std::endl;
    // decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
        auto decimal_column = decimal_data_type->create_column();
        auto& data = ((vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int128>>*)
                              decimal_column.get())
                             ->get_data();
        for (int i = 0; i < 1024; ++i) {
            __int128_t value = __int128_t(i * pow(10, 9) + i * pow(10, 8));
            data.push_back(value);
        }
        check_pb_col(decimal_data_type, *decimal_column.get());
    }
    // bitmap
    std::cout << "==== bitmap === " << std::endl;
    {
        vectorized::DataTypePtr bitmap_data_type(std::make_shared<vectorized::DataTypeBitMap>());
        auto bitmap_column = bitmap_data_type->create_column();
        std::vector<BitmapValue>& container =
                ((vectorized::ColumnBitmap*)bitmap_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            BitmapValue bv;
            for (int j = 0; j <= i; ++j) {
                bv.add(j);
            }
            container.push_back(bv);
        }
        check_pb_col(bitmap_data_type, *bitmap_column.get());
    }
    // hll
    std::cout << "==== hll === " << std::endl;
    {
        vectorized::DataTypePtr hll_data_type(std::make_shared<vectorized::DataTypeHLL>());
        auto hll_column = hll_data_type->create_column();
        std::vector<HyperLogLog>& container =
                ((vectorized::ColumnHLL*)hll_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            HyperLogLog hll;
            hll.update(i);
            container.push_back(hll);
        }
        check_pb_col(hll_data_type, *hll_column.get());
    }
    // quantilestate
    std::cout << "==== quantilestate === " << std::endl;
    {
        vectorized::DataTypePtr quantile_data_type(
                std::make_shared<vectorized::DataTypeQuantileState>());
        auto quantile_column = quantile_data_type->create_column();
        std::vector<QuantileState>& container =
                ((vectorized::ColumnQuantileState*)quantile_column.get())->get_data();
        const long max_rand = 1000000L;
        double lower_bound = 0;
        double upper_bound = 100;
        srandom(time(nullptr));
        for (int i = 0; i < 1024; ++i) {
            QuantileState q;
            double random_double =
                    lower_bound + (upper_bound - lower_bound) * (random() % max_rand) / max_rand;
            q.add_value(random_double);
            container.push_back(q);
        }
        check_pb_col(quantile_data_type, *quantile_column.get());
    }
    // nullable string
    std::cout << "==== nullable string === " << std::endl;
    {
        vectorized::DataTypePtr string_data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(string_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_null_elements(1024);
        check_pb_col(nullable_data_type, *nullable_column.get());
    }
    // nullable decimal
    std::cout << "==== nullable decimal === " << std::endl;
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(decimal_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_null_elements(1024);
        check_pb_col(nullable_data_type, *nullable_column.get());
    }
    // int with 1024 batch size
    std::cout << "==== int with 1024 batch size === " << std::endl;
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())
                ->insert_range_from_not_nullable(*vec, 0, 1024);
        check_pb_col(nullable_data_type, *nullable_column.get());
    }
    // ipv4
    std::cout << "==== ipv4 === " << std::endl;
    {
        auto vec = vectorized::ColumnVector<IPv4>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeIPv4>());
        check_pb_col(data_type, *vec.get());
    }
    // ipv6
    std::cout << "==== ipv6 === " << std::endl;
    {
        auto vec = vectorized::ColumnVector<IPv6>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeIPv6>());
        check_pb_col(data_type, *vec.get());
    }
}

TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTest) {
    serialize_and_deserialize_pb_test();
}

TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTestMap) {
    std::cout << "==== map<string, string> === " << std::endl;
    DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr m = std::make_shared<DataTypeMap>(s, d);
    Array k1, k2, v1, v2;
    k1.push_back("null");
    k1.push_back("doris");
    k1.push_back("clever amory");
    v1.push_back("ss");
    v1.push_back(Null());
    v1.push_back("NULL");
    k2.push_back("hello amory");
    k2.push_back("NULL");
    k2.push_back("cute amory");
    k2.push_back("doris");
    v2.push_back("s");
    v2.push_back("0");
    v2.push_back("sf");
    v2.push_back(Null());
    Map m1, m2;
    m1.push_back(k1);
    m1.push_back(v1);
    m2.push_back(k2);
    m2.push_back(v2);
    MutableColumnPtr map_column = m->create_column();
    map_column->reserve(2);
    map_column->insert(m1);
    map_column->insert(m2);
    /*
    +-----------------------------------------+
    |(Map(Nullable(String), Nullable(String)))                       |
    +-----------------------------------------+
    |{"null":"ss", "doris":null, "clever amory":"NULL"}              |
    |{"hello amory":"s", "NULL":"0", "cute amory":"sf", "doris":null}|
    +-----------------------------------------+
    */

    vectorized::ColumnWithTypeAndName type_and_name(map_column->get_ptr(), m, "");
    Block block;
    block.insert(type_and_name);
    check_pb_col(m, *map_column.get());
}

TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTestMap2) {
    std::cout << "==== map<string,map<string, string>> === " << std::endl;
    DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr m = std::make_shared<DataTypeMap>(s, d);
    Array k1, k2, v1, v2;
    k1.push_back("null");
    k1.push_back("doris");
    k1.push_back("clever amory");
    v1.push_back("ss");
    v1.push_back(Null());
    v1.push_back("NULL");
    k2.push_back("hello amory");
    k2.push_back("NULL");
    k2.push_back("cute amory");
    k2.push_back("doris");
    v2.push_back("s");
    v2.push_back("0");
    v2.push_back("sf");
    v2.push_back(Null());
    Map m1, m2;
    m1.push_back(k1);
    m1.push_back(v1);
    m2.push_back(k2);
    m2.push_back(v2);
    MutableColumnPtr map_column = m->create_column();
    map_column->reserve(2);
    map_column->insert(m1);
    map_column->insert(m2);
    /*
    +-----------------------------------------+
    |(Map(Nullable(String), Nullable(String)))                       |
    +-----------------------------------------+
    |{"null":"ss", "doris":null, "clever amory":"NULL"}              |
    |{"hello amory":"s", "NULL":"0", "cute amory":"sf", "doris":null}|
    +-----------------------------------------+
    */

    DataTypePtr outer_string_type =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto outer_string_key_column = outer_string_type->create_column();
    std::string str1 = "nested_complex_map_1";
    std::string str2 = "nested_complex_map_2";
    outer_string_key_column->insert_data(str1.c_str(), str1.size());
    outer_string_key_column->insert_data(str2.c_str(), str2.size());

    auto outer_offset_column = vectorized::ColumnArray::ColumnOffsets::create();
    outer_offset_column->get_data().push_back(1);
    outer_offset_column->get_data().push_back(2);
    DataTypePtr outer_type = std::make_shared<DataTypeMap>(s, m);

    /*
    outer_map_column: Map(Nullable(String), Map(Nullable(String), Nullable(String)))
    +----------------------------------------------------------------+
    |(Map(Nullable(String), Map(Nullable(String), Nullable(String))))|
    +----------------------------------------------------------------+
    |{"nested_complex_map_1":{"null":"ss", "doris":null, "clever amory":"NULL"}}|
    |{"nested_complex_map_2":{"hello amory":"s", "NULL":"0", "cute amory":"sf", "doris":null}}|
    +----------------------------------------------------------------+
    */
    auto outer_map_column =
            vectorized::ColumnMap::create(std::move(outer_string_key_column), std::move(map_column),
                                          std::move(outer_offset_column));

    vectorized::ColumnWithTypeAndName type_and_name_outer(outer_map_column->get_ptr(), outer_type,
                                                          "");
    Block block2;
    block2.insert(type_and_name_outer);
    check_pb_col(outer_type, *outer_map_column.get());
}

TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTestStruct) {
    std::cout << "==== struct<string, int64, uint8> === " << std::endl;
    DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
    DataTypePtr m = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
    DataTypePtr st = std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {s, d, m});
    Tuple t1, t2;
    t1.push_back(String("amory cute"));
    t1.push_back(__int128_t(37));
    t1.push_back(true);
    t2.push_back("null");
    t2.push_back(__int128_t(26));
    t2.push_back(false);
    MutableColumnPtr struct_column = st->create_column();
    struct_column->reserve(2);
    struct_column->insert(t1);
    struct_column->insert(t2);
    /*
    +-------------------------------------------------------------------+
    |(Struct(1:Nullable(String), 2:Nullable(Int128), 3:Nullable(UInt8)))|
    +-------------------------------------------------------------------+
    |                                                {amory cute, 37, 1}|
    |                                                      {null, 26, 0}|
    +-------------------------------------------------------------------+
    */
    vectorized::ColumnWithTypeAndName type_and_name(struct_column->get_ptr(), st, "");
    Block block;
    block.insert(type_and_name);
    check_pb_col(st, *struct_column.get());
}

TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTestStruct2) {
    std::cout << "==== struct<string,struct<string, int64, uint8>> === " << std::endl;
    DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    DataTypePtr m = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
    DataTypePtr st = std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {s, d, m});
    Tuple t1, t2;
    t1.push_back(String("amory cute"));
    t1.push_back(37);
    t1.push_back(true);
    t2.push_back("null");
    t2.push_back(26);
    t2.push_back(false);
    MutableColumnPtr struct_column = st->create_column();
    struct_column->reserve(2);
    struct_column->insert(t1);
    struct_column->insert(t2);

    DataTypePtr string_type =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr outer_struct =
            std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {string_type, st});
    auto outer_string_key_column = string_type->create_column();
    std::string str1 = "nested_complex_struct_1";
    std::string str2 = "nested_complex_struct_2";
    outer_string_key_column->insert_data(str1.c_str(), str1.size());
    outer_string_key_column->insert_data(str2.c_str(), str2.size());

    std::vector<ColumnPtr> vector_columns;
    vector_columns.emplace_back(outer_string_key_column->get_ptr());
    vector_columns.emplace_back(struct_column->get_ptr());
    auto outer_struct_column = ColumnStruct::create(vector_columns);
    /*
    +-------------------------------------------------------------------------------------------------+
    |(Struct(1:Nullable(String), 2:Struct(1:Nullable(String), 2:Nullable(Int128), 3:Nullable(UInt8))))|
    +-------------------------------------------------------------------------------------------------+
    |                                                   {nested_complex_struct_1, {amory cute, 37, 1}}|
    |                                                         {nested_complex_struct_2, {null, 26, 0}}|
    +-------------------------------------------------------------------------------------------------+
    */
    vectorized::ColumnWithTypeAndName type_and_name(outer_struct_column->get_ptr(), outer_struct,
                                                    "");
    Block block;
    block.insert(type_and_name);
    check_pb_col(outer_struct, *outer_struct_column.get());
}

TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTestDateTime) {
    std::cout << "==== datetime === " << std::endl;
    // datetime
    {
        auto vec = vectorized::ColumnDateTimeV2::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 10; ++i) {
            uint16_t year = 2022;
            uint8_t month = 5;
            uint8_t day = 24;
            uint8_t hour = 12;
            uint8_t minute = i;
            uint8_t second = 0;
            uint32_t microsecond = 123000;
            auto value = ((uint64_t)(((uint64_t)year << 46) | ((uint64_t)month << 42) |
                                     ((uint64_t)day << 37) | ((uint64_t)hour << 32) |
                                     ((uint64_t)minute << 26) | ((uint64_t)second << 20) |
                                     (uint64_t)microsecond));
            DateV2Value<DateTimeV2ValueType> datetime_v2;
            datetime_v2.from_datetime(value);
            auto datetime_val = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(datetime_v2);
            data.push_back(datetime_val);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeDateTimeV2>(6));
        vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "");
        Block block;
        block.insert(type_and_name);
        check_pb_col(data_type, *vec.get());
    }
}

TEST(DataTypeSerDePbTest, DataTypeScalaSerDeTestLargeInt) {
    std::cout << "==== LargeInt === " << std::endl;
    // LargeInt
    {
        auto vec = vectorized::ColumnVector<Int128>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 10; ++i) {
            data.push_back(500000000000 + i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt128>());
        vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "");
        Block block;
        block.insert(type_and_name);
        check_pb_col(data_type, *vec.get());
    }
}
} // namespace doris::vectorized