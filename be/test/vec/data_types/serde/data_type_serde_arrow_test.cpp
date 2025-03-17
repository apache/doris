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

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <arrow/visit_type_inline.h>
#include <arrow/visitor.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/types.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "olap/hll.h"
#include "runtime/descriptors.cpp"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "util/string_parser.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
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
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/arrow_column_to_doris_column.h"

namespace doris::vectorized {

void serialize_and_deserialize_arrow_test(std::vector<PrimitiveType> cols, int row_num,
                                          bool is_nullable) {
    auto block = std::make_shared<Block>();
    for (int i = 0; i < cols.size(); i++) {
        std::string col_name = std::to_string(i);
        TypeDescriptor type_desc(cols[i]);
        switch (cols[i]) {
        case TYPE_BOOLEAN: {
            auto vec = vectorized::ColumnVector<UInt8>::create();
            auto& data = vec->get_data();
            for (int i = 0; i < row_num; ++i) {
                data.push_back(i % 2);
            }
            vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeUInt8>());
            vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, col_name);
            block->insert(std::move(type_and_name));
        } break;
        case TYPE_INT:
            if (is_nullable) {
                {
                    auto column_vector_int32 = vectorized::ColumnVector<Int32>::create();
                    auto column_nullable_vector =
                            vectorized::make_nullable(std::move(column_vector_int32));
                    auto mutable_nullable_vector = std::move(*column_nullable_vector).mutate();
                    for (int i = 0; i < row_num; i++) {
                        if (i % 2 == 0) {
                            mutable_nullable_vector->insert_default();
                        } else {
                            mutable_nullable_vector->insert(int32(i));
                        }
                    }
                    auto data_type = vectorized::make_nullable(
                            std::make_shared<vectorized::DataTypeInt32>());
                    vectorized::ColumnWithTypeAndName type_and_name(
                            mutable_nullable_vector->get_ptr(), data_type, col_name);
                    block->insert(type_and_name);
                }
            } else {
                auto vec = vectorized::ColumnVector<Int32>::create();
                auto& data = vec->get_data();
                for (int i = 0; i < row_num; ++i) {
                    data.push_back(i);
                }
                vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
                vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type,
                                                                col_name);
                block->insert(std::move(type_and_name));
            }
            break;
        case TYPE_DECIMAL32:
            type_desc.precision = 9;
            type_desc.scale = 2;
            {
                vectorized::DataTypePtr decimal_data_type =
                        std::make_shared<DataTypeDecimal<Decimal32>>(type_desc.precision,
                                                                     type_desc.scale);
                auto decimal_column = decimal_data_type->create_column();
                auto& data = ((vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int32>>*)
                                      decimal_column.get())
                                     ->get_data();
                for (int i = 0; i < row_num; ++i) {
                    if (i == 0) {
                        data.push_back(Int32(0));
                        continue;
                    }
                    Int32 val;
                    StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
                    i % 2 == 0 ? val = StringParser::string_to_decimal<TYPE_DECIMAL32>(
                                         "1234567.56", 11, type_desc.precision, type_desc.scale,
                                         &result)
                               : val = StringParser::string_to_decimal<TYPE_DECIMAL32>(
                                         "-1234567.56", 12, type_desc.precision, type_desc.scale,
                                         &result);
                    EXPECT_TRUE(result == StringParser::PARSE_SUCCESS);
                    data.push_back(val);
                }

                vectorized::ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(),
                                                                decimal_data_type, col_name);
                block->insert(type_and_name);
            }
            break;
        case TYPE_DECIMAL64:
            type_desc.precision = 18;
            type_desc.scale = 6;
            {
                vectorized::DataTypePtr decimal_data_type =
                        std::make_shared<DataTypeDecimal<Decimal64>>(type_desc.precision,
                                                                     type_desc.scale);
                auto decimal_column = decimal_data_type->create_column();
                auto& data = ((vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int64>>*)
                                      decimal_column.get())
                                     ->get_data();
                for (int i = 0; i < row_num; ++i) {
                    if (i == 0) {
                        data.push_back(Int64(0));
                        continue;
                    }
                    Int64 val;
                    StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
                    std::string decimal_string =
                            i % 2 == 0 ? "-123456789012.123456" : "123456789012.123456";
                    val = StringParser::string_to_decimal<TYPE_DECIMAL64>(
                            decimal_string.c_str(), decimal_string.size(), type_desc.precision,
                            type_desc.scale, &result);
                    EXPECT_TRUE(result == StringParser::PARSE_SUCCESS);
                    data.push_back(val);
                }
                vectorized::ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(),
                                                                decimal_data_type, col_name);
                block->insert(type_and_name);
            }
            break;
        case TYPE_DECIMAL128I:
            type_desc.precision = 27;
            type_desc.scale = 9;
            {
                vectorized::DataTypePtr decimal_data_type(
                        doris::vectorized::create_decimal(27, 9, true));
                auto decimal_column = decimal_data_type->create_column();
                auto& data = ((vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int128>>*)
                                      decimal_column.get())
                                     ->get_data();
                for (int i = 0; i < row_num; ++i) {
                    auto value = __int128_t(i * pow(10, 9) + i * pow(10, 8));
                    data.push_back(value);
                }
                vectorized::ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(),
                                                                decimal_data_type, col_name);
                block->insert(type_and_name);
            }
            break;
        case TYPE_STRING: {
            auto strcol = vectorized::ColumnString::create();
            for (int i = 0; i < row_num; ++i) {
                std::string is = std::to_string(i);
                strcol->insert_data(is.c_str(), is.size());
            }
            vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
            vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type, col_name);
            block->insert(type_and_name);
        } break;
        case TYPE_HLL: {
            vectorized::DataTypePtr hll_data_type(std::make_shared<vectorized::DataTypeHLL>());
            auto hll_column = hll_data_type->create_column();
            std::vector<HyperLogLog>& container =
                    ((vectorized::ColumnHLL*)hll_column.get())->get_data();
            for (int i = 0; i < row_num; ++i) {
                HyperLogLog hll;
                hll.update(i);
                container.push_back(hll);
            }
            vectorized::ColumnWithTypeAndName type_and_name(hll_column->get_ptr(), hll_data_type,
                                                            col_name);

            block->insert(type_and_name);
        } break;
        case TYPE_DATEV2: {
            auto column_vector_date_v2 = vectorized::ColumnVector<vectorized::UInt32>::create();
            auto& date_v2_data = column_vector_date_v2->get_data();
            for (int i = 0; i < row_num; ++i) {
                DateV2Value<DateV2ValueType> value;
                value.from_date_int64(20210501);
                date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&value));
            }
            vectorized::DataTypePtr date_v2_type(std::make_shared<vectorized::DataTypeDateV2>());
            vectorized::ColumnWithTypeAndName test_date_v2(column_vector_date_v2->get_ptr(),
                                                           date_v2_type, col_name);
            block->insert(test_date_v2);
        } break;
        case TYPE_DATE: // int64
        {
            auto column_vector_date = vectorized::ColumnVector<vectorized::Int64>::create();
            auto& date_data = column_vector_date->get_data();
            for (int i = 0; i < row_num; ++i) {
                VecDateTimeValue value;
                value.from_date_int64(20210501);
                date_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
            }
            vectorized::DataTypePtr date_type(std::make_shared<vectorized::DataTypeDate>());
            vectorized::ColumnWithTypeAndName test_date(column_vector_date->get_ptr(), date_type,
                                                        col_name);
            block->insert(test_date);
        } break;
        case TYPE_DATETIME: // int64
        {
            auto column_vector_datetime = vectorized::ColumnVector<vectorized::Int64>::create();
            auto& datetime_data = column_vector_datetime->get_data();
            for (int i = 0; i < row_num; ++i) {
                VecDateTimeValue value;
                value.from_date_int64(20210501080910);
                datetime_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
            }
            vectorized::DataTypePtr datetime_type(std::make_shared<vectorized::DataTypeDateTime>());
            vectorized::ColumnWithTypeAndName test_datetime(column_vector_datetime->get_ptr(),
                                                            datetime_type, col_name);
            block->insert(test_datetime);
        } break;
        case TYPE_DATETIMEV2: // uint64
        {
            auto column_vector_datetimev2 = vectorized::ColumnVector<vectorized::UInt64>::create();
            DateV2Value<DateTimeV2ValueType> value;
            string date_literal = "2022-01-01 11:11:11.111";
            cctz::time_zone ctz;
            TimezoneUtils::find_cctz_time_zone("UTC", ctz);
            EXPECT_TRUE(value.from_date_str(date_literal.c_str(), date_literal.size(), ctz, 3));
            char to[64] = {};
            std::cout << "value: " << value.to_string(to) << std::endl;
            for (int i = 0; i < row_num; ++i) {
                column_vector_datetimev2->insert(value.to_date_int_val());
            }
            vectorized::DataTypePtr datetimev2_type(
                    std::make_shared<vectorized::DataTypeDateTimeV2>(3));
            vectorized::ColumnWithTypeAndName test_datetimev2(column_vector_datetimev2->get_ptr(),
                                                              datetimev2_type, col_name);
            block->insert(test_datetimev2);
        } break;
        case TYPE_ARRAY: // array
            type_desc.add_sub_type(TYPE_STRING, true);
            {
                DataTypePtr s =
                        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
                DataTypePtr au = std::make_shared<DataTypeArray>(s);
                Array a1, a2;
                a1.push_back(Field("sss"));
                a1.push_back(Null());
                a1.push_back(Field("clever amory"));
                a2.push_back(Field("hello amory"));
                a2.push_back(Null());
                a2.push_back(Field("cute amory"));
                a2.push_back(Field("sf"));
                MutableColumnPtr array_column = au->create_column();
                array_column->reserve(2);
                array_column->insert(a1);
                array_column->insert(a2);
                vectorized::ColumnWithTypeAndName type_and_name(array_column->get_ptr(), au,
                                                                col_name);
                block->insert(type_and_name);
            }
            break;
        case TYPE_MAP:
            type_desc.add_sub_type(TYPE_STRING, true);
            type_desc.add_sub_type(TYPE_STRING, true);
            {
                DataTypePtr s =
                        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
                ;
                DataTypePtr d =
                        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
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
                vectorized::ColumnWithTypeAndName type_and_name(map_column->get_ptr(), m, col_name);
                block->insert(type_and_name);
            }
            break;
        case TYPE_STRUCT:
            type_desc.add_sub_type(TYPE_STRING, "name", true);
            type_desc.add_sub_type(TYPE_LARGEINT, "age", true);
            type_desc.add_sub_type(TYPE_BOOLEAN, "is", true);
            {
                DataTypePtr s =
                        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
                DataTypePtr d =
                        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
                DataTypePtr m =
                        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
                DataTypePtr st =
                        std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {s, d, m});
                Tuple t1, t2;
                t1.push_back(Field("amory cute"));
                t1.push_back(__int128_t(37));
                t1.push_back(true);
                t2.push_back("null");
                t2.push_back(__int128_t(26));
                t2.push_back(false);
                MutableColumnPtr struct_column = st->create_column();
                struct_column->reserve(2);
                struct_column->insert(t1);
                struct_column->insert(t2);
                vectorized::ColumnWithTypeAndName type_and_name(struct_column->get_ptr(), st,
                                                                col_name);
                block->insert(type_and_name);
            }
            break;
        case TYPE_IPV4: {
            auto vec = vectorized::ColumnIPv4::create();
            auto& data = vec->get_data();
            for (int i = 0; i < row_num; ++i) {
                data.push_back(i);
            }
            vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeIPv4>());
            vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, col_name);
            block->insert(std::move(type_and_name));
        } break;
        case TYPE_IPV6: {
            auto vec = vectorized::ColumnIPv6::create();
            auto& data = vec->get_data();
            for (int i = 0; i < row_num; ++i) {
                data.push_back(i);
            }
            vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeIPv6>());
            vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, col_name);
            block->insert(std::move(type_and_name));
        } break;
        default:
            LOG(FATAL) << "error column type";
        }
    }
    std::shared_ptr<arrow::RecordBatch> record_batch =
            CommonDataTypeSerdeTest::serialize_arrow(block);
    auto assert_block = std::make_shared<Block>(block->clone_empty());
    CommonDataTypeSerdeTest::deserialize_arrow(assert_block, record_batch);
    CommonDataTypeSerdeTest::compare_two_blocks(block, assert_block);
}

TEST(DataTypeSerDeArrowTest, DataTypeScalaSerDeTest) {
    std::vector<PrimitiveType> cols = {
            TYPE_INT,        TYPE_INT,       TYPE_STRING, TYPE_DECIMAL128I, TYPE_BOOLEAN,
            TYPE_DECIMAL32,  TYPE_DECIMAL64, TYPE_IPV4,   TYPE_IPV6,        TYPE_DATETIME,
            TYPE_DATETIMEV2, TYPE_DATE,      TYPE_DATEV2,
    };
    serialize_and_deserialize_arrow_test(cols, 7, true);
    serialize_and_deserialize_arrow_test(cols, 7, false);
}

TEST(DataTypeSerDeArrowTest, DataTypeCollectionSerDeTest) {
    std::vector<PrimitiveType> cols = {TYPE_ARRAY, TYPE_MAP, TYPE_STRUCT};
    serialize_and_deserialize_arrow_test(cols, 7, true);
    serialize_and_deserialize_arrow_test(cols, 7, false);
}

TEST(DataTypeSerDeArrowTest, DataTypeMapNullKeySerDeTest) {
    std::string col_name = "map_null_key";
    auto block = std::make_shared<Block>();
    {
        DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        DataTypePtr m = std::make_shared<DataTypeMap>(s, d);
        Array k1, k2, v1, v2, k3, v3;
        k1.push_back("doris");
        k1.push_back("clever amory");
        v1.push_back(Null());
        v1.push_back(30);
        k2.push_back("hello amory");
        k2.push_back("NULL");
        k2.push_back("cute amory");
        k2.push_back("doris");
        v2.push_back(26);
        v2.push_back(Null());
        v2.push_back(6);
        v2.push_back(7);
        k3.push_back("test");
        v3.push_back(11);
        Map m1, m2, m3;
        m1.push_back(k1);
        m1.push_back(v1);
        m2.push_back(k2);
        m2.push_back(v2);
        m3.push_back(k3);
        m3.push_back(v3);
        MutableColumnPtr map_column = m->create_column();
        map_column->reserve(3);
        map_column->insert(m1);
        map_column->insert(m2);
        map_column->insert(m3);
        vectorized::ColumnWithTypeAndName type_and_name(map_column->get_ptr(), m, col_name);
        block->insert(type_and_name);
    }

    std::shared_ptr<arrow::RecordBatch> record_batch =
            CommonDataTypeSerdeTest::serialize_arrow(block);
    auto assert_block = std::make_shared<Block>(block->clone_empty());
    CommonDataTypeSerdeTest::deserialize_arrow(assert_block, record_batch);
    CommonDataTypeSerdeTest::compare_two_blocks(block, assert_block);
}

} // namespace doris::vectorized
