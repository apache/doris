
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
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "olap/hll.h"
#include "runtime/descriptors.cpp"
#include "runtime/descriptors.h"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "util/bitmap_value.h"
#include "util/quantile_state.h"
#include "util/string_parser.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_quantilestate.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/io/io_helper.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/arrow_column_to_doris_column.h"

namespace doris::vectorized {

template <bool is_scalar>
void serialize_and_deserialize_arrow_test() {
    vectorized::Block block;
    std::vector<std::tuple<std::string, FieldType, int, PrimitiveType, bool>> cols;
    if constexpr (is_scalar) {
        cols = {
                {"k1", FieldType::OLAP_FIELD_TYPE_INT, 1, TYPE_INT, false},
                {"k7", FieldType::OLAP_FIELD_TYPE_INT, 7, TYPE_INT, true},
                {"k2", FieldType::OLAP_FIELD_TYPE_STRING, 2, TYPE_STRING, false},
                {"k3", FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 3, TYPE_DECIMAL128I, false},
                {"k11", FieldType::OLAP_FIELD_TYPE_DATETIME, 11, TYPE_DATETIME, false},
                {"k4", FieldType::OLAP_FIELD_TYPE_BOOL, 4, TYPE_BOOLEAN, false},
                {"k5", FieldType::OLAP_FIELD_TYPE_DECIMAL32, 5, TYPE_DECIMAL32, false},
                {"k6", FieldType::OLAP_FIELD_TYPE_DECIMAL64, 6, TYPE_DECIMAL64, false},
                {"k12", FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 12, TYPE_DATETIMEV2, false},
        };
    } else {
        cols = {{"a", FieldType::OLAP_FIELD_TYPE_ARRAY, 6, TYPE_ARRAY, true},
                {"m", FieldType::OLAP_FIELD_TYPE_MAP, 8, TYPE_MAP, true},
                {"s", FieldType::OLAP_FIELD_TYPE_STRUCT, 5, TYPE_STRUCT, true}};
    }

    int row_num = 7;
    // make desc and generate block
    TupleDescriptor tuple_desc(PTupleDescriptor(), true);
    for (auto t : cols) {
        TSlotDescriptor tslot;
        std::string col_name = std::get<0>(t);
        tslot.__set_colName(col_name);
        TypeDescriptor type_desc(std::get<3>(t));
        bool is_nullable(std::get<4>(t));
        switch (std::get<3>(t)) {
        case TYPE_BOOLEAN:
            tslot.__set_slotType(type_desc.to_thrift());
            {
                auto vec = vectorized::ColumnVector<UInt8>::create();
                auto& data = vec->get_data();
                for (int i = 0; i < row_num; ++i) {
                    data.push_back(i % 2);
                }
                vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeUInt8>());
                vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type,
                                                                col_name);
                block.insert(std::move(type_and_name));
            }
            break;
        case TYPE_INT:
            tslot.__set_slotType(type_desc.to_thrift());
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
                    block.insert(type_and_name);
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
                block.insert(std::move(type_and_name));
            }
            break;
        case TYPE_DECIMAL32:
            type_desc.precision = 9;
            type_desc.scale = 2;
            tslot.__set_slotType(type_desc.to_thrift());
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
                block.insert(type_and_name);
            }
            break;
        case TYPE_DECIMAL64:
            type_desc.precision = 18;
            type_desc.scale = 6;
            tslot.__set_slotType(type_desc.to_thrift());
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
                block.insert(type_and_name);
            }
            break;
        case TYPE_DECIMAL128I:
            type_desc.precision = 27;
            type_desc.scale = 9;
            tslot.__set_slotType(type_desc.to_thrift());
            {
                vectorized::DataTypePtr decimal_data_type(
                        doris::vectorized::create_decimal(27, 9, true));
                auto decimal_column = decimal_data_type->create_column();
                auto& data = ((vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int128>>*)
                                      decimal_column.get())
                                     ->get_data();
                for (int i = 0; i < row_num; ++i) {
                    __int128_t value = i * pow(10, 9) + i * pow(10, 8);
                    data.push_back(value);
                }
                vectorized::ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(),
                                                                decimal_data_type, col_name);
                block.insert(type_and_name);
            }
            break;
        case TYPE_STRING:
            tslot.__set_slotType(type_desc.to_thrift());
            {
                auto strcol = vectorized::ColumnString::create();
                for (int i = 0; i < row_num; ++i) {
                    std::string is = std::to_string(i);
                    strcol->insert_data(is.c_str(), is.size());
                }
                vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
                vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type,
                                                                col_name);
                block.insert(type_and_name);
            }
            break;
        case TYPE_HLL:
            tslot.__set_slotType(type_desc.to_thrift());
            {
                vectorized::DataTypePtr hll_data_type(std::make_shared<vectorized::DataTypeHLL>());
                auto hll_column = hll_data_type->create_column();
                std::vector<HyperLogLog>& container =
                        ((vectorized::ColumnHLL*)hll_column.get())->get_data();
                for (int i = 0; i < row_num; ++i) {
                    HyperLogLog hll;
                    hll.update(i);
                    container.push_back(hll);
                }
                vectorized::ColumnWithTypeAndName type_and_name(hll_column->get_ptr(),
                                                                hll_data_type, col_name);

                block.insert(type_and_name);
            }
            break;
        case TYPE_DATEV2:
            tslot.__set_slotType(type_desc.to_thrift());
            {
                auto column_vector_date_v2 = vectorized::ColumnVector<vectorized::UInt32>::create();
                auto& date_v2_data = column_vector_date_v2->get_data();
                for (int i = 0; i < row_num; ++i) {
                    vectorized::DateV2Value<doris::vectorized::DateV2ValueType> value;
                    value.from_date((uint32_t)((2022 << 9) | (6 << 5) | 6));
                    date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&value));
                }
                vectorized::DataTypePtr date_v2_type(
                        std::make_shared<vectorized::DataTypeDateV2>());
                vectorized::ColumnWithTypeAndName test_date_v2(column_vector_date_v2->get_ptr(),
                                                               date_v2_type, col_name);
                block.insert(test_date_v2);
            }
            break;
        case TYPE_DATE: // int64
            tslot.__set_slotType(type_desc.to_thrift());
            {
                auto column_vector_date = vectorized::ColumnVector<vectorized::Int64>::create();
                auto& date_data = column_vector_date->get_data();
                for (int i = 0; i < row_num; ++i) {
                    vectorized::VecDateTimeValue value;
                    value.from_date_int64(20210501);
                    date_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
                }
                vectorized::DataTypePtr date_type(std::make_shared<vectorized::DataTypeDate>());
                vectorized::ColumnWithTypeAndName test_date(column_vector_date->get_ptr(),
                                                            date_type, col_name);
                block.insert(test_date);
            }
            break;
        case TYPE_DATETIME: // int64
            tslot.__set_slotType(type_desc.to_thrift());
            {
                auto column_vector_datetime = vectorized::ColumnVector<vectorized::Int64>::create();
                auto& datetime_data = column_vector_datetime->get_data();
                for (int i = 0; i < row_num; ++i) {
                    vectorized::VecDateTimeValue value;
                    value.from_date_int64(20210501080910);
                    datetime_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
                }
                vectorized::DataTypePtr datetime_type(
                        std::make_shared<vectorized::DataTypeDateTime>());
                vectorized::ColumnWithTypeAndName test_datetime(column_vector_datetime->get_ptr(),
                                                                datetime_type, col_name);
                block.insert(test_datetime);
            }
            break;
        case TYPE_DATETIMEV2: // uint64
            tslot.__set_slotType(type_desc.to_thrift());
            {
                // 2022-01-01 11:11:11.111
                auto column_vector_datetimev2 =
                        vectorized::ColumnVector<vectorized::UInt64>::create();
                //                auto& datetimev2_data = column_vector_datetimev2->get_data();
                DateV2Value<DateTimeV2ValueType> value;
                string date_literal = "2022-01-01 11:11:11.111";
                value.from_date_str(date_literal.c_str(), date_literal.size());
                char to[64] = {};
                std::cout << "value: " << value.to_string(to) << std::endl;
                for (int i = 0; i < row_num; ++i) {
                    column_vector_datetimev2->insert(value.to_date_int_val());
                }
                vectorized::DataTypePtr datetimev2_type(
                        std::make_shared<vectorized::DataTypeDateTimeV2>());
                vectorized::ColumnWithTypeAndName test_datetimev2(
                        column_vector_datetimev2->get_ptr(), datetimev2_type, col_name);
                block.insert(test_datetimev2);
            }
            break;
        case TYPE_ARRAY: // array
            type_desc.add_sub_type(TYPE_STRING, true);
            tslot.__set_slotType(type_desc.to_thrift());
            {
                DataTypePtr s =
                        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
                DataTypePtr au = std::make_shared<DataTypeArray>(s);
                Array a1, a2;
                a1.push_back(String("sss"));
                a1.push_back(Null());
                a1.push_back(String("clever amory"));
                a2.push_back(String("hello amory"));
                a2.push_back(Null());
                a2.push_back(String("cute amory"));
                a2.push_back(String("sf"));
                MutableColumnPtr array_column = au->create_column();
                array_column->reserve(2);
                array_column->insert(a1);
                array_column->insert(a2);
                vectorized::ColumnWithTypeAndName type_and_name(array_column->get_ptr(), au,
                                                                col_name);
                block.insert(type_and_name);
            }
            break;
        case TYPE_MAP:
            type_desc.add_sub_type(TYPE_STRING, true);
            type_desc.add_sub_type(TYPE_STRING, true);
            tslot.__set_slotType(type_desc.to_thrift());
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
                block.insert(type_and_name);
            }
            break;
        case TYPE_STRUCT:
            type_desc.add_sub_type(TYPE_STRING, "name", true);
            type_desc.add_sub_type(TYPE_LARGEINT, "age", true);
            type_desc.add_sub_type(TYPE_BOOLEAN, "is", true);
            tslot.__set_slotType(type_desc.to_thrift());
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
                vectorized::ColumnWithTypeAndName type_and_name(struct_column->get_ptr(), st,
                                                                col_name);
                block.insert(type_and_name);
            }
            break;
        default:
            break;
        }

        tslot.__set_col_unique_id(std::get<2>(t));
        SlotDescriptor* slot = new SlotDescriptor(tslot);
        tuple_desc.add_slot(slot);
    }

    RowDescriptor row_desc(&tuple_desc, true);
    // arrow schema
    std::shared_ptr<arrow::Schema> _arrow_schema;
    EXPECT_EQ(convert_to_arrow_schema(row_desc, &_arrow_schema), Status::OK());

    // serialize
    std::shared_ptr<arrow::RecordBatch> result;
    std::cout << "block data: " << block.dump_data(0, row_num) << std::endl;
    std::cout << "_arrow_schema: " << _arrow_schema->ToString(true) << std::endl;

    static_cast<void>(
            convert_to_arrow_batch(block, _arrow_schema, arrow::default_memory_pool(), &result));
    Block new_block = block.clone_empty();
    EXPECT_TRUE(result != nullptr);
    std::cout << "result: " << result->ToString() << std::endl;
    // deserialize
    for (auto t : cols) {
        std::string real_column_name = std::get<0>(t);
        auto* array = result->GetColumnByName(real_column_name).get();
        auto& column_with_type_and_name = new_block.get_by_name(real_column_name);
        if (std::get<3>(t) == PrimitiveType::TYPE_DATE ||
            std::get<3>(t) == PrimitiveType::TYPE_DATETIME) {
            {
                auto strcol = vectorized::ColumnString::create();
                vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
                vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type,
                                                                real_column_name);
                static_cast<void>(arrow_column_to_doris_column(
                        array, 0, type_and_name.column, type_and_name.type, block.rows(), "UTC"));
                {
                    auto& col = column_with_type_and_name.column.get()->assume_mutable_ref();
                    auto& date_data = static_cast<ColumnVector<Int64>&>(col).get_data();
                    for (int i = 0; i < strcol->size(); ++i) {
                        StringRef str = strcol->get_data_at(i);
                        vectorized::VecDateTimeValue value;
                        value.from_date_str(str.data, str.size);
                        date_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
                    }
                }
            }
            continue;
        } else if (std::get<3>(t) == PrimitiveType::TYPE_DATEV2) {
            auto strcol = vectorized::ColumnString::create();
            vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
            vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type,
                                                            real_column_name);
            static_cast<void>(arrow_column_to_doris_column(
                    array, 0, type_and_name.column, type_and_name.type, block.rows(), "UTC"));
            {
                auto& col = column_with_type_and_name.column.get()->assume_mutable_ref();
                auto& date_data = static_cast<ColumnVector<UInt32>&>(col).get_data();
                for (int i = 0; i < strcol->size(); ++i) {
                    StringRef str = strcol->get_data_at(i);
                    DateV2Value<DateV2ValueType> value;
                    value.from_date_str(str.data, str.size);
                    date_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&value));
                }
            }
            continue;
        } else if (std::get<3>(t) == PrimitiveType::TYPE_DATETIMEV2) {
            // now we only support read doris datetimev2 to arrow
            block.erase(real_column_name);
            new_block.erase(real_column_name);
            continue;
        }
        static_cast<void>(arrow_column_to_doris_column(array, 0, column_with_type_and_name.column,
                                                       column_with_type_and_name.type, block.rows(),
                                                       "UTC"));
    }

    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}

TEST(DataTypeSerDeArrowTest, DataTypeScalaSerDeTest) {
    serialize_and_deserialize_arrow_test<true>();
}

TEST(DataTypeSerDeArrowTest, DataTypeCollectionSerDeTest) {
    serialize_and_deserialize_arrow_test<false>();
}

TEST(DataTypeSerDeArrowTest, DataTypeMapNullKeySerDeTest) {
    TupleDescriptor tuple_desc(PTupleDescriptor(), true);
    TSlotDescriptor tslot;
    std::string col_name = "map_null_key";
    tslot.__set_colName(col_name);
    TypeDescriptor type_desc(TYPE_MAP);
    type_desc.add_sub_type(TYPE_STRING, true);
    type_desc.add_sub_type(TYPE_INT, true);
    tslot.__set_slotType(type_desc.to_thrift());
    vectorized::Block block;
    {
        DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        ;
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
        block.insert(type_and_name);
    }

    tslot.__set_col_unique_id(1);
    SlotDescriptor* slot = new SlotDescriptor(tslot);
    tuple_desc.add_slot(slot);
    RowDescriptor row_desc(&tuple_desc, true);
    // arrow schema
    std::shared_ptr<arrow::Schema> _arrow_schema;
    EXPECT_EQ(convert_to_arrow_schema(row_desc, &_arrow_schema), Status::OK());

    // serialize
    std::shared_ptr<arrow::RecordBatch> result;
    std::cout << "block structure: " << block.dump_structure() << std::endl;
    std::cout << "_arrow_schema: " << _arrow_schema->ToString(true) << std::endl;

    static_cast<void>(
            convert_to_arrow_batch(block, _arrow_schema, arrow::default_memory_pool(), &result));
    Block new_block = block.clone_empty();
    EXPECT_TRUE(result != nullptr);
    std::cout << "result: " << result->ToString() << std::endl;
    // deserialize
    auto* array = result->GetColumnByName(col_name).get();
    auto& column_with_type_and_name = new_block.get_by_name(col_name);
    static_cast<void>(arrow_column_to_doris_column(array, 0, column_with_type_and_name.column,
                                                   column_with_type_and_name.type, block.rows(),
                                                   "UTC"));
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    // new block row_index 0, 2 which row has key null will be filter
    EXPECT_EQ(new_block.dump_one_line(0, 1), "{\"doris\":null, \"clever amory\":30}");
    EXPECT_EQ(new_block.dump_one_line(2, 1), "{\"test\":11}");
    EXPECT_EQ(block.dump_data(1, 1), new_block.dump_data(1, 1));
}

} // namespace doris::vectorized
