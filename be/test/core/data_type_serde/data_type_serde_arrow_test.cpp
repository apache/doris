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

#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/util/decimal.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/visit_type_inline.h>
#include <arrow/visitor.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/types.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/common_data_type_serder_test.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_hll.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_quantilestate.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "core/types.h"
#include "core/value/hll.h"
#include "core/value/vdatetime_value.h"
#include "exec/common/arrow_column_to_doris_column.h"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "runtime/descriptors.cpp"
#include "util/string_parser.hpp"

namespace doris {

std::shared_ptr<Block> create_test_block(std::vector<PrimitiveType> cols, int row_num,
                                         bool is_nullable) {
    auto block = std::make_shared<Block>();
    for (int i = 0; i < cols.size(); i++) {
        std::string col_name = std::to_string(i);
        int precision = 0, scale = 0;
        switch (cols[i]) {
        case TYPE_DECIMAL32: {
            precision = 9;
            scale = 2;
            break;
        }
        case TYPE_DECIMAL64: {
            precision = 18;
            scale = 6;
            break;
        }
        case TYPE_DECIMAL128I: {
            precision = 27;
            scale = 9;
            break;
        }
        default:
            break;
        }
        DataTypePtr type_desc;
        if (!is_complex_type(cols[i])) {
            type_desc =
                    DataTypeFactory::instance().create_data_type(cols[i], false, precision, scale);
        }
        switch (cols[i]) {
        case TYPE_BOOLEAN: {
            auto vec = ColumnVector<TYPE_BOOLEAN>::create();
            auto& data = vec->get_data();
            for (int i = 0; i < row_num; ++i) {
                data.push_back(i % 2);
            }
            DataTypePtr data_type(std::make_shared<DataTypeUInt8>());
            ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, col_name);
            block->insert(std::move(type_and_name));
        } break;
        case TYPE_INT:
            if (is_nullable) {
                {
                    auto column_vector_int32 = ColumnVector<TYPE_INT>::create();
                    auto column_nullable_vector = make_nullable(std::move(column_vector_int32));
                    auto mutable_nullable_vector = std::move(*column_nullable_vector).mutate();
                    for (int i = 0; i < row_num; i++) {
                        if (i % 2 == 0) {
                            mutable_nullable_vector->insert_default();
                        } else {
                            mutable_nullable_vector->insert(
                                    Field::create_field<TYPE_INT>(int32_t(i)));
                        }
                    }
                    auto data_type = make_nullable(std::make_shared<DataTypeInt32>());
                    ColumnWithTypeAndName type_and_name(mutable_nullable_vector->get_ptr(),
                                                        data_type, col_name);
                    block->insert(type_and_name);
                }
            } else {
                auto vec = ColumnVector<TYPE_INT>::create();
                auto& data = vec->get_data();
                for (int i = 0; i < row_num; ++i) {
                    data.push_back(i);
                }
                DataTypePtr data_type(std::make_shared<DataTypeInt32>());
                ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, col_name);
                block->insert(std::move(type_and_name));
            }
            break;
        case TYPE_DECIMAL32: {
            DataTypePtr decimal_data_type = std::make_shared<DataTypeDecimal32>(9, 2);
            type_desc = decimal_data_type;
            auto decimal_column = decimal_data_type->create_column();
            auto& data = ((ColumnDecimal32*)decimal_column.get())->get_data();
            for (int i = 0; i < row_num; ++i) {
                if (i == 0) {
                    data.push_back(Int32(0));
                    continue;
                }
                Int32 val;
                StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
                i % 2 == 0 ? val = StringParser::string_to_decimal<TYPE_DECIMAL32>(
                                     "1234567.56", 10, type_desc->get_precision(),
                                     type_desc->get_scale(), &result)
                           : val = StringParser::string_to_decimal<TYPE_DECIMAL32>(
                                     "-1234567.56", 11, type_desc->get_precision(),
                                     type_desc->get_scale(), &result);
                EXPECT_TRUE(result == StringParser::PARSE_SUCCESS);
                data.push_back(val);
            }

            ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(), decimal_data_type,
                                                col_name);
            block->insert(type_and_name);
        } break;
        case TYPE_DECIMAL64: {
            DataTypePtr decimal_data_type = std::make_shared<DataTypeDecimal64>(18, 6);
            type_desc = decimal_data_type;
            auto decimal_column = decimal_data_type->create_column();
            auto& data = ((ColumnDecimal64*)decimal_column.get())->get_data();
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
                        decimal_string.c_str(), decimal_string.size(), type_desc->get_precision(),
                        type_desc->get_scale(), &result);
                EXPECT_TRUE(result == StringParser::PARSE_SUCCESS);
                data.push_back(val);
            }
            ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(), decimal_data_type,
                                                col_name);
            block->insert(type_and_name);
        } break;
        case TYPE_DECIMAL128I: {
            DataTypePtr decimal_data_type(doris::create_decimal(27, 9, true));
            type_desc = decimal_data_type;
            auto decimal_column = decimal_data_type->create_column();
            auto& data = ((ColumnDecimal128V3*)decimal_column.get())->get_data();
            for (int i = 0; i < row_num; ++i) {
                auto value = __int128_t(i * pow(10, 9) + i * pow(10, 8));
                data.push_back(value);
            }
            ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(), decimal_data_type,
                                                col_name);
            block->insert(type_and_name);
        } break;
        case TYPE_STRING: {
            auto strcol = ColumnString::create();
            for (int i = 0; i < row_num; ++i) {
                std::string is = std::to_string(i);
                strcol->insert_data(is.c_str(), is.size());
            }
            DataTypePtr data_type(std::make_shared<DataTypeString>());
            ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type, col_name);
            block->insert(type_and_name);
        } break;
        case TYPE_HLL: {
            DataTypePtr hll_data_type(std::make_shared<DataTypeHLL>());
            auto hll_column = hll_data_type->create_column();
            std::vector<HyperLogLog>& container = ((ColumnHLL*)hll_column.get())->get_data();
            for (int i = 0; i < row_num; ++i) {
                HyperLogLog hll;
                hll.update(i);
                container.push_back(hll);
            }
            ColumnWithTypeAndName type_and_name(hll_column->get_ptr(), hll_data_type, col_name);

            block->insert(type_and_name);
        } break;
        case TYPE_DATEV2: {
            auto column_vector_date_v2 = ColumnVector<TYPE_DATEV2>::create();
            auto& date_v2_data = column_vector_date_v2->get_data();
            for (int i = 0; i < row_num; ++i) {
                DateV2Value<DateV2ValueType> value;
                value.from_date_int64(20210501);
                date_v2_data.push_back(*reinterpret_cast<UInt32*>(&value));
            }
            DataTypePtr date_v2_type(std::make_shared<DataTypeDateV2>());
            ColumnWithTypeAndName test_date_v2(column_vector_date_v2->get_ptr(), date_v2_type,
                                               col_name);
            block->insert(test_date_v2);
        } break;
        case TYPE_DATE: // int64
        {
            auto column_vector_date = ColumnVector<TYPE_DATE>::create();
            auto& date_data = column_vector_date->get_data();
            for (int i = 0; i < row_num; ++i) {
                VecDateTimeValue value;
                value.from_date_int64(20210501);
                date_data.push_back(value);
            }
            DataTypePtr date_type(std::make_shared<DataTypeDate>());
            ColumnWithTypeAndName test_date(column_vector_date->get_ptr(), date_type, col_name);
            block->insert(test_date);
        } break;
        case TYPE_DATETIME: // int64
        {
            auto column_vector_datetime = ColumnVector<TYPE_DATETIME>::create();
            auto& datetime_data = column_vector_datetime->get_data();
            for (int i = 0; i < row_num; ++i) {
                VecDateTimeValue value;
                value.from_date_int64(20210501080910);
                datetime_data.push_back(value);
            }
            DataTypePtr datetime_type(std::make_shared<DataTypeDateTime>());
            ColumnWithTypeAndName test_datetime(column_vector_datetime->get_ptr(), datetime_type,
                                                col_name);
            block->insert(test_datetime);
        } break;
        case TYPE_DATETIMEV2: // uint64
        {
            auto column_vector_datetimev2 = ColumnVector<TYPE_DATETIMEV2>::create();
            DateV2Value<DateTimeV2ValueType> value;
            std::string date_literal = "2022-01-01 11:11:11.111";
            cctz::time_zone ctz;
            TimezoneUtils::find_cctz_time_zone("UTC", ctz);
            {
                CastParameters p;
                EXPECT_TRUE(CastToDatetimeV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                        {date_literal.c_str(), date_literal.size()}, value, &ctz, 3, p));
            }
            char to[64] = {};
            std::cout << "value: " << value.to_string(to) << std::endl;
            for (int i = 0; i < row_num; ++i) {
                column_vector_datetimev2->insert(Field::create_field<TYPE_DATETIMEV2>(value));
            }
            DataTypePtr datetimev2_type(std::make_shared<DataTypeDateTimeV2>(3));
            ColumnWithTypeAndName test_datetimev2(column_vector_datetimev2->get_ptr(),
                                                  datetimev2_type, col_name);
            block->insert(test_datetimev2);
        } break;
        case TYPE_ARRAY: // array
        {
            DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
            DataTypePtr au = std::make_shared<DataTypeArray>(s);
            Array a1, a2;
            a1.push_back(Field::create_field<TYPE_STRING>("sss"));
            a1.push_back(Field());
            a1.push_back(Field::create_field<TYPE_STRING>("clever amory"));
            a2.push_back(Field::create_field<TYPE_STRING>("hello amory"));
            a2.push_back(Field());
            a2.push_back(Field::create_field<TYPE_STRING>("cute amory"));
            a2.push_back(Field::create_field<TYPE_STRING>("sf"));
            MutableColumnPtr array_column = au->create_column();
            array_column->reserve(2);
            array_column->insert(Field::create_field<TYPE_ARRAY>(a1));
            array_column->insert(Field::create_field<TYPE_ARRAY>(a2));
            ColumnWithTypeAndName type_and_name(array_column->get_ptr(), au, col_name);
            block->insert(type_and_name);
            type_desc = au;
            break;
        }
        case TYPE_MAP: {
            DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
            DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
            DataTypePtr m = std::make_shared<DataTypeMap>(s, d);
            type_desc = m;
            Array k1, k2, v1, v2;
            k1.push_back(Field::create_field<TYPE_STRING>("null"));
            k1.push_back(Field::create_field<TYPE_STRING>("doris"));
            k1.push_back(Field::create_field<TYPE_STRING>("clever amory"));
            v1.push_back(Field::create_field<TYPE_STRING>("ss"));
            v1.push_back(Field());
            v1.push_back(Field::create_field<TYPE_STRING>("NULL"));
            k2.push_back(Field::create_field<TYPE_STRING>("hello amory"));
            k2.push_back(Field::create_field<TYPE_STRING>("NULL"));
            k2.push_back(Field::create_field<TYPE_STRING>("cute amory"));
            k2.push_back(Field::create_field<TYPE_STRING>("doris"));
            v2.push_back(Field::create_field<TYPE_STRING>("s"));
            v2.push_back(Field::create_field<TYPE_STRING>("0"));
            v2.push_back(Field::create_field<TYPE_STRING>("sf"));
            v2.push_back(Field());
            Map m1, m2;
            m1.push_back(Field::create_field<TYPE_ARRAY>(k1));
            m1.push_back(Field::create_field<TYPE_ARRAY>(v1));
            m2.push_back(Field::create_field<TYPE_ARRAY>(k2));
            m2.push_back(Field::create_field<TYPE_ARRAY>(v2));
            MutableColumnPtr map_column = m->create_column();
            map_column->reserve(2);
            map_column->insert(Field::create_field<TYPE_MAP>(m1));
            map_column->insert(Field::create_field<TYPE_MAP>(m2));
            ColumnWithTypeAndName type_and_name(map_column->get_ptr(), m, col_name);
            block->insert(type_and_name);
        } break;
        case TYPE_STRUCT: {
            DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
            DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
            DataTypePtr m = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
            DataTypePtr st = std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {s, d, m});
            type_desc = st;
            Struct t1, t2;
            t1.push_back(Field::create_field<TYPE_STRING>("amory cute"));
            t1.push_back(Field::create_field<TYPE_LARGEINT>(__int128_t(37)));
            t1.push_back(Field::create_field<TYPE_BOOLEAN>(true));
            t2.push_back(Field::create_field<TYPE_STRING>("null"));
            t2.push_back(Field::create_field<TYPE_LARGEINT>(__int128_t(26)));
            t2.push_back(Field::create_field<TYPE_BOOLEAN>(false));
            MutableColumnPtr struct_column = st->create_column();
            struct_column->reserve(2);
            struct_column->insert(Field::create_field<TYPE_STRUCT>(t1));
            struct_column->insert(Field::create_field<TYPE_STRUCT>(t2));
            ColumnWithTypeAndName type_and_name(struct_column->get_ptr(), st, col_name);
            block->insert(type_and_name);
        } break;
        case TYPE_IPV4: {
            auto vec = ColumnIPv4::create();
            auto& data = vec->get_data();
            for (int i = 0; i < row_num; ++i) {
                data.push_back(i);
            }
            DataTypePtr data_type(std::make_shared<DataTypeIPv4>());
            ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, col_name);
            block->insert(std::move(type_and_name));
        } break;
        case TYPE_IPV6: {
            auto vec = ColumnIPv6::create();
            auto& data = vec->get_data();
            for (int i = 0; i < row_num; ++i) {
                data.push_back(i);
            }
            DataTypePtr data_type(std::make_shared<DataTypeIPv6>());
            ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, col_name);
            block->insert(std::move(type_and_name));
        } break;
        case TYPE_LARGEINT: {
            auto vec = ColumnInt128::create();
            auto& data = vec->get_data();
            for (int i = 0; i < row_num; ++i) {
                data.push_back(__int128_t(i));
            }
            DataTypePtr data_type(std::make_shared<DataTypeInt128>());
            ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, col_name);
            block->insert(std::move(type_and_name));
        } break;
        default:
            LOG(FATAL) << "error column type";
        }
    }
    return block;
}

void serialize_and_deserialize_arrow_test(std::vector<PrimitiveType> cols, int row_num,
                                          bool is_nullable) {
    std::shared_ptr<Block> block = create_test_block(cols, row_num, is_nullable);
    std::shared_ptr<arrow::RecordBatch> record_batch =
            CommonDataTypeSerdeTest::serialize_arrow(block);
    auto assert_block = std::make_shared<Block>(block->clone_empty());
    CommonDataTypeSerdeTest::deserialize_arrow(assert_block, record_batch);
    CommonDataTypeSerdeTest::compare_two_blocks(block, assert_block);
}

void block_converter_test(std::vector<PrimitiveType> cols, int row_num, bool is_nullable) {
    std::shared_ptr<Block> source_block = create_test_block(cols, row_num, is_nullable);
    std::shared_ptr<arrow::RecordBatch> record_batch;
    std::shared_ptr<arrow::Schema> schema;
    Status status = Status::OK();
    status = get_arrow_schema_from_block(*source_block, &schema, TimezoneUtils::default_time_zone);
    ASSERT_TRUE(status.ok() && schema);
    cctz::time_zone default_timezone; //default UTC
    status = convert_to_arrow_batch(*source_block, schema, arrow::default_memory_pool(),
                                    &record_batch, default_timezone);
    ASSERT_TRUE(status.ok() && record_batch);
    auto target_block = std::make_shared<Block>(source_block->clone_empty());
    DataTypes source_data_types = source_block->get_data_types();
    status = convert_from_arrow_batch(record_batch, source_data_types, &*target_block,
                                      default_timezone);
    ASSERT_TRUE(status.ok() && target_block);
    CommonDataTypeSerdeTest::compare_two_blocks(source_block, target_block);
}

TEST(DataTypeSerDeArrowTest, DataTypeScalaSerDeTest) {
    std::vector<PrimitiveType> cols = {
            TYPE_INT,       TYPE_INT,        TYPE_STRING, TYPE_DECIMAL128I, TYPE_BOOLEAN,
            TYPE_DECIMAL32, TYPE_DECIMAL64,  TYPE_IPV4,   TYPE_IPV6,        TYPE_LARGEINT,
            TYPE_DATETIME,  TYPE_DATETIMEV2, TYPE_DATE,   TYPE_DATEV2,
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
        k1.push_back(Field::create_field<TYPE_STRING>("doris"));
        k1.push_back(Field::create_field<TYPE_STRING>("clever amory"));
        v1.push_back(Field());
        v1.push_back(Field::create_field<TYPE_INT>(30));
        k2.push_back(Field::create_field<TYPE_STRING>("hello amory"));
        k2.push_back(Field::create_field<TYPE_STRING>("NULL"));
        k2.push_back(Field::create_field<TYPE_STRING>("cute amory"));
        k2.push_back(Field::create_field<TYPE_STRING>("doris"));
        v2.push_back(Field::create_field<TYPE_INT>(26));
        v2.push_back(Field());
        v2.push_back(Field::create_field<TYPE_INT>(6));
        v2.push_back(Field::create_field<TYPE_INT>(7));
        k3.push_back(Field::create_field<TYPE_STRING>("test"));
        v3.push_back(Field::create_field<TYPE_INT>(11));
        Map m1, m2, m3;
        m1.push_back(Field::create_field<TYPE_ARRAY>(k1));
        m1.push_back(Field::create_field<TYPE_ARRAY>(v1));
        m2.push_back(Field::create_field<TYPE_ARRAY>(k2));
        m2.push_back(Field::create_field<TYPE_ARRAY>(v2));
        m3.push_back(Field::create_field<TYPE_ARRAY>(k3));
        m3.push_back(Field::create_field<TYPE_ARRAY>(v3));
        MutableColumnPtr map_column = m->create_column();
        map_column->reserve(3);
        map_column->insert(Field::create_field<TYPE_MAP>(m1));
        map_column->insert(Field::create_field<TYPE_MAP>(m2));
        map_column->insert(Field::create_field<TYPE_MAP>(m3));
        ColumnWithTypeAndName type_and_name(map_column->get_ptr(), m, col_name);
        block->insert(type_and_name);
    }

    std::shared_ptr<arrow::RecordBatch> record_batch =
            CommonDataTypeSerdeTest::serialize_arrow(block);
    auto assert_block = std::make_shared<Block>(block->clone_empty());
    CommonDataTypeSerdeTest::deserialize_arrow(assert_block, record_batch);
    CommonDataTypeSerdeTest::compare_two_blocks(block, assert_block);
}

TEST(DataTypeSerDeArrowTest, BigStringSerDeTest) {
    std::string col_name = "big_string";
    auto block = std::make_shared<Block>();
    auto strcol = ColumnString::create();
    // 2G, if > 4G report string column length is too large: total_length=4402341462
    for (int i = 0; i < 20; ++i) {
        std::string is(107374182, '0'); // 100M
        strcol->insert_data(is.c_str(), is.size());
    }
    DataTypePtr data_type(std::make_shared<DataTypeString>());
    ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type, col_name);
    block->insert(type_and_name);

    std::shared_ptr<arrow::RecordBatch> record_batch =
            CommonDataTypeSerdeTest::serialize_arrow(block);
    auto assert_block = std::make_shared<Block>(block->clone_empty());
    CommonDataTypeSerdeTest::deserialize_arrow(assert_block, record_batch);
    CommonDataTypeSerdeTest::compare_two_blocks(block, assert_block);
}

TEST(DataTypeSerDeArrowTest, IcebergUuidStringToFixedSizeBinary) {
    auto block = std::make_shared<Block>();
    auto strcol = ColumnString::create();
    strcol->insert_data("550e8400-e29b-41d4-a716-446655440000", 36);
    strcol->insert_data("00112233445566778899aabbccddeeff", 32);
    DataTypePtr data_type(std::make_shared<DataTypeString>());
    block->insert(ColumnWithTypeAndName(strcol->get_ptr(), data_type, "uuid_col"));

    auto metadata = arrow::KeyValueMetadata::Make({"originalType"}, {"uuid"});
    auto schema =
            arrow::schema({arrow::field("uuid_col", arrow::fixed_size_binary(16), true, metadata)});

    std::shared_ptr<arrow::RecordBatch> record_batch;
    cctz::time_zone default_timezone;
    Status status = convert_to_arrow_batch(*block, schema, arrow::default_memory_pool(),
                                           &record_batch, default_timezone);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(nullptr, record_batch);
    ASSERT_EQ(2, record_batch->num_rows());

    auto uuid_array =
            std::static_pointer_cast<arrow::FixedSizeBinaryArray>(record_batch->column(0));
    ASSERT_EQ(16, uuid_array->byte_width());

    const uint8_t expected0[] = {0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
                                 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00};
    const uint8_t expected1[] = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                                 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff};
    EXPECT_EQ(0, std::memcmp(uuid_array->GetValue(0), expected0, sizeof(expected0)));
    EXPECT_EQ(0, std::memcmp(uuid_array->GetValue(1), expected1, sizeof(expected1)));
}

TEST(DataTypeSerDeArrowTest, NestedIcebergUuidStringToFixedSizeBinary) {
    auto block = std::make_shared<Block>();
    DataTypePtr data_type = std::make_shared<DataTypeStruct>(
            std::vector<DataTypePtr> {std::make_shared<DataTypeString>()});
    auto struct_column = data_type->create_column();

    Struct row;
    row.push_back(Field::create_field<TYPE_STRING>("550e8400-e29b-41d4-a716-446655440000"));
    struct_column->insert(Field::create_field<TYPE_STRUCT>(row));
    block->insert(ColumnWithTypeAndName(struct_column->get_ptr(), data_type, "uuid_struct"));

    auto metadata = arrow::KeyValueMetadata::Make({"originalType"}, {"uuid"});
    auto schema = arrow::schema({arrow::field(
            "uuid_struct",
            arrow::struct_({arrow::field("id", arrow::fixed_size_binary(16), true, metadata)}),
            true)});

    std::shared_ptr<arrow::RecordBatch> record_batch;
    cctz::time_zone default_timezone;
    Status status = convert_to_arrow_batch(*block, schema, arrow::default_memory_pool(),
                                           &record_batch, default_timezone);
    ASSERT_TRUE(status.ok()) << status;

    auto struct_array = std::static_pointer_cast<arrow::StructArray>(record_batch->column(0));
    auto uuid_array = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(struct_array->field(0));
    const uint8_t expected[] = {0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
                                0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00};
    EXPECT_EQ(0, std::memcmp(uuid_array->GetValue(0), expected, sizeof(expected)));
}

TEST(DataTypeSerDeArrowTest, CharToFixedSizeBinaryPadsZeros) {
    auto block = std::make_shared<Block>();
    auto strcol = ColumnString::create();
    strcol->insert_data("ab", 2);
    DataTypePtr data_type(std::make_shared<DataTypeString>(4, TYPE_CHAR));
    block->insert(ColumnWithTypeAndName(strcol->get_ptr(), data_type, "fixed_col"));

    auto schema = arrow::schema({arrow::field("fixed_col", arrow::fixed_size_binary(4), true)});

    std::shared_ptr<arrow::RecordBatch> record_batch;
    cctz::time_zone default_timezone;
    Status status = convert_to_arrow_batch(*block, schema, arrow::default_memory_pool(),
                                           &record_batch, default_timezone);
    ASSERT_TRUE(status.ok()) << status;

    auto fixed_array =
            std::static_pointer_cast<arrow::FixedSizeBinaryArray>(record_batch->column(0));
    const char expected[] = {'a', 'b', '\0', '\0'};
    EXPECT_EQ(0, std::memcmp(fixed_array->GetValue(0), expected, sizeof(expected)));
}

TEST(DataTypeSerDeArrowTest, StringToLargeBinary) {
    auto block = std::make_shared<Block>();
    auto strcol = ColumnString::create();
    strcol->insert_data("binary-value", 12);
    DataTypePtr data_type(std::make_shared<DataTypeString>());
    block->insert(ColumnWithTypeAndName(strcol->get_ptr(), data_type, "bin_col"));

    auto schema = arrow::schema({arrow::field("bin_col", arrow::large_binary(), true)});

    std::shared_ptr<arrow::RecordBatch> record_batch;
    cctz::time_zone default_timezone;
    Status status = convert_to_arrow_batch(*block, schema, arrow::default_memory_pool(),
                                           &record_batch, default_timezone);
    ASSERT_TRUE(status.ok()) << status;

    auto binary_array = std::static_pointer_cast<arrow::LargeBinaryArray>(record_batch->column(0));
    ASSERT_EQ(12, binary_array->value_length(0));
    const uint8_t* raw = binary_array->value_data()->data() + binary_array->value_offset(0);
    EXPECT_EQ(0, std::memcmp(raw, "binary-value", 12));
}

TEST(DataTypeSerDeArrowTest, BlockConverterTest) {
    std::vector<PrimitiveType> cols = {
            TYPE_INT,       TYPE_INT,        TYPE_STRING, TYPE_DECIMAL128I, TYPE_BOOLEAN,
            TYPE_DECIMAL32, TYPE_DECIMAL64,  TYPE_IPV4,   TYPE_IPV6,        TYPE_LARGEINT,
            TYPE_DATETIME,  TYPE_DATETIMEV2, TYPE_DATE,   TYPE_DATEV2,
    };
    block_converter_test(cols, 7, true);
    block_converter_test(cols, 7, false);
}

// A utf8 column whose data exceeds the (test-lowered) int32 offset limit must be split by rows
// into several batches, each keeping the fixed utf8 schema, and the batches must reconstruct the
// original rows byte-for-byte in order.
TEST(DataTypeSerDeArrowTest, ConvertToArrowBatchesSplitsUtf8ByRows) {
    std::vector<std::string> values;
    for (int i = 0; i < 10; ++i) {
        std::string v(9, 'x');
        v.push_back(static_cast<char>('0' + i)); // 10-byte distinct values
        values.push_back(v);
    }
    auto strcol = ColumnString::create();
    for (const auto& v : values) {
        strcol->insert_data(v.data(), v.size());
    }
    auto block = std::make_shared<Block>();
    block->insert(
            ColumnWithTypeAndName(strcol->get_ptr(), std::make_shared<DataTypeString>(), "s"));
    auto schema = arrow::schema({arrow::field("s", arrow::utf8(), false)});

    // 32-byte cap => at most three 10-byte values per batch.
    const int64_t saved = config::arrow_flight_result_max_utf8_bytes;
    config::arrow_flight_result_max_utf8_bytes = 32;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    cctz::time_zone tz;
    Status status =
            convert_to_arrow_batches(*block, schema, arrow::default_memory_pool(), &batches, tz);
    config::arrow_flight_result_max_utf8_bytes = saved;

    ASSERT_TRUE(status.ok()) << status;
    ASSERT_GT(batches.size(), 1U) << "expected the oversized block to be split";

    std::vector<std::string> restored;
    for (const auto& batch : batches) {
        ASSERT_EQ(batch->schema()->field(0)->type()->id(), arrow::Type::STRING);
        ASSERT_TRUE(batch->Validate().ok());
        auto arr = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
        ASSERT_LT(arr->value_offset(arr->length()), 32) << "a batch reached the int32 offset cap";
        for (int64_t r = 0; r < arr->length(); ++r) {
            restored.push_back(arr->GetString(r));
        }
    }
    EXPECT_EQ(restored, values) << "split batches must reconstruct the original rows in order";
}

// Under the default (2GB) limit a small block is emitted as a single batch (no split, no cost).
TEST(DataTypeSerDeArrowTest, ConvertToArrowBatchesNoSplitUnderLimit) {
    auto strcol = ColumnString::create();
    strcol->insert_data("hello", 5);
    strcol->insert_data("world", 5);
    auto block = std::make_shared<Block>();
    block->insert(
            ColumnWithTypeAndName(strcol->get_ptr(), std::make_shared<DataTypeString>(), "s"));
    auto schema = arrow::schema({arrow::field("s", arrow::utf8(), false)});

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    cctz::time_zone tz;
    Status status =
            convert_to_arrow_batches(*block, schema, arrow::default_memory_pool(), &batches, tz);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(batches.size(), 1U);
    auto arr = std::static_pointer_cast<arrow::StringArray>(batches[0]->column(0));
    ASSERT_EQ(arr->length(), 2);
    EXPECT_EQ(arr->GetString(0), "hello");
    EXPECT_EQ(arr->GetString(1), "world");
}

// A single value that alone exceeds the limit cannot be represented with int32 offsets; the
// converter must return a clear error rather than crashing or corrupting data.
TEST(DataTypeSerDeArrowTest, ConvertToArrowBatchesSingleValueExceedsLimitErrors) {
    auto strcol = ColumnString::create();
    strcol->insert_data("0123456789abcdef", 16); // one 16-byte value
    auto block = std::make_shared<Block>();
    block->insert(
            ColumnWithTypeAndName(strcol->get_ptr(), std::make_shared<DataTypeString>(), "s"));
    auto schema = arrow::schema({arrow::field("s", arrow::utf8(), false)});

    const int64_t saved = config::arrow_flight_result_max_utf8_bytes;
    config::arrow_flight_result_max_utf8_bytes = 8; // 16-byte value cannot fit in 8
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    cctz::time_zone tz;
    Status status =
            convert_to_arrow_batches(*block, schema, arrow::default_memory_pool(), &batches, tz);
    config::arrow_flight_result_max_utf8_bytes = saved;

    ASSERT_FALSE(status.ok()) << "a single oversized value must be a clear error, not a crash";
}

// With two utf8 columns of different widths, the tighter-growing column decides each cut point.
TEST(DataTypeSerDeArrowTest, ConvertToArrowBatchesSplitDrivenByTightestColumn) {
    auto cola = ColumnString::create();
    auto colb = ColumnString::create();
    std::vector<std::string> a_vals;
    std::vector<std::string> b_vals;
    for (int i = 0; i < 5; ++i) {
        std::string a(9, 'a');
        a.push_back(static_cast<char>('0' + i)); // 10 bytes
        std::string b(19, 'b');
        b.push_back(static_cast<char>('0' + i)); // 20 bytes
        a_vals.push_back(a);
        b_vals.push_back(b);
        cola->insert_data(a.data(), a.size());
        colb->insert_data(b.data(), b.size());
    }
    auto block = std::make_shared<Block>();
    block->insert(ColumnWithTypeAndName(cola->get_ptr(), std::make_shared<DataTypeString>(), "a"));
    block->insert(ColumnWithTypeAndName(colb->get_ptr(), std::make_shared<DataTypeString>(), "b"));
    auto schema = arrow::schema(
            {arrow::field("a", arrow::utf8(), false), arrow::field("b", arrow::utf8(), false)});

    const int64_t saved = config::arrow_flight_result_max_utf8_bytes;
    config::arrow_flight_result_max_utf8_bytes = 32;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    cctz::time_zone tz;
    Status status =
            convert_to_arrow_batches(*block, schema, arrow::default_memory_pool(), &batches, tz);
    config::arrow_flight_result_max_utf8_bytes = saved;

    ASSERT_TRUE(status.ok()) << status;
    // col b at 20 bytes/row: two rows (40) exceed 32, so each batch holds exactly one row.
    ASSERT_EQ(batches.size(), 5U);
    std::vector<std::string> ra;
    std::vector<std::string> rb;
    for (const auto& batch : batches) {
        ASSERT_TRUE(batch->Validate().ok());
        auto arr_a = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
        auto arr_b = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
        for (int64_t r = 0; r < arr_a->length(); ++r) {
            ra.push_back(arr_a->GetString(r));
            rb.push_back(arr_b->GetString(r));
        }
    }
    EXPECT_EQ(ra, a_vals);
    EXPECT_EQ(rb, b_vals);
}

// Boundary: with threshold N, a value of N-1 bytes fits in one batch (the most a batch may
// hold); a value of exactly N bytes cannot fit int32 offsets and must error. This mirrors the
// default threshold INT32_MAX vs the Arrow builder limit INT32_MAX - 1.
TEST(DataTypeSerDeArrowTest, ConvertToArrowBatchesThresholdBoundary) {
    const int64_t saved = config::arrow_flight_result_max_utf8_bytes;
    cctz::time_zone tz;
    auto schema = arrow::schema({arrow::field("s", arrow::utf8(), false)});
    auto make_block = [](size_t value_len) {
        auto strcol = ColumnString::create();
        std::string v(value_len, 'a');
        strcol->insert_data(v.data(), v.size());
        auto block = std::make_shared<Block>();
        block->insert(
                ColumnWithTypeAndName(strcol->get_ptr(), std::make_shared<DataTypeString>(), "s"));
        return block;
    };

    // value == threshold - 1 -> fits in a single batch.
    {
        auto block = make_block(15);
        config::arrow_flight_result_max_utf8_bytes = 16;
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        Status st = convert_to_arrow_batches(*block, schema, arrow::default_memory_pool(), &batches,
                                             tz);
        config::arrow_flight_result_max_utf8_bytes = saved;
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(batches.size(), 1U);
        auto arr = std::static_pointer_cast<arrow::StringArray>(batches[0]->column(0));
        EXPECT_EQ(arr->value_offset(arr->length()), 15);
    }
    // value == threshold -> a single value cannot fit -> clear error.
    {
        auto block = make_block(16);
        config::arrow_flight_result_max_utf8_bytes = 16;
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        Status st = convert_to_arrow_batches(*block, schema, arrow::default_memory_pool(), &batches,
                                             tz);
        config::arrow_flight_result_max_utf8_bytes = saved;
        ASSERT_FALSE(st.ok()) << "value of exactly threshold bytes must error";
    }
}

// A transform-serde type (LARGEINT -> utf8 text) must NOT be split by physical byte_size: even
// when its physical size exceeds a lowered threshold it stays on the fast path (one batch),
// because get_data_at() bytes are not the emitted arrow payload for such types (and calling
// get_data_at on e.g. a Variant column would throw).
TEST(DataTypeSerDeArrowTest, ConvertToArrowBatchesDoesNotSplitTransformType) {
    DataTypePtr type = DataTypeFactory::instance().create_data_type(TYPE_LARGEINT, false);
    auto mcol = type->create_column();
    mcol->insert(Field::create_field<TYPE_LARGEINT>(Int128(1)));
    mcol->insert(Field::create_field<TYPE_LARGEINT>(Int128(22)));
    mcol->insert(Field::create_field<TYPE_LARGEINT>(Int128(333)));
    auto block = std::make_shared<Block>();
    block->insert(ColumnWithTypeAndName(mcol->get_ptr(), type, "n"));

    std::shared_ptr<arrow::Schema> schema;
    Status sst = get_arrow_schema_from_block(*block, &schema, TimezoneUtils::default_time_zone);
    ASSERT_TRUE(sst.ok()) << sst;
    ASSERT_EQ(schema->field(0)->type()->id(), arrow::Type::STRING); // largeint -> utf8

    const int64_t saved = config::arrow_flight_result_max_utf8_bytes;
    config::arrow_flight_result_max_utf8_bytes = 20; // < physical byte_size (3 * 16 = 48 bytes)
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    cctz::time_zone tz;
    Status st =
            convert_to_arrow_batches(*block, schema, arrow::default_memory_pool(), &batches, tz);
    config::arrow_flight_result_max_utf8_bytes = saved;

    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(batches.size(), 1U) << "transform type must not be split by physical byte_size";
    auto arr = std::static_pointer_cast<arrow::StringArray>(batches[0]->column(0));
    ASSERT_EQ(arr->length(), 3);
    EXPECT_EQ(arr->GetString(0), "1");
    EXPECT_EQ(arr->GetString(1), "22");
    EXPECT_EQ(arr->GetString(2), "333");
}

} // namespace doris
