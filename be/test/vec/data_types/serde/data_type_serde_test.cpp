
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

#include "vec/data_types/serde/data_type_serde.h"

#include <gen_cpp/types.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "olap/hll.h"
#include "util/bitmap_value.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "util/quantile_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_variant.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_quantilestate.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

inline void column_to_pb(const DataTypePtr data_type, const IColumn& col, PValues* result) {
    const DataTypeSerDeSPtr serde = data_type->get_serde();
    static_cast<void>(serde->write_column_to_pb(col, *result, 0, col.size()));
}

inline void pb_to_column(const DataTypePtr data_type, PValues& result, IColumn& col) {
    auto serde = data_type->get_serde();
    static_cast<void>(serde->read_column_from_pb(col, result));
}

inline void check_pb_col(const DataTypePtr data_type, const IColumn& col) {
    PValues pv = PValues();
    column_to_pb(data_type, col, &pv);
    std::string s1 = pv.DebugString();

    auto col1 = data_type->create_column();
    pb_to_column(data_type, pv, *col1);
    PValues as_pv = PValues();
    column_to_pb(data_type, *col1, &as_pv);

    std::string s2 = as_pv.DebugString();
    EXPECT_EQ(s1, s2);
}

inline void serialize_and_deserialize_pb_test() {
    // int
    {
        auto vec = vectorized::ColumnInt32::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        check_pb_col(data_type, *vec.get());
    }
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
    // decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
        auto decimal_column = decimal_data_type->create_column();
        auto& data = ((vectorized::ColumnDecimal128V3*)decimal_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            __int128_t value = __int128_t(i * pow(10, 9) + i * pow(10, 8));
            data.push_back(value);
        }
        check_pb_col(decimal_data_type, *decimal_column.get());
    }
    // bitmap
    {
        vectorized::DataTypePtr bitmap_data_type(std::make_shared<vectorized::DataTypeBitMap>());
        auto bitmap_column = bitmap_data_type->create_column();
        std::vector<BitmapValue>& container =
                ((vectorized::ColumnBitmap*)bitmap_column.get())->get_data();
        for (int i = 0; i < 4; ++i) {
            BitmapValue bv;
            for (int j = 0; j <= i; ++j) {
                bv.add(j);
            }
            container.push_back(bv);
        }
        check_pb_col(bitmap_data_type, *bitmap_column.get());
    }
    // hll
    {
        vectorized::DataTypePtr hll_data_type(std::make_shared<vectorized::DataTypeHLL>());
        auto hll_column = hll_data_type->create_column();
        std::vector<HyperLogLog>& container =
                ((vectorized::ColumnHLL*)hll_column.get())->get_data();
        for (int i = 0; i < 4; ++i) {
            HyperLogLog hll;
            hll.update(i);
            container.push_back(hll);
        }
        check_pb_col(hll_data_type, *hll_column.get());
    }
    // quantilestate
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
    {
        vectorized::DataTypePtr string_data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(string_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_many_defaults(1024);
        check_pb_col(nullable_data_type, *nullable_column.get());
    }
    // nullable decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(decimal_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_many_defaults(1024);
        check_pb_col(nullable_data_type, *nullable_column.get());
    }
    // int with 1024 batch size
    // ipv4
    {
        auto vec = vectorized::ColumnIPv4 ::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeIPv4>());
        check_pb_col(data_type, *vec.get());
    }
    // ipv6
    {
        auto vec = vectorized::ColumnIPv6::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeIPv6>());
        check_pb_col(data_type, *vec.get());
    }
}

TEST(DataTypeSerDeTest, DataTypeScalaSerDeTest) {
    serialize_and_deserialize_pb_test();
}

TEST(DataTypeSerDeTest, DataTypeRowStoreSerDeTest) {
    // ipv6
    {
        std::string ip = "5be8:dde9:7f0b:d5a7:bd01:b3be:9c69:573b";
        auto vec = vectorized::ColumnIPv6::create();
        IPv6Value ipv6;
        EXPECT_TRUE(ipv6.from_string(ip));
        vec->insert(Field::create_field<TYPE_IPV6>(ipv6.value()));

        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeIPv6>());
        auto serde = data_type->get_serde(0);
        JsonbWriterT<JsonbOutStream> jsonb_writer;
        Arena pool;
        jsonb_writer.writeStartObject();
        serde->write_one_cell_to_jsonb(*vec, jsonb_writer, pool, 0, 0);
        jsonb_writer.writeEndObject();
        auto jsonb_column = ColumnString::create();
        jsonb_column->insert_data(jsonb_writer.getOutput()->getBuffer(),
                                  jsonb_writer.getOutput()->getSize());
        StringRef jsonb_data = jsonb_column->get_data_at(0);
        JsonbDocument* pdoc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(jsonb_data.data, jsonb_data.size, &pdoc);
        ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st.to_string();
        JsonbDocument& doc = *pdoc;
        for (auto it = doc->begin(); it != doc->end(); ++it) {
            serde->read_one_cell_from_jsonb(*vec, it->value());
        }
        EXPECT_TRUE(vec->size() == 2);
        IPv6 data = vec->get_element(1);
        IPv6Value ipv6_value(data);
        EXPECT_EQ(ipv6_value.to_string(), ip);
    }

    // ipv4
    {
        std::string ip = "192.0.0.1";
        auto vec = vectorized::ColumnIPv4::create();
        IPv4Value ipv4;
        EXPECT_TRUE(ipv4.from_string(ip));
        vec->insert(Field::create_field<TYPE_IPV4>(ipv4.value()));

        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeIPv4>());
        auto serde = data_type->get_serde(0);
        JsonbWriterT<JsonbOutStream> jsonb_writer;
        Arena pool;
        jsonb_writer.writeStartObject();
        serde->write_one_cell_to_jsonb(*vec, jsonb_writer, pool, 0, 0);
        jsonb_writer.writeEndObject();
        auto jsonb_column = ColumnString::create();
        jsonb_column->insert_data(jsonb_writer.getOutput()->getBuffer(),
                                  jsonb_writer.getOutput()->getSize());
        StringRef jsonb_data = jsonb_column->get_data_at(0);
        JsonbDocument* pdoc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(jsonb_data.data, jsonb_data.size, &pdoc);
        ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st.to_string();
        JsonbDocument& doc = *pdoc;
        for (auto it = doc->begin(); it != doc->end(); ++it) {
            serde->read_one_cell_from_jsonb(*vec, it->value());
        }
        EXPECT_TRUE(vec->size() == 2);
        IPv4 data = vec->get_element(1);
        IPv4Value ipv4_value(data);
        EXPECT_EQ(ipv4_value.to_string(), ip);
    }
}

TEST(DataTypeSerDeTest, DeserializeFromSparseColumnTest) {
    auto sparse_column = ColumnVariant::create_sparse_column_fn();
    auto& column_map = assert_cast<ColumnMap&>(*sparse_column);
    // auto& key = assert_cast<ColumnString&>(column_map.get_keys());
    auto& value = assert_cast<ColumnString&>(column_map.get_values());
    // auto& offsets = column_map.get_offsets();
    auto data_type = ColumnVariant::get_sparse_column_type();
    std::string file_path = std::string(getenv("ROOT")) +
                            "/be/test/util/test_data/deserialize_from_sparse_column_test.bin";

    // Field string_field = Field::create_field<TYPE_STRING>("123");
    // FieldInfo info = {PrimitiveType::TYPE_STRING, false, false, 0};
    // ColumnVariant::Subcolumn string_subcolumn = {0, true, true};
    // string_subcolumn.insert(string_field, info);
    // string_subcolumn.serialize_to_sparse_column(&key, "a", &value, 0);

    // Field int_field = Field::create_field<TYPE_INT>(123);
    // info.scalar_type_id = PrimitiveType::TYPE_INT;
    // ColumnVariant::Subcolumn int_subcolumn = {0, true, true};
    // int_subcolumn.insert(int_field, info);
    // int_subcolumn.serialize_to_sparse_column(&key, "b", &value, 0);

    // Field largeint_field = Field::create_field<TYPE_LARGEINT>(__int128_t(123));
    // info.scalar_type_id = PrimitiveType::TYPE_LARGEINT;
    // ColumnVariant::Subcolumn largeint_subcolumn = {0, true, true};
    // largeint_subcolumn.insert(largeint_field, info);
    // largeint_subcolumn.serialize_to_sparse_column(&key, "c", &value, 0);

    // Field double_field = Field::create_field<TYPE_DOUBLE>(123.456);
    // info.scalar_type_id = PrimitiveType::TYPE_DOUBLE;
    // ColumnVariant::Subcolumn double_subcolumn = {0, true, true};
    // double_subcolumn.insert(double_field, info);
    // double_subcolumn.serialize_to_sparse_column(&key, "d", &value, 0);

    // Field bool_field = Field::create_field<TYPE_BOOLEAN>(true);
    // info.scalar_type_id = PrimitiveType::TYPE_BOOLEAN;
    // ColumnVariant::Subcolumn bool_subcolumn = {0, true, true};
    // bool_subcolumn.insert(bool_field, info);
    // bool_subcolumn.serialize_to_sparse_column(&key, "e", &value, 0);

    // Field datetime_field = Field::create_field<TYPE_DATETIMEV2>(23232323);
    // info.scalar_type_id = PrimitiveType::TYPE_DATETIMEV2;
    // info.scale = 3;
    // ColumnVariant::Subcolumn datetime_subcolumn = {0, true, true};
    // datetime_subcolumn.insert(datetime_field, info);
    // datetime_subcolumn.serialize_to_sparse_column(&key, "f", &value, 0);

    // Field date_field = Field::create_field<TYPE_DATEV2>(154543245);
    // info.scalar_type_id = PrimitiveType::TYPE_DATEV2;
    // info.scale = 3;
    // ColumnVariant::Subcolumn date_subcolumn = {0, true, true};
    // date_subcolumn.insert(date_field, info);
    // date_subcolumn.serialize_to_sparse_column(&key, "g", &value, 0);

    // Field ipv4_field = Field::create_field<TYPE_IPV4>(367357);
    // info.scalar_type_id = PrimitiveType::TYPE_IPV4;
    // ColumnVariant::Subcolumn ipv4_subcolumn = {0, true, true};
    // ipv4_subcolumn.insert(ipv4_field, info);
    // ipv4_subcolumn.serialize_to_sparse_column(&key, "h", &value, 0);

    // Field ipv6_field = Field::create_field<TYPE_IPV6>(36534645);
    // info.scalar_type_id = PrimitiveType::TYPE_IPV6;
    // ColumnVariant::Subcolumn ipv6_subcolumn = {0, true, true};
    // ipv6_subcolumn.insert(ipv6_field, info);
    // ipv6_subcolumn.serialize_to_sparse_column(&key, "i", &value, 0);

    // Field decimal32_field =
    //         Field::create_field<TYPE_DECIMAL32>(DecimalField<Decimal32>(3456345634, 2));
    // info.scalar_type_id = PrimitiveType::TYPE_DECIMAL32;
    // info.precision = 5;
    // info.scale = 2;
    // ColumnVariant::Subcolumn decimal32_subcolumn = {0, true, true};
    // decimal32_subcolumn.insert(decimal32_field, info);
    // decimal32_subcolumn.serialize_to_sparse_column(&key, "j", &value, 0);

    // Field decimal64_field =
    //         Field::create_field<TYPE_DECIMAL64>(DecimalField<Decimal64>(13452435, 6));
    // info.scalar_type_id = PrimitiveType::TYPE_DECIMAL64;
    // info.precision = 12;
    // info.scale = 6;
    // ColumnVariant::Subcolumn decimal64_subcolumn = {0, true, true};
    // decimal64_subcolumn.insert(decimal64_field, info);
    // decimal64_subcolumn.serialize_to_sparse_column(&key, "k", &value, 0);

    // Field decimal128i_field =
    //         Field::create_field<TYPE_DECIMAL128I>(DecimalField<Decimal128V3>(2342345, 12));
    // info.scalar_type_id = PrimitiveType::TYPE_DECIMAL128I;
    // info.precision = 32;
    // info.scale = 12;
    // ColumnVariant::Subcolumn decimal128i_subcolumn = {0, true, true};
    // decimal128i_subcolumn.insert(decimal128i_field, info);
    // decimal128i_subcolumn.serialize_to_sparse_column(&key, "l", &value, 0);

    // Field decimal256_field =
    //         Field::create_field<TYPE_DECIMAL256>(DecimalField<Decimal256>(Decimal256(2345243), 5));
    // info.scalar_type_id = PrimitiveType::TYPE_DECIMAL256;
    // info.precision = 52;
    // info.scale = 5;
    // ColumnVariant::Subcolumn decimal256_subcolumn = {0, true, true};
    // decimal256_subcolumn.insert(decimal256_field, info);
    // decimal256_subcolumn.serialize_to_sparse_column(&key, "m", &value, 0);

    // Field jsonb_field = Field::create_field<TYPE_JSONB>(JsonbField("abc", 3));
    // info.scalar_type_id = PrimitiveType::TYPE_JSONB;
    // ColumnVariant::Subcolumn jsonb_subcolumn = {0, true, true};
    // jsonb_subcolumn.insert(jsonb_field, info);
    // jsonb_subcolumn.serialize_to_sparse_column(&key, "n", &value, 0);

    // Field array_field = Field::create_field<TYPE_ARRAY>(Array(3));
    // info.scalar_type_id = PrimitiveType::TYPE_JSONB;
    // info.num_dimensions = 1;
    // auto& array = array_field.get<Array>();
    // array[0] = jsonb_field;
    // array[1] = Field();
    // array[2] = jsonb_field;

    // ColumnVariant::Subcolumn array_subcolumn = {0, true, true};
    // array_subcolumn.insert(array_field, info);
    // array_subcolumn.serialize_to_sparse_column(&key, "o", &value, 0);
    // offsets.push_back(key.size());

    // auto size = data_type->get_uncompressed_serialized_bytes(*sparse_column, 8);
    // char* buf = new char[size];
    // data_type->serialize(*sparse_column, buf, 8);
    // {
    //     std::ofstream ofs(file_path, std::ios::binary);
    //     ASSERT_TRUE(ofs.is_open());
    //     ofs.write(buf, static_cast<std::streamsize>(size));
    //     ofs.close();
    // }
    // delete[] buf;

    std::string read_data;
    {
        std::ifstream ifs(file_path, std::ios::binary);
        ASSERT_TRUE(ifs.is_open());
        ifs.seekg(0, std::ios::end);
        std::streamsize fsize = ifs.tellg();
        ifs.seekg(0, std::ios::beg);
        read_data.resize(static_cast<size_t>(fsize));
        ifs.read(read_data.data(), fsize);
    }

    sparse_column->clear();

    data_type->deserialize(read_data.data(), &sparse_column, 8);

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 0);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_STRING);
        EXPECT_EQ(subcolumn.get_last_field().get<String>(), "123");
        subcolumn.deserialize_from_sparse_column(&value, 0);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_STRING);
        EXPECT_EQ(subcolumn.get_last_field().get<String>(), "123");
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 1);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(), PrimitiveType::TYPE_INT);
        EXPECT_EQ(subcolumn.get_last_field().get<Int32>(), 123);
        subcolumn.deserialize_from_sparse_column(&value, 1);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(), PrimitiveType::TYPE_INT);
        EXPECT_EQ(subcolumn.get_last_field().get<Int32>(), 123);
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 2);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_LARGEINT);
        EXPECT_EQ(subcolumn.get_last_field().get<Int64>(), 123);
        subcolumn.deserialize_from_sparse_column(&value, 2);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_LARGEINT);
        EXPECT_EQ(subcolumn.get_last_field().get<Int64>(), 123);
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 3);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DOUBLE);
        EXPECT_EQ(subcolumn.get_last_field().get<double>(), 123.456);
        subcolumn.deserialize_from_sparse_column(&value, 3);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DOUBLE);
        EXPECT_EQ(subcolumn.get_last_field().get<double>(), 123.456);
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 4);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_BOOLEAN);
        EXPECT_EQ(subcolumn.get_last_field().get<bool>(), true);
        subcolumn.deserialize_from_sparse_column(&value, 4);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_BOOLEAN);
        EXPECT_EQ(subcolumn.get_last_field().get<bool>(), true);
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 5);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DATETIMEV2);
        EXPECT_EQ(subcolumn.get_last_field().get<UInt64>(), 23232323);
        subcolumn.deserialize_from_sparse_column(&value, 5);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DATETIMEV2);
        EXPECT_EQ(subcolumn.get_last_field().get<UInt64>(), 23232323);
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 6);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DATEV2);
        EXPECT_EQ(subcolumn.get_last_field().get<UInt64>(), 154543245);
        subcolumn.deserialize_from_sparse_column(&value, 6);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DATEV2);
        EXPECT_EQ(subcolumn.get_last_field().get<UInt64>(), 154543245);
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 7);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_IPV4);
        EXPECT_EQ(subcolumn.get_last_field().get<IPv4>(), static_cast<IPv4>(367357));
        subcolumn.deserialize_from_sparse_column(&value, 7);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_IPV4);
        EXPECT_EQ(subcolumn.get_last_field().get<IPv4>(), static_cast<IPv4>(367357));
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 8);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_IPV6);
        EXPECT_EQ(subcolumn.get_last_field().get<IPv6>(), static_cast<IPv6>(36534645));
        subcolumn.deserialize_from_sparse_column(&value, 8);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_IPV6);
        EXPECT_EQ(subcolumn.get_last_field().get<IPv6>(), static_cast<IPv6>(36534645));
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 9);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DECIMAL32);
        auto v = subcolumn.get_last_field().get<DecimalField<Decimal32>>();
        EXPECT_EQ(static_cast<Int32>(v.get_value()), static_cast<Int32>(3456345634));
        subcolumn.deserialize_from_sparse_column(&value, 9);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DECIMAL32);
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 10);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DECIMAL64);
        auto v = subcolumn.get_last_field().get<DecimalField<Decimal64>>();
        EXPECT_EQ(static_cast<Int64>(v.get_value()), static_cast<Int64>(13452435));
        subcolumn.deserialize_from_sparse_column(&value, 10);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DECIMAL64);
        v = subcolumn.get_last_field().get<DecimalField<Decimal64>>();
        EXPECT_EQ(static_cast<Int64>(v.get_value()), static_cast<Int64>(13452435));
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 11);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DECIMAL128I);
        auto v = subcolumn.get_last_field().get<DecimalField<Decimal128V3>>();
        EXPECT_EQ(static_cast<Int128>(v.get_value()), static_cast<Int128>(2342345));
        subcolumn.deserialize_from_sparse_column(&value, 11);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DECIMAL128I);
        v = subcolumn.get_last_field().get<DecimalField<Decimal128V3>>();
        EXPECT_EQ(static_cast<Int128>(v.get_value()), static_cast<Int128>(2342345));
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 12);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DECIMAL256);
        auto v = subcolumn.get_last_field().get<DecimalField<Decimal256>>();
        EXPECT_TRUE(v.get_value() == Decimal256(2345243));
        subcolumn.deserialize_from_sparse_column(&value, 12);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_DECIMAL256);
        v = subcolumn.get_last_field().get<DecimalField<Decimal256>>();
        EXPECT_TRUE(v.get_value() == Decimal256(2345243));
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 13);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_JSONB);
        subcolumn.deserialize_from_sparse_column(&value, 13);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_JSONB);
    }

    {
        ColumnVariant::Subcolumn subcolumn = {0, true, true};
        subcolumn.deserialize_from_sparse_column(&value, 14);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_ARRAY);
        EXPECT_EQ(subcolumn.get_dimensions(), 1);
        EXPECT_EQ(subcolumn.get_least_common_base_type_id(), PrimitiveType::TYPE_JSONB);
        auto v = subcolumn.get_last_field();
        auto& arr = v.get<Array>();
        EXPECT_EQ(arr.size(), 3);
        EXPECT_FALSE(arr[0].is_null());
        EXPECT_TRUE(arr[1].is_null());
        EXPECT_FALSE(arr[2].is_null());
        subcolumn.deserialize_from_sparse_column(&value, 14);
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(),
                  PrimitiveType::TYPE_ARRAY);
        EXPECT_EQ(subcolumn.get_dimensions(), 1);
        EXPECT_EQ(subcolumn.get_least_common_base_type_id(), PrimitiveType::TYPE_JSONB);

        v = subcolumn.get_last_field();
        arr = v.get<Array>();
        EXPECT_EQ(arr.size(), 3);
        EXPECT_FALSE(arr[0].is_null());
        EXPECT_TRUE(arr[1].is_null());
        EXPECT_FALSE(arr[2].is_null());
    }
}
} // namespace doris::vectorized
