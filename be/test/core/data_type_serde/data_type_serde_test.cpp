
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

#include "core/data_type_serde/data_type_serde.h"

#include <arrow/api.h>
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

#include "core/column/column.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_hll.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_quantilestate.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/types.h"
#include "core/value/bitmap_value.h"
#include "core/value/hll.h"
#include "core/value/quantile_state.h"
#include "gtest/gtest_pred_impl.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"

namespace doris {

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
        auto vec = ColumnInt32::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        DataTypePtr data_type(std::make_shared<DataTypeInt32>());
        check_pb_col(data_type, *vec.get());
    }
    // string
    {
        auto strcol = ColumnString::create();
        for (int i = 0; i < 1024; ++i) {
            std::string is = std::to_string(i);
            strcol->insert_data(is.c_str(), is.size());
        }
        DataTypePtr data_type(std::make_shared<DataTypeString>());
        check_pb_col(data_type, *strcol.get());
    }
    // decimal
    {
        DataTypePtr decimal_data_type(doris::create_decimal(27, 9, true));
        auto decimal_column = decimal_data_type->create_column();
        auto& data = ((ColumnDecimal128V3*)decimal_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            __int128_t value = __int128_t(i * pow(10, 9) + i * pow(10, 8));
            data.push_back(value);
        }
        check_pb_col(decimal_data_type, *decimal_column.get());
    }
    // bitmap
    {
        DataTypePtr bitmap_data_type(std::make_shared<DataTypeBitMap>());
        auto bitmap_column = bitmap_data_type->create_column();
        std::vector<BitmapValue>& container = ((ColumnBitmap*)bitmap_column.get())->get_data();
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
        DataTypePtr hll_data_type(std::make_shared<DataTypeHLL>());
        auto hll_column = hll_data_type->create_column();
        std::vector<HyperLogLog>& container = ((ColumnHLL*)hll_column.get())->get_data();
        for (int i = 0; i < 4; ++i) {
            HyperLogLog hll;
            hll.update(i);
            container.push_back(hll);
        }
        check_pb_col(hll_data_type, *hll_column.get());
    }
    // quantilestate
    {
        DataTypePtr quantile_data_type(std::make_shared<DataTypeQuantileState>());
        auto quantile_column = quantile_data_type->create_column();
        std::vector<QuantileState>& container =
                ((ColumnQuantileState*)quantile_column.get())->get_data();
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
        DataTypePtr string_data_type(std::make_shared<DataTypeString>());
        DataTypePtr nullable_data_type(std::make_shared<DataTypeNullable>(string_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((ColumnNullable*)nullable_column.get())->insert_many_defaults(1024);
        check_pb_col(nullable_data_type, *nullable_column.get());
    }
    // nullable decimal
    {
        DataTypePtr decimal_data_type(doris::create_decimal(27, 9, true));
        DataTypePtr nullable_data_type(std::make_shared<DataTypeNullable>(decimal_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((ColumnNullable*)nullable_column.get())->insert_many_defaults(1024);
        check_pb_col(nullable_data_type, *nullable_column.get());
    }
    // int with 1024 batch size
    // ipv4
    {
        auto vec = ColumnIPv4 ::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        DataTypePtr data_type(std::make_shared<DataTypeIPv4>());
        check_pb_col(data_type, *vec.get());
    }
    // ipv6
    {
        auto vec = ColumnIPv6::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        DataTypePtr data_type(std::make_shared<DataTypeIPv6>());
        check_pb_col(data_type, *vec.get());
    }
}

TEST(DataTypeSerDeTest, DataTypeScalaSerDeTest) {
    serialize_and_deserialize_pb_test();
}

TEST(DataTypeSerDeTest, DataTypeRowStoreSerDeTest) {
    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    // ipv6
    {
        std::string ip = "5be8:dde9:7f0b:d5a7:bd01:b3be:9c69:573b";
        auto vec = ColumnIPv6::create();
        IPv6Value ipv6;
        EXPECT_TRUE(ipv6.from_string(ip));
        vec->insert(Field::create_field<TYPE_IPV6>(ipv6.value()));

        DataTypePtr data_type(std::make_shared<DataTypeIPv6>());
        auto serde = data_type->get_serde(0);
        JsonbWriterT<JsonbOutStream> jsonb_writer;
        Arena pool;
        jsonb_writer.writeStartObject();
        serde->write_one_cell_to_jsonb(*vec, jsonb_writer, pool, 0, 0, options);
        jsonb_writer.writeEndObject();
        auto jsonb_column = ColumnString::create();
        jsonb_column->insert_data(jsonb_writer.getOutput()->getBuffer(),
                                  jsonb_writer.getOutput()->getSize());
        StringRef jsonb_data = jsonb_column->get_data_at(0);
        const JsonbDocument* pdoc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(jsonb_data.data, jsonb_data.size, &pdoc);
        ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st.to_string();
        const JsonbDocument& doc = *pdoc;
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
        auto vec = ColumnIPv4::create();
        IPv4Value ipv4;
        EXPECT_TRUE(ipv4.from_string(ip));
        vec->insert(Field::create_field<TYPE_IPV4>(ipv4.value()));

        DataTypePtr data_type(std::make_shared<DataTypeIPv4>());
        auto serde = data_type->get_serde(0);
        JsonbWriterT<JsonbOutStream> jsonb_writer;
        Arena pool;
        jsonb_writer.writeStartObject();
        serde->write_one_cell_to_jsonb(*vec, jsonb_writer, pool, 0, 0, options);
        jsonb_writer.writeEndObject();
        auto jsonb_column = ColumnString::create();
        jsonb_column->insert_data(jsonb_writer.getOutput()->getBuffer(),
                                  jsonb_writer.getOutput()->getSize());
        StringRef jsonb_data = jsonb_column->get_data_at(0);
        const JsonbDocument* pdoc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(jsonb_data.data, jsonb_data.size, &pdoc);
        ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st.to_string();
        const JsonbDocument& doc = *pdoc;
        for (auto it = doc->begin(); it != doc->end(); ++it) {
            serde->read_one_cell_from_jsonb(*vec, it->value());
        }
        EXPECT_TRUE(vec->size() == 2);
        IPv4 data = vec->get_element(1);
        IPv4Value ipv4_value(data);
        EXPECT_EQ(ipv4_value.to_string(), ip);
    }
}

TEST(DataTypeSerDeTest, VariantWriteColumnToArrowSupportsLargeString) {
    auto variant_column = ColumnVariantV2::create();
    Slice value("\"variant value\"", 15);
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(DataTypeVariantV2SerDe()
                        .deserialize_one_cell_from_json(*variant_column, value, options)
                        .ok());

    auto data_type = std::make_shared<DataTypeVariant>();
    auto serde = data_type->get_serde(0);
    arrow::LargeStringBuilder builder;
    auto ctz = cctz::utc_time_zone();
    auto st = serde->write_column_to_arrow(*variant_column, nullptr, &builder, 0,
                                           variant_column->size(), ctz);
    EXPECT_TRUE(st.ok()) << st.to_string();

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto* string_array = dynamic_cast<arrow::LargeStringArray*>(array.get());
    ASSERT_NE(string_array, nullptr);
    ASSERT_EQ(string_array->length(), 1);
    EXPECT_EQ(string_array->GetString(0), "variant value");
}
} // namespace doris
