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

#include <arrow/api.h>
#include <cctz/time_zone.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <orc/OrcFile.hh>
#include <string>
#include <vector>

#include "gen_cpp/types.pb.h"
#include "util/jsonb_writer.h"
#include "util/slice.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_varbinary.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/data_types/serde/data_type_varbinary_serde.h"

namespace doris::vectorized {

static std::string make_bytes(size_t n, uint8_t seed = 0x31) {
    std::string s;
    s.resize(n);
    for (size_t i = 0; i < n; ++i) {
        s[i] = static_cast<char>(seed + i);
    }
    if (n >= 3) {
        s[n / 3] = '\0';
        s[(2 * n) / 3] = '\0';
    }
    return s;
}

class DataTypeVarbinarySerDeTest : public ::testing::Test {};

TEST_F(DataTypeVarbinarySerDeTest, Name) {
    DataTypeVarbinarySerDe serde;
    EXPECT_EQ(serde.get_name(), std::string("Varbinary"));
}

TEST_F(DataTypeVarbinarySerDeTest, JsonTextNotSupported) {
    DataTypeVarbinarySerDe serde;
    auto col = ColumnVarbinary::create();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());
    std::vector<std::string> vals = {make_bytes(0), make_bytes(5), std::string("ABC", 3)};
    for (auto& v : vals) {
        vb->insert_data(v.data(), v.size());
    }

    DataTypeSerDe::FormatOptions opt;

    // serialize_one_cell_to_json
    {
        auto out = ColumnString::create();
        VectorBufferWriter bw(*out);
        auto st = serde.serialize_one_cell_to_json(*col, 0, bw, opt);
        EXPECT_FALSE(st.ok());
    }
    // serialize_column_to_json
    {
        auto out = ColumnString::create();
        VectorBufferWriter bw(*out);
        auto st = serde.serialize_column_to_json(*col, 0, col->size(), bw, opt);
        EXPECT_FALSE(st.ok());
    }
    // deserialize_one_cell_from_json
    {
        std::string json = "deadbeef";
        Slice s(json.data(), json.size());
        auto st = serde.deserialize_one_cell_from_json(*vb, s, opt);
        EXPECT_FALSE(st.ok());
    }
    // deserialize_column_from_json_vector
    {
        std::vector<Slice> slices;
        std::string a = "aa";
        std::string b = "bb";
        slices.emplace_back(a.data(), a.size());
        slices.emplace_back(b.data(), b.size());
        uint64_t num = 0;
        auto st = serde.deserialize_column_from_json_vector(*vb, slices, &num, opt);
        EXPECT_FALSE(st.ok());
    }
}

TEST_F(DataTypeVarbinarySerDeTest, ProtobufNotSupported) {
    DataTypeVarbinarySerDe serde;
    auto col = ColumnVarbinary::create();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());
    std::string v = make_bytes(4);
    vb->insert_data(v.data(), v.size());

    PValues pv;
    auto st1 = serde.write_column_to_pb(*col, pv, 0, 1);
    EXPECT_FALSE(st1.ok());

    auto st2 = serde.read_column_from_pb(*vb, pv);
    EXPECT_FALSE(st2.ok());
}

TEST_F(DataTypeVarbinarySerDeTest, JsonbThrows) {
    DataTypeVarbinarySerDe serde;
    auto col = ColumnVarbinary::create();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());
    std::string v = make_bytes(6);
    vb->insert_data(v.data(), v.size());

    JsonbWriterT<JsonbOutStream> jw;
    Arena pool;

    EXPECT_THROW({ serde.write_one_cell_to_jsonb(*col, jw, pool, 0, 0); }, doris::Exception);
    EXPECT_THROW({ serde.read_one_cell_from_jsonb(*vb, nullptr); }, doris::Exception);
}

TEST_F(DataTypeVarbinarySerDeTest, MysqlTextAndBinaryAndConst) {
    DataTypeVarbinarySerDe serde;
    auto col = ColumnVarbinary::create();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());
    std::vector<std::string> vals = {make_bytes(1, 0x11), make_bytes(3, 0x22), make_bytes(5, 0x33)};
    for (auto& v : vals) {
        vb->insert_data(v.data(), v.size());
    }

    DataTypeSerDe::FormatOptions opt;

    // text protocol
    {
        MysqlRowBuffer<false> rb;
        for (int i = 0; i < static_cast<int>(vals.size()); ++i) {
            auto st = serde.write_column_to_mysql(*col, rb, i, false, opt);
            EXPECT_TRUE(st.ok()) << st.to_string();
        }
        EXPECT_GT(rb.length(), 0);
        // const column behavior: always pick row 0
        MysqlRowBuffer<false> rb_const;
        for (int i = 0; i < 5; ++i) {
            auto st = serde.write_column_to_mysql(*col, rb_const, i, true, opt);
            EXPECT_TRUE(st.ok()) << st.to_string();
        }
        EXPECT_GT(rb_const.length(), 0);
    }
    // binary protocol (smoke)
    {
        MysqlRowBuffer<true> rb;
        for (int i = 0; i < static_cast<int>(vals.size()); ++i) {
            auto st = serde.write_column_to_mysql(*col, rb, i, false, opt);
            EXPECT_TRUE(st.ok()) << st.to_string();
        }
        EXPECT_GT(rb.length(), 0);
    }
}

TEST_F(DataTypeVarbinarySerDeTest, ArrowNotSupported) {
    DataTypeVarbinarySerDe serde;
    auto col = ColumnVarbinary::create();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());
    std::string v = make_bytes(2);
    vb->insert_data(v.data(), v.size());

    auto builder = std::make_shared<arrow::BinaryBuilder>();
    cctz::time_zone tz;

    auto st = serde.write_column_to_arrow(*col, nullptr, builder.get(), 0, 1, tz);
    EXPECT_FALSE(st.ok());

    std::shared_ptr<arrow::Array> arr;
    ASSERT_TRUE(builder->Finish(&arr).ok());
    st = serde.read_column_from_arrow(*vb, arr.get(), 0, 1, tz);
    EXPECT_FALSE(st.ok());
}

TEST_F(DataTypeVarbinarySerDeTest, OrcNotSupported) {
    DataTypeVarbinarySerDe serde;
    auto col = ColumnVarbinary::create();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());
    std::string v = make_bytes(3);
    vb->insert_data(v.data(), v.size());

    Arena arena;
    auto batch = std::make_unique<orc::StringVectorBatch>(1, *orc::getDefaultPool());
    auto st = serde.write_column_to_orc("UTC", *col, nullptr, batch.get(), 0, 0, arena);
    EXPECT_FALSE(st.ok());
}

} // namespace doris::vectorized
