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
#include "vec/core/types.h"
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

TEST_F(DataTypeVarbinarySerDeTest, JsonTextBehavior) {
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
        auto st = serde.serialize_one_cell_to_json(*col, 1, bw, opt);
        EXPECT_TRUE(st.ok()) << st.to_string();
        bw.commit();
        auto written = assert_cast<ColumnString&>(*out).get_data_at(0);
        auto original = assert_cast<ColumnVarbinary&>(*col).get_data_at(1);
        EXPECT_EQ(written.size, original.size);
        EXPECT_EQ(memcmp(written.data, original.data, original.size), 0);
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
        EXPECT_TRUE(st.ok()) << st.to_string();
        auto inserted = vb->get_data_at(vb->size() - 1);
        EXPECT_EQ(inserted.size, json.size());
        EXPECT_EQ(std::memcmp(inserted.data, json.data(), json.size()), 0);
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
        const unsigned char* buf = reinterpret_cast<const unsigned char*>(rb.buf());
        EXPECT_EQ(buf[0], 1);
        EXPECT_EQ(buf[2], 3);
        MysqlRowBuffer<false> rb_const;
        for (int i = 0; i < 3; ++i) {
            auto st = serde.write_column_to_mysql(*col, rb_const, i, true, opt);
            EXPECT_TRUE(st.ok()) << st.to_string();
        }
        EXPECT_EQ(reinterpret_cast<const unsigned char*>(rb_const.buf())[0], 1);
    }
    // binary protocol (smoke)
    {
        MysqlRowBuffer<true> rb;
        rb.start_binary_row(vals.size());
        for (int i = 0; i < static_cast<int>(vals.size()); ++i) {
            auto st = serde.write_column_to_mysql(*col, rb, i, false, opt);
            EXPECT_TRUE(st.ok()) << st.to_string();
        }
        EXPECT_GT(rb.length(), 0);
    }
}

TEST_F(DataTypeVarbinarySerDeTest, ArrowWriteSupportedReadNotImplemented) {
    DataTypeVarbinarySerDe serde;
    auto col = ColumnVarbinary::create();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());
    std::string v = make_bytes(2);
    vb->insert_data(v.data(), v.size());

    auto builder = std::make_shared<arrow::BinaryBuilder>();
    cctz::time_zone tz;

    auto st = serde.write_column_to_arrow(*col, nullptr, builder.get(), 0, 1, tz);
    EXPECT_TRUE(st.ok()) << st.to_string();

    std::shared_ptr<arrow::Array> arr;
    ASSERT_TRUE(builder->Finish(&arr).ok());
    st = serde.read_column_from_arrow(*vb, arr.get(), 0, 1, tz);
    EXPECT_FALSE(st.ok());

    auto* binary_array = dynamic_cast<arrow::BinaryArray*>(arr.get());
    ASSERT_NE(binary_array, nullptr);
    ASSERT_EQ(binary_array->length(), 1);
    ASSERT_FALSE(binary_array->IsNull(0));
    auto view = vb->get_data_at(0);
    EXPECT_EQ(binary_array->value_length(0), static_cast<int>(view.size));
    const uint8_t* raw = binary_array->value_data()->data() + binary_array->value_offset(0);
    EXPECT_EQ(memcmp(raw, view.data, view.size), 0);
}

TEST_F(DataTypeVarbinarySerDeTest, OrcWriteSupported) {
    DataTypeVarbinarySerDe serde;
    auto col = ColumnVarbinary::create();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());
    std::string v = make_bytes(3);
    vb->insert_data(v.data(), v.size());

    Arena arena;
    auto batch = std::make_unique<orc::StringVectorBatch>(1, *orc::getDefaultPool());
    auto st = serde.write_column_to_orc("UTC", *col, nullptr, batch.get(), 0, 0, arena);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(batch->numElements, 0);
    auto st2 = serde.write_column_to_orc("UTC", *col, nullptr, batch.get(), 0, 1, arena);
    EXPECT_TRUE(st2.ok());
    EXPECT_EQ(batch->numElements, 1);
    EXPECT_EQ(batch->length[0], 3);
    EXPECT_EQ(memcmp(batch->data[0], v.data(), 3), 0);
}

TEST_F(DataTypeVarbinarySerDeTest, ArrowBinaryAndStringWithNullsAndInvalidType) {
    DataTypeVarbinarySerDe serde;
    auto col = ColumnVarbinary::create();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());
    std::vector<std::string> vals = {std::string("A", 1), std::string("BC", 2),
                                     std::string("XYZ", 3)};
    for (auto& v : vals) {
        vb->insert_data(v.data(), v.size());
    }

    // null map: second row null
    NullMap nulls = {0, 1, 0};
    cctz::time_zone tz;

    // BinaryBuilder path + nulls
    {
        auto builder = std::make_shared<arrow::BinaryBuilder>();
        auto st = serde.write_column_to_arrow(*col, &nulls, builder.get(), 0, vals.size(), tz);
        EXPECT_TRUE(st.ok()) << st.to_string();
        std::shared_ptr<arrow::Array> arr;
        ASSERT_TRUE(builder->Finish(&arr).ok());
        auto* bin = dynamic_cast<arrow::BinaryArray*>(arr.get());
        ASSERT_NE(bin, nullptr);
        ASSERT_EQ(bin->length(), static_cast<int>(vals.size()));
        ASSERT_FALSE(bin->IsNull(0));
        ASSERT_TRUE(bin->IsNull(1));
        ASSERT_FALSE(bin->IsNull(2));
        // row 0
        ASSERT_EQ(bin->value_length(0), static_cast<int>(vals[0].size()));
        const uint8_t* p0 = bin->value_data()->data() + bin->value_offset(0);
        EXPECT_EQ(memcmp(p0, vals[0].data(), vals[0].size()), 0);
        // row 2
        ASSERT_EQ(bin->value_length(2), static_cast<int>(vals[2].size()));
        const uint8_t* p2 = bin->value_data()->data() + bin->value_offset(2);
        EXPECT_EQ(memcmp(p2, vals[2].data(), vals[2].size()), 0);
    }

    // StringBuilder path (no nulls)
    {
        auto builder = std::make_shared<arrow::StringBuilder>();
        auto st = serde.write_column_to_arrow(*col, nullptr, builder.get(), 0, vals.size(), tz);
        EXPECT_TRUE(st.ok()) << st.to_string();
        std::shared_ptr<arrow::Array> arr;
        ASSERT_TRUE(builder->Finish(&arr).ok());
        auto* str_arr = dynamic_cast<arrow::StringArray*>(arr.get());
        ASSERT_NE(str_arr, nullptr);
        ASSERT_EQ(str_arr->length(), static_cast<int>(vals.size()));
        for (int i = 0; i < str_arr->length(); ++i) {
            ASSERT_FALSE(str_arr->IsNull(i));
            // StringArray shares BinaryArray API for raw buffer checks
            auto* bin = static_cast<arrow::BinaryArray*>(arr.get());
            ASSERT_EQ(bin->value_length(i), static_cast<int>(vals[i].size()));
            const uint8_t* pi = bin->value_data()->data() + bin->value_offset(i);
            EXPECT_EQ(memcmp(pi, vals[i].data(), vals[i].size()), 0);
        }
    }

    // Unsupported builder type
    {
        arrow::Int32Builder ib;
        auto st = serde.write_column_to_arrow(*col, nullptr, &ib, 0, 1, tz);
        EXPECT_FALSE(st.ok());
    }
}

TEST_F(DataTypeVarbinarySerDeTest, OrcWriteStartEndNullMapIgnoredAndEmptyRange) {
    DataTypeVarbinarySerDe serde;
    auto col = ColumnVarbinary::create();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());
    std::vector<std::string> vals = {std::string("aa", 2), std::string("bbb", 3),
                                     std::string("cccc", 4)};
    for (auto& v : vals) {
        vb->insert_data(v.data(), v.size());
    }

    Arena arena;
    auto batch = std::make_unique<orc::StringVectorBatch>(8, *orc::getDefaultPool());

    // Provide a null_map but implementation ignores it; ensure data still written.
    NullMap nulls = {0, 1, 0};
    auto st = serde.write_column_to_orc("UTC", *col, &nulls, batch.get(), /*start=*/1, /*end=*/3,
                                        arena);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(batch->numElements, 2);
    // rows 1 and 2 are filled
    EXPECT_EQ(batch->length[1], static_cast<long>(vals[1].size()));
    EXPECT_EQ(memcmp(batch->data[1], vals[1].data(), vals[1].size()), 0);
    EXPECT_EQ(batch->length[2], static_cast<long>(vals[2].size()));
    EXPECT_EQ(memcmp(batch->data[2], vals[2].data(), vals[2].size()), 0);

    // Empty range should set numElements = 0
    auto st2 = serde.write_column_to_orc("UTC", *col, nullptr, batch.get(), /*start=*/3, /*end=*/3,
                                         arena);
    EXPECT_TRUE(st2.ok());
    EXPECT_EQ(batch->numElements, 0);
}

TEST_F(DataTypeVarbinarySerDeTest, SerializeOneCellToJsonWithRawBytes) {
    DataTypeVarbinarySerDe serde;
    auto col = ColumnVarbinary::create();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());

    // Test binary data with embedded NUL character
    std::string v = std::string("A\0B", 3);
    vb->insert_data(v.data(), v.size());

    DataTypeSerDe::FormatOptions opt;
    auto out = ColumnString::create();
    VectorBufferWriter bw(*out);
    auto st = serde.serialize_one_cell_to_json(*col, 0, bw, opt);
    EXPECT_TRUE(st.ok()) << st.to_string();
    bw.commit();

    auto written = assert_cast<ColumnString&>(*out).get_data_at(0);
    EXPECT_EQ(written.size, v.size());
    EXPECT_EQ(memcmp(written.data, v.data(), v.size()), 0);
}

TEST_F(DataTypeVarbinarySerDeTest, DeserializeOneCellFromJsonWithRawBytes) {
    DataTypeVarbinarySerDe serde;
    auto col = ColumnVarbinary::create();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());

    // Test 1: String with quotes and backslash, inserted as-is
    {
        std::string json = R"("a\b")";
        Slice s(json.data(), json.size());
        DataTypeSerDe::FormatOptions opt;
        auto st = serde.deserialize_one_cell_from_json(*vb, s, opt);
        EXPECT_TRUE(st.ok()) << st.to_string();
        auto inserted = vb->get_data_at(vb->size() - 1);
        EXPECT_EQ(inserted.size, json.size());
        EXPECT_EQ(memcmp(inserted.data, json.data(), json.size()), 0);
    }

    // Test 2: Pure binary data
    {
        std::string binary_data = std::string("\x01\x02\x03\x00\xFF", 5);
        Slice s(binary_data.data(), binary_data.size());
        DataTypeSerDe::FormatOptions opt;
        auto st = serde.deserialize_one_cell_from_json(*vb, s, opt);
        EXPECT_TRUE(st.ok()) << st.to_string();
        auto inserted = vb->get_data_at(vb->size() - 1);
        EXPECT_EQ(inserted.size, binary_data.size());
        EXPECT_EQ(memcmp(inserted.data, binary_data.data(), binary_data.size()), 0);
    }
}

} // namespace doris::vectorized
