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

#include "storage/row_cursor.h"

#include <gtest/gtest.h>

#include <cmath>
#include <cstring>
#include <limits>

#include "common/object_pool.h"
#include "core/decimal12.h"
#include "core/extended_types.h"
#include "core/field.h"
#include "core/packed_int128.h"
#include "core/uint24.h"
#include "core/wide_integer.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet/tablet_schema_helper.h"
#include "util/debug_util.h"
#include "util/slice.h"

namespace doris {

// Helper functions for column types not covered by tablet_schema_helper.h
static TabletColumnPtr create_float_key(int32_t id, bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_FLOAT;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 4;
    column->_index_length = 4;
    return column;
}

static TabletColumnPtr create_double_key(int32_t id, bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_DOUBLE;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 8;
    column->_index_length = 8;
    return column;
}

static TabletColumnPtr create_bigint_key(int32_t id, bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_BIGINT;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 8;
    column->_index_length = 8;
    return column;
}

static TabletColumnPtr create_date_key(int32_t id, bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_DATE;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 3;
    column->_index_length = 3;
    return column;
}

static TabletColumnPtr create_datev2_key(int32_t id, bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_DATEV2;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 4;
    column->_index_length = 4;
    return column;
}

static TabletColumnPtr create_datetime_key(int32_t id, bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_DATETIME;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 8;
    column->_index_length = 8;
    return column;
}

static TabletColumnPtr create_decimal_key(int32_t id, bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_DECIMAL;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 12;
    column->_index_length = 12;
    column->_precision = 6;
    column->_frac = 3;
    return column;
}

static TabletColumnPtr create_datetimev2_key(int32_t id, int32_t scale = 6,
                                             bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_DATETIMEV2;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 8;
    column->_index_length = 8;
    column->_precision = 20 + scale;
    column->_frac = scale;
    return column;
}

static TabletColumnPtr create_timestamptz_key(int32_t id, bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 8;
    column->_index_length = 8;
    return column;
}

static TabletColumnPtr create_decimal32_key(int32_t id, bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_DECIMAL32;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 4;
    column->_index_length = 4;
    column->_precision = 9;
    column->_frac = 3;
    return column;
}

static TabletColumnPtr create_decimal64_key(int32_t id, bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_DECIMAL64;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 8;
    column->_index_length = 8;
    column->_precision = 18;
    column->_frac = 6;
    return column;
}

static TabletColumnPtr create_decimal128i_key(int32_t id, bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_DECIMAL128I;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 16;
    column->_index_length = 16;
    column->_precision = 38;
    column->_frac = 9;
    return column;
}

static TabletColumnPtr create_decimal256_key(int32_t id, bool is_nullable = true) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_DECIMAL256;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 32;
    column->_index_length = 32;
    column->_precision = 76;
    column->_frac = 18;
    return column;
}

void set_tablet_schema_for_scan_key(TabletSchemaSPtr tablet_schema) {
    TabletSchemaPB tablet_schema_pb;

    ColumnPB* column_1 = tablet_schema_pb.add_column();
    column_1->set_unique_id(1);
    column_1->set_name("column_1");
    column_1->set_type("CHAR");
    column_1->set_is_key(true);
    column_1->set_is_nullable(true);
    column_1->set_length(4);
    column_1->set_index_length(4);
    column_1->set_default_value("char");

    ColumnPB* column_2 = tablet_schema_pb.add_column();
    column_2->set_unique_id(2);
    column_2->set_name("column_2");
    column_2->set_type("VARCHAR");
    column_2->set_is_key(true);
    column_2->set_is_nullable(true);
    column_2->set_length(16 + OLAP_VARCHAR_MAX_BYTES);
    column_2->set_index_length(20);

    ColumnPB* column_3 = tablet_schema_pb.add_column();
    column_3->set_unique_id(3);
    column_3->set_name("column_3");
    column_3->set_type("LARGEINT");
    column_3->set_is_nullable(true);
    column_3->set_length(16);
    column_3->set_aggregation("MAX");
    column_3->set_is_key(false);

    ColumnPB* column_4 = tablet_schema_pb.add_column();
    column_4->set_unique_id(9);
    column_4->set_name("column_4");
    column_4->set_type("DECIMAL");
    column_4->set_is_nullable(true);
    column_4->set_length(12);
    column_4->set_aggregation("MIN");
    column_4->set_is_key(false);

    tablet_schema->init_from_pb(tablet_schema_pb);
}

class TestRowCursor : public testing::Test {
public:
    TestRowCursor() { _arena.reset(new Arena()); }

    virtual void SetUp() {}
    virtual void TearDown() {}

    std::unique_ptr<Arena> _arena;
};

TEST_F(TestRowCursor, InitRowCursorWithScanKey) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    set_tablet_schema_for_scan_key(tablet_schema);

    std::vector<Field> scan_keys;
    scan_keys.push_back(Field::create_field<TYPE_STRING>(String("char_exceed_length")));
    scan_keys.push_back(Field::create_field<TYPE_STRING>(String("varchar_exceed_length")));

    RowCursor row;
    Status res = row.init_scan_key(tablet_schema, scan_keys);
    EXPECT_EQ(res, Status::OK());
    EXPECT_EQ(row.field_count(), 2);
}

TEST_F(TestRowCursor, encode_key) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_int_key(0));
    tablet_schema->_cols.push_back(create_int_key(1));
    tablet_schema->_cols.push_back(create_int_key(2));
    tablet_schema->_cols.push_back(create_int_value(3));
    tablet_schema->_num_columns = 4;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 3;

    // test encoding with padding
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 2));

        row.mutable_field(0) = Field::create_field<TYPE_INT>(int32_t(12345));
        row.mutable_field(1) = Field::create_field<TYPE_INT>(int32_t(54321));

        {
            std::string buf;
            row.encode_key_with_padding(&buf, 3, true);
            EXPECT_STREQ("0280003039028000D43100", hexdump(buf.c_str(), buf.size()).c_str());
        }

        // test with null
        row.mutable_field(0) = Field::create_field<TYPE_INT>(int32_t(54321));
        row.mutable_field(1) = Field(PrimitiveType::TYPE_NULL); // null

        {
            std::string buf;
            row.encode_key_with_padding(&buf, 3, false);
            EXPECT_STREQ("028000D43101FF", hexdump(buf.c_str(), buf.size()).c_str());
        }
        // encode key
        {
            std::string buf;
            row.encode_key(&buf, 2);
            EXPECT_STREQ("028000D43101", hexdump(buf.c_str(), buf.size()).c_str());
        }
    }
}

// ========================================================================
// Comprehensive encode_key tests covering multiple type combinations.
// Expected values are pre-computed string constants. After any refactoring
// of RowCursor, these tests ensure encoding behavior is preserved.
//
// Encoding rules summary:
//   Signed integers: XOR sign bit, then big-endian
//   Unsigned integers (DATE, DATEV2): just big-endian, no sign-bit XOR
//   Float/Double: canonicalize NaN, sortable bit transform, XOR sign bit, big-endian
//   DECIMAL: encode integer(int64) as BIGINT + fraction(int32) as INT
//   CHAR: zero-padded to col_length; encode_ascending uses index_size bytes,
//         full_encode uses all col_length bytes
//   VARCHAR/STRING: encode_ascending uses min(index_size, len) bytes,
//                   full_encode uses all bytes
//   Key markers: NORMAL=0x02, NULL_FIRST=0x01, MINIMAL=0x00, MAXIMAL=0xFF
// ========================================================================

// Test 1: INT + FLOAT + CHAR(8)
TEST_F(TestRowCursor, encode_key_int_float_char) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_int_key(0));
    tablet_schema->_cols.push_back(create_float_key(1));
    tablet_schema->_cols.push_back(create_char_key(2)); // CHAR(8), index_length=1
    tablet_schema->_num_columns = 3;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 3;

    // All 3 keys present: INT(12345), FLOAT(3.14f), CHAR('ab')
    // _encode_field for CHAR pads to col_length=8 with \0
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 3));

        row.mutable_field(0) = Field::create_field<TYPE_INT>(int32_t(12345));
        row.mutable_field(1) = Field::create_field<TYPE_FLOAT>(3.14f);
        row.mutable_field(2) = Field::create_field<TYPE_STRING>(String("ab"));

        // encode_key (encode_ascending): CHAR uses index_size=1 byte
        {
            std::string buf;
            row.encode_key(&buf, 3);
            EXPECT_STREQ("028000303902C048F5C30261", hexdump(buf.c_str(), buf.size()).c_str());
        }

        // encode_key<true> (full_encode_ascending): CHAR uses all 8 bytes (padded)
        {
            std::string buf;
            row.encode_key<true>(&buf, 3);
            EXPECT_STREQ("028000303902C048F5C3026162000000000000",
                         hexdump(buf.c_str(), buf.size()).c_str());
        }
    }

    // Padding: only 2 keys initialized, 3rd padded as KEY_MINIMAL_MARKER(0x00)
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 2));

        row.mutable_field(0) = Field::create_field<TYPE_INT>(int32_t(12345));
        row.mutable_field(1) = Field::create_field<TYPE_FLOAT>(3.14f);

        {
            std::string buf;
            row.encode_key_with_padding(&buf, 3, true);
            EXPECT_STREQ("028000303902C048F5C300", hexdump(buf.c_str(), buf.size()).c_str());
        }

        // Padding maximal marker (0xFF)
        {
            std::string buf;
            row.encode_key_with_padding(&buf, 3, false);
            EXPECT_STREQ("028000303902C048F5C3FF", hexdump(buf.c_str(), buf.size()).c_str());
        }
    }

    // Null: INT(12345) present, FLOAT null, CHAR('ab') present
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 3));

        row.mutable_field(0) = Field::create_field<TYPE_INT>(int32_t(12345));
        // field(1) stays null (default from init)
        row.mutable_field(2) = Field::create_field<TYPE_STRING>(String("ab"));

        {
            std::string buf;
            row.encode_key(&buf, 3);
            EXPECT_STREQ("0280003039010261", hexdump(buf.c_str(), buf.size()).c_str());
        }
    }
}

// Test 2: INT + DATE + VARCHAR
TEST_F(TestRowCursor, encode_key_int_date_varchar) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_int_key(0));
    tablet_schema->_cols.push_back(create_date_key(1));
    tablet_schema->_cols.push_back(create_varchar_key(2)); // index_length=4
    tablet_schema->_num_columns = 3;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 3;

    RowCursor row;
    static_cast<void>(row.init(tablet_schema, 3));

    row.mutable_field(0) = Field::create_field<TYPE_INT>(int32_t(12345));
    // DATE(2020-01-01): raw uint24_t = 2020*512 + 1*32 + 1 = 1034273
    row.mutable_field(1) =
            Field::create_field_from_olap_value<TYPE_DATE>(uint24_t(2020 * 512 + 1 * 32 + 1));
    row.mutable_field(2) = Field::create_field<TYPE_STRING>(String("hello"));

    // encode_key: VARCHAR('hello') truncated to 4 bytes
    {
        std::string buf;
        row.encode_key(&buf, 3);
        EXPECT_STREQ("0280003039020FC8210268656C6C", hexdump(buf.c_str(), buf.size()).c_str());
    }

    // full_encode: VARCHAR('hello') all 5 bytes
    {
        std::string buf;
        row.encode_key<true>(&buf, 3);
        EXPECT_STREQ("0280003039020FC8210268656C6C6F", hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// Test 3: DOUBLE(NaN) + DECIMAL + STRING
TEST_F(TestRowCursor, encode_key_double_nan_decimal_string) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_double_key(0));
    tablet_schema->_cols.push_back(create_decimal_key(1));
    tablet_schema->_cols.push_back(create_string_key(2)); // index_length=4
    tablet_schema->_num_columns = 3;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 3;

    RowCursor row;
    static_cast<void>(row.init(tablet_schema, 3));

    row.mutable_field(0) =
            Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN());
    // DECIMAL(123.456000000): stored as decimal12_t{123, 456000000}
    row.mutable_field(1) =
            Field::create_field_from_olap_value<TYPE_DECIMALV2>(decimal12_t {123, 456000000});
    row.mutable_field(2) = Field::create_field<TYPE_STRING>(String("hello world"));

    // encode_key: STRING truncated to 4 bytes
    {
        std::string buf;
        row.encode_key(&buf, 3);
        EXPECT_STREQ("02FFF800000000000002800000000000007B9B2E02000268656C6C",
                     hexdump(buf.c_str(), buf.size()).c_str());
    }

    // full_encode: STRING all 11 bytes
    {
        std::string buf;
        row.encode_key<true>(&buf, 3);
        EXPECT_STREQ("02FFF800000000000002800000000000007B9B2E02000268656C6C6F20776F726C64",
                     hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// Test 4: BIGINT + DATEV2 + CHAR(8, full)
TEST_F(TestRowCursor, encode_key_bigint_datev2_char) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_bigint_key(0));
    tablet_schema->_cols.push_back(create_datev2_key(1));
    tablet_schema->_cols.push_back(create_char_key(2)); // CHAR(8), index_length=1
    tablet_schema->_num_columns = 3;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 3;

    RowCursor row;
    static_cast<void>(row.init(tablet_schema, 3));

    row.mutable_field(0) = Field::create_field<TYPE_BIGINT>(int64_t(9999999999LL));
    // DATEV2(2024-12-31): packed = (2024<<9)|(12<<5)|31 = 1036703
    {
        uint32_t packed = (2024 << 9) | (12 << 5) | 31;
        row.mutable_field(1) = Field::create_field<TYPE_DATEV2>(packed);
    }
    row.mutable_field(2) = Field::create_field<TYPE_STRING>(String("abcdefgh"));

    // encode_key: CHAR index_size=1 -> only 'a' (0x61)
    {
        std::string buf;
        row.encode_key(&buf, 3);
        EXPECT_STREQ("0280000002540BE3FF02000FD19F0261", hexdump(buf.c_str(), buf.size()).c_str());
    }

    // full_encode: CHAR all 8 bytes
    {
        std::string buf;
        row.encode_key<true>(&buf, 3);
        EXPECT_STREQ("0280000002540BE3FF02000FD19F026162636465666768",
                     hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// Test 5: FLOAT(inf) + DATETIME + VARCHAR
TEST_F(TestRowCursor, encode_key_float_inf_datetime_varchar) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_float_key(0));
    tablet_schema->_cols.push_back(create_datetime_key(1));
    tablet_schema->_cols.push_back(create_varchar_key(2)); // index_length=4
    tablet_schema->_num_columns = 3;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 3;

    RowCursor row;
    static_cast<void>(row.init(tablet_schema, 3));

    row.mutable_field(0) = Field::create_field<TYPE_FLOAT>(std::numeric_limits<float>::infinity());
    // DATETIME(2020-01-01 12:00:00) = 20200101120000 in olap datetime format
    row.mutable_field(1) =
            Field::create_field_from_olap_value<TYPE_DATETIME>(uint64_t(20200101120000ULL));
    row.mutable_field(2) = Field::create_field<TYPE_STRING>(String("ab"));

    // encode_key: VARCHAR('ab') only 2 bytes (< index_size=4, not truncated)
    {
        std::string buf;
        row.encode_key(&buf, 3);
        EXPECT_STREQ("02FF800000028000125F33DA0800026162",
                     hexdump(buf.c_str(), buf.size()).c_str());
    }

    // full_encode: same result since VARCHAR('ab') is only 2 bytes
    {
        std::string buf;
        row.encode_key<true>(&buf, 3);
        EXPECT_STREQ("02FF800000028000125F33DA0800026162",
                     hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// Test: encode_key_with_padding for MOW mode (is_mow=true)
TEST_F(TestRowCursor, encode_key_with_padding_mow) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_int_key(0));
    tablet_schema->_cols.push_back(create_int_key(1));
    tablet_schema->_cols.push_back(create_int_key(2));
    tablet_schema->_num_columns = 3;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 3;

    RowCursor row;
    static_cast<void>(row.init(tablet_schema, 2));

    row.mutable_field(0) = Field::create_field<TYPE_INT>(int32_t(12345));
    row.mutable_field(1) = Field::create_field<TYPE_INT>(int32_t(54321));

    // MOW padding_minimal=false -> KEY_NORMAL_NEXT_MARKER(0x03) instead of 0xFF
    {
        std::string buf;
        row.encode_key_with_padding<true>(&buf, 3, false);
        EXPECT_STREQ("0280003039028000D43103", hexdump(buf.c_str(), buf.size()).c_str());
    }

    // MOW padding_minimal=true -> KEY_MINIMAL_MARKER(0x00)
    {
        std::string buf;
        row.encode_key_with_padding<true>(&buf, 3, true);
        EXPECT_STREQ("0280003039028000D43100", hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// Test: negative values for various types
TEST_F(TestRowCursor, encode_key_negative_values) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_int_key(0));
    tablet_schema->_cols.push_back(create_double_key(1));
    tablet_schema->_cols.push_back(create_decimal_key(2));
    tablet_schema->_num_columns = 3;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 3;

    RowCursor row;
    static_cast<void>(row.init(tablet_schema, 3));

    row.mutable_field(0) = Field::create_field<TYPE_INT>(int32_t(-12345));
    row.mutable_field(1) = Field::create_field<TYPE_DOUBLE>(-1.0);
    // DECIMAL(-123, -456000000) representing -123.456000000
    row.mutable_field(2) =
            Field::create_field_from_olap_value<TYPE_DECIMALV2>(decimal12_t {-123, -456000000});

    {
        std::string buf;
        row.encode_key(&buf, 3);
        EXPECT_STREQ("027FFFCFC702400FFFFFFFFFFFFF027FFFFFFFFFFFFF8564D1FE00",
                     hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// Test: float special values (-inf, NaN) and boundary integers
TEST_F(TestRowCursor, encode_key_float_special_values) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_float_key(0));
    tablet_schema->_cols.push_back(create_int_key(1));
    tablet_schema->_num_columns = 2;
    tablet_schema->_num_key_columns = 2;
    tablet_schema->_num_short_key_columns = 2;

    // Test -infinity
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 2));
        row.mutable_field(0) =
                Field::create_field<TYPE_FLOAT>(-std::numeric_limits<float>::infinity());
        row.mutable_field(1) = Field::create_field<TYPE_INT>(int32_t(0));

        std::string buf;
        row.encode_key(&buf, 2);
        EXPECT_STREQ("02007FFFFF0280000000", hexdump(buf.c_str(), buf.size()).c_str());
    }

    // Test INT boundary values
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 2));
        row.mutable_field(0) =
                Field::create_field<TYPE_FLOAT>(std::numeric_limits<float>::quiet_NaN());
        row.mutable_field(1) = Field::create_field<TYPE_INT>(std::numeric_limits<int32_t>::max());

        std::string buf;
        row.encode_key(&buf, 2);
        EXPECT_STREQ("02FFC0000002FFFFFFFF", hexdump(buf.c_str(), buf.size()).c_str());
    }

    // Test INT_MIN
    {
        TabletSchemaSPtr schema2 = std::make_shared<TabletSchema>();
        schema2->_cols.push_back(create_int_key(0));
        schema2->_num_columns = 1;
        schema2->_num_key_columns = 1;
        schema2->_num_short_key_columns = 1;

        RowCursor row2;
        static_cast<void>(row2.init(schema2, 1));
        row2.mutable_field(0) = Field::create_field<TYPE_INT>(std::numeric_limits<int32_t>::min());

        std::string buf;
        row2.encode_key(&buf, 1);
        EXPECT_STREQ("0200000000", hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// Test: all null keys
TEST_F(TestRowCursor, encode_key_all_null) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_int_key(0));
    tablet_schema->_cols.push_back(create_float_key(1));
    tablet_schema->_cols.push_back(create_varchar_key(2));
    tablet_schema->_num_columns = 3;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 3;

    RowCursor row;
    static_cast<void>(row.init(tablet_schema, 3));
    // All fields stay null (default from init)

    {
        std::string buf;
        row.encode_key(&buf, 3);
        // All null: three 0x01 markers
        EXPECT_STREQ("010101", hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// DATETIME v1 encoding
TEST_F(TestRowCursor, encode_key_datetime_v1) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_datetime_key(0));
    tablet_schema->_num_columns = 1;
    tablet_schema->_num_key_columns = 1;
    tablet_schema->_num_short_key_columns = 1;

    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        row.mutable_field(0) =
                Field::create_field_from_olap_value<TYPE_DATETIME>(uint64_t(20241231123045ULL));

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("0280001268C7641265", hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// DATETIMEV2 encoding at various precisions
TEST_F(TestRowCursor, encode_key_datetimev2) {
    // --- DATETIMEV2(0) ---
    {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->_cols.push_back(create_datetimev2_key(0, 0));
        tablet_schema->_num_columns = 1;
        tablet_schema->_num_key_columns = 1;
        tablet_schema->_num_short_key_columns = 1;

        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        uint64_t packed = (uint64_t(2024) << 46) | (uint64_t(12) << 42) | (uint64_t(31) << 37) |
                          (uint64_t(12) << 32) | (uint64_t(30) << 26) | (uint64_t(45) << 20) |
                          uint64_t(0);
        row.mutable_field(0) = Field::create_field<TYPE_DATETIMEV2>(packed);

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("0201FA33EC7AD00000", hexdump(buf.c_str(), buf.size()).c_str());
    }

    // --- DATETIMEV2(3) ---
    {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->_cols.push_back(create_datetimev2_key(0, 3));
        tablet_schema->_num_columns = 1;
        tablet_schema->_num_key_columns = 1;
        tablet_schema->_num_short_key_columns = 1;

        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        uint64_t packed = (uint64_t(2024) << 46) | (uint64_t(12) << 42) | (uint64_t(31) << 37) |
                          (uint64_t(12) << 32) | (uint64_t(30) << 26) | (uint64_t(45) << 20) |
                          uint64_t(123000);
        row.mutable_field(0) = Field::create_field<TYPE_DATETIMEV2>(packed);

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("0201FA33EC7AD1E078", hexdump(buf.c_str(), buf.size()).c_str());
    }

    // --- DATETIMEV2(6) ---
    {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->_cols.push_back(create_datetimev2_key(0, 6));
        tablet_schema->_num_columns = 1;
        tablet_schema->_num_key_columns = 1;
        tablet_schema->_num_short_key_columns = 1;

        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        uint64_t packed = (uint64_t(2024) << 46) | (uint64_t(12) << 42) | (uint64_t(31) << 37) |
                          (uint64_t(12) << 32) | (uint64_t(30) << 26) | (uint64_t(45) << 20) |
                          uint64_t(123456);
        row.mutable_field(0) = Field::create_field<TYPE_DATETIMEV2>(packed);

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("0201FA33EC7AD1E240", hexdump(buf.c_str(), buf.size()).c_str());
    }

    // Null DATETIMEV2
    {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->_cols.push_back(create_datetimev2_key(0, 6));
        tablet_schema->_num_columns = 1;
        tablet_schema->_num_key_columns = 1;
        tablet_schema->_num_short_key_columns = 1;

        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        // field(0) stays null

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("01", hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// TIMESTAMPTZ encoding
TEST_F(TestRowCursor, encode_key_timestamptz) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_timestamptz_key(0));
    tablet_schema->_num_columns = 1;
    tablet_schema->_num_key_columns = 1;
    tablet_schema->_num_short_key_columns = 1;

    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        // TimestampTzValue wraps DateV2Value<DateTimeV2ValueType> internally.
        // The uint64_t is the packed DateTimeV2 representation.
        row.mutable_field(0) =
                Field::create_field<TYPE_TIMESTAMPTZ>(TimestampTzValue(1704067200000000ULL));

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("0200060DD710212000", hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// DECIMAL32 encoding
TEST_F(TestRowCursor, encode_key_decimal32) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_decimal32_key(0));
    tablet_schema->_num_columns = 1;
    tablet_schema->_num_key_columns = 1;
    tablet_schema->_num_short_key_columns = 1;

    // Positive: 12345
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        row.mutable_field(0) = Field::create_field<TYPE_DECIMAL32>(Decimal32(int32_t(12345)));

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("0280003039", hexdump(buf.c_str(), buf.size()).c_str());
    }

    // Negative: -12345
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        row.mutable_field(0) = Field::create_field<TYPE_DECIMAL32>(Decimal32(int32_t(-12345)));

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("027FFFCFC7", hexdump(buf.c_str(), buf.size()).c_str());
    }

    // Zero
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        row.mutable_field(0) = Field::create_field<TYPE_DECIMAL32>(Decimal32(int32_t(0)));

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("0280000000", hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// DECIMAL64 encoding
TEST_F(TestRowCursor, encode_key_decimal64) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_decimal64_key(0));
    tablet_schema->_num_columns = 1;
    tablet_schema->_num_key_columns = 1;
    tablet_schema->_num_short_key_columns = 1;

    // Positive: 9999999999
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        row.mutable_field(0) =
                Field::create_field<TYPE_DECIMAL64>(Decimal64(int64_t(9999999999LL)));

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("0280000002540BE3FF", hexdump(buf.c_str(), buf.size()).c_str());
    }

    // Negative: -123456789
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        row.mutable_field(0) =
                Field::create_field<TYPE_DECIMAL64>(Decimal64(int64_t(-123456789LL)));

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("027FFFFFFFF8A432EB", hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// DECIMAL128I encoding
TEST_F(TestRowCursor, encode_key_decimal128i) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_decimal128i_key(0));
    tablet_schema->_num_columns = 1;
    tablet_schema->_num_key_columns = 1;
    tablet_schema->_num_short_key_columns = 1;

    // Positive: 123456789012345678
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        int128_t val = static_cast<int128_t>(123456789012345678LL);
        row.mutable_field(0) = Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(val));

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("02800000000000000001B69B4BA630F34E",
                     hexdump(buf.c_str(), buf.size()).c_str());
    }

    // Negative: -123456789012345678
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        int128_t val = static_cast<int128_t>(-123456789012345678LL);
        row.mutable_field(0) = Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(val));

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("027FFFFFFFFFFFFFFFFE4964B459CF0CB2",
                     hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// DECIMAL256 encoding
TEST_F(TestRowCursor, encode_key_decimal256) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_decimal256_key(0));
    tablet_schema->_num_columns = 1;
    tablet_schema->_num_key_columns = 1;
    tablet_schema->_num_short_key_columns = 1;

    // Positive: 123456789012345678901234567890
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        wide::Int256 val({0xC373E0EE4E3F0AD2ULL, 0x000000018EE90FF6ULL, 0ULL, 0ULL});
        row.mutable_field(0) = Field::create_field<TYPE_DECIMAL256>(Decimal256(val));

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("0280000000000000000000000000000000000000018EE90FF6C373E0EE4E3F0AD2",
                     hexdump(buf.c_str(), buf.size()).c_str());
    }

    // Negative: -123456789012345678901234567890
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 1));
        wide::Int256 val({0x3C8C1F11B1C0F52EULL, 0xFFFFFFFE7116F009ULL, 0xFFFFFFFFFFFFFFFFULL,
                          0xFFFFFFFFFFFFFFFFULL});
        row.mutable_field(0) = Field::create_field<TYPE_DECIMAL256>(Decimal256(val));

        std::string buf;
        row.encode_key(&buf, 1);
        EXPECT_STREQ("027FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE7116F0093C8C1F11B1C0F52E",
                     hexdump(buf.c_str(), buf.size()).c_str());
    }
}

// Combined test: DATETIMEV2 + DECIMAL128I + DECIMAL32
TEST_F(TestRowCursor, encode_key_datetimev2_decimal128i_decimal32) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_datetimev2_key(0, 3));
    tablet_schema->_cols.push_back(create_decimal128i_key(1));
    tablet_schema->_cols.push_back(create_decimal32_key(2));
    tablet_schema->_num_columns = 3;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 3;

    RowCursor row;
    static_cast<void>(row.init(tablet_schema, 3));

    // DATETIMEV2(3): 2024-12-31 12:30:45.123
    {
        uint64_t packed = (uint64_t(2024) << 46) | (uint64_t(12) << 42) | (uint64_t(31) << 37) |
                          (uint64_t(12) << 32) | (uint64_t(30) << 26) | (uint64_t(45) << 20) |
                          uint64_t(123000);
        row.mutable_field(0) = Field::create_field<TYPE_DATETIMEV2>(packed);
    }
    // DECIMAL128I: 123456789012345678
    {
        int128_t val = static_cast<int128_t>(123456789012345678LL);
        row.mutable_field(1) = Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(val));
    }
    // DECIMAL32: -12345
    row.mutable_field(2) = Field::create_field<TYPE_DECIMAL32>(Decimal32(int32_t(-12345)));

    {
        std::string buf;
        row.encode_key(&buf, 3);
        EXPECT_STREQ("0201FA33EC7AD1E07802800000000000000001B69B4BA630F34E027FFFCFC7",
                     hexdump(buf.c_str(), buf.size()).c_str());
    }
}

} // namespace doris
