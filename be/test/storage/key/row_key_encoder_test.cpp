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

// The test builds TabletSchema/TabletColumn objects field by field, the same
// way the existing storage unit tests do (tablet_schema_helper.cpp does the same).
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#define private public
#define protected public
#pragma clang diagnostic pop

#include "storage/key/row_key_encoder.h"

#include <gtest/gtest.h>

#include <array>
#include <optional>
#include <string>
#include <vector>

#include "common/consts.h"
#include "core/block/block.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet/tablet_schema_helper.h"
#include "storage/utils.h"

namespace doris {
namespace {

constexpr uint8_t kNull = KeyConsts::KEY_NULL_FIRST_MARKER; // 0x01
constexpr uint8_t kNormal = KeyConsts::KEY_NORMAL_MARKER;   // 0x02
constexpr uint8_t kMinimal = KeyConsts::KEY_MINIMAL_MARKER; // 0x00

// A hidden sequence column is identified by its reserved name.
TabletColumnPtr create_seq_col(int32_t uid) {
    auto c = std::make_shared<TabletColumn>();
    c->_unique_id = uid;
    c->_col_name = SEQUENCE_COL;
    c->_type = FieldType::OLAP_FIELD_TYPE_INT;
    c->_is_key = false;
    c->_is_nullable = true;
    c->_length = 4;
    c->_index_length = 4;
    return c;
}

void fill_int(MutableColumns& cols, uint32_t cid, const std::vector<std::optional<int32_t>>& vals) {
    for (const auto& v : vals) {
        if (v.has_value()) {
            int32_t x = *v;
            cols[cid]->insert_data(reinterpret_cast<const char*>(&x), sizeof(x));
        } else {
            cols[cid]->insert_default(); // NULL for a nullable column
        }
    }
}

void fill_char(MutableColumns& cols, uint32_t cid, const std::vector<std::string>& vals) {
    for (const auto& s : vals) {
        cols[cid]->insert_data(s.data(), s.size());
    }
}

uint8_t byte_at(const std::string& s, size_t i) {
    return static_cast<uint8_t>(s[i]);
}

// key(0), key(1), value(2): two int key columns.
TabletSchemaSPtr two_int_key_schema() {
    auto s = std::make_shared<TabletSchema>();
    s->append_column(*create_int_key(0));
    s->append_column(*create_int_key(1));
    s->append_column(*create_int_value(2));
    s->_keys_type = DUP_KEYS;
    s->_num_short_key_columns = 2;
    return s;
}

// char(8) key (index_length 1) + value: tests short-key truncation.
TabletSchemaSPtr char_key_schema() {
    auto s = std::make_shared<TabletSchema>();
    s->append_column(*create_char_key(0, /*is_nullable=*/true, /*length=*/8));
    s->append_column(*create_int_value(1));
    s->_keys_type = DUP_KEYS;
    s->_num_short_key_columns = 1;
    return s;
}

// key(0), value(1), seq(2): unique-key table with a sequence column.
TabletSchemaSPtr seq_schema() {
    auto s = std::make_shared<TabletSchema>();
    s->append_column(*create_int_key(0));
    s->append_column(*create_int_value(1));
    s->append_column(*create_seq_col(2));
    s->_keys_type = UNIQUE_KEYS;
    s->_num_short_key_columns = 1;
    return s;
}

// key(uid 0), value(uid 1) with the value column declared as the cluster key.
TabletSchemaSPtr cluster_key_schema() {
    auto s = std::make_shared<TabletSchema>();
    s->append_column(*create_int_key(0));
    s->append_column(*create_int_value(1));
    s->_keys_type = UNIQUE_KEYS;
    s->_cluster_key_uids = {1};
    s->_num_short_key_columns = 1;
    return s;
}

std::string to_hex(const std::string& s) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out;
    out.reserve(s.size() * 2);
    for (unsigned char c : s) {
        out.push_back(kDigits[c >> 4]);
        out.push_back(kDigits[c & 0xf]);
    }
    return out;
}

template <typename T>
void fill_raw(MutableColumns& cols, uint32_t cid, T v) {
    cols[cid]->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
}

TabletColumnPtr make_key_column(int32_t uid, FieldType type, int32_t length, int32_t index_length,
                                int32_t precision = 0, int32_t frac = 0) {
    auto c = std::make_shared<TabletColumn>();
    c->_unique_id = uid;
    c->_col_name = "k" + std::to_string(uid);
    c->_type = type;
    c->_is_key = true;
    c->_is_nullable = false;
    c->_length = length;
    c->_index_length = index_length;
    c->_precision = precision;
    c->_frac = frac;
    return c;
}

// One key column of the given type + one int value column, unique keys so the
// primary-key view exists.
TabletSchemaSPtr single_key_schema(const TabletColumnPtr& key_col) {
    auto s = std::make_shared<TabletSchema>();
    s->append_column(*key_col);
    s->append_column(*create_int_value(100));
    s->_keys_type = UNIQUE_KEYS;
    s->_num_short_key_columns = 1;
    return s;
}

} // namespace

// A small holder so the convertor (which the accessors point into) and the
// source block stay alive until after the encode calls.
class RowKeyEncoderTest : public testing::Test {
protected:
    void build(const TabletSchemaSPtr& schema, size_t num_rows,
               const std::function<void(MutableColumns&)>& fill) {
        _schema = schema;
        _num_rows = num_rows;
        _block = schema->create_block();
        {
            auto guard = _block.mutate_columns_scoped();
            fill(guard.mutable_columns());
        }
        _convertor = std::make_unique<OlapBlockDataConvertor>(schema.get());
        _convertor->set_source_content(&_block, 0, num_rows);
    }

    IOlapColumnDataAccessor* acc(uint32_t cid) {
        auto [st, accessor] = _convertor->convert_column_data(cid);
        EXPECT_TRUE(st.ok()) << st;
        return accessor;
    }

    // Encode one row through all three views and compare against literal hex
    // goldens. The goldens pin the on-disk key byte layout: any coder change
    // or CPU-architecture (endianness) drift must fail these expectations.
    void check_golden(const TabletSchemaSPtr& schema,
                      const std::function<void(MutableColumns&)>& fill, const std::string& full_hex,
                      const std::string& primary_hex, const std::string& short_hex) {
        build(schema, 1, fill);
        RowKeyEncoder enc(*_schema, /*mow=*/true);
        std::vector<IOlapColumnDataAccessor*> keys {acc(0)};
        EXPECT_EQ(to_hex(enc.full_encode(keys, 0)), full_hex);
        EXPECT_EQ(to_hex(enc.full_encode_primary_keys(keys, 0)), primary_hex);
        EXPECT_EQ(to_hex(enc.encode_short_keys(keys, 0)), short_hex);
    }

    TabletSchemaSPtr _schema;
    Block _block;
    std::unique_ptr<OlapBlockDataConvertor> _convertor;
    size_t _num_rows = 0;
};

// Each non-null key column is one marker byte + the encoded value.
TEST_F(RowKeyEncoderTest, FullEncodeIntKeyLayout) {
    build(two_int_key_schema(), 1, [](MutableColumns& c) {
        fill_int(c, 0, {1});
        fill_int(c, 1, {2});
        fill_int(c, 2, {0});
    });
    RowKeyEncoder enc(*_schema, /*mow=*/false);
    std::vector<IOlapColumnDataAccessor*> keys {acc(0), acc(1)};
    std::string k = enc.full_encode(keys, 0);

    ASSERT_EQ(k.size(), 1u + 4u + 1u + 4u);
    EXPECT_EQ(byte_at(k, 0), kNormal);
    EXPECT_EQ(byte_at(k, 5), kNormal);
}

// A null key value is a single null marker with no value bytes.
TEST_F(RowKeyEncoderTest, NullKeyUsesNullMarker) {
    build(two_int_key_schema(), 1, [](MutableColumns& c) {
        fill_int(c, 0, {std::nullopt});
        fill_int(c, 1, {7});
        fill_int(c, 2, {0});
    });
    RowKeyEncoder enc(*_schema, /*mow=*/false);
    std::vector<IOlapColumnDataAccessor*> keys {acc(0), acc(1)};
    std::string k = enc.full_encode(keys, 0);

    ASSERT_EQ(k.size(), 1u + (1u + 4u));
    EXPECT_EQ(byte_at(k, 0), kNull);
    EXPECT_EQ(byte_at(k, 1), kNormal);
}

// The encoded key is a sortable byte string: byte-by-byte order over the
// encodings must match the multi-column key order, and nulls sort first.
TEST_F(RowKeyEncoderTest, FullEncodePreservesAscendingOrder) {
    build(two_int_key_schema(), 5, [](MutableColumns& c) {
        fill_int(c, 0, {std::nullopt, -5, -5, 2, 2});
        fill_int(c, 1, {0, 3, 9, -100, 7});
        fill_int(c, 2, {0, 0, 0, 0, 0});
    });
    RowKeyEncoder enc(*_schema, /*mow=*/false);
    std::vector<IOlapColumnDataAccessor*> keys {acc(0), acc(1)};

    std::vector<std::string> encoded;
    for (size_t pos = 0; pos < _num_rows; ++pos) {
        encoded.push_back(enc.full_encode(keys, pos));
    }
    for (size_t i = 0; i + 1 < encoded.size(); ++i) {
        EXPECT_LT(encoded[i], encoded[i + 1]) << "rows " << i << " and " << i + 1;
    }
}

// Without cluster keys the sort key and the schema (primary) key are identical.
TEST_F(RowKeyEncoderTest, PrimaryKeysEqualFullEncodeWithoutClusterKey) {
    build(two_int_key_schema(), 1, [](MutableColumns& c) {
        fill_int(c, 0, {3});
        fill_int(c, 1, {4});
        fill_int(c, 2, {0});
    });
    RowKeyEncoder enc(*_schema, /*mow=*/true);
    std::vector<IOlapColumnDataAccessor*> keys {acc(0), acc(1)};
    EXPECT_EQ(enc.full_encode(keys, 0), enc.full_encode_primary_keys(keys, 0));
}

// With cluster keys the segment sorts by the cluster key columns, while the
// primary key index is still built over the schema key columns.
TEST_F(RowKeyEncoderTest, ClusterKeySortDiffersFromPrimaryKeys) {
    build(cluster_key_schema(), 1, [](MutableColumns& c) {
        fill_int(c, 0, {5});  // schema key
        fill_int(c, 1, {99}); // cluster key (and value)
    });
    RowKeyEncoder enc(*_schema, /*mow=*/true);
    ASSERT_EQ(enc.num_sort_key_columns(), 1u);

    std::vector<IOlapColumnDataAccessor*> sort_keys {acc(1)};
    std::vector<IOlapColumnDataAccessor*> primary_keys {acc(0)};
    EXPECT_NE(enc.full_encode(sort_keys, 0), enc.full_encode_primary_keys(primary_keys, 0));
}

// Short keys truncate each column to its index length; the full key keeps the
// whole value, so it is strictly longer for a char column with index_length 1.
TEST_F(RowKeyEncoderTest, ShortKeyTruncatesCharToIndexLength) {
    build(char_key_schema(), 1, [](MutableColumns& c) {
        fill_char(c, 0, {"abcdefgh"});
        fill_int(c, 1, {0});
    });
    RowKeyEncoder enc(*_schema, /*mow=*/false);
    std::vector<IOlapColumnDataAccessor*> keys {acc(0)};

    std::string full = enc.full_encode(keys, 0);
    std::string short_key = enc.encode_short_keys(keys, 0);

    EXPECT_EQ(byte_at(short_key, 0), kNormal);
    EXPECT_EQ(short_key.size(), 1u + 1u); // marker + index_length(1)
    EXPECT_GT(full.size(), short_key.size());
}

// A null sequence value is encoded as the minimal value of the column length so
// it sorts before any real sequence value sharing the same key.
TEST_F(RowKeyEncoderTest, AppendSeqSuffixNullAndNormal) {
    build(seq_schema(), 2, [](MutableColumns& c) {
        fill_int(c, 0, {1, 1});
        fill_int(c, 1, {0, 0});
        fill_int(c, 2, {std::nullopt, 7});
    });
    RowKeyEncoder enc(*_schema, /*mow=*/true);
    std::vector<IOlapColumnDataAccessor*> keys {acc(0)};
    IOlapColumnDataAccessor* seq = acc(2);

    std::string null_seq = enc.full_encode(keys, 0);
    size_t base = null_seq.size();
    enc.append_seq_suffix(&null_seq, seq, 0);
    // null marker + 4 minimal bytes (seq column length)
    ASSERT_EQ(null_seq.size(), base + 1u + 4u);
    EXPECT_EQ(byte_at(null_seq, base), kNull);
    EXPECT_EQ(byte_at(null_seq, base + 1), kMinimal);

    std::string normal_seq = enc.full_encode(keys, 1);
    size_t base1 = normal_seq.size();
    enc.append_seq_suffix(&normal_seq, seq, 1);
    EXPECT_EQ(byte_at(normal_seq, base1), kNormal);

    // same key, null sequence sorts before a real one
    EXPECT_LT(null_seq, normal_seq);
}

// The row id suffix keeps the encoded keys ordered by row id.
TEST_F(RowKeyEncoderTest, AppendRowidSuffixOrdersByRowid) {
    build(cluster_key_schema(), 1, [](MutableColumns& c) {
        fill_int(c, 0, {5});
        fill_int(c, 1, {99});
    });
    RowKeyEncoder enc(*_schema, /*mow=*/true);
    std::vector<IOlapColumnDataAccessor*> sort_keys {acc(1)};

    std::string base = enc.full_encode(sort_keys, 0);
    std::string r1 = base;
    enc.append_rowid_suffix(&r1, 1);
    std::string r2 = base;
    enc.append_rowid_suffix(&r2, 2);

    EXPECT_EQ(byte_at(r1, base.size()), kNormal);
    EXPECT_GT(r1.size(), base.size());
    EXPECT_LT(r1, r2);
}

// Short keys over fixed-length int columns are not truncated (index length ==
// column length), so the short key over both key columns equals the full key.
TEST_F(RowKeyEncoderTest, ShortKeyIntColumnsEqualFullEncode) {
    build(two_int_key_schema(), 1, [](MutableColumns& c) {
        fill_int(c, 0, {5});
        fill_int(c, 1, {6});
        fill_int(c, 2, {0});
    });
    RowKeyEncoder enc(*_schema, /*mow=*/false);
    std::vector<IOlapColumnDataAccessor*> keys {acc(0), acc(1)};

    std::string short_key = enc.encode_short_keys(keys, 0);
    EXPECT_EQ(short_key.size(), (1u + 4u) + (1u + 4u)); // two non-truncated int columns
    EXPECT_EQ(short_key, enc.full_encode(keys, 0));
}

// A null short-key column is a single null marker with no value bytes, exactly
// as in the full key.
TEST_F(RowKeyEncoderTest, ShortKeyNullColumnUsesNullMarker) {
    build(char_key_schema(), 1, [](MutableColumns& c) {
        c[0]->insert_default(); // NULL char key (the column is nullable)
        fill_int(c, 1, {0});
    });
    RowKeyEncoder enc(*_schema, /*mow=*/false);
    std::vector<IOlapColumnDataAccessor*> keys {acc(0)};

    std::string short_key = enc.encode_short_keys(keys, 0);
    ASSERT_EQ(short_key.size(), 1u); // marker only, value truncated away
    EXPECT_EQ(byte_at(short_key, 0), kNull);
}

// Short keys keep only a prefix and drop the rest: two char values sharing the
// first index_length byte collide in the short key, while their full keys still
// differ.
TEST_F(RowKeyEncoderTest, ShortKeyTruncationCollides) {
    build(char_key_schema(), 2, [](MutableColumns& c) {
        fill_char(c, 0, {"axxxxxxx", "ayyyyyyy"}); // share the first byte 'a'
        fill_int(c, 1, {0, 0});
    });
    RowKeyEncoder enc(*_schema, /*mow=*/false);
    std::vector<IOlapColumnDataAccessor*> keys {acc(0)};

    EXPECT_EQ(enc.encode_short_keys(keys, 0), enc.encode_short_keys(keys, 1)); // collide
    EXPECT_NE(enc.full_encode(keys, 0), enc.full_encode(keys, 1));             // full differs
}

// ---- Golden byte matrix over every key-column type coder -------------------
//
// Every hex golden below is a literal snapshot of the encoded bytes. Format of
// a non-null key column: one KEY_NORMAL_MARKER byte (0x02) followed by the
// KeyCoder output — big-endian with the sign bit flipped for signed integrals
// and decimals, plain big-endian for unsigned types, raw bytes for strings
// (truncated to index_length in the short key).
//
// Not covered here: legacy v1 DATE/DATETIME/DECIMAL (only constructible
// through v1 value APIs; new tables use the v2 types pinned below),
// FLOAT/DOUBLE (not legal key columns), and UNSIGNED_INT/UNSIGNED_BIGINT
// (reachable only as the rowid suffix, pinned by GoldenSeqAndRowidSuffix).

TEST_F(RowKeyEncoderTest, GoldenTinyInt) {
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_TINYINT, 1, 1)),
            [](MutableColumns& c) {
                fill_raw<int8_t>(c, 0, 5);
                fill_int(c, 1, {0});
            },
            "0285", "0285", "0285");
}

TEST_F(RowKeyEncoderTest, GoldenSmallInt) {
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_SMALLINT, 2, 2)),
            [](MutableColumns& c) {
                fill_raw<int16_t>(c, 0, 5);
                fill_int(c, 1, {0});
            },
            "028005", "028005", "028005");
}

TEST_F(RowKeyEncoderTest, GoldenInt) {
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_INT, 4, 4)),
            [](MutableColumns& c) {
                fill_raw<int32_t>(c, 0, 5);
                fill_int(c, 1, {0});
            },
            "0280000005", "0280000005", "0280000005");
}

TEST_F(RowKeyEncoderTest, GoldenIntNegative) {
    // -5: sign-flip + big-endian => 0x7ffffffb, sorts below the +5 golden above
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_INT, 4, 4)),
            [](MutableColumns& c) {
                fill_raw<int32_t>(c, 0, -5);
                fill_int(c, 1, {0});
            },
            "027ffffffb", "027ffffffb", "027ffffffb");
}

TEST_F(RowKeyEncoderTest, GoldenBigInt) {
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_BIGINT, 8, 8)),
            [](MutableColumns& c) {
                fill_raw<int64_t>(c, 0, 5);
                fill_int(c, 1, {0});
            },
            "028000000000000005", "028000000000000005", "028000000000000005");
}

TEST_F(RowKeyEncoderTest, GoldenLargeInt) {
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_LARGEINT, 16, 16)),
            [](MutableColumns& c) {
                fill_raw<__int128>(c, 0, 5);
                fill_int(c, 1, {0});
            },
            "0280000000000000000000000000000005", "0280000000000000000000000000000005",
            "0280000000000000000000000000000005");
}

TEST_F(RowKeyEncoderTest, GoldenBool) {
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_BOOL, 1, 1)),
            [](MutableColumns& c) {
                fill_raw<uint8_t>(c, 0, 1);
                fill_int(c, 1, {0});
            },
            "0201", "0201", "0201");
}

TEST_F(RowKeyEncoderTest, GoldenDateV2) {
    // DATEV2 stores a raw uint32; the coder is plain big-endian (no sign flip).
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_DATEV2, 4, 4)),
            [](MutableColumns& c) {
                fill_raw<uint32_t>(c, 0, 0x01020304);
                fill_int(c, 1, {0});
            },
            "0201020304", "0201020304", "0201020304");
}

TEST_F(RowKeyEncoderTest, GoldenDateTimeV2) {
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 8, 8)),
            [](MutableColumns& c) {
                fill_raw<uint64_t>(c, 0, 0x0102030405060708ULL);
                fill_int(c, 1, {0});
            },
            "020102030405060708", "020102030405060708", "020102030405060708");
}

TEST_F(RowKeyEncoderTest, GoldenDecimal32) {
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_DECIMAL32, 4, 4, 9, 2)),
            [](MutableColumns& c) {
                fill_raw<int32_t>(c, 0, 5);
                fill_int(c, 1, {0});
            },
            "0280000005", "0280000005", "0280000005");
}

TEST_F(RowKeyEncoderTest, GoldenDecimal64) {
    check_golden(
            single_key_schema(
                    make_key_column(0, FieldType::OLAP_FIELD_TYPE_DECIMAL64, 8, 8, 18, 4)),
            [](MutableColumns& c) {
                fill_raw<int64_t>(c, 0, 5);
                fill_int(c, 1, {0});
            },
            "028000000000000005", "028000000000000005", "028000000000000005");
}

TEST_F(RowKeyEncoderTest, GoldenDecimal128I) {
    check_golden(
            single_key_schema(
                    make_key_column(0, FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 16, 16, 38, 6)),
            [](MutableColumns& c) {
                fill_raw<__int128>(c, 0, 5);
                fill_int(c, 1, {0});
            },
            "0280000000000000000000000000000005", "0280000000000000000000000000000005",
            "0280000000000000000000000000000005");
}

TEST_F(RowKeyEncoderTest, GoldenDecimal256) {
    // Little-endian limb layout of wide::Int256 value 5 (x86-64); the coder
    // sign-flips and emits the 32 bytes big-endian.
    std::array<uint8_t, 32> raw {};
    raw[0] = 5;
    check_golden(
            single_key_schema(
                    make_key_column(0, FieldType::OLAP_FIELD_TYPE_DECIMAL256, 32, 32, 76, 8)),
            [&raw](MutableColumns& c) {
                c[0]->insert_data(reinterpret_cast<const char*>(raw.data()), raw.size());
                fill_int(c, 1, {0});
            },
            "028000000000000000000000000000000000000000000000000000000000000005",
            "028000000000000000000000000000000000000000000000000000000000000005",
            "028000000000000000000000000000000000000000000000000000000000000005");
}

TEST_F(RowKeyEncoderTest, GoldenIpv4) {
    // IPV4 stores a raw uint32 (127.0.0.1), plain big-endian, no sign flip.
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_IPV4, 4, 4)),
            [](MutableColumns& c) {
                fill_raw<uint32_t>(c, 0, 0x7f000001);
                fill_int(c, 1, {0});
            },
            "027f000001", "027f000001", "027f000001");
}

TEST_F(RowKeyEncoderTest, GoldenIpv6) {
    // ::1 == uint128 value 1
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_IPV6, 16, 16)),
            [](MutableColumns& c) {
                fill_raw<unsigned __int128>(c, 0, 1);
                fill_int(c, 1, {0});
            },
            "0200000000000000000000000000000001", "0200000000000000000000000000000001",
            "0200000000000000000000000000000001");
}

TEST_F(RowKeyEncoderTest, GoldenChar) {
    // CHAR(8) "abcdefgh": full/primary keep all bytes, the short key truncates
    // to index_length(2). Covers the string coder in all three views.
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_CHAR, 8, 2)),
            [](MutableColumns& c) {
                fill_char(c, 0, {"abcdefgh"});
                fill_int(c, 1, {0});
            },
            "026162636465666768", "026162636465666768", "026162");
}

TEST_F(RowKeyEncoderTest, GoldenVarchar) {
    // VARCHAR value "ab" shorter than index_length(4): the short key keeps all
    // available bytes instead of failing.
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_VARCHAR, 16, 4)),
            [](MutableColumns& c) {
                fill_char(c, 0, {"ab"});
                fill_int(c, 1, {0});
            },
            "026162", "026162", "026162");
}

TEST_F(RowKeyEncoderTest, GoldenString) {
    check_golden(
            single_key_schema(make_key_column(0, FieldType::OLAP_FIELD_TYPE_STRING, 1024, 4)),
            [](MutableColumns& c) {
                fill_char(c, 0, {"abcdef"});
                fill_int(c, 1, {0});
            },
            "02616263646566", "02616263646566", "0261626364");
}

// The two key suffixes: an int sequence value and the UNSIGNED_INT rowid.
TEST_F(RowKeyEncoderTest, GoldenSeqAndRowidSuffix) {
    build(seq_schema(), 1, [](MutableColumns& c) {
        fill_int(c, 0, {1});
        fill_int(c, 1, {0});
        fill_int(c, 2, {7});
    });
    RowKeyEncoder enc(*_schema, /*mow=*/true);
    std::string seq_suffix;
    enc.append_seq_suffix(&seq_suffix, acc(2), 0);
    EXPECT_EQ(to_hex(seq_suffix), "0280000007");

    RowKeyEncoder cluster_enc(*cluster_key_schema(), /*mow=*/true);
    std::string rowid_suffix;
    cluster_enc.append_rowid_suffix(&rowid_suffix, 3);
    EXPECT_EQ(to_hex(rowid_suffix), "0200000003");
}

} // namespace doris
