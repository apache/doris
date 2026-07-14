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

// White-box access to TabletColumn/TabletSchema private fields: the test
// builds schema objects field by field, the same way the existing storage
// unit tests do (tablet_schema_helper.cpp does the same). The macros wrap
// only this include and are #undef-ed right away, so gtest, standard and
// other Doris headers below are parsed with their access specifiers intact.
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#include "storage/tablet/tablet_schema.h" // IWYU pragma: keep
#undef private
#undef protected
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/consts.h"
#include "core/block/block.h"
#include "core/data_type/data_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/extended_types.h"
#include "core/string_ref.h"
#include "core/value/vdatetime_value.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/key/row_key_encoder.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema_helper.h"
#include "storage/utils.h"
#include "testutil/test_util.h"

// The tests are organized as two orthogonal axes plus two suffix cases:
//
//   axis A (types):     AllKeyTypesTable uses ONE explicit data table whose key
//                       contains all 17 default-converted physical key-column
//                       types, then runs it through every relevant schema path.
//   axis B (encodings): the full RowKeyEncoder surface - full_encode (sort
//                       view), full_encode_primary_keys (primary view),
//                       encode_short_keys (index_length truncation), the null
//                       value form, and byte-order == logical-order - applied
//                       per column segment by one loop body.
//
//   SeqSuffix           sequence-column suffix over every seq-eligible type.
//   RowidSuffix         rowid as the duplicate-PK tie-breaker, including the
//                       relevant UNSIGNED_INT byte boundaries.
//
// AllKeyTypesTable checks the ordering contract and exact byte layout from one
// explicit data table. The suffix cases retain focused checks for behavior that
// the table cannot make independently decide row order.

namespace doris {
namespace {

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

// The storage bit layout of DATEV2 (DateV2ValueType: day:5 | month:4 |
// year:23 from the LSB), so the test values are readable as real dates.
constexpr uint32_t datev2_bits(uint32_t year, uint32_t month, uint32_t day) {
    return (year << 9) | (month << 5) | day;
}

// The storage bit layout of DATETIMEV2 and TIMESTAMPTZ (DateTimeV2ValueType:
// microsecond:20 | second:6 | minute:6 | hour:5 | day:5 | month:4 | year:18
// from the LSB; TimestampTzValue stores the same layout normalized to UTC).
constexpr uint64_t datetimev2_bits(uint64_t year, uint64_t month, uint64_t day, uint64_t hour,
                                   uint64_t minute, uint64_t second, uint64_t microsecond = 0) {
    return (year << 46) | (month << 42) | (day << 37) | (hour << 32) | (minute << 26) |
           (second << 20) | microsecond;
}

// Nullable so the null-value rows can be inserted; nullability itself never
// changes the encoded bytes of a non-null value.
TabletColumnPtr make_key_column(int32_t uid, FieldType type, int32_t length, int32_t index_length,
                                int32_t precision = 0, int32_t frac = 0) {
    auto c = std::make_shared<TabletColumn>();
    c->_unique_id = uid;
    c->_col_name = "k" + std::to_string(uid);
    c->_type = type;
    c->_is_key = true;
    c->_is_nullable = true;
    c->_length = length;
    c->_index_length = index_length;
    c->_precision = precision;
    c->_frac = frac;
    return c;
}

// A hidden sequence column is identified by its reserved name.
TabletColumnPtr make_seq_column(
        int32_t uid, FieldType type, int32_t length,
        FieldAggregationMethod aggregation = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE) {
    auto c = std::make_shared<TabletColumn>();
    c->_unique_id = uid;
    c->_col_name = SEQUENCE_COL;
    c->_type = type;
    c->_is_key = false;
    c->_is_nullable = true;
    c->_aggregation = aggregation;
    c->_length = length;
    c->_index_length = length;
    c->_precision = 0;
    c->_frac = 0;
    return c;
}

// key(uid 0), cluster key(uid 1), sequence column(uid 2).
TabletSchemaSPtr cluster_key_with_sequence_schema() {
    auto s = std::make_shared<TabletSchema>();
    s->append_column(*create_int_key(0));
    s->append_column(*create_int_value(1, FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE));
    s->append_column(*make_seq_column(2, FieldType::OLAP_FIELD_TYPE_INT, sizeof(int32_t)));
    s->_keys_type = UNIQUE_KEYS;
    s->_cluster_key_uids = {1};
    s->_num_short_key_columns = 1;
    return s;
}

struct KeyColumnSpec {
    const char* name;
    FieldType type;
    int32_t length;
    int32_t index_length;
    int32_t precision;
    int32_t frac;
};

constexpr size_t kNumKeyColumns = 17;
constexpr std::array<KeyColumnSpec, kNumKeyColumns> kKeyColumns = {{
        {"tinyint", FieldType::OLAP_FIELD_TYPE_TINYINT, 1, 1, 0, 0},
        {"smallint", FieldType::OLAP_FIELD_TYPE_SMALLINT, 2, 2, 0, 0},
        {"int", FieldType::OLAP_FIELD_TYPE_INT, 4, 4, 0, 0},
        {"bigint", FieldType::OLAP_FIELD_TYPE_BIGINT, 8, 8, 0, 0},
        {"largeint", FieldType::OLAP_FIELD_TYPE_LARGEINT, 16, 16, 0, 0},
        {"bool", FieldType::OLAP_FIELD_TYPE_BOOL, 1, 1, 0, 0},
        {"datev2", FieldType::OLAP_FIELD_TYPE_DATEV2, 4, 4, 0, 0},
        {"datetimev2", FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 8, 8, 0, 0},
        {"timestamptz", FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, 8, 8, 0, 0},
        {"decimal32", FieldType::OLAP_FIELD_TYPE_DECIMAL32, 4, 4, 9, 2},
        {"decimal64", FieldType::OLAP_FIELD_TYPE_DECIMAL64, 8, 8, 18, 4},
        {"decimal128i", FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 16, 16, 38, 6},
        {"decimal256", FieldType::OLAP_FIELD_TYPE_DECIMAL256, 32, 32, 76, 8},
        {"ipv4", FieldType::OLAP_FIELD_TYPE_IPV4, 4, 4, 0, 0},
        {"ipv6", FieldType::OLAP_FIELD_TYPE_IPV6, 16, 16, 0, 0},
        {"char", FieldType::OLAP_FIELD_TYPE_CHAR, 8, 2, 0, 0},
        {"varchar", FieldType::OLAP_FIELD_TYPE_VARCHAR, 16, 4, 0, 0},
}};

// These are all eight production-valid table shapes around RowKeyEncoder. DUP,
// AGG and UNIQUE merge-on-read are listed separately to make the non-MOW key
// models explicit, even though the encoder itself intentionally treats them
// alike. Sequence is valid for both UNIQUE MOR and MOW; cluster keys are valid
// only for UNIQUE MOW tables.
struct AllKeySchemaCase {
    const char* name;
    KeysType keys_type;
    bool enable_unique_key_merge_on_write;
    bool has_sequence;
    bool has_cluster_keys;
    size_t num_short_key_columns;
};

constexpr std::array<AllKeySchemaCase, 8> kAllKeySchemaCases = {{
        // DUP uses the ordinary three-column prefix. The remaining cases keep
        // all 17 columns so every type is also covered by short-key encoding.
        {"duplicate_keys", DUP_KEYS, false, false, false, 3},
        {"aggregate_keys", AGG_KEYS, false, false, false, kNumKeyColumns},
        {"unique_keys_merge_on_read", UNIQUE_KEYS, false, false, false, kNumKeyColumns},
        {"unique_keys_merge_on_read_sequence", UNIQUE_KEYS, false, true, false, kNumKeyColumns},
        {"unique_keys_merge_on_write", UNIQUE_KEYS, true, false, false, kNumKeyColumns},
        {"unique_keys_merge_on_write_sequence", UNIQUE_KEYS, true, true, false, kNumKeyColumns},
        {"unique_keys_merge_on_write_cluster", UNIQUE_KEYS, true, false, true, kNumKeyColumns},
        {"unique_keys_merge_on_write_cluster_sequence", UNIQUE_KEYS, true, true, true,
         kNumKeyColumns},
}};

constexpr int32_t kClusterKeyUidBase = 100;
constexpr int32_t kSequenceColumnUid = 200;

// Keep tinyint first so the explicit rows remain globally sorted, but swap
// smallint and int. This makes the cluster coder order observably different
// from the primary coder order while VARCHAR remains the final short-key
// column, as required by a production TabletSchema.
constexpr std::array<size_t, kNumKeyColumns> kClusterKeyOrder = {0, 2,  1,  3,  4,  5,  6,  7, 8,
                                                                 9, 10, 11, 12, 13, 14, 15, 16};

TabletColumnPtr make_cluster_column(size_t key) {
    const auto& spec = kKeyColumns[key];
    auto column = make_key_column(kClusterKeyUidBase + static_cast<int32_t>(key), spec.type,
                                  spec.length, spec.index_length, spec.precision, spec.frac);
    column->_col_name = "c" + std::to_string(key);
    column->_is_key = false;
    column->_aggregation = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE;
    return column;
}

using KeyCell = std::optional<std::string_view>;
using KeyDataRow = std::array<KeyCell, kNumKeyColumns>;

// All table data is written out here, in logical ascending order. Each column
// owns one three-row group ordered as NULL < low < high, and only that target
// cell changes inside the group. This makes the deciding column obvious during
// review. Different groups use distinct, meaningful profiles instead of one
// repeated filler row. Increasing k0 profile values keep the whole table sorted
// by k0, then k1, ..., k16. Numeric targets include their supported boundaries;
// time, IP and string targets use ordinary recognizable values.
constexpr std::array<KeyDataRow, 51> kAllKeyData = {{
        // k0 tinyint, k1 smallint, k2 int, k3 bigint, k4 largeint, k5 bool,
        // k6 datev2, k7 datetimev2, k8 timestamptz, k9..k12 decimals,
        // k13 ipv4, k14 ipv6, k15 char, k16 varchar.
        // k0 group, profile 00: NULL < TINYINT_MIN < the next tinyint.
        {std::nullopt, "100", "1000", "10000", "100000", "1", "2024-01-01", "2024-02-01 08:00:00",
         "2024-03-01 08:00:00 +08:00", "1000.00", "10000.0000", "100000.000000", "1000000.00000000",
         "10.0.0.1", "2001:db8::1", "node0000", "site-000"},
        {"-128", "100", "1000", "10000", "100000", "1", "2024-01-01", "2024-02-01 08:00:00",
         "2024-03-01 08:00:00 +08:00", "1000.00", "10000.0000", "100000.000000", "1000000.00000000",
         "10.0.0.1", "2001:db8::1", "node0000", "site-000"},
        {"-127", "100", "1000", "10000", "100000", "1", "2024-01-01", "2024-02-01 08:00:00",
         "2024-03-01 08:00:00 +08:00", "1000.00", "10000.0000", "100000.000000", "1000000.00000000",
         "10.0.0.1", "2001:db8::1", "node0000", "site-000"},

        // k1 group, profile 01: NULL < SMALLINT_MIN < SMALLINT_MAX.
        {"-112", std::nullopt, "1001", "10001", "100001", "0", "2024-01-02", "2024-02-02 08:01:00",
         "2024-03-02 08:01:00 +08:00", "1000.01", "10000.0001", "100000.000001", "1000000.00000001",
         "10.0.0.2", "2001:db8::2", "node0001", "site-001"},
        {"-112", "-32768", "1001", "10001", "100001", "0", "2024-01-02", "2024-02-02 08:01:00",
         "2024-03-02 08:01:00 +08:00", "1000.01", "10000.0001", "100000.000001", "1000000.00000001",
         "10.0.0.2", "2001:db8::2", "node0001", "site-001"},
        {"-112", "32767", "1001", "10001", "100001", "0", "2024-01-02", "2024-02-02 08:01:00",
         "2024-03-02 08:01:00 +08:00", "1000.01", "10000.0001", "100000.000001", "1000000.00000001",
         "10.0.0.2", "2001:db8::2", "node0001", "site-001"},

        // k2 group, profile 02: NULL < INT_MIN < INT_MAX.
        {"-96", "102", std::nullopt, "10002", "100002", "1", "2024-01-03", "2024-02-03 08:02:00",
         "2024-03-03 08:02:00 +08:00", "1000.02", "10000.0002", "100000.000002", "1000000.00000002",
         "10.0.0.3", "2001:db8::3", "node0002", "site-002"},
        {"-96", "102", "-2147483648", "10002", "100002", "1", "2024-01-03", "2024-02-03 08:02:00",
         "2024-03-03 08:02:00 +08:00", "1000.02", "10000.0002", "100000.000002", "1000000.00000002",
         "10.0.0.3", "2001:db8::3", "node0002", "site-002"},
        {"-96", "102", "2147483647", "10002", "100002", "1", "2024-01-03", "2024-02-03 08:02:00",
         "2024-03-03 08:02:00 +08:00", "1000.02", "10000.0002", "100000.000002", "1000000.00000002",
         "10.0.0.3", "2001:db8::3", "node0002", "site-002"},

        // k3 group, profile 03: NULL < BIGINT_MIN < BIGINT_MAX.
        {"-80", "103", "1003", std::nullopt, "100003", "0", "2024-01-04", "2024-02-04 08:03:00",
         "2024-03-04 08:03:00 +08:00", "1000.03", "10000.0003", "100000.000003", "1000000.00000003",
         "10.0.0.4", "2001:db8::4", "node0003", "site-003"},
        {"-80", "103", "1003", "-9223372036854775808", "100003", "0", "2024-01-04",
         "2024-02-04 08:03:00", "2024-03-04 08:03:00 +08:00", "1000.03", "10000.0003",
         "100000.000003", "1000000.00000003", "10.0.0.4", "2001:db8::4", "node0003", "site-003"},
        {"-80", "103", "1003", "9223372036854775807", "100003", "0", "2024-01-04",
         "2024-02-04 08:03:00", "2024-03-04 08:03:00 +08:00", "1000.03", "10000.0003",
         "100000.000003", "1000000.00000003", "10.0.0.4", "2001:db8::4", "node0003", "site-003"},

        // k4 group, profile 04: NULL < LARGEINT_MIN < LARGEINT_MAX.
        {"-64", "104", "1004", "10004", std::nullopt, "1", "2024-01-05", "2024-02-05 08:04:00",
         "2024-03-05 08:04:00 +08:00", "1000.04", "10000.0004", "100000.000004", "1000000.00000004",
         "10.0.0.5", "2001:db8::5", "node0004", "site-004"},
        {"-64", "104", "1004", "10004", "-170141183460469231731687303715884105728", "1",
         "2024-01-05", "2024-02-05 08:04:00", "2024-03-05 08:04:00 +08:00", "1000.04", "10000.0004",
         "100000.000004", "1000000.00000004", "10.0.0.5", "2001:db8::5", "node0004", "site-004"},
        {"-64", "104", "1004", "10004", "170141183460469231731687303715884105727", "1",
         "2024-01-05", "2024-02-05 08:04:00", "2024-03-05 08:04:00 +08:00", "1000.04", "10000.0004",
         "100000.000004", "1000000.00000004", "10.0.0.5", "2001:db8::5", "node0004", "site-004"},

        // k5 group, profile 05: NULL < false < true.
        {"-48", "105", "1005", "10005", "100005", std::nullopt, "2024-01-06", "2024-02-06 08:05:00",
         "2024-03-06 08:05:00 +08:00", "1000.05", "10000.0005", "100000.000005", "1000000.00000005",
         "10.0.0.6", "2001:db8::6", "node0005", "site-005"},
        {"-48", "105", "1005", "10005", "100005", "0", "2024-01-06", "2024-02-06 08:05:00",
         "2024-03-06 08:05:00 +08:00", "1000.05", "10000.0005", "100000.000005", "1000000.00000005",
         "10.0.0.6", "2001:db8::6", "node0005", "site-005"},
        {"-48", "105", "1005", "10005", "100005", "1", "2024-01-06", "2024-02-06 08:05:00",
         "2024-03-06 08:05:00 +08:00", "1000.05", "10000.0005", "100000.000005", "1000000.00000005",
         "10.0.0.6", "2001:db8::6", "node0005", "site-005"},

        // k6 group, profile 06: NULL < February 1 < New Year's Eve.
        {"-32", "106", "1006", "10006", "100006", "1", std::nullopt, "2024-02-07 08:06:00",
         "2024-03-07 08:06:00 +08:00", "1000.06", "10000.0006", "100000.000006", "1000000.00000006",
         "10.0.0.7", "2001:db8::7", "node0006", "site-006"},
        {"-32", "106", "1006", "10006", "100006", "1", "2024-02-01", "2024-02-07 08:06:00",
         "2024-03-07 08:06:00 +08:00", "1000.06", "10000.0006", "100000.000006", "1000000.00000006",
         "10.0.0.7", "2001:db8::7", "node0006", "site-006"},
        {"-32", "106", "1006", "10006", "100006", "1", "2024-12-31", "2024-02-07 08:06:00",
         "2024-03-07 08:06:00 +08:00", "1000.06", "10000.0006", "100000.000006", "1000000.00000006",
         "10.0.0.7", "2001:db8::7", "node0006", "site-006"},

        // k7 group, profile 07: NULL < a morning meeting < an evening meeting.
        {"-24", "107", "1007", "10007", "100007", "0", "2024-01-08", std::nullopt,
         "2024-03-08 08:07:00 +08:00", "1000.07", "10000.0007", "100000.000007", "1000000.00000007",
         "10.0.0.8", "2001:db8::8", "node0007", "site-007"},
        {"-24", "107", "1007", "10007", "100007", "0", "2024-01-08", "2024-03-01 09:00:00",
         "2024-03-08 08:07:00 +08:00", "1000.07", "10000.0007", "100000.000007", "1000000.00000007",
         "10.0.0.8", "2001:db8::8", "node0007", "site-007"},
        {"-24", "107", "1007", "10007", "100007", "0", "2024-01-08", "2024-03-01 18:30:00",
         "2024-03-08 08:07:00 +08:00", "1000.07", "10000.0007", "100000.000007", "1000000.00000007",
         "10.0.0.8", "2001:db8::8", "node0007", "site-007"},

        // k8 group, profile 08: NULL < a zoned morning meeting < a zoned evening meeting.
        {"-16", "108", "1008", "10008", "100008", "1", "2024-01-09", "2024-02-09 08:08:00",
         std::nullopt, "1000.08", "10000.0008", "100000.000008", "1000000.00000008", "10.0.0.9",
         "2001:db8::9", "node0008", "site-008"},
        {"-16", "108", "1008", "10008", "100008", "1", "2024-01-09", "2024-02-09 08:08:00",
         "2024-04-01 09:00:00 +08:00", "1000.08", "10000.0008", "100000.000008", "1000000.00000008",
         "10.0.0.9", "2001:db8::9", "node0008", "site-008"},
        {"-16", "108", "1008", "10008", "100008", "1", "2024-01-09", "2024-02-09 08:08:00",
         "2024-04-01 18:30:00 +08:00", "1000.08", "10000.0008", "100000.000008", "1000000.00000008",
         "10.0.0.9", "2001:db8::9", "node0008", "site-008"},

        // k9 group, profile 09: NULL < DECIMAL32_MIN < DECIMAL32_MAX.
        {"-8", "109", "1009", "10009", "100009", "0", "2024-01-10", "2024-02-10 08:09:00",
         "2024-03-10 08:09:00 +08:00", std::nullopt, "10000.0009", "100000.000009",
         "1000000.00000009", "10.0.0.10", "2001:db8::a", "node0009", "site-009"},
        {"-8", "109", "1009", "10009", "100009", "0", "2024-01-10", "2024-02-10 08:09:00",
         "2024-03-10 08:09:00 +08:00", "-9999999.99", "10000.0009", "100000.000009",
         "1000000.00000009", "10.0.0.10", "2001:db8::a", "node0009", "site-009"},
        {"-8", "109", "1009", "10009", "100009", "0", "2024-01-10", "2024-02-10 08:09:00",
         "2024-03-10 08:09:00 +08:00", "9999999.99", "10000.0009", "100000.000009",
         "1000000.00000009", "10.0.0.10", "2001:db8::a", "node0009", "site-009"},

        // k10 group, profile 10: NULL < DECIMAL64_MIN < DECIMAL64_MAX.
        {"0", "110", "1010", "10010", "100010", "1", "2024-01-11", "2024-02-11 08:10:00",
         "2024-03-11 08:10:00 +08:00", "1000.10", std::nullopt, "100000.000010", "1000000.00000010",
         "10.0.0.11", "2001:db8::b", "node0010", "site-010"},
        {"0", "110", "1010", "10010", "100010", "1", "2024-01-11", "2024-02-11 08:10:00",
         "2024-03-11 08:10:00 +08:00", "1000.10", "-99999999999999.9999", "100000.000010",
         "1000000.00000010", "10.0.0.11", "2001:db8::b", "node0010", "site-010"},
        {"0", "110", "1010", "10010", "100010", "1", "2024-01-11", "2024-02-11 08:10:00",
         "2024-03-11 08:10:00 +08:00", "1000.10", "99999999999999.9999", "100000.000010",
         "1000000.00000010", "10.0.0.11", "2001:db8::b", "node0010", "site-010"},

        // k11 group, profile 11: NULL < DECIMAL128_MIN < DECIMAL128_MAX.
        {"8", "111", "1011", "10011", "100011", "0", "2024-01-12", "2024-02-12 08:11:00",
         "2024-03-12 08:11:00 +08:00", "1000.11", "10000.0011", std::nullopt, "1000000.00000011",
         "10.0.0.12", "2001:db8::c", "node0011", "site-011"},
        {"8", "111", "1011", "10011", "100011", "0", "2024-01-12", "2024-02-12 08:11:00",
         "2024-03-12 08:11:00 +08:00", "1000.11", "10000.0011",
         "-99999999999999999999999999999999.999999", "1000000.00000011", "10.0.0.12", "2001:db8::c",
         "node0011", "site-011"},
        {"8", "111", "1011", "10011", "100011", "0", "2024-01-12", "2024-02-12 08:11:00",
         "2024-03-12 08:11:00 +08:00", "1000.11", "10000.0011",
         "99999999999999999999999999999999.999999", "1000000.00000011", "10.0.0.12", "2001:db8::c",
         "node0011", "site-011"},

        // k12 group, profile 12: NULL < DECIMAL256_MIN < DECIMAL256_MAX.
        {"16", "112", "1012", "10012", "100012", "1", "2024-01-13", "2024-02-13 08:12:00",
         "2024-03-13 08:12:00 +08:00", "1000.12", "10000.0012", "100000.000012", std::nullopt,
         "10.0.0.13", "2001:db8::d", "node0012", "site-012"},
        {"16", "112", "1012", "10012", "100012", "1", "2024-01-13", "2024-02-13 08:12:00",
         "2024-03-13 08:12:00 +08:00", "1000.12", "10000.0012", "100000.000012",
         "-99999999999999999999999999999999999999999999999999999999999999999999.99999999",
         "10.0.0.13", "2001:db8::d", "node0012", "site-012"},
        {"16", "112", "1012", "10012", "100012", "1", "2024-01-13", "2024-02-13 08:12:00",
         "2024-03-13 08:12:00 +08:00", "1000.12", "10000.0012", "100000.000012",
         "99999999999999999999999999999999999999999999999999999999999999999999.99999999",
         "10.0.0.13", "2001:db8::d", "node0012", "site-012"},

        // k13 group, profile 13: NULL < a service address < a private LAN address.
        {"32", "113", "1013", "10013", "100013", "0", "2024-01-14", "2024-02-14 08:13:00",
         "2024-03-14 08:13:00 +08:00", "1000.13", "10000.0013", "100000.000013", "1000000.00000013",
         std::nullopt, "2001:db8::e", "node0013", "site-013"},
        {"32", "113", "1013", "10013", "100013", "0", "2024-01-14", "2024-02-14 08:13:00",
         "2024-03-14 08:13:00 +08:00", "1000.13", "10000.0013", "100000.000013", "1000000.00000013",
         "10.10.0.1", "2001:db8::e", "node0013", "site-013"},
        {"32", "113", "1013", "10013", "100013", "0", "2024-01-14", "2024-02-14 08:13:00",
         "2024-03-14 08:13:00 +08:00", "1000.13", "10000.0013", "100000.000013", "1000000.00000013",
         "192.168.1.100", "2001:db8::e", "node0013", "site-013"},

        // k14 group, profile 14: NULL < two addresses from the documentation prefix.
        {"64", "114", "1014", "10014", "100014", "1", "2024-01-15", "2024-02-15 08:14:00",
         "2024-03-15 08:14:00 +08:00", "1000.14", "10000.0014", "100000.000014", "1000000.00000014",
         "10.0.0.15", std::nullopt, "node0014", "site-014"},
        {"64", "114", "1014", "10014", "100014", "1", "2024-01-15", "2024-02-15 08:14:00",
         "2024-03-15 08:14:00 +08:00", "1000.14", "10000.0014", "100000.000014", "1000000.00000014",
         "10.0.0.15", "2001:db8:1::1", "node0014", "site-014"},
        {"64", "114", "1014", "10014", "100014", "1", "2024-01-15", "2024-02-15 08:14:00",
         "2024-03-15 08:14:00 +08:00", "1000.14", "10000.0014", "100000.000014", "1000000.00000014",
         "10.0.0.15", "2001:db8:2::1", "node0014", "site-014"},

        // k15 group, profile 15: NULL < two node IDs sharing the two-byte short-key prefix.
        {"96", "115", "1015", "10015", "100015", "0", "2024-01-16", "2024-02-16 08:15:00",
         "2024-03-16 08:15:00 +08:00", "1000.15", "10000.0015", "100000.000015", "1000000.00000015",
         "10.0.0.16", "2001:db8::10", std::nullopt, "site-015"},
        {"96", "115", "1015", "10015", "100015", "0", "2024-01-16", "2024-02-16 08:15:00",
         "2024-03-16 08:15:00 +08:00", "1000.15", "10000.0015", "100000.000015", "1000000.00000015",
         "10.0.0.16", "2001:db8::10", "node1501", "site-015"},
        {"96", "115", "1015", "10015", "100015", "0", "2024-01-16", "2024-02-16 08:15:00",
         "2024-03-16 08:15:00 +08:00", "1000.15", "10000.0015", "100000.000015", "1000000.00000015",
         "10.0.0.16", "2001:db8::10", "node1599", "site-015"},

        // k16 group, profile 16: NULL < east-hub < west-hub.
        {"127", "116", "1016", "10016", "100016", "1", "2024-01-17", "2024-02-17 08:16:00",
         "2024-03-17 08:16:00 +08:00", "1000.16", "10000.0016", "100000.000016", "1000000.00000016",
         "10.0.0.17", "2001:db8::11", "node0016", std::nullopt},
        {"127", "116", "1016", "10016", "100016", "1", "2024-01-17", "2024-02-17 08:16:00",
         "2024-03-17 08:16:00 +08:00", "1000.16", "10000.0016", "100000.000016", "1000000.00000016",
         "10.0.0.17", "2001:db8::11", "node0016", "east-hub"},
        {"127", "116", "1016", "10016", "100016", "1", "2024-01-17", "2024-02-17 08:16:00",
         "2024-03-17 08:16:00 +08:00", "1000.16", "10000.0016", "100000.000016", "1000000.00000016",
         "10.0.0.17", "2001:db8::11", "node0016", "west-hub"},
}};

size_t first_different_key(const KeyDataRow& left, const KeyDataRow& right) {
    return static_cast<size_t>(std::mismatch(left.begin(), left.end(), right.begin()).first -
                               left.begin());
}

std::string key_data_row_label(size_t row) {
    static constexpr std::array<const char*, 3> kVariants = {"null", "low", "high"};
    const size_t key = row / kVariants.size();
    return "k" + std::to_string(key) + "_" + kKeyColumns[key].name + "." +
           kVariants[row % kVariants.size()];
}

void expect_null_first_encoding(const std::vector<std::string>& encoded_keys, size_t null_row,
                                size_t value_row) {
    auto difference = std::mismatch(encoded_keys[null_row].begin(), encoded_keys[null_row].end(),
                                    encoded_keys[value_row].begin(), encoded_keys[value_row].end());
    ASSERT_NE(difference.first, encoded_keys[null_row].end());
    ASSERT_NE(difference.second, encoded_keys[value_row].end());
    EXPECT_EQ(static_cast<uint8_t>(*difference.first), KeyConsts::KEY_NULL_FIRST_MARKER);
    EXPECT_EQ(static_cast<uint8_t>(*difference.second), KeyConsts::KEY_NORMAL_MARKER);
    EXPECT_LT(encoded_keys[null_row], encoded_keys[value_row]);
}

// One row of the sequence-suffix table: a seq-eligible type, its column
// length (which sizes the null minimal-value filler), one known value and its
// raw coder bytes.
struct SeqCase {
    const char* name;
    FieldType type;
    int32_t length;
    // inserts exactly one row (the normal value) into c[2]
    std::function<void(MutableColumns&)> fill_value;
    std::string value_hex;
};

// The FE allows sequence columns of isFixedPointType() || isDateType()
// (PropertyAnalyzer::analyzeSequenceType and the sequence_col mapping share
// the check): the five integer types plus all five date types. BOOL is NOT
// seq-eligible. DATE and DATETIME remain here for legacy TabletSchemas even
// though newly created tables use their v2 counterparts.
std::vector<SeqCase> seq_cases() {
    return {
            {"tinyint", FieldType::OLAP_FIELD_TYPE_TINYINT, 1,
             [](MutableColumns& c) { fill_raw<int8_t>(c, 2, 5); }, "85"},
            {"smallint", FieldType::OLAP_FIELD_TYPE_SMALLINT, 2,
             [](MutableColumns& c) { fill_raw<int16_t>(c, 2, 5); }, "8005"},
            {"int", FieldType::OLAP_FIELD_TYPE_INT, 4,
             [](MutableColumns& c) { fill_raw<int32_t>(c, 2, 5); }, "80000005"},
            {"bigint", FieldType::OLAP_FIELD_TYPE_BIGINT, 8,
             [](MutableColumns& c) { fill_raw<int64_t>(c, 2, 5); }, "8000000000000005"},
            {"largeint", FieldType::OLAP_FIELD_TYPE_LARGEINT, 16,
             [](MutableColumns& c) { fill_raw<__int128>(c, 2, 5); },
             "80000000000000000000000000000005"},
            {"date", FieldType::OLAP_FIELD_TYPE_DATE, 3,
             [](MutableColumns& c) {
                 const auto value =
                         VecDateTimeValue::create_from_olap_date(datev2_bits(2024, 6, 15));
                 fill_raw<VecDateTimeValue>(c, 2, value);
             },
             "0fd0cf"},
            {"datetime", FieldType::OLAP_FIELD_TYPE_DATETIME, 8,
             [](MutableColumns& c) {
                 const auto value =
                         VecDateTimeValue::create_from_olap_datetime(uint64_t(20240615123045ULL));
                 fill_raw<VecDateTimeValue>(c, 2, value);
             },
             "80001268a2aca865"},
            {"datev2", FieldType::OLAP_FIELD_TYPE_DATEV2, 4,
             [](MutableColumns& c) {
                 fill_raw<uint32_t>(c, 2, datev2_bits(2024, 6, 15)); // 2024-06-15
             },
             "000fd0cf"},
            {"datetimev2", FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 8,
             [](MutableColumns& c) {
                 // 2024-06-15 12:30:45
                 fill_raw<uint64_t>(c, 2, datetimev2_bits(2024, 6, 15, 12, 30, 45));
             },
             "01fa19ec7ad00000"},
            {"timestamptz", FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, 8,
             [](MutableColumns& c) {
                 fill_raw<uint64_t>(c, 2, datetimev2_bits(2024, 6, 15, 12, 30, 45));
             },
             "01fa19ec7ad00000"},
    };
}

} // namespace

// A small holder so the convertor (which the accessors point into) and the
// source block stay alive until after the encode calls.
class RowKeyEncoderTest : public testing::Test {
protected:
    void build(const TabletSchemaSPtr& schema, size_t num_rows,
               const std::function<void(MutableColumns&)>& fill) {
        _schema = schema;
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

    TabletSchemaSPtr _schema;
    Block _block;
    std::unique_ptr<OlapBlockDataConvertor> _convertor;
};

// The all-key-types data table has all 17 current key-column types, one column
// per type. One schema uses the common three-column short-key prefix; the other
// schemas span all types (VARCHAR is last, as required when it participates in
// a multi-column short key). The same data is run through non-MOW, MOW,
// sequence and cluster-key schema shapes below.
//
// kAllKeyData is the only row-data source. For key k, rows 3k..3k+2 are
// NULL/low/high with every non-target cell equal, so k alone determines both
// transitions. Each group has its own profile, and increasing k0 profile values
// keep all 51 rows globally sorted.
TEST_F(RowKeyEncoderTest, AllKeyTypesTable) {
    static_assert(kAllKeyData.size() == 3 * kNumKeyColumns);

    // Validate the explicit table before any schema-dependent conversion.
    for (size_t key = 0; key < kNumKeyColumns; ++key) {
        const size_t null_row = 3 * key;
        const size_t low_row = null_row + 1;
        const size_t high_row = low_row + 1;
        EXPECT_EQ(first_different_key(kAllKeyData[null_row], kAllKeyData[low_row]), key)
                << kKeyColumns[key].name;
        EXPECT_EQ(first_different_key(kAllKeyData[low_row], kAllKeyData[high_row]), key)
                << kKeyColumns[key].name;
        for (size_t other_key = 0; other_key < kNumKeyColumns; ++other_key) {
            if (other_key == key) {
                continue;
            }
            EXPECT_EQ(kAllKeyData[null_row][other_key], kAllKeyData[low_row][other_key])
                    << "target=" << kKeyColumns[key].name
                    << ", other=" << kKeyColumns[other_key].name;
            EXPECT_EQ(kAllKeyData[low_row][other_key], kAllKeyData[high_row][other_key])
                    << "target=" << kKeyColumns[key].name
                    << ", other=" << kKeyColumns[other_key].name;
        }
        for (size_t row = 0; row < kAllKeyData.size(); ++row) {
            EXPECT_EQ(!kAllKeyData[row][key].has_value(), row == null_row) << "row=" << row;
        }
    }

    // Adjacent groups deliberately use different values in every column; only
    // the three rows inside one group repeat their non-target profile.
    for (size_t key = 1; key < kNumKeyColumns; ++key) {
        const size_t previous_low_row = 3 * (key - 1) + 1;
        const size_t low_row = 3 * key + 1;
        EXPECT_EQ(first_different_key(kAllKeyData[previous_low_row], kAllKeyData[low_row]), 0);
        for (size_t profile_key = 0; profile_key < kNumKeyColumns; ++profile_key) {
            EXPECT_NE(kAllKeyData[previous_low_row][profile_key], kAllKeyData[low_row][profile_key])
                    << "groups=" << key - 1 << "/" << key
                    << ", key=" << kKeyColumns[profile_key].name;
        }
    }

    constexpr size_t kCharColumn = 15;
    constexpr size_t kSequenceSourceColumn = 2;

    // Keep independent byte-for-byte references for the primary-column and
    // reordered cluster-column sort views. Primary-key bytes always use the
    // former, including in cluster-key schemas.
    std::array<std::vector<std::string>, 2> canonical_full_keys;
    std::array<std::vector<std::string>, 2> canonical_all_column_short_keys;

    // The result file is intentionally exhaustive: one row for every input
    // row in every schema case. Source-row labels make the cluster-key view
    // split visible without decoding the hex strings during review.
    std::vector<std::string> golden_schemas {"schema"};
    std::vector<std::string> golden_physical_rows {"physical_row"};
    std::vector<std::string> golden_sort_data_rows {"sort_data_row"};
    std::vector<std::string> golden_primary_data_rows {"primary_data_row"};
    std::vector<std::string> golden_full_keys {"full_key"};
    std::vector<std::string> golden_primary_keys {"primary_key"};
    std::vector<std::string> golden_short_keys {"short_key"};
    std::vector<std::string> golden_primary_index_keys {"primary_index_key"};

    for (const auto& schema_case : kAllKeySchemaCases) {
        SCOPED_TRACE(schema_case.name);
        const bool mow = schema_case.keys_type == UNIQUE_KEYS &&
                         schema_case.enable_unique_key_merge_on_write;
        ASSERT_FALSE(schema_case.has_cluster_keys && !mow);

        auto schema = std::make_shared<TabletSchema>();
        for (size_t key = 0; key < kKeyColumns.size(); ++key) {
            const auto& spec = kKeyColumns[key];
            schema->append_column(*make_key_column(static_cast<int32_t>(key), spec.type,
                                                   spec.length, spec.index_length, spec.precision,
                                                   spec.frac));
        }
        if (schema_case.has_cluster_keys) {
            for (size_t key = 0; key < kNumKeyColumns; ++key) {
                schema->append_column(*make_cluster_column(key));
            }
            for (const size_t key : kClusterKeyOrder) {
                schema->_cluster_key_uids.push_back(kClusterKeyUidBase + static_cast<int32_t>(key));
            }
        }
        if (schema_case.has_sequence) {
            const auto aggregation = mow ? FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE
                                         : FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE;
            schema->append_column(
                    *make_seq_column(kSequenceColumnUid, FieldType::OLAP_FIELD_TYPE_INT,
                                     kKeyColumns[kSequenceSourceColumn].length, aggregation));
        }
        schema->_keys_type = schema_case.keys_type;
        schema->_num_short_key_columns = schema_case.num_short_key_columns;

        ASSERT_EQ(schema->num_key_columns(), kNumKeyColumns);
        ASSERT_GT(schema_case.num_short_key_columns, 0);
        ASSERT_LE(schema_case.num_short_key_columns, kNumKeyColumns);
        ASSERT_EQ(schema->has_sequence_col(), schema_case.has_sequence);
        ASSERT_EQ(schema->cluster_key_uids().size(),
                  schema_case.has_cluster_keys ? kNumKeyColumns : 0);
        if (schema_case.has_cluster_keys) {
            for (size_t position = 0; position < kNumKeyColumns; ++position) {
                EXPECT_EQ(schema->cluster_key_uids()[position],
                          kClusterKeyUidBase + static_cast<int32_t>(kClusterKeyOrder[position]));
            }
        }

        _schema = schema;
        _block = schema->create_block();
        auto timezone = cctz::utc_time_zone();
        DataTypeSerDe::FormatOptions format_options;
        format_options.timezone = &timezone;
        {
            auto guard = _block.mutate_columns_scoped();
            auto& columns = guard.mutable_columns();
            const auto insert_data_column = [&](size_t schema_column, size_t data_column,
                                                bool reverse_rows) -> testing::AssertionResult {
                const auto serde = guard.get_datatype_by_position(schema_column)->get_serde();
                for (size_t row = 0; row < kAllKeyData.size(); ++row) {
                    const size_t source_row = reverse_rows ? kAllKeyData.size() - 1 - row : row;
                    const auto& cell = kAllKeyData[source_row][data_column];
                    if (!cell.has_value()) {
                        columns[schema_column]->insert_default();
                        continue;
                    }
                    StringRef value(cell->data(), cell->size());
                    const auto status = serde->from_string_strict_mode(
                            value, *columns[schema_column], format_options);
                    if (!status.ok()) {
                        return testing::AssertionFailure()
                               << "schema=" << schema_case.name << ", row=" << row
                               << ", key=" << kKeyColumns[data_column].name << ", value=" << *cell
                               << ": " << status;
                    }
                }
                return testing::AssertionSuccess();
            };

            // A cluster-key segment is physically ordered by cluster columns;
            // primary keys are deliberately reversed to prove the two views
            // are selected independently.
            for (size_t key = 0; key < kNumKeyColumns; ++key) {
                ASSERT_TRUE(insert_data_column(key, key, schema_case.has_cluster_keys));
            }
            if (schema_case.has_cluster_keys) {
                for (size_t key = 0; key < kNumKeyColumns; ++key) {
                    const int32_t column =
                            schema->field_index(kClusterKeyUidBase + static_cast<int32_t>(key));
                    ASSERT_GE(column, 0);
                    ASSERT_TRUE(insert_data_column(static_cast<size_t>(column), key, false));
                }
            }
            if (schema_case.has_sequence) {
                ASSERT_TRUE(insert_data_column(static_cast<size_t>(schema->sequence_col_idx()),
                                               kSequenceSourceColumn,
                                               schema_case.has_cluster_keys));
            }
        }
        _convertor = std::make_unique<OlapBlockDataConvertor>(_schema.get());
        _convertor->set_source_content(&_block, 0, kAllKeyData.size());

        std::vector<IOlapColumnDataAccessor*> primary_columns;
        primary_columns.reserve(kNumKeyColumns);
        for (size_t key = 0; key < kNumKeyColumns; ++key) {
            primary_columns.push_back(acc(key));
        }

        std::vector<IOlapColumnDataAccessor*> sort_columns;
        sort_columns.reserve(kNumKeyColumns);
        if (schema_case.has_cluster_keys) {
            for (const auto uid : schema->cluster_key_uids()) {
                const int32_t column = schema->field_index(static_cast<int32_t>(uid));
                ASSERT_GE(column, 0);
                sort_columns.push_back(acc(static_cast<uint32_t>(column)));
            }
        } else {
            sort_columns = primary_columns;
        }
        auto short_key_columns = sort_columns;
        short_key_columns.resize(schema_case.num_short_key_columns);
        const auto data_column_in_short_key = [&](size_t data_column) {
            if (!schema_case.has_cluster_keys) {
                return data_column < schema_case.num_short_key_columns;
            }
            for (size_t position = 0; position < schema_case.num_short_key_columns; ++position) {
                if (kClusterKeyOrder[position] == data_column) {
                    return true;
                }
            }
            return false;
        };

        IOlapColumnDataAccessor* sequence_column = nullptr;
        if (schema_case.has_sequence) {
            sequence_column = acc(static_cast<uint32_t>(schema->sequence_col_idx()));
        }

        RowKeyEncoder encoder(*_schema, mow);
        ASSERT_EQ(encoder.num_sort_key_columns(), kNumKeyColumns);
        ASSERT_EQ(sort_columns.size(), kNumKeyColumns);
        ASSERT_EQ(short_key_columns.size(), schema_case.num_short_key_columns);

        for (size_t key = 0; key < kNumKeyColumns; ++key) {
            for (size_t row = 0; row < kAllKeyData.size(); ++row) {
                const size_t primary_source_row =
                        schema_case.has_cluster_keys ? kAllKeyData.size() - 1 - row : row;
                EXPECT_EQ(_block.get_by_position(key).column->is_null_at(row),
                          !kAllKeyData[primary_source_row][key].has_value())
                        << "row=" << row << ", key=" << kKeyColumns[key].name;
                if (schema_case.has_cluster_keys) {
                    const int32_t cluster_column =
                            schema->field_index(kClusterKeyUidBase + static_cast<int32_t>(key));
                    ASSERT_GE(cluster_column, 0);
                    EXPECT_EQ(_block.get_by_position(static_cast<size_t>(cluster_column))
                                      .column->is_null_at(row),
                              !kAllKeyData[row][key].has_value())
                            << "row=" << row << ", cluster_key=" << kKeyColumns[key].name;
                }
            }
        }

        std::vector<std::string> full_keys;
        std::vector<std::string> primary_keys;
        std::vector<std::string> short_keys;
        std::vector<std::string> primary_index_keys;
        full_keys.reserve(kAllKeyData.size());
        primary_keys.reserve(kAllKeyData.size());
        short_keys.reserve(kAllKeyData.size());
        primary_index_keys.reserve(kAllKeyData.size());

        for (size_t row = 0; row < kAllKeyData.size(); ++row) {
            SCOPED_TRACE(row);
            full_keys.push_back(encoder.full_encode(sort_columns, row));
            primary_keys.push_back(encoder.full_encode_primary_keys(primary_columns, row));
            short_keys.push_back(encoder.encode_short_keys(short_key_columns, row));

            std::string primary_index_key = primary_keys.back();
            const size_t primary_source_row =
                    schema_case.has_cluster_keys ? kAllKeyData.size() - 1 - row : row;
            if (schema_case.has_sequence) {
                const size_t suffix_offset = primary_index_key.size();
                encoder.append_seq_suffix(&primary_index_key, sequence_column, row);
                EXPECT_EQ(primary_index_key.size(),
                          suffix_offset + 1 + kKeyColumns[kSequenceSourceColumn].length);
                const uint8_t expected_marker =
                        kAllKeyData[primary_source_row][kSequenceSourceColumn].has_value()
                                ? KeyConsts::KEY_NORMAL_MARKER
                                : KeyConsts::KEY_NULL_FIRST_MARKER;
                EXPECT_EQ(static_cast<uint8_t>(primary_index_key[suffix_offset]), expected_marker);
            }
            if (schema_case.has_cluster_keys) {
                const size_t suffix_offset = primary_index_key.size();
                encoder.append_rowid_suffix(&primary_index_key, static_cast<uint32_t>(row));
                EXPECT_EQ(primary_index_key.size(), suffix_offset + 1 + sizeof(uint32_t));
                EXPECT_EQ(static_cast<uint8_t>(primary_index_key[suffix_offset]),
                          KeyConsts::KEY_NORMAL_MARKER);
            }
            primary_index_keys.push_back(std::move(primary_index_key));

            golden_schemas.emplace_back(schema_case.name);
            golden_physical_rows.push_back(std::to_string(row));
            golden_sort_data_rows.push_back(key_data_row_label(row));
            golden_primary_data_rows.push_back(key_data_row_label(primary_source_row));
            golden_full_keys.push_back(to_hex(full_keys.back()));
            golden_primary_keys.push_back(to_hex(primary_keys.back()));
            golden_short_keys.push_back(to_hex(short_keys.back()));
            golden_primary_index_keys.push_back(to_hex(primary_index_keys.back()));

            size_t expected_short_key_size = 0;
            for (size_t position = 0; position < schema_case.num_short_key_columns; ++position) {
                const size_t data_column =
                        schema_case.has_cluster_keys ? kClusterKeyOrder[position] : position;
                const auto& cell = kAllKeyData[row][data_column];
                ++expected_short_key_size; // nullable marker
                if (!cell.has_value()) {
                    continue;
                }
                const auto& spec = kKeyColumns[data_column];
                if (spec.type == FieldType::OLAP_FIELD_TYPE_CHAR ||
                    spec.type == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
                    expected_short_key_size +=
                            std::min(cell->size(), static_cast<size_t>(spec.index_length));
                } else {
                    expected_short_key_size += static_cast<size_t>(spec.index_length);
                }
            }
            EXPECT_EQ(short_keys.back().size(), expected_short_key_size);

            if (!schema_case.has_cluster_keys) {
                EXPECT_EQ(primary_keys.back(), full_keys.back());
            }
            if (row != 0) {
                EXPECT_LT(full_keys[row - 1], full_keys[row]);
                if (schema_case.has_cluster_keys) {
                    EXPECT_GT(primary_keys[row - 1], primary_keys[row]);
                    EXPECT_GT(primary_index_keys[row - 1], primary_index_keys[row]);
                } else {
                    EXPECT_LT(primary_keys[row - 1], primary_keys[row]);
                    EXPECT_LT(primary_index_keys[row - 1], primary_index_keys[row]);
                }
                const size_t target_key = row / 3;
                const bool target_in_short_key =
                        row % 3 == 0 || data_column_in_short_key(target_key);
                const bool truncated_equal = target_key == kCharColumn && row % 3 == 2;
                if (!target_in_short_key || truncated_equal) {
                    EXPECT_EQ(short_keys[row - 1], short_keys[row]);
                } else {
                    EXPECT_LT(short_keys[row - 1], short_keys[row]);
                }
            }
        }

        const size_t sort_view = schema_case.has_cluster_keys ? 1 : 0;
        if (canonical_full_keys[sort_view].empty()) {
            canonical_full_keys[sort_view] = full_keys;
        } else {
            EXPECT_EQ(full_keys, canonical_full_keys[sort_view]);
        }
        if (schema_case.num_short_key_columns == kNumKeyColumns) {
            if (canonical_all_column_short_keys[sort_view].empty()) {
                canonical_all_column_short_keys[sort_view] = short_keys;
            } else {
                EXPECT_EQ(short_keys, canonical_all_column_short_keys[sort_view]);
            }
        }

        if (schema_case.has_cluster_keys) {
            EXPECT_NE(primary_keys.front(), full_keys.front());
            for (size_t row = 0; row < kAllKeyData.size(); ++row) {
                EXPECT_EQ(primary_keys[row], canonical_full_keys[0][kAllKeyData.size() - 1 - row]);
            }

            // SegmentWriter sorts these primary-index entries in memory because
            // physical row order follows cluster keys, not primary keys.
            auto sorted_primary_index_keys = primary_index_keys;
            std::sort(sorted_primary_index_keys.begin(), sorted_primary_index_keys.end());
            for (size_t row = 1; row < sorted_primary_index_keys.size(); ++row) {
                EXPECT_LT(sorted_primary_index_keys[row - 1], sorted_primary_index_keys[row]);
            }
        }

        // The sort view always uses kAllKeyData in its written order.
        for (size_t key = 0; key < kNumKeyColumns; ++key) {
            SCOPED_TRACE(kKeyColumns[key].name);
            const size_t null_row = 3 * key;
            const size_t low_row = null_row + 1;
            const size_t high_row = low_row + 1;
            expect_null_first_encoding(full_keys, null_row, low_row);
            EXPECT_LT(full_keys[low_row], full_keys[high_row]);

            if (!data_column_in_short_key(key)) {
                EXPECT_EQ(short_keys[null_row], short_keys[low_row]);
                EXPECT_EQ(short_keys[low_row], short_keys[high_row]);
            } else {
                expect_null_first_encoding(short_keys, null_row, low_row);
                if (key == kCharColumn) {
                    EXPECT_EQ(short_keys[low_row], short_keys[high_row]);
                } else {
                    EXPECT_LT(short_keys[low_row], short_keys[high_row]);
                }
            }

            // The primary view uses the same source rows, reversed only for a
            // cluster-key segment.
            const auto primary_row = [&](size_t source_row) {
                return schema_case.has_cluster_keys ? kAllKeyData.size() - 1 - source_row
                                                    : source_row;
            };
            const size_t primary_null_row = primary_row(null_row);
            const size_t primary_low_row = primary_row(low_row);
            const size_t primary_high_row = primary_row(high_row);
            expect_null_first_encoding(primary_keys, primary_null_row, primary_low_row);
            EXPECT_LT(primary_keys[primary_low_row], primary_keys[primary_high_row]);
        }
    }

    // Refresh this byte-level snapshot with:
    //   ./run-be-ut.sh --run --filter=RowKeyEncoderTest.AllKeyTypesTable --gen_out -j 64
    const char* root = std::getenv("ROOT");
    ASSERT_NE(root, nullptr);
    check_or_generate_res_file(
            std::string(root) +
                    "/be/test/expected_result/storage/key/row_key_encoder_all_key_types.out",
            {golden_schemas, golden_physical_rows, golden_sort_data_rows, golden_primary_data_rows,
             golden_full_keys, golden_primary_keys, golden_short_keys, golden_primary_index_keys});
}

// The sequence suffix over every seq-eligible type: a normal value appends
// marker + coder bytes; a NULL value appends the null-first marker followed
// by column-length minimal-marker bytes, so within one key a NULL sequence
// sorts before every real one. The filler length is the one type-dependent
// behavior of the suffix.
TEST_F(RowKeyEncoderTest, SeqSuffix) {
    for (const auto& tc : seq_cases()) {
        SCOPED_TRACE(tc.name);
        auto schema = std::make_shared<TabletSchema>();
        schema->append_column(*create_int_key(0));
        schema->append_column(
                *create_int_value(1, FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE));
        schema->append_column(*make_seq_column(2, tc.type, tc.length));
        schema->_keys_type = UNIQUE_KEYS;
        schema->_num_short_key_columns = 1;
        build(schema, 2, [&](MutableColumns& c) {
            fill_int(c, 0, {1, 1});
            fill_int(c, 1, {0, 0});
            tc.fill_value(c);
            c[2]->insert_default(); // row 1: NULL sequence value
        });
        RowKeyEncoder enc(*_schema, /*mow=*/true);
        IOlapColumnDataAccessor* seq = acc(2);

        std::string normal;
        enc.append_seq_suffix(&normal, seq, 0);
        std::string null_suffix;
        enc.append_seq_suffix(&null_suffix, seq, 1);
        EXPECT_EQ(to_hex(normal), "02" + tc.value_hex);
        EXPECT_EQ(to_hex(null_suffix), "01" + std::string(tc.length * 2, '0'));
        EXPECT_LT(null_suffix, normal);
    }
}

// A cluster-key segment may contain several rows with the same primary key and
// sequence value. Their primary-index prefixes are identical, so rowid is the
// only tie-breaker. The chosen rowids cross every relevant byte/sign boundary
// of the UNSIGNED_INT coder.
TEST_F(RowKeyEncoderTest, RowidSuffix) {
    struct RowidDataRow {
        int32_t primary_key;
        int32_t cluster_key;
        int32_t sequence;
        uint32_t rowid;
        std::string_view expected_rowid_suffix;
    };
    constexpr std::array<RowidDataRow, 11> kRows = {{
            {42, 10, 7, 0, "0200000000"},
            {42, 20, 7, 1, "0200000001"},
            {42, 30, 7, 255, "02000000ff"},
            {42, 40, 7, 256, "0200000100"},
            {42, 50, 7, 65535, "020000ffff"},
            {42, 60, 7, 65536, "0200010000"},
            {42, 70, 7, 16777215, "0200ffffff"},
            {42, 80, 7, 16777216, "0201000000"},
            {42, 90, 7, static_cast<uint32_t>(std::numeric_limits<int32_t>::max()), "027fffffff"},
            {42, 100, 7, static_cast<uint32_t>(std::numeric_limits<int32_t>::max()) + 1,
             "0280000000"},
            {42, 110, 7, std::numeric_limits<uint32_t>::max(), "02ffffffff"},
    }};
    constexpr std::string_view kPrimaryWithSequenceHex = "028000002a0280000007";

    auto schema = cluster_key_with_sequence_schema();
    build(schema, kRows.size(), [&](MutableColumns& columns) {
        // Rowid is not a block column. SegmentWriter obtains it from the
        // physical position and passes it separately to append_rowid_suffix().
        for (const auto& data : kRows) {
            fill_raw<int32_t>(columns, 0, data.primary_key);
            fill_raw<int32_t>(columns, 1, data.cluster_key);
            fill_raw<int32_t>(columns, 2, data.sequence);
        }
    });

    RowKeyEncoder encoder(*schema, /*mow=*/true);
    std::vector<IOlapColumnDataAccessor*> sort_columns {acc(1)};
    std::vector<IOlapColumnDataAccessor*> primary_columns {acc(0)};
    IOlapColumnDataAccessor* sequence_column = acc(2);
    std::string previous_sort_key;
    std::string previous_primary_index_key;
    for (size_t row = 0; row < kRows.size(); ++row) {
        const auto& data = kRows[row];
        const std::string sort_key = encoder.full_encode(sort_columns, row);
        std::string primary_index_key = encoder.full_encode_primary_keys(primary_columns, row);
        encoder.append_seq_suffix(&primary_index_key, sequence_column, row);
        EXPECT_EQ(to_hex(primary_index_key), kPrimaryWithSequenceHex);
        encoder.append_rowid_suffix(&primary_index_key, data.rowid);
        EXPECT_EQ(to_hex(primary_index_key),
                  std::string(kPrimaryWithSequenceHex) + std::string(data.expected_rowid_suffix));

        if (row > 0) {
            EXPECT_LT(previous_sort_key, sort_key);
            EXPECT_LT(previous_primary_index_key, primary_index_key);
        }
        previous_sort_key = sort_key;
        previous_primary_index_key = primary_index_key;
    }
}

} // namespace doris
