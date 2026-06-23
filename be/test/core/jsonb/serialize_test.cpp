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
#include "util/jsonb/serialize.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/descriptors.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <math.h>
#include <stdint.h>

#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/exception.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_hll.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_quantilestate.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/field.h"
#include "core/types.h"
#include "core/value/bitmap_value.h"
#include "core/value/hll.h"
#include "core/value/jsonb_value.h"
#include "core/value/quantile_state.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "exec/common/variant_util.h"
#include "exprs/aggregate/aggregate_function.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/descriptors.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/test_util.h"

namespace doris {

// Golden-file path (same convention as be/test/util/jsonb_serialize_test.cpp):
//   - source of truth: be/test/util/test_data/block_to_jsonb_all_types_golden.bin
//   - run-be-ut.sh copies be/test/util/test_data/ into the build dir before tests run,
//     so the runtime path under GetCurrentRunningDir() is /util/test_data/<file>.
//
// File format (little-endian):
//   u32 num_rows
//   for each row:
//     u32 size_i
//     <size_i> bytes : raw JSONB bytes for that row
//
// Regenerate (only after an intentional encoding change):
//   DORIS_REGEN_JSONB_GOLDEN=1 ... ./doris_be_test \
//       --gtest_filter='BlockSerializeTest.GenerateAllRowStoreSupportedTypesGolden'
//   then `git add` the regenerated file.
inline static const std::string kBlockToJsonbGoldenRel =
        "/util/test_data/block_to_jsonb_all_types_golden.bin";

// Generator-side helper: writes a ColumnString full of per-row JSONB blobs to disk.
// The normal verify path NEVER calls this; only the gated regen test does.
static void dump_jsonb_column_to_file(const ColumnString& jsonb_col, const std::string& path) {
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    if (!ofs) {
        throw std::runtime_error("Failed to open golden file for write: " + path);
    }
    const uint32_t num_rows = static_cast<uint32_t>(jsonb_col.size());
    ofs.write(reinterpret_cast<const char*>(&num_rows), sizeof(num_rows));
    for (uint32_t i = 0; i < num_rows; ++i) {
        auto ref = jsonb_col.get_data_at(i);
        const uint32_t sz = static_cast<uint32_t>(ref.size);
        ofs.write(reinterpret_cast<const char*>(&sz), sizeof(sz));
        ofs.write(ref.data, ref.size);
    }
    ofs.close();
    if (!ofs) {
        throw std::runtime_error("Failed to write golden file: " + path);
    }
}

// Verifier-side helper: rebuilds a ColumnString from the golden file. The returned
// column mimics the on-disk row-store column that jsonb_to_block consumes.
static MutableColumnPtr load_jsonb_column_from_file(const std::string& path) {
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs.is_open()) {
        throw std::runtime_error(
                "Golden file not found: " + path +
                ". Regenerate with DORIS_REGEN_JSONB_GOLDEN=1 + the *Generate* test.");
    }
    uint32_t num_rows = 0;
    ifs.read(reinterpret_cast<char*>(&num_rows), sizeof(num_rows));
    auto col = ColumnString::create();
    for (uint32_t i = 0; i < num_rows; ++i) {
        uint32_t sz = 0;
        ifs.read(reinterpret_cast<char*>(&sz), sizeof(sz));
        std::string buf(sz, '\0');
        ifs.read(buf.data(), sz);
        if (!ifs.good() && !ifs.eof()) {
            throw std::runtime_error("Truncated golden file at row " + std::to_string(i) + ": " +
                                     path);
        }
        col->insert_data(buf.data(), sz);
    }
    return col;
}

// Shared: builds the deterministic Block + TabletSchema that covers every row-store
// supported type (the union of types whose SerDe implements write_one_cell_to_jsonb
// without throwing NOT_IMPLEMENTED): numeric scalars, decimals, dates, IPs, strings
// (STRING / VARCHAR / CHAR / JSONB), BITMAP / HLL / QUANTILE_STATE, ARRAY / MAP /
// STRUCT, and Nullable<> wrappers.
//
// Used by:
//   (1) the generator test, which encodes this block via block_to_jsonb and writes
//       the resulting bytes to the golden file.
//   (2) the verifier test, which uses it as (a) the schema/data-type source needed
//       by jsonb_to_block and (b) the ground-truth expected values to assert against
//       the decoded block.
static void build_all_row_store_types_block(Block& block, TabletSchema& schema) {
    constexpr int kNumRows = 3;
    int32_t cid = 1;
    auto add = [&](const std::string& name, DataTypePtr type, MutableColumnPtr col) {
        TabletColumn c;
        c.set_name(name);
        c.set_unique_id(cid++);
        c.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        schema.append_column(c);
        block.insert(ColumnWithTypeAndName(std::move(col), type, name));
    };

    // ---- numeric scalars ----
    {
        auto col = ColumnUInt8::create();
        col->get_data() = {0, 1, 0};
        add("c_bool", std::make_shared<DataTypeUInt8>(), std::move(col));
    }
    {
        auto col = ColumnInt8::create();
        col->get_data() = {-1, 0, 127};
        add("c_tinyint", std::make_shared<DataTypeInt8>(), std::move(col));
    }
    {
        auto col = ColumnInt16::create();
        col->get_data() = {-32000, 0, 32000};
        add("c_smallint", std::make_shared<DataTypeInt16>(), std::move(col));
    }
    {
        auto col = ColumnInt32::create();
        col->get_data() = {-1, 0, 1024};
        add("c_int", std::make_shared<DataTypeInt32>(), std::move(col));
    }
    {
        auto col = ColumnInt64::create();
        col->get_data() = {-1, 0, 1L << 40};
        add("c_bigint", std::make_shared<DataTypeInt64>(), std::move(col));
    }
    {
        auto col = ColumnInt128::create();
        col->get_data() = {static_cast<Int128>(-1), static_cast<Int128>(0),
                           static_cast<Int128>(1) << 100};
        add("c_largeint", std::make_shared<DataTypeInt128>(), std::move(col));
    }
    {
        auto col = ColumnFloat32::create();
        col->get_data() = {-1.5f, 0.0f, 3.14f};
        add("c_float", std::make_shared<DataTypeFloat32>(), std::move(col));
    }
    {
        auto col = ColumnFloat64::create();
        col->get_data() = {-1.5, 0.0, 3.14};
        add("c_double", std::make_shared<DataTypeFloat64>(), std::move(col));
    }

    // ---- date / time ----
    {
        auto col = ColumnDateV2::create();
        auto pack = [](int y, int m, int d) {
            DateV2Value<DateV2ValueType> v;
            v.unchecked_set_time(y, m, d, 0, 0, 0, 0);
            return *reinterpret_cast<UInt32*>(&v);
        };
        col->get_data() = {pack(2022, 6, 6), pack(2024, 12, 31), pack(2026, 5, 20)};
        add("c_datev2", std::make_shared<DataTypeDateV2>(), std::move(col));
    }
    {
        auto col = ColumnDateTimeV2::create();
        auto pack = [](int y, int mo, int d, int h, int mi, int s) {
            DateV2Value<DateTimeV2ValueType> v;
            v.unchecked_set_time(y, mo, d, h, mi, s, 0);
            return *reinterpret_cast<UInt64*>(&v);
        };
        col->get_data() = {pack(2022, 6, 6, 12, 0, 0), pack(2024, 12, 31, 23, 59, 59),
                           pack(2026, 5, 20, 1, 2, 3)};
        add("c_datetimev2", std::make_shared<DataTypeDateTimeV2>(0), std::move(col));
    }
    {
        auto col = ColumnTimeV2::create();
        col->get_data() = {0.0, 1500000.0, 3600000000.0};
        add("c_timev2", std::make_shared<DataTypeTimeV2>(0), std::move(col));
    }

    // ---- legacy DATE / DATETIME (VecDateTimeValue-backed, Int64 storage) ----
    {
        auto col = ColumnDate::create();
        VecDateTimeValue v1;
        VecDateTimeValue v2;
        VecDateTimeValue v3;
        v1.unchecked_set_time(2020, 1, 1, 0, 0, 0);
        v2.unchecked_set_time(2022, 6, 15, 0, 0, 0);
        v3.unchecked_set_time(2026, 5, 20, 0, 0, 0);
        col->insert_value(v1);
        col->insert_value(v2);
        col->insert_value(v3);
        add("c_date", std::make_shared<DataTypeDate>(), std::move(col));
    }
    {
        auto col = ColumnDateTime::create();
        VecDateTimeValue v1;
        VecDateTimeValue v2;
        VecDateTimeValue v3;
        v1.unchecked_set_time(2020, 1, 1, 12, 30, 45);
        v2.unchecked_set_time(2022, 6, 15, 23, 59, 59);
        v3.unchecked_set_time(2026, 5, 20, 1, 2, 3);
        col->insert_value(v1);
        col->insert_value(v2);
        col->insert_value(v3);
        add("c_datetime", std::make_shared<DataTypeDateTime>(), std::move(col));
    }
    // ---- TIMESTAMPTZ (TimestampTzValue, UInt64 storage) ----
    {
        auto col = ColumnTimeStampTz::create();
        TimestampTzValue v1;
        TimestampTzValue v2;
        TimestampTzValue v3;
        v1.unchecked_set_time(2020, 1, 1, 12, 30, 45);
        v2.unchecked_set_time(2024, 12, 31, 23, 59, 59);
        v3.unchecked_set_time(2026, 5, 20, 0, 0, 0);
        col->insert_value(v1);
        col->insert_value(v2);
        col->insert_value(v3);
        add("c_timestamptz", std::make_shared<DataTypeTimeStampTz>(0), std::move(col));
    }

    // ---- IPs ----
    {
        auto col = ColumnIPv4::create();
        col->get_data() = {static_cast<IPv4>(0), static_cast<IPv4>(0x7f000001),
                           static_cast<IPv4>(0xc0a80101)};
        add("c_ipv4", std::make_shared<DataTypeIPv4>(), std::move(col));
    }
    {
        auto col = ColumnIPv6::create();
        col->get_data() = {static_cast<IPv6>(0), static_cast<IPv6>(1),
                           (static_cast<IPv6>(0x1234567890abcdefULL) << 64) |
                                   static_cast<IPv6>(0xfedcba0987654321ULL)};
        add("c_ipv6", std::make_shared<DataTypeIPv6>(), std::move(col));
    }

    // ---- strings ----
    {
        auto col = ColumnString::create();
        col->insert_data("hello", 5);
        col->insert_data("", 0);
        col->insert_data("doris row store", 15);
        add("c_string", std::make_shared<DataTypeString>(), std::move(col));
    }
    {
        // VARCHAR: same SerDe / ColumnString as STRING.
        auto col = ColumnString::create();
        col->insert_data("vc-a", 4);
        col->insert_data("vc-bb", 5);
        col->insert_data("", 0);
        add("c_varchar", std::make_shared<DataTypeString>(64, TYPE_VARCHAR), std::move(col));
    }
    {
        // CHAR: row store does NOT pad to fixed length — bytes are passthrough.
        auto col = ColumnString::create();
        col->insert_data("ch-x", 4);
        col->insert_data("", 0);
        col->insert_data("ch-zzz", 6);
        add("c_char", std::make_shared<DataTypeString>(8, TYPE_CHAR), std::move(col));
    }
    {
        // JSONB: bytes must be valid JSONB binary (dump_data decodes via JsonbToJson).
        auto col = ColumnString::create();
        const char* payloads[] = {"{\"k\":1}", "[1,2,3]", "\"abc\""};
        for (const auto* p : payloads) {
            JsonBinaryValue jb;
            THROW_IF_ERROR(jb.from_json_string(p, strlen(p)));
            col->insert_data(jb.value(), jb.size());
        }
        add("c_jsonb", std::make_shared<DataTypeJsonb>(), std::move(col));
    }

    // ---- decimals ----
    {
        DataTypePtr t = doris::create_decimal(27, 9, true);
        auto col = t->create_column();
        auto& data = static_cast<ColumnDecimal128V2*>(col.get())->get_data();
        for (int i = 1; i <= kNumRows; ++i) {
            data.push_back(static_cast<__int128_t>(i) * 1000000000);
        }
        add("c_decimalv2", t, std::move(col));
    }
    {
        DataTypePtr t = doris::create_decimal(9, 2, false);
        auto col = t->create_column();
        auto& data = static_cast<ColumnDecimal32*>(col.get())->get_data();
        for (int i = 1; i <= kNumRows; ++i) {
            data.push_back(i * 100);
        }
        add("c_decimal32", t, std::move(col));
    }
    {
        DataTypePtr t = doris::create_decimal(18, 4, false);
        auto col = t->create_column();
        auto& data = static_cast<ColumnDecimal64*>(col.get())->get_data();
        for (int i = 1; i <= kNumRows; ++i) {
            data.push_back(static_cast<int64_t>(i) * 10000);
        }
        add("c_decimal64", t, std::move(col));
    }
    {
        DataTypePtr t = doris::create_decimal(27, 9, false);
        auto col = t->create_column();
        auto& data = static_cast<ColumnDecimal128V3*>(col.get())->get_data();
        for (int i = 1; i <= kNumRows; ++i) {
            data.push_back(static_cast<__int128_t>(i) * 1000000000);
        }
        add("c_decimal128i", t, std::move(col));
    }
    {
        DataTypePtr t = doris::create_decimal(76, 10, false);
        auto col = t->create_column();
        auto& data = static_cast<ColumnDecimal256*>(col.get())->get_data();
        for (int i = 1; i <= kNumRows; ++i) {
            data.push_back(wide::Int256(i) * wide::Int256(10000000000LL));
        }
        add("c_decimal256", t, std::move(col));
    }

    // ---- object types ----
    {
        DataTypePtr t = std::make_shared<DataTypeBitMap>();
        auto col = t->create_column();
        auto& container = static_cast<ColumnBitmap*>(col.get())->get_data();
        for (int i = 0; i < kNumRows; ++i) {
            BitmapValue bv;
            for (int j = 0; j <= i; ++j) {
                bv.add(j);
            }
            container.push_back(bv);
        }
        add("c_bitmap", t, std::move(col));
    }
    {
        DataTypePtr t = std::make_shared<DataTypeHLL>();
        auto col = t->create_column();
        auto& container = static_cast<ColumnHLL*>(col.get())->get_data();
        for (int i = 0; i < kNumRows; ++i) {
            HyperLogLog hll;
            hll.update(i);
            container.push_back(hll);
        }
        add("c_hll", t, std::move(col));
    }
    {
        DataTypePtr t = std::make_shared<DataTypeQuantileState>();
        auto col = t->create_column();
        auto& container = static_cast<ColumnQuantileState*>(col.get())->get_data();
        for (int i = 0; i < kNumRows; ++i) {
            QuantileState qs;
            qs.add_value(static_cast<double>(i + 1));
            container.push_back(qs);
        }
        add("c_quantilestate", t, std::move(col));
    }

    // ---- nested ARRAY<INT> ----
    {
        auto off = ColumnOffset64::create();
        auto data = ColumnInt32::create();
        std::vector<ColumnArray::Offset64> offs = {0, 2, 2, 4}; // [[1,2],[],[3,4]]
        std::vector<int32_t> vals = {1, 2, 3, 4};
        for (size_t i = 1; i < offs.size(); ++i) {
            off->insert_data(reinterpret_cast<const char*>(&offs[i]), 0);
        }
        for (auto v : vals) {
            data->insert_data(reinterpret_cast<const char*>(&v), 0);
        }
        DataTypePtr nested = make_nullable(std::make_shared<DataTypeInt32>());
        DataTypePtr t = std::make_shared<DataTypeArray>(nested);
        auto arr = ColumnArray::create(make_nullable(std::move(data)), std::move(off));
        add("c_array_int", t, std::move(arr));
    }

    // ---- nested MAP<STRING, STRING> ----
    {
        DataTypePtr s = make_nullable(std::make_shared<DataTypeString>());
        DataTypePtr t = std::make_shared<DataTypeMap>(s, s);
        MutableColumnPtr col = t->create_column();
        for (int i = 0; i < kNumRows; ++i) {
            Array k;
            Array v;
            k.push_back(Field::create_field<TYPE_STRING>("k" + std::to_string(i)));
            v.push_back(Field::create_field<TYPE_STRING>("v" + std::to_string(i)));
            Map m;
            m.push_back(Field::create_field<TYPE_ARRAY>(k));
            m.push_back(Field::create_field<TYPE_ARRAY>(v));
            col->insert(Field::create_field<TYPE_MAP>(m));
        }
        add("c_map", t, std::move(col));
    }

    // ---- nested STRUCT<INT, STRING> ----
    {
        DataTypePtr i = make_nullable(std::make_shared<DataTypeInt32>());
        DataTypePtr s = make_nullable(std::make_shared<DataTypeString>());
        DataTypePtr t = std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {i, s});
        MutableColumnPtr col = t->create_column();
        for (int x = 0; x < kNumRows; ++x) {
            Struct st;
            st.push_back(Field::create_field<TYPE_INT>(x));
            st.push_back(Field::create_field<TYPE_STRING>("name" + std::to_string(x)));
            col->insert(Field::create_field<TYPE_STRUCT>(st));
        }
        add("c_struct", t, std::move(col));
    }

    // ---- VARIANT (parsed from JSON literals) ----
    {
        auto t = std::make_shared<DataTypeVariant>();
        auto col = t->create_column();
        auto json_col = ColumnString::create();
        const char* jsons[] = {"{\"a\":1}", "{\"b\":\"hello\"}", "[1,2,3]"};
        for (const auto* j : jsons) {
            json_col->insert_data(j, strlen(j));
        }
        ParseConfig parse_config;
        variant_util::parse_json_to_variant(*col, *json_col, parse_config);
        add("c_variant", t, std::move(col));
    }

    // ---- Nullable<> wrappers ----
    // Null cells are skipped on write; jsonb_to_block fills them back via insert_default.
    {
        DataTypePtr t = make_nullable(std::make_shared<DataTypeInt32>());
        auto col = t->create_column();
        col->insert_default();
        col->insert(Field::create_field<TYPE_INT>(42));
        col->insert_default();
        add("c_nullable_int", t, std::move(col));
    }
    {
        DataTypePtr t = make_nullable(std::make_shared<DataTypeString>());
        auto col = t->create_column();
        col->insert(Field::create_field<TYPE_STRING>("a"));
        col->insert_default();
        col->insert(Field::create_field<TYPE_STRING>("c"));
        add("c_nullable_string", t, std::move(col));
    }
    {
        DataTypePtr t = make_nullable(std::make_shared<DataTypeBitMap>());
        auto col = t->create_column();
        BitmapValue bv;
        bv.add(7);
        bv.add(9);
        col->insert_default();
        col->insert(Field::create_field<TYPE_BITMAP>(BitmapValue(bv)));
        col->insert_default();
        add("c_nullable_bitmap", t, std::move(col));
    }

    DCHECK_EQ(block.rows(), kNumRows);
}

// Schema-only scaffold for the verifier: builds an EMPTY destination Block whose column
// types, names, and unique ids must match what build_all_row_store_types_block produces.
// jsonb_to_block will fill rows into this block from the golden file. The two functions
// are intentionally parallel — they must stay in sync (column order, name, type, cid).
static void make_all_row_store_types_dst_block(Block& dst, TabletSchema& schema) {
    int32_t cid = 1;
    auto add = [&](const std::string& name, DataTypePtr type) {
        TabletColumn c;
        c.set_name(name);
        c.set_unique_id(cid++);
        c.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        schema.append_column(c);
        dst.insert(ColumnWithTypeAndName(type->create_column(), type, name));
    };
    add("c_bool", std::make_shared<DataTypeUInt8>());
    add("c_tinyint", std::make_shared<DataTypeInt8>());
    add("c_smallint", std::make_shared<DataTypeInt16>());
    add("c_int", std::make_shared<DataTypeInt32>());
    add("c_bigint", std::make_shared<DataTypeInt64>());
    add("c_largeint", std::make_shared<DataTypeInt128>());
    add("c_float", std::make_shared<DataTypeFloat32>());
    add("c_double", std::make_shared<DataTypeFloat64>());
    add("c_datev2", std::make_shared<DataTypeDateV2>());
    add("c_datetimev2", std::make_shared<DataTypeDateTimeV2>(0));
    add("c_timev2", std::make_shared<DataTypeTimeV2>(0));
    add("c_date", std::make_shared<DataTypeDate>());
    add("c_datetime", std::make_shared<DataTypeDateTime>());
    add("c_timestamptz", std::make_shared<DataTypeTimeStampTz>(0));
    add("c_ipv4", std::make_shared<DataTypeIPv4>());
    add("c_ipv6", std::make_shared<DataTypeIPv6>());
    add("c_string", std::make_shared<DataTypeString>());
    add("c_varchar", std::make_shared<DataTypeString>(64, TYPE_VARCHAR));
    add("c_char", std::make_shared<DataTypeString>(8, TYPE_CHAR));
    add("c_jsonb", std::make_shared<DataTypeJsonb>());
    add("c_decimalv2", doris::create_decimal(27, 9, true));
    add("c_decimal32", doris::create_decimal(9, 2, false));
    add("c_decimal64", doris::create_decimal(18, 4, false));
    add("c_decimal128i", doris::create_decimal(27, 9, false));
    add("c_decimal256", doris::create_decimal(76, 10, false));
    add("c_bitmap", std::make_shared<DataTypeBitMap>());
    add("c_hll", std::make_shared<DataTypeHLL>());
    add("c_quantilestate", std::make_shared<DataTypeQuantileState>());
    add("c_array_int",
        std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>())));
    add("c_map", std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()),
                                               make_nullable(std::make_shared<DataTypeString>())));
    add("c_struct", std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {
                            make_nullable(std::make_shared<DataTypeInt32>()),
                            make_nullable(std::make_shared<DataTypeString>())}));
    add("c_variant", std::make_shared<DataTypeVariant>());
    add("c_nullable_int", make_nullable(std::make_shared<DataTypeInt32>()));
    add("c_nullable_string", make_nullable(std::make_shared<DataTypeString>()));
    add("c_nullable_bitmap", make_nullable(std::make_shared<DataTypeBitMap>()));
}

static void fill_block_with_array_int(Block& block) {
    auto off_column = ColumnOffset64::create();
    auto data_column = ColumnInt32::create();
    // init column array with [[1,2,3],[],[4],[5,6]]
    std::vector<ColumnArray::Offset64> offs = {0, 3, 3, 4, 6};
    std::vector<int32_t> vals = {1, 2, 3, 4, 5, 6};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }

    auto column_array_ptr =
            ColumnArray::create(make_nullable(std::move(data_column)), std::move(off_column));
    DataTypePtr nested_type(std::make_shared<DataTypeInt32>());
    DataTypePtr array_type(std::make_shared<DataTypeArray>(nested_type));
    ColumnWithTypeAndName test_array_int(std::move(column_array_ptr), array_type, "test_array_int");
    block.insert(test_array_int);
}

static void fill_block_with_array_string(Block& block) {
    auto off_column = ColumnOffset64::create();
    auto data_column = ColumnString::create();
    // init column array with [["abc","de"],["fg"],[], [""]];
    std::vector<ColumnArray::Offset64> offs = {0, 2, 3, 3, 4};
    std::vector<std::string> vals = {"abc", "de", "fg", ""};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data(v.data(), v.size());
    }

    auto column_array_ptr =
            ColumnArray::create(make_nullable(std::move(data_column)), std::move(off_column));
    DataTypePtr nested_type(std::make_shared<DataTypeString>());
    DataTypePtr array_type(std::make_shared<DataTypeArray>(nested_type));
    ColumnWithTypeAndName test_array_string(std::move(column_array_ptr), array_type,
                                            "test_array_string");
    block.insert(test_array_string);
}

TEST(BlockSerializeTest, Array) {
    TabletSchema schema;
    TabletColumn c1;
    TabletColumn c2;
    c1.set_name("k1");
    c1.set_unique_id(1);
    c1.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    c2.set_name("k2");
    c2.set_unique_id(2);
    c2.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    schema.append_column(c1);
    schema.append_column(c2);
    // array int and array string
    Block block;
    fill_block_with_array_int(block);
    fill_block_with_array_string(block);
    MutableColumnPtr col = ColumnString::create();
    // serialize
    JsonbSerializeUtil::block_to_jsonb(schema, block, static_cast<ColumnString&>(*col.get()),
                                       block.columns(),
                                       create_data_type_serdes(block.get_data_types()), {});
    // deserialize
    TupleDescriptor read_desc(PTupleDescriptor(), true);
    // slot1
    TSlotDescriptor tslot1;
    tslot1.__set_colName("k1");
    tslot1.nullIndicatorBit = -1;
    tslot1.nullIndicatorByte = 0;
    DataTypePtr type_desc = std::make_shared<DataTypeArray>(
            DataTypeFactory::instance().create_data_type(TYPE_INT, true));
    tslot1.__set_slotType(type_desc->to_thrift());
    tslot1.__set_col_unique_id(1);
    SlotDescriptor* slot = new SlotDescriptor(tslot1);
    read_desc.add_slot(slot);

    // slot2
    TSlotDescriptor tslot2;
    tslot2.__set_colName("k2");
    tslot2.nullIndicatorBit = -1;
    tslot2.nullIndicatorByte = 0;
    DataTypePtr type_desc2 = std::make_shared<DataTypeArray>(
            DataTypeFactory::instance().create_data_type(TYPE_STRING, true));
    tslot2.__set_slotType(type_desc2->to_thrift());
    tslot2.__set_col_unique_id(2);
    SlotDescriptor* slot2 = new SlotDescriptor(tslot2);
    read_desc.add_slot(slot2);

    Block new_block = block.clone_empty();
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
    default_values.resize(read_desc.slots().size());
    for (int i = 0; i < read_desc.slots().size(); ++i) {
        col_uid_to_idx[read_desc.slots()[i]->col_unique_id()] = i;
        default_values[i] = read_desc.slots()[i]->col_default_value();
        std::cout << "uid " << read_desc.slots()[i]->col_unique_id() << ":" << i << std::endl;
    }
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    THROW_IF_ERROR(JsonbSerializeUtil::jsonb_to_block(
            create_data_type_serdes(read_desc.slots()), static_cast<ColumnString&>(*col.get()),
            col_uid_to_idx, new_block, default_values, {}));
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}

TEST(BlockSerializeTest, Map) {
    TabletSchema schema;
    TabletColumn map;
    map.set_name("m");
    map.set_unique_id(1);
    map.set_type(FieldType::OLAP_FIELD_TYPE_MAP);
    schema.append_column(map);
    // map string string
    DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr m = std::make_shared<DataTypeMap>(s, d);
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
    ColumnWithTypeAndName type_and_name(map_column->get_ptr(), m, "test_map");
    Block block;
    block.insert(type_and_name);

    MutableColumnPtr col = ColumnString::create();
    // serialize
    std::cout << "serialize to jsonb" << std::endl;
    JsonbSerializeUtil::block_to_jsonb(schema, block, static_cast<ColumnString&>(*col.get()),
                                       block.columns(),
                                       create_data_type_serdes(block.get_data_types()), {});
    // deserialize
    TupleDescriptor read_desc(PTupleDescriptor(), true);
    // slot
    TSlotDescriptor tslot;
    tslot.__set_colName("m");
    tslot.nullIndicatorBit = -1;
    tslot.nullIndicatorByte = 0;
    DataTypes sub_types;
    sub_types.reserve(2);
    sub_types.push_back(DataTypeFactory::instance().create_data_type(TYPE_STRING, true));
    sub_types.push_back(DataTypeFactory::instance().create_data_type(TYPE_INT, true));
    DataTypePtr type_desc = std::make_shared<DataTypeMap>(sub_types[0], sub_types[1]);
    tslot.__set_col_unique_id(1);
    tslot.__set_slotType(type_desc->to_thrift());
    SlotDescriptor* slot = new SlotDescriptor(tslot);
    read_desc.add_slot(slot);

    Block new_block = block.clone_empty();
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
    default_values.resize(read_desc.slots().size());
    for (int i = 0; i < read_desc.slots().size(); ++i) {
        col_uid_to_idx[read_desc.slots()[i]->col_unique_id()] = i;
        default_values[i] = read_desc.slots()[i]->col_default_value();
        std::cout << "uid " << read_desc.slots()[i]->col_unique_id() << ":" << i << std::endl;
    }
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    std::cout << "deserialize from jsonb" << std::endl;
    THROW_IF_ERROR(JsonbSerializeUtil::jsonb_to_block(
            create_data_type_serdes(read_desc.slots()), static_cast<ColumnString&>(*col.get()),
            col_uid_to_idx, new_block, default_values, {}));
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}

TEST(BlockSerializeTest, Bigstr) {
    DataTypePtr s = std::make_shared<DataTypeString>();
    MutableColumnPtr col = ColumnString::create();
    std::string bigdata;
    bigdata.resize(std::numeric_limits<int32_t>::max() - 5);
    col->insert_data(bigdata.data(), bigdata.length());
    try {
        s->get_uncompressed_serialized_bytes(*col, BeExecVersionManager::get_newest_version());
    } catch (std::exception e) {
        return;
    }
    assert(false);
}

TEST(BlockSerializeTest, Struct) {
    TabletSchema schema;
    TabletColumn struct_col;
    struct_col.set_name("struct");
    struct_col.set_unique_id(1);
    struct_col.set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    schema.append_column(struct_col);
    Block block;
    {
        DataTypePtr s = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        DataTypePtr d = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
        DataTypePtr m = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
        DataTypePtr st = std::make_shared<DataTypeStruct>(std::vector<DataTypePtr> {s, d, m});
        Struct t1, t2;
        t1.push_back(Field::create_field<TYPE_STRING>(String("amory cute")));
        t1.push_back(Field::create_field<TYPE_LARGEINT>(__int128_t(37)));
        t1.push_back(Field::create_field<TYPE_BOOLEAN>(true));
        t2.push_back(Field::create_field<TYPE_STRING>("null"));
        t2.push_back(Field::create_field<TYPE_LARGEINT>(__int128_t(26)));
        t2.push_back(Field::create_field<TYPE_BOOLEAN>(false));
        MutableColumnPtr struct_column = st->create_column();
        struct_column->reserve(2);
        struct_column->insert(Field::create_field<TYPE_STRUCT>(t1));
        struct_column->insert(Field::create_field<TYPE_STRUCT>(t2));
        ColumnWithTypeAndName type_and_name(struct_column->get_ptr(), st, "test_struct");
        block.insert(type_and_name);
    }

    MutableColumnPtr col = ColumnString::create();
    // serialize
    std::cout << "serialize to jsonb" << std::endl;
    JsonbSerializeUtil::block_to_jsonb(schema, block, static_cast<ColumnString&>(*col.get()),
                                       block.columns(),
                                       create_data_type_serdes(block.get_data_types()), {});
    // deserialize
    TupleDescriptor read_desc(PTupleDescriptor(), true);
    // slot
    TSlotDescriptor tslot;
    tslot.__set_colName("struct");
    tslot.nullIndicatorBit = -1;
    tslot.nullIndicatorByte = 0;
    DataTypes sub_types;
    Strings names;
    sub_types.reserve(3);
    names.reserve(3);
    sub_types.push_back(DataTypeFactory::instance().create_data_type(TYPE_STRING, true));
    names.push_back("name");
    sub_types.push_back(DataTypeFactory::instance().create_data_type(TYPE_LARGEINT, true));
    names.push_back("age");
    sub_types.push_back(DataTypeFactory::instance().create_data_type(TYPE_BOOLEAN, true));
    names.push_back("is");
    DataTypePtr type_desc = std::make_shared<DataTypeStruct>(sub_types, names);
    tslot.__set_col_unique_id(1);
    tslot.__set_slotType(type_desc->to_thrift());
    SlotDescriptor* slot = new SlotDescriptor(tslot);
    read_desc.add_slot(slot);

    Block new_block = block.clone_empty();
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
    default_values.resize(read_desc.slots().size());
    for (int i = 0; i < read_desc.slots().size(); ++i) {
        col_uid_to_idx[read_desc.slots()[i]->col_unique_id()] = i;
        default_values[i] = read_desc.slots()[i]->col_default_value();
        std::cout << "uid " << read_desc.slots()[i]->col_unique_id() << ":" << i << std::endl;
    }
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    std::cout << "deserialize from jsonb" << std::endl;
    THROW_IF_ERROR(JsonbSerializeUtil::jsonb_to_block(
            create_data_type_serdes(read_desc.slots()), static_cast<ColumnString&>(*col.get()),
            col_uid_to_idx, new_block, default_values, {}));
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}

TEST(BlockSerializeTest, JsonbBlock) {
    Block block;
    TabletSchema schema;
    std::vector<std::tuple<std::string, FieldType, int, PrimitiveType>> cols {
            {"k1", FieldType::OLAP_FIELD_TYPE_INT, 1, TYPE_INT},
            {"k2", FieldType::OLAP_FIELD_TYPE_STRING, 2, TYPE_STRING},
            {"k3", FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 3, TYPE_DECIMAL128I},
            {"v1", FieldType::OLAP_FIELD_TYPE_BITMAP, 7, TYPE_BITMAP},
            {"v2", FieldType::OLAP_FIELD_TYPE_HLL, 8, TYPE_HLL},
            {"k4", FieldType::OLAP_FIELD_TYPE_STRING, 4, TYPE_STRING},
            {"k5", FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 5, TYPE_DECIMAL128I},
            {"k6", FieldType::OLAP_FIELD_TYPE_INT, 6, TYPE_INT},
            {"k9", FieldType::OLAP_FIELD_TYPE_DATEV2, 9, TYPE_DATEV2}};
    for (auto t : cols) {
        TabletColumn c;
        c.set_name(std::get<0>(t));
        c.set_type(std::get<1>(t));
        c.set_unique_id(std::get<2>(t));
        schema.append_column(c);
    }
    // int
    {
        auto vec = ColumnInt32::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        DataTypePtr data_type(std::make_shared<DataTypeInt32>());
        ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_int");
        block.insert(type_and_name);
    }
    // string
    {
        auto strcol = ColumnString::create();
        for (int i = 0; i < 1024; ++i) {
            std::string is = std::to_string(i);
            strcol->insert_data(is.c_str(), is.size());
        }
        DataTypePtr data_type(std::make_shared<DataTypeString>());
        ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type, "test_string");
        block.insert(type_and_name);
    }
    // decimal
    {
        DataTypePtr decimal_data_type(doris::create_decimal(27, 9, true));
        auto decimal_column = decimal_data_type->create_column();
        auto& data = ((ColumnDecimal128V2*)decimal_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            __int128_t value = __int128_t(i * pow(10, 9) + i * pow(10, 8));
            data.push_back(value);
        }
        ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(), decimal_data_type,
                                            "test_decimal");
        block.insert(type_and_name);
    }
    // bitmap
    {
        DataTypePtr bitmap_data_type(std::make_shared<DataTypeBitMap>());
        auto bitmap_column = bitmap_data_type->create_column();
        std::vector<BitmapValue>& container = ((ColumnBitmap*)bitmap_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            BitmapValue bv;
            for (int j = 0; j <= i; ++j) {
                bv.add(j);
            }
            container.push_back(bv);
        }
        ColumnWithTypeAndName type_and_name(bitmap_column->get_ptr(), bitmap_data_type,
                                            "test_bitmap");
        block.insert(type_and_name);
    }
    // hll
    {
        DataTypePtr hll_data_type(std::make_shared<DataTypeHLL>());
        auto hll_column = hll_data_type->create_column();
        std::vector<HyperLogLog>& container = ((ColumnHLL*)hll_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            HyperLogLog hll;
            hll.update(i);
            container.push_back(hll);
        }
        ColumnWithTypeAndName type_and_name(hll_column->get_ptr(), hll_data_type, "test_hll");

        block.insert(type_and_name);
    }
    // nullable string
    {
        DataTypePtr string_data_type(std::make_shared<DataTypeString>());
        DataTypePtr nullable_data_type(std::make_shared<DataTypeNullable>(string_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((ColumnNullable*)nullable_column.get())->insert_many_defaults(1024);
        ColumnWithTypeAndName type_and_name(nullable_column->get_ptr(), nullable_data_type,
                                            "test_nullable");
        block.insert(type_and_name);
    }
    // nullable decimal
    {
        DataTypePtr decimal_data_type(doris::create_decimal(27, 9, true));
        DataTypePtr nullable_data_type(std::make_shared<DataTypeNullable>(decimal_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((ColumnNullable*)nullable_column.get())->insert_many_defaults(1024);
        ColumnWithTypeAndName type_and_name(nullable_column->get_ptr(), nullable_data_type,
                                            "test_nullable_decimal");
        block.insert(type_and_name);
    }
    // int with 1024 batch size
    {
        auto column_vector_int32 = ColumnInt32::create();
        auto column_nullable_vector = make_nullable(std::move(column_vector_int32));
        auto mutable_nullable_vector = std::move(*column_nullable_vector).mutate();
        for (int i = 0; i < 1024; i++) {
            mutable_nullable_vector->insert(Field::create_field<TYPE_INT>(i));
        }
        auto data_type = make_nullable(std::make_shared<DataTypeInt32>());
        ColumnWithTypeAndName type_and_name(mutable_nullable_vector->get_ptr(), data_type,
                                            "test_nullable_int32");
        block.insert(type_and_name);
    }
    // fill with datev2
    {
        auto column_vector_date_v2 = ColumnDateV2::create();
        auto& date_v2_data = column_vector_date_v2->get_data();
        for (int i = 0; i < 1024; ++i) {
            DateV2Value<DateV2ValueType> value;
            value.unchecked_set_time(2022, 6, 6, 0, 0, 0, 0);
            date_v2_data.push_back(*reinterpret_cast<UInt32*>(&value));
        }
        DataTypePtr date_v2_type(std::make_shared<DataTypeDateV2>());
        ColumnWithTypeAndName test_date_v2(column_vector_date_v2->get_ptr(), date_v2_type,
                                           "test_datev2");
        block.insert(test_date_v2);
    }
    MutableColumnPtr col = ColumnString::create();
    // serialize
    JsonbSerializeUtil::block_to_jsonb(schema, block, static_cast<ColumnString&>(*col.get()),
                                       block.columns(),
                                       create_data_type_serdes(block.get_data_types()), {});
    // deserialize
    TupleDescriptor read_desc(PTupleDescriptor(), true);
    for (auto t : cols) {
        TSlotDescriptor tslot;
        tslot.__set_colName(std::get<0>(t));
        if (std::get<3>(t) == TYPE_DECIMAL128I) {
            auto type_desc =
                    DataTypeFactory::instance().create_data_type(std::get<3>(t), false, 27, 9);
            tslot.__set_slotType(type_desc->to_thrift());
        } else {
            auto type_desc = DataTypeFactory::instance().create_data_type(std::get<3>(t), false);
            tslot.__set_slotType(type_desc->to_thrift());
        }
        tslot.__set_col_unique_id(std::get<2>(t));
        SlotDescriptor* slot = new SlotDescriptor(tslot);
        read_desc.add_slot(slot);
    }
    Block new_block = block.clone_empty();
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
    default_values.resize(read_desc.slots().size());
    for (int i = 0; i < read_desc.slots().size(); ++i) {
        col_uid_to_idx[read_desc.slots()[i]->col_unique_id()] = i;
    }
    THROW_IF_ERROR(
            JsonbSerializeUtil::jsonb_to_block(create_data_type_serdes(block.get_data_types()),
                                               static_cast<const ColumnString&>(*col.get()),
                                               col_uid_to_idx, new_block, default_values, {}));
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}

// Verifier (runs in every UT pass). Never calls block_to_jsonb — only jsonb_to_block.
//
// Flow:
//   1. Build an EMPTY destination block with the right schema (no values).
//   2. Read the golden file's raw JSONB bytes into a ColumnString.
//   3. Decode via jsonb_to_block into the destination.
//   4. For EACH column, inline EXPECT_EQ the decoded cells against hardcoded
//      expected values matching exactly what the generator wrote — same pattern
//      as the CHAR check (per-cell, value-by-value assertion).
//
// The expected literals MUST be kept in sync with what build_all_row_store_types_block
// writes; if you change the test data, also re-run the generator to refresh the
// golden file and update the expected literals here.
TEST(BlockSerializeTest, AllRowStoreSupportedTypes) {
    // 1) Empty destination block matching the encoder's schema.
    Block dst;
    TabletSchema schema;
    make_all_row_store_types_dst_block(dst, schema);

    // 2) Load on-disk golden bytes (NOT generated on the fly).
    const std::string golden_path = GetCurrentRunningDir() + kBlockToJsonbGoldenRel;
    auto jsonb_col = load_jsonb_column_from_file(golden_path);
    constexpr size_t kNumRows = 3;
    ASSERT_EQ(jsonb_col->size(), kNumRows) << "golden file row count unexpected";

    // 3) Decode.
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values(dst.columns());
    for (uint32_t i = 0; i < dst.columns(); ++i) {
        col_uid_to_idx[schema.columns()[i]->unique_id()] = i;
    }
    THROW_IF_ERROR(JsonbSerializeUtil::jsonb_to_block(create_data_type_serdes(dst.get_data_types()),
                                                      assert_cast<const ColumnString&>(*jsonb_col),
                                                      col_uid_to_idx, dst, default_values, {}));

    // 4) Per-column verification — same explicit pattern as the CHAR check.

    // --- ColumnVector scalar checks: compare raw native value at each row. ---
    {
        SCOPED_TRACE("c_bool");
        const auto& col = assert_cast<const ColumnUInt8&>(
                *dst.get_by_position(dst.get_position_by_name("c_bool")).column);
        const std::vector<UInt8> expected = {0, 1, 0};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_tinyint");
        const auto& col = assert_cast<const ColumnInt8&>(
                *dst.get_by_position(dst.get_position_by_name("c_tinyint")).column);
        const std::vector<int8_t> expected = {-1, 0, 127};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_smallint");
        const auto& col = assert_cast<const ColumnInt16&>(
                *dst.get_by_position(dst.get_position_by_name("c_smallint")).column);
        const std::vector<int16_t> expected = {-32000, 0, 32000};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_int");
        const auto& col = assert_cast<const ColumnInt32&>(
                *dst.get_by_position(dst.get_position_by_name("c_int")).column);
        const std::vector<int32_t> expected = {-1, 0, 1024};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_bigint");
        const auto& col = assert_cast<const ColumnInt64&>(
                *dst.get_by_position(dst.get_position_by_name("c_bigint")).column);
        const std::vector<int64_t> expected = {-1, 0, 1L << 40};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_largeint");
        const auto& col = assert_cast<const ColumnInt128&>(
                *dst.get_by_position(dst.get_position_by_name("c_largeint")).column);
        const std::vector<Int128> expected = {static_cast<Int128>(-1), static_cast<Int128>(0),
                                              static_cast<Int128>(1) << 100};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_float");
        const auto& col = assert_cast<const ColumnFloat32&>(
                *dst.get_by_position(dst.get_position_by_name("c_float")).column);
        const std::vector<float> expected = {-1.5f, 0.0f, 3.14f};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_double");
        const auto& col = assert_cast<const ColumnFloat64&>(
                *dst.get_by_position(dst.get_position_by_name("c_double")).column);
        const std::vector<double> expected = {-1.5, 0.0, 3.14};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }

    // --- date / time / IP: raw storage int representation. ---
    {
        SCOPED_TRACE("c_datev2");
        const auto& col = assert_cast<const ColumnDateV2&>(
                *dst.get_by_position(dst.get_position_by_name("c_datev2")).column);
        auto pack = [](int y, int m, int d) {
            DateV2Value<DateV2ValueType> v;
            v.unchecked_set_time(y, m, d, 0, 0, 0, 0);
            return *reinterpret_cast<UInt32*>(&v);
        };
        const std::vector<UInt32> expected = {pack(2022, 6, 6), pack(2024, 12, 31),
                                              pack(2026, 5, 20)};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_datetimev2");
        const auto& col = assert_cast<const ColumnDateTimeV2&>(
                *dst.get_by_position(dst.get_position_by_name("c_datetimev2")).column);
        auto pack = [](int y, int mo, int d, int h, int mi, int s) {
            DateV2Value<DateTimeV2ValueType> v;
            v.unchecked_set_time(y, mo, d, h, mi, s, 0);
            return *reinterpret_cast<UInt64*>(&v);
        };
        const std::vector<UInt64> expected = {pack(2022, 6, 6, 12, 0, 0),
                                              pack(2024, 12, 31, 23, 59, 59),
                                              pack(2026, 5, 20, 1, 2, 3)};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_timev2");
        const auto& col = assert_cast<const ColumnTimeV2&>(
                *dst.get_by_position(dst.get_position_by_name("c_timev2")).column);
        const std::vector<double> expected = {0.0, 1500000.0, 3600000000.0};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        // c_date: ColumnDate stores VecDateTimeValue. VecDateTimeValue::operator== compares
        // the full date+time+type tuple, so building the same expected values is the cleanest
        // cell-by-cell check.
        SCOPED_TRACE("c_date");
        const auto& col = assert_cast<const ColumnDate&>(
                *dst.get_by_position(dst.get_position_by_name("c_date")).column);
        std::vector<VecDateTimeValue> expected(3);
        expected[0].unchecked_set_time(2020, 1, 1, 0, 0, 0);
        expected[1].unchecked_set_time(2022, 6, 15, 0, 0, 0);
        expected[2].unchecked_set_time(2026, 5, 20, 0, 0, 0);
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_datetime");
        const auto& col = assert_cast<const ColumnDateTime&>(
                *dst.get_by_position(dst.get_position_by_name("c_datetime")).column);
        std::vector<VecDateTimeValue> expected(3);
        expected[0].unchecked_set_time(2020, 1, 1, 12, 30, 45);
        expected[1].unchecked_set_time(2022, 6, 15, 23, 59, 59);
        expected[2].unchecked_set_time(2026, 5, 20, 1, 2, 3);
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        // c_timestamptz: ColumnTimeStampTz stores TimestampTzValue (UInt64 underneath).
        // Compare via the raw UInt64 representation derived from the same setup.
        SCOPED_TRACE("c_timestamptz");
        const auto& col = assert_cast<const ColumnTimeStampTz&>(
                *dst.get_by_position(dst.get_position_by_name("c_timestamptz")).column);
        std::vector<TimestampTzValue> expected(3);
        expected[0].unchecked_set_time(2020, 1, 1, 12, 30, 45);
        expected[1].unchecked_set_time(2024, 12, 31, 23, 59, 59);
        expected[2].unchecked_set_time(2026, 5, 20, 0, 0, 0);
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i].to_date_int_val(), expected[i].to_date_int_val())
                    << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_ipv4");
        const auto& col = assert_cast<const ColumnIPv4&>(
                *dst.get_by_position(dst.get_position_by_name("c_ipv4")).column);
        const std::vector<IPv4> expected = {static_cast<IPv4>(0), static_cast<IPv4>(0x7f000001),
                                            static_cast<IPv4>(0xc0a80101)};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_ipv6");
        const auto& col = assert_cast<const ColumnIPv6&>(
                *dst.get_by_position(dst.get_position_by_name("c_ipv6")).column);
        const std::vector<IPv6> expected = {static_cast<IPv6>(0), static_cast<IPv6>(1),
                                            (static_cast<IPv6>(0x1234567890abcdefULL) << 64) |
                                                    static_cast<IPv6>(0xfedcba0987654321ULL)};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i], expected[i]) << "row " << i;
        }
    }

    // --- strings: byte-exact, including the CHAR no-padding invariant. ---
    {
        SCOPED_TRACE("c_string");
        const auto& col = assert_cast<const ColumnString&>(
                *dst.get_by_position(dst.get_position_by_name("c_string")).column);
        const std::vector<std::string> expected = {"hello", "", "doris row store"};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            auto ref = col.get_data_at(i);
            EXPECT_EQ(ref.size, expected[i].size()) << "row " << i;
            EXPECT_EQ(std::string(ref.data, ref.size), expected[i]) << "row " << i;
        }
    }
    {
        // VARCHAR(64) — sizes must equal originally written lengths, NOT 64.
        SCOPED_TRACE("c_varchar");
        const auto& col = assert_cast<const ColumnString&>(
                *dst.get_by_position(dst.get_position_by_name("c_varchar")).column);
        const std::vector<std::string> expected = {"vc-a", "vc-bb", ""};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            auto ref = col.get_data_at(i);
            EXPECT_EQ(ref.size, expected[i].size())
                    << "row " << i << " was padded somewhere (expected len=" << expected[i].size()
                    << ", got " << ref.size << ")";
            EXPECT_EQ(std::string(ref.data, ref.size), expected[i]) << "row " << i;
        }
    }
    {
        // CHAR(8) — same no-padding invariant.
        SCOPED_TRACE("c_char");
        const auto& col = assert_cast<const ColumnString&>(
                *dst.get_by_position(dst.get_position_by_name("c_char")).column);
        const std::vector<std::string> expected = {"ch-x", "", "ch-zzz"};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            auto ref = col.get_data_at(i);
            EXPECT_EQ(ref.size, expected[i].size())
                    << "row " << i << " was padded by block_to_jsonb / jsonb_to_block "
                    << "(expected len=" << expected[i].size() << ", got " << ref.size << ")";
            EXPECT_EQ(std::string(ref.data, ref.size), expected[i]) << "row " << i;
        }
    }
    {
        // JSONB — expected bytes are the JSONB-binary form of each original JSON literal.
        SCOPED_TRACE("c_jsonb");
        const auto& col = assert_cast<const ColumnString&>(
                *dst.get_by_position(dst.get_position_by_name("c_jsonb")).column);
        const char* payloads[] = {"{\"k\":1}", "[1,2,3]", "\"abc\""};
        ASSERT_EQ(col.size(), 3);
        for (size_t i = 0; i < 3; ++i) {
            JsonBinaryValue expected;
            THROW_IF_ERROR(expected.from_json_string(payloads[i], strlen(payloads[i])));
            auto ref = col.get_data_at(i);
            ASSERT_EQ(ref.size, expected.size()) << "row " << i;
            EXPECT_EQ(memcmp(ref.data, expected.value(), expected.size()), 0) << "row " << i;
        }
    }

    // --- decimals: compare raw integer storage. DECIMALV2's CppType is the
    // DecimalV2Value class with a value() accessor; the other decimals' CppType
    // is the Decimal<T> struct with a .value member. ---
    {
        SCOPED_TRACE("c_decimalv2");
        const auto& col = assert_cast<const ColumnDecimal128V2&>(
                *dst.get_by_position(dst.get_position_by_name("c_decimalv2")).column);
        const std::vector<__int128_t> expected = {static_cast<__int128_t>(1) * 1000000000,
                                                  static_cast<__int128_t>(2) * 1000000000,
                                                  static_cast<__int128_t>(3) * 1000000000};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i].value(), expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_decimal32");
        const auto& col = assert_cast<const ColumnDecimal32&>(
                *dst.get_by_position(dst.get_position_by_name("c_decimal32")).column);
        const std::vector<int32_t> expected = {100, 200, 300};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i].value, expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_decimal64");
        const auto& col = assert_cast<const ColumnDecimal64&>(
                *dst.get_by_position(dst.get_position_by_name("c_decimal64")).column);
        const std::vector<int64_t> expected = {10000, 20000, 30000};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i].value, expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_decimal128i");
        const auto& col = assert_cast<const ColumnDecimal128V3&>(
                *dst.get_by_position(dst.get_position_by_name("c_decimal128i")).column);
        const std::vector<__int128_t> expected = {static_cast<__int128_t>(1) * 1000000000,
                                                  static_cast<__int128_t>(2) * 1000000000,
                                                  static_cast<__int128_t>(3) * 1000000000};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i].value, expected[i]) << "row " << i;
        }
    }
    {
        SCOPED_TRACE("c_decimal256");
        const auto& col = assert_cast<const ColumnDecimal256&>(
                *dst.get_by_position(dst.get_position_by_name("c_decimal256")).column);
        const std::vector<wide::Int256> expected = {wide::Int256(1) * wide::Int256(10000000000LL),
                                                    wide::Int256(2) * wide::Int256(10000000000LL),
                                                    wide::Int256(3) * wide::Int256(10000000000LL)};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(col.get_data()[i].value, expected[i]) << "row " << i;
        }
    }

    // --- object columns: walk the underlying object per cell. ---
    {
        // BITMAP rows: {0}, {0,1}, {0,1,2}
        SCOPED_TRACE("c_bitmap");
        const auto& col = assert_cast<const ColumnBitmap&>(
                *dst.get_by_position(dst.get_position_by_name("c_bitmap")).column);
        const std::vector<std::vector<uint64_t>> expected = {{0}, {0, 1}, {0, 1, 2}};
        ASSERT_EQ(col.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            const auto& bv = col.get_data()[i];
            EXPECT_EQ(bv.cardinality(), expected[i].size()) << "row " << i;
            for (uint64_t v : expected[i]) {
                EXPECT_TRUE(bv.contains(v)) << "row " << i << " missing " << v;
            }
        }
    }
    {
        // HLL: each row was updated with a single value -> cardinality estimate is 1.
        SCOPED_TRACE("c_hll");
        const auto& col = assert_cast<const ColumnHLL&>(
                *dst.get_by_position(dst.get_position_by_name("c_hll")).column);
        ASSERT_EQ(col.size(), 3);
        for (size_t i = 0; i < 3; ++i) {
            EXPECT_EQ(col.get_data()[i].estimate_cardinality(), 1) << "row " << i;
        }
    }
    {
        // QuantileState: row i was add_value(i+1). Median over a single value == that value.
        SCOPED_TRACE("c_quantilestate");
        const auto& col = assert_cast<const ColumnQuantileState&>(
                *dst.get_by_position(dst.get_position_by_name("c_quantilestate")).column);
        ASSERT_EQ(col.size(), 3);
        for (size_t i = 0; i < 3; ++i) {
            EXPECT_EQ(col.get_data()[i].get_value_by_percentile(0.5f), static_cast<double>(i + 1))
                    << "row " << i;
        }
    }

    // --- nested columns: walk offsets / sub-columns. ---
    {
        // ARRAY<INT>: [[1,2], [], [3,4]]
        SCOPED_TRACE("c_array_int");
        const auto& arr = assert_cast<const ColumnArray&>(
                *dst.get_by_position(dst.get_position_by_name("c_array_int")).column);
        const auto& offsets = arr.get_offsets();
        const auto& nested_nullable = assert_cast<const ColumnNullable&>(arr.get_data());
        const auto& nested_int =
                assert_cast<const ColumnInt32&>(nested_nullable.get_nested_column());
        const std::vector<std::vector<int32_t>> expected = {{1, 2}, {}, {3, 4}};
        ASSERT_EQ(arr.size(), expected.size());
        size_t cursor = 0;
        for (size_t i = 0; i < expected.size(); ++i) {
            const size_t len = offsets[i] - (i == 0 ? 0 : offsets[i - 1]);
            ASSERT_EQ(len, expected[i].size()) << "row " << i << " array length";
            for (size_t k = 0; k < expected[i].size(); ++k) {
                EXPECT_FALSE(nested_nullable.is_null_at(cursor + k))
                        << "row " << i << " element " << k;
                EXPECT_EQ(nested_int.get_data()[cursor + k], expected[i][k])
                        << "row " << i << " element " << k;
            }
            cursor += len;
        }
    }
    {
        // MAP<STRING, STRING>: {"k0":"v0"}, {"k1":"v1"}, {"k2":"v2"}
        SCOPED_TRACE("c_map");
        const auto& m = assert_cast<const ColumnMap&>(
                *dst.get_by_position(dst.get_position_by_name("c_map")).column);
        const auto& offsets = m.get_offsets();
        const auto& keys_nullable = assert_cast<const ColumnNullable&>(m.get_keys());
        const auto& keys = assert_cast<const ColumnString&>(keys_nullable.get_nested_column());
        const auto& vals_nullable = assert_cast<const ColumnNullable&>(m.get_values());
        const auto& vals = assert_cast<const ColumnString&>(vals_nullable.get_nested_column());
        ASSERT_EQ(m.size(), 3);
        for (size_t i = 0; i < 3; ++i) {
            const size_t start = (i == 0) ? 0 : offsets[i - 1];
            const size_t len = offsets[i] - start;
            ASSERT_EQ(len, 1) << "row " << i << " expected single-entry map";
            EXPECT_FALSE(keys_nullable.is_null_at(start));
            EXPECT_FALSE(vals_nullable.is_null_at(start));
            auto kref = keys.get_data_at(start);
            auto vref = vals.get_data_at(start);
            EXPECT_EQ(std::string(kref.data, kref.size), "k" + std::to_string(i)) << "row " << i;
            EXPECT_EQ(std::string(vref.data, vref.size), "v" + std::to_string(i)) << "row " << i;
        }
    }
    {
        // STRUCT<int, string>: (0,"name0"), (1,"name1"), (2,"name2")
        SCOPED_TRACE("c_struct");
        const auto& st = assert_cast<const ColumnStruct&>(
                *dst.get_by_position(dst.get_position_by_name("c_struct")).column);
        ASSERT_EQ(st.tuple_size(), 2);
        const auto& f0_nullable = assert_cast<const ColumnNullable&>(st.get_column(0));
        const auto& f0 = assert_cast<const ColumnInt32&>(f0_nullable.get_nested_column());
        const auto& f1_nullable = assert_cast<const ColumnNullable&>(st.get_column(1));
        const auto& f1 = assert_cast<const ColumnString&>(f1_nullable.get_nested_column());
        ASSERT_EQ(st.size(), 3);
        for (size_t i = 0; i < 3; ++i) {
            EXPECT_FALSE(f0_nullable.is_null_at(i));
            EXPECT_FALSE(f1_nullable.is_null_at(i));
            EXPECT_EQ(f0.get_data()[i], static_cast<int32_t>(i)) << "row " << i;
            auto sref = f1.get_data_at(i);
            EXPECT_EQ(std::string(sref.data, sref.size), "name" + std::to_string(i)) << "row " << i;
        }
    }
    {
        // c_variant: round-trip semantics are intentionally lossy at the structure
        // level — DataTypeVariantSerDe::read_one_cell_from_jsonb re-inserts each cell
        // as a single path-less Variant whose root holds the encoded JSON blob/string.
        // Validate that (a) the row count matches and (b) each row serializes back to
        // a non-empty JSON string equivalent to what we originally fed in, regardless
        // of internal subcolumn layout.
        SCOPED_TRACE("c_variant");
        const auto& col = assert_cast<const ColumnVariant&>(
                *dst.get_by_position(dst.get_position_by_name("c_variant")).column);
        ASSERT_EQ(col.size(), 3);
        DataTypeSerDe::FormatOptions opts;
        const std::vector<std::string> expected = {"{\"a\":1}", "{\"b\":\"hello\"}", "[1,2,3]"};
        for (size_t i = 0; i < 3; ++i) {
            std::string out;
            col.serialize_one_row_to_string(static_cast<int64_t>(i), &out, opts);
            EXPECT_FALSE(out.empty()) << "row " << i;
            EXPECT_EQ(out, expected[i]) << "row " << i;
        }
    }

    // --- Nullable<> wrappers. ---
    {
        // c_nullable_int: null, 42, null  (insert_default for nullable produces a null)
        SCOPED_TRACE("c_nullable_int");
        const auto& col = assert_cast<const ColumnNullable&>(
                *dst.get_by_position(dst.get_position_by_name("c_nullable_int")).column);
        const auto& nested = assert_cast<const ColumnInt32&>(col.get_nested_column());
        ASSERT_EQ(col.size(), 3);
        EXPECT_TRUE(col.is_null_at(0));
        EXPECT_FALSE(col.is_null_at(1));
        EXPECT_EQ(nested.get_data()[1], 42);
        EXPECT_TRUE(col.is_null_at(2));
    }
    {
        // c_nullable_string: "a", null, "c"
        SCOPED_TRACE("c_nullable_string");
        const auto& col = assert_cast<const ColumnNullable&>(
                *dst.get_by_position(dst.get_position_by_name("c_nullable_string")).column);
        const auto& nested = assert_cast<const ColumnString&>(col.get_nested_column());
        ASSERT_EQ(col.size(), 3);
        EXPECT_FALSE(col.is_null_at(0));
        auto r0 = nested.get_data_at(0);
        EXPECT_EQ(std::string(r0.data, r0.size), "a");
        EXPECT_TRUE(col.is_null_at(1));
        EXPECT_FALSE(col.is_null_at(2));
        auto r2 = nested.get_data_at(2);
        EXPECT_EQ(std::string(r2.data, r2.size), "c");
    }
    {
        // c_nullable_bitmap: null, {7,9}, null
        SCOPED_TRACE("c_nullable_bitmap");
        const auto& col = assert_cast<const ColumnNullable&>(
                *dst.get_by_position(dst.get_position_by_name("c_nullable_bitmap")).column);
        const auto& nested = assert_cast<const ColumnBitmap&>(col.get_nested_column());
        ASSERT_EQ(col.size(), 3);
        EXPECT_TRUE(col.is_null_at(0));
        EXPECT_FALSE(col.is_null_at(1));
        EXPECT_EQ(nested.get_data()[1].cardinality(), 2);
        EXPECT_TRUE(nested.get_data()[1].contains(uint64_t {7}));
        EXPECT_TRUE(nested.get_data()[1].contains(uint64_t {9}));
        EXPECT_TRUE(col.is_null_at(2));
    }
}

// Generator (NOT part of the normal verify suite — SKIPped unless
// DORIS_REGEN_JSONB_GOLDEN=1 is set). When intentionally changing the encoder,
// run this test once to refresh the golden file, then `git add` it.
//
// This is the only test that exercises block_to_jsonb on the all-types fixture.
TEST(BlockSerializeTest, GenerateAllRowStoreSupportedTypesGolden) {
    if (std::getenv("DORIS_REGEN_JSONB_GOLDEN") == nullptr) {
        GTEST_SKIP() << "set DORIS_REGEN_JSONB_GOLDEN=1 (and DORIS_HOME) to regenerate "
                     << "be/test/util/test_data/block_to_jsonb_all_types_golden.bin";
    }
    const char* doris_home = std::getenv("DORIS_HOME");
    ASSERT_NE(doris_home, nullptr) << "DORIS_HOME must be set so the golden file can be "
                                      "written into the source tree.";

    Block block;
    TabletSchema schema;
    build_all_row_store_types_block(block, schema);

    MutableColumnPtr jsonb_col = ColumnString::create();
    JsonbSerializeUtil::block_to_jsonb(schema, block, assert_cast<ColumnString&>(*jsonb_col),
                                       block.columns(),
                                       create_data_type_serdes(block.get_data_types()), {});

    const std::string src_path = std::string(doris_home) + "/be/test/util/test_data/" +
                                 "block_to_jsonb_all_types_golden.bin";
    dump_jsonb_column_to_file(assert_cast<const ColumnString&>(*jsonb_col), src_path);
    std::cout << "[golden] wrote " << jsonb_col->size() << " row(s) to " << src_path
              << "\n[golden] commit this file, then rerun the verifier "
              << "(BlockSerializeTest.AllRowStoreSupportedTypes) to confirm." << std::endl;
}

} // namespace doris
