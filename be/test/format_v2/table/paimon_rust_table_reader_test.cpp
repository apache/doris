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

#include "format_v2/table/paimon_rust_table_reader.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "cctz/time_zone.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "format_v2/column_data.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/url_coding.h"

namespace doris::format::paimon {
namespace {

ColumnDefinition make_column(const std::string& name, const DataTypePtr& type,
                             bool is_partition_key = false) {
    ColumnDefinition column;
    column.name = name;
    column.type = type->is_nullable() ? type : make_nullable(type);
    column.is_partition_key = is_partition_key;
    return column;
}

TFileRangeDesc make_rust_range() {
    TFileRangeDesc range;
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("paimon");
    TPaimonFileDesc paimon_params;
    paimon_params.__set_reader_type(TPaimonReaderType::PAIMON_RUST);
    std::string encoded;
    base64_encode("dummy-split-bytes", &encoded);
    paimon_params.__set_paimon_split(encoded);
    table_format_params.__set_paimon_params(paimon_params);
    range.__set_table_format_params(table_format_params);
    return range;
}

TPaimonTableDesc make_table_descriptor() {
    TPaimonTableDesc descriptor;
    descriptor.__set_table_path("/paimon/warehouse/db.db/t");
    descriptor.__set_schema_json("{}");
    descriptor.__set_database_name("db");
    descriptor.__set_table_name("t");
    descriptor.__set_branch("main");
    descriptor.__set_storage_options({});
    return descriptor;
}

} // namespace

class PaimonRustTableReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _scan_params.__set_paimon_table_desc(make_table_descriptor());
        _query_options.__set_batch_size(3);
        _runtime_state = RuntimeState::create_unique(_query_options, _query_globals);
    }

    Status init_reader_with_count(PaimonRustTableReader* reader,
                                  std::vector<GlobalIndex> count_columns) {
        return reader->init({.projected_columns = {_projected_column},
                             .conjuncts = {},
                             .format = FileFormat::JNI,
                             .scan_params = &_scan_params,
                             .io_ctx = nullptr,
                             .runtime_state = _runtime_state.get(),
                             .scanner_profile = nullptr,
                             .push_down_agg_type = TPushAggOp::type::COUNT,
                             .push_down_count_columns = std::move(count_columns)});
    }

    TFileScanRangeParams _scan_params;
    TQueryOptions _query_options;
    TQueryGlobals _query_globals;
    std::unique_ptr<RuntimeState> _runtime_state;
    ColumnDefinition _projected_column = make_column("k", std::make_shared<DataTypeInt32>());
};

TEST_F(PaimonRustTableReaderTest, ValidatesRustSplit) {
    PaimonRustTableReader reader;

    // Missing paimon_split.
    auto range = make_rust_range();
    range.table_format_params.paimon_params.__isset.paimon_split = false;
    auto status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_split"), std::string::npos) << status;

    // The table is a scan-level descriptor; old per-split metadata is insufficient.
    range = make_rust_range();
    EXPECT_FALSE(reader.TEST_validate_rust_split(range).ok());
    reader.TEST_set_scan_params(&_scan_params);
    _scan_params.__isset.paimon_table_desc = false;
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing scan-level paimon_table_desc"), std::string::npos);
    _scan_params.__isset.paimon_table_desc = true;

    auto& descriptor = _scan_params.paimon_table_desc;
    // Reject missing or empty fields before crossing the FFI boundary.
    for (const auto field : {"table_path", "schema_json", "database_name", "table_name", "branch",
                             "storage_options"}) {
        descriptor = make_table_descriptor();
        const std::string name(field);
        if (name == "table_path") {
            descriptor.__isset.table_path = false;
        } else if (name == "schema_json") {
            descriptor.__set_schema_json("");
        } else if (name == "database_name") {
            descriptor.__isset.database_name = false;
        } else if (name == "table_name") {
            descriptor.__set_table_name("");
        } else if (name == "branch") {
            descriptor.__isset.branch = false;
        } else {
            descriptor.__isset.storage_options = false;
        }
        status = reader.TEST_validate_rust_split(range);
        EXPECT_FALSE(status.ok()) << name;
        EXPECT_NE(status.to_string().find("missing " + name), std::string::npos) << status;
    }
    descriptor = make_table_descriptor();

    // A mismatched reader_type is a protocol error.
    range = make_rust_range();
    range.table_format_params.paimon_params.__set_reader_type(TPaimonReaderType::PAIMON_JNI);
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("invalid reader_type"), std::string::npos) << status;

    // A complete range validates cleanly.
    EXPECT_TRUE(reader.TEST_validate_rust_split(make_rust_range()).ok());
}

TEST_F(PaimonRustTableReaderTest, AdaptiveBatchSizeBoundsWideStringReads) {
    const auto fixture = std::filesystem::path("be/test/exec/test_data/paimon_rust_batch_size");
    const auto read_file = [](const std::filesystem::path& path) {
        std::ifstream input(path, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    };
    auto split = read_file(fixture / "split.bin");
    const auto placeholder = split.find("__BUCKET__");
    ASSERT_NE(placeholder, std::string::npos);
    ASSERT_GE(placeholder, 2);
    const auto bucket = std::filesystem::canonical(fixture / "bucket-0").string();
    ASSERT_LT(bucket.size(), 65536);
    // The Java DataSplit fixture uses a neutral bucket path, encoded with writeUTF.
    const std::string length = {static_cast<char>(bucket.size() >> 8),
                                static_cast<char>(bucket.size() & 0xff)};
    split.replace(placeholder - 2, 2 + std::string("__BUCKET__").size(), length + bucket);
    std::string encoded;
    base64_encode(split, &encoded);
    SplitReadOptions options;
    options.current_range = make_rust_range();
    options.current_split_format = FileFormat::JNI;
    options.all_runtime_filters_applied = true;
    auto& params = options.current_range.table_format_params.paimon_params;
    _scan_params.paimon_table_desc.__set_table_path(std::filesystem::canonical(fixture).string());
    _scan_params.paimon_table_desc.__set_schema_json(read_file(fixture / "schema/schema-0"));
    params.__set_paimon_split(encoded);

    const auto value_column = make_column("v", std::make_shared<DataTypeString>());
    PaimonRustTableReader reader;
    ASSERT_TRUE(reader.init({.projected_columns = {_projected_column, value_column},
                             .conjuncts = {},
                             .format = FileFormat::JNI,
                             .scan_params = &_scan_params,
                             .io_ctx = nullptr,
                             .runtime_state = _runtime_state.get(),
                             .scanner_profile = nullptr})
                        .ok());
    // Doris blocks must follow the latest cap, including while the Rust stream remains open.
    for (const size_t requested : {0, 32, 7}) {
        if (requested != 0) {
            reader.set_batch_size(requested);
        }
        size_t expected = requested == 0 ? 3 : requested;
        bool resized_active_reader = false;
        auto status = reader.prepare_split(options);
        ASSERT_TRUE(status.ok()) << status;
        Block block({ColumnWithTypeAndName(_projected_column.type->create_column(),
                                           _projected_column.type, "k"),
                     ColumnWithTypeAndName(value_column.type->create_column(), value_column.type,
                                           "v")});
        size_t total = 0;
        bool eos = false;
        while (!eos) {
            status = reader.get_block(&block, &eos);
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_LE(block.rows(), expected);
            const auto& keys = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
            const auto& values =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
            for (size_t row = 0; row < block.rows(); ++row) {
                ASSERT_FALSE(keys.is_null_at(row));
                ASSERT_FALSE(values.is_null_at(row));
                EXPECT_EQ(keys.get_nested_column().get_int(row), total + row);
                EXPECT_EQ(values.get_nested_column().get_data_at(row).to_string(),
                          std::string(128 * 1024, 'x'));
            }
            total += block.rows();
            if (requested == 32 && !resized_active_reader && block.rows() > 0) {
                reader.set_batch_size(7);
                expected = 7;
                resized_active_reader = true;
            }
        }
        EXPECT_EQ(total, 65);
        if (requested == 32) {
            EXPECT_TRUE(resized_active_reader);
        }
        ASSERT_TRUE(reader.abort_split().ok());
    }
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(PaimonRustTableReaderTest, TableLevelCountEmitsSyntheticRows) {
    // COUNT(*) with a table-level row count takes the base-class metadata path:
    // prepare_split never opens the rust pipeline and get_block emits synthetic rows.
    PaimonRustTableReader reader;
    ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());

    SplitReadOptions options;
    options.current_range = make_rust_range();
    options.current_split_format = FileFormat::JNI;
    options.all_runtime_filters_applied = true;
    options.current_range.table_format_params.__set_table_level_row_count(5);
    ASSERT_TRUE(reader.prepare_split(options).ok());
    EXPECT_TRUE(reader.current_split_uses_metadata_count());

    Block block = Block({ColumnWithTypeAndName(_projected_column.type->create_column(),
                                               _projected_column.type, _projected_column.name)});
    bool eos = false;
    // The base-class count contract emits batches until a call finds remaining==0:
    // batch_size(3) splits 5 rows into 3 + 2, and only the following call reports eos.
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(block.rows(), 3);
    EXPECT_FALSE(eos);

    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(block.rows(), 2);
    EXPECT_FALSE(eos);

    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(block.rows(), 0);
    EXPECT_TRUE(eos);
}

TEST_F(PaimonRustTableReaderTest, TableLevelCountDisabledByConjuncts) {
    // A row predicate makes the metadata shortcut unsafe: the split must not report a
    // metadata count. It proceeds to the rust pipeline instead, which fails on the dummy
    // split bytes of this test range rather than emitting synthetic count rows.
    PaimonRustTableReader reader;
    ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());

    SplitReadOptions options;
    options.current_range = make_rust_range();
    options.current_split_format = FileFormat::JNI;
    options.all_runtime_filters_applied = true;
    options.current_range.table_format_params.__set_table_level_row_count(5);
    options.conjuncts = VExprContextSPtrs {};
    options.conjuncts->push_back(VExprContext::create_shared(VLiteral::create_shared(
            std::make_shared<DataTypeInt32>(), Field::create_field<TYPE_INT>(1))));

    const auto status = reader.prepare_split(options);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(reader.current_split_uses_metadata_count());

    Block block = Block({ColumnWithTypeAndName(_projected_column.type->create_column(),
                                               _projected_column.type, _projected_column.name)});
    bool eos = false;
    const auto get_block_status = reader.get_block(&block, &eos);
    EXPECT_FALSE(get_block_status.ok());
    EXPECT_NE(get_block_status.to_string().find("paimon-rust reader is not initialized"),
              std::string::npos)
            << get_block_status;
}

TEST_F(PaimonRustTableReaderTest, FailedSplitOpenIsIncludedInRustTotalTime) {
    RuntimeProfile profile("scanner");
    PaimonRustTableReader reader;
    ASSERT_TRUE(reader.init({.projected_columns = {_projected_column},
                             .conjuncts = {},
                             .format = FileFormat::JNI,
                             .scan_params = &_scan_params,
                             .io_ctx = nullptr,
                             .runtime_state = _runtime_state.get(),
                             .scanner_profile = &profile})
                        .ok());
    SplitReadOptions options;
    options.current_range = make_rust_range();
    options.current_split_format = FileFormat::JNI;
    // The invalid split fails during open, before get_block can contribute any time.
    ASSERT_FALSE(reader.prepare_split(options).ok());
    auto* total = profile.get_counter("PaimonRustReader");
    auto* open = profile.get_counter("OpenSplitTime");
    ASSERT_NE(total, nullptr);
    ASSERT_NE(open, nullptr);
    EXPECT_GT(open->value(), 0);
    EXPECT_GE(total->value(), open->value());
}

TEST_F(PaimonRustTableReaderTest, FillsPartitionConstantsForMissingArrowColumns) {
    PaimonRustTableReader reader;
    const auto data_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto partition_type = make_nullable(std::make_shared<DataTypeString>());
    // The output block matches the projected columns exactly (get_block contract):
    // position 0 is the data column k (absent from the arrow batch -> default fill),
    // position 1 is the partition key dt (materialized from split metadata).
    reader.TEST_set_projected_columns(
            {make_column("k", std::make_shared<DataTypeInt32>()),
             make_column("dt", std::make_shared<DataTypeString>(), /*is_partition_key=*/true)});
    std::map<std::string, Field> partition_values;
    partition_values.emplace("dt", Field::create_field<TYPE_STRING>("2024-01-01"));
    reader.TEST_set_partition_values(std::move(partition_values));

    Block block =
            Block({ColumnWithTypeAndName(data_type->create_column(), data_type, "k"),
                   ColumnWithTypeAndName(partition_type->create_column(), partition_type, "dt")});
    const size_t rows = 4;
    ASSERT_TRUE(reader.TEST_fill_non_arrow_columns(&block, rows).ok());

    // The data column is absent from both the arrow batch and split metadata: filled
    // with defaults.
    EXPECT_EQ(block.get_by_position(0).column->size(), rows);

    // The partition position is a constant column with the split value broadcast
    // to every row.
    const auto& column_with_type = block.get_by_position(1);
    EXPECT_EQ(column_with_type.column->size(), rows);
    const auto* const_column = check_and_get_column<ColumnConst>(*column_with_type.column);
    ASSERT_NE(const_column, nullptr);
    EXPECT_EQ(const_column->size(), rows);
    const auto value_field = const_column->get_field();
    const auto& value = value_field.get<TYPE_STRING>();
    EXPECT_EQ(std::string(value.data(), value.size()), "2024-01-01");
}

TEST_F(PaimonRustTableReaderTest, TruncatesNullablePartitionAndDefaultConstants) {
    _query_options.__set_truncate_char_or_varchar_columns(true);
    _runtime_state = RuntimeState::create_unique(_query_options, _query_globals);
    for (const auto primitive : {PrimitiveType::TYPE_CHAR, PrimitiveType::TYPE_VARCHAR}) {
        for (bool is_null : {false, true}) {
            PaimonRustTableReader reader;
            ASSERT_TRUE(init_reader_with_count(&reader, {}).ok());
            const auto type = make_nullable(std::make_shared<DataTypeString>(3, primitive));
            auto partition = make_column("partition_key", type, true);
            auto default_column = make_column("default_value", type);
            const auto value = is_null ? Field() : Field::create_field<TYPE_STRING>("abcdef");
            default_column.default_expr =
                    VExprContext::create_shared(VLiteral::create_shared(type, value));
            reader.TEST_set_projected_columns({partition, default_column});
            reader.TEST_set_partition_values({{"partition_key", value}});
            Block block({ColumnWithTypeAndName(type->create_column(), type, "partition_key"),
                         ColumnWithTypeAndName(type->create_column(), type, "default_value")});
            ASSERT_TRUE(reader.TEST_fill_non_arrow_columns(&block, 3).ok());
            ASSERT_TRUE(is_column_const(*block.get_by_position(0).column));
            // Split constants wrap nullable data; truncation must preserve both the row count
            // and null map when it materializes that outer wrapper.
            ASSERT_TRUE(reader.TEST_truncate_char_or_varchar_columns(&block).ok());
            ASSERT_EQ(block.rows(), 3);
            for (size_t column = 0; column < 2; ++column) {
                for (size_t row = 0; row < 3; ++row) {
                    Field actual;
                    block.get_by_position(column).column->get(row, actual);
                    if (is_null) {
                        EXPECT_TRUE(actual.is_null());
                    } else {
                        const auto& text = actual.get<TYPE_STRING>();
                        EXPECT_EQ(std::string(text.data(), text.size()), "abc");
                    }
                }
            }
        }
    }
}

TEST_F(PaimonRustTableReaderTest, DirectPathTruncatesBoundedStringColumns) {
    // The pinned rust reader maps paimon CHAR(n)/VARCHAR(n) to lengthless Arrow
    // Utf8, so the direct Arrow path must enforce truncate_char_or_varchar_
    // columns itself: a value written under an old wider schema and read into
    // a column narrowed to VARCHAR(3) truncates to "abc" — the semantics
    // TableReader::finalize_chunk applies on the normal path, which this
    // direct path bypasses. Unbounded STRING columns and non-string columns
    // stay untouched, and with the option off nothing truncates.
    const auto fill_block = [] {
        const auto varchar3 =
                make_nullable(std::make_shared<DataTypeString>(3, PrimitiveType::TYPE_VARCHAR));
        const auto string_type = make_nullable(std::make_shared<DataTypeString>());
        const auto int_type = std::make_shared<DataTypeInt32>();
        Block block = Block({ColumnWithTypeAndName(varchar3->create_column(), varchar3, "v"),
                             ColumnWithTypeAndName(string_type->create_column(), string_type, "s"),
                             ColumnWithTypeAndName(int_type->create_column(), int_type, "k")});
        for (auto [idx, field] : std::initializer_list<std::pair<size_t, Field>> {
                     {0, Field::create_field<TYPE_STRING>("abcdefghij")},
                     {1, Field::create_field<TYPE_STRING>("0123456789ABCDEF")},
                     {2, Field::create_field<TYPE_INT>(1)},
                     {0, Field::create_field<TYPE_STRING>("ab")},
                     {1, Field::create_field<TYPE_STRING>("xyz")},
                     {2, Field::create_field<TYPE_INT>(2)}}) {
            auto column = IColumn::mutate(block.get_by_position(idx).column);
            column->insert(field);
            block.get_by_position(idx).column = std::move(column);
        }
        return block;
    };
    const auto value_at = [](const Block& block, size_t idx, size_t row) {
        Field field;
        block.get_by_position(idx).column->get(row, field);
        const auto& value = field.get<TYPE_STRING>();
        return std::string(value.data(), value.size());
    };

    // Option off (the SetUp default): historical values return unchanged.
    {
        PaimonRustTableReader reader;
        ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());
        Block block = fill_block();
        ASSERT_TRUE(reader.TEST_truncate_char_or_varchar_columns(&block).ok());
        EXPECT_EQ(value_at(block, 0, 0), "abcdefghij");
    }

    // Option on: the VARCHAR(3) column truncates to its declared length; the
    // unbounded STRING column and the INT column stay untouched.
    _query_options.__set_truncate_char_or_varchar_columns(true);
    _runtime_state = RuntimeState::create_unique(_query_options, _query_globals);
    {
        PaimonRustTableReader reader;
        ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());
        Block block = fill_block();
        ASSERT_TRUE(reader.TEST_truncate_char_or_varchar_columns(&block).ok());
        EXPECT_EQ(value_at(block, 0, 0), "abc");
        EXPECT_EQ(value_at(block, 0, 1), "ab");
        EXPECT_EQ(value_at(block, 1, 0), "0123456789ABCDEF");
        EXPECT_EQ(value_at(block, 1, 1), "xyz");
        Field int_field;
        block.get_by_position(2).column->get(0, int_field);
        EXPECT_EQ(int_field.get<TYPE_INT>(), 1);
    }
}

TEST_F(PaimonRustTableReaderTest, MaterializesInSessionTimezone) {
    // TIMESTAMP_LTZ values materialize as session-local civil times: the
    // materialization timezone must come from the session, not a fixed default
    // (epoch 0 reads as 08:00 in a +08:00 session and 00:00 in UTC).
    const auto hour_of_epoch_zero = [](const cctz::time_zone& tz) {
        return tz.lookup(cctz::time_point<cctz::seconds>(std::chrono::seconds(0))).cs.hour();
    };

    PaimonRustTableReader reader;
    _runtime_state->set_timezone("+08:00");
    ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());
    EXPECT_EQ(hour_of_epoch_zero(reader.TEST_ctz()), 8);

    PaimonRustTableReader utc_reader;
    _runtime_state->set_timezone("UTC");
    ASSERT_TRUE(init_reader_with_count(&utc_reader, std::vector<GlobalIndex> {}).ok());
    EXPECT_EQ(hour_of_epoch_zero(utc_reader.TEST_ctz()), 0);
}

TEST_F(PaimonRustTableReaderTest, OptionLogRendersKeysOnly) {
    // Storage-option values must never reach the log: credential keys arrive
    // under many spellings and cases (AWS_SECRET_KEY, AWS_TOKEN,
    // fs.oss.accessKeySecret, s3.secret-key, ...), and a key-name blocklist
    // that misses one alias leaks the value, so the diagnostics rendering
    // prints key names only.
    const std::map<std::string, std::string> options {
            {"AWS_ACCESS_KEY", "admin"},
            {"AWS_SECRET_KEY", "leak-if-logged-1"},
            {"AWS_TOKEN", "leak-if-logged-2"},
            {"fs.oss.accessKeySecret", "leak-if-logged-3"},
            {"s3.access-key", "leak-if-logged-4"},
            {"s3.secret-key", "leak-if-logged-5"},
            {"s3.endpoint", "http://leak-if-logged-6:19001"},
    };
    const std::string rendered = PaimonRustTableReader::TEST_format_options(options);

    // Every key is rendered, no value and no '=' separator ever is.
    EXPECT_NE(rendered.find("AWS_SECRET_KEY"), std::string::npos);
    EXPECT_NE(rendered.find("fs.oss.accessKeySecret"), std::string::npos);
    EXPECT_NE(rendered.find("s3.secret-key"), std::string::npos);
    EXPECT_EQ(rendered.find('='), std::string::npos);
    for (int i = 1; i <= 6; ++i) {
        EXPECT_EQ(rendered.find("leak-if-logged-" + std::to_string(i)), std::string::npos)
                << rendered;
    }
    EXPECT_EQ(rendered.find("admin"), std::string::npos) << rendered;
}

} // namespace doris::format::paimon
