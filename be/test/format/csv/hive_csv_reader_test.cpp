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

#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <sstream>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "exec/scan/scanner.h"
#include "format/csv/csv_reader.h"
#include "format/csv/hive_csv_parser.h"
#include "format_v2/delimited_text/csv_reader.h"
#include "io/io_common.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

using OracleRow = std::array<std::optional<std::string>, 3>;
struct OracleRecord {
    std::string input;
    OracleRow expected;
};

std::string unhex(const std::string& hex) {
    if (hex == "EMPTY") {
        return "";
    }
    std::string value;
    for (size_t i = 0; i < hex.size(); i += 2) {
        value.push_back(static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16)));
    }
    return value;
}

std::map<std::string, std::vector<OracleRecord>> read_oracle() {
    // HiveCsvReaderCompatibilityTest regenerates and verifies this shared corpus with Hive 3.1.3.
    const char* root = std::getenv("ROOT");
    if (root == nullptr) {
        ADD_FAILURE() << "Set ROOT to the repository root, as run-be-ut.sh does";
        return {};
    }
    std::ifstream input(
            std::string(root) +
            "/fe/fe-connector/fe-connector-hive/src/test/resources/hive-csv-oracle.tsv");
    EXPECT_TRUE(input.is_open());
    std::map<std::string, std::vector<OracleRecord>> corpus;
    std::string line;
    while (std::getline(input, line)) {
        std::istringstream fields(line);
        std::string tuple;
        std::string raw;
        std::getline(fields, tuple, '\t');
        std::getline(fields, raw, '\t');
        OracleRecord record {.input = unhex(raw), .expected = {}};
        for (auto& value : record.expected) {
            std::string encoded;
            std::getline(fields, encoded, '\t');
            if (encoded != "NULL") {
                value = unhex(encoded);
            }
        }
        corpus[unhex(tuple)].push_back(std::move(record));
    }
    EXPECT_EQ(corpus.size(), 6);
    return corpus;
}

TEST(HiveCsvParserTest, HiveRecordOracle) {
    for (const auto& [tuple, records] : read_oracle()) {
        std::string separator = tuple.substr(0, tuple.size() - 2);
        for (size_t limit : {1, 2, 3}) {
            HiveCsvParser parser(separator, tuple[tuple.size() - 2], tuple.back(), limit);
            for (const auto& record : records) {
                std::vector<Slice> fields;
                parser.parse(Slice(record.input), &fields);
                ASSERT_LE(fields.size(), limit);
                for (size_t i = 0; i < limit; ++i) {
                    std::optional<std::string> actual;
                    if (i < fields.size()) {
                        actual = fields[i].to_string();
                    }
                    EXPECT_EQ(actual, record.expected[i]) << "field " << i;
                }
            }
        }
    }
}

TEST(HiveCsvLineReaderTest, CrLfAcrossBuffersAndQuotes) {
    HiveCsvLineReaderCtx context;
    const std::string input = "qleft\r\nrightq|tail\n";
    const auto* data = reinterpret_cast<const uint8_t*>(input.data());
    EXPECT_EQ(context.read_line(data, 6), nullptr);
    EXPECT_EQ(context.read_line(data, input.size()), data + 5);
    EXPECT_EQ(context.line_delimiter_length(), 2);
    context.refresh();
    EXPECT_EQ(context.read_line(data + 7, input.size() - 7), data + input.size() - 1);
    EXPECT_EQ(context.line_delimiter_length(), 1);
    const std::string cr = "left\rright";
    data = reinterpret_cast<const uint8_t*>(cr.data());
    EXPECT_EQ(context.read_line(data, cr.size()), data + 4);
    EXPECT_EQ(context.line_delimiter_length(), 1);
}

TFileScanRangeParams hive_csv_params(const std::string& tuple, const std::vector<int>& projection,
                                     const std::vector<SlotDescriptor*>& slots) {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    params.__set_file_type(TFileType::FILE_LOCAL);
    params.__set_compress_type(TFileCompressType::PLAIN);
    params.__set_column_idxs(projection);
    params.file_attributes.__set_hive_open_csv(true);
    params.file_attributes.__set_header_type("");
    params.file_attributes.__set_trim_double_quotes(true);
    auto& text = params.file_attributes.text_params;
    text.__set_column_separator(tuple.substr(0, tuple.size() - 2));
    text.__set_line_delimiter("\n");
    text.__set_enclose(tuple[tuple.size() - 2]);
    text.__set_escape(tuple.back());
    text.__set_null_format("");
    text.__set_empty_field_as_null(false);
    params.file_attributes.__isset.text_params = true;
    params.__isset.file_attributes = true;
    for (auto* slot : slots) {
        TFileScanSlotInfo info;
        info.__set_slot_id(slot->id());
        info.__set_is_file_slot(true);
        params.required_slots.push_back(info);
    }
    return params;
}

void check_reader_rows(CsvReader* v1, format::csv::CsvReader* v2,
                       const std::vector<SlotDescriptor*>& slots, const DataTypePtr& type,
                       const std::vector<OracleRecord>& records, const std::vector<int>& projection,
                       size_t skipped_rows, bool count) {
    size_t row_index = skipped_rows;
    bool eof = false;
    while (!eof) {
        Block block;
        for (auto* slot : slots) {
            block.insert({type->create_column(), type, slot->col_name()});
        }
        size_t rows = 0;
        auto status =
                v2 ? v2->get_block(&block, &rows, &eof) : v1->get_next_block(&block, &rows, &eof);
        ASSERT_TRUE(status.ok()) << status;
        for (size_t row = 0; row < rows; ++row, ++row_index) {
            ASSERT_LT(row_index, records.size());
            if (count) {
                continue;
            }
            for (size_t col = 0; col < projection.size(); ++col) {
                const auto& column =
                        static_cast<const ColumnNullable&>(*block.get_by_position(col).column);
                std::optional<std::string> actual;
                if (!column.is_null_at(row)) {
                    actual = column.get_nested_column().get_data_at(row).to_string();
                }
                EXPECT_EQ(actual, records[row_index].expected[projection[col]])
                        << "record " << row_index << " column " << projection[col];
            }
        }
    }
    EXPECT_EQ(row_index, records.size());
}

void check_reader_count(format::csv::CsvReader* reader, size_t expected) {
    format::FileAggregateRequest request;
    request.agg_type = TPushAggOp::COUNT;
    format::FileAggregateResult result;
    ASSERT_TRUE(reader->get_aggregate_result(request, &result).ok());
    EXPECT_EQ(result.count, expected);
}

class HiveOpenCsvReaderTest : public testing::TestWithParam<bool> {
protected:
    void SetUp() override {
        _directory = std::filesystem::temp_directory_path() /
                     (GetParam() ? "hive_csv_reader_v2" : "hive_csv_reader_v1");
        std::filesystem::create_directories(_directory);
    }
    void TearDown() override { std::filesystem::remove_all(_directory); }

    void check_file(const std::string& tuple, const std::vector<OracleRecord>& records,
                    const std::string& delimiter, const std::vector<int>& projection,
                    int64_t start_offset = 0, size_t skipped_rows = 0, bool count = false) {
        const auto path = (_directory / "records.csv").string();
        {
            std::ofstream output(path, std::ios::binary);
            for (const auto& record : records) {
                output << record.input << delimiter;
            }
        }
        MockRuntimeState state;
        // Tiny batches exercise parser-buffer lifetime and state reset between blocks.
        state._query_options.__set_batch_size(7);
        state._query_options.__set_keep_carriage_return(true);
        RuntimeProfile profile("hive_csv_reader");
        ObjectPool pool;
        DescriptorTblBuilder builder(&pool);
        auto& tuple_builder = builder.declare_tuple();
        auto type = make_nullable(std::make_shared<DataTypeString>());
        for (int index : projection) {
            tuple_builder << TupleDescBuilder::SlotType {type, "c" + std::to_string(index)};
        }
        auto slots = builder.build()->get_tuple_descriptor(0)->slots();
        auto params = hive_csv_params(tuple, projection, slots);
        ScannerCounter counter;
        TFileRangeDesc range;
        range.__set_path(path);
        range.__set_start_offset(start_offset);
        range.__set_size(std::filesystem::file_size(path) - start_offset);
        range.__set_file_size(std::filesystem::file_size(path));
        std::unique_ptr<CsvReader> v1;
        std::unique_ptr<format::csv::CsvReader> v2;
        if (GetParam()) {
            auto properties = std::make_shared<io::FileSystemProperties>();
            properties->system_type = TFileType::FILE_LOCAL;
            auto description = std::make_unique<io::FileDescription>();
            description->path = path;
            description->range_start_offset = start_offset;
            description->range_size = range.size;
            description->file_size = range.file_size;
            v2 = std::make_unique<format::csv::CsvReader>(properties, description, nullptr,
                                                          &profile, &params, slots);
            ASSERT_TRUE(v2->init(&state).ok());
            auto request = std::make_shared<format::FileScanRequest>();
            for (size_t i = 0; i < projection.size(); ++i) {
                format::LocalColumnId id(projection[i]);
                request->non_predicate_columns.push_back(format::LocalColumnIndex::top_level(id));
                request->local_positions.emplace(id, format::LocalIndex(i));
            }
            ASSERT_TRUE(v2->open(request).ok());
            if (count) {
                check_reader_count(v2.get(), records.size() - skipped_rows);
                ASSERT_TRUE(v2->close().ok());
                return;
            }
        } else {
            v1 = CsvReader::create_unique(&state, &profile, &counter, params, range, slots, 7,
                                          nullptr);
            ASSERT_TRUE(v1->init_reader(false).ok());
            if (count) {
                v1->set_push_down_agg_type(TPushAggOp::COUNT);
            }
        }
        check_reader_rows(v1.get(), v2.get(), slots, type, records, projection, skipped_rows,
                          count);
        ASSERT_TRUE((GetParam() ? v2->close() : v1->close()).ok());
    }

    std::filesystem::path _directory;
};

TEST_P(HiveOpenCsvReaderTest, HiveRecordOracleAcrossBatchesAndProjections) {
    for (const auto& [tuple, records] : read_oracle()) {
        for (const std::string delimiter : {"\n", "\r\n", "\r"}) {
            check_file(tuple, records, delimiter, {0, 1, 2});
            check_file(tuple, records, delimiter, {2, 0});
            check_file(tuple, records, delimiter, {0, 1, 2}, 0, 0, true);
        }
    }
}

TEST_P(HiveOpenCsvReaderTest, UnterminatedQuotesDoNotJoinPhysicalRecordsOrSplits) {
    std::vector<OracleRecord> records = {
            {.input = "qleft", .expected = {}},
            {.input = "rightq|tail", .expected = {}},
            {.input = "left|qunclosed", .expected = {"lft", {}, {}}},
            {.input = "abcqleft|rightq|tail", .expected = {"abcqlft|right", "tail", {}}}};
    check_file("|qe", records, "\n", {0, 1, 2});
    check_file("|qe", records, "\n", {2, 0}, 2, 1);
}

TEST_P(HiveOpenCsvReaderTest, BomIsOnlyStrippedAtFileStart) {
    std::vector<OracleRecord> records(9, {.input = "plain", .expected = {"plain", {}, {}}});
    records[0].input = "\xef\xbb\xbfplain";
    records[7] = {.input = "\xef\xbb\xbfplain", .expected = {"\xef\xbb\xbfplain", {}, {}}};
    check_file("|qe", records, "\n", {0, 1, 2});
}

INSTANTIATE_TEST_SUITE_P(BothScanners, HiveOpenCsvReaderTest, testing::Bool());

} // namespace
} // namespace doris
