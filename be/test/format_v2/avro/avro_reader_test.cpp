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

#include "format_v2/avro/avro_reader.h"

#include <gtest/gtest.h>
#include <zstd.h>

#include <avro/Allocation.hh>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <fstream>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "format_v2/column_mapper.h"
#include "format_v2/table_reader.h"
#include "io/io_common.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "storage/utils.h"
#include "testutil/scoped_temp_dir.h"
#include "util/defer_op.h"

namespace doris::format::avro {
namespace {

constexpr const char* ID_SCHEMA =
        R"({"type":"record","name":"r","fields":[{"name":"old_name","type":"long","field-id":11}]})";

class AvroReaderTest : public testing::Test {
protected:
    test::ScopedTempDirectory temp {"avro_reader"};
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile {"AvroReaderTest"};
    std::string path = (temp.path() / "data.avro").string();
    std::vector<ColumnDefinition> schema;
    std::shared_ptr<io::FileSystemProperties> properties =
            std::make_shared<io::FileSystemProperties>();

    void write_ids(::avro::Codec codec, size_t rows = 8) {
        const auto avro_schema = ::avro::compileJsonSchemaFromString(ID_SCHEMA);
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema, 32, codec);
        ::avro::GenericDatum datum(avro_schema);
        for (size_t i = 0; i < rows; ++i) {
            datum.value<::avro::GenericRecord>().fieldAt(0) = ::avro::GenericDatum(int64_t(i));
            writer.write(datum);
            // Force multiple blocks; split correctness must not depend on a single writer buffer.
            if (i % 2 == 1) writer.flush();
        }
        writer.close();
    }

    std::unique_ptr<AvroReader> reader(int64_t start = 0, int64_t size = -1,
                                       bool timestamp_tz = false, bool varbinary = false,
                                       std::optional<GlobalRowIdContext> context = std::nullopt) {
        properties->system_type = TFileType::FILE_LOCAL;
        auto file = std::make_unique<io::FileDescription>();
        file->path = path;
        file->file_size = std::filesystem::file_size(path);
        file->range_start_offset = start;
        file->range_size = size;
        return std::make_unique<AvroReader>(properties, file, std::make_shared<io::IOContext>(),
                                            &profile, timestamp_tz, varbinary, context);
    }

    Status initialize(AvroReader* input) {
        RETURN_IF_ERROR(input->init(&state));
        schema.clear();
        return input->get_schema(&schema);
    }

    Block block_for(const std::vector<int32_t>& ordinals, FileScanRequest* request) {
        Block block;
        for (size_t i = 0; i < ordinals.size(); ++i) {
            const auto id = ordinals[i];
            const auto field = id == ROW_POSITION_COLUMN_ID   ? row_position_column_definition()
                               : id == GLOBAL_ROWID_COLUMN_ID ? global_rowid_column_definition()
                                                              : schema.at(id);
            request->local_positions.emplace(LocalColumnId(id), LocalIndex(i));
            request->non_predicate_columns.push_back(LocalColumnIndex {.index = id});
            block.insert({field.type->create_column(), field.type, field.name});
        }
        return block;
    }

    std::vector<std::vector<std::string>> read_all(AvroReader* input, Block block) {
        std::vector<std::vector<std::string>> result;
        bool eof = false;
        while (!eof) {
            block.clear_column_data();
            size_t rows = 0;
            const auto status = input->get_block(&block, &rows, &eof);
            EXPECT_TRUE(status.ok()) << status;
            if (!status.ok()) break;
            for (size_t row = 0; row < rows; ++row) {
                std::vector<std::string> values;
                for (size_t i = 0; i < block.columns(); ++i) {
                    const auto& field = block.get_by_position(i);
                    values.push_back(field.type->to_string(*field.column, row));
                }
                result.push_back(std::move(values));
            }
        }
        return result;
    }
};

TEST_F(AvroReaderTest, ReadsAllSupportedCodecsAndAbsolutePositions) {
    for (const auto codec :
         {::avro::NULL_CODEC, ::avro::DEFLATE_CODEC, ::avro::SNAPPY_CODEC, ::avro::ZSTD_CODEC}) {
        write_ids(codec);
        auto input = reader();
        ASSERT_TRUE(initialize(input.get()).ok());
        ASSERT_EQ(schema.size(), 1);
        EXPECT_EQ(schema[0].get_identifier_field_id(), 11);
        EXPECT_TRUE(schema[0].type->is_nullable());
        EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_BIGINT);
        auto request = std::make_shared<FileScanRequest>();
        auto block = block_for({ROW_POSITION_COLUMN_ID, 0}, request.get());
        ASSERT_TRUE(input->open(request).ok());
        input->set_batch_size(3);
        const auto rows = read_all(input.get(), block);
        ASSERT_EQ(rows.size(), 8);
        for (size_t i = 0; i < rows.size(); ++i) {
            EXPECT_EQ(rows[i], (std::vector<std::string> {std::to_string(i), std::to_string(i)}));
        }
        EXPECT_TRUE(input->close().ok());
        EXPECT_TRUE(input->close().ok());
    }
}

TEST_F(AvroReaderTest, AdjacentByteRangesOwnEveryBlockExactlyOnce) {
    write_ids(::avro::DEFLATE_CODEC);
    const auto length = std::filesystem::file_size(path);
    // Include cuts in the header, count/size varints, payload and 16-byte sync marker.
    for (size_t cut = 1; cut < length; ++cut) {
        std::vector<std::vector<std::string>> all;
        for (const auto [start, size] :
             {std::pair<int64_t, int64_t> {0, cut}, {cut, length - cut}}) {
            auto input = reader(start, size);
            ASSERT_TRUE(initialize(input.get()).ok()) << cut;
            auto request = std::make_shared<FileScanRequest>();
            auto block = block_for({0, ROW_POSITION_COLUMN_ID}, request.get());
            const auto status = input->open(request);
            ASSERT_TRUE(status.ok()) << status << " cut=" << cut;
            auto rows = read_all(input.get(), block);
            all.insert(all.end(), rows.begin(), rows.end());
        }
        ASSERT_EQ(all.size(), 8) << cut;
        for (size_t i = 0; i < all.size(); ++i) {
            EXPECT_EQ(all[i][0], std::to_string(i)) << cut;
            EXPECT_EQ(all[i][1], std::to_string(i)) << cut;
        }
    }
}

TEST_F(AvroReaderTest, GlobalLocationsAreVisibleThroughTheColumnMapper) {
    write_ids(::avro::NULL_CODEC);
    auto input =
            reader(0, -1, false, false, GlobalRowIdContext {GlobalRowLoacationV2::VERSION, 17, 42});
    ASSERT_TRUE(initialize(input.get()).ok());
    ASSERT_EQ(schema.size(), 2);
    auto value = schema[0];
    value.name = "renamed";
    auto rowid = global_rowid_column_definition();
    rowid.name += "table_alias";
    auto mapper = input->create_column_mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper->create_mapping({value, rowid}, {}, schema).ok());
    ASSERT_EQ(mapper->mappings().size(), 2);
    EXPECT_EQ(mapper->mappings()[1].file_local_id, GLOBAL_ROWID_COLUMN_ID);
    auto request = std::make_shared<FileScanRequest>();
    ASSERT_TRUE(mapper->create_scan_request({}, {value, rowid}, request.get(), &state).ok());
    EXPECT_TRUE(request->local_positions.contains(LocalColumnId(GLOBAL_ROWID_COLUMN_ID)));
}

TEST_F(AvroReaderTest, CountCarrierPromotedToPredicateRetainsItsValue) {
    write_ids(::avro::NULL_CODEC, 2);
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    request->count_star_placeholder_columns.emplace_back(0);
    request->predicate_columns = request->non_predicate_columns;
    request->non_predicate_columns.clear();
    ASSERT_TRUE(input->open(request).ok());
    EXPECT_EQ(read_all(input.get(), block), (std::vector<std::vector<std::string>> {{"0"}, {"1"}}));
}

TEST_F(AvroReaderTest, WideRowsRespectTheBlockByteBudget) {
    const auto previous = config::enable_adaptive_batch_size;
    config::enable_adaptive_batch_size = true;
    Defer restore([previous] { config::enable_adaptive_batch_size = previous; });
    TQueryOptions options;
    options.__set_preferred_block_size_bytes(1024 * 1024);
    RuntimeState bounded_state {options, TQueryGlobals()};
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"value","type":"string"}]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
        ::avro::GenericDatum datum(avro_schema);
        datum.value<::avro::GenericRecord>().fieldAt(0).value<std::string>().assign(1024 * 1024 + 1,
                                                                                    'x');
        for (int i = 0; i < 4; ++i) writer.write(datum);
        writer.close();
    }
    auto input = reader();
    ASSERT_TRUE(input->init(&bounded_state).ok());
    ASSERT_TRUE(input->get_schema(&schema).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    input->set_batch_size(4096);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(input->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 1);
    EXPECT_FALSE(eof);
}

TEST_F(AvroReaderTest, NegativeCollectionContinuationBlocksAreDecoded) {
    const auto avro_schema =
            ::avro::compileJsonSchemaFromString(R"({"type":"record","name":"r","fields":[
      {"name":"items","type":{"type":"array","items":"long"}},
      {"name":"labels","type":{"type":"map","values":"long"}}]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
        writer.close();
    }
    std::ifstream header(path, std::ios::binary);
    const std::string data((std::istreambuf_iterator<char>(header)), {});
    header.close();
    // One record: [10,20] and {a:1,b:2}. Each collection has a positive block followed by
    // a negative-count block with an explicit byte size, as Java BlockingBinaryEncoder emits.
    const std::vector<uint8_t> payload {2, 20, 1, 2, 40, 0, 2, 2, 'a', 2, 1, 6, 2, 'b', 4, 0};
    {
        std::ofstream out(path, std::ios::binary | std::ios::app);
        out.put(2);
        out.put(payload.size() * 2);
        out.write(reinterpret_cast<const char*>(payload.data()), payload.size());
        out.write(data.data() + data.size() - 16, 16);
    }
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0, 1}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    const auto rows = read_all(input.get(), block);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0][0], "[10, 20]");
    EXPECT_EQ(rows[0][1], "{\"a\":1, \"b\":2}");
}

TEST_F(AvroReaderTest, SelectedRowsRetainAbsoluteGlobalLocations) {
    write_ids(::avro::SNAPPY_CODEC);
    auto input =
            reader(0, -1, false, false, GlobalRowIdContext {GlobalRowLoacationV2::VERSION, 17, 42});
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0, GLOBAL_ROWID_COLUMN_ID}, request.get());
    request->row_ids = std::vector<int64_t> {0, 3, 7};
    ASSERT_TRUE(input->supports_rowid_fetch());
    ASSERT_TRUE(input->open(request).ok());
    size_t rows = 0;
    bool eof = false;
    const auto status = input->get_block(&block, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(rows, 3);
    for (size_t row = 0; row < rows; ++row) {
        const auto value = block.get_by_position(1).column->get_data_at(row);
        ASSERT_EQ(value.size, sizeof(GlobalRowLoacationV2));
        GlobalRowLoacationV2 location(0, 0, 0, 0);
        std::memcpy(&location, value.data, sizeof(location));
        EXPECT_EQ(location.backend_id, 17);
        EXPECT_EQ(location.file_local.file_id, 42);
        EXPECT_EQ(location.file_local.row_id, (*request->row_ids)[row]);
        EXPECT_EQ(block.get_by_position(0).type->to_string(*block.get_by_position(0).column, row),
                  std::to_string((*request->row_ids)[row]));
    }
}

TEST_F(AvroReaderTest, CountWithoutProjectedColumnsAndEmptyContainer) {
    for (const size_t count : {0, 8}) {
        write_ids(::avro::NULL_CODEC, count);
        auto input = reader();
        ASSERT_TRUE(initialize(input.get()).ok());
        auto request = std::make_shared<FileScanRequest>();
        ASSERT_TRUE(input->open(request).ok());
        EXPECT_EQ(read_all(input.get(), Block()).size(), count);
    }
}

TEST_F(AvroReaderTest, NullableNestedValuesAndFieldIdentifiers) {
    const auto avro_schema =
            ::avro::compileJsonSchemaFromString(R"({"type":"record","name":"row","fields":[
      {"name":"number","type":["long","null"],"field-id":1},
      {"name":"nested","field-id":2,"type":["null",{"type":"record","name":"nested","fields":[
        {"name":"text","type":"string","field-id":3}]}]},
      {"name":"items","field-id":4,"type":{"type":"array","items":["null","int"],"element-id":5}},
      {"name":"labels","field-id":6,"type":{"type":"map","values":["null","string"],"key-id":7,"value-id":8}}
    ]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
        ::avro::GenericDatum datum(avro_schema);
        auto& record = datum.value<::avro::GenericRecord>();
        record.fieldAt(0).selectBranch(0);
        record.fieldAt(0).value<int64_t>() = 42;
        record.fieldAt(1).selectBranch(1);
        record.fieldAt(1).value<::avro::GenericRecord>().fieldAt(0).value<std::string>() = "value";
        auto& items = record.fieldAt(2).value<::avro::GenericArray>().value();
        // The array's schema is a union; use its actual datum shape for encoded NULLs.
        auto item_schema = avro_schema.root()->leafAt(2)->leafAt(0);
        ::avro::GenericDatum item(item_schema);
        item.selectBranch(1);
        item.value<int32_t>() = 7;
        items.push_back(item);
        item.selectBranch(0);
        items.push_back(item);
        writer.write(datum);
        record.fieldAt(0).selectBranch(1);
        record.fieldAt(1).selectBranch(0);
        items.clear();
        writer.write(datum);
        writer.close();
    }
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    EXPECT_EQ(schema[1].children[0].get_identifier_field_id(), 3);
    EXPECT_EQ(schema[2].children[0].get_identifier_field_id(), 5);
    EXPECT_EQ(schema[3].children[0].get_identifier_field_id(), 7);
    EXPECT_EQ(schema[3].children[1].get_identifier_field_id(), 8);
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({2, 0, 1, 3}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    const auto rows = read_all(input.get(), block);
    ASSERT_EQ(rows.size(), 2);
    EXPECT_EQ(rows[0][1], "42");
    EXPECT_EQ(rows[1][1], "NULL");
    EXPECT_EQ(rows[1][2], "NULL");
    EXPECT_EQ(rows[1][0], "[]");
}

TEST_F(AvroReaderTest, LogicalTypesPreserveNegativeEpochAndDecimalValues) {
    const auto avro_schema =
            ::avro::compileJsonSchemaFromString(R"({"type":"record","name":"logical","fields":[
      {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}},
      {"name":"day","type":{"type":"int","logicalType":"date"}},
      {"name":"instant","type":{"type":"long","logicalType":"timestamp-micros"}},
      {"name":"clock","type":{"type":"long","logicalType":"local-timestamp-millis"}},
      {"name":"nanos","type":{"type":"long","logicalType":"local-timestamp-nanos"}},
      {"name":"raw","type":"bytes"}
    ]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
        ::avro::GenericDatum datum(avro_schema);
        auto& record = datum.value<::avro::GenericRecord>();
        record.fieldAt(0).value<std::vector<uint8_t>>() = {0xcf, 0xc7};
        record.fieldAt(1).value<int32_t>() = -1;
        record.fieldAt(2).value<int64_t>() = -1;
        record.fieldAt(3).value<int64_t>() = -1;
        record.fieldAt(4).value<int64_t>() = -1;
        record.fieldAt(5).value<std::vector<uint8_t>>() = {0, 1, 255};
        writer.write(datum);
        writer.close();
    }
    auto input = reader(0, -1, true, true);
    ASSERT_TRUE(initialize(input.get()).ok());
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_DECIMAL64);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_precision(), 12);
    EXPECT_EQ(remove_nullable(schema[5].type)->get_primitive_type(), TYPE_VARBINARY);
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0, 1, 2, 3, 4}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    size_t size = 0;
    bool eof = false;
    ASSERT_TRUE(input->get_block(&block, &size, &eof).ok());
    std::vector<std::vector<std::string>> rows(1);
    for (size_t i = 0; i < block.columns(); ++i) {
        const auto& column = block.get_by_position(i);
        rows[0].push_back(column.type->to_string(*column.column, 0));
    }
    ASSERT_EQ(size, 1);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0][0], "-123.45");
    EXPECT_EQ(rows[0][1], "1969-12-31");
    const auto& instant = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    const auto& values = assert_cast<const ColumnTimeStampTz&>(instant.get_nested_column());
    EXPECT_EQ(values.get_element(0).to_string(cctz::utc_time_zone(), 6),
              "1969-12-31 23:59:59.999999+00:00");
    EXPECT_EQ(rows[0][3], "1969-12-31 23:59:59.999");
    EXPECT_EQ(rows[0][4], "1969-12-31 23:59:59.999999999");
}

TEST_F(AvroReaderTest, LogicalUuidsAndWideFixedDecimals) {
    const auto avro_schema =
            ::avro::compileJsonSchemaFromString(R"({"type":"record","name":"r","fields":[
      {"name":"text","type":{"type":"string","logicalType":"uuid"}},
      {"name":"binary","type":{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}},
      {"name":"amount","type":{"type":"fixed","name":"d","size":40,"logicalType":"decimal","precision":12,"scale":2}}
    ]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
        ::avro::GenericDatum datum(avro_schema);
        auto& record = datum.value<::avro::GenericRecord>();
        record.fieldAt(0).value<std::string>() = "00112233-4455-6677-8899-aabbccddeeff";
        record.fieldAt(1).value<::avro::GenericFixed>().value() = {
                0,    0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff};
        auto& decimal = record.fieldAt(2).value<::avro::GenericFixed>().value();
        decimal.assign(40, 255);
        decimal[38] = 0xcf;
        decimal[39] = 0xc7;
        writer.write(datum);
        writer.close();
    }
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0, 1, 2}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    const auto rows = read_all(input.get(), block);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0],
              (std::vector<std::string> {"00112233-4455-6677-8899-aabbccddeeff",
                                         "00112233-4455-6677-8899-aabbccddeeff", "-123.45"}));
}

TEST_F(AvroReaderTest, IcebergLogicalMapPreservesNonStringKeysAndNullValues) {
    // Register logicalType=map through the production reader before compiling the writer schema.
    write_ids(::avro::NULL_CODEC);
    auto registration = reader();
    ASSERT_TRUE(initialize(registration.get()).ok());
    ASSERT_TRUE(registration->close().ok());
    const auto avro_schema =
            ::avro::compileJsonSchemaFromString(R"({"type":"record","name":"r","fields":[
      {"name":"entries","field-id":1,"type":{"type":"array","logicalType":"map","items":{
        "type":"record","name":"entry","fields":[{"name":"key","type":"long","field-id":2},
          {"name":"value","type":["null","string"],"field-id":3}]}}}]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
        ::avro::GenericDatum datum(avro_schema);
        auto& entries = datum.value<::avro::GenericRecord>()
                                .fieldAt(0)
                                .value<::avro::GenericArray>()
                                .value();
        ::avro::GenericDatum entry(avro_schema.root()->leafAt(0)->leafAt(0));
        auto& record = entry.value<::avro::GenericRecord>();
        record.fieldAt(0).value<int64_t>() = 42;
        record.fieldAt(1).selectBranch(1);
        record.fieldAt(1).value<std::string>() = "value";
        entries.push_back(entry);
        record.fieldAt(0).value<int64_t>() = 7;
        record.fieldAt(1).selectBranch(0);
        entries.push_back(entry);
        writer.write(datum);
        writer.close();
    }
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    EXPECT_EQ(schema[0].children[0].get_identifier_field_id(), 2);
    EXPECT_EQ(schema[0].children[1].get_identifier_field_id(), 3);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_MAP);
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    EXPECT_EQ(read_all(input.get(), block),
              (std::vector<std::vector<std::string>> {{"{42:\"value\", 7:null}"}}));
}

TEST_F(AvroReaderTest, RejectsNegativeBlockCountsAndLengths) {
    write_ids(::avro::NULL_CODEC);
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    ASSERT_TRUE(input->close().ok());
    ::avro::DataFileReaderBase header(path.c_str());
    const auto offset = header.previousSync();
    header.close();
    std::ifstream file(path, std::ios::binary);
    const std::string data((std::istreambuf_iterator<char>(file)), {});
    file.close();
    // Both first-block headers are one-byte varints; zig-zag 1 represents -1.
    for (int byte = 0; byte < 2; ++byte) {
        auto corrupt = data;
        corrupt[offset + byte] = 1;
        {
            std::ofstream out(path, std::ios::binary);
            out.write(corrupt.data(), corrupt.size());
        }
        input = reader();
        ASSERT_TRUE(initialize(input.get()).ok());
        auto request = std::make_shared<FileScanRequest>();
        auto block = block_for({0}, request.get());
        const auto status = input->open(request);
        EXPECT_FALSE(status.ok()) << status;
        EXPECT_TRUE(input->close().ok());
    }
}

TEST_F(AvroReaderTest, NamedTypesAreReusableWithoutBeingRecursive) {
    const auto avro_schema =
            ::avro::compileJsonSchemaFromString(R"({"type":"record","name":"row","fields":[
      {"name":"a","type":{"type":"fixed","name":"id","size":4}},
      {"name":"b","type":"id"}]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
        ::avro::GenericDatum datum(avro_schema);
        datum.value<::avro::GenericRecord>().fieldAt(0).value<::avro::GenericFixed>().value() = {
                'a', 'b', 'c', 'd'};
        datum.value<::avro::GenericRecord>().fieldAt(1).value<::avro::GenericFixed>().value() = {
                'e', 'f', 'g', 'h'};
        writer.write(datum);
        writer.close();
    }
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({1, 0}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    const auto rows = read_all(input.get(), block);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0], (std::vector<std::string> {"efgh", "abcd"}));
}

TEST_F(AvroReaderTest, RejectsUnsupportedSchemasWithoutConstructingRecursiveValues) {
    for (const char* json :
         {R"({"type":"record","name":"r","fields":[{"name":"v","type":["int","string"]}]})",
          R"({"type":"record","name":"r","fields":[{"name":"v","type":["null","r"]}]})"}) {
        {
            ::avro::DataFileWriter<::avro::GenericDatum> writer(
                    path.c_str(), ::avro::compileJsonSchemaFromString(json));
            writer.close();
        }
        auto input = reader();
        const auto status = initialize(input.get());
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("unsupported"), std::string::npos) << status;
    }
}

TEST_F(AvroReaderTest, ZstandardFramesMayOmitTheContentSize) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(ID_SCHEMA);
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema, 32,
                                                            ::avro::ZSTD_CODEC);
        writer.close();
    }
    ::avro::DataFileReaderBase header(path.c_str());
    const auto header_size = header.previousSync();
    header.close();
    std::ifstream input_file(path, std::ios::binary);
    const std::string original((std::istreambuf_iterator<char>(input_file)), {});
    input_file.close();
    const char datum = 14;
    std::vector<char> compressed(ZSTD_compressBound(1));
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context(ZSTD_createCCtx(), &ZSTD_freeCCtx);
    ASSERT_FALSE(ZSTD_isError(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_contentSizeFlag, 0)));
    const auto length =
            ZSTD_compress2(context.get(), compressed.data(), compressed.size(), &datum, 1);
    ASSERT_FALSE(ZSTD_isError(length));
    ASSERT_EQ(ZSTD_getFrameContentSize(compressed.data(), length), ZSTD_CONTENTSIZE_UNKNOWN);
    ASSERT_LT(length, 64);
    {
        std::ofstream out(path, std::ios::binary);
        out.write(original.data(), header_size);
        out.put(2);
        out.put(static_cast<char>(length * 2));
        out.write(compressed.data(), length);
        out.write(original.data() + header_size - 16, 16);
    }
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    const auto status = input->open(request);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(read_all(input.get(), block), (std::vector<std::vector<std::string>> {{"7"}}));
}

TEST_F(AvroReaderTest, HugeMetadataMapCountsFailBeforeAllocatingNodes) {
    {
        std::ofstream out(path, std::ios::binary);
        const std::string header("Obj\1\x80\x80\x80\x80\x08", 9);
        out.write(header.data(), header.size());
    }
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "avro_metadata_limit", 8 * 1024 * 1024);
    SCOPED_ATTACH_TASK(tracker);
    auto input = reader();
    const auto status = initialize(input.get());
    EXPECT_EQ(status.code(), ErrorCode::QUERY_MEMORY_EXCEEDED) << status;
    EXPECT_EQ(::avro::allocationHook.check, nullptr);
    EXPECT_TRUE(input->close().ok());
}

TEST_F(AvroReaderTest, HugeArrayCountsFailTheBudgetBeforeAllocatingElements) {
    const auto avro_schema =
            ::avro::compileJsonSchemaFromString(R"({"type":"record","name":"r","fields":[
      {"name":"items","type":{"type":"array","items":"long"}}]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
        writer.close();
    }
    std::ifstream header(path, std::ios::binary);
    const std::string data((std::istreambuf_iterator<char>(header)), {});
    header.close();
    // OCF framing is valid; the tiny payload advertises 2^30 array elements without storing them.
    const std::string payload("\x80\x80\x80\x80\x08\0", 6);
    {
        std::ofstream out(path, std::ios::binary | std::ios::app);
        out.put(2);
        out.put(12);
        out.write(payload.data(), payload.size());
        out.write(data.data() + data.size() - 16, 16);
    }
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "avro_huge_array", 8 * 1024 * 1024);
    SCOPED_ATTACH_TASK(tracker);
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    size_t rows = 0;
    bool eof = false;
    const auto status = input->get_block(&block, &rows, &eof);
    EXPECT_EQ(status.code(), ErrorCode::QUERY_MEMORY_EXCEEDED) << status;
    EXPECT_EQ(::avro::allocationHook.check, nullptr);
    EXPECT_TRUE(input->close().ok());
}

TEST_F(AvroReaderTest, FixedAndCompressedBlocksReserveBeforeMaterializing) {
    for (bool compressed : {false, true}) {
        const auto avro_schema = ::avro::compileJsonSchemaFromString(
                compressed
                        ? R"({"type":"record","name":"r","fields":[{"name":"value","type":"string"}]})"
                        : R"({"type":"record","name":"r","fields":[{"name":"value","type":{"type":"fixed","name":"large","size":134217728}}]})");
        {
            ::avro::DataFileWriter<::avro::GenericDatum> writer(
                    path.c_str(), avro_schema, 32,
                    compressed ? ::avro::ZSTD_CODEC : ::avro::NULL_CODEC);
            if (compressed) {
                ::avro::GenericDatum datum(avro_schema);
                datum.value<::avro::GenericRecord>().fieldAt(0).value<std::string>().assign(
                        16 * 1024 * 1024, 'x');
                writer.write(datum);
            }
            writer.close();
        }
        if (!compressed) {
            std::ifstream header(path, std::ios::binary);
            const std::string bytes((std::istreambuf_iterator<char>(header)), {});
            header.close();
            std::ofstream out(path, std::ios::binary | std::ios::app);
            out.put(2);
            out.put(2);
            out.put(0);
            out.write(bytes.data() + bytes.size() - 16, 16);
        }
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                        "avro_scratch_limit", 8 * 1024 * 1024);
        SCOPED_ATTACH_TASK(tracker);
        auto input = reader();
        ASSERT_TRUE(initialize(input.get()).ok());
        auto request = std::make_shared<FileScanRequest>();
        auto block = block_for({0}, request.get());
        const auto status = input->open(request);
        EXPECT_EQ(status.code(), ErrorCode::QUERY_MEMORY_EXCEEDED) << status;
        EXPECT_EQ(::avro::allocationHook.check, nullptr);
        EXPECT_TRUE(input->close().ok());
    }
}

TEST_F(AvroReaderTest, RangesNeverDecompressUnownedLargeBlocks) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"value","type":"string"}]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema, 32,
                                                            ::avro::ZSTD_CODEC);
        ::avro::GenericDatum datum(avro_schema);
        auto& value = datum.value<::avro::GenericRecord>().fieldAt(0).value<std::string>();
        value = "x";
        writer.write(datum);
        writer.flush();
        value.assign(16 * 1024 * 1024, 'y');
        writer.write(datum);
        writer.flush();
        value = "z";
        writer.write(datum);
        writer.close();
    }
    int64_t second = 0, third = 0;
    {
        ::avro::DataFileReaderBase oracle(path.c_str());
        oracle.init();
        ::avro::GenericDatum datum(avro_schema);
        ::avro::GenericReader::read(oracle.decoder(), datum);
        oracle.decr();
        ASSERT_TRUE(oracle.hasMore());
        second = oracle.previousSync();
        ::avro::GenericReader::read(oracle.decoder(), datum);
        oracle.decr();
        ASSERT_TRUE(oracle.hasMore());
        third = oracle.previousSync();
        oracle.close();
    }
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "avro_range_scratch", 8 * 1024 * 1024);
    SCOPED_ATTACH_TASK(tracker);
    for (bool prefix : {true, false}) {
        auto input = prefix ? reader(0, second) : reader(third, -1);
        ASSERT_TRUE(initialize(input.get()).ok());
        auto request = std::make_shared<FileScanRequest>();
        auto block = block_for({ROW_POSITION_COLUMN_ID, 0}, request.get());
        const auto status = input->open(request);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(
                read_all(input.get(), block),
                (std::vector<std::vector<std::string>> {{prefix ? "0" : "2", prefix ? "x" : "z"}}));
        EXPECT_TRUE(input->close().ok());
    }
}

TEST_F(AvroReaderTest, RetainedScratchRemainsChargedBetweenBatchesAndIsReleasedOnClose) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"value","type":"string"}]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema, 32,
                                                            ::avro::SNAPPY_CODEC);
        ::avro::GenericDatum datum(avro_schema);
        datum.value<::avro::GenericRecord>().fieldAt(0).value<std::string>().assign(128 * 1024,
                                                                                    'x');
        writer.write(datum);
        writer.write(datum);
        writer.close();
    }
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "avro_retained_scratch", 8 * 1024 * 1024);
    SCOPED_ATTACH_TASK(tracker);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const auto baseline = tracker->consumption();
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    input->set_batch_size(1);
    for (int batch = 0; batch < 2; ++batch) {
        block.clear_column_data();
        size_t rows = 0;
        bool eof = false;
        ASSERT_TRUE(input->get_block(&block, &rows, &eof).ok());
        EXPECT_EQ(rows, 1);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_GT(tracker->consumption(), baseline + 256 * 1024);
    }
    block = Block();
    EXPECT_TRUE(input->close().ok());
    input.reset();
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_EQ(tracker->consumption(), baseline);
}

TEST_F(AvroReaderTest, NestedNullableArraysHaveLinearAccountingAndChargeUnionStorage) {
    constexpr size_t count = 4096;
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"value","type":{"type":"array","items":["null",{"type":"array","items":"long"}]}}]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
        ::avro::GenericDatum datum(avro_schema);
        auto& values = datum.value<::avro::GenericRecord>()
                               .fieldAt(0)
                               .value<::avro::GenericArray>()
                               .value();
        const auto element = avro_schema.root()->leafAt(0)->leafAt(0);
        for (size_t i = 0; i < count; ++i) {
            ::avro::GenericDatum value(element);
            value.selectBranch(1);
            values.push_back(std::move(value));
        }
        writer.write(datum);
        writer.write(datum);
        writer.close();
    }
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "avro_nested_arrays", 16 * 1024 * 1024);
    SCOPED_ATTACH_TASK(tracker);
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    input->set_batch_size(1);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const auto before = tracker->consumption();
    for (size_t i = 0; i < 2; ++i) {
        size_t rows = 0;
        bool eof = false;
        ASSERT_TRUE(input->get_block(&block, &rows, &eof).ok());
        EXPECT_EQ(rows, 1);
        EXPECT_EQ(block.get_by_position(0).column->size(), 1);
        block.clear_column_data();
    }
    block = Block();
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    // Each decoded element owns its union and array containers in addition to its vector slot.
    const auto minimum = count * (sizeof(::avro::GenericDatum) + sizeof(::avro::GenericUnion) +
                                  sizeof(::avro::GenericArray));
    EXPECT_GE(tracker->consumption() - before, minimum);
    const auto* passes = profile.get_counter("AvroScratchAccountingPasses");
    ASSERT_NE(passes, nullptr);
    EXPECT_LE(passes->value(), 6);
    EXPECT_TRUE(input->close().ok());
}

TEST_F(AvroReaderTest, RejectsTruncatedHeadersAndBlocks) {
    write_ids(::avro::SNAPPY_CODEC);
    std::ifstream input_file(path, std::ios::binary);
    const std::string data((std::istreambuf_iterator<char>(input_file)), {});
    for (const size_t size : {size_t(0), size_t(3), size_t(16), data.size() - 1}) {
        {
            std::ofstream out(path, std::ios::binary);
            out.write(data.data(), size);
        }
        auto input = reader();
        auto status = initialize(input.get());
        if (status.ok()) {
            auto request = std::make_shared<FileScanRequest>();
            auto block = block_for({0}, request.get());
            status = input->open(request);
            bool eof = false;
            while (status.ok() && !eof) {
                block.clear_column_data();
                size_t rows = 0;
                status = input->get_block(&block, &rows, &eof);
            }
        }
        EXPECT_FALSE(status.ok()) << size;
    }
}

} // namespace
} // namespace doris::format::avro
