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

#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <fstream>
#include <tuple>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "format_v2/column_mapper.h"
#include "format_v2/table/hive_reader.h"
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
    RuntimeState state {TQueryOptions(), [] {
                            TQueryGlobals globals;
                            globals.__set_time_zone("UTC");
                            return globals;
                        }()};
    RuntimeProfile profile {"AvroReaderTest"};
    io::FileReaderStats io_stats;
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
            if (i % 2 == 1) {
                writer.flush();
            }
        }
        writer.close();
    }

    void write_raw_block(const ::avro::ValidSchema& avro_schema, ::avro::Codec codec,
                         const std::vector<uint8_t>& payload, int64_t count = 1) {
        {
            ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema, 32,
                                                                codec);
            writer.close();
        }
        ::avro::DataFileReaderBase header(path.c_str());
        const auto header_size = header.previousSync();
        header.close();
        std::ifstream in(path, std::ios::binary);
        const std::string original((std::istreambuf_iterator<char>(in)), {});
        in.close();
        std::ofstream out(path, std::ios::binary);
        out.write(original.data(), header_size);
        auto write_long = [&](uint64_t value) {
            value *= 2;
            while (value >= 128) {
                out.put(static_cast<char>((value & 127) | 128));
                value >>= 7;
            }
            out.put(static_cast<char>(value));
        };
        write_long(count);
        write_long(payload.size());
        out.write(reinterpret_cast<const char*>(payload.data()), payload.size());
        out.write(original.data() + header_size - 16, 16);
    }

    void write_header_json(const std::string& json,
                           const std::optional<std::vector<uint8_t>>& payload = std::nullopt) {
        std::ofstream out(path, std::ios::binary);
        out.write("Obj\x01", 4);
        auto write_long = [&](uint64_t value) {
            value *= 2;
            while (value >= 128) {
                out.put(static_cast<char>((value & 127) | 128));
                value >>= 7;
            }
            out.put(static_cast<char>(value));
        };
        write_long(1);
        const std::string key = "avro.schema";
        write_long(key.size());
        out.write(key.data(), key.size());
        write_long(json.size());
        out.write(json.data(), json.size());
        write_long(0);
        out.write("0123456789abcdef", 16);
        if (payload) {
            write_long(1);
            write_long(payload->size());
            out.write(reinterpret_cast<const char*>(payload->data()), payload->size());
            out.write("0123456789abcdef", 16);
        }
    }

    std::unique_ptr<AvroReader> reader(int64_t start = 0, int64_t size = -1,
                                       bool timestamp_tz = false, bool varbinary = false,
                                       std::optional<GlobalRowIdContext> context = std::nullopt,
                                       bool hive_compatible = false) {
        properties->system_type = TFileType::FILE_LOCAL;
        auto file = std::make_unique<io::FileDescription>();
        file->path = path;
        file->file_size = std::filesystem::file_size(path);
        file->range_start_offset = start;
        file->range_size = size;
        auto io_context = std::make_shared<io::IOContext>();
        io_context->file_reader_stats = &io_stats;
        return std::make_unique<AvroReader>(properties, file, io_context, &profile, timestamp_tz,
                                            varbinary, context, hive_compatible);
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
            if (!status.ok()) {
                break;
            }
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

TEST_F(AvroReaderTest, SplitAlignmentRejectsSyncMarkersInsidePayloads) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"id","type":"long"},
                {"name":"payload","type":"bytes"}]})");
    const std::string placeholder(16, '!');
    for (const auto& suffix : {std::string(24, '\x80'), std::string("\x01\x00", 2),
                               std::string("\x02\x04xx", 4) + std::string(16, '?'),
                               std::string("\x02\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01", 11),
                               std::string("\x02\x04\x54\x00", 4) + placeholder}) {
        SCOPED_TRACE(suffix);
        const std::string payload = "prefix" + placeholder + suffix;
        {
            ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
            for (int64_t id = 0; id < 2; ++id) {
                ::avro::GenericDatum datum(avro_schema);
                auto& record = datum.value<::avro::GenericRecord>();
                record.fieldAt(0).value<int64_t>() = id;
                record.fieldAt(1).value<std::vector<uint8_t>>() =
                        id == 0 ? std::vector<uint8_t>(payload.begin(), payload.end())
                                : std::vector<uint8_t> {'e', 'n', 'd'};
                writer.write(datum);
                writer.flush();
            }
            writer.close();
        }
        ::avro::DataFileReaderBase header(path.c_str());
        const auto header_end = header.previousSync();
        header.close();
        std::ifstream in(path, std::ios::binary);
        std::string data((std::istreambuf_iterator<char>(in)), {});
        in.close();
        const auto embedded = data.find(placeholder, header_end);
        ASSERT_NE(embedded, std::string::npos);
        const auto sync = data.substr(header_end - 16, 16);
        // The last fixture contains a fully framed fake block, including its closing sync.
        for (size_t match = embedded; match != std::string::npos;
             match = data.find(placeholder, match + placeholder.size())) {
            data.replace(match, placeholder.size(), sync);
        }
        {
            std::ofstream out(path, std::ios::binary);
            out.write(data.data(), data.size());
        }
        // Cut before, within and after a payload marker; both ranges must still own only
        // complete OCF blocks, including when no absolute row-position column is requested.
        for (size_t cut = embedded; cut <= embedded + 17; ++cut) {
            for (const bool coordinates : {false, true}) {
                std::vector<std::vector<std::string>> all;
                for (const auto [start, size] :
                     {std::pair<int64_t, int64_t> {0, cut}, {cut, data.size() - cut}}) {
                    auto input = reader(start, size);
                    ASSERT_TRUE(initialize(input.get()).ok());
                    auto request = std::make_shared<FileScanRequest>();
                    auto block =
                            block_for(coordinates ? std::vector<int32_t> {0, ROW_POSITION_COLUMN_ID}
                                                  : std::vector<int32_t> {0},
                                      request.get());
                    const auto status = input->open(request);
                    ASSERT_TRUE(status.ok()) << status << " cut=" << cut;
                    auto rows = read_all(input.get(), block);
                    all.insert(all.end(), rows.begin(), rows.end());
                }
                EXPECT_EQ(all, coordinates ? (std::vector<std::vector<std::string>> {{"0", "0"},
                                                                                     {"1", "1"}})
                                           : (std::vector<std::vector<std::string>> {{"0"}, {"1"}}))
                        << "cut=" << cut;
            }
        }
    }
}

TEST_F(AvroReaderTest, RejectsCaseCollidingFieldsAtEveryRecordLevel) {
    const std::string record =
            R"({"type":"record","name":"collision","fields":[{"name":"Foo","type":"int"},
                {"name":"foo","type":"int"}]})";
    for (const auto& value : {record, std::string("{\"type\":\"array\",\"items\":") + record + "}",
                              std::string("{\"type\":\"map\",\"values\":") + record + "}",
                              std::string("[\"null\",") + record + "]"}) {
        for (const bool nested : {false, true}) {
            const auto json = nested ? "{\"type\":\"record\",\"name\":\"outer\",\"fields\":[{"
                                       "\"name\":\"value\",\"type\":" +
                                               value + "}]}"
                                     : record;
            const auto avro_schema = ::avro::compileJsonSchemaFromString(json);
            {
                ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
                writer.close();
            }
            auto input = reader();
            const auto status = initialize(input.get());
            EXPECT_FALSE(status.ok()) << json;
            EXPECT_NE(status.to_string().find("ambiguous"), std::string::npos) << status;
        }
    }
}

TEST_F(AvroReaderTest, GlobalLocationsAreVisibleThroughTheColumnMapper) {
    write_ids(::avro::NULL_CODEC);
    auto input = reader(
            0, -1, false, false,
            GlobalRowIdContext {
                    .version = GlobalRowLoacationV2::VERSION, .backend_id = 17, .file_id = 42});
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
        for (int i = 0; i < 4; ++i) {
            writer.write(datum);
        }
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
    auto input = reader(
            0, -1, false, false,
            GlobalRowIdContext {
                    .version = GlobalRowLoacationV2::VERSION, .backend_id = 17, .file_id = 42});
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
    for (const auto codec :
         {::avro::NULL_CODEC, ::avro::DEFLATE_CODEC, ::avro::SNAPPY_CODEC, ::avro::ZSTD_CODEC}) {
        for (const size_t count : {0, 8}) {
            write_ids(codec, count);
            auto input = reader();
            ASSERT_TRUE(initialize(input.get()).ok());
            auto request = std::make_shared<FileScanRequest>();
            ASSERT_TRUE(input->open(request).ok());
            EXPECT_EQ(read_all(input.get(), Block()).size(), count);
        }
    }
}

TEST_F(AvroReaderTest, NegativeMetadataBlocksEnforceExactBounds) {
    const std::string json =
            R"({"type":"record","name":"r","fields":[{"name":"v","type":"long"}]})";
    write_header_json(json);
    std::ifstream input_file(path, std::ios::binary);
    const std::string original((std::istreambuf_iterator<char>(input_file)), {});
    input_file.close();
    const std::string entry = original.substr(5, original.size() - 5 - 17);
    const std::string tail = original.substr(original.size() - 17);
    auto encode = [](uint64_t value) {
        std::string bytes;
        value *= 2;
        while (value >= 128) {
            bytes.push_back(static_cast<char>((value & 127) | 128));
            value >>= 7;
        }
        bytes.push_back(static_cast<char>(value));
        return bytes;
    };
    for (const int delta : {-1, 0, 1}) {
        SCOPED_TRACE(delta);
        // A negative block can be followed by another positive metadata block.
        const std::string bytes = "Obj\x01" + std::string(1, '\x01') +
                                  encode(entry.size() + delta) + entry + encode(1) + encode(10) +
                                  "avro.codec" + encode(4) + "null" + tail;
        std::ofstream output(path, std::ios::binary);
        output.write(bytes.data(), bytes.size());
        output.close();
        auto input = reader();
        const auto status = initialize(input.get());
        EXPECT_EQ(status.ok(), delta == 0) << status;
        EXPECT_TRUE(input->close().ok());
    }
}

TEST_F(AvroReaderTest, EmptyRootRecordsKeepZeroByteRowsAndSplitPositions) {
    const auto empty = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"empty_root","fields":[]})");
    for (auto codec :
         {::avro::NULL_CODEC, ::avro::DEFLATE_CODEC, ::avro::SNAPPY_CODEC, ::avro::ZSTD_CODEC}) {
        for (size_t count : {size_t(0), size_t(7)}) {
            SCOPED_TRACE(std::to_string(codec) + " count=" + std::to_string(count));
            {
                ::avro::DataFileWriter<::avro::GenericDatum> output(path.c_str(), empty, 32, codec);
                ::avro::GenericDatum datum(empty);
                for (size_t i = 0; i < count; ++i) {
                    output.write(datum);
                    if (i % 2 == 1) {
                        output.flush();
                    }
                }
                output.close();
            }
            auto input = reader();
            ASSERT_TRUE(initialize(input.get()).ok());
            EXPECT_TRUE(schema.empty());
            input->set_batch_size(2);
            ASSERT_TRUE(input->open(std::make_shared<FileScanRequest>()).ok());
            EXPECT_EQ(read_all(input.get(), Block()).size(), count);
            EXPECT_TRUE(input->close().ok());
            const int64_t bytes = std::filesystem::file_size(path);
            for (int64_t cut : {int64_t(0), int64_t(4), bytes / 2, bytes - 1, bytes}) {
                std::vector<std::vector<std::string>> combined;
                for (auto [offset, size] :
                     {std::pair<int64_t, int64_t> {0, cut}, {cut, bytes - cut}}) {
                    auto split = reader(offset, size);
                    ASSERT_TRUE(initialize(split.get()).ok());
                    auto request = std::make_shared<FileScanRequest>();
                    auto block = block_for({ROW_POSITION_COLUMN_ID}, request.get());
                    split->set_batch_size(2);
                    ASSERT_TRUE(split->open(request).ok());
                    const auto rows = read_all(split.get(), block);
                    combined.insert(combined.end(), rows.begin(), rows.end());
                }
                ASSERT_EQ(combined.size(), count);
                for (size_t i = 0; i < count; ++i) {
                    EXPECT_EQ(combined[i][0], std::to_string(i));
                }
            }
        }
    }
}

TEST_F(AvroReaderTest, EmptyRootRecordsKeepTableCountRowsThroughANullableCarrier) {
    const auto empty = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"empty_root","fields":[]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> output(path.c_str(), empty);
        ::avro::GenericDatum datum(empty);
        for (int i = 0; i < 5; ++i) {
            output.write(datum);
        }
        output.close();
    }
    TFileScanRangeParams params;
    params.__set_file_type(TFileType::FILE_LOCAL);
    params.__set_format_type(TFileFormatType::FORMAT_AVRO);
    ColumnDefinition carrier;
    carrier.name = "__dummy_col";
    carrier.identifier = Field::create_field<TYPE_STRING>(carrier.name);
    carrier.type = make_nullable(std::make_shared<DataTypeString>());
    TableReader table;
    ASSERT_TRUE(table.init({.projected_columns = {carrier},
                            .conjuncts = {},
                            .format = FileFormat::AVRO,
                            .scan_params = &params,
                            .io_ctx = std::make_shared<io::IOContext>(),
                            .runtime_state = &state,
                            .scanner_profile = &profile,
                            .push_down_agg_type = TPushAggOp::COUNT,
                            .push_down_count_columns = std::vector<GlobalIndex> {}})
                        .ok());
    SplitReadOptions split;
    split.current_range.__set_path(path);
    split.current_range.__set_start_offset(0);
    split.current_range.__set_size(std::filesystem::file_size(path));
    split.current_range.__set_file_size(std::filesystem::file_size(path));
    split.current_split_format = FileFormat::AVRO;
    ASSERT_TRUE(table.prepare_split(split).ok());
    Block block {{carrier.type->create_column(), carrier.type, carrier.name}};
    bool eof = false;
    size_t rows = 0;
    while (!eof) {
        block.clear_column_data();
        const auto status = table.get_block(&block, &eof);
        ASSERT_TRUE(status.ok()) << status;
        rows += block.rows();
        for (size_t row = 0; row < block.rows(); ++row) {
            EXPECT_TRUE(block.get_by_position(0).column->is_null_at(row));
        }
    }
    EXPECT_EQ(rows, 5);
    EXPECT_TRUE(table.close().ok());
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

TEST_F(AvroReaderTest, MapperTimestampProjectionPreservesLoadColumnType) {
    for (const char* logical : {"timestamp-millis", "timestamp-micros"}) {
        const auto json = std::string(R"({"type":"record","name":"r","fields":[
          {"name":"instant","type":{"type":"long","logicalType":")") +
                          logical + R"("}}]})";
        const auto avro_schema = ::avro::compileJsonSchemaFromString(json);
        {
            ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
            ::avro::GenericDatum datum(avro_schema);
            datum.value<::avro::GenericRecord>().fieldAt(0).value<int64_t>() = 0;
            writer.write(datum);
            writer.close();
        }
        auto input = reader();
        ASSERT_TRUE(initialize(input.get()).ok());
        auto mapper = input->create_column_mapper({.mode = TableColumnMappingMode::BY_NAME});
        ASSERT_TRUE(mapper->create_mapping(schema, {}, schema).ok());
        auto request = std::make_shared<FileScanRequest>();
        ASSERT_TRUE(mapper->create_scan_request({}, schema, request.get(), &state).ok());
        Block block;
        for (const auto& mapping : mapper->mappings()) {
            EXPECT_EQ(remove_nullable(mapping.file_type)->get_primitive_type(), TYPE_DATETIMEV2);
            block.insert({mapping.file_type->create_column(), mapping.file_type,
                          mapping.file_column_name});
        }
        ASSERT_TRUE(input->open(request).ok());
        EXPECT_EQ(
                read_all(input.get(), block),
                (std::vector<std::vector<std::string>> {{logical == std::string("timestamp-millis")
                                                                 ? "1970-01-01 00:00:00.000"
                                                                 : "1970-01-01 00:00:00.000000"}}));
    }
}

TEST_F(AvroReaderTest, RejectsInstantNanosecondsWithoutLosingTimezoneOrPrecision) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"instant",
                "type":{"type":"long","logicalType":"timestamp-nanos"}}]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
        writer.close();
    }
    for (const bool timestamp_tz : {false, true}) {
        auto input = reader(0, -1, timestamp_tz);
        const auto status = initialize(input.get());
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("timestamp-nanos"), std::string::npos) << status;
    }
}

TEST_F(AvroReaderTest, HiveWriterTimezoneAndCalendarStayInTheHivePath) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[
                {"name":"day","type":{"type":"int","logicalType":"date"}},
                {"name":"clock","type":{"type":"long","logicalType":"timestamp-millis"}}]})");
    ::avro::Metadata metadata;
    const std::string writer_zone = "America/Los_Angeles";
    metadata["writer.time.zone"] = {writer_zone.begin(), writer_zone.end()};
    metadata["writer.proleptic"] = {'f', 'a', 'l', 's', 'e'};
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema, 32,
                                                            ::avro::NULL_CODEC, metadata);
        ::avro::GenericDatum datum(avro_schema);
        auto& record = datum.value<::avro::GenericRecord>();
        record.fieldAt(0).value<int32_t>() = -141428;
        record.fieldAt(1).value<int64_t>() = 1704182400000LL;
        writer.write(datum);
        writer.close();
    }
    for (const bool hive : {false, true}) {
        auto input = reader(0, -1, false, false, std::nullopt, hive);
        ASSERT_TRUE(initialize(input.get()).ok());
        auto request = std::make_shared<FileScanRequest>();
        auto block = block_for({0, 1}, request.get());
        ASSERT_TRUE(input->open(request).ok());
        EXPECT_EQ(read_all(input.get(), block),
                  (std::vector<std::vector<std::string>> {
                          {hive ? "1582-10-04" : "1582-10-14",
                           hive ? "2024-01-02 00:00:00.000" : "2024-01-02 08:00:00.000"}}));
    }
}

TEST_F(AvroReaderTest, HiveCalendarsCoverOldWritersAndCenturyBoundaries) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[
                {"name":"day","type":{"type":"int","logicalType":"date"}}]})");
    for (const int writer_version : {2, 3, 4}) {
        ::avro::Metadata metadata;
        if (writer_version >= 3) {
            metadata["writer.time.zone"] = {'U', 'T', 'C'};
        }
        if (writer_version == 4) {
            metadata["writer.proleptic"] = {'f', 'a', 'l', 's', 'e'};
        }
        const std::vector<int32_t> days =
                writer_version == 3
                        ? std::vector<int32_t> {-141438, -141427, -171606, -171605, -171605}
                        : std::vector<int32_t> {-141428, -141427, -171597, -171596, -171595};
        {
            ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema, 32,
                                                                ::avro::NULL_CODEC, metadata);
            for (const auto day : days) {
                ::avro::GenericDatum datum(avro_schema);
                datum.value<::avro::GenericRecord>().fieldAt(0).value<int32_t>() = day;
                writer.write(datum);
            }
            writer.close();
        }
        auto input = reader(0, -1, false, false, std::nullopt, true);
        ASSERT_TRUE(initialize(input.get()).ok());
        auto request = std::make_shared<FileScanRequest>();
        auto block = block_for({0}, request.get());
        ASSERT_TRUE(input->open(request).ok());
        EXPECT_EQ(read_all(input.get(), block),
                  (std::vector<std::vector<std::string>> {{"1582-10-04"},
                                                          {"1582-10-15"},
                                                          {"1500-02-28"},
                                                          {"1500-03-01"},
                                                          {"1500-03-01"}}));
    }
}

TEST_F(AvroReaderTest, SchemaExpansionIsAdmittedBeforeBuildingColumnTrees) {
    std::string json = R"({"type":"record","name":"root","fields":[
        {"name":"n0","type":{"type":"record","name":"N0","fields":[{"name":"v","type":"long"}]}})";
    for (int i = 1; i <= 15; ++i) {
        const auto previous = "N" + std::to_string(i - 1);
        json += ",{\"name\":\"n" + std::to_string(i) +
                "\",\"type\":{\"type\":\"record\",\"name\":\"N" + std::to_string(i) +
                "\",\"fields\":[{\"name\":\"a\",\"type\":\"" + previous +
                "\"},{\"name\":\"b\",\"type\":\"" + previous + "\"}]}}";
    }
    json += "]}";
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(
                path.c_str(), ::avro::compileJsonSchemaFromString(json));
        writer.close();
    }
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "avro_schema_limit", 8 * 1024 * 1024);
    SCOPED_ATTACH_TASK(tracker);
    auto input = reader();
    EXPECT_FALSE(initialize(input.get()).ok());
    EXPECT_TRUE(input->close().ok());
}

TEST_F(AvroReaderTest, RepeatedRecordNamesAreAdmittedBeforeBuildingColumnTrees) {
    std::string json = R"({"type":"record","name":"root","fields":[
        {"name":"n0","type":{"type":"record","name":"Shared","fields":[{"name":")";
    json += std::string(64 * 1024, 'a');
    json += R"(","type":"long"}]}})";
    for (int i = 1; i < 128; ++i) {
        json += ",{\"name\":\"n" + std::to_string(i) + "\",\"type\":\"Shared\"}";
    }
    json += "]}";
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(
                path.c_str(), ::avro::compileJsonSchemaFromString(json));
        writer.close();
    }
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "avro_schema_name_limit", 8 * 1024 * 1024);
    SCOPED_ATTACH_TASK(tracker);
    auto input = reader();
    const auto status = initialize(input.get());
    EXPECT_FALSE(status.ok()) << status;
    EXPECT_TRUE(input->close().ok());
}

TEST_F(AvroReaderTest, UnprojectedNamesAreAdmittedForTheWholeMappingLifetime) {
    std::string json = R"({"type":"record","name":"root","fields":[
        {"name":"n0","type":{"type":"record","name":"Shared","fields":[{"name":")";
    json += std::string(64 * 1024, 'a');
    json += R"(","type":"long"}]}})";
    for (int i = 1; i < 128; ++i) {
        json += ",{\"name\":\"n" + std::to_string(i) + "\",\"type\":\"Shared\"}";
    }
    json += R"(,{"name":"id","type":"int"}]})";
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(
                path.c_str(), ::avro::compileJsonSchemaFromString(json));
        writer.close();
    }
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "avro_schema_name_limit", 54 * 1024 * 1024);
    SCOPED_ATTACH_TASK(tracker);
    TFileScanRangeParams params;
    params.__set_file_type(TFileType::FILE_LOCAL);
    params.__set_format_type(TFileFormatType::FORMAT_AVRO);
    ColumnDefinition projected;
    projected.name = "id";
    projected.identifier = Field::create_field<TYPE_STRING>("id");
    projected.type = make_nullable(std::make_shared<DataTypeInt32>());
    TableReader table;
    ASSERT_TRUE(table.init({.projected_columns = {projected},
                            .conjuncts = {},
                            .format = FileFormat::AVRO,
                            .scan_params = &params,
                            .io_ctx = std::make_shared<io::IOContext>(),
                            .runtime_state = &state,
                            .scanner_profile = &profile})
                        .ok());
    SplitReadOptions split;
    split.current_range.__set_path(path);
    split.current_range.__set_start_offset(0);
    split.current_range.__set_size(std::filesystem::file_size(path));
    split.current_range.__set_file_size(std::filesystem::file_size(path));
    split.current_split_format = FileFormat::AVRO;
    ASSERT_TRUE(table.prepare_split(split).ok());
    Block block;
    block.insert({projected.type->create_column(), projected.type, projected.name});
    bool eof = false;
    // Unrequested writer fields still live in the reader, table schema and mapper schema.
    EXPECT_FALSE(table.get_block(&block, &eof).ok());
    EXPECT_TRUE(table.close().ok());
}

TEST_F(AvroReaderTest, HiveValidatesImplicitAndTransportedReaderSchemasBeforeMapping) {
    auto record = [](const std::string& type) {
        return "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"v\",\"type\":" + type +
               "}]}";
    };
    auto scan = [&](DataTypePtr type, const std::string& literal, bool hdfs, Block* block) {
        TFileScanRangeParams params;
        params.__set_file_type(TFileType::FILE_LOCAL);
        params.__set_format_type(TFileFormatType::FORMAT_AVRO);
        if (!literal.empty()) {
            if (hdfs) {
                THdfsConf conf;
                conf.__set_key("avro.schema.literal");
                conf.__set_value(literal);
                THdfsParams hdfs_params;
                hdfs_params.__set_hdfs_conf({conf});
                params.__set_hdfs_params(hdfs_params);
            } else {
                params.__set_properties({{"avro.schema.literal", literal}});
            }
        }
        ColumnDefinition projected;
        projected.name = "v";
        projected.identifier = Field::create_field<TYPE_STRING>("v");
        projected.type = make_nullable(type);
        hive::HiveReader table;
        auto status = table.init({.projected_columns = {projected},
                                  .conjuncts = {},
                                  .format = FileFormat::AVRO,
                                  .scan_params = &params,
                                  .io_ctx = std::make_shared<io::IOContext>(),
                                  .runtime_state = &state,
                                  .scanner_profile = &profile});
        if (!status.ok()) {
            return status;
        }
        Defer close([&] { EXPECT_TRUE(table.close().ok()); });
        SplitReadOptions split;
        split.current_range.__set_path(path);
        split.current_range.__set_start_offset(0);
        split.current_range.__set_size(std::filesystem::file_size(path));
        split.current_range.__set_file_size(std::filesystem::file_size(path));
        split.current_split_format = FileFormat::AVRO;
        RETURN_IF_ERROR(table.prepare_split(split));
        block->insert({projected.type->create_column(), projected.type, projected.name});
        bool eof = false;
        return table.get_block(block, &eof);
    };
    const std::string date = R"({"type":"int","logicalType":"date"})";
    const std::string millis = R"({"type":"long","logicalType":"timestamp-millis"})";
    const std::string micros = R"({"type":"long","logicalType":"timestamp-micros"})";
    for (const auto& [writer_type, table_type, accepted] :
         std::vector<std::tuple<std::string, DataTypePtr, bool>> {
                 {date, std::make_shared<DataTypeInt32>(), false},
                 {R"("int")", std::make_shared<DataTypeDateV2>(), false},
                 {R"("long")", std::make_shared<DataTypeInt32>(), false},
                 {micros, std::make_shared<DataTypeDateTimeV2>(6), false},
                 {millis, std::make_shared<DataTypeDateTimeV2>(3), true},
                 {date, std::make_shared<DataTypeDateV2>(), true},
                 {R"("int")", std::make_shared<DataTypeInt64>(), true},
                 {R"("int")", std::make_shared<DataTypeInt8>(), true}}) {
        SCOPED_TRACE(writer_type + " -> " + table_type->get_name());
        write_raw_block(::avro::compileJsonSchemaFromString(record(writer_type)),
                        ::avro::NULL_CODEC, {2});
        Block block;
        const auto status = scan(table_type, "", false, &block);
        EXPECT_EQ(status.ok(), accepted) << status;
        if (accepted) {
            EXPECT_EQ(block.rows(), 1);
        }
    }
    write_raw_block(::avro::compileJsonSchemaFromString(record(R"("int")")), ::avro::NULL_CODEC,
                    {2});
    const std::string explicit_date =
            R"({"type":"record","name":"r","fields":[{"name":"v","type":["null",{"type":"int","logicalType":"date"}],"default":null}]})";
    const std::string explicit_int =
            R"({"type":"record","name":"r","fields":[{"name":"v","type":["null","int"],"default":null}]})";
    for (bool hdfs : {false, true}) {
        Block rejected, accepted;
        EXPECT_FALSE(scan(std::make_shared<DataTypeInt32>(), explicit_date, hdfs, &rejected).ok());
        EXPECT_TRUE(scan(std::make_shared<DataTypeInt32>(), explicit_int, hdfs, &accepted).ok());
        ASSERT_EQ(accepted.rows(), 1);
        EXPECT_EQ(
                accepted.get_by_position(0).type->to_string(*accepted.get_by_position(0).column, 0),
                "1");
    }
    // An explicit reader promotion must not be skipped on the way to the HMS output type.
    // 16777217 rounds to 16777216 as FLOAT but remains exact as DOUBLE.
    write_raw_block(::avro::compileJsonSchemaFromString(record(R"("int")")), ::avro::NULL_CODEC,
                    {0x82, 0x80, 0x80, 0x10});
    const std::string explicit_float =
            R"({"type":"record","name":"r","fields":[{"name":"v","type":["null","float"],"default":null}]})";
    const std::string explicit_long =
            R"({"type":"record","name":"r","fields":[{"name":"v","type":["null","long"],"default":null}]})";
    for (bool hdfs : {false, true}) {
        Block rounded, exact, widened, missing;
        EXPECT_FALSE(
                scan(std::make_shared<DataTypeFloat64>(), explicit_float, hdfs, &rounded).ok());
        EXPECT_TRUE(scan(std::make_shared<DataTypeFloat32>(), explicit_float, hdfs, &exact).ok());
        EXPECT_TRUE(scan(std::make_shared<DataTypeInt64>(), explicit_long, hdfs, &widened).ok());
        EXPECT_FALSE(scan(std::make_shared<DataTypeInt32>(),
                          R"({"type":"record","name":"r","fields":[]})", hdfs, &missing)
                             .ok());
    }
    for (const auto* logical : {"char", "varchar", "ChAr", "VaRcHaR"}) {
        const std::string value_type = std::string(R"({"type":"string","logicalType":")") +
                                       logical + R"(","maxLength":3})";
        // Preserve the Hive annotation: Avro C++ would erase it while writing a fixture.
        write_header_json(record(value_type), std::vector<uint8_t> {8, 'a', 'b', 'c', 'd'});
        const std::string literal =
                "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"v\",\"type\":["
                "\"null\"," +
                value_type + "],\"default\":null}]}";
        Block implicit_block, explicit_block;
        EXPECT_FALSE(scan(std::make_shared<DataTypeString>(), "", false, &implicit_block).ok());
        EXPECT_FALSE(
                scan(std::make_shared<DataTypeString>(), literal, false, &explicit_block).ok());
    }
    write_raw_block(::avro::compileJsonSchemaFromString(record(R"("bytes")")), ::avro::NULL_CODEC,
                    {2, 0xff});
    const std::string explicit_string =
            R"({"type":"record","name":"r","fields":[{"name":"v","type":["null","string"],"default":null}]})";
    Block invalid_utf8;
    // Java's bytes-to-string resolution replaces FF with U+FFFD; raw insertion would retain FF.
    EXPECT_FALSE(
            scan(std::make_shared<DataTypeString>(), explicit_string, false, &invalid_utf8).ok());
}

TEST_F(AvroReaderTest, ValidatesPhysicalReaderSchemaBeforeSqlCasts) {
    auto record = [](const std::string& type, const std::string& name = "v",
                     const std::string& record_name = "r") {
        return "{\"type\":\"record\",\"name\":\"" + record_name + "\",\"fields\":[{\"name\":\"" +
               name + "\",\"type\":[\"null\"," + type + "],\"default\":null}]}";
    };
    const std::string date = R"({"type":"int","logicalType":"date"})";
    const std::string millis = R"({"type":"long","logicalType":"timestamp-millis"})";
    const std::string micros = R"({"type":"long","logicalType":"timestamp-micros"})";
    const std::string decimal =
            R"({"type":"bytes","logicalType":"decimal","precision":9,"scale":2})";
    const std::vector<std::pair<std::string, std::string>> incompatible = {
            {date, R"("int")"},
            {R"("int")", date},
            {millis, R"("long")"},
            {micros, millis},
            {R"({"type":"long","logicalType":"local-timestamp-millis"})", millis},
            {decimal, R"("bytes")"},
            {R"("bytes")", decimal},
            {R"({"type":"string","logicalType":"uuid"})", R"("string")"},
            {R"("long")", R"("int")"},
            {R"("double")", R"("long")"},
            {R"({"type":"array","items":"long"})", R"("long")"},
            {"{\"type\":\"array\",\"items\":" + date + "}", R"({"type":"array","items":"int"})"},
            {"{\"type\":\"map\",\"values\":" + micros + "}",
             "{\"type\":\"map\",\"values\":" + millis + "}"},
            {record(date, "child", "nested"), record(R"("int")", "child", "nested")}};
    for (const auto& [writer, read] : incompatible) {
        SCOPED_TRACE(writer + " -> " + read);
        write_header_json(record(writer));
        auto input = reader();
        ASSERT_TRUE(initialize(input.get()).ok());
        EXPECT_FALSE(input->validate_read_schema(record(read), true).ok());
        EXPECT_FALSE(input->validate_read_schema(record(read), false).ok());
    }
    for (const auto& [writer, read] :
         std::vector<std::pair<std::string, std::string>> {{date, date},
                                                           {millis, millis},
                                                           {decimal, decimal},
                                                           {R"("int")", R"("long")"},
                                                           {R"("float")", R"("double")"}}) {
        write_header_json(record(writer));
        auto input = reader();
        ASSERT_TRUE(initialize(input.get()).ok());
        EXPECT_TRUE(input->validate_read_schema(record(read), true).ok());
    }
    write_header_json(record(R"("long")", "Value"));
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    EXPECT_FALSE(input->validate_read_schema(record(R"("long")", "value"), true).ok());
    EXPECT_TRUE(input->validate_read_schema(record(R"("long")", "added"), true).ok());
    EXPECT_FALSE(input->validate_read_schema(record(R"("long")", "Value", "other"), true).ok());
    EXPECT_TRUE(input->validate_read_schema(record(R"("long")", "Value", "other"), false).ok());
}

TEST_F(AvroReaderTest, RejectsHiveAnnotationCaseVariantsBeforeCompilerErasure) {
    for (const auto* logical : {"ChAr", "VaRcHaR", "DeCiMaL", "dec\u0131mal"}) {
        const bool decimal = std::string(logical) != "ChAr" && std::string(logical) != "VaRcHaR";
        const std::string physical = decimal ? R"("bytes")" : R"("string")";
        const std::string annotated =
                std::string("{\"type\":") + physical + ",\"logicalType\":\"" + logical +
                (decimal ? "\",\"precision\":9,\"scale\":2}" : "\",\"maxLength\":3}");
        for (int nesting = 0; nesting < 4; ++nesting) {
            auto schema = [&](const std::string& value) {
                const auto nested = nesting == 1   ? "{\"type\":\"array\",\"items\":" + value + "}"
                                    : nesting == 2 ? "{\"type\":\"map\",\"values\":" + value + "}"
                                    : nesting == 3 ? "{\"type\":\"record\",\"name\":\"n\","
                                                     "\"fields\":[{\"name\":\"child\",\"type\":" +
                                                             value + "}]}"
                                                   : value;
                return "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"v\",\"type\":" +
                       nested + "}]}";
            };
            SCOPED_TRACE(std::string(logical) + " nesting=" + std::to_string(nesting));
            write_header_json(schema(annotated));
            auto writer_annotation = reader();
            ASSERT_TRUE(initialize(writer_annotation.get()).ok());
            EXPECT_FALSE(writer_annotation->validate_read_schema(schema(annotated), false).ok());
            EXPECT_TRUE(writer_annotation->close().ok());
            write_header_json(schema(physical));
            auto reader_annotation = reader();
            ASSERT_TRUE(initialize(reader_annotation.get()).ok());
            EXPECT_FALSE(reader_annotation->validate_read_schema(schema(annotated), false).ok());
        }
    }
}

TEST_F(AvroReaderTest, RejectsDuplicateSchemaKeysBeforeDefaultConstruction) {
    for (const auto* json :
         {R"({"type":"record","name":"r","fields":[{"name":"v","type":["null","long"],"default":null,"default":null}]})",
          R"({"type":"record","name":"r","fields":[{"name":"v","type":{"type":"string","logicalType":"char","logicalType":"other","maxLength":3}}]})",
          R"({"type":"record","name":"r","fields":[{"name":"v","type":"long","type":"int"}]})"}) {
        SCOPED_TRACE(json);
        write_header_json(json);
        auto input = reader();
        const auto status = initialize(input.get());
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("Duplicate"), std::string::npos) << status;
    }
    write_header_json(R"({"type":"record","name":"r","fields":[{"name":"v","type":"long"}]})");
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    EXPECT_FALSE(
            input->validate_read_schema(
                         R"({"type":"record","name":"r","fields":[{"name":"v","type":["null","long"],"default":null,"default":null}]})",
                         true)
                    .ok());
}

TEST_F(AvroReaderTest, WriterNullDefaultsDoNotRequireTheFirstUnionBranch) {
    for (const auto* value : {R"("long")",
                              R"({"type":"record","name":"child","fields":[{"name":"v",
                                  "type":{"type":"fixed","name":"fixed_value","size":4096}}]})"}) {
        const std::string json =
                R"({"type":"record","name":"root","fields":[{"name":"value","type":[)" +
                std::string(value) + R"(,"null"],"default":null}]})";
        write_header_json(json);
        auto input = reader();
        const auto status = initialize(input.get());
        ASSERT_TRUE(status.ok()) << status;
        auto request = std::make_shared<FileScanRequest>();
        auto block = block_for({0}, request.get());
        ASSERT_TRUE(input->open(request).ok());
        EXPECT_TRUE(read_all(input.get(), block).empty());
    }
}

TEST_F(AvroReaderTest, LateRangesAvoidTinyPrefixReads) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"id","type":"long"},
                {"name":"payload","type":"string"}]})");
    size_t cut = 0;
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
        for (int64_t i = 0; i < 1024; ++i) {
            ::avro::GenericDatum datum(avro_schema);
            auto& record = datum.value<::avro::GenericRecord>();
            record.fieldAt(0).value<int64_t>() = i;
            record.fieldAt(1).value<std::string>().assign(4096, 'x');
            writer.write(datum);
            writer.flush();
            if (i == 899) {
                cut = std::filesystem::file_size(path);
            }
        }
        writer.close();
    }
    for (const bool coordinates : {false, true}) {
        io_stats = {};
        auto input = reader(cut);
        ASSERT_TRUE(initialize(input.get()).ok());
        auto request = std::make_shared<FileScanRequest>();
        auto block = block_for(coordinates ? std::vector<int32_t> {ROW_POSITION_COLUMN_ID, 0}
                                           : std::vector<int32_t> {0},
                               request.get());
        const auto before = io_stats.read_calls;
        ASSERT_TRUE(input->open(request).ok());
        // Ordinary scans must prove boundaries through the same coalesced prefix as row-id scans.
        EXPECT_LE(io_stats.read_calls - before, 10);
        const auto rows = read_all(input.get(), block);
        ASSERT_EQ(rows.size(), 124);
        for (size_t i = 0; i < rows.size(); ++i) {
            EXPECT_EQ(rows[i].back(), std::to_string(i + 900));
            if (coordinates) {
                EXPECT_EQ(rows[i].front(), std::to_string(i + 900));
            }
        }
    }
}

TEST_F(AvroReaderTest, NullUnionsDoNotReserveUnselectedFixedStorage) {
    for (const bool null_first : {false, true}) {
        for (const bool nested : {false, true}) {
            const std::string large = R"({"type":"fixed","name":"large","size":134217728})";
            const auto members = null_first ? "\"null\"," + large : large + ",\"null\"";
            const std::string value_type = "[" + members + "]";
            const auto type = nested ? "[\"null\",{\"type\":\"record\",\"name\":\"inner\","
                                       "\"fields\":[{\"name\":\"v\",\"type\":" +
                                               value_type + "}]}]"
                                     : value_type;
            const auto avro_schema = ::avro::compileJsonSchemaFromString(
                    R"({"type":"record","name":"r","fields":[{"name":"value","type":)" + type +
                    "}]}");
            std::vector<uint8_t> payload;
            if (nested) {
                payload.push_back(2);
            }
            payload.push_back(null_first ? 0 : 2);
            write_raw_block(avro_schema, ::avro::NULL_CODEC, payload);
            auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                            "avro_null_union", 8 * 1024 * 1024);
            SCOPED_ATTACH_TASK(tracker);
            auto input = reader();
            ASSERT_TRUE(initialize(input.get()).ok());
            auto request = std::make_shared<FileScanRequest>();
            auto block = block_for({0}, request.get());
            const auto status = input->open(request);
            ASSERT_TRUE(status.ok()) << status;
            const auto rows = read_all(input.get(), block);
            ASSERT_EQ(rows.size(), 1);
            if (nested) {
                EXPECT_NE(rows[0][0], "NULL");
            } else {
                EXPECT_EQ(rows[0][0], "NULL");
            }
        }
    }
}

TEST_F(AvroReaderTest, EmptyContainersDoNotAdmitUnmaterializedFixedValues) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"value",
                "type":{"type":"fixed","name":"large","size":134217728}}]})");
    for (const auto codec :
         {::avro::NULL_CODEC, ::avro::DEFLATE_CODEC, ::avro::SNAPPY_CODEC, ::avro::ZSTD_CODEC}) {
        {
            ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema, 32,
                                                                codec);
            writer.close();
        }
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                        "avro_empty_fixed", 8 * 1024 * 1024);
        SCOPED_ATTACH_TASK(tracker);
        auto input = reader();
        ASSERT_TRUE(initialize(input.get()).ok());
        auto request = std::make_shared<FileScanRequest>();
        auto block = block_for({0}, request.get());
        const auto status = input->open(request);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_TRUE(read_all(input.get(), block).empty());
    }
}

TEST_F(AvroReaderTest, RepeatedUnionBranchesReuseTheirAdmittedStorage) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"value",
                "type":["null",{"type":"fixed","name":"large","size":1048576}]}]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema, 32,
                                                            ::avro::SNAPPY_CODEC);
        for (int i = 0; i < 2; ++i) {
            ::avro::GenericDatum datum(avro_schema);
            auto& value = datum.value<::avro::GenericRecord>().fieldAt(0);
            value.selectBranch(1);
            value.value<::avro::GenericFixed>().value().assign(1048576, 'x');
            writer.write(datum);
            writer.flush();
        }
        writer.close();
    }
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "avro_reused_union", 5 * 1024 * 1024);
    SCOPED_ATTACH_TASK(tracker);
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    input->set_batch_size(1);
    const auto rows = read_all(input.get(), block);
    ASSERT_EQ(rows.size(), 2);
    EXPECT_EQ(rows[0][0].size(), 1048576);
    EXPECT_EQ(rows[1][0], rows[0][0]);
}

TEST_F(AvroReaderTest, DeflateFinishesPendingOutputAfterConsumingItsInput) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"value",
                "type":{"type":"fixed","name":"s","size":65538}}]})");
    std::vector<uint8_t> compressed;
    size_t bits = 0;
    auto bit = [&](unsigned value) {
        if (bits % 8 == 0) {
            compressed.push_back(0);
        }
        compressed.back() |= (value & 1) << (bits++ % 8);
    };
    auto symbol = [&](unsigned value) {
        const unsigned length = value <= 143 ? 8 : value <= 255 ? 9 : value <= 279 ? 7 : 8;
        const unsigned code = value <= 143   ? value + 48
                              : value <= 255 ? value - 144 + 400
                              : value <= 279 ? value - 256
                                             : value - 280 + 192;
        for (int i = length - 1; i >= 0; --i) {
            bit(code >> i);
        }
    };
    bit(1);
    bit(1);
    bit(0); // Final fixed-Huffman block.
    for (int i = 0; i < 1034; ++i) {
        symbol('x');
    }
    for (int i = 0; i < 250; ++i) {
        symbol(285); // Length 258, distance 1.
        for (int j = 0; j < 5; ++j) {
            bit(0);
        }
    }
    symbol(258); // The final length-4 match crosses the 64 KiB output boundary.
    for (int j = 0; j < 5; ++j) {
        bit(0);
    }
    symbol(256);
    write_raw_block(avro_schema, ::avro::DEFLATE_CODEC, compressed);
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    const auto rows = read_all(input.get(), block);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0][0], std::string(65538, 'x'));
}

TEST_F(AvroReaderTest, EmptyDeflateBlocksReleaseTheirCodecWorkspaces) {
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(
                path.c_str(), ::avro::compileJsonSchemaFromString(ID_SCHEMA), 32,
                ::avro::DEFLATE_CODEC);
        for (int i = 0; i < 40; ++i) {
            writer.flush();
        }
        writer.close();
    }
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "avro_empty_blocks", 8 * 1024 * 1024);
    SCOPED_ATTACH_TASK(tracker);
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    size_t rows = 0;
    bool eof = false;
    const auto status = input->get_block(&block, &rows, &eof);
    EXPECT_TRUE(status.ok()) << status;
    EXPECT_EQ(rows, 0);
    EXPECT_TRUE(eof);
}

TEST_F(AvroReaderTest, EmptyDeflatePayloadStillRejectsRecordsRequiringBytes) {
    for (const auto& json :
         {std::string(ID_SCHEMA), std::string(R"({"type":"record","name":"r","fields":[
                                {"name":"v","type":["null","int"]}]})"),
          std::string(R"({"type":"record","name":"r","fields":[
                                {"name":"v","type":{"type":"array","items":"null"}}]})")}) {
        const auto avro_schema = ::avro::compileJsonSchemaFromString(json);
        for (bool projected : {false, true}) {
            write_raw_block(avro_schema, ::avro::DEFLATE_CODEC, {});
            auto input = reader();
            ASSERT_TRUE(initialize(input.get()).ok());
            auto request = std::make_shared<FileScanRequest>();
            Block block = projected ? block_for({0}, request.get()) : Block();
            ASSERT_TRUE(input->open(request).ok());
            size_t rows = 0;
            bool eof = false;
            EXPECT_FALSE(input->get_block(&block, &rows, &eof).ok());
            EXPECT_EQ(rows, 0);
            EXPECT_TRUE(input->close().ok());
        }
    }
}

TEST_F(AvroReaderTest, DeflateZeroByteRecordsRetainNullColumns) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"v","type":"null"}]})");
    write_raw_block(avro_schema, ::avro::DEFLATE_CODEC, {}, 3);
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    const auto rows = read_all(input.get(), block);
    ASSERT_EQ(rows.size(), 3);
    for (const auto& row : rows) {
        EXPECT_EQ(row[0], "NULL");
    }
}

TEST_F(AvroReaderTest, ZstandardRequiresAFrameEvenForNullValues) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"value","type":"null"}]})");
    write_raw_block(avro_schema, ::avro::ZSTD_CODEC, {});
    auto input = reader();
    ASSERT_TRUE(initialize(input.get()).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    size_t rows = 0;
    bool eof = false;
    EXPECT_FALSE(input->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 0);
}

TEST_F(AvroReaderTest, HiveAcceptsJavaFixedOffsetWriterZones) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"clock",
                "type":{"type":"long","logicalType":"timestamp-millis"}}]})");
    const std::vector<std::pair<std::string, std::string>> cases {
            {"GMT+08:00", "1970-01-01 08:00:00.000"},
            {"GMT-13:00", "1969-12-31 11:00:00.000"},
            {"UTC+15:00", "1970-01-01 15:00:00.000"},
            {"UT+18:00", "1970-01-01 18:00:00.000"},
            {"-18:00", "1969-12-31 06:00:00.000"},
            {"+08:00:30", "1970-01-01 08:00:30.000"},
            {"Z", "1970-01-01 00:00:00.000"},
            {"GMT+18:00:01", ""},
            {"+19:00", ""},
            {"UTC-18:01", ""},
            {"GMT+08:60", ""}};
    for (const auto& [zone, expected] : cases) {
        ::avro::Metadata metadata {{"writer.time.zone", {zone.begin(), zone.end()}},
                                   {"writer.proleptic", {'t', 'r', 'u', 'e'}}};
        {
            ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema, 32,
                                                                ::avro::NULL_CODEC, metadata);
            ::avro::GenericDatum datum(avro_schema);
            writer.write(datum);
            writer.close();
        }
        auto input = reader(0, -1, false, false, std::nullopt, true);
        const auto status = initialize(input.get());
        if (expected.empty()) {
            EXPECT_FALSE(status.ok()) << zone;
            continue;
        }
        ASSERT_TRUE(status.ok()) << zone << ": " << status;
        auto request = std::make_shared<FileScanRequest>();
        auto block = block_for({0}, request.get());
        ASSERT_TRUE(input->open(request).ok());
        EXPECT_EQ(read_all(input.get(), block),
                  (std::vector<std::vector<std::string>> {{expected}}));
    }
}

TEST_F(AvroReaderTest, OldHiveTimestampsRejectAmbiguousHistoricalTimezoneRules) {
    const auto avro_schema = ::avro::compileJsonSchemaFromString(
            R"({"type":"record","name":"r","fields":[{"name":"clock",
                "type":{"type":"long","logicalType":"timestamp-millis"}}]})");
    {
        ::avro::DataFileWriter<::avro::GenericDatum> writer(path.c_str(), avro_schema);
        ::avro::GenericDatum datum(avro_schema);
        datum.value<::avro::GenericRecord>().fieldAt(0).value<int64_t>() = -5364644400000LL;
        writer.write(datum);
        writer.close();
    }
    TQueryGlobals globals;
    globals.__set_time_zone("America/New_York");
    RuntimeState historical_state {TQueryOptions(), globals};
    auto input = reader(0, -1, false, false, std::nullopt, true);
    ASSERT_TRUE(input->init(&historical_state).ok());
    ASSERT_TRUE(input->get_schema(&schema).ok());
    auto request = std::make_shared<FileScanRequest>();
    auto block = block_for({0}, request.get());
    ASSERT_TRUE(input->open(request).ok());
    size_t rows = 0;
    bool eof = false;
    const auto status = input->get_block(&block, &rows, &eof);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("1900"), std::string::npos) << status;
    EXPECT_EQ(rows, 0);
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
        auto status = input->open(request);
        size_t rows = 0;
        bool eof = false;
        // Container initialization leaves compressed blocks untouched until the first read.
        if (status.ok()) {
            status = input->get_block(&block, &rows, &eof);
        }
        EXPECT_EQ(status.code(), ErrorCode::QUERY_MEMORY_EXCEEDED) << status;
        EXPECT_EQ(rows, 0);
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
