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

#include <CLucene.h>
#include <CLucene/config/repl_wchar.h>
#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <google/protobuf/message.h>
#include <gtest/gtest.h>
#include <omp.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "common/consts.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type.h"
#include "core/field.h"
#include "core/value/bitmap_value.h"
#include "core/value/hll.h"
#include "core/value/quantile_state.h"
#include "cpp/sync_point.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "storage/binlog.h"
#include "storage/data_dir.h"
#include "storage/index/ann/ann_index_reader.h"
#include "storage/index/ann/ann_search_params.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/primary_key_index.h"
#include "storage/iterators.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/partial_update_info.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment_creator.h"
#include "storage/schema.h"
#include "storage/segment/segment.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "testutil/creators.h"
#include "testutil/variant_util.h"
#include "util/defer_op.h"
#include "util/slice.h"

namespace doris {
namespace {

constexpr std::string_view kTestDir = "./ut_dir/segment_flusher_format_test";
constexpr std::string_view kGoldenDir = "./be/test/storage/test_data/segment_flusher_format";
constexpr std::string_view kGoldenOutputDirEnv = "DORIS_SEGMENT_FLUSHER_GOLDEN_OUTPUT_DIR";
constexpr int32_t kRowBinlogSystemColumnCount = 3;
constexpr size_t kExpectedGoldenCaseCount = 76;
constexpr size_t kExpectedGoldenSegmentCount = 154;
constexpr size_t kExternalIndexRows = 180;
constexpr size_t kAnnDimensions = 4;

struct TypeCase {
    std::string_view name;
    std::string_view storage_type;
    int32_t length;
    int32_t index_length;
    int32_t precision;
    int32_t scale;
    std::array<std::string_view, 3> values;
};

// These are all types accepted by FE as an OLAP key and supported by BE's KeyCoder.
constexpr std::array kKeyTypes {
        TypeCase {"bool", "BOOLEAN", 1, 1, 0, 0, {"0", "1", "1"}},
        TypeCase {"tinyint", "TINYINT", 1, 1, 0, 0, {"-7", "0", "9"}},
        TypeCase {"smallint", "SMALLINT", 2, 2, 0, 0, {"-300", "0", "700"}},
        TypeCase {"int", "INT", 4, 4, 0, 0, {"-30000", "0", "70000"}},
        TypeCase {"bigint", "BIGINT", 8, 8, 0, 0, {"-300000", "0", "700000"}},
        TypeCase {"largeint",
                  "LARGEINT",
                  16,
                  16,
                  0,
                  0,
                  {"-300000000000000000", "0", "700000000000000000"}},
        TypeCase {"date", "DATE", 3, 3, 0, 0, {"1999-12-31", "2024-01-01", "2038-01-19"}},
        TypeCase {"datetime",
                  "DATETIME",
                  8,
                  8,
                  0,
                  0,
                  {"1999-12-31 23:59:59", "2024-01-01 00:00:00", "2038-01-19 03:14:07"}},
        TypeCase {"datev2", "DATEV2", 4, 4, 0, 0, {"1999-12-31", "2024-01-01", "2038-01-19"}},
        TypeCase {"datetimev2",
                  "DATETIMEV2",
                  8,
                  8,
                  0,
                  6,
                  {"1999-12-31 23:59:59.000001", "2024-01-01 00:00:00.123456",
                   "2038-01-19 03:14:07.999999"}},
        TypeCase {"timestamptz",
                  "TIMESTAMPTZ",
                  8,
                  8,
                  0,
                  6,
                  {"1999-12-31 23:59:59.000001 +00:00", "2024-01-01 00:00:00.123456 +00:00",
                   "2038-01-19 03:14:07.999999 +00:00"}},
        TypeCase {"char", "CHAR", 12, 12, 0, 0, {"a", "middle", "zzzz"}},
        TypeCase {"varchar", "VARCHAR", 66, 20, 0, 0, {"a", "middle", "zzzz"}},
        TypeCase {"decimalv2",
                  "DECIMAL",
                  12,
                  12,
                  27,
                  9,
                  {"-123.456789012", "0.000000001", "987.654321098"}},
        TypeCase {"decimal32", "DECIMAL32", 4, 4, 9, 2, {"-123.45", "0.01", "987.65"}},
        TypeCase {"decimal64", "DECIMAL64", 8, 8, 18, 4, {"-123456.7890", "0.0001", "987654.3210"}},
        TypeCase {"decimal128",
                  "DECIMAL128I",
                  16,
                  16,
                  38,
                  9,
                  {"-123456789.123456789", "0.000000001", "987654321.987654321"}},
        TypeCase {"decimal256",
                  "DECIMAL256",
                  32,
                  32,
                  76,
                  18,
                  {"-123456789012345678.123456789012345678", "0.000000000000000001",
                   "987654321098765432.987654321098765432"}},
        TypeCase {"ipv4", "IPV4", 4, 4, 0, 0, {"1.1.1.1", "10.20.30.40", "255.255.255.254"}},
        TypeCase {"ipv6",
                  "IPV6",
                  16,
                  16,
                  0,
                  0,
                  {"::1", "2001:db8::1", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe"}},
};

// FE accepts the five signed integer types and the five date-like types as sequence columns.
constexpr std::array kSequenceTypes {
        kKeyTypes[1], kKeyTypes[2], kKeyTypes[3], kKeyTypes[4], kKeyTypes[5],
        kKeyTypes[6], kKeyTypes[7], kKeyTypes[8], kKeyTypes[9], kKeyTypes[10],
};

constexpr std::array kAdditionalScalarValueTypes {
        TypeCase {"float", "FLOAT", 4, 4, 0, 0, {"-1.25", "0.5", "9.75"}},
        TypeCase {"double", "DOUBLE", 8, 8, 0, 0, {"-1.23456789", "0.00000001", "98765.4321"}},
        TypeCase {"string",
                  "STRING",
                  std::numeric_limits<int32_t>::max(),
                  20,
                  0,
                  0,
                  {"short", "a medium length string", "a string with UTF-8: 中文"}},
        TypeCase {
                "jsonb",
                "JSONB",
                std::numeric_limits<int32_t>::max(),
                std::numeric_limits<int32_t>::max(),
                0,
                0,
                {R"({"a":1})", R"({"array":[1,2],"flag":true})", R"({"nested":{"name":"doris"}})"}},
};

class LocalSegmentFileWriterCreator final : public FileWriterCreator {
public:
    LocalSegmentFileWriterCreator(std::string directory, TabletSchemaSPtr schema)
            : _directory(std::move(directory)), _schema(std::move(schema)) {}

    Status create(uint32_t segment_id, io::FileWriterPtr& file_writer,
                  FileType file_type) override {
        EXPECT_EQ(file_type, FileType::SEGMENT_FILE);
        return io::global_local_filesystem()->create_file(path(segment_id), &file_writer);
    }

    Status create(uint32_t segment_id, IndexFileWriterPtr* file_writer) override {
        const auto storage_format = _schema->get_inverted_index_storage_format();
        std::string index_path_prefix {
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(path(segment_id))};
        io::FileWriterPtr compound_file_writer;
        if (storage_format != InvertedIndexStorageFormatPB::V1) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_file(
                    segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix),
                    &compound_file_writer));
        }
        *file_writer = std::make_unique<segment_v2::IndexFileWriter>(
                io::global_local_filesystem(), std::move(index_path_prefix), "10002", segment_id,
                storage_format, std::move(compound_file_writer), true, 10001);
        return Status::OK();
    }

    std::string path(uint32_t segment_id) const {
        return fmt::format("{}/segment_{}.dat", _directory, segment_id);
    }

    const std::string& directory() const { return _directory; }

    std::vector<std::string> index_paths(uint32_t segment_id) const {
        if (!_schema->has_inverted_index() && !_schema->has_ann_index()) {
            return {};
        }
        // This test creator does not own V1's per-index files. V2 and V3 use the compound index
        // writer supplied by create(), so those files can be fingerprinted here.
        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            return {};
        }
        const auto segment_path = path(segment_id);
        const auto index_path_prefix =
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(segment_path);
        return {segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix)};
    }

private:
    std::string _directory;
    TabletSchemaSPtr _schema;
};

class TestSegmentCollector final : public SegmentCollector {
public:
    Status add(uint32_t segment_id, SegmentStatistics& statistics) override {
        segment_ids.push_back(segment_id);
        segment_statistics.push_back(statistics);
        return Status::OK();
    }

    std::vector<uint32_t> segment_ids;
    std::vector<SegmentStatistics> segment_statistics;
};

ColumnPB* add_column(TabletSchemaPB* schema, int32_t unique_id, std::string_view name,
                     const TypeCase& type, bool is_key, bool is_nullable,
                     std::string_view aggregation = "NONE") {
    auto* column = schema->add_column();
    column->set_unique_id(unique_id);
    column->set_name(name.data(), name.size());
    column->set_type(type.storage_type.data(), type.storage_type.size());
    column->set_is_key(is_key);
    column->set_is_nullable(is_nullable);
    column->set_aggregation(aggregation.data(), aggregation.size());
    column->set_length(type.length);
    column->set_index_length(type.index_length);
    column->set_precision(type.precision);
    column->set_frac(type.scale);
    return column;
}

ColumnPB* add_child_column(ColumnPB* parent, int32_t unique_id, std::string_view name,
                           const TypeCase& type, bool is_nullable) {
    auto* column = parent->add_children_columns();
    column->set_unique_id(unique_id);
    column->set_name(name.data(), name.size());
    column->set_type(type.storage_type.data(), type.storage_type.size());
    column->set_is_key(false);
    column->set_is_nullable(is_nullable);
    column->set_aggregation("NONE");
    column->set_length(type.length);
    column->set_index_length(type.index_length);
    column->set_precision(type.precision);
    column->set_frac(type.scale);
    return column;
}

ColumnPB* add_hidden_column(TabletSchemaPB* schema, int32_t unique_id, std::string_view name,
                            const TypeCase& type, std::string_view aggregation,
                            std::string_view default_value) {
    auto* column = add_column(schema, unique_id, name, type, false, false, aggregation);
    column->set_visible(false);
    column->set_default_value(default_value.data(), default_value.size());
    return column;
}

constexpr std::array<size_t, kKeyTypes.size()> kWideKeyTypeOrder {
        3, 0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19, 12,
};

constexpr std::array<size_t, kKeyTypes.size()> kClusterKeyTypeOrder {
        1, 3, 0, 2, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19, 12,
};

struct WideKeySchemaOptions {
    KeysType keys_type;
    TabletStorageFormatPB storage_format;
    bool nullable_keys;
    bool enable_mow;
    bool without_key = false;
    bool with_cluster_key = false;
    const TypeCase* sequence_type = nullptr;
};

std::array<int32_t, kKeyTypes.size()> add_wide_key_columns(TabletSchemaPB* schema_pb,
                                                           bool nullable_keys) {
    DORIS_CHECK_EQ(schema_pb->column_size(), 0);
    std::array<int32_t, kKeyTypes.size()> uid_by_type_index {};
    for (const auto type_index : kWideKeyTypeOrder) {
        const auto unique_id = schema_pb->column_size();
        uid_by_type_index[type_index] = unique_id;
        add_column(schema_pb, unique_id, fmt::format("k_{}", kKeyTypes[type_index].name),
                   kKeyTypes[type_index], true, nullable_keys);
    }
    return uid_by_type_index;
}

std::set<std::string> wide_key_column_names() {
    std::set<std::string> names;
    for (const auto type_index : kWideKeyTypeOrder) {
        names.emplace(fmt::format("k_{}", kKeyTypes[type_index].name));
    }
    return names;
}

TabletSchemaSPtr create_wide_key_schema(const WideKeySchemaOptions& options) {
    DORIS_CHECK(!options.enable_mow || options.keys_type == UNIQUE_KEYS);
    DORIS_CHECK(!options.without_key || options.keys_type == DUP_KEYS);
    DORIS_CHECK(!options.with_cluster_key ||
                (options.keys_type == UNIQUE_KEYS && options.enable_mow));
    DORIS_CHECK(options.sequence_type == nullptr || options.keys_type == UNIQUE_KEYS);

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(options.keys_type);
    // This is equivalent to the legal explicit table property "short_key" = "20". VARCHAR is
    // deliberately last, so every supported KeyCoder participates on short-key writer paths.
    schema_pb.set_num_short_key_columns(options.without_key ? 0
                                                            : cast_set<int32_t>(kKeyTypes.size()));
    schema_pb.set_num_rows_per_row_block(2);
    schema_pb.set_compression_type(LZ4F);
    schema_pb.set_storage_format(options.storage_format);

    int32_t next_unique_id = 0;
    std::array<int32_t, kKeyTypes.size()> uid_by_type_index {};
    if (!options.without_key) {
        uid_by_type_index = add_wide_key_columns(&schema_pb, options.nullable_keys);
        next_unique_id = cast_set<int32_t>(kKeyTypes.size());
    }

    const auto value_aggregation = options.keys_type == AGG_KEYS ? "SUM"
                                   : options.keys_type == UNIQUE_KEYS && !options.enable_mow
                                           ? "REPLACE"
                                           : "NONE";
    add_column(&schema_pb, next_unique_id++, "v", kKeyTypes[3], false, true, value_aggregation);
    if (options.with_cluster_key) {
        for (const auto type_index : kClusterKeyTypeOrder) {
            schema_pb.add_cluster_key_uids(uid_by_type_index[type_index]);
        }
    }
    if (options.keys_type == UNIQUE_KEYS) {
        add_hidden_column(&schema_pb, next_unique_id, DELETE_SIGN, kKeyTypes[1], value_aggregation,
                          "0");
        schema_pb.set_delete_sign_idx(next_unique_id++);
        add_hidden_column(&schema_pb, next_unique_id, VERSION_COL, kKeyTypes[4], value_aggregation,
                          "0");
        schema_pb.set_version_col_idx(next_unique_id++);
    }
    if (options.sequence_type != nullptr) {
        auto* sequence = add_hidden_column(&schema_pb, next_unique_id, SEQUENCE_COL,
                                           *options.sequence_type, value_aggregation, "");
        sequence->set_is_nullable(true);
        sequence->clear_default_value();
        schema_pb.set_sequence_col_idx(next_unique_id++);
    }
    schema_pb.set_next_column_unique_id(next_unique_id);

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

Status append_text_value(Block* block, size_t column_index, std::string_view value) {
    auto& column = block->get_by_position(column_index);
    StringRef text(value.data(), value.size());
    return column.type->from_string(text, column.column->assert_mutable().get());
}

const TypeCase& key_type_from_column_name(std::string_view name) {
    DORIS_CHECK(name.starts_with("k_"));
    const auto type_name = name.substr(2);
    const auto it = std::find_if(kKeyTypes.begin(), kKeyTypes.end(),
                                 [&](const TypeCase& type) { return type.name == type_name; });
    DORIS_CHECK(it != kKeyTypes.end());
    return *it;
}

Result<Block> create_wide_key_block(const TabletSchemaSPtr& schema,
                                    const WideKeySchemaOptions& options, int segment_ordinal) {
    Block block = schema->create_block();
    for (size_t row = 0; row < 3; ++row) {
        const size_t aggregate_value_index = options.keys_type == AGG_KEYS && row == 1 ? 0 : row;
        for (size_t column_index = 0; column_index < block.columns(); ++column_index) {
            const auto& column = block.get_by_position(column_index);
            const auto& name = column.name;
            if (name.starts_with("k_")) {
                const auto& key_type = key_type_from_column_name(name);
                if (options.nullable_keys && row == 0) {
                    column.column->assert_mutable()->insert_default();
                    continue;
                }
                size_t value_index = aggregate_value_index;
                if (options.with_cluster_key && key_type.name == "int") {
                    constexpr std::array<size_t, 3> kUnsortedPrimaryKeyOrder {0, 2, 1};
                    value_index = kUnsortedPrimaryKeyOrder[row];
                }
                RETURN_IF_ERROR_RESULT(
                        append_text_value(&block, column_index, key_type.values[value_index]));
            } else if (name == "v") {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index,
                        std::to_string(segment_ordinal * 100 + static_cast<int>(row) * 10 + 10)));
            } else if (name == SEQUENCE_COL && row != 1) {
                DORIS_CHECK(options.sequence_type != nullptr);
                RETURN_IF_ERROR_RESULT(append_text_value(&block, column_index,
                                                         options.sequence_type->values[row]));
            } else {
                column.column->assert_mutable()->insert_default();
            }
        }
    }
    return block;
}

TabletSchemaSPtr create_all_scalar_value_schema(TabletStorageFormatPB storage_format,
                                                bool nullable_values, bool with_bloom_filters) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(2);
    schema_pb.set_compression_type(LZ4F);
    schema_pb.set_storage_format(storage_format);
    add_column(&schema_pb, 0, "k", kKeyTypes[3], true, false);

    int32_t unique_id = 1;
    for (const auto& type : kKeyTypes) {
        auto* column = add_column(&schema_pb, unique_id, fmt::format("v_{}", type.name), type,
                                  false, nullable_values);
        column->set_is_bf_column(with_bloom_filters && type.storage_type != "BOOLEAN" &&
                                 type.storage_type != "TINYINT");
        ++unique_id;
    }
    for (const auto& type : kAdditionalScalarValueTypes) {
        auto* column = add_column(&schema_pb, unique_id, fmt::format("v_{}", type.name), type,
                                  false, nullable_values);
        column->set_is_bf_column(with_bloom_filters && type.name == "string");
        ++unique_id;
    }
    if (with_bloom_filters) {
        schema_pb.set_bf_fpp(0.01);
    }

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

Result<Block> create_all_scalar_value_block(const TabletSchemaSPtr& schema, int segment_ordinal) {
    Block block = schema->create_block();
    for (size_t row = 0; row < 3; ++row) {
        RETURN_IF_ERROR_RESULT(append_text_value(
                &block, 0, std::to_string(segment_ordinal * 10 + static_cast<int>(row))));
        size_t column_index = 1;
        for (const auto& type : kKeyTypes) {
            if (schema->column(column_index).is_nullable() && row == 1) {
                block.get_by_position(column_index).column->assert_mutable()->insert_default();
            } else {
                RETURN_IF_ERROR_RESULT(append_text_value(&block, column_index,
                                                         type.values[(row + segment_ordinal) % 3]));
            }
            ++column_index;
        }
        for (const auto& type : kAdditionalScalarValueTypes) {
            if (schema->column(column_index).is_nullable() && row == 1) {
                block.get_by_position(column_index).column->assert_mutable()->insert_default();
            } else {
                RETURN_IF_ERROR_RESULT(append_text_value(&block, column_index,
                                                         type.values[(row + segment_ordinal) % 3]));
            }
            ++column_index;
        }
    }
    return block;
}

TabletSchemaSPtr create_embedded_index_schema(TabletStorageFormatPB storage_format,
                                              bool nullable_values) {
    constexpr TypeCase indexed_string_type {
            "indexed_string", "STRING", std::numeric_limits<int32_t>::max(), 20, 0, 0, {}};
    constexpr TypeCase ngram_varchar_type {"ngram_varchar", "VARCHAR", 258, 20, 0, 0, {}};

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(2);
    schema_pb.set_compression_type(LZ4F);
    schema_pb.set_storage_format(storage_format);
    schema_pb.set_storage_page_size(4096);
    schema_pb.set_storage_dict_page_size(4096);
    schema_pb.set_bf_fpp(0.01);
    add_column(&schema_pb, 0, "k", kKeyTypes[3], true, false);
    auto* classic_bloom = add_column(&schema_pb, 1, "v_classic_bloom", indexed_string_type, false,
                                     nullable_values);
    classic_bloom->set_is_bf_column(true);
    auto* ngram_bloom =
            add_column(&schema_pb, 2, "v_ngram_bloom", ngram_varchar_type, false, nullable_values);
    ngram_bloom->set_is_bf_column(true);

    auto* ngram_index = schema_pb.add_index();
    ngram_index->set_index_id(10000);
    ngram_index->set_index_name("v_ngram_bloom_idx");
    ngram_index->set_index_type(IndexType::NGRAM_BF);
    ngram_index->add_col_unique_id(2);
    (*ngram_index->mutable_properties())["gram_size"] = "5";
    (*ngram_index->mutable_properties())["bf_size"] = "1024";

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

Result<Block> create_embedded_index_block(const TabletSchemaSPtr& schema, int segment_ordinal) {
    constexpr size_t kRows = 180;
    constexpr size_t kPayloadSize = 96;
    Block block = schema->create_block();
    for (size_t row = 0; row < kRows; ++row) {
        RETURN_IF_ERROR_RESULT(
                append_text_value(&block, 0, std::to_string(segment_ordinal * kRows + row)));
        if (schema->column(1).is_nullable() && row % 37 == 0) {
            block.get_by_position(1).column->assert_mutable()->insert_default();
            block.get_by_position(2).column->assert_mutable()->insert_default();
            continue;
        }

        std::string classic_value = fmt::format("classic-{}-{}-", segment_ordinal, row);
        std::string ngram_value = fmt::format("ngram-{}-{}-", segment_ordinal, row);
        for (size_t byte = 0; byte < kPayloadSize; ++byte) {
            classic_value.push_back(
                    static_cast<char>('a' + (row * 17 + byte * 11 + segment_ordinal) % 26));
            ngram_value.push_back(
                    static_cast<char>('a' + (row * 7 + byte * 19 + segment_ordinal) % 26));
        }
        RETURN_IF_ERROR_RESULT(append_text_value(&block, 1, classic_value));
        RETURN_IF_ERROR_RESULT(append_text_value(&block, 2, ngram_value));
    }
    return block;
}

TabletSchemaSPtr create_external_inverted_index_schema(InvertedIndexStorageFormatPB storage_format,
                                                       bool nullable_value) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(2);
    schema_pb.set_compression_type(LZ4F);
    schema_pb.set_storage_format(TABLET_STORAGE_FORMAT_V2);
    schema_pb.set_inverted_index_storage_format(storage_format);
    add_column(&schema_pb, 0, "k", kKeyTypes[3], true, false);
    add_column(&schema_pb, 1, "v_inverted", kAdditionalScalarValueTypes[2], false, nullable_value);

    auto* index = schema_pb.add_index();
    index->set_index_id(10001);
    index->set_index_name("v_inverted_idx");
    index->set_index_type(IndexType::INVERTED);
    index->add_col_unique_id(1);
    (*index->mutable_properties())[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_NONE;

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

Result<Block> create_external_inverted_index_block(const TabletSchemaSPtr& schema,
                                                   int segment_ordinal) {
    Block block = schema->create_block();
    for (size_t row = 0; row < kExternalIndexRows; ++row) {
        RETURN_IF_ERROR_RESULT(append_text_value(
                &block, 0, std::to_string(segment_ordinal * kExternalIndexRows + row)));
        if (schema->column(1).is_nullable() && row % 37 == 0) {
            block.get_by_position(1).column->assert_mutable()->insert_default();
        } else {
            RETURN_IF_ERROR_RESULT(append_text_value(
                    &block, 1, fmt::format("inverted-segment-{}-row-{}", segment_ordinal, row)));
        }
    }
    return block;
}

TabletSchemaSPtr create_ann_index_schema(InvertedIndexStorageFormatPB storage_format) {
    constexpr TypeCase array_type {
            "array", "ARRAY", OLAP_ARRAY_MAX_LENGTH, OLAP_ARRAY_MAX_LENGTH, 0, 0, {}};
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(2);
    schema_pb.set_compression_type(LZ4F);
    schema_pb.set_storage_format(TABLET_STORAGE_FORMAT_V2);
    schema_pb.set_inverted_index_storage_format(storage_format);
    add_column(&schema_pb, 0, "k", kKeyTypes[3], true, false);
    auto* vector = add_column(&schema_pb, 1, "v_ann", array_type, false, false);
    add_child_column(vector, 101, "item", kAdditionalScalarValueTypes[0], false);

    auto* index = schema_pb.add_index();
    index->set_index_id(10002);
    index->set_index_name("v_ann_idx");
    index->set_index_type(IndexType::ANN);
    index->add_col_unique_id(1);
    (*index->mutable_properties())["index_type"] = "hnsw";
    (*index->mutable_properties())["metric_type"] = "l2_distance";
    (*index->mutable_properties())["dim"] = "4";
    (*index->mutable_properties())["max_degree"] = "16";

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

std::array<float, kAnnDimensions> ann_vector(int segment_ordinal, size_t row) {
    std::array<float, kAnnDimensions> vector;
    for (size_t dimension = 0; dimension < vector.size(); ++dimension) {
        vector[dimension] =
                static_cast<float>(segment_ordinal * kExternalIndexRows * kAnnDimensions +
                                   row * kAnnDimensions + dimension);
    }
    return vector;
}

Result<Block> create_ann_index_block(const TabletSchemaSPtr& schema, int segment_ordinal) {
    Block block = schema->create_block();
    for (size_t row = 0; row < kExternalIndexRows; ++row) {
        RETURN_IF_ERROR_RESULT(append_text_value(
                &block, 0, std::to_string(segment_ordinal * kExternalIndexRows + row)));
        Array vector;
        for (const auto value : ann_vector(segment_ordinal, row)) {
            vector.push_back(Field::create_field<TYPE_FLOAT>(value));
        }
        block.get_by_position(1).column->assert_mutable()->insert(
                Field::create_field<TYPE_ARRAY>(std::move(vector)));
    }
    return block;
}

TabletSchemaSPtr create_complex_value_schema(TabletStorageFormatPB storage_format,
                                             bool nullable_complex_values,
                                             bool with_variant_bloom_filter) {
    constexpr TypeCase array_type {
            "array", "ARRAY", OLAP_ARRAY_MAX_LENGTH, OLAP_ARRAY_MAX_LENGTH, 0, 0, {}};
    constexpr TypeCase map_type {"map", "MAP", OLAP_MAP_MAX_LENGTH, OLAP_MAP_MAX_LENGTH, 0, 0, {}};
    constexpr TypeCase struct_type {
            "struct", "STRUCT", OLAP_STRUCT_MAX_LENGTH, OLAP_STRUCT_MAX_LENGTH, 0, 0, {}};
    constexpr TypeCase hll_type {"hll", "HLL", 16387, 16387, 0, 0, {}};
    constexpr TypeCase bitmap_type {"bitmap", "BITMAP", 16, 16, 0, 0, {}};
    constexpr TypeCase quantile_type {"quantile", "QUANTILE_STATE", 16, 16, 0, 0, {}};
    constexpr TypeCase variant_type {"variant",
                                     "VARIANT",
                                     std::numeric_limits<int32_t>::max(),
                                     std::numeric_limits<int32_t>::max(),
                                     0,
                                     0,
                                     {}};
    constexpr TypeCase agg_state_type {"agg_state", "AGG_STATE", 1, 1, 0, 0, {}};

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(2);
    schema_pb.set_compression_type(LZ4F);
    schema_pb.set_storage_format(storage_format);
    add_column(&schema_pb, 0, "k", kKeyTypes[3], true, false);

    auto* array = add_column(&schema_pb, 1, "v_array", array_type, false, nullable_complex_values);
    add_child_column(array, 101, "item", kKeyTypes[3], true);

    auto* map = add_column(&schema_pb, 2, "v_map", map_type, false, nullable_complex_values);
    add_child_column(map, 201, "key", kKeyTypes[12], true);
    add_child_column(map, 202, "value", kKeyTypes[3], true);

    auto* structure =
            add_column(&schema_pb, 3, "v_struct", struct_type, false, nullable_complex_values);
    add_child_column(structure, 301, "f_int", kKeyTypes[3], true);
    add_child_column(structure, 302, "f_text", kKeyTypes[12], true);
    auto* nested_array = add_child_column(structure, 303, "f_array", array_type, true);
    add_child_column(nested_array, 304, "item", kKeyTypes[3], true);

    add_column(&schema_pb, 4, "v_hll", hll_type, false, false);
    add_column(&schema_pb, 5, "v_bitmap", bitmap_type, false, false);
    add_column(&schema_pb, 6, "v_quantile", quantile_type, false, false);
    auto* variant =
            add_column(&schema_pb, 7, "v_variant", variant_type, false, nullable_complex_values);
    variant->set_variant_max_subcolumns_count(8);
    variant->set_is_bf_column(with_variant_bloom_filter);
    if (with_variant_bloom_filter) {
        schema_pb.set_bf_fpp(0.01);
    }
    auto add_agg_state_column = [&](int32_t unique_id, std::string_view name,
                                    std::string_view function_name) {
        auto* column = add_column(&schema_pb, unique_id, name, agg_state_type, false, false,
                                  function_name);
        column->set_result_is_nullable(false);
        column->set_be_exec_version(BeExecVersionManager::get_newest_version());
        return column;
    };
    auto* count = add_agg_state_column(8, "v_agg_count", "count");
    add_child_column(count, 801, "arg", kKeyTypes[3], false);
    auto* hll_union = add_agg_state_column(9, "v_agg_hll_union", "hll_union");
    add_child_column(hll_union, 901, "arg", hll_type, false);
    auto* array_agg = add_agg_state_column(10, "v_agg_array", "array_agg");
    add_child_column(array_agg, 1001, "arg", kKeyTypes[3], true);
    auto* map_agg = add_agg_state_column(11, "v_agg_map", "map_agg_v1");
    add_child_column(map_agg, 1101, "key", kKeyTypes[12], true);
    add_child_column(map_agg, 1102, "value", kKeyTypes[3], true);
    auto* bitmap_union = add_agg_state_column(12, "v_agg_bitmap_union", "bitmap_union");
    add_child_column(bitmap_union, 1201, "arg", bitmap_type, false);

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

Result<Block> create_complex_value_block(const TabletSchemaSPtr& schema, int segment_ordinal,
                                         bool nullable_complex_values) {
    const std::array<std::string_view, 3> arrays {"[1, null, 3]", "[]", "[-7, 0, 9]"};
    const std::array<std::string_view, 3> maps {R"({"a":1,"b":null})", "{}",
                                                R"({"left":-7,"right":9})"};
    const std::array<std::string_view, 3> structs {
            R"({"f_int":7,"f_text":"x","f_array":[1,null,3]})", "{}",
            R"({"f_int":-9,"f_text":"中文","f_array":[]})"};
    const std::array<std::string_view, 3> variants {
            R"({"a":1,"nested":{"x":"one"}})", R"({"a":2,"array":[1,2]})",
            R"({"different":true,"nested":{"x":"three","y":3.5}})"};

    Block block = schema->create_block();
    for (size_t row = 0; row < 3; ++row) {
        const auto value_index = (row + segment_ordinal) % 3;
        RETURN_IF_ERROR_RESULT(append_text_value(
                &block, 0, std::to_string(segment_ordinal * 10 + static_cast<int>(row))));
        if (nullable_complex_values && row == 1) {
            block.get_by_position(1).column->assert_mutable()->insert_default();
            block.get_by_position(2).column->assert_mutable()->insert_default();
            block.get_by_position(3).column->assert_mutable()->insert_default();
        } else {
            RETURN_IF_ERROR_RESULT(append_text_value(&block, 1, arrays[value_index]));
            RETURN_IF_ERROR_RESULT(append_text_value(&block, 2, maps[value_index]));
            RETURN_IF_ERROR_RESULT(append_text_value(&block, 3, structs[value_index]));
        }

        auto* hll =
                assert_cast<ColumnHLL*>(block.get_by_position(4).column->assert_mutable().get());
        hll->insert_value(HyperLogLog(static_cast<uint64_t>(100 + segment_ordinal * 10 + row)));
        auto* bitmap =
                assert_cast<ColumnBitmap*>(block.get_by_position(5).column->assert_mutable().get());
        bitmap->insert_value(BitmapValue(static_cast<uint64_t>(200 + segment_ordinal * 10 + row)));
        QuantileState quantile;
        quantile.add_value(static_cast<double>(segment_ordinal * 10 + row) + 0.25);
        auto* quantile_column = assert_cast<ColumnQuantileState*>(
                block.get_by_position(6).column->assert_mutable().get());
        quantile_column->insert_value(std::move(quantile));

        auto variant_column = block.get_by_position(7).column->assert_mutable();
        if (nullable_complex_values && row == 1) {
            variant_column->insert_default();
        } else if (nullable_complex_values) {
            auto& nullable = assert_cast<ColumnNullable&>(*variant_column);
            auto& variant = assert_cast<ColumnVariant&>(nullable.get_nested_column());
            VariantUtil::insert_root_scalar_field(
                    variant, Field::create_field<TYPE_STRING>(String(variants[value_index])));
            nullable.get_null_map_column().insert_value(0);
        } else {
            auto& variant = assert_cast<ColumnVariant&>(*variant_column);
            VariantUtil::insert_root_scalar_field(
                    variant, Field::create_field<TYPE_STRING>(String(variants[value_index])));
        }

        const uint64_t count = 300 + segment_ordinal * 10 + row;
        std::string serialized_count(sizeof(count), '\0');
        std::memcpy(serialized_count.data(), &count, sizeof(count));
        block.get_by_position(8).column->assert_mutable()->insert(
                Field::create_field<TYPE_STRING>(String(std::move(serialized_count))));

        HyperLogLog aggregate_hll(static_cast<uint64_t>(400 + segment_ordinal * 10 + row));
        std::string serialized_hll(aggregate_hll.max_serialized_size(), '\0');
        serialized_hll.resize(
                aggregate_hll.serialize(reinterpret_cast<uint8_t*>(serialized_hll.data())));
        block.get_by_position(9).column->assert_mutable()->insert(
                Field::create_field<TYPE_STRING>(String(std::move(serialized_hll))));

        block.get_by_position(10).column->assert_mutable()->insert(Field::create_field<TYPE_ARRAY>(
                Array {Field::create_field<TYPE_INT>(static_cast<int32_t>(1 + row)), Field(),
                       Field::create_field<TYPE_INT>(static_cast<int32_t>(10 + row))}));
        block.get_by_position(11).column->assert_mutable()->insert(Field::create_field<TYPE_MAP>(
                Map {Field::create_field<TYPE_ARRAY>(
                             Array {Field::create_field<TYPE_STRING>(String("a")),
                                    Field::create_field<TYPE_STRING>(String("b"))}),
                     Field::create_field<TYPE_ARRAY>(
                             Array {Field::create_field<TYPE_INT>(static_cast<int32_t>(20 + row)),
                                    Field()})}));
        auto* aggregate_bitmap = assert_cast<ColumnBitmap*>(
                block.get_by_position(12).column->assert_mutable().get());
        aggregate_bitmap->insert_value(
                BitmapValue(static_cast<uint64_t>(500 + segment_ordinal * 10 + row)));
    }
    return block;
}

TabletSchemaSPtr create_row_store_schema(KeysType keys_type = DUP_KEYS, bool enable_mow = false) {
    DORIS_CHECK(!enable_mow || keys_type == UNIQUE_KEYS);
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(keys_type);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(2);
    schema_pb.set_compression_type(LZ4F);
    schema_pb.set_storage_format(TABLET_STORAGE_FORMAT_V2);
    schema_pb.set_store_row_column(true);
    add_column(&schema_pb, 0, "k", kKeyTypes[3], true, false);
    const auto aggregation = keys_type == UNIQUE_KEYS && !enable_mow ? "REPLACE" : "NONE";
    add_column(&schema_pb, 1, "v", kAdditionalScalarValueTypes[2], false, true, aggregation);
    int32_t next_unique_id = 2;
    if (keys_type == UNIQUE_KEYS) {
        add_hidden_column(&schema_pb, next_unique_id, DELETE_SIGN, kKeyTypes[1], aggregation, "0");
        schema_pb.set_delete_sign_idx(next_unique_id++);
    }
    add_hidden_column(&schema_pb, next_unique_id++, BeConsts::ROW_STORE_COL,
                      kAdditionalScalarValueTypes[2], aggregation, "");
    if (keys_type == UNIQUE_KEYS) {
        add_hidden_column(&schema_pb, next_unique_id, VERSION_COL, kKeyTypes[4], aggregation, "0");
        schema_pb.set_version_col_idx(next_unique_id++);
    }
    schema_pb.set_next_column_unique_id(next_unique_id);

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

Result<Block> create_row_store_block(const TabletSchemaSPtr& schema, int segment_ordinal,
                                     size_t rows = 3, size_t value_size = 40) {
    Block block = schema->create_block();
    for (size_t row = 0; row < rows; ++row) {
        for (size_t column_index = 0; column_index < block.columns(); ++column_index) {
            const auto& name = block.get_by_position(column_index).name;
            if (name == "k") {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index,
                        std::to_string(segment_ordinal * 100 + static_cast<int>(row))));
            } else if (name == "v") {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index,
                        fmt::format("segment-{}-row-{}-{}", segment_ordinal, row,
                                    std::string(value_size, static_cast<char>('a' + row % 26)))));
            } else {
                block.get_by_position(column_index).column->assert_mutable()->insert_default();
            }
        }
    }
    return block;
}

TabletSchemaSPtr create_variant_row_store_schema() {
    constexpr TypeCase variant_type {"variant",
                                     "VARIANT",
                                     std::numeric_limits<int32_t>::max(),
                                     std::numeric_limits<int32_t>::max(),
                                     0,
                                     0,
                                     {}};
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(2);
    schema_pb.set_compression_type(LZ4F);
    schema_pb.set_storage_format(TABLET_STORAGE_FORMAT_V2);
    schema_pb.set_store_row_column(true);
    add_column(&schema_pb, 0, "k", kKeyTypes[3], true, false);
    auto* variant = add_column(&schema_pb, 1, "v_variant", variant_type, false, false);
    variant->set_variant_max_subcolumns_count(8);
    add_column(&schema_pb, 2, "v", kAdditionalScalarValueTypes[2], false, true);
    add_hidden_column(&schema_pb, 3, BeConsts::ROW_STORE_COL, kAdditionalScalarValueTypes[2],
                      "NONE", "");
    schema_pb.set_next_column_unique_id(4);

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

Result<Block> create_variant_row_store_block(const TabletSchemaSPtr& schema, int segment_ordinal,
                                             size_t rows = 3, size_t value_size = 40) {
    const std::array<std::string_view, 3> variants {
            R"({"a":1,"nested":{"x":"one"}})", R"({"a":2,"array":[1,2]})",
            R"({"different":true,"nested":{"x":"three","y":3.5}})"};
    Block block = schema->create_block();
    for (size_t row = 0; row < rows; ++row) {
        RETURN_IF_ERROR_RESULT(append_text_value(
                &block, 0, std::to_string(segment_ordinal * 100 + static_cast<int>(row))));
        auto* variant = assert_cast<ColumnVariant*>(
                block.get_by_position(1).column->assert_mutable().get());
        VariantUtil::insert_root_scalar_field(
                *variant,
                Field::create_field<TYPE_STRING>(String(variants[(row + segment_ordinal) % 3])));
        RETURN_IF_ERROR_RESULT(append_text_value(
                &block, 2,
                fmt::format("segment-{}-row-{}-{}", segment_ordinal, row,
                            std::string(value_size, static_cast<char>('a' + row % 26)))));
        block.get_by_position(3).column->assert_mutable()->insert_default();
    }
    return block;
}

struct PartialUpdateSchemaOptions {
    bool flexible;
    bool with_row_store = false;
    bool with_variants = false;
    bool with_sequence = false;
};

TabletSchemaSPtr create_partial_update_schema(const PartialUpdateSchemaOptions& options) {
    DORIS_CHECK(!options.flexible || !options.with_variants);
    constexpr TypeCase bitmap_type {"bitmap", "BITMAP", 16, 16, 0, 0, {}};
    constexpr TypeCase variant_type {"variant",
                                     "VARIANT",
                                     std::numeric_limits<int32_t>::max(),
                                     std::numeric_limits<int32_t>::max(),
                                     0,
                                     0,
                                     {}};
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(UNIQUE_KEYS);
    // See create_wide_key_schema: all twenty key types form one explicit short key.
    schema_pb.set_num_short_key_columns(cast_set<int32_t>(kKeyTypes.size()));
    schema_pb.set_num_rows_per_row_block(2);
    schema_pb.set_compression_type(LZ4F);
    schema_pb.set_storage_format(TABLET_STORAGE_FORMAT_V2);
    schema_pb.set_store_row_column(options.with_row_store);
    add_wide_key_columns(&schema_pb, false);
    int32_t next_unique_id = cast_set<int32_t>(kKeyTypes.size());
    auto* v1 = add_column(&schema_pb, next_unique_id++, "v1", kKeyTypes[3], false, true, "NONE");
    v1->set_default_value("0");
    auto* v2 = add_column(&schema_pb, next_unique_id++, "v2", kKeyTypes[3], false, true, "NONE");
    v2->set_default_value("0");
    if (options.with_variants) {
        for (const auto name : {"v_variant_updated", "v_variant_missing"}) {
            auto* variant = add_column(&schema_pb, next_unique_id++, name, variant_type, false,
                                       name == std::string_view("v_variant_missing"), "NONE");
            variant->set_variant_max_subcolumns_count(8);
        }
    }
    add_hidden_column(&schema_pb, next_unique_id, DELETE_SIGN, kKeyTypes[1], "NONE", "0");
    schema_pb.set_delete_sign_idx(next_unique_id++);
    if (options.with_row_store) {
        add_hidden_column(&schema_pb, next_unique_id++, BeConsts::ROW_STORE_COL,
                          kAdditionalScalarValueTypes[2], "NONE", "");
    }
    add_hidden_column(&schema_pb, next_unique_id, VERSION_COL, kKeyTypes[4], "NONE", "0");
    schema_pb.set_version_col_idx(next_unique_id++);
    if (options.flexible) {
        add_hidden_column(&schema_pb, next_unique_id, SKIP_BITMAP_COL, bitmap_type, "NONE",
                          std::string_view("\0", 1));
        schema_pb.set_skip_bitmap_col_idx(next_unique_id++);
    }
    if (options.with_sequence) {
        auto* sequence = add_hidden_column(&schema_pb, next_unique_id, SEQUENCE_COL, kKeyTypes[3],
                                           "NONE", "");
        sequence->set_is_nullable(true);
        sequence->clear_default_value();
        schema_pb.set_sequence_col_idx(next_unique_id++);
    }
    schema_pb.set_next_column_unique_id(next_unique_id);

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

Status append_partial_key_value(Block* block, size_t column_index, std::string_view name,
                                int logical_key) {
    if (name == "k_int") {
        return append_text_value(block, column_index, std::to_string(logical_key));
    }
    const auto& key_type = key_type_from_column_name(name);
    return append_text_value(
            block, column_index,
            key_type.values[static_cast<size_t>(logical_key) % key_type.values.size()]);
}

Result<Block> create_fixed_partial_update_block(const TabletSchemaSPtr& schema,
                                                const PartialUpdateInfo& partial_update_info,
                                                int segment_ordinal) {
    const std::array<std::string_view, 3> variants {
            R"({"a":1,"nested":{"x":"one"}})", R"({"a":2,"array":[1,2]})",
            R"({"different":true,"nested":{"x":"three","y":3.5}})"};
    Block block = schema->create_block_by_cids(partial_update_info.update_cids);
    for (int row = 0; row < 3; ++row) {
        for (size_t column_index = 0; column_index < block.columns(); ++column_index) {
            const auto& name = block.get_by_position(column_index).name;
            if (name.starts_with("k_")) {
                RETURN_IF_ERROR_RESULT(append_partial_key_value(&block, column_index, name,
                                                                segment_ordinal * 10 + row));
            } else if (name == "v1") {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(100 + segment_ordinal * 10 + row)));
            } else if (name == SEQUENCE_COL) {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(6000 + segment_ordinal * 10 + row)));
            } else if (name == "v_variant_updated") {
                auto* variant = assert_cast<ColumnVariant*>(
                        block.get_by_position(column_index).column->assert_mutable().get());
                VariantUtil::insert_root_scalar_field(
                        *variant, Field::create_field<TYPE_STRING>(String(variants[row])));
            } else {
                return ResultError(
                        Status::InternalError("unexpected fixed partial-update column {}", name));
            }
        }
    }
    return block;
}

Result<Block> create_flexible_partial_update_block(const TabletSchemaSPtr& schema,
                                                   int segment_ordinal) {
    DORIS_CHECK_EQ(schema->num_variant_columns(), 0);
    Block block = schema->create_block();
    auto* skip_bitmap = assert_cast<ColumnBitmap*>(
            block.get_by_position(schema->skip_bitmap_col_idx()).column->assert_mutable().get());
    constexpr std::array<int, 4> key_offsets {0, 0, 1, 2};
    for (size_t row = 0; row < key_offsets.size(); ++row) {
        for (size_t column_index = 0; column_index < block.columns(); ++column_index) {
            const auto& name = block.get_by_position(column_index).name;
            if (name.starts_with("k_")) {
                RETURN_IF_ERROR_RESULT(append_partial_key_value(
                        &block, column_index, name, segment_ordinal * 10 + key_offsets[row]));
            } else if (name == "v1") {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(100 + segment_ordinal * 10 + row)));
            } else if (name == "v2") {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(200 + segment_ordinal * 10 + row)));
            } else if (name == SEQUENCE_COL) {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(6000 + segment_ordinal * 10 + row)));
            } else if (name == DELETE_SIGN) {
                RETURN_IF_ERROR_RESULT(
                        append_text_value(&block, column_index, row == 0 ? "1" : "0"));
            } else if (name == BeConsts::ROW_STORE_COL) {
                block.get_by_position(column_index).column->assert_mutable()->insert_default();
            } else if (name == VERSION_COL) {
                block.get_by_position(column_index).column->assert_mutable()->insert_default();
            } else if (name != SKIP_BITMAP_COL) {
                return ResultError(Status::InternalError(
                        "unexpected flexible partial-update column {}", name));
            }
        }

        const auto skipped_value_index = schema->field_index(row % 2 == 0 ? "v2" : "v1");
        DCHECK_GE(skipped_value_index, 0);
        BitmapValue skipped_columns;
        skipped_columns.add(static_cast<uint64_t>(
                schema->column(static_cast<size_t>(skipped_value_index)).unique_id()));
        if (row >= 2) {
            skipped_columns.add(
                    static_cast<uint64_t>(schema->column(schema->delete_sign_idx()).unique_id()));
        }
        if (schema->has_sequence_col() && row % 2 == 1) {
            skipped_columns.add(
                    static_cast<uint64_t>(schema->column(schema->sequence_col_idx()).unique_id()));
        }
        skip_bitmap->insert_value(std::move(skipped_columns));
    }
    return block;
}

Result<Block> create_integer_tablet_block(const TabletSchemaSPtr& schema, int segment_ordinal,
                                          bool include_binlog_columns = false) {
    Block block = schema->create_block();
    for (int row = 0; row < 3; ++row) {
        for (size_t column_index = 0; column_index < block.columns(); ++column_index) {
            const auto& name = block.get_by_position(column_index).name;
            if (name == "k1") {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(segment_ordinal * 10 + row)));
            } else if (name == "v1") {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(100 + segment_ordinal * 10 + row)));
            } else if (name == "v2") {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(200 + segment_ordinal * 10 + row)));
            } else if (name == SEQUENCE_COL) {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(6000 + segment_ordinal * 10 + row)));
            } else if (name == BINLOG_TSO_COL) {
                DCHECK(include_binlog_columns);
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(10000 + segment_ordinal * 10 + row)));
            } else if (name == BINLOG_LSN_COL) {
                DCHECK(include_binlog_columns);
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(20000 + segment_ordinal * 10 + row)));
            } else if (name == BINLOG_OP_COL) {
                DCHECK(include_binlog_columns);
                RETURN_IF_ERROR_RESULT(append_text_value(&block, column_index, "0"));
            } else if (name == COMMIT_TSO_COL) {
                RETURN_IF_ERROR_RESULT(append_text_value(&block, column_index, "0"));
            } else if (name == DELETE_SIGN) {
                RETURN_IF_ERROR_RESULT(
                        append_text_value(&block, column_index, row == 2 ? "1" : "0"));
            } else {
                block.get_by_position(column_index).column->assert_mutable()->insert_default();
            }
        }
    }
    return block;
}

Result<Block> create_binlog_partial_update_block(const TabletSchemaSPtr& schema,
                                                 const PartialUpdateInfo& partial_update_info,
                                                 int segment_ordinal) {
    Block block = schema->create_block_by_cids(partial_update_info.update_cids);
    for (int row = 0; row < 3; ++row) {
        for (size_t column_index = 0; column_index < block.columns(); ++column_index) {
            const auto& name = block.get_by_position(column_index).name;
            if (name == "k1") {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(segment_ordinal * 10 + row)));
            } else if (name == "v1") {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(3000 + segment_ordinal * 10 + row)));
            } else if (name == SEQUENCE_COL) {
                RETURN_IF_ERROR_RESULT(append_text_value(
                        &block, column_index, std::to_string(6000 + segment_ordinal * 10 + row)));
            } else {
                return ResultError(
                        Status::InternalError("unexpected fixed partial-update column {}", name));
            }
        }
    }
    return block;
}

Result<Block> create_mow_history_block(const TabletSchemaSPtr& schema) {
    constexpr std::array<int, 4> keys {0, 1, 10, 11};
    Block block = schema->create_block();
    for (const int key : keys) {
        for (size_t column_index = 0; column_index < block.columns(); ++column_index) {
            const auto& name = block.get_by_position(column_index).name;
            if (name.starts_with("k_")) {
                RETURN_IF_ERROR_RESULT(append_partial_key_value(&block, column_index, name, key));
            } else if (name == "k" || name == "k1") {
                RETURN_IF_ERROR_RESULT(
                        append_text_value(&block, column_index, std::to_string(key)));
            } else if (name == "v1") {
                RETURN_IF_ERROR_RESULT(
                        append_text_value(&block, column_index, std::to_string(3000 + key)));
            } else if (name == "v2") {
                RETURN_IF_ERROR_RESULT(
                        append_text_value(&block, column_index, std::to_string(4000 + key)));
            } else if (name == SEQUENCE_COL) {
                RETURN_IF_ERROR_RESULT(
                        append_text_value(&block, column_index, std::to_string(5000 + key)));
            } else if (name == "v_variant_updated" || name == "v_variant_missing") {
                auto variant_column = block.get_by_position(column_index).column->assert_mutable();
                auto history_json = fmt::format("{{\"history\":true,\"key\":{}}}", key);
                if (name == "v_variant_missing") {
                    auto& nullable = assert_cast<ColumnNullable&>(*variant_column);
                    auto& variant = assert_cast<ColumnVariant&>(nullable.get_nested_column());
                    VariantUtil::insert_root_scalar_field(
                            variant,
                            Field::create_field<TYPE_STRING>(String(std::move(history_json))));
                    nullable.get_null_map_column().insert_value(0);
                } else {
                    auto& variant = assert_cast<ColumnVariant&>(*variant_column);
                    VariantUtil::insert_root_scalar_field(
                            variant,
                            Field::create_field<TYPE_STRING>(String(std::move(history_json))));
                }
            } else {
                block.get_by_position(column_index).column->assert_mutable()->insert_default();
            }
        }
    }
    return block;
}

struct ExternalInvertedIndexSignature {
    std::string field;
    std::map<std::string, std::vector<int32_t>> postings;
    std::vector<uint32_t> null_rows;
};

Status read_term_postings(lucene::index::IndexReader* index_reader, lucene::index::Term* term,
                          std::vector<int32_t>* row_ids) {
    segment_v2::TermDocsPtr term_docs;
    segment_v2::ErrorContext error_context;
    try {
        term_docs = segment_v2::make_term_doc_ptr(index_reader, term);
        while (term_docs->next()) {
            row_ids->push_back(term_docs->doc());
        }
    } catch (CLuceneError& error) {
        error_context.eptr = std::current_exception();
        error_context.err_msg =
                fmt::format("failed to decode inverted index postings: {}", error.what());
    }
    FINALLY({ FINALLY_CLOSE(term_docs); })
    return Status::OK();
}

Status read_null_bitmap_signature(segment_v2::DorisCompoundReader* compound_reader,
                                  const std::string& index_path_prefix,
                                  std::vector<uint32_t>* null_rows) {
    std::unique_ptr<lucene::store::IndexInput> null_bitmap_input;
    segment_v2::ErrorContext error_context;
    Status read_status = Status::OK();
    try {
        const auto* null_bitmap_file_name =
                segment_v2::InvertedIndexDescriptor::get_temporary_null_bitmap_file_name();
        if (compound_reader->fileExists(null_bitmap_file_name)) {
            CLuceneError open_error;
            if (!compound_reader->openInput(null_bitmap_file_name, null_bitmap_input, open_error,
                                            4096)) {
                read_status = Status::IOError("failed to open null bitmap in {}: {}",
                                              index_path_prefix, open_error.what());
            } else {
                const auto null_bitmap_size = null_bitmap_input->length();
                if (null_bitmap_size == 0) {
                    read_status = Status::Corruption("empty null bitmap in {}", index_path_prefix);
                } else {
                    std::string null_bitmap_bytes(null_bitmap_size, '\0');
                    null_bitmap_input->readBytes(
                            reinterpret_cast<uint8_t*>(null_bitmap_bytes.data()), null_bitmap_size);
                    const auto null_bitmap =
                            roaring::Roaring::read(null_bitmap_bytes.data(), false);
                    for (const auto row_id : null_bitmap) {
                        null_rows->push_back(row_id);
                    }
                }
            }
        }
    } catch (CLuceneError& error) {
        error_context.eptr = std::current_exception();
        error_context.err_msg = fmt::format("failed to decode null bitmap in {}: {}",
                                            index_path_prefix, error.what());
    }
    FINALLY({ FINALLY_CLOSE(null_bitmap_input); })
    return read_status;
}

Status read_external_inverted_index_signature(const std::string& index_path_prefix,
                                              InvertedIndexStorageFormatPB storage_format,
                                              const TabletIndex* index_meta,
                                              ExternalInvertedIndexSignature* signature) {
    auto index_file_reader = std::make_shared<segment_v2::IndexFileReader>(
            io::global_local_filesystem(), index_path_prefix, storage_format);
    RETURN_IF_ERROR(index_file_reader->init());
    auto compound_reader_result = index_file_reader->open(index_meta);
    if (!compound_reader_result.has_value()) {
        return compound_reader_result.error();
    }
    auto compound_reader = std::move(compound_reader_result).value();

    std::unique_ptr<lucene::index::IndexReader> index_reader;
    std::unique_ptr<lucene::index::TermEnum> terms;
    segment_v2::ErrorContext error_context;
    Status read_status = Status::OK();
    try {
        index_reader.reset(lucene::index::IndexReader::open(compound_reader.get()));
        terms.reset(index_reader->terms());
        while (terms->next()) {
            auto* term = terms->term(false);
            const auto term_field =
                    lucene_wcstoutf8string(term->field(), lenOfString(term->field()));
            if (signature->field.empty()) {
                signature->field = term_field;
            } else if (signature->field != term_field) {
                read_status = Status::Corruption("inverted index {} contains fields {} and {}",
                                                 index_path_prefix, signature->field, term_field);
                break;
            }
            const auto term_text = lucene_wcstoutf8string(term->text(), term->textLength());
            auto& row_ids = signature->postings[term_text];
            read_status = read_term_postings(index_reader.get(), term, &row_ids);
            if (!read_status.ok()) {
                break;
            }
        }

        if (read_status.ok()) {
            read_status = read_null_bitmap_signature(compound_reader.get(), index_path_prefix,
                                                     &signature->null_rows);
        }
    } catch (CLuceneError& error) {
        error_context.eptr = std::current_exception();
        error_context.err_msg = fmt::format("failed to decode inverted index {}: {}",
                                            index_path_prefix, error.what());
    }
    FINALLY({
        FINALLY_CLOSE(terms);
        FINALLY_CLOSE(index_reader);
    })
    return read_status;
}

Status verify_external_inverted_index_contents(const std::string& index_path_prefix,
                                               InvertedIndexStorageFormatPB storage_format,
                                               const TabletSchemaSPtr& schema,
                                               uint32_t segment_id) {
    const auto indexes = schema->inverted_indexs(schema->column(1));
    DORIS_CHECK_EQ(indexes.size(), 1);
    ExternalInvertedIndexSignature signature;
    RETURN_IF_ERROR(read_external_inverted_index_signature(index_path_prefix, storage_format,
                                                           indexes[0], &signature));

    ExternalInvertedIndexSignature expected;
    expected.field = std::to_string(schema->column(1).unique_id());
    for (size_t row = 0; row < kExternalIndexRows; ++row) {
        if (schema->column(1).is_nullable() && row % 37 == 0) {
            expected.null_rows.push_back(row);
        } else {
            expected.postings.emplace(fmt::format("inverted-segment-{}-row-{}", segment_id, row),
                                      std::vector<int32_t> {cast_set<int32_t>(row)});
        }
    }

    if (signature.field != expected.field) {
        return Status::Corruption("inverted index segment {} has field {}, expected {}", segment_id,
                                  signature.field, expected.field);
    }
    for (const auto& [term, expected_row_ids] : expected.postings) {
        const auto actual = signature.postings.find(term);
        if (actual == signature.postings.end()) {
            return Status::Corruption("inverted index segment {} is missing term {}", segment_id,
                                      term);
        }
        if (actual->second != expected_row_ids) {
            return Status::Corruption(
                    "inverted index segment {} term {} has row {}, expected {}", segment_id, term,
                    actual->second.empty() ? -1 : actual->second.front(), expected_row_ids.front());
        }
    }
    for (const auto& posting : signature.postings) {
        if (!expected.postings.contains(posting.first)) {
            return Status::Corruption("inverted index segment {} has unexpected term {}",
                                      segment_id, posting.first);
        }
    }
    if (signature.null_rows != expected.null_rows) {
        return Status::Corruption(
                "inverted index segment {} null rows differ: first actual {}, first expected {}",
                segment_id, signature.null_rows.empty() ? -1 : signature.null_rows.front(),
                expected.null_rows.empty() ? -1 : expected.null_rows.front());
    }
    return Status::OK();
}

Status verify_ann_index_contents(const std::string& index_path_prefix,
                                 InvertedIndexStorageFormatPB storage_format,
                                 const TabletSchemaSPtr& schema, uint32_t segment_id) {
    const auto* index_meta = schema->ann_index(schema->column(1));
    DORIS_CHECK(index_meta != nullptr);
    auto index_file_reader = std::make_shared<segment_v2::IndexFileReader>(
            io::global_local_filesystem(), index_path_prefix, storage_format);
    segment_v2::AnnIndexReader index_reader(index_meta, std::move(index_file_reader), "",
                                            segment_id, kExternalIndexRows);
    io::IOContext io_context;
    RETURN_IF_ERROR(index_reader.load_index(&io_context));

    std::vector<uint64_t> top1_row_ids;
    top1_row_ids.reserve(kExternalIndexRows);
    for (size_t row = 0; row < kExternalIndexRows; ++row) {
        const auto query_vector = ann_vector(segment_id, row);
        roaring::Roaring candidates;
        candidates.addRange(0, kExternalIndexRows);
        VectorSearchUserParams user_params;
        user_params.hnsw_ef_search = kExternalIndexRows;
        user_params.hnsw_check_relative_distance = false;
        user_params.hnsw_bounded_queue = false;
        segment_v2::AnnTopNParam param {
                .query_value = query_vector.data(),
                .query_value_size = query_vector.size(),
                .limit = 1,
                ._user_params = user_params,
                .roaring = &candidates,
                .rows_of_segment = kExternalIndexRows,
                .enable_result_cache = false,
        };
        segment_v2::AnnIndexStats stats;
        RETURN_IF_ERROR(index_reader.query(&io_context, &param, &stats));
        if (param.row_ids == nullptr || param.row_ids->size() != 1 || param.distance == nullptr) {
            return Status::Corruption("invalid ANN Top-1 result for segment {} row {}", segment_id,
                                      row);
        }
        const auto result_row_id = param.row_ids->front();
        if (candidates.cardinality() != 1 || !candidates.contains(result_row_id) ||
            param.distance[0] != 0) {
            return Status::Corruption(
                    "invalid ANN Top-1 signature for segment {} row {}: result row {}, distance {}",
                    segment_id, row, result_row_id, param.distance[0]);
        }
        top1_row_ids.push_back(result_row_id);
    }

    std::vector<uint64_t> expected_row_ids(kExternalIndexRows);
    std::iota(expected_row_ids.begin(), expected_row_ids.end(), 0);
    if (top1_row_ids != expected_row_ids) {
        return Status::Corruption("ANN Top-1 row-id signature changed for segment {}", segment_id);
    }
    return Status::OK();
}

Status verify_external_index_contents(const TabletSchemaSPtr& schema, uint32_t segment_id,
                                      const std::string& segment_path,
                                      const std::vector<std::string>& auxiliary_paths) {
    if ((!schema->has_inverted_index() && !schema->has_ann_index()) ||
        schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
        return Status::OK();
    }
    if (auxiliary_paths.size() != 1) {
        return Status::Corruption("segment {} produced {} compound index files", segment_id,
                                  auxiliary_paths.size());
    }

    const std::string index_path_prefix {
            segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(segment_path)};
    const auto storage_format = schema->get_inverted_index_storage_format();
    if (schema->has_inverted_index()) {
        return verify_external_inverted_index_contents(index_path_prefix, storage_format, schema,
                                                       segment_id);
    }
    DORIS_CHECK(schema->has_ann_index());
    return verify_ann_index_contents(index_path_prefix, storage_format, schema, segment_id);
}

struct AuxiliaryFileFingerprint {
    std::string filename;
    std::string bytes;

    bool operator==(const AuxiliaryFileFingerprint& rhs) const {
        // CLucene writes a wall-clock generation into the compound index. Its decoded portable
        // content is compared above; repeat-run equality only anchors the auxiliary manifest.
        return filename == rhs.filename;
    }
};

struct SegmentFingerprint {
    uint32_t segment_id;
    std::string bytes;
    std::vector<AuxiliaryFileFingerprint> auxiliary_files;

    bool operator==(const SegmentFingerprint& rhs) const {
        return segment_id == rhs.segment_id && bytes == rhs.bytes &&
               auxiliary_files == rhs.auxiliary_files;
    }
};

AuxiliaryFileFingerprint auxiliary_file_fingerprint(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    EXPECT_TRUE(input.is_open()) << path;
    std::string bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    return {.filename = std::filesystem::path(path).filename().string(), .bytes = std::move(bytes)};
}

SegmentFingerprint fingerprint(uint32_t segment_id, const std::string& path,
                               const std::vector<std::string>& auxiliary_paths) {
    auto segment_file = auxiliary_file_fingerprint(path);
    std::vector<AuxiliaryFileFingerprint> auxiliary_files;
    auxiliary_files.reserve(auxiliary_paths.size());
    for (const auto& auxiliary_path : auxiliary_paths) {
        auxiliary_files.push_back(auxiliary_file_fingerprint(auxiliary_path));
    }
    return {.segment_id = segment_id,
            .bytes = std::move(segment_file.bytes),
            .auxiliary_files = std::move(auxiliary_files)};
}

Result<std::string> read_file_bytes(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        return ResultError(Status::IOError("failed to open golden Segment file {}", path));
    }
    return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

Status verify_segment_manifest(std::string_view directory, std::string_view case_name,
                               std::string_view manifest_name,
                               const std::vector<uint32_t>& segment_ids) {
    std::set<std::string> expected_files;
    for (const auto segment_id : segment_ids) {
        expected_files.insert(fmt::format("segment_{}.dat", segment_id));
    }
    if (expected_files.size() != segment_ids.size()) {
        return Status::InternalError("duplicate Segment ids generated for {}", case_name);
    }

    std::vector<io::FileInfo> files;
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->list(directory, true, &files, &exists));
    if (!exists) {
        return Status::IOError("missing {} Segment directory for {}", manifest_name, case_name);
    }
    std::set<std::string> actual_files;
    for (const auto& file : files) {
        if (std::string_view(file.file_name).ends_with(".dat")) {
            actual_files.insert(file.file_name);
        }
    }
    if (actual_files != expected_files) {
        return Status::InternalError(
                "{} Segment manifest changed for {}: expected {} files, found {} files",
                manifest_name, case_name, expected_files.size(), actual_files.size());
    }
    return Status::OK();
}

Status verify_checked_in_golden_root_manifest() {
    std::error_code iteration_error;
    std::filesystem::recursive_directory_iterator iterator(kGoldenDir, iteration_error);
    if (iteration_error) {
        return Status::IOError("failed to list checked-in golden directory: {}",
                               iteration_error.message());
    }

    size_t case_count = 0;
    size_t segment_count = 0;
    const std::filesystem::recursive_directory_iterator end;
    for (; iterator != end; iterator.increment(iteration_error)) {
        if (iteration_error) {
            return Status::IOError("failed to list checked-in golden directory: {}",
                                   iteration_error.message());
        }
        if (iterator.depth() == 0 && iterator->is_directory(iteration_error)) {
            ++case_count;
        } else if (iterator->is_regular_file(iteration_error)) {
            const auto filename = iterator->path().filename().string();
            if (std::string_view(filename).starts_with("segment_") &&
                std::string_view(filename).ends_with(".dat")) {
                ++segment_count;
            }
        }
        if (iteration_error) {
            return Status::IOError("failed to inspect checked-in golden directory: {}",
                                   iteration_error.message());
        }
    }
    if (case_count != kExpectedGoldenCaseCount || segment_count != kExpectedGoldenSegmentCount) {
        return Status::InternalError(
                "checked-in golden root manifest changed: expected {} cases and {} Segments, "
                "found {} cases and {} Segments",
                kExpectedGoldenCaseCount, kExpectedGoldenSegmentCount, case_count, segment_count);
    }
    return Status::OK();
}

struct LogicalSegmentContents {
    Block block;
    std::vector<std::string> physical_column_metadata;
    std::vector<std::string> primary_keys;
    bool has_primary_key_index = false;
    int64_t primary_key_index_rows = 0;
    std::string min_primary_key;
    std::string max_primary_key;
};

void clear_page_pointer_offsets(google::protobuf::Message* message) {
    const auto* descriptor = message->GetDescriptor();
    const auto* reflection = message->GetReflection();
    if (descriptor == segment_v2::PagePointerPB::descriptor()) {
        const auto* offset = descriptor->FindFieldByName("offset");
        DORIS_CHECK(offset != nullptr);
        reflection->SetUInt64(message, offset, 0);
        return;
    }

    std::vector<const google::protobuf::FieldDescriptor*> fields;
    reflection->ListFields(*message, &fields);
    for (const auto* field : fields) {
        if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            continue;
        }
        if (field->is_repeated()) {
            for (int index = 0; index < reflection->FieldSize(*message, field); ++index) {
                clear_page_pointer_offsets(
                        reflection->MutableRepeatedMessage(message, field, index));
            }
        } else {
            clear_page_pointer_offsets(reflection->MutableMessage(message, field));
        }
    }
}

void normalize_physical_column_metadata(segment_v2::ColumnMetaPB* column_meta) {
    // Partial update may change the order in which top-level columns are written. Top-level order
    // and page offsets therefore are not part of this oracle, but every physical column and all
    // remaining format metadata must still be present and equal.
    clear_page_pointer_offsets(column_meta);
}

std::string physical_column_metadata_signature(segment_v2::ColumnMetaPB column_meta) {
    normalize_physical_column_metadata(&column_meta);
    return TabletSchema::deterministic_string_serialize(column_meta);
}

bool logical_field_equal(const Field& current, const Field& golden);

bool logical_field_vector_equal(const FieldVector& current, const FieldVector& golden) {
    if (current.size() != golden.size()) {
        return false;
    }
    for (size_t index = 0; index < current.size(); ++index) {
        if (!logical_field_equal(current[index], golden[index])) {
            return false;
        }
    }
    return true;
}

bool logical_variant_equal(const VariantMap& current, const VariantMap& golden) {
    if (current.size() != golden.size()) {
        return false;
    }
    auto current_entry = current.begin();
    auto golden_entry = golden.begin();
    for (; current_entry != current.end(); ++current_entry, ++golden_entry) {
        const auto& current_value = current_entry->second;
        const auto& golden_value = golden_entry->second;
        if (current_entry->first != golden_entry->first ||
            current_value.base_scalar_type_id != golden_value.base_scalar_type_id ||
            current_value.num_dimensions != golden_value.num_dimensions ||
            current_value.precision != golden_value.precision ||
            current_value.scale != golden_value.scale ||
            current_value.need_convert != golden_value.need_convert ||
            !logical_field_equal(current_value.field, golden_value.field)) {
            return false;
        }
    }
    return true;
}

bool logical_field_equal(const Field& current, const Field& golden) {
    if (current.get_type() != golden.get_type()) {
        return false;
    }
    switch (current.get_type()) {
    case TYPE_NULL:
        return true;
    case TYPE_ARRAY:
        return logical_field_vector_equal(current.get<TYPE_ARRAY>(), golden.get<TYPE_ARRAY>());
    case TYPE_STRUCT:
        return logical_field_vector_equal(current.get<TYPE_STRUCT>(), golden.get<TYPE_STRUCT>());
    case TYPE_MAP:
        return logical_field_vector_equal(current.get<TYPE_MAP>(), golden.get<TYPE_MAP>());
    case TYPE_VARIANT:
        return logical_variant_equal(current.get<TYPE_VARIANT>(), golden.get<TYPE_VARIANT>());
    case TYPE_JSONB: {
        const auto& current_jsonb = current.get<TYPE_JSONB>();
        const auto& golden_jsonb = golden.get<TYPE_JSONB>();
        return current_jsonb.get_size() == golden_jsonb.get_size() &&
               (current_jsonb.get_size() == 0 ||
                std::memcmp(current_jsonb.get_value(), golden_jsonb.get_value(),
                            current_jsonb.get_size()) == 0);
    }
    default:
        return current == golden;
    }
}

Result<LogicalSegmentContents> read_logical_segment(const std::string& path, uint32_t segment_id,
                                                    const TabletSchemaSPtr& schema,
                                                    const RowsetWriterContext& context,
                                                    bool read_primary_keys) {
    std::shared_ptr<segment_v2::Segment> segment;
    RETURN_IF_ERROR_RESULT(segment_v2::Segment::open(
            io::global_local_filesystem(), path, context.tablet_id, segment_id, context.rowset_id,
            schema, io::FileReaderOptions {}, &segment));

    std::vector<std::string> physical_column_metadata;
    RETURN_IF_ERROR_RESULT(segment->traverse_column_meta_pbs(
            [&](const segment_v2::ColumnMetaPB& persisted_column_meta) {
                physical_column_metadata.push_back(
                        physical_column_metadata_signature(persisted_column_meta));
            }));
    std::sort(physical_column_metadata.begin(), physical_column_metadata.end());

    std::vector<ColumnId> column_ids(schema->num_columns());
    std::iota(column_ids.begin(), column_ids.end(), 0);
    auto read_schema = std::make_shared<Schema>(schema->columns(), column_ids);
    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    read_options.tablet_schema = schema;
    std::unique_ptr<RowwiseIterator> iterator;
    RETURN_IF_ERROR_RESULT(segment->new_iterator(read_schema, read_options, &iterator));

    MutableBlock contents(schema->create_block());
    while (true) {
        Block batch = schema->create_block();
        auto status = iterator->next_batch(&batch);
        if (status.is<ErrorCode::END_OF_FILE>()) {
            break;
        }
        RETURN_IF_ERROR_RESULT(status);
        RETURN_IF_ERROR_RESULT(contents.add_rows(&batch, 0, batch.rows()));
    }

    std::vector<std::string> primary_keys;
    int64_t primary_key_index_rows = 0;
    std::string min_primary_key;
    std::string max_primary_key;
    if (read_primary_keys) {
        RETURN_IF_ERROR_RESULT(segment->load_pk_index_and_bf(&stats));
        const auto* primary_key_index = segment->get_primary_key_index();
        if (primary_key_index == nullptr) {
            return ResultError(Status::InternalError("missing primary-key index in {}", path));
        }
        primary_key_index_rows = primary_key_index->num_rows();
        if (primary_key_index_rows != static_cast<int64_t>(segment->num_rows())) {
            return ResultError(Status::InternalError(
                    "primary-key index row count mismatch in {}: index={}, segment={}", path,
                    primary_key_index_rows, segment->num_rows()));
        }
        min_primary_key = segment->min_key();
        max_primary_key = segment->max_key();

        size_t sequence_suffix_size = 0;
        if (schema->has_sequence_col()) {
            sequence_suffix_size =
                    static_cast<size_t>(schema->column(schema->sequence_col_idx()).length()) + 1;
        }
        primary_keys.resize(segment->num_rows());
        for (uint32_t row_id = 0; row_id < segment->num_rows(); ++row_id) {
            RETURN_IF_ERROR_RESULT(segment->read_key_by_rowid(row_id, &primary_keys[row_id]));
            const auto& encoded_key = primary_keys[row_id];
            if (encoded_key.size() < sequence_suffix_size) {
                return ResultError(Status::InternalError("invalid encoded primary key in {} row {}",
                                                         path, row_id));
            }

            // read_key_by_rowid() already removes a cluster-key rowid. Strip only the sequence
            // suffix before lookup_row_key(), which validates both Bloom membership and the index.
            const Slice normalized_key(encoded_key.data(),
                                       encoded_key.size() - sequence_suffix_size);
            RowLocation location;
            auto lookup_status =
                    segment->lookup_row_key(normalized_key, schema.get(), /*with_seq_col=*/false,
                                            /*with_rowid=*/false, &location, &stats);
            if (!lookup_status.ok()) {
                return ResultError(Status::InternalError(
                        "primary-key bloom/index lookup failed in {} row {}: {}", path, row_id,
                        lookup_status.to_string()));
            }
        }
    }
    return LogicalSegmentContents {.block = contents.to_block(),
                                   .physical_column_metadata = std::move(physical_column_metadata),
                                   .primary_keys = std::move(primary_keys),
                                   .has_primary_key_index = read_primary_keys,
                                   .primary_key_index_rows = primary_key_index_rows,
                                   .min_primary_key = std::move(min_primary_key),
                                   .max_primary_key = std::move(max_primary_key)};
}

Status compare_logical_segments(std::string_view case_name, uint32_t segment_id,
                                const LogicalSegmentContents& current,
                                const LogicalSegmentContents& golden) {
    if (current.physical_column_metadata != golden.physical_column_metadata) {
        return Status::InternalError("physical column metadata changed for {} segment {}",
                                     case_name, segment_id);
    }
    if (current.block.columns() != golden.block.columns() ||
        current.block.rows() != golden.block.rows()) {
        return Status::InternalError(
                "logical Segment shape changed for {} segment {}: current={}x{}, golden={}x{}",
                case_name, segment_id, current.block.rows(), current.block.columns(),
                golden.block.rows(), golden.block.columns());
    }
    for (size_t column_id = 0; column_id < current.block.columns(); ++column_id) {
        const auto& current_column = current.block.get_by_position(column_id);
        const auto& golden_column = golden.block.get_by_position(column_id);
        if (current_column.name != golden_column.name ||
            !current_column.type->equals(*golden_column.type)) {
            return Status::InternalError(
                    "logical Segment schema changed for {} segment {} column {}", case_name,
                    segment_id, column_id);
        }
        for (size_t row_id = 0; row_id < current.block.rows(); ++row_id) {
            bool values_equal = false;
            if (current_column.type->get_primitive_type() == TYPE_VARIANT) {
                Field current_value;
                Field golden_value;
                current_column.column->get(row_id, current_value);
                golden_column.column->get(row_id, golden_value);
                values_equal = logical_field_equal(current_value, golden_value);
            } else {
                values_equal = current_column.column->compare_at(row_id, row_id,
                                                                 *golden_column.column, 1) == 0;
            }
            if (!values_equal) {
                return Status::InternalError(
                        "logical Segment value changed for {} segment {} column {} ({}) row {}",
                        case_name, segment_id, column_id, current_column.name, row_id);
            }
        }
    }
    if (current.primary_keys != golden.primary_keys) {
        return Status::InternalError("primary key index changed for {} segment {}", case_name,
                                     segment_id);
    }
    if (current.has_primary_key_index != golden.has_primary_key_index ||
        current.primary_key_index_rows != golden.primary_key_index_rows ||
        current.min_primary_key != golden.min_primary_key ||
        current.max_primary_key != golden.max_primary_key) {
        return Status::InternalError("primary key index metadata changed for {} segment {}",
                                     case_name, segment_id);
    }
    return Status::OK();
}

class SegmentFlusherFormatTest : public testing::Test {
protected:
    Status verify_golden_segments(std::string_view case_name, const TabletSchemaSPtr& schema,
                                  const RowsetWriterContext& context,
                                  const LocalSegmentFileWriterCreator& file_writer_creator,
                                  const std::vector<uint32_t>& segment_ids) const {
        const char* golden_output_dir = std::getenv(kGoldenOutputDirEnv.data());
        if (golden_output_dir != nullptr && std::string_view(golden_output_dir).empty()) {
            return Status::InvalidArgument("{} must not be empty", kGoldenOutputDirEnv);
        }
        RETURN_IF_ERROR(verify_segment_manifest(file_writer_creator.directory(), case_name,
                                                "generated", segment_ids));
        if (case_name.ends_with("_repeat")) {
            return Status::OK();
        }
        if (golden_output_dir != nullptr) {
            std::error_code canonical_error;
            const auto output_root =
                    std::filesystem::weakly_canonical(golden_output_dir, canonical_error);
            if (canonical_error) {
                return Status::IOError("failed to resolve {}: {}", kGoldenOutputDirEnv,
                                       canonical_error.message());
            }
            const auto checked_in_root =
                    std::filesystem::weakly_canonical(kGoldenDir, canonical_error);
            if (canonical_error) {
                return Status::IOError("failed to resolve checked-in golden directory: {}",
                                       canonical_error.message());
            }
            if (io::LocalFileSystem::contain_path(checked_in_root, output_root)) {
                return Status::InvalidArgument("{} must be outside the checked-in golden directory",
                                               kGoldenOutputDirEnv);
            }
            const auto output_case_dir = fmt::format("{}/{}", output_root.string(), case_name);
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(
                    output_case_dir, /*failed_if_exists=*/true));
            for (const auto segment_id : segment_ids) {
                RETURN_IF_ERROR(io::global_local_filesystem()->copy_path(
                        file_writer_creator.path(segment_id),
                        fmt::format("{}/segment_{}.dat", output_case_dir, segment_id)));
            }
            return Status::OK();
        }

        RETURN_IF_ERROR(verify_segment_manifest(fmt::format("{}/{}", kGoldenDir, case_name),
                                                case_name, "golden", segment_ids));
        for (const auto segment_id : segment_ids) {
            const auto current_path = file_writer_creator.path(segment_id);
            const auto golden_path =
                    fmt::format("{}/{}/segment_{}.dat", kGoldenDir, case_name, segment_id);
            if (context.partial_update_info != nullptr &&
                context.partial_update_info->is_partial_update()) {
                // Partial update may intentionally change physical column/page order. Compare all
                // stored columns and the primary-key index after reading both Segment files.
                const bool read_primary_keys = context.enable_unique_key_merge_on_write;
                auto current_result = read_logical_segment(current_path, segment_id, schema,
                                                           context, read_primary_keys);
                if (!current_result.has_value()) {
                    return current_result.error();
                }
                auto golden_result = read_logical_segment(golden_path, segment_id, schema, context,
                                                          read_primary_keys);
                if (!golden_result.has_value()) {
                    return golden_result.error();
                }
                RETURN_IF_ERROR(compare_logical_segments(
                        case_name, segment_id, current_result.value(), golden_result.value()));
                continue;
            }

            auto current_result = read_file_bytes(current_path);
            if (!current_result.has_value()) {
                return current_result.error();
            }
            auto golden_result = read_file_bytes(golden_path);
            if (!golden_result.has_value()) {
                return golden_result.error();
            }
            if (current_result.value() != golden_result.value()) {
                return Status::InternalError(
                        "Segment bytes changed for {} segment {}: current size {}, golden size {}",
                        case_name, segment_id, current_result.value().size(),
                        golden_result.value().size());
            }
        }
        return Status::OK();
    }

    void SetUp() override {
        _saved_enable_vertical_writer = config::enable_vertical_segment_writer;
        _saved_compression_threshold_kb = config::segment_compression_threshold_kb;
        _saved_omp_threads_limit = config::omp_threads_limit;
        _saved_omp_max_threads = omp_get_max_threads();
        if (std::getenv(kGoldenOutputDirEnv.data()) == nullptr) {
            ASSERT_TRUE(verify_checked_in_golden_root_manifest().ok());
        }
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
        std::vector<StorePath> store_paths;
        store_paths.emplace_back(std::string(kTestDir), -1);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(store_paths);
        ASSERT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
    }

    void TearDown() override {
        config::enable_vertical_segment_writer = _saved_enable_vertical_writer;
        config::segment_compression_threshold_kb = _saved_compression_threshold_kb;
        config::omp_threads_limit = _saved_omp_threads_limit;
        omp_set_num_threads(_saved_omp_max_threads);
        ExecEnv::GetInstance()->set_tmp_file_dir(nullptr);
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    Result<std::vector<SegmentFingerprint>> flush_blocks(
            std::string_view case_name, const TabletSchemaSPtr& schema, std::vector<Block> blocks,
            bool enable_vertical_writer, int32_t compression_threshold_kb = 0,
            DataWriteType write_type = DataWriteType::TYPE_DIRECT,
            bool enable_unique_key_merge_on_write = false,
            const std::function<void(RowsetWriterContext&)>& configure_context = {}) {
        const auto directory = fmt::format("{}/{}", kTestDir, case_name);
        RETURN_IF_ERROR_RESULT(io::global_local_filesystem()->create_directory(directory));

        for (auto& block : blocks) {
            auto columns = block.get_columns();
            MutableColumns cloned_columns;
            cloned_columns.reserve(columns.size());
            for (const auto& column : columns) {
                cloned_columns.push_back(column->clone_resized(block.rows()));
            }
            block.set_columns(std::move(cloned_columns));
        }

        auto file_writer_creator =
                std::make_shared<LocalSegmentFileWriterCreator>(directory, schema);
        auto segment_collector = std::make_shared<TestSegmentCollector>();
        RowsetWriterContext context;
        context.tablet_schema = schema;
        context.tablet_path = directory;
        context.tablet_id = 10001;
        context.rowset_id.init(10002);
        context.max_rows_per_segment = 1024;
        context.write_type = write_type;
        context.enable_unique_key_merge_on_write = enable_unique_key_merge_on_write;
        context.file_writer_creator = file_writer_creator;
        context.segment_collector = segment_collector;
        if (configure_context) {
            configure_context(context);
        }

        config::enable_vertical_segment_writer = enable_vertical_writer;
        config::segment_compression_threshold_kb = compression_threshold_kb;
        std::vector<uint32_t> expected_segment_ids(blocks.size());
        std::iota(expected_segment_ids.begin(), expected_segment_ids.end(), 0);

        auto* sync_point = SyncPoint::get_instance();
        const bool sync_point_was_enabled = sync_point->get_enable();
        sync_point->enable_processing();
        Defer restore_sync_point {[sync_point, sync_point_was_enabled] {
            if (!sync_point_was_enabled) {
                sync_point->disable_processing();
            }
        }};
        std::vector<uint32_t> vertical_segment_ids;
        SyncPoint::CallbackGuard vertical_writer_guard;
        sync_point->set_call_back(
                "SegmentFlusher::flush_vertical_segment_writer",
                [&vertical_segment_ids](auto&& args) {
                    auto* segment_id = try_any_cast<uint32_t*>(args[0]);
                    vertical_segment_ids.push_back(*segment_id);
                },
                &vertical_writer_guard);

        SegmentFileCollection segment_files;
        InvertedIndexFileCollection index_files;
        SegmentFlusher flusher(context, segment_files, index_files);
        for (size_t segment_id = 0; segment_id < blocks.size(); ++segment_id) {
            RETURN_IF_ERROR_RESULT(flusher.flush_single_block(&blocks[segment_id],
                                                              static_cast<int32_t>(segment_id)));
        }
        RETURN_IF_ERROR_RESULT(flusher.close());

        if (segment_collector->segment_ids != expected_segment_ids) {
            return ResultError(Status::InternalError("unexpected collected segment ids"));
        }
        const auto expected_vertical_segment_ids =
                enable_vertical_writer ? expected_segment_ids : std::vector<uint32_t> {};
        if (vertical_segment_ids != expected_vertical_segment_ids) {
            return ResultError(Status::InternalError(
                    "unexpected Segment writer path for {}: vertical={}, expected {} vertical "
                    "flushes, observed {}",
                    case_name, enable_vertical_writer, expected_vertical_segment_ids.size(),
                    vertical_segment_ids.size()));
        }
        std::vector<SegmentFingerprint> fingerprints;
        fingerprints.reserve(blocks.size());
        for (size_t segment_id = 0; segment_id < blocks.size(); ++segment_id) {
            const auto segment_path = file_writer_creator->path(segment_id);
            const auto auxiliary_paths = file_writer_creator->index_paths(segment_id);
            RETURN_IF_ERROR_RESULT(verify_external_index_contents(schema, segment_id, segment_path,
                                                                  auxiliary_paths));
            fingerprints.push_back(fingerprint(segment_id, segment_path, auxiliary_paths));
        }
        RETURN_IF_ERROR_RESULT(verify_golden_segments(case_name, schema, context,
                                                      *file_writer_creator, expected_segment_ids));
        return fingerprints;
    }

    Result<std::vector<SegmentFingerprint>> add_block_with_segment_creator(
            std::string_view case_name, const TabletSchemaSPtr& schema, Block block,
            int32_t max_rows_per_segment, int32_t compression_threshold_kb,
            DataWriteType write_type) {
        const auto directory = fmt::format("{}/{}", kTestDir, case_name);
        RETURN_IF_ERROR_RESULT(io::global_local_filesystem()->create_directory(directory));

        auto columns = block.get_columns();
        MutableColumns cloned_columns;
        cloned_columns.reserve(columns.size());
        for (const auto& column : columns) {
            cloned_columns.push_back(column->clone_resized(block.rows()));
        }
        block.set_columns(std::move(cloned_columns));

        auto file_writer_creator =
                std::make_shared<LocalSegmentFileWriterCreator>(directory, schema);
        auto segment_collector = std::make_shared<TestSegmentCollector>();
        RowsetWriterContext context;
        context.tablet_schema = schema;
        context.tablet_path = directory;
        context.tablet_id = 10001;
        context.rowset_id.init(10002);
        context.max_rows_per_segment = max_rows_per_segment;
        context.write_type = write_type;
        context.file_writer_creator = file_writer_creator;
        context.segment_collector = segment_collector;

        config::segment_compression_threshold_kb = compression_threshold_kb;
        SegmentFileCollection segment_files;
        InvertedIndexFileCollection index_files;
        SegmentCreator creator(context, segment_files, index_files);
        RETURN_IF_ERROR_RESULT(creator.add_block(&block));
        RETURN_IF_ERROR_RESULT(creator.close());

        std::vector<SegmentFingerprint> fingerprints;
        fingerprints.reserve(segment_collector->segment_ids.size());
        for (const auto segment_id : segment_collector->segment_ids) {
            fingerprints.push_back(fingerprint(segment_id, file_writer_creator->path(segment_id),
                                               file_writer_creator->index_paths(segment_id)));
        }
        RETURN_IF_ERROR_RESULT(verify_golden_segments(
                case_name, schema, context, *file_writer_creator, segment_collector->segment_ids));
        return fingerprints;
    }

    Result<std::vector<SegmentFingerprint>> add_block_with_segment_creator_twice(
            std::string_view case_name, const TabletSchemaSPtr& schema, Block block,
            int32_t max_rows_per_segment, int32_t compression_threshold_kb,
            DataWriteType write_type) {
        auto first = add_block_with_segment_creator(case_name, schema, block, max_rows_per_segment,
                                                    compression_threshold_kb, write_type);
        if (!first.has_value()) {
            return unexpected(first.error());
        }
        auto second = add_block_with_segment_creator(fmt::format("{}_repeat", case_name), schema,
                                                     std::move(block), max_rows_per_segment,
                                                     compression_threshold_kb, write_type);
        if (!second.has_value()) {
            return unexpected(second.error());
        }
        if (first.value() != second.value()) {
            return ResultError(
                    Status::InternalError("segment bytes are not deterministic for {}", case_name));
        }
        return first;
    }

    Result<std::vector<SegmentFingerprint>> flush_twice(
            std::string_view case_name, const TabletSchemaSPtr& schema, std::vector<Block> blocks,
            bool enable_vertical_writer, int32_t compression_threshold_kb = 0,
            DataWriteType write_type = DataWriteType::TYPE_DIRECT,
            bool enable_unique_key_merge_on_write = false,
            const std::function<void(RowsetWriterContext&)>& configure_context = {}) {
        auto first = flush_blocks(case_name, schema, blocks, enable_vertical_writer,
                                  compression_threshold_kb, write_type,
                                  enable_unique_key_merge_on_write, configure_context);
        if (!first.has_value()) {
            return unexpected(first.error());
        }
        auto second = flush_blocks(fmt::format("{}_repeat", case_name), schema, std::move(blocks),
                                   enable_vertical_writer, compression_threshold_kb, write_type,
                                   enable_unique_key_merge_on_write, configure_context);
        if (!second.has_value()) {
            return unexpected(second.error());
        }
        if (first.value() != second.value()) {
            return ResultError(
                    Status::InternalError("segment bytes are not deterministic for {}", case_name));
        }
        return first;
    }

    Result<Block> read_direct_segment_block(std::string_view source_name,
                                            const TabletSchemaSPtr& schema, Block block) {
        const auto source_case_name = fmt::format("{}_source_repeat", source_name);
        auto source_result = flush_blocks(source_case_name, schema, {std::move(block)}, false, 0,
                                          DataWriteType::TYPE_DIRECT);
        if (!source_result.has_value()) {
            return unexpected(source_result.error());
        }
        if (source_result.value().size() != 1) {
            return ResultError(Status::InternalError("direct source for {} produced {} Segments",
                                                     source_name, source_result.value().size()));
        }

        RowsetWriterContext read_context;
        read_context.tablet_schema = schema;
        read_context.tablet_id = 10001;
        read_context.rowset_id.init(10002);
        auto contents_result =
                read_logical_segment(fmt::format("{}/{}/segment_0.dat", kTestDir, source_case_name),
                                     0, schema, read_context, false);
        if (!contents_result.has_value()) {
            return unexpected(contents_result.error());
        }
        auto contents = std::move(contents_result).value();

        bool found_row_store = false;
        for (size_t column_id = 0; column_id < schema->num_columns(); ++column_id) {
            const auto& schema_column = schema->column(column_id);
            const auto& block_column = *contents.block.get_by_position(column_id).column;
            if (schema_column.is_row_store_column()) {
                found_row_store = true;
                const auto& row_store = assert_cast<const ColumnString&>(block_column);
                for (size_t row_id = 0; row_id < contents.block.rows(); ++row_id) {
                    if (row_store.get_data_at(row_id).size == 0) {
                        return ResultError(Status::InternalError(
                                "direct source for {} has an empty RowStore at row {}", source_name,
                                row_id));
                    }
                }
            } else if (schema_column.is_variant_type()) {
                const auto& variant = assert_cast<const ColumnVariant&>(block_column);
                if (variant.is_scalar_variant() || !variant.is_finalized()) {
                    return ResultError(Status::InternalError(
                            "direct source for {} did not materialize its Variant column",
                            source_name));
                }
            }
        }
        DORIS_CHECK(found_row_store);
        return std::move(contents.block);
    }

private:
    bool _saved_enable_vertical_writer = false;
    int32_t _saved_compression_threshold_kb = 0;
    int32_t _saved_omp_threads_limit = -1;
    int _saved_omp_max_threads = 1;
};

class SegmentFlusherTransformFormatTest : public SegmentFlusherFormatTest {
protected:
    void SetUp() override {
        SegmentFlusherFormatTest::SetUp();
        _saved_storage_root_path = config::storage_root_path;
        _storage_root_path =
                std::filesystem::absolute(fmt::format("{}/storage", kTestDir)).string();
        config::storage_root_path = _storage_root_path;
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_storage_root_path).ok());

        EngineOptions options;
        options.store_paths.emplace_back(_storage_root_path, -1);
        auto engine = std::make_unique<StorageEngine>(options);
        _engine = engine.get();
        ASSERT_TRUE(_engine->open().ok());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        _data_dir = std::make_unique<DataDir>(*_engine, _storage_root_path);
        ASSERT_TRUE(_data_dir->update_capacity().ok());
    }

    void TearDown() override {
        _tablets.clear();
        _data_dir.reset();
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        config::storage_root_path = _saved_storage_root_path;
        SegmentFlusherFormatTest::TearDown();
    }

    TabletSharedPtr make_mow_tablet(const TabletSchemaSPtr& schema, int64_t tablet_id) {
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_tablet_id = tablet_id;
        DORIS_CHECK(tablet_meta->set_partition_id(10).ok());
        tablet_meta->_schema = schema;
        tablet_meta->_enable_unique_key_merge_on_write = true;
        return std::make_shared<Tablet>(*_engine, tablet_meta, _data_dir.get(),
                                        fmt::format("format_ut_{}", tablet_id));
    }

    std::shared_ptr<MowContext> make_mow_context(
            int64_t tablet_id, const std::vector<RowsetSharedPtr>& rowsets = {}) const {
        auto rowset_ids = std::make_shared<RowsetIdUnorderedSet>();
        for (const auto& rowset : rowsets) {
            rowset_ids->insert(rowset->rowset_id());
        }
        return std::make_shared<MowContext>(2, 1, std::move(rowset_ids), rowsets,
                                            std::make_shared<DeleteBitmap>(tablet_id));
    }

    Result<RowsetSharedPtr> write_mow_history(const TabletSharedPtr& tablet,
                                              const TabletSchemaSPtr& schema,
                                              int64_t rowset_numeric_id) {
        RowsetWriterContext context;
        context.rowset_id.init(rowset_numeric_id);
        context.tablet_id = tablet->tablet_id();
        context.tablet_schema_hash = 1;
        context.partition_id = 10;
        context.rowset_type = BETA_ROWSET;
        context.tablet_path = _storage_root_path;
        context.rowset_state = VISIBLE;
        context.segments_overlap = NONOVERLAPPING;
        context.max_rows_per_segment = 1024;
        context.tablet_schema = schema;
        context.tablet = tablet;
        context.data_dir = tablet->data_dir();
        context.version = {2, 2};
        context.enable_unique_key_merge_on_write = true;
        context.write_type = DataWriteType::TYPE_DIRECT;

        auto writer_result = RowsetFactory::create_rowset_writer(*_engine, context, false);
        if (!writer_result.has_value()) {
            return unexpected(writer_result.error());
        }
        auto writer = std::move(writer_result).value();
        auto block_result = create_mow_history_block(schema);
        if (!block_result.has_value()) {
            return unexpected(block_result.error());
        }
        Block block = std::move(block_result).value();
        RETURN_IF_ERROR_RESULT(writer->add_block(&block));
        RETURN_IF_ERROR_RESULT(writer->flush());
        RowsetSharedPtr rowset;
        RETURN_IF_ERROR_RESULT(writer->build(rowset));
        return rowset;
    }

    TabletSharedPtr create_binlog_tablet(int64_t tablet_id, bool enable_mow,
                                         bool include_before_columns = false,
                                         bool with_sequence = false) {
        auto request = testutil::create_tablet_request(
                tablet_id, 270068390, 10001, 1,
                enable_mow ? TKeysType::UNIQUE_KEYS : TKeysType::DUP_KEYS,
                {{"k1", TPrimitiveType::INT, true},
                 {"v1", TPrimitiveType::INT, false, true, TAggregationType::NONE},
                 {"v2", TPrimitiveType::BIGINT, false, true, TAggregationType::NONE}});
        if (enable_mow) {
            request.tablet_schema.columns.push_back(testutil::create_tablet_column(
                    {DELETE_SIGN, TPrimitiveType::TINYINT, false, true, TAggregationType::NONE}));
            request.tablet_schema.__set_delete_sign_idx(
                    static_cast<int32_t>(request.tablet_schema.columns.size()) - 1);
            request.tablet_schema.columns.push_back(testutil::create_tablet_column(
                    {VERSION_COL, TPrimitiveType::BIGINT, false, true, TAggregationType::NONE}));
            request.tablet_schema.__set_version_col_idx(
                    static_cast<int32_t>(request.tablet_schema.columns.size()) - 1);
        }
        request.tablet_schema.columns.push_back(testutil::create_tablet_column(
                {COMMIT_TSO_COL, TPrimitiveType::BIGINT, false, true, TAggregationType::NONE}));
        request.tablet_schema.__set_commit_tso_col_idx(
                static_cast<int32_t>(request.tablet_schema.columns.size()) - 1);
        if (with_sequence) {
            auto sequence_column = testutil::create_tablet_column(
                    {SEQUENCE_COL, TPrimitiveType::INT, false, true, TAggregationType::NONE, true});
            request.tablet_schema.columns.push_back(std::move(sequence_column));
            request.tablet_schema.__set_sequence_col_idx(
                    static_cast<int32_t>(request.tablet_schema.columns.size()) - 1);
        }
        auto find_source_column = [&](std::string_view name) -> TColumn& {
            auto& columns = request.tablet_schema.columns;
            const auto it =
                    std::find_if(columns.begin(), columns.end(),
                                 [&](const TColumn& column) { return column.column_name == name; });
            DORIS_CHECK(it != columns.end());
            return *it;
        };
        find_source_column("v2").__set_is_allow_null(true);
        if (enable_mow) {
            find_source_column(DELETE_SIGN).__set_visible(false);
            find_source_column(DELETE_SIGN).__set_default_value("0");
            find_source_column(VERSION_COL).__set_visible(false);
            find_source_column(VERSION_COL).__set_default_value("0");
        }
        find_source_column(COMMIT_TSO_COL).__set_visible(false);
        find_source_column(COMMIT_TSO_COL).__set_default_value("0");
        if (with_sequence) {
            find_source_column(SEQUENCE_COL).__set_visible(false);
        }
        if (enable_mow) {
            request.__set_enable_unique_key_merge_on_write(true);
        }
        testutil::enable_row_binlog(&request);
        auto erase_binlog_column = [&](std::string_view name) {
            auto& columns = request.row_binlog_schema.columns;
            const auto it =
                    std::find_if(columns.begin(), columns.end(),
                                 [&](const TColumn& column) { return column.column_name == name; });
            DORIS_CHECK(it != columns.end());
            columns.erase(it);
        };
        if (enable_mow) {
            erase_binlog_column(DELETE_SIGN);
            request.row_binlog_schema.__set_delete_sign_idx(-1);
            erase_binlog_column(VERSION_COL);
            request.row_binlog_schema.__set_version_col_idx(-1);
        }
        erase_binlog_column(COMMIT_TSO_COL);
        request.row_binlog_schema.__set_commit_tso_col_idx(-1);
        if (with_sequence) {
            erase_binlog_column(SEQUENCE_COL);
            request.row_binlog_schema.__set_sequence_col_idx(-1);
        }
        auto find_binlog_column = [&](std::string_view name) -> TColumn& {
            auto& columns = request.row_binlog_schema.columns;
            const auto it =
                    std::find_if(columns.begin(), columns.end(),
                                 [&](const TColumn& column) { return column.column_name == name; });
            DORIS_CHECK(it != columns.end());
            return *it;
        };
        find_binlog_column("v1").__set_is_allow_null(true);
        find_binlog_column("v2").__set_is_allow_null(true);
        find_binlog_column(BINLOG_TSO_COL).__set_is_allow_null(true);
        find_binlog_column(BINLOG_TSO_COL).__set_visible(false);
        find_binlog_column(BINLOG_LSN_COL).__set_visible(false);
        find_binlog_column(BINLOG_OP_COL).__set_visible(false);
        int32_t binlog_tso_idx = static_cast<int32_t>(request.row_binlog_schema.columns.size()) -
                                 kRowBinlogSystemColumnCount;
        if (include_before_columns) {
            auto before_v1 = find_binlog_column("v1");
            before_v1.__set_column_name("__BEFORE__v1__");
            before_v1.__set_is_key(false);
            before_v1.__set_is_allow_null(true);
            auto before_v2 = find_binlog_column("v2");
            before_v2.__set_column_name("__BEFORE__v2__");
            before_v2.__set_is_key(false);
            before_v2.__set_is_allow_null(true);
            request.row_binlog_schema.columns.insert(
                    request.row_binlog_schema.columns.begin() + binlog_tso_idx,
                    std::move(before_v2));
            request.row_binlog_schema.columns.insert(
                    request.row_binlog_schema.columns.begin() + binlog_tso_idx,
                    std::move(before_v1));
            binlog_tso_idx += 2;
        }
        request.row_binlog_schema.__set_binlog_tso_idx(binlog_tso_idx);
        request.row_binlog_schema.__set_binlog_lsn_idx(binlog_tso_idx + 1);
        request.row_binlog_schema.__set_binlog_op_idx(binlog_tso_idx + 2);

        RuntimeProfile profile("SegmentFlusherTransformFormatTest");
        EXPECT_TRUE(_engine->create_tablet(request, &profile).ok());
        auto tablet = _engine->tablet_manager()->get_tablet(tablet_id);
        EXPECT_NE(tablet, nullptr);
        _tablets.push_back(tablet);
        return tablet;
    }

    void configure_partial_update_context(
            RowsetWriterContext& context, const TabletSharedPtr& tablet,
            const std::shared_ptr<PartialUpdateInfo>& partial_update_info,
            const std::vector<RowsetSharedPtr>& history = {}) const {
        context.tablet_id = tablet->tablet_id();
        context.tablet = tablet;
        context.data_dir = tablet->data_dir();
        context.partial_update_info = partial_update_info;
        context.mow_context = make_mow_context(tablet->tablet_id(), history);
        context.is_transient_rowset_writer = false;
    }

    void configure_row_binlog_context(
            RowsetWriterContext& context, const TabletSharedPtr& tablet,
            const std::shared_ptr<PartialUpdateInfo>& partial_update_info = nullptr,
            const std::vector<RowsetSharedPtr>& history = {}, bool need_before = false) const {
        context.tablet_id = tablet->tablet_id();
        context.tablet = tablet;
        context.data_dir = tablet->data_dir();
        context.is_transient_rowset_writer = partial_update_info == nullptr && history.empty();
        context.partial_update_info = partial_update_info;
        if (partial_update_info != nullptr || !history.empty()) {
            context.mow_context = make_mow_context(tablet->tablet_id(), history);
        }
        context.write_binlog_opt().enable = true;
        context.write_binlog_opt().set_need_before(need_before);
        auto& options = context.write_binlog_opt().write_binlog_config();
        options.source.tablet_schema = tablet->tablet_schema();
        options.source.partial_update_info = partial_update_info;
        options.source.mow_context = context.mow_context;
        options.source.is_transient_rowset_writer = context.is_transient_rowset_writer;
        options.source.source_write_type = DataWriteType::TYPE_DIRECT;
        for (int64_t segment_id = 0; segment_id < 2; ++segment_id) {
            auto lsn_ids = std::make_shared<std::vector<int64_t>>();
            for (int64_t row = 0; row < 3; ++row) {
                lsn_ids->push_back(1000 + segment_id * 100 + row);
            }
            options.insert_seg_lsn(segment_id, std::move(lsn_ids));
        }
    }

    StorageEngine* _engine = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    std::vector<TabletSharedPtr> _tablets;
    std::string _saved_storage_root_path;
    std::string _storage_root_path;
};

TEST_F(SegmentFlusherFormatTest, VariantLogicalComparisonPreservesScalarTypes) {
    VariantMap boolean_object;
    boolean_object.try_emplace(
            PathInData("different"),
            FieldWithDataType {.field = Field::create_field<TYPE_BOOLEAN>(UInt8(1)),
                               .base_scalar_type_id = TYPE_BOOLEAN});
    VariantMap integer_object;
    integer_object.try_emplace(PathInData("different"),
                               FieldWithDataType {.field = Field::create_field<TYPE_INT>(Int32(1)),
                                                  .base_scalar_type_id = TYPE_INT});

    auto boolean_variant = Field::create_field<TYPE_VARIANT>(std::move(boolean_object));
    auto integer_variant = Field::create_field<TYPE_VARIANT>(std::move(integer_object));
    EXPECT_TRUE(logical_field_equal(boolean_variant, boolean_variant));
    EXPECT_FALSE(logical_field_equal(boolean_variant, integer_variant));
}

TEST_F(SegmentFlusherFormatTest, PhysicalMetadataOracleIgnoresOnlyPageOffsets) {
    segment_v2::ColumnMetaPB column_meta;
    column_meta.set_column_id(7);
    column_meta.set_unique_id(42);
    column_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_STRING));
    column_meta.set_length(64);
    column_meta.set_encoding(segment_v2::DICT_ENCODING);
    column_meta.mutable_dict_page()->set_offset(100);
    column_meta.mutable_dict_page()->set_size(200);

    auto relocated = column_meta;
    relocated.mutable_dict_page()->set_offset(300);
    EXPECT_EQ(physical_column_metadata_signature(column_meta),
              physical_column_metadata_signature(relocated));

    auto rebound = column_meta;
    rebound.set_column_id(8);
    EXPECT_NE(physical_column_metadata_signature(column_meta),
              physical_column_metadata_signature(rebound));

    auto resized = column_meta;
    resized.mutable_dict_page()->set_size(201);
    EXPECT_NE(physical_column_metadata_signature(column_meta),
              physical_column_metadata_signature(resized));

    auto recoded = column_meta;
    recoded.set_encoding(segment_v2::PLAIN_ENCODING);
    EXPECT_NE(physical_column_metadata_signature(column_meta),
              physical_column_metadata_signature(recoded));
}

TEST_F(SegmentFlusherFormatTest, WideKeyTableModelsKeepTheirSegmentBytes) {
    auto run_case = [this](std::string_view name, const WideKeySchemaOptions& options,
                           bool enable_vertical_writer) -> testing::AssertionResult {
        auto schema = create_wide_key_schema(options);
        std::vector<Block> blocks;
        for (int segment_id = 0; segment_id < 2; ++segment_id) {
            auto block_result = create_wide_key_block(schema, options, segment_id);
            if (!block_result.has_value()) {
                return testing::AssertionFailure() << block_result.error();
            }
            blocks.push_back(std::move(block_result).value());
        }
        const auto case_name =
                fmt::format("{}_{}_{}_{}", name,
                            options.storage_format == TABLET_STORAGE_FORMAT_V2 ? "v2" : "v3",
                            options.nullable_keys ? "nullable" : "not_nullable",
                            enable_vertical_writer ? "vertical" : "horizontal");
        auto result = flush_twice(case_name, schema, std::move(blocks), enable_vertical_writer, 0,
                                  DataWriteType::TYPE_DIRECT, options.enable_mow);
        if (!result.has_value()) {
            return testing::AssertionFailure() << result.error();
        }
        return testing::AssertionSuccess();
    };

    struct ModelCase {
        std::string_view name;
        KeysType keys_type;
        bool enable_mow;
        bool with_cluster_key;
        const TypeCase* sequence_type;
        TabletStorageFormatPB horizontal_storage_format;
        bool horizontal_nullable_keys;
    };
    const std::array model_cases {
            ModelCase {"all_keys_dup", DUP_KEYS, false, false, nullptr, TABLET_STORAGE_FORMAT_V2,
                       false},
            ModelCase {"all_keys_agg", AGG_KEYS, false, false, nullptr, TABLET_STORAGE_FORMAT_V3,
                       false},
            ModelCase {"all_keys_unique_mor", UNIQUE_KEYS, false, false, nullptr,
                       TABLET_STORAGE_FORMAT_V2, true},
            ModelCase {"all_keys_unique_mow", UNIQUE_KEYS, true, false, nullptr,
                       TABLET_STORAGE_FORMAT_V3, true},
            ModelCase {"all_keys_unique_mow_sequence", UNIQUE_KEYS, true, false, &kKeyTypes[3],
                       TABLET_STORAGE_FORMAT_V2, false},
            ModelCase {"all_keys_unique_mow_cluster", UNIQUE_KEYS, true, true, nullptr,
                       TABLET_STORAGE_FORMAT_V3, false},
            ModelCase {"all_keys_unique_mow_cluster_sequence", UNIQUE_KEYS, true, true,
                       &kKeyTypes[3], TABLET_STORAGE_FORMAT_V2, true},
    };
    for (const auto& model : model_cases) {
        const WideKeySchemaOptions horizontal_options {
                .keys_type = model.keys_type,
                .storage_format = model.horizontal_storage_format,
                .nullable_keys = model.horizontal_nullable_keys,
                .enable_mow = model.enable_mow,
                .with_cluster_key = model.with_cluster_key,
                .sequence_type = model.sequence_type,
        };
        const WideKeySchemaOptions vertical_options {
                .keys_type = model.keys_type,
                .storage_format = model.horizontal_storage_format == TABLET_STORAGE_FORMAT_V2
                                          ? TABLET_STORAGE_FORMAT_V3
                                          : TABLET_STORAGE_FORMAT_V2,
                .nullable_keys = !model.horizontal_nullable_keys,
                .enable_mow = model.enable_mow,
                .with_cluster_key = model.with_cluster_key,
                .sequence_type = model.sequence_type,
        };
        ASSERT_TRUE(run_case(model.name, horizontal_options, false));
        ASSERT_TRUE(run_case(model.name, vertical_options, true));
    }

    const WideKeySchemaOptions without_key {
            .keys_type = DUP_KEYS,
            .storage_format = TABLET_STORAGE_FORMAT_V3,
            .nullable_keys = false,
            .enable_mow = false,
            .without_key = true,
    };
    ASSERT_TRUE(run_case("dup_without_key", without_key, false));

    for (const auto& sequence_type : kSequenceTypes) {
        if (sequence_type.name == "int") {
            continue;
        }
        const WideKeySchemaOptions options {
                .keys_type = UNIQUE_KEYS,
                .storage_format = TABLET_STORAGE_FORMAT_V2,
                .nullable_keys = false,
                .enable_mow = true,
                .sequence_type = &sequence_type,
        };
        ASSERT_TRUE(
                run_case(fmt::format("all_keys_sequence_{}", sequence_type.name), options, false));
    }
}

TEST_F(SegmentFlusherFormatTest, AllSupportedScalarValueTypesKeepTheirSegmentBytes) {
    struct ScalarCase {
        TabletStorageFormatPB storage_format;
        bool nullable_values;
        bool with_bloom_filters;
        bool enable_vertical_writer;
    };
    // Four pairwise cases cover storage format, nullability, writer implementation, and classic
    // Bloom filter generation across every scalar type that supports it.
    constexpr std::array scalar_cases {
            ScalarCase {TABLET_STORAGE_FORMAT_V2, false, false, false},
            ScalarCase {TABLET_STORAGE_FORMAT_V2, true, false, true},
            ScalarCase {TABLET_STORAGE_FORMAT_V3, true, true, false},
            ScalarCase {TABLET_STORAGE_FORMAT_V3, false, false, true},
    };
    for (const auto& scalar_case : scalar_cases) {
        auto schema = create_all_scalar_value_schema(scalar_case.storage_format,
                                                     scalar_case.nullable_values,
                                                     scalar_case.with_bloom_filters);
        std::vector<Block> blocks;
        for (int segment_id = 0; segment_id < 2; ++segment_id) {
            auto block_result = create_all_scalar_value_block(schema, segment_id);
            ASSERT_TRUE(block_result.has_value()) << block_result.error();
            blocks.push_back(std::move(block_result).value());
        }
        const auto case_name =
                fmt::format("all_scalar_values_{}_{}_{}_{}",
                            scalar_case.storage_format == TABLET_STORAGE_FORMAT_V2 ? "v2" : "v3",
                            scalar_case.nullable_values ? "nullable" : "not_nullable",
                            scalar_case.with_bloom_filters ? "with_bloom" : "without_bloom",
                            scalar_case.enable_vertical_writer ? "vertical" : "horizontal");
        auto first = flush_blocks(case_name, schema, blocks, scalar_case.enable_vertical_writer);
        ASSERT_TRUE(first.has_value()) << first.error();
        auto second = flush_blocks(case_name + "_repeat", schema, std::move(blocks),
                                   scalar_case.enable_vertical_writer);
        ASSERT_TRUE(second.has_value()) << second.error();
        ASSERT_EQ(first.value(), second.value()) << case_name;
    }
}

TEST_F(SegmentFlusherFormatTest, EmbeddedAndExternalIndexesKeepTheirSegmentBytes) {
    struct EmbeddedIndexCase {
        TabletStorageFormatPB storage_format;
        bool nullable_values;
        bool enable_vertical_writer;
    };
    constexpr std::array embedded_index_cases {
            EmbeddedIndexCase {TABLET_STORAGE_FORMAT_V2, false, false},
            EmbeddedIndexCase {TABLET_STORAGE_FORMAT_V2, true, true},
            EmbeddedIndexCase {TABLET_STORAGE_FORMAT_V3, true, false},
            EmbeddedIndexCase {TABLET_STORAGE_FORMAT_V3, false, true},
    };
    for (const auto& index_case : embedded_index_cases) {
        auto schema =
                create_embedded_index_schema(index_case.storage_format, index_case.nullable_values);
        std::vector<Block> blocks;
        for (int segment_id = 0; segment_id < 2; ++segment_id) {
            auto block_result = create_embedded_index_block(schema, segment_id);
            ASSERT_TRUE(block_result.has_value()) << block_result.error();
            blocks.push_back(std::move(block_result).value());
        }
        const auto case_name =
                fmt::format("embedded_bloom_{}_{}_{}",
                            index_case.storage_format == TABLET_STORAGE_FORMAT_V2 ? "v2" : "v3",
                            index_case.nullable_values ? "nullable" : "not_nullable",
                            index_case.enable_vertical_writer ? "vertical" : "horizontal");
        auto first = flush_blocks(case_name, schema, blocks, index_case.enable_vertical_writer);
        ASSERT_TRUE(first.has_value()) << first.error();
        auto second = flush_blocks(case_name + "_repeat", schema, std::move(blocks),
                                   index_case.enable_vertical_writer);
        ASSERT_TRUE(second.has_value()) << second.error();
        ASSERT_EQ(first.value(), second.value()) << case_name;
    }

    constexpr std::array<std::pair<InvertedIndexStorageFormatPB, std::string_view>, 3>
            kInvertedIndexFormats {{{InvertedIndexStorageFormatPB::V1, "v1"},
                                    {InvertedIndexStorageFormatPB::V2, "v2"},
                                    {InvertedIndexStorageFormatPB::V3, "v3"}}};
    struct ExternalIndexCase {
        InvertedIndexStorageFormatPB storage_format;
        std::string_view storage_format_name;
        bool nullable_value;
        bool enable_vertical_writer;
    };
    constexpr std::array external_index_cases {
            ExternalIndexCase {InvertedIndexStorageFormatPB::V1, "v1", false, false},
            ExternalIndexCase {InvertedIndexStorageFormatPB::V1, "v1", true, true},
            ExternalIndexCase {InvertedIndexStorageFormatPB::V2, "v2", true, false},
            ExternalIndexCase {InvertedIndexStorageFormatPB::V2, "v2", false, true},
            ExternalIndexCase {InvertedIndexStorageFormatPB::V3, "v3", false, false},
            ExternalIndexCase {InvertedIndexStorageFormatPB::V3, "v3", true, true},
    };
    for (const auto& index_case : external_index_cases) {
        auto schema = create_external_inverted_index_schema(index_case.storage_format,
                                                            index_case.nullable_value);
        std::vector<Block> blocks;
        for (int segment_id = 0; segment_id < 2; ++segment_id) {
            auto block_result = create_external_inverted_index_block(schema, segment_id);
            ASSERT_TRUE(block_result.has_value()) << block_result.error();
            blocks.push_back(std::move(block_result).value());
        }
        const auto case_name =
                fmt::format("external_inverted_{}_{}_{}", index_case.storage_format_name,
                            index_case.nullable_value ? "nullable" : "not_nullable",
                            index_case.enable_vertical_writer ? "vertical" : "horizontal");
        auto result = flush_twice(case_name, schema, std::move(blocks),
                                  index_case.enable_vertical_writer);
        ASSERT_TRUE(result.has_value()) << result.error();
        if (index_case.storage_format != InvertedIndexStorageFormatPB::V1) {
            for (const auto& segment : result.value()) {
                ASSERT_EQ(segment.auxiliary_files.size(), 1) << case_name;
                ASSERT_FALSE(segment.auxiliary_files[0].bytes.empty()) << case_name;
            }
        }
    }

    // Faiss HNSW construction uses OpenMP above 100 vectors. Keep construction single-threaded so
    // the repeat semantic queries exercise a reproducible graph; raw index bytes are not compared.
    config::omp_threads_limit = 1;
    for (const auto& [storage_format, storage_format_name] :
         std::array {kInvertedIndexFormats[1], kInvertedIndexFormats[2]}) {
        auto schema = create_ann_index_schema(storage_format);
        for (const bool enable_vertical_writer : {false, true}) {
            std::vector<Block> blocks;
            for (int segment_id = 0; segment_id < 2; ++segment_id) {
                auto block_result = create_ann_index_block(schema, segment_id);
                ASSERT_TRUE(block_result.has_value()) << block_result.error();
                blocks.push_back(std::move(block_result).value());
            }
            const auto case_name = fmt::format("ann_{}_{}", storage_format_name,
                                               enable_vertical_writer ? "vertical" : "horizontal");
            auto result = flush_twice(case_name, schema, std::move(blocks), enable_vertical_writer);
            ASSERT_TRUE(result.has_value()) << result.error();
            for (const auto& segment : result.value()) {
                ASSERT_EQ(segment.auxiliary_files.size(), 1) << case_name;
                ASSERT_FALSE(segment.auxiliary_files[0].bytes.empty()) << case_name;
            }
        }
    }
}

TEST_F(SegmentFlusherFormatTest, ComplexObjectAndVariantValuesKeepTheirSegmentBytes) {
    constexpr int32_t kNoCompressionThresholdKb = 1'000'000;
    struct ComplexCase {
        TabletStorageFormatPB storage_format;
        bool nullable_values;
        bool with_variant_bloom_filter;
        bool enable_vertical_writer;
        bool compressed;
    };
    constexpr std::array complex_cases {
            ComplexCase {TABLET_STORAGE_FORMAT_V2, false, false, false, true},
            ComplexCase {TABLET_STORAGE_FORMAT_V2, false, true, false, false},
            ComplexCase {TABLET_STORAGE_FORMAT_V2, true, false, true, true},
            ComplexCase {TABLET_STORAGE_FORMAT_V2, true, true, true, false},
            ComplexCase {TABLET_STORAGE_FORMAT_V3, false, false, true, false},
            ComplexCase {TABLET_STORAGE_FORMAT_V3, false, true, true, true},
            ComplexCase {TABLET_STORAGE_FORMAT_V3, true, false, false, false},
            ComplexCase {TABLET_STORAGE_FORMAT_V3, true, true, false, true},
    };
    for (const auto& complex_case : complex_cases) {
        auto schema = create_complex_value_schema(complex_case.storage_format,
                                                  complex_case.nullable_values,
                                                  complex_case.with_variant_bloom_filter);
        std::vector<Block> blocks;
        for (int segment_id = 0; segment_id < 2; ++segment_id) {
            auto block_result =
                    create_complex_value_block(schema, segment_id, complex_case.nullable_values);
            ASSERT_TRUE(block_result.has_value()) << block_result.error();
            blocks.push_back(std::move(block_result).value());
        }
        const auto compression_threshold_kb =
                complex_case.compressed ? 0 : kNoCompressionThresholdKb;
        const auto case_name =
                fmt::format("complex_{}_{}_{}_{}_{}",
                            complex_case.storage_format == TABLET_STORAGE_FORMAT_V2 ? "v2" : "v3",
                            complex_case.nullable_values ? "nullable" : "not_nullable",
                            complex_case.with_variant_bloom_filter ? "with_variant_bloom"
                                                                   : "without_variant_bloom",
                            complex_case.enable_vertical_writer ? "vertical" : "horizontal",
                            complex_case.compressed ? "compressed" : "uncompressed");
        auto first = flush_blocks(case_name, schema, blocks, complex_case.enable_vertical_writer,
                                  compression_threshold_kb);
        ASSERT_TRUE(first.has_value()) << first.error();
        auto second = flush_blocks(case_name + "_repeat", schema, std::move(blocks),
                                   complex_case.enable_vertical_writer, compression_threshold_kb);
        ASSERT_TRUE(second.has_value()) << second.error();
        ASSERT_EQ(first.value(), second.value()) << case_name;
    }
}

TEST_F(SegmentFlusherFormatTest, RowStoreAndSegmentCreatorPathsKeepTheirSegmentBytes) {
    auto run_plain_row_store_case = [this](std::string_view case_name,
                                           const TabletSchemaSPtr& schema, DataWriteType write_type,
                                           bool enable_vertical_writer,
                                           bool enable_mow) -> testing::AssertionResult {
        std::vector<Block> blocks;
        for (int segment_id = 0; segment_id < 2; ++segment_id) {
            auto block_result = create_row_store_block(schema, segment_id);
            if (!block_result.has_value()) {
                return testing::AssertionFailure() << block_result.error();
            }
            auto block = std::move(block_result).value();
            // TYPE_DEFAULT and compaction preserve an existing RowStore column. Feed them Blocks
            // read from source Segments so the test exercises the actual path.
            if (write_type == DataWriteType::TYPE_DEFAULT ||
                write_type == DataWriteType::TYPE_COMPACTION) {
                auto direct_block = read_direct_segment_block(
                        fmt::format("{}_{}", case_name, segment_id), schema, std::move(block));
                if (!direct_block.has_value()) {
                    return testing::AssertionFailure() << direct_block.error();
                }
                block = std::move(direct_block).value();
            }
            blocks.push_back(std::move(block));
        }
        auto result = flush_twice(case_name, schema, std::move(blocks), enable_vertical_writer, 0,
                                  write_type, enable_mow);
        if (!result.has_value()) {
            return testing::AssertionFailure() << result.error();
        }
        return testing::AssertionSuccess();
    };

    struct RowStoreWriteCase {
        DataWriteType write_type;
        bool enable_vertical_writer;
    };
    constexpr std::array write_cases {
            RowStoreWriteCase {DataWriteType::TYPE_DEFAULT, false},
            RowStoreWriteCase {DataWriteType::TYPE_DIRECT, true},
            RowStoreWriteCase {DataWriteType::TYPE_SCHEMA_CHANGE, false},
            RowStoreWriteCase {DataWriteType::TYPE_COMPACTION, true},
    };
    auto schema = create_row_store_schema();
    for (const auto& write_case : write_cases) {
        const auto case_name =
                fmt::format("row_store_dup_{}_{}", static_cast<int>(write_case.write_type),
                            write_case.enable_vertical_writer ? "vertical" : "horizontal");
        ASSERT_TRUE(run_plain_row_store_case(case_name, schema, write_case.write_type,
                                             write_case.enable_vertical_writer, false));
    }
    auto mor_schema = create_row_store_schema(UNIQUE_KEYS, false);
    ASSERT_TRUE(run_plain_row_store_case("row_store_unique_mor_1_horizontal", mor_schema,
                                         DataWriteType::TYPE_DIRECT, false, false));
    auto mow_schema = create_row_store_schema(UNIQUE_KEYS, true);
    ASSERT_TRUE(run_plain_row_store_case("row_store_unique_mow_vertical", mow_schema,
                                         DataWriteType::TYPE_DIRECT, true, true));

    auto block_result = create_row_store_block(schema, 0, 5);
    ASSERT_TRUE(block_result.has_value()) << block_result.error();
    auto add_block_result = add_block_with_segment_creator_twice(
            "row_store_segment_creator", schema, std::move(block_result).value(), 2, 0,
            DataWriteType::TYPE_DIRECT);
    ASSERT_TRUE(add_block_result.has_value()) << add_block_result.error();
    ASSERT_EQ(add_block_result.value().size(), 3);

    auto variant_row_store_schema = create_variant_row_store_schema();
    for (const auto& write_case : write_cases) {
        const auto case_name =
                fmt::format("variant_row_store_{}_{}", static_cast<int>(write_case.write_type),
                            write_case.enable_vertical_writer ? "vertical" : "horizontal");
        std::vector<Block> blocks;
        for (int segment_id = 0; segment_id < 2; ++segment_id) {
            auto variant_block =
                    create_variant_row_store_block(variant_row_store_schema, segment_id);
            ASSERT_TRUE(variant_block.has_value()) << variant_block.error();
            auto block = std::move(variant_block).value();
            if (write_case.write_type == DataWriteType::TYPE_DEFAULT ||
                write_case.write_type == DataWriteType::TYPE_COMPACTION) {
                auto direct_block =
                        read_direct_segment_block(fmt::format("{}_{}", case_name, segment_id),
                                                  variant_row_store_schema, std::move(block));
                ASSERT_TRUE(direct_block.has_value()) << direct_block.error();
                block = std::move(direct_block).value();
            }
            blocks.push_back(std::move(block));
        }
        auto result = flush_twice(case_name, variant_row_store_schema, std::move(blocks),
                                  write_case.enable_vertical_writer, 0, write_case.write_type);
        ASSERT_TRUE(result.has_value()) << result.error();
    }

    auto variant_block = create_variant_row_store_block(variant_row_store_schema, 0, 5);
    ASSERT_TRUE(variant_block.has_value()) << variant_block.error();
    auto variant_add_block_result = add_block_with_segment_creator_twice(
            "variant_row_store_segment_creator", variant_row_store_schema,
            std::move(variant_block).value(), 2, 0, DataWriteType::TYPE_DIRECT);
    ASSERT_TRUE(variant_add_block_result.has_value()) << variant_add_block_result.error();
    ASSERT_EQ(variant_add_block_result.value().size(), 3);
}

TEST_F(SegmentFlusherTransformFormatTest, PartialUpdateAndRowBinlogPathsKeepTheirSegmentBytes) {
    // Flexible partial update is a VerticalSegmentWriter-only generation path on the baseline.
    constexpr std::array kFlexiblePartialWriterModes {true};
    auto record = [](Result<std::vector<SegmentFingerprint>> result) {
        if (!result.has_value()) {
            return testing::AssertionFailure() << result.error();
        }
        return testing::AssertionSuccess();
    };

    auto fixed_schema = create_partial_update_schema({.flexible = false});
    auto fixed_tablet = make_mow_tablet(fixed_schema, 21001);
    auto fixed_history_result = write_mow_history(fixed_tablet, fixed_schema, 31001);
    ASSERT_TRUE(fixed_history_result.has_value()) << fixed_history_result.error();
    std::vector<RowsetSharedPtr> fixed_history {fixed_history_result.value()};
    auto fixed_partial_update = std::make_shared<PartialUpdateInfo>();
    auto fixed_update_columns = wide_key_column_names();
    fixed_update_columns.emplace("v1");
    ASSERT_TRUE(fixed_partial_update
                        ->init(fixed_tablet->tablet_id(), 1, *fixed_schema,
                               UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                               PartialUpdateNewRowPolicyPB::APPEND, fixed_update_columns, false, 0,
                               0, "UTC", "")
                        .ok());
    for (const bool enable_vertical_writer : {false, true}) {
        std::vector<Block> blocks;
        for (int segment_id = 0; segment_id < 2; ++segment_id) {
            auto block_result = create_fixed_partial_update_block(
                    fixed_schema, *fixed_partial_update, segment_id);
            ASSERT_TRUE(block_result.has_value()) << block_result.error();
            blocks.push_back(std::move(block_result).value());
        }
        const auto case_name =
                fmt::format("fixed_partial_{}", enable_vertical_writer ? "vertical" : "horizontal");
        ASSERT_TRUE(record(flush_twice(case_name, fixed_schema, std::move(blocks),
                                       enable_vertical_writer, 0, DataWriteType::TYPE_DIRECT, true,
                                       [this, fixed_tablet, fixed_partial_update,
                                        fixed_history](RowsetWriterContext& context) {
                                           configure_partial_update_context(context, fixed_tablet,
                                                                            fixed_partial_update,
                                                                            fixed_history);
                                       })));
    }

    auto flexible_schema = create_partial_update_schema({.flexible = true});
    auto flexible_tablet = make_mow_tablet(flexible_schema, 21002);
    auto flexible_history_result = write_mow_history(flexible_tablet, flexible_schema, 31002);
    ASSERT_TRUE(flexible_history_result.has_value()) << flexible_history_result.error();
    std::vector<RowsetSharedPtr> flexible_history {flexible_history_result.value()};
    auto flexible_partial_update = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(flexible_partial_update
                        ->init(flexible_tablet->tablet_id(), 1, *flexible_schema,
                               UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS,
                               PartialUpdateNewRowPolicyPB::APPEND, {}, false, 0, 0, "UTC", "")
                        .ok());
    for (const bool enable_vertical_writer : kFlexiblePartialWriterModes) {
        std::vector<Block> blocks;
        for (int segment_id = 0; segment_id < 2; ++segment_id) {
            auto block_result = create_flexible_partial_update_block(flexible_schema, segment_id);
            ASSERT_TRUE(block_result.has_value()) << block_result.error();
            blocks.push_back(std::move(block_result).value());
        }
        const auto case_name = fmt::format("flexible_partial_{}",
                                           enable_vertical_writer ? "vertical" : "horizontal");
        ASSERT_TRUE(record(flush_twice(case_name, flexible_schema, std::move(blocks),
                                       enable_vertical_writer, 0, DataWriteType::TYPE_DIRECT, true,
                                       [this, flexible_tablet, flexible_partial_update,
                                        flexible_history](RowsetWriterContext& context) {
                                           configure_partial_update_context(
                                                   context, flexible_tablet,
                                                   flexible_partial_update, flexible_history);
                                       })));
    }

    auto sequence_fixed_schema =
            create_partial_update_schema({.flexible = false, .with_sequence = true});
    auto sequence_fixed_tablet = make_mow_tablet(sequence_fixed_schema, 21006);
    auto sequence_fixed_history_result =
            write_mow_history(sequence_fixed_tablet, sequence_fixed_schema, 31008);
    ASSERT_TRUE(sequence_fixed_history_result.has_value()) << sequence_fixed_history_result.error();
    std::vector<RowsetSharedPtr> sequence_fixed_history {sequence_fixed_history_result.value()};
    auto run_fixed_sequence_case = [&](std::string_view sequence_case,
                                       const std::set<std::string>& update_columns) {
        auto partial_update = std::make_shared<PartialUpdateInfo>();
        auto status = partial_update->init(
                sequence_fixed_tablet->tablet_id(), 1, *sequence_fixed_schema,
                UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS, PartialUpdateNewRowPolicyPB::APPEND,
                update_columns, false, 0, 0, "UTC", "");
        if (!status.ok()) {
            return testing::AssertionFailure() << status;
        }
        for (const bool enable_vertical_writer : {false, true}) {
            std::vector<Block> blocks;
            for (int segment_id = 0; segment_id < 2; ++segment_id) {
                auto block_result = create_fixed_partial_update_block(sequence_fixed_schema,
                                                                      *partial_update, segment_id);
                if (!block_result.has_value()) {
                    return testing::AssertionFailure() << block_result.error();
                }
                blocks.push_back(std::move(block_result).value());
            }
            const auto case_name = fmt::format("{}_{}", sequence_case,
                                               enable_vertical_writer ? "vertical" : "horizontal");
            auto recorded = record(flush_twice(
                    case_name, sequence_fixed_schema, std::move(blocks), enable_vertical_writer, 0,
                    DataWriteType::TYPE_DIRECT, true,
                    [this, sequence_fixed_tablet, partial_update,
                     sequence_fixed_history](RowsetWriterContext& context) {
                        configure_partial_update_context(context, sequence_fixed_tablet,
                                                         partial_update, sequence_fixed_history);
                    }));
            if (!recorded) {
                return recorded;
            }
        }
        return testing::AssertionSuccess();
    };
    auto sequence_omitted_columns = wide_key_column_names();
    sequence_omitted_columns.emplace("v1");
    ASSERT_TRUE(
            run_fixed_sequence_case("fixed_partial_without_sequence", sequence_omitted_columns));

    auto sequence_flexible_schema = create_partial_update_schema(
            {.flexible = true, .with_row_store = true, .with_sequence = true});
    auto sequence_flexible_tablet = make_mow_tablet(sequence_flexible_schema, 21007);
    auto sequence_flexible_history_result =
            write_mow_history(sequence_flexible_tablet, sequence_flexible_schema, 31009);
    ASSERT_TRUE(sequence_flexible_history_result.has_value())
            << sequence_flexible_history_result.error();
    std::vector<RowsetSharedPtr> sequence_flexible_history {
            sequence_flexible_history_result.value()};
    auto sequence_flexible_partial_update = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(sequence_flexible_partial_update
                        ->init(sequence_flexible_tablet->tablet_id(), 1, *sequence_flexible_schema,
                               UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS,
                               PartialUpdateNewRowPolicyPB::APPEND, {}, false, 0, 0, "UTC", "")
                        .ok());
    for (const bool enable_vertical_writer : kFlexiblePartialWriterModes) {
        std::vector<Block> blocks;
        for (int segment_id = 0; segment_id < 2; ++segment_id) {
            auto block_result =
                    create_flexible_partial_update_block(sequence_flexible_schema, segment_id);
            ASSERT_TRUE(block_result.has_value()) << block_result.error();
            blocks.push_back(std::move(block_result).value());
        }
        const auto case_name = fmt::format("flexible_partial_sequence_row_store_{}",
                                           enable_vertical_writer ? "vertical" : "horizontal");
        ASSERT_TRUE(record(
                flush_twice(case_name, sequence_flexible_schema, std::move(blocks),
                            enable_vertical_writer, 0, DataWriteType::TYPE_DIRECT, true,
                            [this, sequence_flexible_tablet, sequence_flexible_partial_update,
                             sequence_flexible_history](RowsetWriterContext& context) {
                                configure_partial_update_context(context, sequence_flexible_tablet,
                                                                 sequence_flexible_partial_update,
                                                                 sequence_flexible_history);
                            })));
    }

    auto transform_fixed_schema = create_partial_update_schema({.flexible = false,
                                                                .with_row_store = true,
                                                                .with_variants = true,
                                                                .with_sequence = true});
    auto transform_fixed_tablet = make_mow_tablet(transform_fixed_schema, 21003);
    auto transform_fixed_history_result =
            write_mow_history(transform_fixed_tablet, transform_fixed_schema, 31004);
    ASSERT_TRUE(transform_fixed_history_result.has_value())
            << transform_fixed_history_result.error();
    std::vector<RowsetSharedPtr> transform_fixed_history {transform_fixed_history_result.value()};
    auto transform_fixed_partial_update = std::make_shared<PartialUpdateInfo>();
    auto transform_fixed_update_columns = wide_key_column_names();
    transform_fixed_update_columns.emplace("v1");
    transform_fixed_update_columns.emplace("v_variant_updated");
    transform_fixed_update_columns.emplace(SEQUENCE_COL);
    ASSERT_TRUE(transform_fixed_partial_update
                        ->init(transform_fixed_tablet->tablet_id(), 1, *transform_fixed_schema,
                               UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                               PartialUpdateNewRowPolicyPB::APPEND, transform_fixed_update_columns,
                               false, 0, 0, "UTC", "")
                        .ok());
    for (const bool enable_vertical_writer : {false, true}) {
        std::vector<Block> blocks;
        for (int segment_id = 0; segment_id < 2; ++segment_id) {
            auto block_result = create_fixed_partial_update_block(
                    transform_fixed_schema, *transform_fixed_partial_update, segment_id);
            ASSERT_TRUE(block_result.has_value()) << block_result.error();
            blocks.push_back(std::move(block_result).value());
        }
        const auto case_name = fmt::format("fixed_partial_sequence_variant_row_store_{}",
                                           enable_vertical_writer ? "vertical" : "horizontal");
        ASSERT_TRUE(record(
                flush_twice(case_name, transform_fixed_schema, std::move(blocks),
                            enable_vertical_writer, 0, DataWriteType::TYPE_DIRECT, true,
                            [this, transform_fixed_tablet, transform_fixed_partial_update,
                             transform_fixed_history](RowsetWriterContext& context) {
                                configure_partial_update_context(context, transform_fixed_tablet,
                                                                 transform_fixed_partial_update,
                                                                 transform_fixed_history);
                            })));
    }

    auto plain_binlog_tablet = create_binlog_tablet(22001, false);
    ASSERT_NE(plain_binlog_tablet, nullptr);
    std::vector<Block> plain_binlog_blocks;
    for (int segment_id = 0; segment_id < 2; ++segment_id) {
        auto block_result =
                create_integer_tablet_block(plain_binlog_tablet->tablet_schema(), segment_id);
        ASSERT_TRUE(block_result.has_value()) << block_result.error();
        plain_binlog_blocks.push_back(std::move(block_result).value());
    }
    ASSERT_TRUE(record(flush_twice(
            "plain_row_binlog_horizontal", plain_binlog_tablet->row_binlog_tablet_schema(),
            std::move(plain_binlog_blocks), false, 0, DataWriteType::TYPE_DIRECT, false,
            [this, plain_binlog_tablet](RowsetWriterContext& context) {
                configure_row_binlog_context(context, plain_binlog_tablet);
            })));

    auto mow_binlog_tablet = create_binlog_tablet(22002, true);
    ASSERT_NE(mow_binlog_tablet, nullptr);
    std::vector<Block> plain_mow_binlog_blocks;
    for (int segment_id = 0; segment_id < 2; ++segment_id) {
        auto block_result =
                create_integer_tablet_block(mow_binlog_tablet->tablet_schema(), segment_id);
        ASSERT_TRUE(block_result.has_value()) << block_result.error();
        plain_mow_binlog_blocks.push_back(std::move(block_result).value());
    }
    ASSERT_TRUE(record(flush_twice(
            "plain_mow_row_binlog_horizontal", mow_binlog_tablet->row_binlog_tablet_schema(),
            std::move(plain_mow_binlog_blocks), false, 0, DataWriteType::TYPE_DIRECT, false,
            [this, mow_binlog_tablet](RowsetWriterContext& context) {
                configure_row_binlog_context(context, mow_binlog_tablet);
            })));
    auto binlog_history_result =
            write_mow_history(mow_binlog_tablet, mow_binlog_tablet->tablet_schema(), 31003);
    ASSERT_TRUE(binlog_history_result.has_value()) << binlog_history_result.error();
    std::vector<RowsetSharedPtr> binlog_history {binlog_history_result.value()};
    auto binlog_partial_update = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(binlog_partial_update
                        ->init(mow_binlog_tablet->tablet_id(), 1,
                               *mow_binlog_tablet->tablet_schema(),
                               UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                               PartialUpdateNewRowPolicyPB::APPEND, {"k1", "v1"}, false, 0, 0,
                               "UTC", "")
                        .ok());
    std::vector<Block> mow_binlog_blocks;
    for (int segment_id = 0; segment_id < 2; ++segment_id) {
        auto block_result = create_binlog_partial_update_block(mow_binlog_tablet->tablet_schema(),
                                                               *binlog_partial_update, segment_id);
        ASSERT_TRUE(block_result.has_value()) << block_result.error();
        mow_binlog_blocks.push_back(std::move(block_result).value());
    }
    ASSERT_TRUE(record(
            flush_twice("mow_row_binlog_horizontal", mow_binlog_tablet->row_binlog_tablet_schema(),
                        std::move(mow_binlog_blocks), false, 0, DataWriteType::TYPE_DIRECT, false,
                        [this, mow_binlog_tablet, binlog_partial_update,
                         binlog_history](RowsetWriterContext& context) {
                            configure_row_binlog_context(context, mow_binlog_tablet,
                                                         binlog_partial_update, binlog_history);
                        })));

    auto sequence_binlog_tablet = create_binlog_tablet(22004, true, false, true);
    ASSERT_NE(sequence_binlog_tablet, nullptr);
    auto sequence_binlog_history_result = write_mow_history(
            sequence_binlog_tablet, sequence_binlog_tablet->tablet_schema(), 31010);
    ASSERT_TRUE(sequence_binlog_history_result.has_value())
            << sequence_binlog_history_result.error();
    std::vector<RowsetSharedPtr> sequence_binlog_history {sequence_binlog_history_result.value()};
    auto sequence_binlog_partial_update = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(sequence_binlog_partial_update
                        ->init(sequence_binlog_tablet->tablet_id(), 1,
                               *sequence_binlog_tablet->tablet_schema(),
                               UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                               PartialUpdateNewRowPolicyPB::APPEND,
                               {"k1", "v1", std::string(SEQUENCE_COL)}, false, 0, 0, "UTC", "")
                        .ok());
    std::vector<Block> sequence_binlog_blocks;
    for (int segment_id = 0; segment_id < 2; ++segment_id) {
        auto block_result =
                create_binlog_partial_update_block(sequence_binlog_tablet->tablet_schema(),
                                                   *sequence_binlog_partial_update, segment_id);
        ASSERT_TRUE(block_result.has_value()) << block_result.error();
        sequence_binlog_blocks.push_back(std::move(block_result).value());
    }
    ASSERT_TRUE(record(flush_twice("mow_sequence_row_binlog_horizontal",
                                   sequence_binlog_tablet->row_binlog_tablet_schema(),
                                   std::move(sequence_binlog_blocks), false, 0,
                                   DataWriteType::TYPE_DIRECT, false,
                                   [this, sequence_binlog_tablet, sequence_binlog_partial_update,
                                    sequence_binlog_history](RowsetWriterContext& context) {
                                       configure_row_binlog_context(context, sequence_binlog_tablet,
                                                                    sequence_binlog_partial_update,
                                                                    sequence_binlog_history);
                                   })));

    auto before_binlog_tablet = create_binlog_tablet(22003, true, true);
    ASSERT_NE(before_binlog_tablet, nullptr);
    auto before_binlog_history_result =
            write_mow_history(before_binlog_tablet, before_binlog_tablet->tablet_schema(), 31006);
    ASSERT_TRUE(before_binlog_history_result.has_value()) << before_binlog_history_result.error();
    std::vector<RowsetSharedPtr> before_binlog_history {before_binlog_history_result.value()};
    auto before_upsert_info = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(before_upsert_info
                        ->init(before_binlog_tablet->tablet_id(), 1,
                               *before_binlog_tablet->tablet_schema(),
                               UniqueKeyUpdateModePB::UPSERT, PartialUpdateNewRowPolicyPB::APPEND,
                               {}, false, 0, 0, "UTC", "")
                        .ok());
    std::vector<Block> before_binlog_blocks;
    for (int segment_id = 0; segment_id < 2; ++segment_id) {
        auto block_result =
                create_integer_tablet_block(before_binlog_tablet->tablet_schema(), segment_id);
        ASSERT_TRUE(block_result.has_value()) << block_result.error();
        before_binlog_blocks.push_back(std::move(block_result).value());
    }
    auto before_result = flush_twice(
            "mow_row_binlog_before", before_binlog_tablet->row_binlog_tablet_schema(),
            before_binlog_blocks, false, 0, DataWriteType::TYPE_DIRECT, false,
            [this, before_binlog_tablet, before_upsert_info,
             before_binlog_history](RowsetWriterContext& context) {
                configure_row_binlog_context(context, before_binlog_tablet, before_upsert_info,
                                             before_binlog_history, true);
            });
    ASSERT_TRUE(before_result.has_value()) << before_result.error();

    std::vector<Block> compacted_binlog_blocks;
    for (int segment_id = 0; segment_id < 2; ++segment_id) {
        auto block_result = create_integer_tablet_block(
                plain_binlog_tablet->row_binlog_tablet_schema(), segment_id, true);
        ASSERT_TRUE(block_result.has_value()) << block_result.error();
        compacted_binlog_blocks.push_back(std::move(block_result).value());
    }
    ASSERT_TRUE(record(flush_twice(
            "compacted_row_binlog_horizontal", plain_binlog_tablet->row_binlog_tablet_schema(),
            std::move(compacted_binlog_blocks), false, 0, DataWriteType::TYPE_COMPACTION, false,
            [this, plain_binlog_tablet](RowsetWriterContext& context) {
                configure_row_binlog_context(context, plain_binlog_tablet);
            })));
}

} // namespace
} // namespace doris
