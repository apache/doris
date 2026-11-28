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

#include "format/table/parquet_metadata_reader.h"

#include <fmt/format.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/parquet_types.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/coding.h"
#include "util/thrift_util.h"

namespace doris {
namespace {

std::string encode_int32(int32_t value) {
    std::string out(sizeof(int32_t), '\0');
    memcpy(out.data(), &value, sizeof(int32_t));
    return out;
}

std::vector<SlotDescriptor*> make_slots(
        ObjectPool* pool, const std::vector<std::pair<std::string, PrimitiveType>>& specs) {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;
    for (int i = 0; i < static_cast<int>(specs.size()); ++i) {
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(specs[i].second)
                                       .nullable(true)
                                       .column_name(specs[i].first)
                                       .column_pos(i)
                                       .build());
    }
    tuple_builder.build(&dtb);

    DescriptorTbl* desc_tbl = nullptr;
    Status st = DescriptorTbl::create(pool, dtb.desc_tbl(), &desc_tbl);
    EXPECT_TRUE(st.ok()) << st;
    if (!st.ok() || desc_tbl == nullptr) {
        return {};
    }
    return desc_tbl->get_tuple_descriptor(0)->slots();
}

Status write_minimal_parquet_file(const std::filesystem::path& file_path,
                                  const std::string& bin_min, const std::string& bin_max) {
    tparquet::FileMetaData meta;
    meta.__set_version(2);
    meta.__set_num_rows(2);

    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(3);

    tparquet::SchemaElement id_elem;
    id_elem.__set_name("id");
    id_elem.__set_type(tparquet::Type::INT32);
    id_elem.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);

    tparquet::SchemaElement v_group;
    v_group.__set_name("v");
    v_group.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    v_group.__set_num_children(1);

    tparquet::SchemaElement v_metadata;
    v_metadata.__set_name("metadata");
    v_metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    v_metadata.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);

    tparquet::SchemaElement label_elem;
    label_elem.__set_name("label");
    label_elem.__set_type(tparquet::Type::BYTE_ARRAY);
    label_elem.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    label_elem.__set_converted_type(tparquet::ConvertedType::UTF8);
    tparquet::LogicalType string_logical;
    string_logical.__set_STRING(tparquet::StringType());
    label_elem.__set_logicalType(string_logical);

    meta.__set_schema({root, id_elem, v_group, v_metadata, label_elem});

    auto make_chunk = [](const std::vector<std::string>& path, tparquet::Type::type type,
                         const tparquet::Statistics& stats) {
        tparquet::ColumnMetaData col_meta;
        col_meta.__set_type(type);
        col_meta.__set_encodings({tparquet::Encoding::PLAIN});
        col_meta.__set_path_in_schema(path);
        col_meta.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        col_meta.__set_num_values(2);
        col_meta.__set_total_uncompressed_size(16);
        col_meta.__set_total_compressed_size(16);
        col_meta.__set_data_page_offset(4);
        col_meta.__set_statistics(stats);

        tparquet::ColumnChunk chunk;
        chunk.__set_file_offset(4);
        chunk.__set_meta_data(col_meta);
        return chunk;
    };

    tparquet::Statistics id_stats;
    id_stats.__set_min_value(encode_int32(1));
    id_stats.__set_max_value(encode_int32(2));
    id_stats.__set_min(encode_int32(1));
    id_stats.__set_max(encode_int32(2));

    tparquet::Statistics bin_stats;
    bin_stats.__set_min_value(bin_min);
    bin_stats.__set_max_value(bin_max);
    bin_stats.__set_min(bin_min);
    bin_stats.__set_max(bin_max);

    tparquet::Statistics label_stats;
    label_stats.__set_min_value("alpha");
    label_stats.__set_max_value("omega");

    tparquet::RowGroup rg;
    rg.__set_num_rows(2);
    rg.__set_total_byte_size(48);
    rg.__set_columns({make_chunk({"id"}, tparquet::Type::INT32, id_stats),
                      make_chunk({"v", "metadata"}, tparquet::Type::BYTE_ARRAY, bin_stats),
                      make_chunk({"label"}, tparquet::Type::BYTE_ARRAY, label_stats)});
    meta.__set_row_groups({rg});

    std::vector<uint8_t> footer_bytes;
    ThriftSerializer serializer(/*compact=*/true, /*initial_buffer_size=*/256);
    RETURN_IF_ERROR(serializer.serialize(&meta, &footer_bytes));

    uint8_t footer_len_le[4];
    encode_fixed32_le(footer_len_le, static_cast<uint32_t>(footer_bytes.size()));

    std::ofstream ofs(file_path, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) {
        return Status::IOError("Failed to create temp parquet file: {}", file_path.string());
    }
    ofs.write("PAR1", 4);
    ofs.write(reinterpret_cast<const char*>(footer_bytes.data()),
              static_cast<std::streamsize>(footer_bytes.size()));
    ofs.write(reinterpret_cast<const char*>(footer_len_le), sizeof(footer_len_le));
    ofs.write("PAR1", 4);
    ofs.close();
    if (!ofs) {
        return Status::IOError("Failed to write temp parquet file: {}", file_path.string());
    }
    return Status::OK();
}

TMetaScanRange make_metadata_scan_range(const std::string& file_path) {
    TParquetMetadataParams params;
    params.__set_mode("parquet_metadata");
    params.__set_file_type(TFileType::FILE_LOCAL);
    params.__set_paths({file_path});

    TMetaScanRange scan_range;
    scan_range.__set_parquet_params(params);
    return scan_range;
}

class ParquetMetadataReaderTest : public testing::Test {
protected:
    void SetUp() override {
        const auto* test_info = testing::UnitTest::GetInstance()->current_test_info();
        _tmp_dir = std::filesystem::temp_directory_path() /
                   fmt::format("parquet_metadata_reader_test_{}_{}_{}", getpid(),
                               test_info->test_suite_name(), test_info->name());
        std::filesystem::create_directories(_tmp_dir);
        _parquet_path = _tmp_dir / "varbinary_stats.parquet";
        const Status st = write_minimal_parquet_file(_parquet_path, _bin_min, _bin_max);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(_tmp_dir, ec);
    }

    std::filesystem::path _tmp_dir;
    std::filesystem::path _parquet_path;
    const std::string _bin_min = std::string("\x01\x01\x00\x02\x01\x61\x62", 7);
    const std::string _bin_max = std::string("\x01\x01\x00\x02\xFF\x79\x7A", 7);
};

TEST_F(ParquetMetadataReaderTest, ProjectPathAndColumnIdOnlyWithVarbinaryStats) {
    ObjectPool pool;
    auto slots = make_slots(&pool, {{"path_in_schema", TYPE_STRING}, {"column_id", TYPE_BIGINT}});
    ASSERT_EQ(2, slots.size());

    RuntimeState state;
    ParquetMetadataReader reader(slots, &state, nullptr,
                                 make_metadata_scan_range(_parquet_path.string()));
    ASSERT_TRUE(reader.init_reader().ok());

    Block block;
    size_t read_rows = 0;
    bool eof = false;
    Status st = reader._do_get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(3, read_rows);
    ASSERT_FALSE(eof);

    const auto& path_col = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                                   .get_nested_column();
    const auto& col_id_col = assert_cast<const ColumnInt64&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column)
                    .get_nested_column());

    EXPECT_EQ("id", path_col.get_data_at(0).to_string());
    EXPECT_EQ(0, col_id_col.get_data()[0]);
    EXPECT_EQ("v.metadata", path_col.get_data_at(1).to_string());
    EXPECT_EQ(1, col_id_col.get_data()[1]);
    EXPECT_EQ("label", path_col.get_data_at(2).to_string());
    EXPECT_EQ(2, col_id_col.get_data()[2]);
}

TEST_F(ParquetMetadataReaderTest, DecodeRequestedVarbinaryAndStringStats) {
    ObjectPool pool;
    auto slots = make_slots(&pool, {{"path_in_schema", TYPE_STRING},
                                    {"stats_min", TYPE_STRING},
                                    {"stats_max", TYPE_STRING},
                                    {"stats_min_value", TYPE_STRING},
                                    {"stats_max_value", TYPE_STRING}});
    ASSERT_EQ(5, slots.size());

    RuntimeState state;
    ParquetMetadataReader reader(slots, &state, nullptr,
                                 make_metadata_scan_range(_parquet_path.string()));
    ASSERT_TRUE(reader.init_reader().ok());

    Block block;
    size_t read_rows = 0;
    bool eof = false;
    Status st = reader._do_get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(3, read_rows);

    const auto& path_col = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& min_col = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& max_col = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    const auto& min_val_col = assert_cast<const ColumnNullable&>(*block.get_by_position(3).column);
    const auto& max_val_col = assert_cast<const ColumnNullable&>(*block.get_by_position(4).column);

    EXPECT_EQ("id", path_col.get_nested_column().get_data_at(0).to_string());
    EXPECT_EQ("1", min_col.get_nested_column().get_data_at(0).to_string());
    EXPECT_EQ("2", max_col.get_nested_column().get_data_at(0).to_string());
    EXPECT_EQ("1", min_val_col.get_nested_column().get_data_at(0).to_string());
    EXPECT_EQ("2", max_val_col.get_nested_column().get_data_at(0).to_string());

    EXPECT_EQ("v.metadata", path_col.get_nested_column().get_data_at(1).to_string());
    EXPECT_EQ(_bin_min, min_col.get_nested_column().get_data_at(1).to_string());
    EXPECT_EQ(_bin_max, max_col.get_nested_column().get_data_at(1).to_string());
    EXPECT_EQ(_bin_min, min_val_col.get_nested_column().get_data_at(1).to_string());
    EXPECT_EQ(_bin_max, max_val_col.get_nested_column().get_data_at(1).to_string());

    EXPECT_EQ("label", path_col.get_nested_column().get_data_at(2).to_string());
    EXPECT_TRUE(min_col.is_null_at(2));
    EXPECT_TRUE(max_col.is_null_at(2));
    EXPECT_EQ("alpha", min_val_col.get_nested_column().get_data_at(2).to_string());
    EXPECT_EQ("omega", max_val_col.get_nested_column().get_data_at(2).to_string());
}

} // namespace
} // namespace doris
