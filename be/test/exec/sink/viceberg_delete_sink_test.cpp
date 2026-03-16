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

#include "exec/sink/viceberg_delete_sink.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <filesystem>
#include <fstream>

#include "common/consts.h"
#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exec/common/endian.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "util/uid_util.h"

namespace doris {

class VIcebergDeleteSinkTest : public testing::Test {
protected:
    void SetUp() override {
        // Create a basic TDataSink for testing
        _t_data_sink.__set_type(TDataSinkType::ICEBERG_DELETE_SINK);

        TIcebergDeleteSink delete_sink;
        delete_sink.__set_db_name("test_db");
        delete_sink.__set_tb_name("test_table");
        delete_sink.__set_delete_type(TFileContent::POSITION_DELETES);
        delete_sink.__set_file_format(TFileFormatType::FORMAT_PARQUET);
        delete_sink.__set_compress_type(TFileCompressType::SNAPPYBLOCK);
        delete_sink.__set_output_path("/tmp/iceberg/test");
        delete_sink.__set_table_location("/tmp/iceberg/test_table");

        std::map<std::string, std::string> hadoop_conf;
        hadoop_conf["fs.defaultFS"] = "hdfs://localhost:9000";
        delete_sink.__set_hadoop_config(hadoop_conf);

        _t_data_sink.__set_iceberg_delete_sink(delete_sink);
    }

    TDataSink build_local_delete_sink(const std::string& output_path, int32_t format_version) {
        TDataSink t_data_sink;
        t_data_sink.__set_type(TDataSinkType::ICEBERG_DELETE_SINK);

        TIcebergDeleteSink delete_sink;
        delete_sink.__set_db_name("test_db");
        delete_sink.__set_tb_name("test_table");
        delete_sink.__set_delete_type(TFileContent::POSITION_DELETES);
        delete_sink.__set_file_format(TFileFormatType::FORMAT_PARQUET);
        delete_sink.__set_compress_type(TFileCompressType::SNAPPYBLOCK);
        delete_sink.__set_output_path(output_path);
        delete_sink.__set_table_location(output_path);
        delete_sink.__set_file_type(TFileType::FILE_LOCAL);
        delete_sink.__set_format_version(format_version);

        t_data_sink.__set_iceberg_delete_sink(delete_sink);
        return t_data_sink;
    }

    TDataSink _t_data_sink;
};

TEST_F(VIcebergDeleteSinkTest, TestInitProperties) {
    VExprContextSPtrs output_exprs;
    auto sink = std::make_shared<VIcebergDeleteSink>(_t_data_sink, output_exprs, nullptr, nullptr);

    ObjectPool pool;
    Status status = sink->init_properties(&pool);
    ASSERT_TRUE(status.ok());
}

TEST_F(VIcebergDeleteSinkTest, TestGetRowIdColumnIndex) {
    VExprContextSPtrs output_exprs;
    auto sink = std::make_shared<VIcebergDeleteSink>(_t_data_sink, output_exprs, nullptr, nullptr);

    // Create a block with $row_id column
    Block block;

    // Add a regular column
    auto col1 = ColumnInt32::create();
    col1->insert_value(1);
    block.insert(ColumnWithTypeAndName(std::move(col1), std::make_shared<DataTypeInt32>(), "id"));

    // Add __DORIS_ICEBERG_ROWID_COL__ column (as struct)
    auto file_path_col = ColumnString::create();
    file_path_col->insert_data("file1.parquet", 13);

    auto row_pos_col = ColumnInt64::create();
    row_pos_col->insert_value(100);

    Columns struct_cols;
    struct_cols.push_back(std::move(file_path_col));
    struct_cols.push_back(std::move(row_pos_col));

    auto struct_col = ColumnStruct::create(std::move(struct_cols));

    DataTypes struct_types;
    struct_types.push_back(std::make_shared<DataTypeString>());
    struct_types.push_back(std::make_shared<DataTypeInt64>());

    Strings field_names = {"file_path", "row_position"};
    auto struct_type = std::make_shared<DataTypeStruct>(struct_types, field_names);

    block.insert(ColumnWithTypeAndName(std::move(struct_col), struct_type,
                                       doris::BeConsts::ICEBERG_ROWID_COL));

    // Test finding the column
    int idx = sink->_get_row_id_column_index(block);
    ASSERT_EQ(1, idx);
    ASSERT_EQ(doris::BeConsts::ICEBERG_ROWID_COL, block.get_by_position(idx).name);
}

TEST_F(VIcebergDeleteSinkTest, TestGetRowIdColumnIndexWithIcebergRowId) {
    VExprContextSPtrs output_exprs;
    auto sink = std::make_shared<VIcebergDeleteSink>(_t_data_sink, output_exprs, nullptr, nullptr);

    Block block;

    auto file_path_col = ColumnString::create();
    file_path_col->insert_data("file1.parquet", 13);

    auto row_pos_col = ColumnInt64::create();
    row_pos_col->insert_value(100);

    Columns struct_cols;
    struct_cols.push_back(std::move(file_path_col));
    struct_cols.push_back(std::move(row_pos_col));

    auto struct_col = ColumnStruct::create(std::move(struct_cols));

    DataTypes struct_types;
    struct_types.push_back(std::make_shared<DataTypeString>());
    struct_types.push_back(std::make_shared<DataTypeInt64>());

    Strings field_names = {"file_path", "row_position"};
    auto struct_type = std::make_shared<DataTypeStruct>(struct_types, field_names);

    block.insert(ColumnWithTypeAndName(std::move(struct_col), struct_type,
                                       doris::BeConsts::ICEBERG_ROWID_COL));

    int idx = sink->_get_row_id_column_index(block);
    ASSERT_EQ(0, idx);
    ASSERT_EQ(doris::BeConsts::ICEBERG_ROWID_COL, block.get_by_position(idx).name);
}

TEST_F(VIcebergDeleteSinkTest, TestCollectPositionDeletes) {
    VExprContextSPtrs output_exprs;
    auto sink = std::make_shared<VIcebergDeleteSink>(_t_data_sink, output_exprs, nullptr, nullptr);

    // Create a block with multiple $row_id entries
    Block block;

    auto file_path_col = ColumnString::create();
    file_path_col->insert_data("file1.parquet", 13);
    file_path_col->insert_data("file2.parquet", 13);
    file_path_col->insert_data("file1.parquet", 13);

    auto row_pos_col = ColumnInt64::create();
    row_pos_col->insert_value(100);
    row_pos_col->insert_value(200);
    row_pos_col->insert_value(150);

    auto spec_id_col = ColumnInt32::create();
    spec_id_col->insert_value(1);
    spec_id_col->insert_value(2);
    spec_id_col->insert_value(1);

    auto partition_data_col = ColumnString::create();
    partition_data_col->insert_data("p=1", 3);
    partition_data_col->insert_data("p=2", 3);
    partition_data_col->insert_data("p=1", 3);

    Columns struct_cols;
    struct_cols.push_back(std::move(file_path_col));
    struct_cols.push_back(std::move(row_pos_col));
    struct_cols.push_back(std::move(spec_id_col));
    struct_cols.push_back(std::move(partition_data_col));

    auto struct_col = ColumnStruct::create(std::move(struct_cols));

    DataTypes struct_types;
    struct_types.push_back(std::make_shared<DataTypeString>());
    struct_types.push_back(std::make_shared<DataTypeInt64>());
    struct_types.push_back(std::make_shared<DataTypeInt32>());
    struct_types.push_back(std::make_shared<DataTypeString>());

    Strings field_names = {"file_path", "row_position", "partition_spec_id", "partition_data"};
    auto struct_type = std::make_shared<DataTypeStruct>(struct_types, field_names);

    block.insert(ColumnWithTypeAndName(std::move(struct_col), struct_type,
                                       doris::BeConsts::ICEBERG_ROWID_COL));

    // Extract and group
    std::map<std::string, IcebergFileDeletion> file_deletions;
    Status status = sink->_collect_position_deletes(block, file_deletions);
    ASSERT_TRUE(status.ok());

    // Verify grouping
    ASSERT_EQ(2, file_deletions.size());

    ASSERT_TRUE(file_deletions.count("file1.parquet") > 0);
    const auto& file1 = file_deletions.at("file1.parquet");
    ASSERT_EQ(1, file1.partition_spec_id);
    ASSERT_EQ("p=1", file1.partition_data_json);
    ASSERT_EQ(2, file1.rows_to_delete.cardinality());
    ASSERT_TRUE(file1.rows_to_delete.contains(static_cast<uint64_t>(100)));
    ASSERT_TRUE(file1.rows_to_delete.contains(static_cast<uint64_t>(150)));

    ASSERT_TRUE(file_deletions.count("file2.parquet") > 0);
    const auto& file2 = file_deletions.at("file2.parquet");
    ASSERT_EQ(2, file2.partition_spec_id);
    ASSERT_EQ("p=2", file2.partition_data_json);
    ASSERT_EQ(1, file2.rows_to_delete.cardinality());
    ASSERT_TRUE(file2.rows_to_delete.contains(static_cast<uint64_t>(200)));
}

TEST_F(VIcebergDeleteSinkTest, TestCollectPositionDeletesByFieldNames) {
    VExprContextSPtrs output_exprs;
    auto sink = std::make_shared<VIcebergDeleteSink>(_t_data_sink, output_exprs, nullptr, nullptr);

    Block block;

    auto row_pos_col = ColumnInt64::create();
    row_pos_col->insert_value(100);

    auto file_path_col = ColumnString::create();
    file_path_col->insert_data("file1.parquet", 13);

    auto partition_data_col = ColumnString::create();
    partition_data_col->insert_data("[\"p=1\"]", 7);

    auto spec_id_col = ColumnInt32::create();
    spec_id_col->insert_value(3);

    Columns struct_cols;
    struct_cols.push_back(std::move(row_pos_col));
    struct_cols.push_back(std::move(file_path_col));
    struct_cols.push_back(std::move(partition_data_col));
    struct_cols.push_back(std::move(spec_id_col));

    DataTypes struct_types;
    struct_types.push_back(std::make_shared<DataTypeInt64>());
    struct_types.push_back(std::make_shared<DataTypeString>());
    struct_types.push_back(std::make_shared<DataTypeString>());
    struct_types.push_back(std::make_shared<DataTypeInt32>());

    // Standard field names are accepted even if order changes.
    Strings field_names = {"row_position", "file_path", "partition_data", "partition_spec_id"};
    auto struct_type = std::make_shared<DataTypeStruct>(struct_types, field_names);

    auto struct_col = ColumnStruct::create(std::move(struct_cols));
    block.insert(ColumnWithTypeAndName(std::move(struct_col), struct_type,
                                       doris::BeConsts::ICEBERG_ROWID_COL));

    std::map<std::string, IcebergFileDeletion> file_deletions;
    Status status = sink->_collect_position_deletes(block, file_deletions);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(1, file_deletions.size());
    const auto& file = file_deletions.at("file1.parquet");
    ASSERT_EQ(3, file.partition_spec_id);
    ASSERT_EQ("[\"p=1\"]", file.partition_data_json);
    ASSERT_EQ(1, file.rows_to_delete.cardinality());
    ASSERT_TRUE(file.rows_to_delete.contains(static_cast<uint64_t>(100)));
}

TEST_F(VIcebergDeleteSinkTest, TestCollectPositionDeletesRejectNonStandardFieldNames) {
    VExprContextSPtrs output_exprs;
    auto sink = std::make_shared<VIcebergDeleteSink>(_t_data_sink, output_exprs, nullptr, nullptr);

    Block block;

    auto row_pos_col = ColumnInt64::create();
    row_pos_col->insert_value(100);

    auto file_path_col = ColumnString::create();
    file_path_col->insert_data("file1.parquet", 13);

    auto partition_data_col = ColumnString::create();
    partition_data_col->insert_data("[\"p=1\"]", 7);

    auto spec_id_col = ColumnInt32::create();
    spec_id_col->insert_value(3);

    Columns struct_cols;
    struct_cols.push_back(std::move(row_pos_col));
    struct_cols.push_back(std::move(file_path_col));
    struct_cols.push_back(std::move(partition_data_col));
    struct_cols.push_back(std::move(spec_id_col));

    DataTypes struct_types;
    struct_types.push_back(std::make_shared<DataTypeInt64>());
    struct_types.push_back(std::make_shared<DataTypeString>());
    struct_types.push_back(std::make_shared<DataTypeString>());
    struct_types.push_back(std::make_shared<DataTypeInt32>());

    Strings field_names = {"pos", "file_path", "partition_data_json", "spec_id"};
    auto struct_type = std::make_shared<DataTypeStruct>(struct_types, field_names);

    auto struct_col = ColumnStruct::create(std::move(struct_cols));
    block.insert(ColumnWithTypeAndName(std::move(struct_col), struct_type,
                                       doris::BeConsts::ICEBERG_ROWID_COL));

    std::map<std::string, IcebergFileDeletion> file_deletions;
    Status status = sink->_collect_position_deletes(block, file_deletions);
    ASSERT_FALSE(status.ok());
}

TEST_F(VIcebergDeleteSinkTest, TestCollectPositionDeletesFallbackPartitionInfo) {
    TDataSink t_data_sink;
    t_data_sink.__set_type(TDataSinkType::ICEBERG_DELETE_SINK);

    TIcebergDeleteSink delete_sink;
    delete_sink.__set_db_name("test_db");
    delete_sink.__set_tb_name("test_table");
    delete_sink.__set_delete_type(TFileContent::POSITION_DELETES);
    delete_sink.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    delete_sink.__set_compress_type(TFileCompressType::SNAPPYBLOCK);
    delete_sink.__set_output_path("/tmp/iceberg/test");
    delete_sink.__set_table_location("/tmp/iceberg/test_table");
    delete_sink.__set_partition_spec_id(11);
    delete_sink.__set_partition_data_json("[\"11\"]");

    t_data_sink.__set_iceberg_delete_sink(delete_sink);

    VExprContextSPtrs output_exprs;
    auto sink = std::make_shared<VIcebergDeleteSink>(t_data_sink, output_exprs, nullptr, nullptr);
    ObjectPool pool;
    ASSERT_TRUE(sink->init_properties(&pool).ok());

    Block block;

    auto file_path_col = ColumnString::create();
    file_path_col->insert_data("file1.parquet", 13);

    auto row_pos_col = ColumnInt64::create();
    row_pos_col->insert_value(100);

    Columns struct_cols;
    struct_cols.push_back(std::move(file_path_col));
    struct_cols.push_back(std::move(row_pos_col));

    auto struct_col = ColumnStruct::create(std::move(struct_cols));

    DataTypes struct_types;
    struct_types.push_back(std::make_shared<DataTypeString>());
    struct_types.push_back(std::make_shared<DataTypeInt64>());

    Strings field_names = {"file_path", "row_position"};
    auto struct_type = std::make_shared<DataTypeStruct>(struct_types, field_names);

    block.insert(ColumnWithTypeAndName(std::move(struct_col), struct_type,
                                       doris::BeConsts::ICEBERG_ROWID_COL));

    std::map<std::string, IcebergFileDeletion> file_deletions;
    Status status = sink->_collect_position_deletes(block, file_deletions);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(1, file_deletions.size());
    const auto& file = file_deletions.at("file1.parquet");
    ASSERT_EQ(11, file.partition_spec_id);
    ASSERT_EQ("[\"11\"]", file.partition_data_json);
    ASSERT_EQ(1, file.rows_to_delete.cardinality());
    ASSERT_TRUE(file.rows_to_delete.contains(static_cast<uint64_t>(100)));
}

TEST_F(VIcebergDeleteSinkTest, TestCollectPositionDeletesInvalidRowPosition) {
    VExprContextSPtrs output_exprs;
    auto sink = std::make_shared<VIcebergDeleteSink>(_t_data_sink, output_exprs, nullptr, nullptr);

    Block block;

    auto file_path_col = ColumnString::create();
    file_path_col->insert_data("file1.parquet", 13);

    auto row_pos_col = ColumnInt64::create();
    row_pos_col->insert_value(-1);

    Columns struct_cols;
    struct_cols.push_back(std::move(file_path_col));
    struct_cols.push_back(std::move(row_pos_col));

    auto struct_col = ColumnStruct::create(std::move(struct_cols));

    DataTypes struct_types;
    struct_types.push_back(std::make_shared<DataTypeString>());
    struct_types.push_back(std::make_shared<DataTypeInt64>());

    Strings field_names = {"file_path", "row_position"};
    auto struct_type = std::make_shared<DataTypeStruct>(struct_types, field_names);

    block.insert(ColumnWithTypeAndName(std::move(struct_col), struct_type,
                                       doris::BeConsts::ICEBERG_ROWID_COL));

    std::map<std::string, IcebergFileDeletion> file_deletions;
    Status status = sink->_collect_position_deletes(block, file_deletions);
    ASSERT_FALSE(status.ok());
}

TEST_F(VIcebergDeleteSinkTest, TestBuildPositionDeleteBlock) {
    VExprContextSPtrs output_exprs;
    auto sink = std::make_shared<VIcebergDeleteSink>(_t_data_sink, output_exprs, nullptr, nullptr);

    std::string file_path = "test_file.parquet";
    std::vector<int64_t> positions = {10, 20, 30, 40};

    Block output_block;
    Status status = sink->_build_position_delete_block(file_path, positions, output_block);
    ASSERT_TRUE(status.ok());

    // Verify block structure
    ASSERT_EQ(2, output_block.columns());
    ASSERT_EQ(4, output_block.rows());

    // Verify column names
    ASSERT_EQ("file_path", output_block.get_by_position(0).name);
    ASSERT_EQ("pos", output_block.get_by_position(1).name);

    // Verify file_path column
    auto file_path_column =
            check_and_get_column<ColumnString>(output_block.get_by_position(0).column.get());
    ASSERT_NE(nullptr, file_path_column);
    for (size_t i = 0; i < 4; i++) {
        ASSERT_EQ(file_path, file_path_column->get_data_at(i).to_string());
    }

    // Verify pos column
    auto pos_column =
            check_and_get_column<ColumnInt64>(output_block.get_by_position(1).column.get());
    ASSERT_NE(nullptr, pos_column);
    ASSERT_EQ(10, pos_column->get_element(0));
    ASSERT_EQ(20, pos_column->get_element(1));
    ASSERT_EQ(30, pos_column->get_element(2));
    ASSERT_EQ(40, pos_column->get_element(3));
}

TEST_F(VIcebergDeleteSinkTest, TestGenerateDeleteFilePath) {
    VExprContextSPtrs output_exprs;
    auto sink = std::make_shared<VIcebergDeleteSink>(_t_data_sink, output_exprs, nullptr, nullptr);
    ObjectPool pool;
    Status status = sink->init_properties(&pool);
    ASSERT_TRUE(status.ok());

    std::string data_file_path = "data/file1.parquet";
    std::string delete_file_path = sink->_generate_delete_file_path(data_file_path);

    // Verify the path format
    ASSERT_FALSE(delete_file_path.empty());
    const auto& delete_sink = _t_data_sink.iceberg_delete_sink;
    std::string expected_base =
            delete_sink.__isset.output_path ? delete_sink.output_path : delete_sink.table_location;
    if (!expected_base.empty() && expected_base.back() != '/') {
        expected_base += '/';
    }
    ASSERT_TRUE(delete_file_path.rfind(expected_base, 0) == 0);
    ASSERT_NE(std::string::npos, delete_file_path.find("delete_pos_"));
}

TEST_F(VIcebergDeleteSinkTest, TestUnsupportedDeleteType) {
    // Create a TDataSink for an unsupported delete type
    TDataSink t_eq_delete_sink;
    t_eq_delete_sink.__set_type(TDataSinkType::ICEBERG_DELETE_SINK);

    TIcebergDeleteSink delete_sink;
    delete_sink.__set_db_name("test_db");
    delete_sink.__set_tb_name("test_table");
    delete_sink.__set_delete_type(TFileContent::EQUALITY_DELETES);
    delete_sink.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    delete_sink.__set_compress_type(TFileCompressType::SNAPPYBLOCK);
    delete_sink.__set_output_path("/tmp/iceberg/test");

    std::map<std::string, std::string> hadoop_conf;
    delete_sink.__set_hadoop_config(hadoop_conf);

    t_eq_delete_sink.__set_iceberg_delete_sink(delete_sink);

    VExprContextSPtrs output_exprs;
    auto sink =
            std::make_shared<VIcebergDeleteSink>(t_eq_delete_sink, output_exprs, nullptr, nullptr);

    ObjectPool pool;
    Status status = sink->init_properties(&pool);
    ASSERT_FALSE(status.ok());
}

TEST_F(VIcebergDeleteSinkTest, TestWriteDeletionVectorsToSingleSharedPuffin) {
    std::filesystem::path temp_dir =
            std::filesystem::temp_directory_path() / ("iceberg_delete_sink_test_" + generate_uuid_string());
    ASSERT_TRUE(std::filesystem::create_directories(temp_dir));

    TDataSink t_data_sink = build_local_delete_sink(temp_dir.string(), 3);
    VExprContextSPtrs output_exprs;
    auto sink = std::make_shared<VIcebergDeleteSink>(t_data_sink, output_exprs, nullptr, nullptr);
    ObjectPool pool;
    ASSERT_TRUE(sink->init_properties(&pool).ok());

    std::map<std::string, IcebergFileDeletion> file_deletions;
    auto [file1_it, file1_inserted] =
            file_deletions.emplace("file1.parquet", IcebergFileDeletion(1, "[\"p=1\"]"));
    ASSERT_TRUE(file1_inserted);
    file1_it->second.rows_to_delete.add(10);
    file1_it->second.rows_to_delete.add(20);

    auto [file2_it, file2_inserted] =
            file_deletions.emplace("file2.parquet", IcebergFileDeletion(2, "[\"p=2\"]"));
    ASSERT_TRUE(file2_inserted);
    file2_it->second.rows_to_delete.add(30);

    ASSERT_TRUE(sink->_write_deletion_vector_files(file_deletions).ok());
    ASSERT_EQ(2, sink->_commit_data_list.size());
    ASSERT_EQ(2, sink->_delete_file_count);

    const auto& first_commit = sink->_commit_data_list[0];
    const auto& second_commit = sink->_commit_data_list[1];
    ASSERT_EQ(first_commit.file_path, second_commit.file_path);
    ASSERT_EQ(first_commit.file_size, second_commit.file_size);
    ASSERT_LT(first_commit.content_offset, second_commit.content_offset);

    size_t puffin_file_count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(temp_dir)) {
        if (entry.path().extension() == ".puffin") {
            ++puffin_file_count;
        }
    }
    ASSERT_EQ(1, puffin_file_count);

    std::ifstream input(first_commit.file_path, std::ios::binary);
    ASSERT_TRUE(input.good());
    std::string file_bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    ASSERT_EQ(static_cast<size_t>(first_commit.file_size), file_bytes.size());
    ASSERT_GE(file_bytes.size(), 16);
    ASSERT_EQ("PFA1", std::string(file_bytes.data(), 4));
    ASSERT_EQ("PFA1", std::string(file_bytes.data() + file_bytes.size() - 4, 4));

    uint32_t footer_size = LittleEndian::Load32(file_bytes.data() + file_bytes.size() - 12);
    size_t footer_start = file_bytes.size() - 12 - footer_size;
    std::string footer_json = file_bytes.substr(footer_start, footer_size);

    rapidjson::Document footer_doc;
    footer_doc.Parse(footer_json.c_str(), footer_json.size());
    ASSERT_FALSE(footer_doc.HasParseError());
    ASSERT_TRUE(footer_doc.HasMember("blobs"));
    ASSERT_TRUE(footer_doc["blobs"].IsArray());
    ASSERT_EQ(2u, footer_doc["blobs"].Size());

    std::map<std::string, const TIcebergCommitData*> commit_data_by_file;
    commit_data_by_file.emplace(first_commit.referenced_data_file_path, &first_commit);
    commit_data_by_file.emplace(second_commit.referenced_data_file_path, &second_commit);

    for (const auto& blob : footer_doc["blobs"].GetArray()) {
        ASSERT_TRUE(blob.IsObject());
        ASSERT_TRUE(blob.HasMember("properties"));
        ASSERT_TRUE(blob["properties"].IsObject());
        const auto& properties = blob["properties"];
        ASSERT_TRUE(properties.HasMember("referenced-data-file"));
        ASSERT_TRUE(properties.HasMember("cardinality"));

        std::string referenced_data_file = properties["referenced-data-file"].GetString();
        auto commit_it = commit_data_by_file.find(referenced_data_file);
        ASSERT_NE(commit_data_by_file.end(), commit_it);

        const auto* commit_data = commit_it->second;
        ASSERT_EQ(commit_data->content_offset, blob["offset"].GetInt64());
        ASSERT_EQ(commit_data->content_size_in_bytes, blob["length"].GetInt64());
        ASSERT_EQ(std::to_string(commit_data->row_count), properties["cardinality"].GetString());
    }

    std::filesystem::remove_all(temp_dir);
}

} // namespace doris
