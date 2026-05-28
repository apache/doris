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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "format/reader/file_reader.h"
#include "format/reader/table_reader.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
class Block;
struct DeleteFileDesc;
namespace io {
struct FileDescription;
struct FileSystemProperties;
} // namespace io
} // namespace doris

namespace doris::iceberg {

// Iceberg table-level reader。
// 该层继承 TableReader，复用多文件编排和动态分区裁剪等通用能力；同时组合
// FileReader 完成 data file 物理读取，不继承具体文件格式 reader。
class IcebergTableReader : public reader::TableReader {
public:
    ~IcebergTableReader() override = default;

    Status prepare_split(const reader::SplitReadOptions& options) override;

protected:
    Status materialize_virtual_columns(Block* table_block) override;

    Status customize_file_scan_request(reader::FileScanRequest* file_request) override;

    Status _parse_deletion_vector_file(const TTableFormatFileDesc& t_desc, DeleteFileDesc* desc,
                                       bool* has_delete_file) override;

    Status _init_delete_predicates(const TTableFormatFileDesc& t_desc);

private:
    static constexpr int MIN_SUPPORT_DELETE_FILES_VERSION = 2;
    static constexpr int POSITION_DELETE = 1;
    static constexpr int EQUALITY_DELETE = 2;
    static constexpr int DELETION_VECTOR = 3;

    struct RowLineageColumns {
        int64_t first_row_id = -1;
        int64_t last_updated_sequence_number = -1;
    };

    static constexpr const char* ICEBERG_FILE_PATH = "file_path";
    static constexpr const char* ICEBERG_ROW_POS = "pos";
    static constexpr size_t ICEBERG_FILE_PATH_BLOCK_POSITION = 0;
    static constexpr size_t ICEBERG_ROW_POS_BLOCK_POSITION = 1;

    class PositionDeleteRowsCollector final {
    public:
        PositionDeleteRowsCollector(std::string data_file_path, reader::DeleteRows* rows);

        Status collect(const Block& block, size_t read_rows);

    private:
        std::string _data_file_path;
        reader::DeleteRows* _rows = nullptr;
    };

    static std::string _iceberg_delete_vector_cache_key(const TIcebergDeleteFileDesc& delete_file);

    static std::shared_ptr<io::FileSystemProperties> _delete_file_system_properties(
            const TFileScanRangeParams& scan_params);

    static std::unique_ptr<io::FileDescription> _delete_file_description(
            const TFileRangeDesc& range);

    static const reader::SchemaField* _find_delete_field(
            const std::vector<reader::SchemaField>& schema, const std::string& name);

    Status _append_row_position_output_column(reader::FileScanRequest* request);

    Status _append_equality_delete_predicates(reader::FileScanRequest* request);

    Status _init_equality_delete_predicates(
            const std::vector<TIcebergDeleteFileDesc>& delete_files);

    std::string _data_file_path() const;

    // Read equality/position delete files.
    Status _read_parquet_equality_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                              const TFileScanRangeParams& scan_params,
                                              IcebergDeleteFileIOContext* delete_io_ctx);
    Status _read_parquet_position_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                              const TFileScanRangeParams& scan_params,
                                              IcebergDeleteFileIOContext* delete_io_ctx,
                                              PositionDeleteRowsCollector* collector);

    // Read position delete files and collect deleted row positions to update DeletePredicate.
    Status _init_position_delete_rows(const std::vector<TIcebergDeleteFileDesc>& delete_files);

    // Materialize row lineage virtual columns based on the position delete file.
    Status _materialize_row_lineage_row_id(Block* table_block, size_t column_idx);
    Status _materialize_row_lineage_last_updated_sequence_number(Block* table_block,
                                                                 size_t column_idx);

    RowLineageColumns _row_lineage_columns;
    size_t _row_position_block_position = 0;
    const TIcebergFileDesc* _iceberg_params = nullptr;
    bool _delete_predicates_initialized = false;
    reader::DeleteRows _position_delete_rows_storage;
    struct EqualityDeleteFilter {
        std::vector<int> field_ids;
        std::vector<DataTypePtr> key_types;
        Block delete_block;
    };
    std::vector<EqualityDeleteFilter> _equality_delete_filters;

    bool _need_row_lineage_row_id() const;
};

} // namespace doris::iceberg
