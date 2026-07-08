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
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "format_v2/file_reader.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
class Block;
struct DeleteFileDesc;
namespace io {
struct FileDescription;
struct FileSystemProperties;
} // namespace io
} // namespace doris

namespace doris::format::iceberg {

// Iceberg table-level reader.
// It reuses TableReader for split orchestration, dynamic partition pruning and table-block
// finalization, while composing a FileReader for physical data-file reads instead of inheriting
// from a concrete file-format reader.
class IcebergTableReader : public format::TableReader {
public:
    ~IcebergTableReader() override = default;
    Status init(format::TableReadOptions&& options) override {
        RETURN_IF_ERROR(format::TableReader::init(std::move(options)));
        _mapper_options.mode = format::TableColumnMappingMode::BY_FIELD_ID;
        return Status::OK();
    }

    Status prepare_split(const format::SplitReadOptions& options) override;
    std::string debug_string() const override;
    format::TableColumnMappingMode mapping_mode() const override {
        return !_data_reader.file_schema.empty() && _has_field_id(_data_reader.file_schema)
                       ? format::TableColumnMappingMode::BY_FIELD_ID
                       : format::TableColumnMappingMode::BY_NAME;
    }

protected:
    Status materialize_virtual_columns(Block* table_block) override;

    Status customize_file_scan_request(format::FileScanRequest* file_request) override;

    bool _supports_aggregate_pushdown(TPushAggOp::type agg_type) const override;

    Status _parse_deletion_vector_file(const TTableFormatFileDesc& t_desc, DeleteFileDesc* desc,
                                       bool* has_delete_file) override;

    Status _init_delete_predicates(const TTableFormatFileDesc& t_desc);

private:
    bool _has_field_id(const std::vector<format::ColumnDefinition>& schema) const {
        for (const auto& field : schema) {
            // TopN lazy materialization asks the file reader to synthesize GLOBAL_ROWID in the
            // first-phase scan. That virtual column is not an Iceberg data field and therefore has
            // no Iceberg field id. Do not let it downgrade schema-evolution reads to BY_NAME,
            // otherwise old data files whose physical names predate a rename (for example,
            // table column `new_new_id` stored as file column `id`) are materialized as defaults.
            if (field.column_type != format::ColumnType::DATA_COLUMN) {
                continue;
            }
            if (!field.has_identifier_field_id()) {
                return false;
            }
            if (!_has_field_id(field.children)) {
                return false;
            }
        }
        return true;
    }
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
        PositionDeleteRowsCollector(std::string data_file_path, format::DeleteRows* rows);

        Status collect(const Block& block, size_t read_rows);

    private:
        std::string _data_file_path;
        format::DeleteRows* _rows = nullptr;
    };

    static std::string _iceberg_delete_vector_cache_key(const TIcebergDeleteFileDesc& delete_file);

    static std::shared_ptr<io::FileSystemProperties> _delete_file_system_properties(
            const TFileScanRangeParams& scan_params);

    static std::unique_ptr<io::FileDescription> _delete_file_description(
            const TFileRangeDesc& range);

    std::string _data_file_path() const;

    // Append row position column to file scan request for position delete handling.
    Status _append_row_position_output_column(format::FileScanRequest* request);
    // Append equality delete predicates to file scan request based on the delete files in iceberg
    // params. DeleteVector and position delete files use the common DeleteRows path in TableReader.
    Status _append_equality_delete_predicates(format::FileScanRequest* request);

    Status _init_equality_delete_predicates(
            const std::vector<TIcebergDeleteFileDesc>& delete_files);

    // Read equality/position delete files.
    Status _create_delete_file_reader(const TIcebergDeleteFileDesc& delete_file,
                                      const TFileScanRangeParams& scan_params,
                                      IcebergDeleteFileIOContext* delete_io_ctx,
                                      std::unique_ptr<format::FileReader>* reader);
    Status _read_equality_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                      const TFileScanRangeParams& scan_params,
                                      IcebergDeleteFileIOContext* delete_io_ctx);
    Status _read_position_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                      const TFileScanRangeParams& scan_params,
                                      IcebergDeleteFileIOContext* delete_io_ctx,
                                      PositionDeleteRowsCollector* collector);

    // Read position delete files and collect deleted row positions to update DeletePredicate.
    Status _init_position_delete_rows(const std::vector<TIcebergDeleteFileDesc>& delete_files);

    // Materialize row lineage virtual columns based on the position delete file.
    Status _materialize_iceberg_rowid(Block* table_block, size_t column_idx);
    Status _materialize_row_lineage_row_id(Block* table_block, size_t column_idx);
    Status _materialize_row_lineage_last_updated_sequence_number(Block* table_block,
                                                                 size_t column_idx);

    RowLineageColumns _row_lineage_columns;
    size_t _row_position_block_position = 0;
    std::optional<TIcebergFileDesc> _iceberg_params;
    bool _delete_predicates_initialized = false;
    format::DeleteRows _position_delete_rows_storage;
    struct EqualityDeleteFilter {
        std::vector<int> field_ids;
        std::vector<DataTypePtr> key_types;
        Block delete_block;
    };
    std::vector<EqualityDeleteFilter> _equality_delete_filters;

    bool _need_row_lineage_row_id() const;
    bool _need_iceberg_rowid() const;
};

} // namespace doris::format::iceberg
