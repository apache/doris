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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "format/reader/file_reader.h"
#include "format/reader/table_reader.h"

namespace doris {
class EqualityDeleteBase;
class RuntimeProfile;
} // namespace doris

namespace doris::iceberg {

enum class IcebergDeleteContent : int8_t {
    POSITION_DELETE = 1,
    EQUALITY_DELETE = 2,
    DELETION_VECTOR = 3,
};

// Iceberg data file 摘要。它描述当前要读取的物理 data file，不承载列映射逻辑。
struct IcebergDataFile final : public reader::BaseDataFile {
    std::string original_path;
    int64_t sequence_number = 0;
    int64_t first_row_id = -1;
    int64_t last_updated_sequence_number = -1;
    int32_t partition_spec_id = 0;
    std::string partition_data_json;
};

// Iceberg delete file 摘要。position/equality/deletion vector 的具体读取在
// IcebergTableReader 实现阶段补齐。
struct IcebergDeleteFile final : public reader::BaseDataFile {
    IcebergDeleteContent content = IcebergDeleteContent::POSITION_DELETE;
    int64_t sequence_number = 0;
    std::optional<int64_t> position_lower_bound;
    std::optional<int64_t> position_upper_bound;
    std::optional<int64_t> content_offset;
    std::optional<int64_t> content_size_in_bytes;
    std::vector<reader::ColumnId> equality_field_ids;
};

// 单个 Iceberg data file 的 scan 输入。
// 该结构只进入 IcebergTableReader，不直接传给 ParquetReader。
struct IcebergScanTask final : public reader::ScanTask {
    std::vector<IcebergDeleteFile> positional_deletes;
    std::vector<IcebergDeleteFile> equality_deletes;
    std::vector<IcebergDeleteFile> deletion_vectors;
};

class IcebergDataReaderFactory {
public:
    virtual ~IcebergDataReaderFactory() = default;
    virtual Status create(const IcebergScanTask& task,
                          std::unique_ptr<reader::FileReader>* reader) = 0;
};

struct IcebergEqualityDeleteData {
    std::vector<reader::ColumnId> field_ids;
    Block delete_block;
};

class IcebergDeleteFileLoader {
public:
    virtual ~IcebergDeleteFileLoader() = default;

    virtual Status load_position_deletes(const IcebergDeleteFile& delete_file,
                                         const IcebergDataFile& data_file,
                                         std::vector<int64_t>* rows) = 0;

    virtual Status load_deletion_vector(const IcebergDeleteFile& delete_file,
                                        const IcebergDataFile& data_file,
                                        std::vector<int64_t>* rows) = 0;

    virtual Status load_equality_deletes(const IcebergDeleteFile& delete_file,
                                         const std::vector<reader::SchemaField>& data_file_schema,
                                         IcebergEqualityDeleteData* delete_data) = 0;
};

struct IcebergTableReadParams {
    reader::TableReadOptions table_options;
    std::shared_ptr<IcebergDataReaderFactory> data_reader_factory;
    std::shared_ptr<IcebergDeleteFileLoader> delete_file_loader;
    std::map<std::string, reader::ColumnId> name_mapping;
    RuntimeProfile* profile = nullptr;
};

// Iceberg table-level reader。
// 该层继承 TableReader，复用多文件编排和动态分区裁剪等通用能力；同时组合
// FileReader 完成 data file 物理读取，不继承具体文件格式 reader。
class IcebergTableReader : public reader::TableReader {
public:
    IcebergTableReader();
    ~IcebergTableReader() override;

    using reader::TableReader::init;
    Status init(IcebergTableReadParams params);
    Status close() override;

protected:
    Status create_reader_for_task(const reader::ScanTask& task,
                                  std::unique_ptr<reader::FileReader>* reader) override;
    Status open_reader() override;
    Status close_current_reader() override;
    Status finalize_chunk(Block* block) override;
    Status materialize_virtual_columns(Block* table_block) override;

private:
    Status build_position_visibility(reader::FileScanRequest* request);
    Status load_position_delete_rows(std::vector<int64_t>* rows);
    Status load_deletion_vector_rows(std::vector<int64_t>* rows);
    Status build_equality_delete_state(reader::FileScanRequest* request,
                                       const std::vector<reader::SchemaField>& file_schema);
    Status apply_residual_filters(Block* block, IColumn::Filter* filter);
    Status apply_position_deletes(Block* block, IColumn::Filter* filter);
    Status apply_equality_deletes(Block* block, IColumn::Filter* filter);
    Status materialize_legacy_row_id(Block* block);
    Status materialize_row_lineage_row_id(Block* block);
    Status materialize_last_updated_sequence_number(Block* block);
    Status clear_task_state();

    void update_row_lineage_file_column_state();
    std::map<int32_t, reader::TableFilter> file_pushdown_filters() const;
    bool needs_row_positions() const;
    bool has_projected_column(const std::string& name) const;
    bool is_row_lineage_column(const reader::TableColumn& column) const;
    const reader::TableColumn* find_projected_column(reader::ColumnId field_id) const;
    Status ensure_equality_key_column(reader::ColumnId field_id,
                                      const std::vector<reader::SchemaField>& file_schema,
                                      reader::FileScanRequest* request);
    std::shared_ptr<IcebergDataReaderFactory> _data_reader_factory;
    std::shared_ptr<IcebergDeleteFileLoader> _delete_file_loader;
    RuntimeProfile* _runtime_profile = nullptr;

    const IcebergScanTask* _current_iceberg_task = nullptr;
    const IcebergDataFile* _current_data_file = nullptr;

    std::shared_ptr<std::vector<int64_t>> _position_delete_rows;
    bool _need_row_positions = false;
    std::vector<int64_t> _current_batch_row_positions;
    bool _row_lineage_row_id_from_file = false;
    bool _row_lineage_last_updated_sequence_number_from_file = false;

    std::vector<Block> _equality_delete_blocks;
    std::vector<std::vector<int>> _equality_delete_field_ids;
    std::vector<std::unique_ptr<EqualityDeleteBase>> _equality_delete_impls;
    std::unordered_map<int, std::string> _id_to_block_column_name;
    std::set<std::string> _equality_helper_column_names;
    std::map<std::string, reader::ColumnId> _name_mapping;

    static constexpr const char* ROW_LINEAGE_ROW_ID = "_row_id";
    static constexpr const char* ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER =
            "_last_updated_sequence_number";
};

} // namespace doris::iceberg
