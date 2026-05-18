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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "format/reader/file_reader.h"
#include "format/reader/table_reader.h"

namespace doris {
class Block;
} // namespace doris

namespace doris::iceberg {

// Iceberg data file 摘要。它描述当前要读取的物理 data file，不承载列映射逻辑。
struct IcebergDataFile {
    std::string path;
    std::string format;
    int64_t record_count = 0;
    int64_t file_size = 0;
    int64_t sequence_number = 0;
    int64_t first_row_id = -1;
};

// Iceberg delete file 摘要。position/equality/deletion vector 的具体读取在
// IcebergTableReader 实现阶段补齐。
struct IcebergDeleteFile {
    std::string path;
    std::string format;
    int64_t sequence_number = 0;
    std::vector<reader::ColumnId> equality_field_ids;
};

// 单个 Iceberg data file 的 scan 输入。
// 该结构只进入 IcebergTableReader，不直接传给 ParquetReader。
struct IcebergScanTask {
    IcebergDataFile data_file;
    std::vector<IcebergDeleteFile> positional_deletes;
    std::vector<IcebergDeleteFile> equality_deletes;
    std::vector<IcebergDeleteFile> deletion_vectors;
};

struct IcebergReadOptions {
    reader::TableReadOptions table_options;
    bool enable_position_delete = true;
    bool enable_equality_delete = true;
    bool enable_deletion_vector = true;
};

// Iceberg table-level reader。
// 该层继承 TableReader，复用多文件编排和动态分区裁剪等通用能力；同时组合
// FileReader 完成 data file 物理读取，不继承具体文件格式 reader。
class IcebergTableReader : public reader::TableReader {
public:
    IcebergTableReader() = default;

    explicit IcebergTableReader(std::unique_ptr<reader::FileReader> data_reader)
            : _data_reader(std::move(data_reader)) {}

    ~IcebergTableReader() override = default;

    Status init(const IcebergReadOptions& options,
                std::unique_ptr<reader::FileReader> data_reader) {
        _iceberg_options = options;
        _data_reader = std::move(data_reader);
        return reader::TableReader::init(options.table_options);
    }

    Status bind(const std::vector<reader::TableColumn>& iceberg_schema) {
        // 真实实现会绑定 Iceberg 当前 schema，并准备 field-id based mapping 输入。
        _iceberg_schema = iceberg_schema;
        return Status::OK();
    }

    Status init(const reader::TableScanRequest& request) {
        // 保存 table-level projection/filter，后续由 TableColumnMapper 转成 FileScanRequest。
        _table_scan_request = request;
        return Status::OK();
    }

    Status open_task(const IcebergScanTask& task) {
        // 真实实现会读取 data file schema，创建 field-id mapping，应用 position deletes，
        // 并初始化底层 ParquetReader。
        _scan_task = task;
        std::vector<reader::SchemaField> file_schema;
        if (_data_reader) {
            RETURN_IF_ERROR(_data_reader->get_schema(&file_schema));
        }
        reader::TableColumnMapperOptions mapper_options;
        mapper_options.mode = reader::TableColumnMappingMode::BY_FIELD_ID;
        _column_mapper = reader::TableColumnMapper(mapper_options);
        RETURN_IF_ERROR(_column_mapper.create_mapping(_iceberg_schema, file_schema, &_mappings));

        reader::FileScanRequest file_request;
        RETURN_IF_ERROR(_column_mapper.create_scan_request(_table_scan_request, _mappings,
                                                           &file_request));
        RETURN_IF_ERROR(apply_position_deletes(&file_request));
        if (_data_reader) {
            RETURN_IF_ERROR(_data_reader->init(file_request));
        }
        return Status::OK();
    }

    Status next(Block* table_block, size_t* rows, bool* eof) override {
        // 真实实现会读取 file-local block，finalize 成 table block，再应用 equality delete
        // 和 Iceberg virtual columns。stub 默认 EOF。
        // 后续实现应在 IcebergTableReader 内部持有 file-local block；这里仅复用输出指针
        // 作为 header-only API 占位，避免在骨架阶段引入 Block 的完整定义。
        Block* file_block = table_block;
        if (_data_reader) {
            RETURN_IF_ERROR(_data_reader->next(file_block, rows, eof));
        }
        RETURN_IF_ERROR(finalize_chunk(file_block, table_block));
        RETURN_IF_ERROR(apply_equality_deletes(table_block));
        RETURN_IF_ERROR(materialize_virtual_columns(table_block, rows != nullptr ? *rows : 0));
        return Status::OK();
    }

    Status finalize_chunk(Block* file_block, Block* table_block) {
        // 真实实现会根据 ColumnMapping 执行 finalize_expr/default/partition/generated
        // expressions，把 file-local block 写成 table block。
        (void)file_block;
        (void)table_block;
        return Status::OK();
    }

    Status apply_position_deletes(reader::FileScanRequest* request) {
        // 真实实现会把 position delete / deletion vector 转换成 file-local delete 信息。
        (void)request;
        return Status::OK();
    }

    Status apply_equality_deletes(Block* table_block) {
        // 真实实现会在 table block 上应用 equality delete。
        (void)table_block;
        return Status::OK();
    }

    Status materialize_virtual_columns(Block* table_block, size_t rows) {
        // 真实实现会物化 _row_id、_last_updated_sequence_number 等 Iceberg 虚拟列。
        (void)table_block;
        (void)rows;
        return Status::OK();
    }

    Status close() override {
        if (_data_reader) {
            RETURN_IF_ERROR(_data_reader->close());
        }
        _data_reader.reset();
        return Status::OK();
    }

private:
    IcebergReadOptions _iceberg_options;
    IcebergScanTask _scan_task;
    reader::TableScanRequest _table_scan_request;
    std::vector<reader::TableColumn> _iceberg_schema;
    std::vector<reader::ColumnMapping> _mappings;
    reader::TableColumnMapper _column_mapper;
    std::unique_ptr<reader::FileReader> _data_reader;
};

} // namespace doris::iceberg
