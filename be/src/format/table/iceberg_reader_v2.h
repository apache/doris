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

// IcebergTableReader 的完整初始化输入。
// 这些字段共同决定一次 table scan 的语义，除非后续有明确的生命周期差异，否则不拆成
// bind/init/set_tasks 多个阶段，避免调用点暴露半初始化状态。
struct IcebergTableReadParams {
    IcebergReadOptions options;
    std::vector<reader::TableColumn> iceberg_schema;
    reader::TableScanRequest scan_request;
    std::vector<IcebergScanTask> scan_tasks;
    std::unique_ptr<reader::FileReader> data_reader;
};

// Iceberg table-level reader。
// 该层继承 TableReader，复用多文件编排和动态分区裁剪等通用能力；同时组合
// FileReader 完成 data file 物理读取，不继承具体文件格式 reader。
class IcebergTableReader : public reader::TableReader {
public:
    IcebergTableReader() = default;

    ~IcebergTableReader() override = default;

    // 初始化一次 Iceberg table scan。
    // params 必须一次性提供 schema、projection/filter、scan tasks 和底层 FileReader；
    // 这样 IcebergTableReader 不会暴露 bind/set_tasks 等半初始化阶段。
    Status init(IcebergTableReadParams params) {
        // 一次性保存 Iceberg table scan 所需输入。TableReader 负责 reader 切换流程；
        // IcebergTableReader 只提供后续要打开的 task 以及 table/file schema 映射语义。
        _iceberg_options = params.options;
        _iceberg_schema = std::move(params.iceberg_schema);
        _table_scan_request = std::move(params.scan_request);
        _scan_tasks = std::move(params.scan_tasks);
        _data_reader = std::move(params.data_reader);
        _next_task_idx = 0;
        return reader::TableReader::init(_iceberg_options.table_options);
    }

    // 关闭当前 Iceberg scan。
    // 先让 TableReader 关闭当前 task reader，再释放 IcebergTableReader 持有的底层
    // FileReader。
    Status close() override {
        RETURN_IF_ERROR(reader::TableReader::close());
        _data_reader.reset();
        return Status::OK();
    }

protected:
    // 打开单个 Iceberg scan task。
    // 该方法完成当前 data file 的 schema mapping、filter localization、position delete
    // 注入，并初始化底层 FileReader；它由 TableReader 的 reader 切换流程调用。
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

    // 打开下一个 Iceberg task。
    // TableReader 负责循环和 EOF 处理；这里仅从 _scan_tasks 中取下一个 task 并调用
    // open_task。
    Status open_next_reader(bool* has_reader) override {
        if (_next_task_idx >= _scan_tasks.size()) {
            if (has_reader != nullptr) {
                *has_reader = false;
            }
            return Status::OK();
        }
        RETURN_IF_ERROR(open_task(_scan_tasks[_next_task_idx++]));
        if (has_reader != nullptr) {
            *has_reader = true;
        }
        return Status::OK();
    }

    // 读取当前 Iceberg task 的下一批 table block。
    // 这里组合底层 FileReader 输出的 file-local block，并负责 equality delete、
    // virtual columns 和 finalize，最终输出 table/global schema block。
    Status read_current(Block* table_block, size_t* rows, bool* eof) override {
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

    // 将 file-local block 转换为 table/global schema block。
    // 这里执行 ColumnMapping 中的 finalize_expr、缺失列填充、partition/generated 列
    // 物化以及复杂列 remap。
    Status finalize_chunk(Block* file_block, Block* table_block) {
        // 真实实现会根据 ColumnMapping 执行 finalize_expr/default/partition/generated
        // expressions，把 file-local block 写成 table block。
        (void)file_block;
        (void)table_block;
        return Status::OK();
    }

    // 将 Iceberg position delete / deletion vector 转换成底层 reader 可消费的删除信息。
    // 这一步发生在读取 data file 前，因此会修改 FileScanRequest。
    Status apply_position_deletes(reader::FileScanRequest* request) {
        // 真实实现会把 position delete / deletion vector 转换成 file-local delete 信息。
        (void)request;
        return Status::OK();
    }

    // 在 table block 上应用 equality delete。
    // equality delete 依赖 table-level 列语义，因此不能下沉到 ParquetReader。
    Status apply_equality_deletes(Block* table_block) {
        // 真实实现会在 table block 上应用 equality delete。
        (void)table_block;
        return Status::OK();
    }

    // 物化 Iceberg 虚拟列。
    // 例如 _row_id、_last_updated_sequence_number 等，它们不来自 Parquet 文件物理列。
    Status materialize_virtual_columns(Block* table_block, size_t rows) {
        // 真实实现会物化 _row_id、_last_updated_sequence_number 等 Iceberg 虚拟列。
        (void)table_block;
        (void)rows;
        return Status::OK();
    }

    // 关闭当前 task 对应的底层 FileReader。
    // 该方法由 TableReader 在切换 reader 或 close 时调用，要求可重复调用。
    Status close_current_reader() override {
        if (_data_reader) {
            RETURN_IF_ERROR(_data_reader->close());
        }
        return Status::OK();
    }

private:
    IcebergReadOptions _iceberg_options;
    IcebergScanTask _scan_task;
    std::vector<IcebergScanTask> _scan_tasks;
    size_t _next_task_idx = 0;
    reader::TableScanRequest _table_scan_request;
    std::vector<reader::TableColumn> _iceberg_schema;
    std::vector<reader::ColumnMapping> _mappings;
    reader::TableColumnMapper _column_mapper;
    std::unique_ptr<reader::FileReader> _data_reader;
};

} // namespace doris::iceberg
