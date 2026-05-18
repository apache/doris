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
struct IcebergDataFile final : public reader::BaseDataFile {
    int64_t sequence_number = 0;
    int64_t first_row_id = -1;
};

// Iceberg delete file 摘要。position/equality/deletion vector 的具体读取在
// IcebergTableReader 实现阶段补齐。
struct IcebergDeleteFile final : public reader::BaseDataFile {
    int64_t sequence_number = 0;
    std::vector<reader::ColumnId> equality_field_ids;
};

// 单个 Iceberg data file 的 scan 输入。
// 该结构只进入 IcebergTableReader，不直接传给 ParquetReader。
struct IcebergScanTask final : public reader::ScanTask {
    std::vector<IcebergDeleteFile> positional_deletes;
    std::vector<IcebergDeleteFile> equality_deletes;
    std::vector<IcebergDeleteFile> deletion_vectors;
};

// Iceberg table-level reader。
// 该层继承 TableReader，复用多文件编排和动态分区裁剪等通用能力；同时组合
// FileReader 完成 data file 物理读取，不继承具体文件格式 reader。
class IcebergTableReader : public reader::TableReader {
public:
    IcebergTableReader() = default;

    ~IcebergTableReader() override = default;

    // 初始化一次 Iceberg table scan。
    // options 必须一次性提供 schema、projection/filter 和 scan tasks，避免暴露
    // bind/set_tasks 等半初始化阶段。
    Status init(reader::TableReadOptions options) override {
        // 一次性保存 Iceberg table scan 所需输入。TableReader 负责 reader 切换流程；
        // IcebergTableReader 只提供后续要打开的 task 以及 table/file schema 映射语义。
        return reader::TableReader::init(std::move(options));
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
    // 将 file-local block 转换为 table/global schema block。
    // 这里执行 ColumnMapping 中的 finalize_expr、缺失列填充、partition/generated 列
    // 物化以及复杂列 remap。
    Status finalize_chunk(Block* block) override {
        // 真实实现会根据 ColumnMapping 执行 finalize_expr/default/partition/generated
        // expressions，把 file-local block 写成 table block。
        RETURN_IF_ERROR(apply_equality_deletes(block));
        return Status::OK();
    }

    // 物化 Iceberg 虚拟列。
    // 例如 _row_id、_last_updated_sequence_number 等，它们不来自 Parquet 文件物理列。
    Status materialize_virtual_columns(Block* table_block) override {
        // 真实实现会物化 _row_id、_last_updated_sequence_number 等 Iceberg 虚拟列。
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
    Status apply_equality_deletes(Block* block) {
        // 真实实现会在 table block 上应用 equality delete。
        return Status::OK();
    }
};

} // namespace doris::iceberg
