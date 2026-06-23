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
#include <vector>

#include "common/status.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_profile.h"

namespace doris {
namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::format::parquet {

struct ParquetReaderScanState;

// ============================================================================
// Parquet 文件物理读取层 — FileReader 接口的 Parquet 实现
// ============================================================================
//
// 职责边界：
//   ✓ 理解 Parquet file-local schema，处理文件打开、元数据解析、批次读取
//   ✗ 不理解 Iceberg/global schema，不处理 table-level cast/default/generated/partition 列
//
// 被 TableReader（HiveReader、IcebergTableReader）通过 FileReader 接口调用。
// TableReader 负责 schema mapping、predicate localization，生成 FileScanRequest 后
// 传给 ParquetReader::open()。
//
// 生命周期（TableReader 视角）：
//   init() → get_schema() → open(request) → get_block() [loop] → close()
//
// init()     — 打开文件，解析 footer，构建 ParquetColumnSchema 树
// get_schema() — 暴露 file-local ColumnDefinition[]，供 TableReader 做 schema matching
// open()     — 接收 FileScanRequest，执行 RowGroup 裁剪（plan_parquet_row_groups），
//              初始化 ParquetScanScheduler
// get_block() — 委托 ParquetScanScheduler 逐批读取（late materialization）
// close()    — 释放 Arrow 文件资源
// ============================================================================
class ParquetReader : public format::FileReader {
public:
    ParquetReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                  std::unique_ptr<io::FileDescription>& file_description,
                  std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                  std::optional<format::GlobalRowIdContext> global_rowid_context = std::nullopt,
                  bool enable_mapping_timestamp_tz = false);
    ~ParquetReader() override;

    Status init(RuntimeState* state) override;

    Status get_schema(std::vector<format::ColumnDefinition>* file_schema) const override;

    Status open(std::shared_ptr<format::FileScanRequest> request) override;

    // 读取下一批 Parquet file-local block。
    // 返回的列保持 file-local 语义，不补 default/generated/partition 列。
    Status get_block(Block* file_block, size_t* rows, bool* eof) override;

    // 聚合下推：从已裁剪选中的 RowGroup statistics 中提取 COUNT / MIN / MAX。
    Status get_aggregate_result(const format::FileAggregateRequest& request,
                                format::FileAggregateResult* result) override;

    void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) override;

    int64_t get_total_rows() const override;

    Status close() override;

protected:
    void _init_profile() override;

private:
    // 递归将 ParquetColumnSchema 树转换为 ColumnDefinition。
    // identifier 生成规则：有 parquet_field_id → Field(INT, field_id)，否则 → Field(STRING, name)
    void _fill_column_definition(const ParquetColumnSchema& column_schema,
                                 format::ColumnDefinition* field) const;

    std::unique_ptr<ParquetReaderScanState>
            _state;                  // 全部扫描状态（file_context + schema + scheduler）
    ParquetProfile _parquet_profile; // RuntimeProfile 计数器集合
    std::optional<format::GlobalRowIdContext> _global_rowid_context; // 全局 RowId 上下文
    bool _enable_mapping_timestamp_tz = false; // 是否将 UTC timestamp 映射为 TIMESTAMPTZ
};

} // namespace doris::format::parquet
