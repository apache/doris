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

namespace doris::parquet {

struct ParquetReaderScanState;

// Parquet 文件物理读取层。
// 该类只理解 Parquet file-local schema 和 FileScanRequest，不理解 Iceberg/global
// schema，不处理 table-level cast/default/generated/partition 语义。
class ParquetReader : public format::FileReader {
public:
    ParquetReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                  std::unique_ptr<io::FileDescription>& file_description,
                  std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                  std::optional<format::GlobalRowIdContext> global_rowid_context = std::nullopt,
                  bool enable_mapping_timestamp_tz = false);
    ~ParquetReader() override;

    // 打开 Parquet 文件并解析 footer metadata。
    // init 成功后可以调用 get_schema() 获取 Parquet file-local schema。
    Status init(RuntimeState* state) override;

    // 返回 init() 阶段解析出的 Parquet 文件自身 schema。
    // 该方法只能在 init() 成功后调用，不要求 open() 已经执行。
    // 这里不做 Iceberg schema evolution，也不把字段转换成 table/global schema。
    Status get_schema(std::vector<format::ColumnDefinition>* file_schema) const override;

    Status open(std::shared_ptr<format::FileScanRequest> request) override;
    // 读取下一批 Parquet file-local block。
    // 该方法只能在 init() 成功后调用。
    // 返回列必须保持 file-local 语义，不能在这里补 default/generated/partition 列。
    Status get_block(Block* file_block, size_t* rows, bool* eof) override;

    Status get_aggregate_result(const format::FileAggregateRequest& request,
                                format::FileAggregateResult* result) override;

    Status close() override;

protected:
    void _init_profile() override;

private:
    void _fill_column_definition(const ParquetColumnSchema& column_schema,
                                 format::ColumnDefinition* field) const;

    std::unique_ptr<ParquetReaderScanState> _state;
    ParquetProfile _parquet_profile;
    std::optional<format::GlobalRowIdContext> _global_rowid_context;
    bool _enable_mapping_timestamp_tz = false;
};

} // namespace doris::parquet
