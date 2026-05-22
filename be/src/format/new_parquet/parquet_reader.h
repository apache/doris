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
#include <vector>

#include "common/status.h"
#include "format/reader/file_reader.h"

namespace doris {
namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::parquet {

struct ParquetReaderScanState;

// ParquetReader 的 file-local scan 请求。
// 当前没有新增 Parquet-only 字段，但保留独立类型，便于后续加入 row group/page index
// 等 Parquet 专属选项。
struct ParquetScanRequest : public reader::FileScanRequest {};

// Parquet 文件物理读取层。
// 该类只理解 Parquet file-local schema 和 ParquetScanRequest，不理解 Iceberg/global
// schema，不处理 table-level cast/default/generated/partition 语义。
class ParquetReader : public reader::FileReader {
public:
    ParquetReader();
    ~ParquetReader() override;

    // 打开 Parquet 文件并解析 footer metadata。
    // open 成功后可以调用 get_schema() 获取 Parquet file-local schema。
    Status open(io::FileReaderSPtr file, io::IOContext* io_ctx = nullptr) override;

    // 解析 Parquet footer 并返回 Parquet 文件自身的 schema。
    // 该方法只能在 open() 成功后调用，不要求 init() 已经执行。
    // 这里不做 Iceberg schema evolution，也不把字段转换成 table/global schema。
    Status get_schema(std::vector<reader::SchemaField>* file_schema) const override;

    // 初始化 Parquet 专属 scan。
    // init 成功后可以调用 get_block() 读取 Parquet file-local block。
    // 后续可以在 ParquetScanRequest 中扩展 row group、page index、bloom filter 等
    // Parquet-only 选项；table-level 语义仍然必须由 TableColumnMapper 提前转换。
    Status init(const ParquetScanRequest& request);

    // 读取下一批 Parquet file-local block。
    // 该方法只能在 init() 成功后调用。
    // 返回列必须保持 file-local 语义，不能在这里补 default/generated/partition 列。
    Status get_block(Block* file_block, size_t* rows, bool* eof) override;

    Status close() override;

    // 通用 FileReader 初始化入口。
    // 当上层只持有 reader::FileReader 指针时会走该接口；Parquet 专属参数通过
    // ParquetScanRequest 重载表达。
    Status init(const reader::FileScanRequest& request) override;

private:
    std::unique_ptr<ParquetReaderScanState> _state;
};

} // namespace doris::parquet
