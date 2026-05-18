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

#include <vector>

#include "common/status.h"
#include "format/reader/file_reader.h"

namespace doris {
namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::parquet {

// ParquetReader 的 file-local scan 请求。
// 当前没有新增 Parquet-only 字段，但保留独立类型，便于后续加入 row group/page index
// 等 Parquet 专属选项。
struct ParquetScanRequest : public reader::FileScanRequest {};

// Parquet 文件物理读取层。
// 该类只理解 Parquet file-local schema 和 ParquetScanRequest，不理解 Iceberg/global
// schema，不处理 table-level cast/default/generated/partition 语义。
class ParquetReader : public reader::FileReader {
public:
    virtual ~ParquetReader() = default;

    Status get_schema(std::vector<reader::SchemaField>* file_schema) const override {
        // 真实实现会从 Parquet footer / schema descriptor 展开 file-local schema。
        file_schema->clear();
        return Status::OK();
    }

    Status init(const ParquetScanRequest& request) {
        // 真实实现会根据 projected_file_columns、local_filters 和 reader_expression_map
        // 初始化 row group、column chunk、page reader 以及延时物化计划。
        return reader::FileReader::init(request);
    }

    Status next(Block* file_block, size_t* rows, bool* eof) override {
        // 真实实现会输出 file-local block。stub 默认立即 EOF。
        return reader::FileReader::next(file_block, rows, eof);
    }

    Status init(const reader::FileScanRequest& request) override {
        return reader::FileReader::init(request);
    }
};

} // namespace doris::parquet
