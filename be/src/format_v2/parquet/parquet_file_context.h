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

#include <arrow/io/interfaces.h>
#include <parquet/api/reader.h>

#include <memory>

#include "common/status.h"
#include "io/fs/file_reader.h"

namespace doris::format::parquet {

// Parquet 文件上下文 — 管理 Arrow 层文件对象和元数据的生命周期。
//
// 该类是 Doris 与 Arrow Parquet C++ library 的边界：
// - open():  将 Doris 的 io::FileReader 包装为 Arrow::RandomAccessFile，
//            然后用 Arrow 的 ParquetFileReader::Open() 解析 footer。
// - close(): 释放 Arrow 持有的文件句柄和 reader 资源。
//
// metadata 和 schema 在 open() 后可用，供 build_parquet_column_schema()、
// plan_parquet_row_groups() 等使用。ParquetColumnReaderFactory 通过
// file_reader->RowGroup(idx) 按需打开 RowGroupReader。
struct ParquetFileContext {
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;   // Doris FileReader 的 Arrow 包装
    std::unique_ptr<::parquet::ParquetFileReader> file_reader; // Arrow Parquet 文件解析器
    std::shared_ptr<::parquet::FileMetaData> metadata;         // Footer metadata (RowGroup 信息)
    const ::parquet::SchemaDescriptor* schema = nullptr;       // 物理 leaf column schema

    Status open(io::FileReaderSPtr input_file_reader, io::IOContext* io_ctx);
    Status close();
};

Status arrow_status_to_doris_status(const arrow::Status& status);

} // namespace doris::format::parquet
