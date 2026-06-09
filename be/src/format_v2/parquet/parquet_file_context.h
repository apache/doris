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

namespace doris::parquet {

struct ParquetFileContext {
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
    std::unique_ptr<::parquet::ParquetFileReader> file_reader;
    std::shared_ptr<::parquet::FileMetaData> metadata;
    const ::parquet::SchemaDescriptor* schema = nullptr;

    Status open(io::FileReaderSPtr input_file_reader, io::IOContext* io_ctx);
    Status close();
};

Status arrow_status_to_doris_status(const arrow::Status& status);

} // namespace doris::parquet
