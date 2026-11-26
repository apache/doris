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
#include <memory>
#include <span>
#include <vector>

#include "format_v2/file_reader.h"

namespace doris::format::deltalake {

// The scan range owns the rows for the lifetime of the reader. No file is opened.
class DeltaHistoryFileReader final : public FileReader {
public:
    DeltaHistoryFileReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                           std::unique_ptr<io::FileDescription>& file_description,
                           std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                           std::span<const TDeltaLakeHistoryRow> rows);

    Status init(RuntimeState* state) override;
    Status get_schema(std::vector<ColumnDefinition>* schema) const override;
    std::unique_ptr<TableColumnMapper> create_column_mapper(
            TableColumnMapperOptions options) const override;
    void set_batch_size(size_t size) override;
    Status get_block(Block* block, size_t* rows, bool* eof) override;

private:
    const std::span<const TDeltaLakeHistoryRow> _rows;
    size_t _offset = 0;
    size_t _batch_size = 1024;
};

} // namespace doris::format::deltalake
