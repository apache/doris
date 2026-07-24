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

#include <gen_cpp/internal_service.pb.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "format_v2/file_reader.h"

namespace doris {
class WalFileReader;
}

namespace doris::format::wal {

Status parse_wal_column_ids(const std::string& encoded, std::vector<int32_t>* column_ids);

class WalReader final : public FileReader {
public:
    WalReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
              std::unique_ptr<io::FileDescription>& file_description,
              std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
              const std::vector<ColumnDefinition>& projected_columns);
    ~WalReader() override;

    Status init(RuntimeState* state) override;
    Status get_schema(std::vector<ColumnDefinition>* file_schema) const override;
    std::unique_ptr<TableColumnMapper> create_column_mapper(
            TableColumnMapperOptions options) const override;
    Status open(std::shared_ptr<FileScanRequest> request) override;
    Status get_block(Block* file_block, size_t* rows, bool* eof) override;
    Status close() override;

private:
    Status _ensure_schema_loaded() const;
    Status _validate_block_version(const PBlock& pblock) const;
    Status _init_schema_from_block(const PBlock* pblock) const;
    Status _materialize_requested_columns(const Block& source_block, Block* file_block) const;

    const std::vector<ColumnDefinition> _projected_columns;
    std::shared_ptr<doris::WalFileReader> _wal_reader;
    std::string _wal_path;
    uint32_t _version = 0;
    std::vector<int32_t> _column_ids;
    mutable std::vector<ColumnDefinition> _file_schema;
    mutable PBlock _first_block;
    mutable bool _first_block_loaded = false;
    mutable bool _first_block_consumed = false;
    mutable bool _schema_inited = false;
    bool _reader_eof = false;
};

} // namespace doris::format::wal
