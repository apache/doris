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

#include <gen_cpp/data.pb.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "format_v2/file_reader.h"

namespace doris::format::native {

// FileScannerV2 reader for Doris Native files.
//
// Native files are self-describing only through the first serialized PBlock. TableReader asks for
// schema before open(), so this reader may read and cache that first PBlock during get_schema() and
// then replay it as the first data batch after open().
class NativeReader final : public FileReader {
public:
    NativeReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                 std::unique_ptr<io::FileDescription>& file_description,
                 std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile);
    ~NativeReader() override;

    Status init(RuntimeState* state) override;
    Status get_schema(std::vector<ColumnDefinition>* file_schema) const override;
    std::unique_ptr<TableColumnMapper> create_column_mapper(
            TableColumnMapperOptions options) const override;
    Status open(std::shared_ptr<FileScanRequest> request) override;
    Status get_block(Block* file_block, size_t* rows, bool* eof) override;
    Status close() override;

private:
    void _init_profile() override;
    Status _validate_and_consume_header();
    Status _ensure_schema_loaded() const;
    Status _read_next_pblock(std::string* buffer, bool* eof) const;
    Status _init_schema_from_pblock(const PBlock& pblock) const;
    Status _materialize_requested_columns(const Block& source_block, Block* file_block) const;
    Status _apply_filters(Block* file_block, size_t* rows) const;

    RuntimeState* _runtime_state = nullptr;
    mutable int64_t _current_offset = 0;
    mutable int64_t _file_size = 0;
    mutable bool _reader_eof = true;
    mutable bool _schema_inited = false;
    mutable std::vector<ColumnDefinition> _file_schema;
    mutable std::string _first_block_buffer;
    mutable bool _first_block_loaded = false;
    mutable bool _first_block_consumed = false;
    RuntimeProfile::Counter* _total_time = nullptr;
    RuntimeProfile::Counter* _read_block_time = nullptr;
    RuntimeProfile::Counter* _deserialize_time = nullptr;
    RuntimeProfile::Counter* _materialize_time = nullptr;
    RuntimeProfile::Counter* _filter_time = nullptr;
};

} // namespace doris::format::native
