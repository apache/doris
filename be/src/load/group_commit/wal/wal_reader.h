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
#include "format/table/table_format_reader.h"
#include "load/group_commit/wal/wal_file_reader.h"
#include "runtime/descriptors.h"

namespace doris {
struct ScannerCounter;

/// WAL-specific initialization context.
/// Extends ReaderInitContext with output tuple descriptor (unique to WAL reader).
struct WalInitContext final : public ReaderInitContext {
    const TupleDescriptor* output_tuple_descriptor = nullptr;
};

class WalReader : public TableFormatReader {
    ENABLE_FACTORY_CREATOR(WalReader);

public:
    WalReader(RuntimeState* state);
    ~WalReader() override = default;
    Status init_reader(const TupleDescriptor* tuple_descriptor);
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override;

    Status close() override {
        if (_wal_reader) {
            return _wal_reader->finalize();
        }
        return Status::OK();
    }

protected:
    // ---- Unified init_reader(ReaderInitContext*) overrides ----
    Status _open_file_reader(ReaderInitContext* ctx) override;
    Status _do_init_reader(ReaderInitContext* ctx) override;

private:
    RuntimeState* _state = nullptr;
    int64_t _wal_id;
    std::string _wal_path;
    std::shared_ptr<doris::WalFileReader> _wal_reader = nullptr;
    const TupleDescriptor* _tuple_descriptor = nullptr;
    // column_id, column_pos
    std::map<int64_t, int64_t> _column_pos_map;
    int64_t _column_id_count;
    uint32_t _version = 0;
};
} // namespace doris
