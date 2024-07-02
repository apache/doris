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
#include "olap/wal/wal_reader.h"
#include "runtime/descriptors.h"
#include "vec/exec/format/generic_reader.h"

namespace doris {
namespace vectorized {
struct ScannerCounter;
class WalReader : public GenericReader {
public:
    WalReader(RuntimeState* state);
    ~WalReader() override = default;
    Status init_reader(const TupleDescriptor* tuple_descriptor);
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    Status close() override {
        if (_wal_reader) {
            return _wal_reader->finalize();
        }
        return Status::OK();
    }

private:
    RuntimeState* _state = nullptr;
    int64_t _wal_id;
    std::string _wal_path;
    std::shared_ptr<doris::WalReader> _wal_reader = nullptr;
    const TupleDescriptor* _tuple_descriptor = nullptr;
    // column_id, column_pos
    std::map<int64_t, int64_t> _column_pos_map;
    int64_t _column_id_count;
    uint32_t _version = 0;
};
} // namespace vectorized
} // namespace doris
