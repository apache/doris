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
#include "olap/wal_reader.h"
#include "vec/exec/format/generic_reader.h"
namespace doris {
namespace vectorized {
struct ScannerCounter;
class WalReader : public GenericReader {
public:
    WalReader(RuntimeState* state);
    ~WalReader() override;
    Status init_reader();
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;
    static void string_split(const std::string& str, const std::string& splits,
                             std::vector<std::string>& res);

private:
    RuntimeState* _state;
    std::string _wal_path;
    std::string _path_split = "/";
    int64_t _wal_id;
    std::shared_ptr<doris::WalReader> _wal_reader = nullptr;
};
} // namespace vectorized
} // namespace doris
