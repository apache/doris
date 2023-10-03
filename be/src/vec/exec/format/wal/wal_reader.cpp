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

#include "wal_reader.h"

#include "common/logging.h"
#include "olap/wal_manager.h"
#include "runtime/runtime_state.h"
namespace doris::vectorized {
WalReader::WalReader(RuntimeState* state) : _state(state) {
    _wal_id = state->wal_id();
}
WalReader::~WalReader() {
    if (_wal_reader.get() != nullptr) {
        static_cast<void>(_wal_reader->finalize());
    }
}
Status WalReader::init_reader() {
    RETURN_IF_ERROR(_state->exec_env()->wal_mgr()->get_wal_path(_wal_id, _wal_path));
    RETURN_IF_ERROR(_state->exec_env()->wal_mgr()->create_wal_reader(_wal_path, _wal_reader));
    return Status::OK();
}
Status WalReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    PBlock pblock;
    auto st = _wal_reader->read_block(pblock);
    if (st.is<ErrorCode::END_OF_FILE>()) {
        LOG(INFO) << "read eof on wal:" << _wal_path;
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    if (!st.ok()) {
        LOG(WARNING) << "Failed to read wal on path = " << _wal_path;
        return st;
    }
    vectorized::Block tmp_block;
    static_cast<void>(tmp_block.deserialize(pblock));
    block->swap(tmp_block);
    *read_rows = block->rows();
    VLOG_DEBUG << "read block rows:" << *read_rows;
    return Status::OK();
}

void WalReader::string_split(const std::string& str, const std::string& splits,
                             std::vector<std::string>& res) {
    if (str == "") return;
    std::string strs = str + splits;
    size_t pos = strs.find(splits);
    int step = splits.size();
    while (pos != strs.npos) {
        std::string temp = strs.substr(0, pos);
        res.push_back(temp);
        strs = strs.substr(pos + step, strs.size());
        pos = strs.find(splits);
    }
}

Status WalReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                              std::unordered_set<std::string>* missing_cols) {
    return Status::OK();
}

} // namespace doris::vectorized