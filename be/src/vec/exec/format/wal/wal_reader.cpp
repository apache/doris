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

#include "agent/be_exec_version_manager.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "gutil/strings/split.h"
#include "olap/wal/wal_manager.h"
#include "runtime/runtime_state.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
WalReader::WalReader(RuntimeState* state) : _state(state) {
    _wal_id = state->wal_id();
}

Status WalReader::init_reader(const TupleDescriptor* tuple_descriptor) {
    _tuple_descriptor = tuple_descriptor;
    RETURN_IF_ERROR(_state->exec_env()->wal_mgr()->get_wal_path(_wal_id, _wal_path));
    _wal_reader = std::make_shared<doris::WalReader>(_wal_path);
    RETURN_IF_ERROR(_wal_reader->init());
    return Status::OK();
}

Status WalReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    //read src block
    PBlock pblock;
    auto st = _wal_reader->read_block(pblock);
    // Due to historical reasons, be_exec_version=3 will use the new way to serialize block
    // in doris 2.1.0, now it has been corrected to use the old way to do serialize and deserialize
    // in the latest version. So if a wal is created by 2.1.0 (wal version=0 && be_exec_version=3),
    // it should upgrade the be_exec_version to 4 to use the new way to deserialize pblock to solve
    // compatibility issues.see https://github.com/apache/doris/pull/32299
    if (_version == 0 && pblock.has_be_exec_version() &&
        pblock.be_exec_version() == OLD_WAL_SERDE) {
        VLOG_DEBUG << "need to set be_exec_version to 4 to solve compatibility issues";
        pblock.set_be_exec_version(USE_NEW_SERDE);
    }
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
    int be_exec_version = pblock.has_be_exec_version() ? pblock.be_exec_version() : 0;
    if (!BeExecVersionManager::check_be_exec_version(be_exec_version)) {
        return Status::DataQualityError("check be exec version fail when reading wal file {}",
                                        _wal_path);
    }
    vectorized::Block src_block;
    RETURN_IF_ERROR(src_block.deserialize(pblock));
    //convert to dst block
    vectorized::Block dst_block;
    int index = 0;
    auto output_block_columns = block->get_columns_with_type_and_name();
    size_t output_block_column_size = output_block_columns.size();
    TEST_SYNC_POINT_CALLBACK("WalReader::set_column_id_count", &_column_id_count);
    TEST_SYNC_POINT_CALLBACK("WalReader::set_out_block_column_size", &output_block_column_size);
    if (_column_id_count != src_block.columns() ||
        output_block_column_size != _tuple_descriptor->slots().size()) {
        return Status::InternalError(
                "not equal wal _column_id_count={} vs wal block columns size={}, "
                "output block columns size={} vs tuple_descriptor size={}",
                std::to_string(_column_id_count), std::to_string(src_block.columns()),
                std::to_string(output_block_column_size),
                std::to_string(_tuple_descriptor->slots().size()));
    }
    for (auto slot_desc : _tuple_descriptor->slots()) {
        auto pos = _column_pos_map[slot_desc->col_unique_id()];
        if (pos >= src_block.columns()) {
            return Status::InternalError("read wal {} fail, pos {}, columns size {}", _wal_path,
                                         pos, src_block.columns());
        }
        vectorized::ColumnPtr column_ptr = src_block.get_by_position(pos).column;
        if (column_ptr != nullptr && slot_desc->is_nullable()) {
            column_ptr = make_nullable(column_ptr);
        }
        dst_block.insert(index, vectorized::ColumnWithTypeAndName(
                                        std::move(column_ptr), output_block_columns[index].type,
                                        output_block_columns[index].name));
        index++;
    }
    block->swap(dst_block);
    *read_rows = block->rows();
    VLOG_DEBUG << "read block rows:" << *read_rows;
    return Status::OK();
}

Status WalReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                              std::unordered_set<std::string>* missing_cols) {
    std::string col_ids;
    RETURN_IF_ERROR(_wal_reader->read_header(_version, col_ids));
    std::vector<std::string> column_id_vector =
            strings::Split(col_ids, ",", strings::SkipWhitespace());
    _column_id_count = column_id_vector.size();
    try {
        int64_t pos = 0;
        for (auto col_id_str : column_id_vector) {
            auto col_id = std::strtoll(col_id_str.c_str(), NULL, 10);
            _column_pos_map.emplace(col_id, pos);
            pos++;
        }
    } catch (const std::invalid_argument& e) {
        return Status::InvalidArgument("Invalid format, {}", e.what());
    }
    return Status::OK();
}

} // namespace doris::vectorized