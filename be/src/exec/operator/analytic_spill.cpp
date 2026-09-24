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

#include "exec/operator/analytic_spill.h"

#include <fmt/format.h>

#include <utility>

#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_writer.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris {

AnalyticPartitionStore::AnalyticPartitionStore(RuntimeProfile* profile, int node_id,
                                               bool has_peer_groups)
        : _profile(profile), _node_id(node_id), _has_peer_groups(has_peer_groups) {}

AnalyticPartitionStore::~AnalyticPartitionStore() {
    if (_peer_group_writer) {
        (void)_peer_group_writer->close();
    }
    if (_data_writer) {
        (void)_data_writer->close();
    }
}

Status AnalyticPartitionStore::_create_writer(RuntimeState* state, const char* label,
                                              SpillFileSPtr& file, SpillFileWriterSPtr& writer) {
    auto relative_path =
            fmt::format("{}/{}-{}-{}-{}", print_id(state->query_id()), label, _node_id,
                        state->task_id(), ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    RETURN_IF_ERROR(
            ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path, file));
    return file->create_writer(state, _profile, writer);
}

Status AnalyticPartitionStore::append_block(RuntimeState* state, Block block) {
    DCHECK_GT(block.rows(), 0);
    _rows += block.rows();
    if (_data_writer) {
        return _data_writer->write_block(state, block);
    }
    _blocks_memory_bytes += block.allocated_bytes();
    _blocks.emplace_back(std::move(block));
    return Status::OK();
}

Status AnalyticPartitionStore::_ensure_peer_group_writer(RuntimeState* state) {
    DCHECK(_has_peer_groups);
    if (_peer_group_writer) {
        return Status::OK();
    }
    return _create_writer(state, "analytic-peer", _peer_group_file, _peer_group_writer);
}

Status AnalyticPartitionStore::_flush_peer_group_ends(RuntimeState* state) {
    if (_peer_group_ends.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_ensure_peer_group_writer(state));
    auto column = ColumnInt64::create();
    column->get_data().assign(_peer_group_ends.begin(), _peer_group_ends.end());
    Block block;
    block.insert({std::move(column), std::make_shared<DataTypeInt64>(), "peer_group_end"});
    RETURN_IF_ERROR(_peer_group_writer->write_block(state, block));
    _release_peer_group_buffer();
    return Status::OK();
}

Status AnalyticPartitionStore::append_peer_group_end(RuntimeState* state, int64_t group_end) {
    DCHECK(_has_peer_groups);
    DCHECK_GT(group_end, 0);
    DCHECK_LE(group_end, _rows);
    _peer_group_ends.push_back(group_end);
    ++_peer_group_count;
    if (_peer_group_ends.size() * sizeof(int64_t) >= state->spill_buffer_size_bytes()) {
        RETURN_IF_ERROR(_flush_peer_group_ends(state));
    }
    return Status::OK();
}

void AnalyticPartitionStore::_release_blocks() {
    std::vector<Block>().swap(_blocks);
    _blocks_memory_bytes = 0;
}

void AnalyticPartitionStore::_release_peer_group_buffer() {
    std::vector<int64_t>().swap(_peer_group_ends);
}

Status AnalyticPartitionStore::spill(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);
    if (!_data_writer) {
        RETURN_IF_ERROR(_create_writer(state, "analytic", _data_file, _data_writer));
    }
    for (const auto& block : _blocks) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(_data_writer->write_block(state, block));
    }
    _release_blocks();
    if (_has_peer_groups) {
        RETURN_IF_ERROR(_flush_peer_group_ends(state));
    }
    return Status::OK();
}

size_t AnalyticPartitionStore::revocable_mem_size() const {
    return _blocks_memory_bytes + _peer_group_ends.capacity() * sizeof(int64_t);
}

Status AnalyticPartitionStore::seal(RuntimeState* state,
                                    std::shared_ptr<AnalyticSpillPartition>* partition) {
    RETURN_IF_CANCELLED(state);
    DCHECK_GT(_rows, 0);
    if (_data_writer) {
        RETURN_IF_ERROR(_data_writer->close());
        _data_writer.reset();
    }
    if (_peer_group_writer) {
        RETURN_IF_ERROR(_flush_peer_group_ends(state));
        RETURN_IF_ERROR(_peer_group_writer->close());
        _peer_group_writer.reset();
    }

    auto result = std::make_shared<AnalyticSpillPartition>();
    result->rows = _rows;
    result->blocks = std::move(_blocks);
    result->data_file = std::move(_data_file);
    result->peer_group_ends = std::move(_peer_group_ends);
    result->peer_group_file = std::move(_peer_group_file);
    *partition = std::move(result);
    _blocks_memory_bytes = 0;
    return Status::OK();
}

} // namespace doris
