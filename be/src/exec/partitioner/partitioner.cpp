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

#include "exec/partitioner/partitioner.h"

#include "agent/be_exec_version_manager.h"
#include "common/cast_set.h"
#include "common/status.h"
#include "core/column/column_const.h"
#include "exec/common/hash_table/hash_key_normalize.h"
#include "exec/exchange/local_exchange_sink_operator.h"
#include "exec/exchange/vdata_stream_sender.h"
#include "runtime/thread_context.h"

namespace doris {

template <typename ChannelIds>
Status Crc32HashPartitioner<ChannelIds>::do_partitioning(RuntimeState* state, Block* block) const {
    size_t rows = block->rows();

    if (rows > 0) {
        auto column_to_keep = block->columns();

        int result_size = cast_set<int>(_partition_expr_ctxs.size());
        std::vector<int> result(result_size);

        _initialize_hash_vals(rows);
        auto* __restrict hashes = _hash_vals.data();
        RETURN_IF_ERROR(_get_partition_column_result(block, result));
        // Equal float keys (-0.0 == +0.0, any NaN) must reach the same channel. Every BE of a
        // query hashes with the same be_exec_version, so mixed-version senders during a rolling
        // upgrade keep the legacy raw-bit convention until FE raises the version.
        const bool normalize_float_keys =
                state->be_exec_version() >= NORMALIZE_FLOAT_HASH_KEY_VERSION;
        for (int j = 0; j < result_size; ++j) {
            const auto& [col, is_const] = unpack_if_const(block->get_by_position(result[j]).column);
            if (is_const) {
                continue;
            }
            if (normalize_float_keys) {
                // The block keeps its own reference, so a float column is hashed from a
                // normalized copy and the rows sent downstream are unchanged.
                ColumnPtr key = col;
                normalize_float_hash_key(key, _partition_expr_ctxs[j]->root()->data_type());
                _do_hash(key, hashes, j);
            } else {
                _do_hash(col, hashes, j);
            }
        }

        for (size_t i = 0; i < rows; i++) {
            hashes[i] = ChannelIds()(hashes[i], _partition_count);
        }

        Block::erase_useless_column(block, column_to_keep);
    }
    return Status::OK();
}

template <typename ChannelIds>
void Crc32HashPartitioner<ChannelIds>::_do_hash(const ColumnPtr& column,
                                                HashValType* __restrict result, int idx) const {
    column->update_crcs_with_value(
            result, _partition_expr_ctxs[idx]->root()->data_type()->get_primitive_type(),
            cast_set<HashValType>(column->size()));
}

template <typename ChannelIds>
Status Crc32HashPartitioner<ChannelIds>::clone(RuntimeState* state,
                                               std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner = new Crc32HashPartitioner<ChannelIds>(_partition_count);
    partitioner.reset(new_partitioner);
    return _clone_expr_ctxs(state, new_partitioner->_partition_expr_ctxs);
}

void Crc32CHashPartitioner::_do_hash(const ColumnPtr& column, HashValType* __restrict result,
                                     int idx) const {
    column->update_crc32c_batch(result, nullptr);
}

Status Crc32CHashPartitioner::clone(RuntimeState* state,
                                    std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner = new Crc32CHashPartitioner(_partition_count);
    partitioner.reset(new_partitioner);
    return _clone_expr_ctxs(state, new_partitioner->_partition_expr_ctxs);
}

template class Crc32HashPartitioner<ShuffleChannelIds>;
template class Crc32HashPartitioner<SpillPartitionChannelIds>;
template class Crc32HashPartitioner<SpillRePartitionChannelIds>;

} // namespace doris
