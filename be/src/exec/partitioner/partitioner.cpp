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

#include "common/cast_set.h"
#include "common/status.h"
#include "core/column/column_const.h"
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
        for (int j = 0; j < result_size; ++j) {
            const auto& [col, is_const] = unpack_if_const(block->get_by_position(result[j]).column);
            if (is_const) {
                continue;
            }
            _do_hash(col, hashes, j);
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

void IdentityHashPartitioner::_do_hash(const ColumnPtr& column, HashValType* __restrict result,
                                       int idx) const {
    // Keep this bit-identical with tablet_info.cpp::_compute_tablet_index_for_identity and FE
    // HashDistributionPruner: single integer column, NULL -> bucket 0, negative-safe modulo.
    const __int128 n = _partition_count;
    const PrimitiveType type = _partition_expr_ctxs[idx]->root()->data_type()->get_primitive_type();
    const size_t rows = column->size();
    for (size_t row = 0; row < rows; ++row) {
        auto val = column->get_data_at(row);
        if (val.data == nullptr) {
            result[row] = 0;
            continue;
        }
        __int128 v = 0;
        switch (type) {
        case TYPE_TINYINT:
            v = *reinterpret_cast<const int8_t*>(val.data);
            break;
        case TYPE_SMALLINT:
            v = *reinterpret_cast<const int16_t*>(val.data);
            break;
        case TYPE_INT:
            v = *reinterpret_cast<const int32_t*>(val.data);
            break;
        case TYPE_BIGINT:
            v = *reinterpret_cast<const int64_t*>(val.data);
            break;
        case TYPE_LARGEINT:
            memcpy(&v, val.data, sizeof(__int128));
            break;
        default:
            LOG(WARNING) << "identity distribution on non-integer column, primitive_type=" << type;
            result[row] = 0;
            continue;
        }
        result[row] = cast_set<HashValType>(((v % n) + n) % n);
    }
}

Status IdentityHashPartitioner::clone(RuntimeState* state,
                                      std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner = new IdentityHashPartitioner(_partition_count);
    partitioner.reset(new_partitioner);
    return _clone_expr_ctxs(state, new_partitioner->_partition_expr_ctxs);
}

template class Crc32HashPartitioner<ShuffleChannelIds>;
template class Crc32HashPartitioner<SpillPartitionChannelIds>;
template class Crc32HashPartitioner<SpillRePartitionChannelIds>;

} // namespace doris
