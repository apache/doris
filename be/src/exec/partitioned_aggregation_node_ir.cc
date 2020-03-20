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

#include "exec/partitioned_aggregation_node.h"

#include "exec/partitioned_hash_table.inline.h"
#include "runtime/buffered_tuple_stream2.inline.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"

namespace doris {

Status PartitionedAggregationNode::process_batch_no_grouping(
        RowBatch* batch, PartitionedHashTableCtx* ht_ctx) {
    for (int i = 0; i < batch->num_rows(); ++i) {
        update_tuple(&_agg_fn_ctxs[0], _singleton_output_tuple, batch->get_row(i));
    }
    return Status::OK();
}

template<bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::process_batch(RowBatch* batch, PartitionedHashTableCtx* ht_ctx) {
    DCHECK(!_hash_partitions.empty());

    // Make sure that no resizes will happen when inserting individual rows to the hash
    // table of each partition by pessimistically assuming that all the rows in each batch
    // will end up to the same partition.
    // TODO: Once we have a histogram with the number of rows per partition, we will have
    // accurate resize calls.
    int num_rows = batch->num_rows();
    RETURN_IF_ERROR(check_and_resize_hash_partitions(num_rows, ht_ctx));

    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(process_row<AGGREGATED_ROWS>(batch->get_row(i), ht_ctx));
    }

    return Status::OK();
}

template<bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::process_row(TupleRow* row, PartitionedHashTableCtx* ht_ctx) {
    uint32_t hash = 0;
    if (AGGREGATED_ROWS) {
        if (!ht_ctx->eval_and_hash_build(row, &hash)) {
            return Status::OK();
        }
    } else {
        if (!ht_ctx->eval_and_hash_probe(row, &hash)) {
            return Status::OK();
        }
    }

    // To process this row, we first see if it can be aggregated or inserted into this
    // partition's hash table. If we need to insert it and that fails, due to OOM, we
    // spill the partition. The partition to spill is not necessarily dst_partition,
    // so we can try again to insert the row.
    Partition* dst_partition = _hash_partitions[hash >> (32 - NUM_PARTITIONING_BITS)];
    if (dst_partition->is_spilled()) {
        // This partition is already spilled, just append the row.
        return append_spilled_row<AGGREGATED_ROWS>(dst_partition, row);
    }

    PartitionedHashTable* ht = dst_partition->hash_tbl.get();
    DCHECK(ht != NULL);
    DCHECK(dst_partition->aggregated_row_stream->is_pinned());
    bool found;
    // Find the appropriate bucket in the hash table. There will always be a free
    // bucket because we checked the size above.
    PartitionedHashTable::Iterator it = ht->find_bucket(ht_ctx, hash, &found);
    DCHECK(!it.at_end()) << "Hash table had no free buckets";
    if (AGGREGATED_ROWS) {
        // If the row is already an aggregate row, it cannot match anything in the
        // hash table since we process the aggregate rows first. These rows should
        // have been aggregated in the initial pass.
        DCHECK(!found);
    } else if (found) {
        // Row is already in hash table. Do the aggregation and we're done.
        update_tuple(&dst_partition->agg_fn_ctxs[0], it.get_tuple(), row);
        return Status::OK();
    }

    // If we are seeing this result row for the first time, we need to construct the
    // result row and initialize it.
    return add_intermediate_tuple<AGGREGATED_ROWS>(dst_partition, ht_ctx, row, hash, it);
}

template<bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::add_intermediate_tuple(
        Partition* partition,
        PartitionedHashTableCtx* ht_ctx,
        TupleRow* row,
        uint32_t hash,
        PartitionedHashTable::Iterator insert_it) {
    while (true) {
        DCHECK(partition->aggregated_row_stream->is_pinned());
        Tuple* intermediate_tuple = construct_intermediate_tuple(partition->agg_fn_ctxs,
                NULL, partition->aggregated_row_stream.get(), &_process_batch_status);

        if (LIKELY(intermediate_tuple != NULL)) {
            update_tuple(&partition->agg_fn_ctxs[0], intermediate_tuple, row, AGGREGATED_ROWS);
            // After copying and initializing the tuple, insert it into the hash table.
            insert_it.set_tuple(intermediate_tuple, hash);
            return Status::OK();
        } else if (!_process_batch_status.ok()) {
            return _process_batch_status;
        }

        // We did not have enough memory to add intermediate_tuple to the stream.
        RETURN_IF_ERROR(spill_partition());
        if (partition->is_spilled()) {
            return append_spilled_row<AGGREGATED_ROWS>(partition, row);
        }
    }
}

template<bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::append_spilled_row(Partition* partition, TupleRow* row) {
    DCHECK(partition->is_spilled());
    BufferedTupleStream2* stream = AGGREGATED_ROWS ?
            partition->aggregated_row_stream.get() : partition->unaggregated_row_stream.get();
    return append_spilled_row(stream, row);
}

template Status PartitionedAggregationNode::process_batch<false>(
    RowBatch*, PartitionedHashTableCtx*);
template Status PartitionedAggregationNode::process_batch<true>(
    RowBatch*, PartitionedHashTableCtx*);

} // end namespace doris
