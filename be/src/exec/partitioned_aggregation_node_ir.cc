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
#include "exprs/expr_context.h"
#include "exprs/new_agg_fn_evaluator.h"
#include "runtime/buffered_tuple_stream3.inline.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"

using namespace doris;

Status PartitionedAggregationNode::ProcessBatchNoGrouping(RowBatch* batch) {
    Tuple* output_tuple = singleton_output_tuple_;
    FOREACH_ROW(batch, 0, batch_iter) {
        UpdateTuple(agg_fn_evals_.data(), output_tuple, batch_iter.get());
    }
    return Status::OK();
}

template <bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::ProcessBatch(RowBatch* batch, PartitionedHashTableCtx* ht_ctx) {
    DCHECK(!hash_partitions_.empty());
    DCHECK(!is_streaming_preagg_);

    // Make sure that no resizes will happen when inserting individual rows to the hash
    // table of each partition by pessimistically assuming that all the rows in each batch
    // will end up to the same partition.
    // TODO: Once we have a histogram with the number of rows per partition, we will have
    // accurate resize calls.
    RETURN_IF_ERROR(CheckAndResizeHashPartitions(AGGREGATED_ROWS, batch->num_rows(), ht_ctx));

    PartitionedHashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
    const int cache_size = expr_vals_cache->capacity();
    const int num_rows = batch->num_rows();
    for (int group_start = 0; group_start < num_rows; group_start += cache_size) {
        EvalAndHashPrefetchGroup<AGGREGATED_ROWS>(batch, group_start, ht_ctx);

        FOREACH_ROW_LIMIT(batch, group_start, cache_size, batch_iter) {
            RETURN_IF_ERROR(ProcessRow<AGGREGATED_ROWS>(batch_iter.get(), ht_ctx));
            expr_vals_cache->NextRow();
        }
        ht_ctx->expr_results_pool_->clear();
        DCHECK(expr_vals_cache->AtEnd());
    }
    return Status::OK();
}

template <bool AGGREGATED_ROWS>
void IR_ALWAYS_INLINE PartitionedAggregationNode::EvalAndHashPrefetchGroup(
        RowBatch* batch, int start_row_idx, PartitionedHashTableCtx* ht_ctx) {
    PartitionedHashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
    const int cache_size = expr_vals_cache->capacity();

    expr_vals_cache->Reset();
    FOREACH_ROW_LIMIT(batch, start_row_idx, cache_size, batch_iter) {
        TupleRow* row = batch_iter.get();
        bool is_null;
        if (AGGREGATED_ROWS) {
            is_null = !ht_ctx->EvalAndHashBuild(row);
        } else {
            is_null = !ht_ctx->EvalAndHashProbe(row);
        }
        // Hoist lookups out of non-null branch to speed up non-null case.
        const uint32_t hash = expr_vals_cache->CurExprValuesHash();
        const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
        PartitionedHashTable* hash_tbl = GetHashTable(partition_idx);
        if (is_null) {
            expr_vals_cache->SetRowNull();
        } else if (config::enable_prefetch) {
            if (LIKELY(hash_tbl != NULL)) hash_tbl->PrefetchBucket<false>(hash);
        }
        expr_vals_cache->NextRow();
    }

    expr_vals_cache->ResetForRead();
}

template <bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::ProcessRow(TupleRow* row, PartitionedHashTableCtx* ht_ctx) {
    PartitionedHashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
    // Hoist lookups out of non-null branch to speed up non-null case.
    const uint32_t hash = expr_vals_cache->CurExprValuesHash();
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    if (expr_vals_cache->IsRowNull()) return Status::OK();
    // To process this row, we first see if it can be aggregated or inserted into this
    // partition's hash table. If we need to insert it and that fails, due to OOM, we
    // spill the partition. The partition to spill is not necessarily dst_partition,
    // so we can try again to insert the row.
    PartitionedHashTable* hash_tbl = GetHashTable(partition_idx);
    Partition* dst_partition = hash_partitions_[partition_idx];
    DCHECK(dst_partition != nullptr);
    DCHECK_EQ(dst_partition->is_spilled(), hash_tbl == NULL);
    if (hash_tbl == NULL) {
        // This partition is already spilled, just append the row.
        return AppendSpilledRow<AGGREGATED_ROWS>(dst_partition, row);
    }

    DCHECK(dst_partition->aggregated_row_stream->is_pinned());
    bool found;
    // Find the appropriate bucket in the hash table. There will always be a free
    // bucket because we checked the size above.
    PartitionedHashTable::Iterator it = hash_tbl->FindBuildRowBucket(ht_ctx, &found);
    DCHECK(!it.AtEnd()) << "Hash table had no free buckets";
    if (AGGREGATED_ROWS) {
        // If the row is already an aggregate row, it cannot match anything in the
        // hash table since we process the aggregate rows first. These rows should
        // have been aggregated in the initial pass.
        DCHECK(!found);
    } else if (found) {
        // Row is already in hash table. Do the aggregation and we're done.
        UpdateTuple(dst_partition->agg_fn_evals.data(), it.GetTuple(), row);
        return Status::OK();
    }

    // If we are seeing this result row for the first time, we need to construct the
    // result row and initialize it.
    return AddIntermediateTuple<AGGREGATED_ROWS>(dst_partition, row, hash, it);
}

template <bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::AddIntermediateTuple(Partition* partition, TupleRow* row,
                                                        uint32_t hash,
                                                        PartitionedHashTable::Iterator insert_it) {
    while (true) {
        DCHECK(partition->aggregated_row_stream->is_pinned());
        Tuple* intermediate_tuple = ConstructIntermediateTuple(
                partition->agg_fn_evals, partition->aggregated_row_stream.get(),
                &process_batch_status_);

        if (LIKELY(intermediate_tuple != NULL)) {
            UpdateTuple(partition->agg_fn_evals.data(), intermediate_tuple, row, AGGREGATED_ROWS);
            // After copying and initializing the tuple, insert it into the hash table.
            insert_it.SetTuple(intermediate_tuple, hash);
            return Status::OK();
        } else if (!process_batch_status_.ok()) {
            return std::move(process_batch_status_);
        }

        // We did not have enough memory to add intermediate_tuple to the stream.
        RETURN_IF_ERROR(SpillPartition(AGGREGATED_ROWS));
        if (partition->is_spilled()) {
            return AppendSpilledRow<AGGREGATED_ROWS>(partition, row);
        }
    }
}

Status PartitionedAggregationNode::ProcessBatchStreaming(bool needs_serialize, RowBatch* in_batch,
                                                         RowBatch* out_batch,
                                                         PartitionedHashTableCtx* ht_ctx,
                                                         int remaining_capacity[PARTITION_FANOUT]) {
    DCHECK(is_streaming_preagg_);
    DCHECK_EQ(out_batch->num_rows(), 0);
    DCHECK_LE(in_batch->num_rows(), out_batch->capacity());

    RowBatch::Iterator out_batch_iterator(out_batch, out_batch->num_rows());
    PartitionedHashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
    const int num_rows = in_batch->num_rows();
    const int cache_size = expr_vals_cache->capacity();
    for (int group_start = 0; group_start < num_rows; group_start += cache_size) {
        EvalAndHashPrefetchGroup<false>(in_batch, group_start, ht_ctx);

        FOREACH_ROW_LIMIT(in_batch, group_start, cache_size, in_batch_iter) {
            // Hoist lookups out of non-null branch to speed up non-null case.
            TupleRow* in_row = in_batch_iter.get();
            const uint32_t hash = expr_vals_cache->CurExprValuesHash();
            const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
            if (!expr_vals_cache->IsRowNull() &&
                !TryAddToHashTable(ht_ctx, hash_partitions_[partition_idx],
                                   GetHashTable(partition_idx), in_row, hash,
                                   &remaining_capacity[partition_idx], &process_batch_status_)) {
                RETURN_IF_ERROR(std::move(process_batch_status_));
                // Tuple is not going into hash table, add it to the output batch.
                Tuple* intermediate_tuple = ConstructIntermediateTuple(
                        agg_fn_evals_, out_batch->tuple_data_pool(), &process_batch_status_);
                if (UNLIKELY(intermediate_tuple == NULL)) {
                    DCHECK(!process_batch_status_.ok());
                    return std::move(process_batch_status_);
                }
                UpdateTuple(agg_fn_evals_.data(), intermediate_tuple, in_row);
                out_batch_iterator.get()->set_tuple(0, intermediate_tuple);
                out_batch_iterator.next();
                out_batch->commit_last_row();
            }
            DCHECK(process_batch_status_.ok());
            expr_vals_cache->NextRow();
        }
        ht_ctx->expr_results_pool_->clear();
        DCHECK(expr_vals_cache->AtEnd());
    }
    if (needs_serialize) {
        FOREACH_ROW(out_batch, 0, out_batch_iter) {
            NewAggFnEvaluator::Serialize(agg_fn_evals_, out_batch_iter.get()->get_tuple(0));
        }
    }

    return Status::OK();
}

bool PartitionedAggregationNode::TryAddToHashTable(PartitionedHashTableCtx* ht_ctx,
                                                   Partition* partition,
                                                   PartitionedHashTable* hash_tbl, TupleRow* in_row,
                                                   uint32_t hash, int* remaining_capacity,
                                                   Status* status) {
    DCHECK(remaining_capacity != NULL);
    DCHECK_EQ(hash_tbl, partition->hash_tbl.get());
    DCHECK_GE(*remaining_capacity, 0);
    bool found;
    // This is called from ProcessBatchStreaming() so the rows are not aggregated.
    PartitionedHashTable::Iterator it = hash_tbl->FindBuildRowBucket(ht_ctx, &found);
    Tuple* intermediate_tuple;
    if (found) {
        intermediate_tuple = it.GetTuple();
    } else if (*remaining_capacity == 0) {
        return false;
    } else {
        intermediate_tuple = ConstructIntermediateTuple(
                partition->agg_fn_evals, partition->aggregated_row_stream.get(), status);
        if (LIKELY(intermediate_tuple != NULL)) {
            it.SetTuple(intermediate_tuple, hash);
            --(*remaining_capacity);
        } else {
            // Avoid repeatedly trying to add tuples when under memory pressure.
            *remaining_capacity = 0;
            return false;
        }
    }
    UpdateTuple(partition->agg_fn_evals.data(), intermediate_tuple, in_row);
    return true;
}

// Instantiate required templates.
template Status PartitionedAggregationNode::ProcessBatch<false>(RowBatch*,
                                                                PartitionedHashTableCtx*);
template Status PartitionedAggregationNode::ProcessBatch<true>(RowBatch*, PartitionedHashTableCtx*);
