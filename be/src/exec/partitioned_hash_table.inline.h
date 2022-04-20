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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/partitioned-hash-table.inline.h
// and modified by Doris

#ifndef DORIS_BE_SRC_EXEC_NEW_PARTITIONED_HASH_TABLE_INLINE_H
#define DORIS_BE_SRC_EXEC_NEW_PARTITIONED_HASH_TABLE_INLINE_H

#include "exec/partitioned_hash_table.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"

namespace doris {

inline bool PartitionedHashTableCtx::EvalAndHashBuild(TupleRow* row) {
    uint8_t* expr_values = expr_values_cache_.cur_expr_values();
    uint8_t* expr_values_null = expr_values_cache_.cur_expr_values_null();
    bool has_null = EvalBuildRow(row, expr_values, expr_values_null);
    if (!stores_nulls() && has_null) return false;
    expr_values_cache_.SetCurExprValuesHash(HashRow(expr_values, expr_values_null));
    return true;
}

inline bool PartitionedHashTableCtx::EvalAndHashProbe(TupleRow* row) {
    uint8_t* expr_values = expr_values_cache_.cur_expr_values();
    uint8_t* expr_values_null = expr_values_cache_.cur_expr_values_null();
    bool has_null = EvalProbeRow(row, expr_values, expr_values_null);
    if (has_null && !(stores_nulls() && finds_some_nulls())) return false;
    expr_values_cache_.SetCurExprValuesHash(HashRow(expr_values, expr_values_null));
    return true;
}

inline void PartitionedHashTableCtx::ExprValuesCache::NextRow() {
    cur_expr_values_ += expr_values_bytes_per_row_;
    cur_expr_values_null_ += num_exprs_;
    ++cur_expr_values_hash_;
    DCHECK_LE(cur_expr_values_hash_ - expr_values_hash_array_.get(), capacity_);
}

template <bool FORCE_NULL_EQUALITY>
inline int64_t PartitionedHashTable::Probe(Bucket* buckets, int64_t num_buckets,
                                           PartitionedHashTableCtx* ht_ctx, uint32_t hash,
                                           bool* found) {
    DCHECK(buckets != nullptr);
    DCHECK_GT(num_buckets, 0);
    *found = false;
    int64_t bucket_idx = hash & (num_buckets - 1);

    // In case of linear probing it counts the total number of steps for statistics and
    // for knowing when to exit the loop (e.g. by capping the total travel length). In case
    // of quadratic probing it is also used for calculating the length of the next jump.
    int64_t step = 0;
    do {
        Bucket* bucket = &buckets[bucket_idx];
        if (LIKELY(!bucket->filled)) return bucket_idx;
        if (hash == bucket->hash) {
            if (ht_ctx != nullptr &&
                ht_ctx->Equals<FORCE_NULL_EQUALITY>(GetRow(bucket, ht_ctx->scratch_row_))) {
                *found = true;
                return bucket_idx;
            }
            // Row equality failed, or not performed. This is a hash collision. Continue
            // searching.
            ++num_hash_collisions_;
        }
        // Move to the next bucket.
        ++step;
        ++travel_length_;
        if (quadratic_probing()) {
            // The i-th probe location is idx = (hash + (step * (step + 1)) / 2) mod num_buckets.
            // This gives num_buckets unique idxs (between 0 and N-1) when num_buckets is a power
            // of 2.
            bucket_idx = (bucket_idx + step) & (num_buckets - 1);
        } else {
            bucket_idx = (bucket_idx + 1) & (num_buckets - 1);
        }
    } while (LIKELY(step < num_buckets));
    DCHECK_EQ(num_filled_buckets_, num_buckets) << "Probing of a non-full table "
                                                << "failed: " << quadratic_probing() << " " << hash;
    return Iterator::BUCKET_NOT_FOUND;
}

inline PartitionedHashTable::HtData* PartitionedHashTable::InsertInternal(
        PartitionedHashTableCtx* ht_ctx, Status* status) {
    ++num_probes_;
    bool found = false;
    uint32_t hash = ht_ctx->expr_values_cache()->CurExprValuesHash();
    int64_t bucket_idx = Probe<true>(buckets_, num_buckets_, ht_ctx, hash, &found);
    DCHECK_NE(bucket_idx, Iterator::BUCKET_NOT_FOUND);
    if (found) {
        // We need to insert a duplicate node, note that this may fail to allocate memory.
        DuplicateNode* new_node = InsertDuplicateNode(bucket_idx, status);
        if (UNLIKELY(new_node == nullptr)) return nullptr;
        return &new_node->htdata;
    } else {
        PrepareBucketForInsert(bucket_idx, hash);
        return &buckets_[bucket_idx].bucketData.htdata;
    }
}

inline bool PartitionedHashTable::Insert(PartitionedHashTableCtx* ht_ctx,
                                         BufferedTupleStream3::FlatRowPtr flat_row, TupleRow* row,
                                         Status* status) {
    HtData* htdata = InsertInternal(ht_ctx, status);
    // If successful insert, update the contents of the newly inserted entry with 'idx'.
    if (LIKELY(htdata != nullptr)) {
        if (stores_tuples()) {
            htdata->tuple = row->get_tuple(0);
        } else {
            htdata->flat_row = flat_row;
        }
        return true;
    }
    return false;
}

template <const bool READ>
inline void PartitionedHashTable::PrefetchBucket(uint32_t hash) {
    int64_t bucket_idx = hash & (num_buckets_ - 1);
    // Two optional arguments:
    // 'rw': 1 means the memory access is write
    // 'locality': 0-3. 0 means no temporal locality. 3 means high temporal locality.
    // On x86, they map to instructions prefetchnta and prefetch{2-0} respectively.
    // TODO: Reconsider the locality level with smaller prefetch batch size.
    __builtin_prefetch(&buckets_[bucket_idx], READ ? 0 : 1, 1);
}

inline PartitionedHashTable::Iterator PartitionedHashTable::FindProbeRow(
        PartitionedHashTableCtx* ht_ctx) {
    ++num_probes_;
    bool found = false;
    uint32_t hash = ht_ctx->expr_values_cache()->CurExprValuesHash();
    int64_t bucket_idx = Probe<false>(buckets_, num_buckets_, ht_ctx, hash, &found);
    if (found) {
        return Iterator(this, ht_ctx->scratch_row(), bucket_idx,
                        stores_duplicates() ? buckets_[bucket_idx].bucketData.duplicates : nullptr);
    }
    return End();
}

// TODO: support lazy evaluation like HashTable::Insert().
inline PartitionedHashTable::Iterator PartitionedHashTable::FindBuildRowBucket(
        PartitionedHashTableCtx* ht_ctx, bool* found) {
    ++num_probes_;
    uint32_t hash = ht_ctx->expr_values_cache()->CurExprValuesHash();
    int64_t bucket_idx = Probe<true>(buckets_, num_buckets_, ht_ctx, hash, found);
    DuplicateNode* duplicates = nullptr;
    if (stores_duplicates() && LIKELY(bucket_idx != Iterator::BUCKET_NOT_FOUND)) {
        duplicates = buckets_[bucket_idx].bucketData.duplicates;
    }
    return Iterator(this, ht_ctx->scratch_row(), bucket_idx, duplicates);
}

inline PartitionedHashTable::Iterator PartitionedHashTable::Begin(
        const PartitionedHashTableCtx* ctx) {
    int64_t bucket_idx = Iterator::BUCKET_NOT_FOUND;
    DuplicateNode* node = nullptr;
    NextFilledBucket(&bucket_idx, &node);
    return Iterator(this, ctx->scratch_row(), bucket_idx, node);
}

inline PartitionedHashTable::Iterator PartitionedHashTable::FirstUnmatched(
        PartitionedHashTableCtx* ctx) {
    int64_t bucket_idx = Iterator::BUCKET_NOT_FOUND;
    DuplicateNode* node = nullptr;
    NextFilledBucket(&bucket_idx, &node);
    Iterator it(this, ctx->scratch_row(), bucket_idx, node);
    // Check whether the bucket, or its first duplicate node, is matched. If it is not
    // matched, then return. Otherwise, move to the first unmatched entry (node or bucket).
    Bucket* bucket = &buckets_[bucket_idx];
    bool has_duplicates = stores_duplicates() && bucket->hasDuplicates;
    if ((!has_duplicates && bucket->matched) || (has_duplicates && node->matched)) {
        it.NextUnmatched();
    }
    return it;
}

inline void PartitionedHashTable::NextFilledBucket(int64_t* bucket_idx, DuplicateNode** node) {
    ++*bucket_idx;
    for (; *bucket_idx < num_buckets_; ++*bucket_idx) {
        if (buckets_[*bucket_idx].filled) {
            *node = stores_duplicates() ? buckets_[*bucket_idx].bucketData.duplicates : nullptr;
            return;
        }
    }
    // Reached the end of the hash table.
    *bucket_idx = Iterator::BUCKET_NOT_FOUND;
    *node = nullptr;
}

inline void PartitionedHashTable::PrepareBucketForInsert(int64_t bucket_idx, uint32_t hash) {
    DCHECK_GE(bucket_idx, 0);
    DCHECK_LT(bucket_idx, num_buckets_);
    Bucket* bucket = &buckets_[bucket_idx];
    DCHECK(!bucket->filled);
    ++num_filled_buckets_;
    bucket->filled = true;
    bucket->matched = false;
    bucket->hasDuplicates = false;
    bucket->hash = hash;
}

inline PartitionedHashTable::DuplicateNode* PartitionedHashTable::AppendNextNode(Bucket* bucket) {
    DCHECK_GT(node_remaining_current_page_, 0);
    bucket->bucketData.duplicates = next_node_;
    ++num_duplicate_nodes_;
    --node_remaining_current_page_;
    return next_node_++;
}

inline PartitionedHashTable::DuplicateNode* PartitionedHashTable::InsertDuplicateNode(
        int64_t bucket_idx, Status* status) {
    DCHECK_GE(bucket_idx, 0);
    DCHECK_LT(bucket_idx, num_buckets_);
    Bucket* bucket = &buckets_[bucket_idx];
    DCHECK(bucket->filled);
    DCHECK(stores_duplicates());
    // Allocate one duplicate node for the new data and one for the preexisting data,
    // if needed.
    while (node_remaining_current_page_ < 1 + !bucket->hasDuplicates) {
        if (UNLIKELY(!GrowNodeArray(status))) return nullptr;
    }
    if (!bucket->hasDuplicates) {
        // This is the first duplicate in this bucket. It means that we need to convert
        // the current entry in the bucket to a node and link it from the bucket.
        next_node_->htdata.flat_row = bucket->bucketData.htdata.flat_row;
        DCHECK(!bucket->matched);
        next_node_->matched = false;
        next_node_->next = nullptr;
        AppendNextNode(bucket);
        bucket->hasDuplicates = true;
        ++num_buckets_with_duplicates_;
    }
    // Link a new node.
    next_node_->next = bucket->bucketData.duplicates;
    next_node_->matched = false;
    return AppendNextNode(bucket);
}

inline TupleRow* PartitionedHashTable::GetRow(HtData& htdata, TupleRow* row) const {
    if (stores_tuples()) {
        return reinterpret_cast<TupleRow*>(&htdata.tuple);
    } else {
        // TODO: GetTupleRow() has interpreted code that iterates over the row's descriptor.
        tuple_stream_->GetTupleRow(htdata.flat_row, row);
        return row;
    }
}

inline TupleRow* PartitionedHashTable::GetRow(Bucket* bucket, TupleRow* row) const {
    DCHECK(bucket != nullptr);
    if (UNLIKELY(stores_duplicates() && bucket->hasDuplicates)) {
        DuplicateNode* duplicate = bucket->bucketData.duplicates;
        DCHECK(duplicate != nullptr);
        return GetRow(duplicate->htdata, row);
    } else {
        return GetRow(bucket->bucketData.htdata, row);
    }
}

inline TupleRow* PartitionedHashTable::Iterator::GetRow() const {
    DCHECK(!AtEnd());
    DCHECK(table_ != nullptr);
    DCHECK(scratch_row_ != nullptr);
    Bucket* bucket = &table_->buckets_[bucket_idx_];
    if (UNLIKELY(table_->stores_duplicates() && bucket->hasDuplicates)) {
        DCHECK(node_ != nullptr);
        return table_->GetRow(node_->htdata, scratch_row_);
    } else {
        return table_->GetRow(bucket->bucketData.htdata, scratch_row_);
    }
}

inline Tuple* PartitionedHashTable::Iterator::GetTuple() const {
    DCHECK(!AtEnd());
    DCHECK(table_->stores_tuples());
    Bucket* bucket = &table_->buckets_[bucket_idx_];
    // TODO: To avoid the hasDuplicates check, store the HtData* in the Iterator.
    if (UNLIKELY(table_->stores_duplicates() && bucket->hasDuplicates)) {
        DCHECK(node_ != nullptr);
        return node_->htdata.tuple;
    } else {
        return bucket->bucketData.htdata.tuple;
    }
}

inline void PartitionedHashTable::Iterator::SetTuple(Tuple* tuple, uint32_t hash) {
    DCHECK(!AtEnd());
    DCHECK(table_->stores_tuples());
    table_->PrepareBucketForInsert(bucket_idx_, hash);
    table_->buckets_[bucket_idx_].bucketData.htdata.tuple = tuple;
}

inline void PartitionedHashTable::Iterator::SetMatched() {
    DCHECK(!AtEnd());
    Bucket* bucket = &table_->buckets_[bucket_idx_];
    if (table_->stores_duplicates() && bucket->hasDuplicates) {
        node_->matched = true;
    } else {
        bucket->matched = true;
    }
    // Used for disabling spilling of hash tables in right and full-outer joins with
    // matches. See IMPALA-1488.
    table_->has_matches_ = true;
}

inline bool PartitionedHashTable::Iterator::IsMatched() const {
    DCHECK(!AtEnd());
    Bucket* bucket = &table_->buckets_[bucket_idx_];
    if (table_->stores_duplicates() && bucket->hasDuplicates) {
        return node_->matched;
    }
    return bucket->matched;
}

inline void PartitionedHashTable::Iterator::SetAtEnd() {
    bucket_idx_ = BUCKET_NOT_FOUND;
    node_ = nullptr;
}

template <const bool READ>
inline void PartitionedHashTable::Iterator::PrefetchBucket() {
    if (LIKELY(!AtEnd())) {
        // HashTable::PrefetchBucket() takes a hash value to index into the hash bucket
        // array. Passing 'bucket_idx_' here is sufficient.
        DCHECK_EQ((bucket_idx_ & ~(table_->num_buckets_ - 1)), 0);
        table_->PrefetchBucket<READ>(bucket_idx_);
    }
}

inline void PartitionedHashTable::Iterator::Next() {
    DCHECK(!AtEnd());
    if (table_->stores_duplicates() && table_->buckets_[bucket_idx_].hasDuplicates &&
        node_->next != nullptr) {
        node_ = node_->next;
    } else {
        table_->NextFilledBucket(&bucket_idx_, &node_);
    }
}

inline void PartitionedHashTable::Iterator::NextDuplicate() {
    DCHECK(!AtEnd());
    if (table_->stores_duplicates() && table_->buckets_[bucket_idx_].hasDuplicates &&
        node_->next != nullptr) {
        node_ = node_->next;
    } else {
        bucket_idx_ = BUCKET_NOT_FOUND;
        node_ = nullptr;
    }
}

inline void PartitionedHashTable::Iterator::NextUnmatched() {
    DCHECK(!AtEnd());
    Bucket* bucket = &table_->buckets_[bucket_idx_];
    // Check if there is any remaining unmatched duplicate node in the current bucket.
    if (table_->stores_duplicates() && bucket->hasDuplicates) {
        while (node_->next != nullptr) {
            node_ = node_->next;
            if (!node_->matched) return;
        }
    }
    // Move to the next filled bucket and return if this bucket is not matched or
    // iterate to the first not matched duplicate node.
    table_->NextFilledBucket(&bucket_idx_, &node_);
    while (bucket_idx_ != Iterator::BUCKET_NOT_FOUND) {
        bucket = &table_->buckets_[bucket_idx_];
        if (!table_->stores_duplicates() || !bucket->hasDuplicates) {
            if (!bucket->matched) return;
        } else {
            while (node_->matched && node_->next != nullptr) {
                node_ = node_->next;
            }
            if (!node_->matched) return;
        }
        table_->NextFilledBucket(&bucket_idx_, &node_);
    }
}

inline void PartitionedHashTableCtx::set_level(int level) {
    DCHECK_GE(level, 0);
    DCHECK_LT(level, seeds_.size());
    level_ = level;
}

inline int64_t PartitionedHashTable::CurrentMemSize() const {
    return num_buckets_ * sizeof(Bucket) + num_duplicate_nodes_ * sizeof(DuplicateNode);
}

inline int64_t PartitionedHashTable::NumInsertsBeforeResize() const {
    return std::max<int64_t>(
            0, static_cast<int64_t>(num_buckets_ * MAX_FILL_FACTOR) - num_filled_buckets_);
}

} // namespace doris

#endif
