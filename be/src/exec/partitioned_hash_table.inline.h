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

#ifndef DORIS_BE_SRC_EXEC_PARTITIONED_HASH_TABLE_INLINE_H
#define DORIS_BE_SRC_EXEC_PARTITIONED_HASH_TABLE_INLINE_H

#include "exec/partitioned_hash_table.h"

namespace doris {

inline bool PartitionedHashTableCtx::eval_and_hash_build(TupleRow* row, uint32_t* hash) {
    bool has_null = EvalBuildRow(row);
    if (!_stores_nulls && has_null) {
        return false;
    }
    *hash = HashCurrentRow();
    return true;
}

inline bool PartitionedHashTableCtx::eval_and_hash_probe(TupleRow* row, uint32_t* hash) {
    bool has_null = EvalProbeRow(row);
    if ((!_stores_nulls || !_finds_nulls) && has_null) {
        return false;
    }
    *hash = HashCurrentRow();
    return true;
}

inline int64_t PartitionedHashTable::probe(Bucket* buckets, int64_t num_buckets,
        PartitionedHashTableCtx* ht_ctx, uint32_t hash, bool* found) {
    DCHECK(buckets != NULL);
    DCHECK_GT(num_buckets, 0);
    *found = false;
    int64_t bucket_idx = hash & (num_buckets - 1);

    // In case of linear probing it counts the total number of steps for statistics and
    // for knowing when to exit the loop (e.g. by capping the total travel length). In case
    // of quadratic probing it is also used for calculating the length of the next jump.
    int64_t step = 0;
    do {
        Bucket* bucket = &buckets[bucket_idx];
        if (!bucket->filled) {
            return bucket_idx;
        }
        if (hash == bucket->hash) {
            if (ht_ctx != NULL && ht_ctx->equals(get_row(bucket, ht_ctx->_row))) {
                *found = true;
                return bucket_idx;
            }
            // Row equality failed, or not performed. This is a hash collision. Continue
            // searching.
            ++_num_hash_collisions;
        }
        // Move to the next bucket.
        ++step;
        ++_travel_length;
        if (_quadratic_probing) {
            // The i-th probe location is idx = (hash + (step * (step + 1)) / 2) mod num_buckets.
            // This gives num_buckets unique idxs (between 0 and N-1) when num_buckets is a power
            // of 2.
            bucket_idx = (bucket_idx + step) & (num_buckets - 1);
        } else {
            bucket_idx = (bucket_idx + 1) & (num_buckets - 1);
        }
    } while (LIKELY(step < num_buckets));
    DCHECK_EQ(_num_filled_buckets, num_buckets) << "Probing of a non-full table "
        << "failed: " << _quadratic_probing << " " << hash;
    return Iterator::BUCKET_NOT_FOUND;
}

inline PartitionedHashTable::HtData* PartitionedHashTable::insert_internal(
        PartitionedHashTableCtx* ht_ctx, uint32_t hash) {
    ++_num_probes;
    bool found = false;
    int64_t bucket_idx = probe(_buckets, _num_buckets, ht_ctx, hash, &found);
    DCHECK_NE(bucket_idx, Iterator::BUCKET_NOT_FOUND);
    if (found) {
        // We need to insert a duplicate node, note that this may fail to allocate memory.
        DuplicateNode* new_node = insert_duplicate_node(bucket_idx);
        if (UNLIKELY(new_node == NULL)) {
            return NULL;
        }
        return &new_node->htdata;
    } else {
        prepare_bucket_for_insert(bucket_idx, hash);
        return &_buckets[bucket_idx].bucketData.htdata;
    }
}

inline bool PartitionedHashTable::insert(PartitionedHashTableCtx* ht_ctx,
        const BufferedTupleStream2::RowIdx& idx, TupleRow* row, uint32_t hash) {
    if (_stores_tuples) {
        return insert(ht_ctx, row->get_tuple(0), hash);
    }
    HtData* htdata = insert_internal(ht_ctx, hash);
    // If successful insert, update the contents of the newly inserted entry with 'idx'.
    if (LIKELY(htdata != NULL)) {
        htdata->idx = idx;
        return true;
    }
    return false;
}

inline bool PartitionedHashTable::insert(
        PartitionedHashTableCtx* ht_ctx, Tuple* tuple, uint32_t hash) {
    DCHECK(_stores_tuples);
    HtData* htdata = insert_internal(ht_ctx, hash);
    // If successful insert, update the contents of the newly inserted entry with 'tuple'.
    if (LIKELY(htdata != NULL)) {
        htdata->tuple = tuple;
        return true;
    }
    return false;
}

inline PartitionedHashTable::Iterator PartitionedHashTable::find(
        PartitionedHashTableCtx* ht_ctx, uint32_t hash) {
    ++_num_probes;
    bool found = false;
    int64_t bucket_idx = probe(_buckets, _num_buckets, ht_ctx, hash, &found);
    if (found) {
        return Iterator(this, ht_ctx->row(), bucket_idx,
                _buckets[bucket_idx].bucketData.duplicates);
    }
    return End();
}

inline PartitionedHashTable::Iterator PartitionedHashTable::find_bucket(
        PartitionedHashTableCtx* ht_ctx, uint32_t hash,
        bool* found) {
    ++_num_probes;
    int64_t bucket_idx = probe(_buckets, _num_buckets, ht_ctx, hash, found);
    DuplicateNode* duplicates = LIKELY(bucket_idx != Iterator::BUCKET_NOT_FOUND) ?
        _buckets[bucket_idx].bucketData.duplicates : NULL;
    return Iterator(this, ht_ctx->row(), bucket_idx, duplicates);
}

inline PartitionedHashTable::Iterator PartitionedHashTable::begin(PartitionedHashTableCtx* ctx) {
    int64_t bucket_idx = Iterator::BUCKET_NOT_FOUND;
    DuplicateNode* node = NULL;
    next_filled_bucket(&bucket_idx, &node);
    return Iterator(this, ctx->row(), bucket_idx, node);
}

inline PartitionedHashTable::Iterator PartitionedHashTable::first_unmatched(
        PartitionedHashTableCtx* ctx) {
    int64_t bucket_idx = Iterator::BUCKET_NOT_FOUND;
    DuplicateNode* node = NULL;
    next_filled_bucket(&bucket_idx, &node);
    Iterator it(this, ctx->row(), bucket_idx, node);
    // Check whether the bucket, or its first duplicate node, is matched. If it is not
    // matched, then return. Otherwise, move to the first unmatched entry (node or bucket).
    Bucket* bucket = &_buckets[bucket_idx];
    if ((!bucket->hasDuplicates && bucket->matched) ||
            (bucket->hasDuplicates && node->matched)) {
        it.next_unmatched();
    }
    return it;
}

inline void PartitionedHashTable::next_filled_bucket(int64_t* bucket_idx, DuplicateNode** node) {
    ++*bucket_idx;
    for (; *bucket_idx < _num_buckets; ++*bucket_idx) {
        if (_buckets[*bucket_idx].filled) {
            *node = _buckets[*bucket_idx].bucketData.duplicates;
            return;
        }
    }
    // Reached the end of the hash table.
    *bucket_idx = Iterator::BUCKET_NOT_FOUND;
    *node = NULL;
}

inline void PartitionedHashTable::prepare_bucket_for_insert(int64_t bucket_idx, uint32_t hash) {
    DCHECK_GE(bucket_idx, 0);
    DCHECK_LT(bucket_idx, _num_buckets);
    Bucket* bucket = &_buckets[bucket_idx];
    DCHECK(!bucket->filled);
    ++_num_filled_buckets;
    bucket->filled = true;
    bucket->matched = false;
    bucket->hasDuplicates = false;
    bucket->hash = hash;
}

inline PartitionedHashTable::DuplicateNode* PartitionedHashTable::append_next_node(
        Bucket* bucket) {
    DCHECK_GT(_node_remaining_current_page, 0);
    bucket->bucketData.duplicates = _next_node;
    ++_num_duplicate_nodes;
    --_node_remaining_current_page;
    return _next_node++;
}

inline PartitionedHashTable::DuplicateNode* PartitionedHashTable::insert_duplicate_node(
        int64_t bucket_idx) {
    DCHECK_GE(bucket_idx, 0);
    DCHECK_LT(bucket_idx, _num_buckets);
    Bucket* bucket = &_buckets[bucket_idx];
    DCHECK(bucket->filled);
    // Allocate one duplicate node for the new data and one for the preexisting data,
    // if needed.
    while (_node_remaining_current_page < 1 + !bucket->hasDuplicates) {
        if (UNLIKELY(!grow_node_array())) {
            return NULL;
        }
    }
    if (!bucket->hasDuplicates) {
        // This is the first duplicate in this bucket. It means that we need to convert
        // the current entry in the bucket to a node and link it from the bucket.
        _next_node->htdata.idx = bucket->bucketData.htdata.idx;
        DCHECK(!bucket->matched);
        _next_node->matched = false;
        _next_node->next = NULL;
        append_next_node(bucket);
        bucket->hasDuplicates = true;
        ++_num_buckets_with_duplicates;
    }
    // Link a new node.
    _next_node->next = bucket->bucketData.duplicates;
    _next_node->matched = false;
    return append_next_node(bucket);
}

inline TupleRow* PartitionedHashTable::get_row(HtData& htdata, TupleRow* row) const {
    if (_stores_tuples) {
        return reinterpret_cast<TupleRow*>(&htdata.tuple);
    } else {
        _tuple_stream->get_tuple_row(htdata.idx, row);
        return row;
    }
}

inline TupleRow* PartitionedHashTable::get_row(Bucket* bucket, TupleRow* row) const {
    DCHECK(bucket != NULL);
    if (UNLIKELY(bucket->hasDuplicates)) {
        DuplicateNode* duplicate = bucket->bucketData.duplicates;
        DCHECK(duplicate != NULL);
        return get_row(duplicate->htdata, row);
    } else {
        return get_row(bucket->bucketData.htdata, row);
    }
}

inline TupleRow* PartitionedHashTable::Iterator::get_row() const {
    DCHECK(!at_end());
    DCHECK(_table != NULL);
    DCHECK(_row != NULL);
    Bucket* bucket = &_table->_buckets[_bucket_idx];
    if (UNLIKELY(bucket->hasDuplicates)) {
        DCHECK(_node != NULL);
        return _table->get_row(_node->htdata, _row);
    } else {
        return _table->get_row(bucket->bucketData.htdata, _row);
    }
}

inline Tuple* PartitionedHashTable::Iterator::get_tuple() const {
    DCHECK(!at_end());
    DCHECK(_table->_stores_tuples);
    Bucket* bucket = &_table->_buckets[_bucket_idx];
    // TODO: To avoid the hasDuplicates check, store the HtData* in the Iterator.
    if (UNLIKELY(bucket->hasDuplicates)) {
        DCHECK(_node != NULL);
        return _node->htdata.tuple;
    } else {
        return bucket->bucketData.htdata.tuple;
    }
}

inline void PartitionedHashTable::Iterator::set_tuple(Tuple* tuple, uint32_t hash) {
    DCHECK(!at_end());
    DCHECK(_table->_stores_tuples);
    _table->prepare_bucket_for_insert(_bucket_idx, hash);
    _table->_buckets[_bucket_idx].bucketData.htdata.tuple = tuple;
}

inline void PartitionedHashTable::Iterator::set_matched() {
    DCHECK(!at_end());
    Bucket* bucket = &_table->_buckets[_bucket_idx];
    if (bucket->hasDuplicates) {
        _node->matched = true;
    } else {
        bucket->matched = true;
    }
    // Used for disabling spilling of hash tables in right and full-outer joins with
    // matches. See IMPALA-1488.
    _table->_has_matches = true;
}

inline bool PartitionedHashTable::Iterator::is_matched() const {
    DCHECK(!at_end());
    Bucket* bucket = &_table->_buckets[_bucket_idx];
    if (bucket->hasDuplicates) {
        return _node->matched;
    }
    return bucket->matched;
}

inline void PartitionedHashTable::Iterator::set_at_end() {
    _bucket_idx = BUCKET_NOT_FOUND;
    _node = NULL;
}

inline void PartitionedHashTable::Iterator::next() {
    DCHECK(!at_end());
    if (_table->_buckets[_bucket_idx].hasDuplicates && _node->next != NULL) {
        _node = _node->next;
    } else {
        _table->next_filled_bucket(&_bucket_idx, &_node);
    }
}

inline void PartitionedHashTable::Iterator::next_duplicate() {
    DCHECK(!at_end());
    if (_table->_buckets[_bucket_idx].hasDuplicates && _node->next != NULL) {
        _node = _node->next;
    } else {
        _bucket_idx = BUCKET_NOT_FOUND;
        _node = NULL;
    }
}

inline void PartitionedHashTable::Iterator::next_unmatched() {
    DCHECK(!at_end());
    Bucket* bucket = &_table->_buckets[_bucket_idx];
    // Check if there is any remaining unmatched duplicate node in the current bucket.
    if (bucket->hasDuplicates) {
        while (_node->next != NULL) {
            _node = _node->next;
            if (!_node->matched) {
                return;
            }
        }
    }
    // Move to the next filled bucket and return if this bucket is not matched or
    // iterate to the first not matched duplicate node.
    _table->next_filled_bucket(&_bucket_idx, &_node);
    while (_bucket_idx != Iterator::BUCKET_NOT_FOUND) {
        bucket = &_table->_buckets[_bucket_idx];
        if (!bucket->hasDuplicates) {
            if (!bucket->matched) {
                return;
            }
        } else {
            while (_node->matched && _node->next != NULL) {
                _node = _node->next;
            }
            if (!_node->matched) {
                return;
            }
        }
        _table->next_filled_bucket(&_bucket_idx, &_node);
    }
}

inline void PartitionedHashTableCtx::set_level(int level) {
    DCHECK_GE(level, 0);
    DCHECK_LT(level, _seeds.size());
    _level = level;
}

} // end namespace doris

#endif // DORIS_BE_SRC_EXEC_PARTITIONED_HASH_TABLE_INLINE_H
