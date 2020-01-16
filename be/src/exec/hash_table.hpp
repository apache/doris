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

#ifndef DORIS_BE_SRC_QUERY_EXEC_HASH_TABLE_HPP
#define DORIS_BE_SRC_QUERY_EXEC_HASH_TABLE_HPP

#include "exec/hash_table.h"

namespace doris {

inline HashTable::Iterator HashTable::find(TupleRow* probe_row) {
    bool has_nulls = eval_probe_row(probe_row);

    if (!_stores_nulls && has_nulls) {
        return end();
    }

    uint32_t hash = hash_current_row();
    int64_t bucket_idx = hash & (_num_buckets - 1);

    Bucket* bucket = &_buckets[bucket_idx];
    int64_t node_idx = bucket->_node_idx;

    while (node_idx != -1) {
        Node* node = get_node(node_idx);

        if (node->_hash == hash && equals(node->data())) {
            return Iterator(this, bucket_idx, node_idx, hash);
        }

        node_idx = node->_next_idx;
    }

    return end();
}

inline HashTable::Iterator HashTable::begin() {
    int64_t bucket_idx = -1;
    Bucket* bucket = next_bucket(&bucket_idx);

    if (bucket != NULL) {
        return Iterator(this, bucket_idx, bucket->_node_idx, 0);
    }

    return end();
}

inline HashTable::Bucket* HashTable::next_bucket(int64_t* bucket_idx) {
    ++*bucket_idx;

    for (; *bucket_idx < _num_buckets; ++*bucket_idx) {
        if (_buckets[*bucket_idx]._node_idx != -1) {
            return &_buckets[*bucket_idx];
        }
    }

    *bucket_idx = -1;
    return NULL;
}

inline void HashTable::insert_impl(TupleRow* row) {
    bool has_null = eval_build_row(row);

    if (!_stores_nulls && has_null) {
        return;
    }

    uint32_t hash = hash_current_row();
    int64_t bucket_idx = hash & (_num_buckets - 1);

    if (_num_nodes == _nodes_capacity) {
        grow_node_array();
    }

    Node* node = get_node(_num_nodes);
    TupleRow* data = node->data();
    node->_hash = hash;
    memcpy(data, row, sizeof(Tuple*) * _num_build_tuples);
    add_to_bucket(&_buckets[bucket_idx], _num_nodes, node);
    ++_num_nodes;
}

inline void HashTable::add_to_bucket(Bucket* bucket, int64_t node_idx, Node* node) {
    if (bucket->_node_idx == -1) {
        ++_num_filled_buckets;
    }

    node->_next_idx = bucket->_node_idx;
    bucket->_node_idx = node_idx;
}

inline void HashTable::move_node(Bucket* from_bucket, Bucket* to_bucket,
                                int64_t node_idx, Node* node, Node* previous_node) {
    int64_t next_idx = node->_next_idx;

    if (previous_node != NULL) {
        previous_node->_next_idx = next_idx;
    } else {
        // Update bucket directly
        from_bucket->_node_idx = next_idx;

        if (next_idx == -1) {
            --_num_filled_buckets;
        }
    }

    add_to_bucket(to_bucket, node_idx, node);
}

template<bool check_match>
inline void HashTable::Iterator::next() {
    if (_bucket_idx == -1) {
        return;
    }

    // TODO: this should prefetch the next tuplerow
    Node* node = _table->get_node(_node_idx);

    // Iterator is not from a full table scan, evaluate equality now.  Only the current
    // bucket needs to be scanned. '_expr_values_buffer' contains the results
    // for the current probe row.
    if (check_match) {
        // TODO: this should prefetch the next node
        int64_t next_idx = node->_next_idx;

        while (next_idx != -1) {
            node = _table->get_node(next_idx);

            if (node->_hash == _scan_hash && _table->equals(node->data())) {
                _node_idx = next_idx;
                return;
            }

            next_idx = node->_next_idx;
        }

        *this = _table->end();
    } else {
        // Move onto the next chained node
        if (node->_next_idx != -1) {
            _node_idx = node->_next_idx;
            return;
        }

        // Move onto the next bucket
        Bucket* bucket = _table->next_bucket(&_bucket_idx);

        if (bucket == NULL) {
            _bucket_idx = -1;
            _node_idx = -1;
        } else {
            _node_idx = bucket->_node_idx;
        }
    }
}

}

#endif
