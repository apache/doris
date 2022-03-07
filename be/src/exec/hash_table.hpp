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

inline bool HashTable::emplace_key(TupleRow* row, TupleRow** dest_addr) {
    if (_num_filled_buckets > _num_buckets_till_resize) {
        if (!resize_buckets(_num_buckets * 2).ok()) {
            return false;
        }
    }
    if (_current_used == _current_capacity) {
        grow_node_array();
    }

    bool has_nulls = eval_build_row(row);

    if (!_stores_nulls && has_nulls) {
        return false;
    }

    uint32_t hash = hash_current_row();
    int64_t bucket_idx = hash & (_num_buckets - 1);

    Bucket* bucket = &_buckets[bucket_idx];
    Node* node = bucket->_node;

    bool will_insert = true;

    if (node == nullptr) {
        will_insert = true;
    } else {
        Node* last_node = node;
        while (node != nullptr) {
            if (node->_hash == hash && equals(node->data())) {
                will_insert = false;
                break;
            }
            last_node = node;
            node = node->_next;
        }
        node = last_node;
    }
    if (will_insert) {
        Node* alloc_node =
                reinterpret_cast<Node*>(_current_nodes + _node_byte_size * _current_used++);
        ++_num_nodes;
        TupleRow* data = alloc_node->data();
        *dest_addr = data;
        alloc_node->_hash = hash;
        if (node == nullptr) {
            add_to_bucket(&_buckets[bucket_idx], alloc_node);
        } else {
            node->_next = alloc_node;
        }
    }
    return will_insert;
}

inline HashTable::Iterator HashTable::find(TupleRow* probe_row, bool probe) {
    bool has_nulls = probe ? eval_probe_row(probe_row) : eval_build_row(probe_row);

    if (!_stores_nulls && has_nulls) {
        return end();
    }

    uint32_t hash = hash_current_row();
    int64_t bucket_idx = hash & (_num_buckets - 1);

    Bucket* bucket = &_buckets[bucket_idx];
    Node* node = bucket->_node;

    while (node != nullptr) {
        if (node->_hash == hash && equals(node->data())) {
            return Iterator(this, bucket_idx, node, hash);
        }

        node = node->_next;
    }

    return end();
}

inline HashTable::Iterator HashTable::begin() {
    int64_t bucket_idx = -1;
    Bucket* bucket = next_bucket(&bucket_idx);

    if (bucket != NULL) {
        return Iterator(this, bucket_idx, bucket->_node, 0);
    }

    return end();
}

inline HashTable::Bucket* HashTable::next_bucket(int64_t* bucket_idx) {
    ++*bucket_idx;

    for (; *bucket_idx < _num_buckets; ++*bucket_idx) {
        if (_buckets[*bucket_idx]._node != nullptr) {
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

    if (_current_used == _current_capacity) {
        grow_node_array();
    }
    // get a node from memory pool
    Node* node = reinterpret_cast<Node*>(_current_nodes + _node_byte_size * _current_used++);

    TupleRow* data = node->data();
    node->_hash = hash;
    memcpy(data, row, sizeof(Tuple*) * _num_build_tuples);
    add_to_bucket(&_buckets[bucket_idx], node);
    ++_num_nodes;
}

inline void HashTable::add_to_bucket(Bucket* bucket, Node* node) {
    if (bucket->_node == nullptr) {
        ++_num_filled_buckets;
    }

    node->_next = bucket->_node;
    bucket->_node = node;
    bucket->_size++;
}

inline void HashTable::move_node(Bucket* from_bucket, Bucket* to_bucket, Node* node,
                                 Node* previous_node) {
    Node* next_node = node->_next;
    from_bucket->_size--;

    if (previous_node != NULL) {
        previous_node->_next = next_node;
    } else {
        // Update bucket directly
        from_bucket->_node = next_node;

        if (next_node == nullptr) {
            --_num_filled_buckets;
        }
    }

    add_to_bucket(to_bucket, node);
}

inline std::pair<int64_t, int64_t> HashTable::minmax_node() {
    bool has_value = false;
    int64_t min_size = std::numeric_limits<int64_t>::max();
    int64_t max_size = std::numeric_limits<int64_t>::min();
    for (const auto bucket : _buckets) {
        int64_t counter = bucket._size;
        if (counter > 0) {
            has_value = true;
            min_size = std::min(counter, min_size);
            max_size = std::max(counter, max_size);
        }
    }
    if (!has_value) {
        return std::make_pair(0, 0);
    }
    return std::make_pair(min_size, max_size);
}

template <bool check_match>
inline void HashTable::Iterator::next() {
    if (_bucket_idx == -1) {
        return;
    }

    // TODO: this should prefetch the next tuplerow
    Node* node = _node;

    // Iterator is not from a full table scan, evaluate equality now.  Only the current
    // bucket needs to be scanned. '_expr_values_buffer' contains the results
    // for the current probe row.
    if (check_match) {
        // TODO: this should prefetch the next node
        Node* next_node = node->_next;

        while (next_node != nullptr) {
            node = next_node;

            if (node->_hash == _scan_hash && _table->equals(node->data())) {
                _node = next_node;
                return;
            }

            next_node = node->_next;
        }

        *this = _table->end();
    } else {
        // Move onto the next chained node
        if (node->_next != nullptr) {
            _node = node->_next;
            return;
        }

        // Move onto the next bucket
        Bucket* bucket = _table->next_bucket(&_bucket_idx);

        if (bucket == NULL) {
            _bucket_idx = -1;
            _node = nullptr;
        } else {
            _node = bucket->_node;
        }
    }
}

} // namespace doris

#endif
