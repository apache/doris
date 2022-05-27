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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/hash-table.cc
// and modified by Doris

#include "exec/hash_table.hpp"

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.hpp"
#include "util/doris_metrics.h"

namespace doris {

HashTable::HashTable(const std::vector<ExprContext*>& build_expr_ctxs,
                     const std::vector<ExprContext*>& probe_expr_ctxs, int num_build_tuples,
                     bool stores_nulls, const std::vector<bool>& finds_nulls, int32_t initial_seed,
                     const std::shared_ptr<MemTracker>& mem_tracker, int64_t num_buckets)
        : _build_expr_ctxs(build_expr_ctxs),
          _probe_expr_ctxs(probe_expr_ctxs),
          _num_build_tuples(num_build_tuples),
          _stores_nulls(stores_nulls),
          _finds_nulls(finds_nulls),
          _initial_seed(initial_seed),
          _node_byte_size(sizeof(Node) + sizeof(Tuple*) * _num_build_tuples),
          _num_filled_buckets(0),
          _current_nodes(nullptr),
          _num_nodes(0),
          _current_capacity(num_buckets),
          _current_used(0),
          _total_capacity(num_buckets) {
    DCHECK_EQ(_build_expr_ctxs.size(), _probe_expr_ctxs.size());

    DCHECK_EQ((num_buckets & (num_buckets - 1)), 0) << "num_buckets must be a power of 2";
    _mem_tracker =
            MemTracker::create_virtual_tracker(-1, mem_tracker->label() + "HashTable", mem_tracker);
    _buckets.resize(num_buckets);
    _num_buckets = num_buckets;
    _num_buckets_till_resize = MAX_BUCKET_OCCUPANCY_FRACTION * _num_buckets;
    _mem_tracker->consume(_buckets.capacity() * sizeof(Bucket));

    // Compute the layout and buffer size to store the evaluated expr results
    _results_buffer_size = Expr::compute_results_layout(
            _build_expr_ctxs, &_expr_values_buffer_offsets, &_var_result_begin);
    _expr_values_buffer = new uint8_t[_results_buffer_size];
    memset(_expr_values_buffer, 0, sizeof(uint8_t) * _results_buffer_size);
    _expr_value_null_bits = new uint8_t[_build_expr_ctxs.size()];

    _alloc_list.reserve(10);
    _end_list.reserve(10);
    _current_nodes = reinterpret_cast<uint8_t*>(malloc(_current_capacity * _node_byte_size));
    // TODO: remove memset later
    memset(_current_nodes, 0, _current_capacity * _node_byte_size);
    _alloc_list.push_back(_current_nodes);
    _end_list.push_back(_current_nodes + _current_capacity * _node_byte_size);

    _mem_tracker->consume(_current_capacity * _node_byte_size);
}

HashTable::~HashTable() {}

void HashTable::close() {
    // TODO: use tr1::array?
    delete[] _expr_values_buffer;
    delete[] _expr_value_null_bits;
    for (auto ptr : _alloc_list) {
        free(ptr);
    }
    _mem_tracker->release(_total_capacity * _node_byte_size);
    _mem_tracker->release(_buckets.size() * sizeof(Bucket));
}

bool HashTable::eval_row(TupleRow* row, const std::vector<ExprContext*>& ctxs) {
    // Put a non-zero constant in the result location for nullptr.
    // We don't want(nullptr, 1) to hash to the same as (0, 1).
    // This needs to be as big as the biggest primitive type since the bytes
    // get copied directly.

    // the 10 is experience value which need bigger than sizeof(Decimal)/sizeof(int64).
    // for if slot is null, we need copy the null value to all type.
    static int64_t null_value[10] = {HashUtil::FNV_SEED, HashUtil::FNV_SEED, 0};
    bool has_null = false;

    for (int i = 0; i < ctxs.size(); ++i) {
        void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];
        void* val = ctxs[i]->get_value(row);

        if (val == nullptr) {
            // If the table doesn't store nulls, no reason to keep evaluating
            if (!_stores_nulls) {
                return true;
            }

            _expr_value_null_bits[i] = true;
            val = &null_value;
            has_null = true;
        } else {
            _expr_value_null_bits[i] = false;
        }

        RawValue::write(val, loc, _build_expr_ctxs[i]->root()->type(), nullptr);
    }

    return has_null;
}

uint32_t HashTable::hash_variable_len_row() {
    uint32_t hash = _initial_seed;
    // Hash the non-var length portions (if there are any)
    if (_var_result_begin != 0) {
        hash = HashUtil::hash(_expr_values_buffer, _var_result_begin, hash);
    }

    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        // non-string and null slots are already part of expr_values_buffer
        if (_build_expr_ctxs[i]->root()->type().is_string_type()) {
            void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];

            if (_expr_value_null_bits[i]) {
                // Hash the null random seed values at 'loc'
                hash = HashUtil::hash(loc, sizeof(StringValue), hash);
            } else {
                // Hash the string
                StringValue* str = reinterpret_cast<StringValue*>(loc);
                hash = HashUtil::hash(str->ptr, str->len, hash);
            }
        }
    }

    return hash;
}

bool HashTable::equals(TupleRow* build_row) {
    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        void* val = _build_expr_ctxs[i]->get_value(build_row);

        if (val == nullptr) {
            if (!(_stores_nulls && _finds_nulls[i])) {
                return false;
            }

            if (!_expr_value_null_bits[i]) {
                return false;
            }

            continue;
        }

        void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];

        if (!RawValue::eq(loc, val, _build_expr_ctxs[i]->root()->type())) {
            return false;
        }
    }

    return true;
}

Status HashTable::resize_buckets(int64_t num_buckets) {
    DCHECK_EQ((num_buckets & (num_buckets - 1)), 0) << "num_buckets must be a power of 2";

    int64_t old_num_buckets = _num_buckets;
    int64_t delta_bytes = (num_buckets - old_num_buckets) * sizeof(Bucket);
    Status st = _mem_tracker->try_consume(delta_bytes);
    if (!st) {
        LOG_EVERY_N(WARNING, 100) << "resize bucket failed: " << st.to_string();
        return st;
    }

    _buckets.resize(num_buckets);

    // If we're doubling the number of buckets, all nodes in a particular bucket
    // either remain there, or move down to an analogous bucket in the other half.
    // In order to efficiently check which of the two buckets a node belongs in, the number
    // of buckets must be a power of 2.
    bool doubled_buckets = (num_buckets == old_num_buckets * 2);

    for (int i = 0; i < _num_buckets; ++i) {
        Bucket* bucket = &_buckets[i];
        Bucket* sister_bucket = &_buckets[i + old_num_buckets];
        Node* last_node = nullptr;
        Node* node = bucket->_node;

        while (node != nullptr) {
            Node* next_node = node->_next;
            uint32_t hash = node->_hash;

            bool node_must_move = true;
            Bucket* move_to = nullptr;

            if (doubled_buckets) {
                node_must_move = ((hash & old_num_buckets) != 0);
                move_to = sister_bucket;
            } else {
                int64_t bucket_idx = hash & (num_buckets - 1);
                node_must_move = (bucket_idx != i);
                move_to = &_buckets[bucket_idx];
            }

            if (node_must_move) {
                move_node(bucket, move_to, node, last_node);
            } else {
                last_node = node;
            }

            node = next_node;
        }
    }

    _num_buckets = num_buckets;
    _num_buckets_till_resize = MAX_BUCKET_OCCUPANCY_FRACTION * _num_buckets;
    return Status::OK();
}

void HashTable::grow_node_array() {
    _current_capacity = _total_capacity / 2;
    _total_capacity += _current_capacity;
    int64_t alloc_size = _current_capacity * _node_byte_size;
    _current_nodes = reinterpret_cast<uint8_t*>(malloc(alloc_size));
    _current_used = 0;
    // TODO: remove memset later
    memset(_current_nodes, 0, alloc_size);
    // add _current_nodes to alloc pool
    _alloc_list.push_back(_current_nodes);
    _end_list.push_back(_current_nodes + alloc_size);

    _mem_tracker->consume(alloc_size);
}

std::string HashTable::debug_string(bool skip_empty, const RowDescriptor* desc) {
    std::stringstream ss;
    ss << std::endl;

    for (int i = 0; i < _buckets.size(); ++i) {
        Node* node = _buckets[i]._node;
        bool first = true;

        if (skip_empty && node == nullptr) {
            continue;
        }

        ss << i << ": ";

        while (node != nullptr) {
            if (!first) {
                ss << ",";
            }

            if (desc == nullptr) {
                ss << node->_hash << "(" << (void*)node->data() << ")";
            } else {
                ss << (void*)node->data() << " " << node->data()->to_string(*desc);
            }

            node = node->_next;
            first = false;
        }

        ss << std::endl;
    }

    return ss.str();
}

} // namespace doris
