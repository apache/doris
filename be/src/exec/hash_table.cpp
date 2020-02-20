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

#include "exec/hash_table.hpp"

#include "codegen/codegen_anyval.h"

#include "exprs/expr.h"
#include "runtime/raw_value.h"
#include "runtime/string_value.hpp"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/doris_metrics.h"

namespace doris {

const float HashTable::MAX_BUCKET_OCCUPANCY_FRACTION = 0.75f;

HashTable::HashTable(const vector<ExprContext*>& build_expr_ctxs,
                     const vector<ExprContext*>& probe_expr_ctxs,
                     int num_build_tuples, bool stores_nulls, 
                     const std::vector<bool>& finds_nulls,
                     int32_t initial_seed,
                     MemTracker* mem_tracker, int64_t num_buckets) :
        _build_expr_ctxs(build_expr_ctxs),
        _probe_expr_ctxs(probe_expr_ctxs),
        _num_build_tuples(num_build_tuples),
        _stores_nulls(stores_nulls),
        _finds_nulls(finds_nulls),
        _initial_seed(initial_seed),
        _node_byte_size(sizeof(Node) + sizeof(Tuple*) * _num_build_tuples),
        _num_filled_buckets(0),
        _nodes(NULL),
        _num_nodes(0),
        _exceeded_limit(false),
        _mem_tracker(mem_tracker),
        _mem_limit_exceeded(false) {
    DCHECK(mem_tracker != NULL);
    DCHECK_EQ(_build_expr_ctxs.size(), _probe_expr_ctxs.size());

    DCHECK_EQ((num_buckets & (num_buckets - 1)), 0) << "num_buckets must be a power of 2";
    _buckets.resize(num_buckets);
    _num_buckets = num_buckets;
    _num_buckets_till_resize = MAX_BUCKET_OCCUPANCY_FRACTION * _num_buckets;
    _mem_tracker->consume(_buckets.capacity() * sizeof(Bucket));

    // Compute the layout and buffer size to store the evaluated expr results
    _results_buffer_size = Expr::compute_results_layout(_build_expr_ctxs,
                           &_expr_values_buffer_offsets, &_var_result_begin);
    _expr_values_buffer = new uint8_t[_results_buffer_size];
    memset(_expr_values_buffer, 0, sizeof(uint8_t) * _results_buffer_size);
    _expr_value_null_bits = new uint8_t[_build_expr_ctxs.size()];

    _nodes_capacity = 1024;
    _nodes = reinterpret_cast<uint8_t*>(malloc(_nodes_capacity * _node_byte_size));
    memset(_nodes, 0, _nodes_capacity * _node_byte_size);

#if 0
    if (DorisMetrics::hash_table_total_bytes() != NULL) {
        DorisMetrics::hash_table_total_bytes()->increment(_nodes_capacity * _node_byte_size);
    }
#endif

    _mem_tracker->consume(_nodes_capacity * _node_byte_size);
    if (_mem_tracker->limit_exceeded()) {
        mem_limit_exceeded(_nodes_capacity * _node_byte_size);
    }
}

HashTable::~HashTable() {
}

void HashTable::close() {
    // TODO: use tr1::array?
    delete[] _expr_values_buffer;
    delete[] _expr_value_null_bits;
    free(_nodes);
#if 0
    if (DorisMetrics::hash_table_total_bytes() != NULL) {
        DorisMetrics::hash_table_total_bytes()->increment(-_nodes_capacity * _node_byte_size);
    }
#endif
    _mem_tracker->release(_nodes_capacity * _node_byte_size);
    _mem_tracker->release(_buckets.size() * sizeof(Bucket));
}

bool HashTable::eval_row(TupleRow* row, const vector<ExprContext*>& ctxs) {
    // Put a non-zero constant in the result location for NULL.
    // We don't want(NULL, 1) to hash to the same as (0, 1).
    // This needs to be as big as the biggest primitive type since the bytes
    // get copied directly.

    // the 10 is experience value which need bigger than sizeof(Decimal)/sizeof(int64).
    // for if slot is null, we need copy the null value to all type.
    static int64_t null_value[10] = {HashUtil::FNV_SEED, HashUtil::FNV_SEED, 0};
    bool has_null = false;

    for (int i = 0; i < ctxs.size(); ++i) {
        void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];
        void* val = ctxs[i]->get_value(row);

        if (val == NULL) {
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

        RawValue::write(val, loc, _build_expr_ctxs[i]->root()->type(), NULL);
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
        } else if (_build_expr_ctxs[i]->root()->type().type == TYPE_DECIMAL) {
            void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];
            if (_expr_value_null_bits[i]) {
                // Hash the null random seed values at 'loc'
                hash = HashUtil::hash(loc, sizeof(StringValue), hash);
            } else {
                DecimalValue* decimal = reinterpret_cast<DecimalValue*>(loc);
                hash = decimal->hash(hash);
            }
        }
        
    }

    return hash;
}

bool HashTable::equals(TupleRow* build_row) {
    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        void* val = _build_expr_ctxs[i]->get_value(build_row);

        if (val == NULL) {
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

void HashTable::resize_buckets(int64_t num_buckets) {
    DCHECK_EQ((num_buckets & (num_buckets - 1)), 0) << "num_buckets must be a power of 2";

    int64_t old_num_buckets = _num_buckets;
    int64_t delta_bytes = (num_buckets - old_num_buckets) * sizeof(Bucket);
    if (!_mem_tracker->try_consume(delta_bytes)) {
        mem_limit_exceeded(delta_bytes);
        return;
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
        Node* last_node = NULL;
        int node_idx = bucket->_node_idx;

        while (node_idx != -1) {
            Node* node = get_node(node_idx);
            int64_t next_idx = node->_next_idx;
            uint32_t hash = node->_hash;

            bool node_must_move = true;
            Bucket* move_to = NULL;

            if (doubled_buckets) {
                node_must_move = ((hash & old_num_buckets) != 0);
                move_to = sister_bucket;
            } else {
                int64_t bucket_idx = hash & (num_buckets - 1);
                node_must_move = (bucket_idx != i);
                move_to = &_buckets[bucket_idx];
            }

            if (node_must_move) {
                move_node(bucket, move_to, node_idx, node, last_node);
            } else {
                last_node = node;
            }

            node_idx = next_idx;
        }
    }

    _num_buckets = num_buckets;
    _num_buckets_till_resize = MAX_BUCKET_OCCUPANCY_FRACTION * _num_buckets;
}

void HashTable::grow_node_array() {
    int64_t old_size = _nodes_capacity * _node_byte_size;
    _nodes_capacity = _nodes_capacity + _nodes_capacity / 2;
    int64_t new_size = _nodes_capacity * _node_byte_size;

    uint8_t* new_nodes = reinterpret_cast<uint8_t*>(malloc(new_size));
    memset(new_nodes, 0, new_size);
    memcpy(new_nodes, _nodes, old_size);
    free(_nodes);
    _nodes = new_nodes; 

#if 0
    if (DorisMetrics::hash_table_total_bytes() != NULL) {
        DorisMetrics::hash_table_total_bytes()->increment(new_size - old_size);
    }
#endif

    _mem_tracker->consume(new_size - old_size);
    if (_mem_tracker->limit_exceeded()) {
        mem_limit_exceeded(new_size - old_size);
    }
}

void HashTable::mem_limit_exceeded(int64_t allocation_size) {
    _mem_limit_exceeded = true;
    _exceeded_limit = true;
    // if (_state != NULL) {
    //     _state->set_mem_limit_exceeded(_mem_tracker, allocation_size);
    // }
}

std::string HashTable::debug_string(bool skip_empty, const RowDescriptor* desc) {
    std::stringstream ss;
    ss << std::endl;

    for (int i = 0; i < _buckets.size(); ++i) {
        int64_t node_idx = _buckets[i]._node_idx;
        bool first = true;

        if (skip_empty && node_idx == -1) {
            continue;
        }

        ss << i << ": ";

        while (node_idx != -1) {
            Node* node = get_node(node_idx);

            if (!first) {
                ss << ",";
            }

            if (desc == NULL) {
                ss << node_idx << "(" << (void*)node->data() << ")";
            } else {
                ss << (void*)node->data() << " " << node->data()->to_string(*desc);
            }

            node_idx = node->_next_idx;
            first = false;
        }

        ss << std::endl;
    }

    return ss.str();
}

}
