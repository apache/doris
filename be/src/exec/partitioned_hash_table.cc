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

#include "exec/partitioned_hash_table.inline.h"

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/slot_ref.h"
#include "runtime/buffered_block_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.hpp"
#include "util/doris_metrics.h"

// DEFINE_bool(enable_quadratic_probing, true, "Enable quadratic probing hash table");

using std::string;
using std::stringstream;
using std::vector;
using std::endl;

namespace doris {

// Random primes to multiply the seed with.
static uint32_t SEED_PRIMES[] = {
    1, // First seed must be 1, level 0 is used by other operators in the fragment.
    1431655781,
    1183186591,
    622729787,
    472882027,
    338294347,
    275604541,
    41161739,
    29999999,
    27475109,
    611603,
    16313357,
    11380003,
    21261403,
    33393119,
    101,
    71043403
};

// Put a non-zero constant in the result location for NULL.
// We don't want(NULL, 1) to hash to the same as (0, 1).
// This needs to be as big as the biggest primitive type since the bytes
// get copied directly.
// TODO find a better approach, since primitives like CHAR(N) can be up to 128 bytes
static int64_t NULL_VALUE[] = { HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED,
                                HashUtil::FNV_SEED, HashUtil::FNV_SEED };

// The first NUM_SMALL_BLOCKS of _nodes are made of blocks less than the IO size (of 8MB)
// to reduce the memory footprint of small queries. In particular, we always first use a
// 64KB and a 512KB block before starting using IO-sized blocks.
static const int64_t INITIAL_DATA_PAGE_SIZES[] = { 64 * 1024, 512 * 1024 };
static const int NUM_SMALL_DATA_PAGES = sizeof(INITIAL_DATA_PAGE_SIZES) / sizeof(int64_t);

PartitionedHashTableCtx::PartitionedHashTableCtx(
        const vector<ExprContext*>& build_expr_ctxs, const vector<ExprContext*>& probe_expr_ctxs,
        bool stores_nulls, bool finds_nulls,
        int32_t initial_seed, int max_levels, int num_build_tuples) :
            _build_expr_ctxs(build_expr_ctxs),
            _probe_expr_ctxs(probe_expr_ctxs),
            _stores_nulls(stores_nulls),
            _finds_nulls(finds_nulls),
            _level(0),
            _row(reinterpret_cast<TupleRow*>(malloc(sizeof(Tuple*) * num_build_tuples))) {
    // Compute the layout and buffer size to store the evaluated expr results
    DCHECK_EQ(_build_expr_ctxs.size(), _probe_expr_ctxs.size());
    DCHECK(!_build_expr_ctxs.empty());
    _results_buffer_size = Expr::compute_results_layout(_build_expr_ctxs,
            &_expr_values_buffer_offsets, &_var_result_begin);
    _expr_values_buffer = new uint8_t[_results_buffer_size];
    memset(_expr_values_buffer, 0, sizeof(uint8_t) * _results_buffer_size);
    _expr_value_null_bits = new uint8_t[build_expr_ctxs.size()];

    // Populate the seeds to use for all the levels. TODO: revisit how we generate these.
    DCHECK_GE(max_levels, 0);
    DCHECK_LT(max_levels, sizeof(SEED_PRIMES) / sizeof(SEED_PRIMES[0]));
    DCHECK_NE(initial_seed, 0);
    _seeds.resize(max_levels + 1);
    _seeds[0] = initial_seed;
    for (int i = 1; i <= max_levels; ++i) {
        _seeds[i] = _seeds[i - 1] * SEED_PRIMES[i];
    }
}

void PartitionedHashTableCtx::close() {
    // TODO: use tr1::array?
    DCHECK(_expr_values_buffer != NULL);
    delete[] _expr_values_buffer;
    _expr_values_buffer = NULL;
    DCHECK(_expr_value_null_bits != NULL);
    delete[] _expr_value_null_bits;
    _expr_value_null_bits = NULL;
    free(_row);
    _row = NULL;
}

bool PartitionedHashTableCtx::eval_row(TupleRow* row, const vector<ExprContext*>& ctxs) {
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
            val = reinterpret_cast<void*>(&NULL_VALUE);
            has_null = true;
        } else {
            _expr_value_null_bits[i] = false;
        }
        DCHECK_LE(_build_expr_ctxs[i]->root()->type().get_slot_size(),
                sizeof(NULL_VALUE));
        RawValue::write(val, loc, _build_expr_ctxs[i]->root()->type(), NULL);
    }
    return has_null;
}

uint32_t PartitionedHashTableCtx::hash_variable_len_row() {
    uint32_t hash_val = _seeds[_level];
    // Hash the non-var length portions (if there are any)
    if (_var_result_begin != 0) {
        hash_val = hash_help(_expr_values_buffer, _var_result_begin, hash_val);
    }

    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        // non-string and null slots are already part of expr_values_buffer
        // if (_build_expr_ctxs[i]->root()->type().type != TYPE_STRING &&
        if (_build_expr_ctxs[i]->root()->type().type != TYPE_VARCHAR) {
            continue;
        }

        void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];
        if (_expr_value_null_bits[i]) {
            // Hash the null random seed values at 'loc'
            hash_val = hash_help(loc, sizeof(StringValue), hash_val);
        } else {
            // Hash the string
            // TODO: when using CRC hash on empty string, this only swaps bytes.
            StringValue* str = reinterpret_cast<StringValue*>(loc);
            hash_val = hash_help(str->ptr, str->len, hash_val);
        }
    }
    return hash_val;
}

bool PartitionedHashTableCtx::equals(TupleRow* build_row) {
    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        void* val = _build_expr_ctxs[i]->get_value(build_row);
        if (val == NULL) {
            if (!_stores_nulls) {
                return false;
            }
            if (!_expr_value_null_bits[i]) {
                return false;
            }
            continue;
        } else {
            if (_expr_value_null_bits[i]) {
                return false;
            }
        }

        void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];
        if (!RawValue::eq(loc, val, _build_expr_ctxs[i]->root()->type())) {
            return false;
        }
    }
    return true;
}

const double PartitionedHashTable::MAX_FILL_FACTOR = 0.75f;

PartitionedHashTable* PartitionedHashTable::create(RuntimeState* state,
        BufferedBlockMgr2::Client* client, int num_build_tuples,
        BufferedTupleStream2* tuple_stream, int64_t max_num_buckets,
        int64_t initial_num_buckets) {
    // return new PartitionedHashTable(FLAGS_enable_quadratic_probing, state, client,
    //         num_build_tuples, tuple_stream, max_num_buckets, initial_num_buckets);
    return new PartitionedHashTable(config::enable_quadratic_probing, state, client,
            num_build_tuples, tuple_stream, max_num_buckets, initial_num_buckets);
}

PartitionedHashTable::PartitionedHashTable(bool quadratic_probing, RuntimeState* state,
        BufferedBlockMgr2::Client* client, int num_build_tuples, BufferedTupleStream2* stream,
        int64_t max_num_buckets, int64_t num_buckets) :
            _state(state),
            _block_mgr_client(client),
            _tuple_stream(stream),
            _stores_tuples(num_build_tuples == 1),
            _quadratic_probing(quadratic_probing),
            _total_data_page_size(0),
            _next_node(NULL),
            _node_remaining_current_page(0),
            _num_duplicate_nodes(0),
            _max_num_buckets(max_num_buckets),
            _buckets(NULL),
            _num_buckets(num_buckets),
            _num_filled_buckets(0),
            _num_buckets_with_duplicates(0),
            _num_build_tuples(num_build_tuples),
            _has_matches(false),
            _num_probes(0),
            _num_failed_probes(0),
            _travel_length(0),
            _num_hash_collisions(0),
            _num_resizes(0) {
    DCHECK_EQ((num_buckets & (num_buckets-1)), 0) << "num_buckets must be a power of 2";
    DCHECK_GT(num_buckets, 0) << "num_buckets must be larger than 0";
    DCHECK(_stores_tuples || stream != NULL);
    DCHECK(client != NULL);
}

bool PartitionedHashTable::init() {
    int64_t buckets_byte_size = _num_buckets * sizeof(Bucket);
    if (!_state->block_mgr2()->consume_memory(_block_mgr_client, buckets_byte_size)) {
        _num_buckets = 0;
        return false;
    }
    _buckets = reinterpret_cast<Bucket*>(malloc(buckets_byte_size));
    memset(_buckets, 0, buckets_byte_size);
    return true;
}

void PartitionedHashTable::close() {
    // Print statistics only for the large or heavily used hash tables.
    // TODO: Tweak these numbers/conditions, or print them always?
    const int64_t LARGE_HT = 128 * 1024;
    const int64_t HEAVILY_USED = 1024 * 1024;
    // TODO: These statistics should go to the runtime profile as well.
    if ((_num_buckets > LARGE_HT) || (_num_probes > HEAVILY_USED)) {
        VLOG(2) << print_stats();
    }
    for (int i = 0; i < _data_pages.size(); ++i) {
        _data_pages[i]->del();
    }
#if 0
    if (DorisMetrics::hash_table_total_bytes() != NULL) {
        DorisMetrics::hash_table_total_bytes()->increment(-_total_data_page_size);
    }
#endif
    _data_pages.clear();
    if (_buckets != NULL) {
        free(_buckets);
    }
    _state->block_mgr2()->release_memory(_block_mgr_client, _num_buckets * sizeof(Bucket));
}

int64_t PartitionedHashTable::current_mem_size() const {
    return _num_buckets * sizeof(Bucket) + _num_duplicate_nodes * sizeof(DuplicateNode);
}

bool PartitionedHashTable::check_and_resize(
        uint64_t buckets_to_fill, PartitionedHashTableCtx* ht_ctx) {
    uint64_t shift = 0;
    while (_num_filled_buckets + buckets_to_fill >
            (_num_buckets << shift) * MAX_FILL_FACTOR) {
        // TODO: next prime instead of double?
        ++shift;
    }
    if (shift > 0) {
        return resize_buckets(_num_buckets << shift, ht_ctx);
    }
    return true;
}

bool PartitionedHashTable::resize_buckets(int64_t num_buckets, PartitionedHashTableCtx* ht_ctx) {
    DCHECK_EQ((num_buckets & (num_buckets-1)), 0)
        << "num_buckets=" << num_buckets << " must be a power of 2";
    DCHECK_GT(num_buckets, _num_filled_buckets) << "Cannot shrink the hash table to "
        "smaller number of buckets than the number of filled buckets.";
    VLOG(2) << "Resizing hash table from "
        << _num_buckets << " to " << num_buckets << " buckets.";
    if (_max_num_buckets != -1 && num_buckets > _max_num_buckets) {
        return false;
    }
    ++_num_resizes;

    // All memory that can grow proportional to the input should come from the block mgrs
    // mem tracker.
    // Note that while we copying over the contents of the old hash table, we need to have
    // allocated both the old and the new hash table. Once we finish, we return the memory
    // of the old hash table.
    int64_t old_size = _num_buckets * sizeof(Bucket);
    int64_t new_size = num_buckets * sizeof(Bucket);
    if (!_state->block_mgr2()->consume_memory(_block_mgr_client, new_size)) {
        return false;
    }
    Bucket* new_buckets = reinterpret_cast<Bucket*>(malloc(new_size));
    DCHECK(new_buckets != NULL);
    memset(new_buckets, 0, new_size);

    // Walk the old table and copy all the filled buckets to the new (resized) table.
    // We do not have to do anything with the duplicate nodes. This operation is expected
    // to succeed.
    for (PartitionedHashTable::Iterator iter = begin(ht_ctx); !iter.at_end();
            next_filled_bucket(&iter._bucket_idx, &iter._node)) {
        Bucket* bucket_to_copy = &_buckets[iter._bucket_idx];
        bool found = false;
        int64_t bucket_idx = probe(new_buckets, num_buckets, NULL, bucket_to_copy->hash, &found);
        DCHECK(!found);
        DCHECK_NE(bucket_idx, Iterator::BUCKET_NOT_FOUND) << " Probe failed even though "
            " there are free buckets. " << num_buckets << " " << _num_filled_buckets;
        Bucket* dst_bucket = &new_buckets[bucket_idx];
        *dst_bucket = *bucket_to_copy;
    }

    _num_buckets = num_buckets;
    free(_buckets);
    _buckets = new_buckets;
    _state->block_mgr2()->release_memory(_block_mgr_client, old_size);
    return true;
}

bool PartitionedHashTable::grow_node_array() {
    int64_t page_size = 0;
    page_size = _state->block_mgr2()->max_block_size();
    if (_data_pages.size() < NUM_SMALL_DATA_PAGES) {
        page_size = std::min(page_size, INITIAL_DATA_PAGE_SIZES[_data_pages.size()]);
    }
    BufferedBlockMgr2::Block* block = NULL;
    Status status = _state->block_mgr2()->get_new_block(
            _block_mgr_client, NULL, &block, page_size);
    DCHECK(status.ok() || block == NULL);
    if (block == NULL) {
        return false;
    }
    _data_pages.push_back(block);
    _next_node = block->allocate<DuplicateNode>(page_size);
#if 0
    if (DorisMetrics::hash_table_total_bytes() != NULL) {
        DorisMetrics::hash_table_total_bytes()->increment(page_size);
    }
#endif
    _node_remaining_current_page = page_size / sizeof(DuplicateNode);
    _total_data_page_size += page_size;
    return true;
}

void PartitionedHashTable::debug_string_tuple(
        stringstream& ss, HtData& htdata, const RowDescriptor* desc) {
    if (_stores_tuples) {
        ss << "(" << htdata.tuple << ")";
    } else {
        ss << "(" << htdata.idx.block() << ", " << htdata.idx.idx()
            << ", " << htdata.idx.offset() << ")";
    }
    if (desc != NULL) {
        Tuple* row[_num_build_tuples];
        ss << " " << get_row(htdata, reinterpret_cast<TupleRow*>(row))->to_string(*desc);
    }
}

string PartitionedHashTable::debug_string(
        bool skip_empty, bool show_match, const RowDescriptor* desc) {
    stringstream ss;
    ss << endl;
    for (int i = 0; i < _num_buckets; ++i) {
        if (skip_empty && !_buckets[i].filled) {
            continue;
        }
        ss << i << ": ";
        if (show_match) {
            if (_buckets[i].matched) {
                ss << " [M]";
            } else {
                ss << " [U]";
            }
        }
        if (_buckets[i].hasDuplicates) {
            DuplicateNode* node = _buckets[i].bucketData.duplicates;
            bool first = true;
            ss << " [D] ";
            while (node != NULL) {
                if (!first) {
                    ss << ",";
                }
                debug_string_tuple(ss, node->htdata, desc);
                node = node->next;
                first = false;
            }
        } else {
            ss << " [B] ";
            if (_buckets[i].filled) {
                debug_string_tuple(ss, _buckets[i].bucketData.htdata, desc);
            } else {
                ss << " - ";
            }
        }
        ss << endl;
    }
    return ss.str();
}

string PartitionedHashTable::print_stats() const {
    double curr_fill_factor = (double)_num_filled_buckets / (double)_num_buckets;
    double avg_travel = (double)_travel_length / (double)_num_probes;
    double avg_collisions = (double)_num_hash_collisions / (double)_num_filled_buckets;
    stringstream ss;
    ss << "Buckets: " << _num_buckets << " " << _num_filled_buckets << " "
        << curr_fill_factor << endl;
    ss << "Duplicates: " << _num_buckets_with_duplicates << " buckets "
        << _num_duplicate_nodes << " nodes" << endl;
    ss << "Probes: " << _num_probes << endl;
    ss << "FailedProbes: " << _num_failed_probes << endl;
    ss << "Travel: " << _travel_length << " " << avg_travel << endl;
    ss << "HashCollisions: " << _num_hash_collisions << " " << avg_collisions << endl;
    ss << "Resizes: " << _num_resizes << endl;
    return ss.str();
}

} // namespace doris

