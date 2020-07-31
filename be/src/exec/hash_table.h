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

#ifndef DORIS_BE_SRC_QUERY_EXEC_HASH_TABLE_H
#define DORIS_BE_SRC_QUERY_EXEC_HASH_TABLE_H

#include <vector>
#include <boost/cstdint.hpp>

#include "codegen/doris_ir.h"
#include "common/logging.h"
#include "util/hash_util.hpp"

namespace doris {

class Expr;
class ExprContext;
class RowDescriptor;
class Tuple;
class TupleRow;
class MemTracker;
class RuntimeState;

using std::vector;

// Hash table implementation designed for hash aggregation and hash joins.  This is not
// templatized and is tailored to the usage pattern for aggregation and joins.  The
// hash table store TupleRows and allows for different exprs for insertions and finds.
// This is the pattern we use for joins and aggregation where the input/build tuple
// row descriptor is different from the find/probe descriptor.
// The table is optimized for the query engine's use case as much as possible and is not
// intended to be a generic hash table implementation.  The API loosely mimics the
// std::hashset API.
//
// The hash table stores evaluated expr results for the current row being processed
// when possible into a contiguous memory buffer. This allows for very efficient
// computation for hashing.  The implementation is also designed to allow codegen
// for some paths.
//
// The hash table does not support removes. The hash table is not thread safe.
//
// The implementation is based on the boost multiset.  The hashtable is implemented by
// two data structures: a vector of buckets and a vector of nodes.  Inserted values
// are stored as nodes (in the order they are inserted).  The buckets (indexed by the
// mod of the hash) contain pointers to the node vector.  Nodes that fall in the same
// bucket are linked together (the bucket pointer gets you the head of that linked list).
// When growing the hash table, the number of buckets is doubled, and nodes from a
// particular bucket either stay in place or move to an analogous bucket in the second
// half of buckets. This behavior allows us to avoid moving about half the nodes each
// time, and maintains good cache properties by only accessing 2 buckets at a time.
// The node vector is modified in place.
// Due to the doubling nature of the buckets, we require that the number of buckets is a
// power of 2. This allows us to determine if a node needs to move by simply checking a
// single bit, and further allows us to initially hash nodes using a bitmask.
//
// TODO: this is not a fancy hash table in terms of memory access patterns (cuckoo-hashing
// or something that spills to disk). We will likely want to invest more time into this.
// TODO: hash-join and aggregation have very different access patterns.  Joins insert
// all the rows and then calls scan to find them.  Aggregation interleaves find() and
// inserts().  We can want to optimize joins more heavily for inserts() (in particular
// growing).
class HashTable {
private:
    struct Node;
public:
    class Iterator;

    // Create a hash table.
    //  - build_exprs are the exprs that should be used to evaluate rows during insert().
    //  - probe_exprs are used during find()
    //  - num_build_tuples: number of Tuples in the build tuple row
    //  - stores_nulls: if false, TupleRows with nulls are ignored during Insert
    //  - num_buckets: number of buckets that the hash table should be initialized to
    //  - mem_limits: if non-empty, all memory allocation for nodes and for buckets is
    //    tracked against those limits; the limits must be valid until the d'tor is called
    //  - initial_seed: Initial seed value to use when computing hashes for rows
    HashTable(
        const std::vector<ExprContext*>& build_exprs,
        const std::vector<ExprContext*>& probe_exprs,
        int num_build_tuples, bool stores_nulls, 
        const std::vector<bool>& finds_nulls,
        int32_t initial_seed,
        const std::shared_ptr<MemTracker>& mem_tracker,
        int64_t num_buckets);

    ~HashTable();

    // Call to cleanup any resources. Must be called once.
    void close();

    // Insert row into the hash table.  Row will be evaluated over _build_expr_ctxs
    // This will grow the hash table if necessary
    void IR_ALWAYS_INLINE insert(TupleRow* row) {
        if (_num_filled_buckets > _num_buckets_till_resize) {
            // TODO: next prime instead of double?
            resize_buckets(_num_buckets * 2);
        }

        insert_impl(row);
    }

    // Insert row into the hash table. if the row is alread exist will not insert
    void IR_ALWAYS_INLINE insert_unique(TupleRow* row) {
        if (find(row, false) == end()) {
            insert(row);
        }
    }


    // Returns the start iterator for all rows that match 'probe_row'.  'probe_row' is
    // evaluated with _probe_expr_ctxs.  The iterator can be iterated until HashTable::end()
    // to find all the matching rows.
    // Only one scan be in progress at any time (i.e. it is not legal to call
    // find(), begin iterating through all the matches, call another find(),
    // and continuing iterator from the first scan iterator).
    // Advancing the returned iterator will go to the next matching row.  The matching
    // rows are evaluated lazily (i.e. computed as the Iterator is moved).
    // Returns HashTable::end() if there is no match.
    Iterator IR_ALWAYS_INLINE find(TupleRow* probe_row, bool probe = true);

    // Returns number of elements in the hash table
    int64_t size() {
        return _num_nodes;
    }

    // Returns the number of buckets
    int64_t num_buckets() {
        return _buckets.size();
    }

    // true if any of the MemTrackers was exceeded
    bool exceeded_limit() const {
        return _exceeded_limit;
    }

    // Returns the load factor (the number of non-empty buckets)
    float load_factor() {
        return _num_filled_buckets / static_cast<float>(_buckets.size());
    }

    // Returns the number of bytes allocated to the hash table
    int64_t byte_size() const {
        return _node_byte_size * _nodes_capacity + sizeof(Bucket) * _buckets.size();
    }

    // Returns the results of the exprs at 'expr_idx' evaluated over the last row
    // processed by the HashTable.
    // This value is invalid if the expr evaluated to NULL.
    // TODO: this is an awkward abstraction but aggregation node can take advantage of
    // it and save some expr evaluation calls.
    void* last_expr_value(int expr_idx) const {
        return _expr_values_buffer + _expr_values_buffer_offsets[expr_idx];
    }

    // Returns if the expr at 'expr_idx' evaluated to NULL for the last row.
    bool last_expr_value_null(int expr_idx) const {
        return _expr_value_null_bits[expr_idx];
    }

    // Return beginning of hash table.  Advancing this iterator will traverse all
    // elements.
    Iterator begin();

    // Returns end marker
    Iterator end() {
        return Iterator();
    }

    // Dump out the entire hash table to string.  If skip_empty, empty buckets are
    // skipped.  If build_desc is non-null, the build rows will be output.  Otherwise
    // just the build row addresses.
    std::string debug_string(bool skip_empty, const RowDescriptor* build_desc);

    // stl-like iterator interface.
    class Iterator {
    public:
        Iterator() : _table(NULL), _bucket_idx(-1), _node_idx(-1) {
        }

        // Iterates to the next element.  In the case where the iterator was
        // from a Find, this will lazily evaluate that bucket, only returning
        // TupleRows that match the current scan row.
        template<bool check_match>
        void IR_ALWAYS_INLINE next();

        // Returns the current row or NULL if at end.
        TupleRow* get_row() {
            if (_node_idx == -1) {
                return NULL;
            }
            return _table->get_node(_node_idx)->data();
        }

        // Returns Hash
        uint32_t get_hash() {
            return _table->get_node(_node_idx)->_hash;
        }

        // Returns if the iterator is at the end
        bool has_next() {
            return _node_idx != -1;
        }

        // Returns true if this iterator is at the end, i.e. get_row() cannot be called.
        bool at_end() {
            return _node_idx == -1;
        }

        // Sets as matched the node currently pointed by the iterator. The iterator
        // cannot be AtEnd().
        void set_matched() {
            DCHECK(!at_end());
            Node *node = _table->get_node(_node_idx);
            node->matched = true;
        }

        bool matched() {
              DCHECK(!at_end());
            Node *node = _table->get_node(_node_idx);
              return node->matched;
        }

        bool operator==(const Iterator& rhs) {
            return _bucket_idx == rhs._bucket_idx && _node_idx == rhs._node_idx;
        }

        bool operator!=(const Iterator& rhs) {
            return _bucket_idx != rhs._bucket_idx || _node_idx != rhs._node_idx;
        }

    private:
        friend class HashTable;

        Iterator(HashTable* table, int bucket_idx, int64_t node, uint32_t hash) :
            _table(table),
            _bucket_idx(bucket_idx),
            _node_idx(node),
            _scan_hash(hash) {
        }

        HashTable* _table;
        // Current bucket idx
        int64_t _bucket_idx;
        // Current node idx (within current bucket)
        int64_t _node_idx;
        // cached hash value for the row passed to find()()
        uint32_t _scan_hash;
    };

private:
    friend class Iterator;
    friend class HashTableTest;

    // Header portion of a Node.  The node data (TupleRow) is right after the
    // node memory to maximize cache hits.
    struct Node {
        int64_t _next_idx;  // chain to next node for collisions
        uint32_t _hash;     // Cache of the hash for _data
        bool matched;

        Node():_next_idx(-1),
               _hash(-1),
               matched(false) {
        } 

        TupleRow* data() {
            uint8_t* mem = reinterpret_cast<uint8_t*>(this);
            DCHECK_EQ(reinterpret_cast<uint64_t>(mem) % 8, 0);
            return reinterpret_cast<TupleRow*>(mem + sizeof(Node));
        }
    };

    struct Bucket {
        int64_t _node_idx;

        Bucket() {
            _node_idx = -1;
        }
    };

    // Returns the next non-empty bucket and updates idx to be the index of that bucket.
    // If there are no more buckets, returns NULL and sets idx to -1
    Bucket* next_bucket(int64_t* bucket_idx);

    // Returns node at idx.  Tracking structures do not use pointers since they will
    // change as the HashTable grows.
    Node* get_node(int64_t idx) {
        DCHECK_NE(idx, -1);
        return reinterpret_cast<Node*>(_nodes + _node_byte_size * idx);
    }

    // Resize the hash table to 'num_buckets'
    void resize_buckets(int64_t num_buckets);

    // Insert row into the hash table
    void IR_ALWAYS_INLINE insert_impl(TupleRow* row);

    // Chains the node at 'node_idx' to 'bucket'.  Nodes in a bucket are chained
    // as a linked list; this places the new node at the beginning of the list.
    void add_to_bucket(Bucket* bucket, int64_t node_idx, Node* node);

    // Moves a node from one bucket to another. 'previous_node' refers to the
    // node (if any) that's chained before this node in from_bucket's linked list.
    void move_node(Bucket* from_bucket, Bucket* to_bucket, int64_t node_idx, Node* node,
                  Node* previous_node);

    // Evaluate the exprs over row and cache the results in '_expr_values_buffer'.
    // Returns whether any expr evaluated to NULL
    // This will be replaced by codegen
    bool eval_row(TupleRow* row, const std::vector<ExprContext*>& exprs);

    // Evaluate 'row' over _build_expr_ctxs caching the results in '_expr_values_buffer'
    // This will be replaced by codegen.  We do not want this function inlined when
    // cross compiled because we need to be able to differentiate between EvalBuildRow
    // and EvalProbeRow by name and the _build_expr_ctxs/_probe_expr_ctxs are baked into
    // the codegen'd function.
    bool IR_NO_INLINE eval_build_row(TupleRow* row) {
        return eval_row(row, _build_expr_ctxs);
    }

    // Evaluate 'row' over _probe_expr_ctxs caching the results in '_expr_values_buffer'
    // This will be replaced by codegen.
    bool IR_NO_INLINE eval_probe_row(TupleRow* row) {
        return eval_row(row, _probe_expr_ctxs);
    }

    // Compute the hash of the values in _expr_values_buffer.
    // This will be replaced by codegen.  We don't want this inlined for replacing
    // with codegen'd functions so the function name does not change.
    uint32_t IR_NO_INLINE hash_current_row() {
        if (_var_result_begin == -1) {
            // This handles NULLs implicitly since a constant seed value was put
            // into results buffer for nulls.
            return HashUtil::hash(_expr_values_buffer, _results_buffer_size, _initial_seed);
        } else {
            return hash_variable_len_row();
        }
    }

    // Compute the hash of the values in _expr_values_buffer for rows with variable length
    // fields (e.g. strings)
    uint32_t hash_variable_len_row();

    // Returns true if the values of build_exprs evaluated over 'build_row' equal
    // the values cached in _expr_values_buffer
    // This will be replaced by codegen.
    bool equals(TupleRow* build_row);

    // Grow the node array.
    void grow_node_array();

    // Sets _mem_tracker_exceeded to true and MEM_LIMIT_EXCEEDED for the query.
    // allocation_size is the attempted size of the allocation that would have
    // brought us over the mem limit.
    void mem_limit_exceeded(int64_t allocation_size);

    // Load factor that will trigger growing the hash table on insert.  This is
    // defined as the number of non-empty buckets / total_buckets
    static const float MAX_BUCKET_OCCUPANCY_FRACTION;

    const std::vector<ExprContext*>& _build_expr_ctxs;
    const std::vector<ExprContext*>& _probe_expr_ctxs;

    // Number of Tuple* in the build tuple row
    const int _num_build_tuples;
    // outer join || has null equal join should be true
    const bool _stores_nulls;
    // true: the null-safe equal '<=>' is true. The row with null shoud be judged.
    // false: the equal '=' is false. The row with null should be filtered.
    const std::vector<bool> _finds_nulls;

    const int32_t _initial_seed;

    // Size of hash table nodes.  This includes a fixed size header and the Tuple*'s that
    // follow.
    const int _node_byte_size;
    // Number of non-empty buckets.  Used to determine when to grow and rehash
    int64_t _num_filled_buckets;
    // Memory to store node data.  This is not allocated from a pool to take advantage
    // of realloc.
    // TODO: integrate with mem pools
    uint8_t* _nodes;
    // number of nodes stored (i.e. size of hash table)
    int64_t _num_nodes;
    // max number of nodes that can be stored in '_nodes' before realloc
    int64_t _nodes_capacity;

    bool _exceeded_limit;   // true if any of _mem_trackers[].limit_exceeded()

    std::shared_ptr<MemTracker> _mem_tracker;
    // Set to true if the hash table exceeds the memory limit. If this is set,
    // subsequent calls to Insert() will be ignored.
    bool _mem_limit_exceeded;

    std::vector<Bucket> _buckets;

    // equal to _buckets.size() but more efficient than the size function
    int64_t _num_buckets;

    // The number of filled buckets to trigger a resize.  This is cached for efficiency
    int64_t _num_buckets_till_resize;

    // Cache of exprs values for the current row being evaluated.  This can either
    // be a build row (during insert()) or probe row (during find()).
    std::vector<int> _expr_values_buffer_offsets;

    // byte offset into _expr_values_buffer that begins the variable length results
    int _var_result_begin;

    // byte size of '_expr_values_buffer'
    int _results_buffer_size;

    // buffer to store evaluated expr results.  This address must not change once
    // allocated since the address is baked into the codegen
    uint8_t* _expr_values_buffer;

    // Use bytes instead of bools to be compatible with llvm.  This address must
    // not change once allocated.
    uint8_t* _expr_value_null_bits;
};

}

#endif
