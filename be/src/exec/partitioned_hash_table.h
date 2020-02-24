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

#ifndef DORIS_BE_SRC_EXEC_PARTITIONED_HASH_TABLE_H
#define DORIS_BE_SRC_EXEC_PARTITIONED_HASH_TABLE_H

#include <vector>
#include <boost/cstdint.hpp>
#include <boost/scoped_ptr.hpp>

#include "codegen/doris_ir.h"
#include "util/logging.h"
#include "runtime/buffered_block_mgr2.h"
#include "runtime/buffered_tuple_stream2.h"
#include "runtime/buffered_tuple_stream2.inline.h"
#include "runtime/mem_tracker.h"
#include "runtime/mem_tracker.h"
#include "runtime/tuple_row.h"
#include "util/hash_util.hpp"
#include "util/bit_util.h"

namespace doris {

class Expr;
class ExprContext;
class MemTracker;
class MemTracker;
class RowDescriptor;
class RuntimeState;
class Tuple;
class TupleRow;
class PartitionedHashTable;

// Linear or quadratic probing hash table implementation tailored to the usage pattern
// for partitioned hash aggregation and hash joins. The hash table stores TupleRows and
// allows for different exprs for insertions and finds. This is the pattern we use for
// joins and aggregation where the input/build tuple row descriptor is different from the
// find/probe descriptor. The implementation is designed to allow codegen for some paths.
//
// In addition to the hash table there is also an accompanying hash table context that
// is used for insertions and probes. For example, the hash table context stores
// evaluated expr results for the current row being processed when possible into a
// contiguous memory buffer. This allows for efficient hash computation.
//
// The hash table does not support removes. The hash table is not thread safe.
// The table is optimized for the partition hash aggregation and hash joins and is not
// intended to be a generic hash table implementation. The API loosely mimics the
// std::hashset API.
//
// The data (rows) are stored in a BufferedTupleStream2. The basic data structure of this
// hash table is a vector of buckets. The buckets (indexed by the mod of the hash)
// contain a pointer to either the slot in the tuple-stream or in case of duplicate
// values, to the head of a linked list of nodes that in turn contain a pointer to
// tuple-stream slots. When inserting an entry we start at the bucket at position
// (hash % size) and search for either a bucket with the same hash or for an empty
// bucket. If a bucket with the same hash is found, we then compare for row equality and
// either insert a duplicate node if the equality is true, or continue the search if the
// row equality is false. Similarly, when probing we start from the bucket at position
// (hash % size) and search for an entry with the same hash or for an empty bucket.
// In the former case, we then check for row equality and continue the search if the row
// equality is false. In the latter case, the probe is not successful. When growing the
// hash table, the number of buckets is doubled. We trigger a resize when the fill
// factor is approx 75%. Due to the doubling nature of the buckets, we require that the
// number of buckets is a power of 2. This allows us to perform a modulo of the hash
// using a bitmask.
//
// We choose to use linear or quadratic probing because they exhibit good (predictable)
// cache behavior.
//
// The first NUM_SMALL_BLOCKS of _nodes are made of blocks less than the IO size (of 8MB)
// to reduce the memory footprint of small queries.
//
// TODO: Compare linear and quadratic probing and remove the loser.
// TODO: We currently use 32-bit hashes. There is room in the bucket structure for at
// least 48-bits. We should exploit this space.
// TODO: Consider capping the probes with a threshold value. If an insert reaches
// that threshold it is inserted to another linked list of overflow entries.
// TODO: Smarter resizes, and perhaps avoid using powers of 2 as the hash table size.
// TODO: this is not a fancy hash table in terms of memory access patterns
// (cuckoo-hashing or something that spills to disk). We will likely want to invest
// more time into this.
// TODO: hash-join and aggregation have very different access patterns.  Joins insert
// all the rows and then calls scan to find them.  Aggregation interleaves find() and
// Inserts().  We may want to optimize joins more heavily for Inserts() (in particular
// growing).
// TODO: Batched interface for inserts and finds.
// TODO: Do we need to check mem limit exceeded so often. Check once per batch?

// Control block for a hash table. This class contains the logic as well as the variables
// needed by a thread to operate on a hash table.
class PartitionedHashTableCtx {
public:
    // Create a hash table context.
    //  - build_exprs are the exprs that should be used to evaluate rows during insert().
    //  - probe_exprs are used during find()
    //  - stores_nulls: if false, TupleRows with nulls are ignored during insert
    //  - finds_nulls: if false, find() returns End() for TupleRows with nulls
    //      even if stores_nulls is true
    //  - initial_seed: Initial seed value to use when computing hashes for rows with
    //    level 0. Other levels have their seeds derived from this seed.
    //  - The max levels we will hash with.
    PartitionedHashTableCtx(const std::vector<ExprContext*>& build_expr_ctxs,
            const std::vector<ExprContext*>& probe_expr_ctxs, bool stores_nulls,
            bool finds_nulls, int32_t initial_seed, int max_levels,
            int num_build_tuples);

    // Call to cleanup any resources.
    void close();

    void set_level(int level);
    int level() const { return _level; }
    uint32_t seed(int level) { return _seeds.at(level); }

    TupleRow* row() const { return _row; }

    // Returns the results of the exprs at 'expr_idx' evaluated over the last row
    // processed.
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

    // Evaluate and hash the build/probe row, returning in *hash. Returns false if this
    // row should be rejected (doesn't need to be processed further) because it
    // contains NULL.
    // These need to be inlined in the IR module so we can find and replace the calls to
    // EvalBuildRow()/EvalProbeRow().
    bool IR_ALWAYS_INLINE eval_and_hash_build(TupleRow* row, uint32_t* hash);
    bool IR_ALWAYS_INLINE eval_and_hash_probe(TupleRow* row, uint32_t* hash);

    int results_buffer_size() const { return _results_buffer_size; }

private:
    friend class PartitionedHashTable;
    friend class PartitionedHashTableTest_HashEmpty_Test;

    // Compute the hash of the values in _expr_values_buffer.
    // This will be replaced by codegen.  We don't want this inlined for replacing
    // with codegen'd functions so the function name does not change.
    uint32_t IR_NO_INLINE HashCurrentRow() {
        DCHECK_LT(_level, _seeds.size());
        if (_var_result_begin == -1) {
            // This handles NULLs implicitly since a constant seed value was put
            // into results buffer for nulls.
            // TODO: figure out which hash function to use. We need to generate uncorrelated
            // hashes by changing just the seed. CRC does not have this property and FNV is
            // okay. We should switch to something else.
            return hash_help(_expr_values_buffer, _results_buffer_size, _seeds[_level]);
        } else {
            return PartitionedHashTableCtx::hash_variable_len_row();
        }
    }

    // Wrapper function for calling correct HashUtil function in non-codegen'd case.
    uint32_t inline hash_help(const void* input, int len, int32_t hash) {
        // Use CRC hash at first level for better performance. Switch to murmur hash at
        // subsequent levels since CRC doesn't randomize well with different seed inputs.
        if (_level == 0) {
            return HashUtil::hash(input, len, hash);
        }
        return HashUtil::murmur_hash2_64(input, len, hash);
    }

    // Evaluate 'row' over build exprs caching the results in '_expr_values_buffer' This
    // will be replaced by codegen.  We do not want this function inlined when cross
    // compiled because we need to be able to differentiate between EvalBuildRow and
    // EvalProbeRow by name and the build/probe exprs are baked into the codegen'd
    // function.
    bool IR_NO_INLINE EvalBuildRow(TupleRow* row) {
        return eval_row(row, _build_expr_ctxs);
    }

    // Evaluate 'row' over probe exprs caching the results in '_expr_values_buffer'
    // This will be replaced by codegen.
    bool IR_NO_INLINE EvalProbeRow(TupleRow* row) {
        return eval_row(row, _probe_expr_ctxs);
    }

    // Compute the hash of the values in _expr_values_buffer for rows with variable length
    // fields (e.g. strings).
    uint32_t hash_variable_len_row();

    // Evaluate the exprs over row and cache the results in '_expr_values_buffer'.
    // Returns whether any expr evaluated to NULL.
    // This will be replaced by codegen.
    bool eval_row(TupleRow* row, const std::vector<ExprContext*>& ctxs);

    // Returns true if the values of build_exprs evaluated over 'build_row' equal
    // the values cached in _expr_values_buffer.
    // This will be replaced by codegen.
    bool IR_NO_INLINE equals(TupleRow* build_row);

    const std::vector<ExprContext*>& _build_expr_ctxs;
    const std::vector<ExprContext*>& _probe_expr_ctxs;

    // Constants on how the hash table should behave. Joins and aggs have slightly
    // different behavior.
    // TODO: these constants are an ideal candidate to be removed with codegen.
    // TODO: ..or with template-ization
    const bool _stores_nulls;
    const bool _finds_nulls;

    // The current level this context is working on. Each level needs to use a
    // different seed.
    int _level;

    // The seeds to use for hashing. Indexed by the level.
    std::vector<uint32_t> _seeds;

    // Cache of exprs values for the current row being evaluated.  This can either
    // be a build row (during insert()) or probe row (during find()).
    std::vector<int> _expr_values_buffer_offsets;

    // Byte offset into '_expr_values_buffer' that begins the variable length results.
    // If -1, there are no variable length slots. Never changes once set, can be removed
    // with codegen.
    int _var_result_begin;

    // Byte size of '_expr_values_buffer'. Never changes once set, can be removed with
    // codegen.
    int _results_buffer_size;

    // Buffer to store evaluated expr results.  This address must not change once
    // allocated since the address is baked into the codegen.
    uint8_t* _expr_values_buffer;

    // Use bytes instead of bools to be compatible with llvm.  This address must
    // not change once allocated.
    uint8_t* _expr_value_null_bits;

    // Scratch buffer to generate rows on the fly.
    TupleRow* _row;

    // Cross-compiled functions to access member variables used in codegen_hash_current_row().
    uint32_t get_hash_seed() const;
};

// The hash table consists of a contiguous array of buckets that contain a pointer to the
// data, the hash value and three flags: whether this bucket is filled, whether this
// entry has been matched (used in right and full joins) and whether this entry has
// duplicates. If there are duplicates, then the data is pointing to the head of a
// linked list of duplicate nodes that point to the actual data. Note that the duplicate
// nodes do not contain the hash value, because all the linked nodes have the same hash
// value, the one in the bucket. The data is either a tuple stream index or a Tuple*.
// This array of buckets is sparse, we are shooting for up to 3/4 fill factor (75%). The
// data allocated by the hash table comes from the BufferedBlockMgr2.
class PartitionedHashTable {
private:

    // Either the row in the tuple stream or a pointer to the single tuple of this row.
    union HtData {
        BufferedTupleStream2::RowIdx idx;
        Tuple* tuple;
    };

    // Linked list of entries used for duplicates.
    struct DuplicateNode {
        // Used for full outer and right {outer, anti, semi} joins. Indicates whether the
        // row in the DuplicateNode has been matched.
        // From an abstraction point of view, this is an awkward place to store this
        // information.
        // TODO: Fold this flag in the next pointer below.
        bool matched;

        // Chain to next duplicate node, NULL when end of list.
        DuplicateNode* next;
        HtData htdata;
    };

    struct Bucket {
        // Whether this bucket contains a vaild entry, or it is empty.
        bool filled;

        // Used for full outer and right {outer, anti, semi} joins. Indicates whether the
        // row in the bucket has been matched.
        // From an abstraction point of view, this is an awkward place to store this
        // information but it is efficient. This space is otherwise unused.
        bool matched;

        // Used in case of duplicates. If true, then the bucketData union should be used as
        // 'duplicates'.
        bool hasDuplicates;

        // Cache of the hash for data.
        // TODO: Do we even have to cache the hash value?
        uint32_t hash;

        // Either the data for this bucket or the linked list of duplicates.
        union {
            HtData htdata;
            DuplicateNode* duplicates;
        } bucketData;
    };

public:
    class Iterator;

    // Returns a newly allocated PartitionedHashTable. The probing algorithm is set by the
    // FLAG_enable_quadratic_probing.
    //  - client: block mgr client to allocate data pages from.
    //  - num_build_tuples: number of Tuples in the build tuple row.
    //  - tuple_stream: the tuple stream which contains the tuple rows index by the
    //    hash table. Can be NULL if the rows contain only a single tuple, in which
    //    case the 'tuple_stream' is unused.
    //  - max_num_buckets: the maximum number of buckets that can be stored. If we
    //    try to grow the number of buckets to a larger number, the inserts will fail.
    //    -1, if it unlimited.
    //  - initial_num_buckets: number of buckets that the hash table should be initialized
    //    with.
    static PartitionedHashTable* create(RuntimeState* state, BufferedBlockMgr2::Client* client,
            int num_build_tuples, BufferedTupleStream2* tuple_stream, int64_t max_num_buckets,
            int64_t initial_num_buckets);

    // Allocates the initial bucket structure. Returns false if OOM.
    bool init();

    // Call to cleanup any resources. Must be called once.
    void close();

    // Inserts the row to the hash table. Returns true if the insertion was successful.
    // Always returns true if the table has free buckets and the key is not a duplicate.
    // The caller is responsible for ensuring that the table has free buckets
    // 'idx' is the index into _tuple_stream for this row. If the row contains more than
    // one tuple, the 'idx' is stored instead of the 'row'. The 'row' is not copied by the
    // hash table and the caller must guarantee it stays in memory. This will not grow the
    // hash table. In the case that there is a need to insert a duplicate node, instead of
    // filling a new bucket, and there is not enough memory to insert a duplicate node,
    // the insert fails and this function returns false.
    // Used during the build phase of hash joins.
    bool IR_ALWAYS_INLINE insert(PartitionedHashTableCtx* ht_ctx,
            const BufferedTupleStream2::RowIdx& idx, TupleRow* row, uint32_t hash);

    // Same as insert() but for inserting a single Tuple. The 'tuple' is not copied by
    // the hash table and the caller must guarantee it stays in memory.
    bool IR_ALWAYS_INLINE insert(PartitionedHashTableCtx* ht_ctx, Tuple* tuple, uint32_t hash);

    // Returns an iterator to the bucket matching the last row evaluated in 'ht_ctx'.
    // Returns PartitionedHashTable::End() if no match is found. The iterator can be iterated until
    // PartitionedHashTable::End() to find all the matching rows. Advancing the returned iterator will
    // go to the next matching row. The matching rows do not need to be evaluated since all
    // the nodes of a bucket are duplicates. One scan can be in progress for each 'ht_ctx'.
    // Used during the probe phase of hash joins.
    Iterator IR_ALWAYS_INLINE find(PartitionedHashTableCtx* ht_ctx, uint32_t hash);

    // If a match is found in the table, return an iterator as in find(). If a match was
    // not present, return an iterator pointing to the empty bucket where the key should
    // be inserted. Returns End() if the table is full. The caller can set the data in
    // the bucket using a Set*() method on the iterator.
    Iterator IR_ALWAYS_INLINE find_bucket(PartitionedHashTableCtx* ht_ctx, uint32_t hash,
            bool* found);

    // Returns number of elements inserted in the hash table
    int64_t size() const {
        return _num_filled_buckets - _num_buckets_with_duplicates + _num_duplicate_nodes;
    }

    // Returns the number of empty buckets.
    int64_t empty_buckets() const { return _num_buckets - _num_filled_buckets; }

    // Returns the number of buckets
    int64_t num_buckets() const { return _num_buckets; }

    // Returns the load factor (the number of non-empty buckets)
    double load_factor() const {
        return static_cast<double>(_num_filled_buckets) / _num_buckets;
    }

    // Returns an estimate of the number of bytes needed to build the hash table
    // structure for 'num_rows'. To do that, it estimates the number of buckets,
    // rounded up to a power of two, and also assumes that there are no duplicates.
    static int64_t EstimateNumBuckets(int64_t num_rows) {
        // Assume max 66% fill factor and no duplicates.
        return BitUtil::next_power_of_two(3 * num_rows / 2);
    }
    static int64_t EstimateSize(int64_t num_rows) {
        int64_t num_buckets = EstimateNumBuckets(num_rows);
        return num_buckets * sizeof(Bucket);
    }

    // Returns the memory occupied by the hash table, takes into account the number of
    // duplicates.
    int64_t current_mem_size() const;

    // Calculates the fill factor if 'buckets_to_fill' additional buckets were to be
    // filled and resizes the hash table so that the projected fill factor is below the
    // max fill factor.
    // If it returns true, then it is guaranteed at least 'rows_to_add' rows can be
    // inserted without need to resize.
    bool check_and_resize(uint64_t buckets_to_fill, PartitionedHashTableCtx* ht_ctx);

    // Returns the number of bytes allocated to the hash table
    int64_t byte_size() const { return _total_data_page_size; }

    // Returns an iterator at the beginning of the hash table.  Advancing this iterator
    // will traverse all elements.
    Iterator begin(PartitionedHashTableCtx* ht_ctx);

    // Return an iterator pointing to the first element (Bucket or DuplicateNode, if the
    // bucket has duplicates) in the hash table that does not have its matched flag set.
    // Used in right joins and full-outer joins.
    Iterator first_unmatched(PartitionedHashTableCtx* ctx);

    // Return true if there was a least one match.
    bool HasMatches() const { return _has_matches; }

    // Return end marker.
    Iterator End() { return Iterator(); }

    // Dump out the entire hash table to string.  If 'skip_empty', empty buckets are
    // skipped.  If 'show_match', it also prints the matched flag of each node. If
    // 'build_desc' is non-null, the build rows will be printed. Otherwise, only the
    // the addresses of the build rows will be printed.
    std::string debug_string(bool skip_empty, bool show_match,
            const RowDescriptor* build_desc);

    // Print the content of a bucket or node.
    void debug_string_tuple(std::stringstream& ss, HtData& htdata, const RowDescriptor* desc);

    // Update and print some statistics that can be used for performance debugging.
    std::string print_stats() const;

    // stl-like iterator interface.
    class Iterator {
    private:
        // Bucket index value when probe is not successful.
        static const int64_t BUCKET_NOT_FOUND = -1;

    public:

        Iterator() : _table(NULL), _row(NULL), _bucket_idx(BUCKET_NOT_FOUND), _node(NULL) { }

        // Iterates to the next element. It should be called only if !AtEnd().
        void IR_ALWAYS_INLINE next();

        // Iterates to the next duplicate node. If the bucket does not have duplicates or
        // when it reaches the last duplicate node, then it moves the Iterator to AtEnd().
        // Used when we want to iterate over all the duplicate nodes bypassing the next()
        // interface (e.g. in semi/outer joins without other_join_conjuncts, in order to
        // iterate over all nodes of an unmatched bucket).
        void IR_ALWAYS_INLINE next_duplicate();

        // Iterates to the next element that does not have its matched flag set. Used in
        // right-outer and full-outer joins.
        void next_unmatched();

        // Return the current row or tuple. Callers must check the iterator is not AtEnd()
        // before calling them.  The returned row is owned by the iterator and valid until
        // the next call to get_row(). It is safe to advance the iterator.
        TupleRow* get_row() const;
        Tuple* get_tuple() const;

        // Set the current tuple for an empty bucket. Designed to be used with the
        // iterator returned from find_bucket() in the case when the value is not found.
        // It is not valid to call this function if the bucket already has an entry.
        void set_tuple(Tuple* tuple, uint32_t hash);

        // Sets as matched the Bucket or DuplicateNode currently pointed by the iterator,
        // depending on whether the bucket has duplicates or not. The iterator cannot be
        // AtEnd().
        void set_matched();

        // Returns the 'matched' flag of the current Bucket or DuplicateNode, depending on
        // whether the bucket has duplicates or not. It should be called only if !AtEnd().
        bool is_matched() const;

        // Resets everything but the pointer to the hash table.
        void set_at_end();

        // Returns true if this iterator is at the end, i.e. get_row() cannot be called.
        bool at_end() const { return _bucket_idx == BUCKET_NOT_FOUND; }

    private:
        friend class PartitionedHashTable;

        Iterator(PartitionedHashTable* table, TupleRow* row, int bucket_idx, DuplicateNode* node)
            : _table(table),
            _row(row),
            _bucket_idx(bucket_idx),
            _node(node) {
            }

        PartitionedHashTable* _table;
        TupleRow* _row;

        // Current bucket idx.
        // TODO: Use uint32_t?
        int64_t _bucket_idx;

        // Pointer to the current duplicate node.
        DuplicateNode* _node;
    };

private:
    friend class Iterator;
    friend class PartitionedHashTableTest;

    // Hash table constructor. Private because Create() should be used, instead
    // of calling this constructor directly.
    //  - quadratic_probing: set to true when the probing algorithm is quadratic, as
    //    opposed to linear.
    PartitionedHashTable(bool quadratic_probing, RuntimeState* state, BufferedBlockMgr2::Client* client,
            int num_build_tuples, BufferedTupleStream2* tuple_stream,
            int64_t max_num_buckets, int64_t initial_num_buckets);

    // Performs the probing operation according to the probing algorithm (linear or
    // quadratic. Returns one of the following:
    // (a) the index of the bucket that contains the entry that matches with the last row
    //     evaluated in 'ht_ctx'. If 'ht_ctx' is NULL then it does not check for row
    //     equality and returns the index of the first empty bucket.
    // (b) the index of the first empty bucket according to the probing algorithm (linear
    //     or quadratic), if the entry is not in the hash table or 'ht_ctx' is NULL.
    // (c) Iterator::BUCKET_NOT_FOUND if the probe was not successful, i.e. the maximum
    //     distance was traveled without finding either an empty or a matching bucket.
    // Using the returned index value, the caller can create an iterator that can be
    // iterated until End() to find all the matching rows.
    // EvalAndHashBuild() or EvalAndHashProb(e) must have been called before calling this.
    // 'hash' must be the hash returned by these functions.
    // 'found' indicates that a bucket that contains an equal row is found.
    //
    // There are wrappers of this function that perform the find and insert logic.
    int64_t IR_ALWAYS_INLINE probe(Bucket* buckets, int64_t num_buckets,
            PartitionedHashTableCtx* ht_ctx, uint32_t hash,  bool* found);

    // Performs the insert logic. Returns the HtData* of the bucket or duplicate node
    // where the data should be inserted. Returns NULL if the insert was not successful.
    HtData* IR_ALWAYS_INLINE insert_internal(PartitionedHashTableCtx* ht_ctx, uint32_t hash);

    // Updates 'bucket_idx' to the index of the next non-empty bucket. If the bucket has
    // duplicates, 'node' will be pointing to the head of the linked list of duplicates.
    // Otherwise, 'node' should not be used. If there are no more buckets, sets
    // 'bucket_idx' to BUCKET_NOT_FOUND.
    void next_filled_bucket(int64_t* bucket_idx, DuplicateNode** node);

    // Resize the hash table to 'num_buckets'. Returns false on OOM.
    bool resize_buckets(int64_t num_buckets, PartitionedHashTableCtx* ht_ctx);

    // Appends the DuplicateNode pointed by _next_node to 'bucket' and moves the _next_node
    // pointer to the next DuplicateNode in the page, updating the remaining node counter.
    DuplicateNode* IR_ALWAYS_INLINE append_next_node(Bucket* bucket);

    // Creates a new DuplicateNode for a entry and chains it to the bucket with index
    // 'bucket_idx'. The duplicate nodes of a bucket are chained as a linked list.
    // This places the new duplicate node at the beginning of the list. If this is the
    // first duplicate entry inserted in this bucket, then the entry already contained by
    // the bucket is converted to a DuplicateNode. That is, the contents of 'data' of the
    // bucket are copied to a DuplicateNode and 'data' is updated to pointing to a
    // DuplicateNode.
    // Returns NULL if the node array could not grow, i.e. there was not enough memory to
    // allocate a new DuplicateNode.
    DuplicateNode* IR_ALWAYS_INLINE insert_duplicate_node(int64_t bucket_idx);

    // Resets the contents of the empty bucket with index 'bucket_idx', in preparation for
    // an insert. Sets all the fields of the bucket other than 'data'.
    void IR_ALWAYS_INLINE prepare_bucket_for_insert(int64_t bucket_idx, uint32_t hash);

    // Return the TupleRow pointed by 'htdata'.
    TupleRow* get_row(HtData& htdata, TupleRow* row) const;

    // Returns the TupleRow of the pointed 'bucket'. In case of duplicates, it
    // returns the content of the first chained duplicate node of the bucket.
    TupleRow* get_row(Bucket* bucket, TupleRow* row) const;

    // Grow the node array. Returns false on OOM.
    bool grow_node_array();

    // Load factor that will trigger growing the hash table on insert.  This is
    // defined as the number of non-empty buckets / total_buckets
    static const double MAX_FILL_FACTOR;

    RuntimeState* _state;

    // Client to allocate data pages with.
    BufferedBlockMgr2::Client* _block_mgr_client;

    // Stream contains the rows referenced by the hash table. Can be NULL if the
    // row only contains a single tuple, in which case the TupleRow indirection
    // is removed by the hash table.
    BufferedTupleStream2* _tuple_stream;

    // Constants on how the hash table should behave. Joins and aggs have slightly
    // different behavior.
    // TODO: these constants are an ideal candidate to be removed with codegen.
    // TODO: ..or with template-ization
    const bool _stores_tuples;

    // Quadratic probing enabled (as opposed to linear).
    const bool _quadratic_probing;

    // Data pages for all nodes. These are always pinned.
    std::vector<BufferedBlockMgr2::Block*> _data_pages;

    // Byte size of all buffers in _data_pages.
    int64_t _total_data_page_size;

    // Next duplicate node to insert. Vaild when _node_remaining_current_page > 0.
    DuplicateNode* _next_node;

    // Number of nodes left in the current page.
    int _node_remaining_current_page;

    // Number of duplicate nodes.
    int64_t _num_duplicate_nodes;

    const int64_t _max_num_buckets;

    // Array of all buckets. Owned by this node. Using c-style array to control
    // control memory footprint.
    Bucket* _buckets;

    // Total number of buckets (filled and empty).
    int64_t _num_buckets;

    // Number of non-empty buckets.  Used to determine when to resize.
    int64_t _num_filled_buckets;

    // Number of (non-empty) buckets with duplicates. These buckets do not point to slots
    // in the tuple stream, rather than to a linked list of Nodes.
    int64_t _num_buckets_with_duplicates;

    // Number of build tuples, used for constructing temp row* for probes.
    // TODO: We should remove it.
    const int _num_build_tuples;

    // Flag used to disable spilling hash tables that already had matches in case of
    // right joins (IMPALA-1488).
    // TODO: Not fail when spilling hash tables with matches in right joins
    bool _has_matches;

    // The stats below can be used for debugging perf.
    // TODO: Should we make these statistics atomic?
    // Number of find(), insert(), or find_bucket() calls that probe the hash table.
    int64_t _num_probes;

    // Number of probes that failed and had to fall back to linear probing without cap.
    int64_t _num_failed_probes;

    // Total distance traveled for each probe. That is the sum of the diff between the end
    // position of a probe (find/insert) and its start position
    // (hash & (_num_buckets - 1)).
    int64_t _travel_length;

    // The number of cases where we had to compare buckets with the same hash value, but
    // the row equality failed.
    int64_t _num_hash_collisions;

    // How many times this table has resized so far.
    int64_t _num_resizes;
};

} // end namespace doris

#endif // DORIS_BE_SRC_EXEC_PARTITIONED_HASH_TABLE_H
