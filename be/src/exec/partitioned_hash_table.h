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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/partitioned-hash-table.h
// and modified by Doris

#ifndef DORIS_BE_SRC_EXEC_NEW_PARTITIONED_HASH_TABLE_H
#define DORIS_BE_SRC_EXEC_NEW_PARTITIONED_HASH_TABLE_H

#include <memory>
#include <vector>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "runtime/buffered_tuple_stream3.h"
#include "runtime/buffered_tuple_stream3.inline.h"
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/bufferpool/suballocator.h"
#include "runtime/tuple_row.h"
#include "util/bitmap.h"
#include "util/hash_util.hpp"

namespace doris {

class Expr;
class ExprContext;
class MemTracker;
class PartitionedHashTable;
class RowDescriptor;
class RuntimeState;
class Tuple;
class TupleRow;

/// Linear or quadratic probing hash table implementation tailored to the usage pattern
/// for partitioned hash aggregation and hash joins. The hash table stores TupleRows and
/// allows for different exprs for insertions and finds. This is the pattern we use for
/// joins and aggregation where the input/build tuple row descriptor is different from the
/// find/probe descriptor. The implementation is designed to allow codegen for some paths.
//
/// In addition to the hash table there is also an accompanying hash table context that is
/// used for insertions and probes. For example, the hash table context stores evaluated
/// expr results for the current row being processed when possible into a contiguous
/// memory buffer. This allows for efficient hash computation.
//
/// The hash table does not support removes. The hash table is not thread safe.
/// The table is optimized for the partition hash aggregation and hash joins and is not
/// intended to be a generic hash table implementation. The API loosely mimics the
/// std::hashset API.
//
/// The data (rows) are stored in a BufferedTupleStream3. The basic data structure of this
/// hash table is a vector of buckets. The buckets (indexed by the mod of the hash)
/// contain a pointer to either the slot in the tuple-stream or in case of duplicate
/// values, to the head of a linked list of nodes that in turn contain a pointer to
/// tuple-stream slots. When inserting an entry we start at the bucket at position
/// (hash % size) and search for either a bucket with the same hash or for an empty
/// bucket. If a bucket with the same hash is found, we then compare for row equality and
/// either insert a duplicate node if the equality is true, or continue the search if the
/// row equality is false. Similarly, when probing we start from the bucket at position
/// (hash % size) and search for an entry with the same hash or for an empty bucket.
/// In the former case, we then check for row equality and continue the search if the row
/// equality is false. In the latter case, the probe is not successful. When growing the
/// hash table, the number of buckets is doubled. We trigger a resize when the fill
/// factor is approx 75%. Due to the doubling nature of the buckets, we require that the
/// number of buckets is a power of 2. This allows us to perform a modulo of the hash
/// using a bitmask.
///
/// We choose to use linear or quadratic probing because they exhibit good (predictable)
/// cache behavior.
///
/// The first NUM_SMALL_BLOCKS of nodes_ are made of blocks less than the IO size (of 8MB)
/// to reduce the memory footprint of small queries.
///
/// TODO: Compare linear and quadratic probing and remove the loser.
/// TODO: We currently use 32-bit hashes. There is room in the bucket structure for at
/// least 48-bits. We should exploit this space.
/// TODO: Consider capping the probes with a threshold value. If an insert reaches
/// that threshold it is inserted to another linked list of overflow entries.
/// TODO: Smarter resizes, and perhaps avoid using powers of 2 as the hash table size.
/// TODO: this is not a fancy hash table in terms of memory access patterns
/// (cuckoo-hashing or something that spills to disk). We will likely want to invest
/// more time into this.
/// TODO: hash-join and aggregation have very different access patterns.  Joins insert all
/// the rows and then calls scan to find them.  Aggregation interleaves FindProbeRow() and
/// Inserts().  We may want to optimize joins more heavily for Inserts() (in particular
/// growing).
/// TODO: Batched interface for inserts and finds.
/// TODO: Do we need to check mem limit exceeded so often. Check once per batch?
/// TODO: as an optimization, compute variable-length data size for the agg node.

/// Control block for a hash table. This class contains the logic as well as the variables
/// needed by a thread to operate on a hash table.
class PartitionedHashTableCtx {
public:
    /// Create a hash table context with the specified parameters, invoke Init() to
    /// initialize the new hash table context and return it in 'ht_ctx'. Expression
    /// evaluators for the build and probe expressions will also be allocated.
    /// Please see the comments of HashTableCtx constructor and Init() for details
    /// of other parameters.
    static Status Create(ObjectPool* pool, RuntimeState* state,
                         const std::vector<Expr*>& build_exprs,
                         const std::vector<Expr*>& probe_exprs, bool stores_nulls,
                         const std::vector<bool>& finds_nulls, int32_t initial_seed, int max_levels,
                         int num_build_tuples, MemPool* mem_pool, MemPool* expr_results_pool,
                         const std::shared_ptr<MemTracker>& tracker, const RowDescriptor& row_desc,
                         const RowDescriptor& row_desc_probe,
                         std::unique_ptr<PartitionedHashTableCtx>* ht_ctx);

    /// Initialize the build and probe expression evaluators.
    Status Open(RuntimeState* state);

    /// Call to cleanup any resources allocated by the expression evaluators.
    void Close(RuntimeState* state);

    /// Free local allocations made by build and probe expression evaluators respectively.
    void FreeBuildLocalAllocations();
    void FreeProbeLocalAllocations();

    /// Free local allocations of both build and probe expression evaluators.
    void FreeLocalAllocations();

    void set_level(int level);

    int ALWAYS_INLINE level() const { return level_; }

    uint32_t ALWAYS_INLINE seed(int level) { return seeds_.at(level); }

    TupleRow* ALWAYS_INLINE scratch_row() const { return scratch_row_; }

    /// Returns the results of the expression at 'expr_idx' evaluated at the current row.
    /// This value is invalid if the expr evaluated to nullptr.
    /// TODO: this is an awkward abstraction but aggregation node can take advantage of
    /// it and save some expr evaluation calls.
    void* ALWAYS_INLINE ExprValue(int expr_idx) const {
        return expr_values_cache_.ExprValuePtr(expr_values_cache_.cur_expr_values(), expr_idx);
    }

    /// Returns if the expression at 'expr_idx' is evaluated to nullptr for the current row.
    bool ALWAYS_INLINE ExprValueNull(int expr_idx) const {
        return static_cast<bool>(*(expr_values_cache_.cur_expr_values_null() + expr_idx));
    }

    /// Evaluate and hash the build/probe row, saving the evaluation to the current row of
    /// the ExprValuesCache in this hash table context: the results are saved in
    /// 'cur_expr_values_', the nullness of expressions values in 'cur_expr_values_null_',
    /// and the hashed expression values in 'cur_expr_values_hash_'. Returns false if this
    /// row should be rejected  (doesn't need to be processed further) because it contains
    /// nullptr. These need to be inlined in the IR module so we can find and replace the
    /// calls to EvalBuildRow()/EvalProbeRow().
    bool EvalAndHashBuild(TupleRow* row);
    bool EvalAndHashProbe(TupleRow* row);

    /// Struct that returns the number of constants replaced by ReplaceConstants().
    struct HashTableReplacedConstants {
        int stores_nulls;
        int finds_some_nulls;
        int stores_tuples;
        int stores_duplicates;
        int quadratic_probing;
    };

    /// To enable prefetching, the hash table building and probing are pipelined by the
    /// exec nodes. A set of rows in a row batch will be evaluated and hashed first and
    /// the corresponding hash table buckets are prefetched before they are probed against
    /// the hash table. ExprValuesCache is a container for caching the results of
    /// expressions evaluations for the rows in a prefetch set to avoid re-evaluating the
    /// rows again during probing. Expressions evaluation can be very expensive.
    ///
    /// The expression evaluation results are cached in the following data structures:
    ///
    /// - 'expr_values_array_' is an array caching the results of the rows
    /// evaluated against either the build or probe expressions. 'cur_expr_values_'
    /// is a pointer into this array.
    /// - 'expr_values_null_array_' is an array caching the nullness of each evaluated
    /// expression in each row. 'cur_expr_values_null_' is a pointer into this array.
    /// - 'expr_values_hash_array_' is an array of cached hash values of the rows.
    /// 'cur_expr_values_hash_' is a pointer into this array.
    /// - 'null_bitmap_' is a bitmap which indicates rows evaluated to nullptr.
    ///
    /// ExprValuesCache provides an iterator like interface for performing a write pass
    /// followed by a read pass. We refrain from providing an interface for random accesses
    /// as there isn't a use case for it now and we want to avoid expensive multiplication
    /// as the buffer size of each row is not necessarily power of two:
    /// - Reset(), ResetForRead(): reset the iterators before writing / reading cached values.
    /// - NextRow(): moves the iterators to point to the next row of cached values.
    /// - AtEnd(): returns true if all cached rows have been read. Valid in read mode only.
    ///
    /// Various metadata information such as layout of results buffer is also stored in
    /// this class. Note that the result buffer doesn't store variable length data. It only
    /// contains pointers to the variable length data (e.g. if an expression value is a
    /// StringValue).
    ///
    class ExprValuesCache {
    public:
        ExprValuesCache();

        /// Allocates memory and initializes various data structures. Return error status
        /// if memory allocation leads to the memory limits of the exec node to be exceeded.
        /// 'tracker' is the memory tracker of the exec node which owns this PartitionedHashTableCtx.
        Status Init(RuntimeState* state, const std::shared_ptr<MemTracker>& tracker,
                    const std::vector<Expr*>& build_exprs);

        /// Frees up various resources and updates memory tracker with proper accounting.
        void Close();

        /// Resets the cache states (iterators, end pointers etc) before writing.
        void Reset() noexcept;

        /// Resets the iterators to the start before reading. Will record the current position
        /// of the iterators in end pointer before resetting so AtEnd() can determine if all
        /// cached values have been read.
        void ResetForRead();

        /// Advances the iterators to the next row by moving to the next entries in the
        /// arrays of cached values.
        void ALWAYS_INLINE NextRow();

        /// Compute the total memory usage of this ExprValuesCache.
        static int MemUsage(int capacity, int results_buffer_size, int num_build_exprs);

        /// Returns the maximum number rows of expression values states which can be cached.
        int ALWAYS_INLINE capacity() const { return capacity_; }

        /// Returns the total size in bytes of a row of evaluated expressions' values.
        int ALWAYS_INLINE expr_values_bytes_per_row() const { return expr_values_bytes_per_row_; }

        /// Returns the offset into the result buffer of the first variable length
        /// data results.
        int ALWAYS_INLINE var_result_offset() const { return var_result_offset_; }

        /// Returns true if the current read pass is complete, meaning all cached values
        /// have been read.
        bool ALWAYS_INLINE AtEnd() const {
            return cur_expr_values_hash_ == cur_expr_values_hash_end_;
        }

        /// Returns true if the current row is null but nulls are not considered in the current
        /// phase (build or probe).
        bool ALWAYS_INLINE IsRowNull() const { return null_bitmap_.Get(CurIdx()); }

        /// Record in a bitmap that the current row is null but nulls are not considered in
        /// the current phase (build or probe).
        void ALWAYS_INLINE SetRowNull() { null_bitmap_.Set(CurIdx(), true); }

        /// Returns the hash values of the current row.
        uint32_t ALWAYS_INLINE CurExprValuesHash() const { return *cur_expr_values_hash_; }

        /// Sets the hash values for the current row.
        void ALWAYS_INLINE SetCurExprValuesHash(uint32_t hash) { *cur_expr_values_hash_ = hash; }

        /// Returns a pointer to the expression value at 'expr_idx' in 'expr_values'.
        template <typename T>
        T ExprValuePtr(T expr_values, int expr_idx) const {
            return expr_values + expr_values_offsets_[expr_idx];
        };

        /// Returns the current row's expression buffer. The expression values in the buffer
        /// are accessed using ExprValuePtr().
        uint8_t* ALWAYS_INLINE cur_expr_values() const { return cur_expr_values_; }

        /// Returns null indicator bytes for the current row, one per expression. Non-zero
        /// bytes mean nullptr, zero bytes mean non-nullptr. Indexed by the expression index.
        /// These are uint8_t instead of bool to simplify codegen with IRBuilder.
        /// TODO: is there actually a valid reason why this is necessary for codegen?
        uint8_t* ALWAYS_INLINE cur_expr_values_null() const { return cur_expr_values_null_; }

        /// Returns the offset into the results buffer of the expression value at 'expr_idx'.
        int ALWAYS_INLINE expr_values_offsets(int expr_idx) const {
            return expr_values_offsets_[expr_idx];
        }

    private:
        friend class PartitionedHashTableCtx;

        /// Resets the iterators to the beginning of the cache values' arrays.
        void ResetIterators();

        /// Returns the offset in number of rows into the cached values' buffer.
        int ALWAYS_INLINE CurIdx() const {
            return cur_expr_values_hash_ - expr_values_hash_array_.get();
        }

        /// Max amount of memory in bytes for caching evaluated expression values.
        static const int MAX_EXPR_VALUES_ARRAY_SIZE = 256 << 10;

        /// Maximum number of rows of expressions evaluation states which this
        /// ExprValuesCache can cache.
        int capacity_;

        /// Byte size of a row of evaluated expression values. Never changes once set,
        /// can be used for constant substitution during codegen.
        int expr_values_bytes_per_row_;

        /// Number of build/probe expressions.
        int num_exprs_;

        /// Pointer into 'expr_values_array_' for the current row's expression values.
        uint8_t* cur_expr_values_;

        /// Pointer into 'expr_values_null_array_' for the current row's nullness of each
        /// expression value.
        uint8_t* cur_expr_values_null_;

        /// Pointer into 'expr_hash_value_array_' for the hash value of current row's
        /// expression values.
        uint32_t* cur_expr_values_hash_;

        /// Pointer to the buffer one beyond the end of the last entry of cached expressions'
        /// hash values.
        uint32_t* cur_expr_values_hash_end_;

        /// Array for caching up to 'capacity_' number of rows worth of evaluated expression
        /// values. Each row consumes 'expr_values_bytes_per_row_' number of bytes.
        std::unique_ptr<uint8_t[]> expr_values_array_;

        /// Array for caching up to 'capacity_' number of rows worth of null booleans.
        /// Each row contains 'num_exprs_' booleans to indicate nullness of expression values.
        /// Used when the hash table supports nullptr. Use 'uint8_t' to guarantee each entry is 1
        /// byte as sizeof(bool) is implementation dependent. The IR depends on this
        /// assumption.
        std::unique_ptr<uint8_t[]> expr_values_null_array_;

        /// Array for caching up to 'capacity_' number of rows worth of hashed values.
        std::unique_ptr<uint32_t[]> expr_values_hash_array_;

        /// One bit for each row. A bit is set if that row is not hashed as it's evaluated
        /// to nullptr but the hash table doesn't support nullptr. Such rows may still be included
        /// in outputs for certain join types (e.g. left anti joins).
        Bitmap null_bitmap_;

        /// Maps from expression index to the byte offset into a row of expression values.
        /// One entry per build/probe expression.
        std::vector<int> expr_values_offsets_;

        /// Byte offset into 'cur_expr_values_' that begins the variable length results for
        /// a row. If -1, there are no variable length slots. Never changes once set, can be
        /// constant substituted with codegen.
        int var_result_offset_;
    };

    ExprValuesCache* ALWAYS_INLINE expr_values_cache() { return &expr_values_cache_; }

private:
    friend class PartitionedAggregationNode;
    friend class PartitionedHashTable;
    friend class HashTableTest_HashEmpty_Test;

    /// Construct a hash table context.
    ///  - build_exprs are the exprs that should be used to evaluate rows during Insert().
    ///  - probe_exprs are used during FindProbeRow()
    ///  - stores_nulls: if false, TupleRows with nulls are ignored during Insert
    ///  - finds_nulls: if finds_nulls[i] is false, FindProbeRow() returns End() for
    ///        TupleRows with nulls in position i even if stores_nulls is true.
    ///  - initial_seed: initial seed value to use when computing hashes for rows with
    ///        level 0. Other levels have their seeds derived from this seed.
    ///  - max_levels: the max lhashevels we will hash with.
    ///  - mem_pool: the MemPool which the expression evaluators allocate from. Owned by the
    ///        exec node which owns this hash table context. Memory usage of the expression
    ///        value cache is charged against its MemTracker.
    ///
    /// TODO: stores_nulls is too coarse: for a hash table in which some columns are joined
    ///       with '<=>' and others with '=', stores_nulls could distinguish between columns
    ///       in which nulls are stored and columns in which they are not, which could save
    ///       space by not storing some rows we know will never match.
    PartitionedHashTableCtx(const std::vector<Expr*>& build_exprs,
                            const std::vector<Expr*>& probe_exprs, bool stores_nulls,
                            const std::vector<bool>& finds_nulls, int32_t initial_seed,
                            int max_levels, MemPool* mem_pool, MemPool* expr_results_pool,
                            const std::shared_ptr<MemTracker>& tracker);

    /// Allocate various buffers for storing expression evaluation results, hash values,
    /// null bits etc. Also allocate evaluators for the build and probe expressions and
    /// store them in 'pool'. Returns error if allocation causes query memory limit to
    /// be exceeded or the evaluators fail to initialize. 'num_build_tuples' is the number
    /// of tuples of a row in the build side, used for computing the size of a scratch row.
    Status Init(ObjectPool* pool, RuntimeState* state, int num_build_tuples,
                const RowDescriptor& row_desc, const RowDescriptor& row_desc_probe);

    /// Compute the hash of the values in 'expr_values' with nullness 'expr_values_null'.
    /// This will be replaced by codegen.  We don't want this inlined for replacing
    /// with codegen'd functions so the function name does not change.
    uint32_t HashRow(const uint8_t* expr_values, const uint8_t* expr_values_null) const noexcept;

    /// Wrapper function for calling correct HashUtil function in non-codegen'd case.
    uint32_t Hash(const void* input, int len, uint32_t hash) const;

    /// Evaluate 'row' over build exprs, storing values into 'expr_values' and nullness into
    /// 'expr_values_null'. This will be replaced by codegen. We do not want this function
    /// inlined when cross compiled because we need to be able to differentiate between
    /// EvalBuildRow and EvalProbeRow by name and the build/probe exprs are baked into the
    /// codegen'd function.
    bool EvalBuildRow(TupleRow* row, uint8_t* expr_values, uint8_t* expr_values_null) noexcept {
        return EvalRow(row, build_expr_evals_, expr_values, expr_values_null);
    }

    /// Evaluate 'row' over probe exprs, storing the values into 'expr_values' and nullness
    /// into 'expr_values_null'. This will be replaced by codegen.
    bool EvalProbeRow(TupleRow* row, uint8_t* expr_values, uint8_t* expr_values_null) noexcept {
        return EvalRow(row, probe_expr_evals_, expr_values, expr_values_null);
    }

    /// Compute the hash of the values in 'expr_values' with nullness 'expr_values_null'
    /// for a row with variable length fields (e.g. strings).
    uint32_t HashVariableLenRow(const uint8_t* expr_values, const uint8_t* expr_values_null) const;

    /// Evaluate the exprs over row, storing the values into 'expr_values' and nullness into
    /// 'expr_values_null'. Returns whether any expr evaluated to nullptr. This will be
    /// replaced by codegen.
    bool EvalRow(TupleRow* row, const std::vector<ExprContext*>& ctxs, uint8_t* expr_values,
                 uint8_t* expr_values_null) noexcept;

    /// Returns true if the values of build_exprs evaluated over 'build_row' equal the
    /// values in 'expr_values' with nullness 'expr_values_null'. FORCE_NULL_EQUALITY is
    /// true if all nulls should be treated as equal, regardless of the values of
    /// 'finds_nulls_'. This will be replaced by codegen.
    template <bool FORCE_NULL_EQUALITY>
    bool Equals(TupleRow* build_row, const uint8_t* expr_values,
                const uint8_t* expr_values_null) const noexcept;

    /// Helper function that calls Equals() with the current row. Always inlined so that
    /// it does not appear in cross-compiled IR.
    template <bool FORCE_NULL_EQUALITY>
    bool ALWAYS_INLINE Equals(TupleRow* build_row) const {
        return Equals<FORCE_NULL_EQUALITY>(build_row, expr_values_cache_.cur_expr_values(),
                                           expr_values_cache_.cur_expr_values_null());
    }

    /// Cross-compiled function to access member variables used in CodegenHashRow().
    uint32_t GetHashSeed() const;

    /// Functions to be replaced by codegen to specialize the hash table.
    bool stores_nulls() const { return stores_nulls_; }
    bool finds_some_nulls() const { return finds_some_nulls_; }

    std::shared_ptr<MemTracker> tracker_;

    const std::vector<Expr*>& build_exprs_;
    std::vector<ExprContext*> build_expr_evals_;

    const std::vector<Expr*>& probe_exprs_;
    std::vector<ExprContext*> probe_expr_evals_;

    /// Constants on how the hash table should behave. Joins and aggs have slightly
    /// different behavior.
    const bool stores_nulls_;
    const std::vector<bool> finds_nulls_;

    /// finds_some_nulls_ is just the logical OR of finds_nulls_.
    const bool finds_some_nulls_;

    /// The current level this context is working on. Each level needs to use a
    /// different seed.
    int level_;

    /// The seeds to use for hashing. Indexed by the level.
    std::vector<uint32_t> seeds_;

    /// The ExprValuesCache for caching expression evaluation results, null bytes and hash
    /// values for rows. Used to store results of batch evaluations of rows.
    ExprValuesCache expr_values_cache_;

    /// Scratch buffer to generate rows on the fly.
    TupleRow* scratch_row_;

    /// MemPool for 'build_expr_evals_' and 'probe_expr_evals_' to allocate expr-managed
    /// memory from. Not owned.
    MemPool* mem_pool_;

    // MemPool for allocations by made EvalRow to copy expr's StringVal result. Not owned
    MemPool* expr_results_pool_;
};

/// The hash table consists of a contiguous array of buckets that contain a pointer to the
/// data, the hash value and three flags: whether this bucket is filled, whether this
/// entry has been matched (used in right and full joins) and whether this entry has
/// duplicates. If there are duplicates, then the data is pointing to the head of a
/// linked list of duplicate nodes that point to the actual data. Note that the duplicate
/// nodes do not contain the hash value, because all the linked nodes have the same hash
/// value, the one in the bucket. The data is either a tuple stream index or a Tuple*.
/// This array of buckets is sparse, we are shooting for up to 3/4 fill factor (75%). The
/// data allocated by the hash table comes from the BufferPool.
class PartitionedHashTable {
private:
    /// Rows are represented as pointers into the BufferedTupleStream data with one
    /// of two formats, depending on the number of tuples in the row.
    union HtData {
        // For rows with multiple tuples per row, a pointer to the flattened TupleRow.
        BufferedTupleStream3::FlatRowPtr flat_row;
        Tuple* tuple;
    };

    /// Linked list of entries used for duplicates.
    struct DuplicateNode {
        /// Used for full outer and right {outer, anti, semi} joins. Indicates whether the
        /// row in the DuplicateNode has been matched.
        /// From an abstraction point of view, this is an awkward place to store this
        /// information.
        /// TODO: Fold this flag in the next pointer below.
        bool matched;

        /// Chain to next duplicate node, nullptr when end of list.
        DuplicateNode* next;
        HtData htdata;
    };

    struct Bucket {
        /// Whether this bucket contains a valid entry, or it is empty.
        bool filled;

        /// Used for full outer and right {outer, anti, semi} joins. Indicates whether the
        /// row in the bucket has been matched.
        /// From an abstraction point of view, this is an awkward place to store this
        /// information but it is efficient. This space is otherwise unused.
        bool matched;

        /// Used in case of duplicates. If true, then the bucketData union should be used as
        /// 'duplicates'.
        bool hasDuplicates;

        /// Cache of the hash for data.
        /// TODO: Do we even have to cache the hash value?
        uint32_t hash;

        /// Either the data for this bucket or the linked list of duplicates.
        union {
            HtData htdata;
            DuplicateNode* duplicates;
        } bucketData;
    };

public:
    class Iterator;

    /// Returns a newly allocated HashTable. The probing algorithm is set by the
    /// FLAG_enable_quadratic_probing.
    ///  - allocator: allocator to allocate bucket directory and data pages from.
    ///  - stores_duplicates: true if rows with duplicate keys may be inserted into the
    ///    hash table.
    ///  - num_build_tuples: number of Tuples in the build tuple row.
    ///  - tuple_stream: the tuple stream which contains the tuple rows index by the
    ///    hash table. Can be nullptr if the rows contain only a single tuple, in which
    ///    case the 'tuple_stream' is unused.
    ///  - max_num_buckets: the maximum number of buckets that can be stored. If we
    ///    try to grow the number of buckets to a larger number, the inserts will fail.
    ///    -1, if it unlimited.
    ///  - initial_num_buckets: number of buckets that the hash table should be initialized
    ///    with.
    static PartitionedHashTable* Create(Suballocator* allocator, bool stores_duplicates,
                                        int num_build_tuples, BufferedTupleStream3* tuple_stream,
                                        int64_t max_num_buckets, int64_t initial_num_buckets);

    /// Allocates the initial bucket structure. Returns a non-OK status if an error is
    /// encountered. If an OK status is returned , 'got_memory' is set to indicate whether
    /// enough memory for the initial buckets was allocated from the Suballocator.
    Status Init(bool* got_memory);

    /// Call to cleanup any resources. Must be called once.
    void Close();

    /// Inserts the row to the hash table. The caller is responsible for ensuring that the
    /// table has free buckets. Returns true if the insertion was successful. Always
    /// returns true if the table has free buckets and the key is not a duplicate. If the
    /// key was a duplicate and memory could not be allocated for the new duplicate node,
    /// returns false. If an error is encountered while creating a duplicate node, returns
    /// false and sets 'status' to the error.
    ///
    /// 'flat_row' is a pointer to the flattened row in 'tuple_stream_' If the row contains
    /// only one tuple, a pointer to that tuple is stored. Otherwise the 'flat_row' pointer
    /// is stored. The 'row' is not copied by the hash table and the caller must guarantee
    /// it stays in memory. This will not grow the hash table.
    bool Insert(PartitionedHashTableCtx* ht_ctx, BufferedTupleStream3::FlatRowPtr flat_row,
                TupleRow* row, Status* status);

    /// Prefetch the hash table bucket which the given hash value 'hash' maps to.
    template <const bool READ>
    void PrefetchBucket(uint32_t hash);

    /// Returns an iterator to the bucket that matches the probe expression results that
    /// are cached at the current position of the ExprValuesCache in 'ht_ctx'. Assumes that
    /// the ExprValuesCache was filled using EvalAndHashProbe(). Returns HashTable::End()
    /// if no match is found. The iterator can be iterated until HashTable::End() to find
    /// all the matching rows. Advancing the returned iterator will go to the next matching
    /// row. The matching rows do not need to be evaluated since all the nodes of a bucket
    /// are duplicates. One scan can be in progress for each 'ht_ctx'. Used in the probe
    /// phase of hash joins.
    Iterator FindProbeRow(PartitionedHashTableCtx* ht_ctx);

    /// If a match is found in the table, return an iterator as in FindProbeRow(). If a
    /// match was not present, return an iterator pointing to the empty bucket where the key
    /// should be inserted. Returns End() if the table is full. The caller can set the data
    /// in the bucket using a Set*() method on the iterator.
    Iterator FindBuildRowBucket(PartitionedHashTableCtx* ht_ctx, bool* found);

    /// Returns number of elements inserted in the hash table
    int64_t size() const {
        return num_filled_buckets_ - num_buckets_with_duplicates_ + num_duplicate_nodes_;
    }

    /// Returns the number of empty buckets.
    int64_t EmptyBuckets() const { return num_buckets_ - num_filled_buckets_; }

    /// Returns the number of buckets
    int64_t num_buckets() const { return num_buckets_; }

    /// Returns the number of filled buckets
    int64_t num_filled_buckets() const { return num_filled_buckets_; }

    /// Returns the time of hash table resize
    int64_t num_resize() const { return num_resizes_; }

    /// Returns the number of bucket_with_duplicates
    int64_t num_buckets_with_duplicates() const { return num_buckets_with_duplicates_; }

    /// Returns the number of bucket_with_duplicates
    int64_t num_duplicates_nodes() const { return num_duplicate_nodes_; }

    /// Returns the number of probe operations
    int64_t num_probe() const { return num_probes_; }

    /// Returns the number of failed probe operations
    int64_t num_failed_probe() const { return num_failed_probes_; }

    /// Returns the number of travel_length of probe operations
    int64_t travel_length() const { return travel_length_; }

    /// Returns the load factor (the number of non-empty buckets)
    double load_factor() const { return static_cast<double>(num_filled_buckets_) / num_buckets_; }

    /// Return an estimate of the number of bytes needed to build the hash table
    /// structure for 'num_rows'. To do that, it estimates the number of buckets,
    /// rounded up to a power of two, and also assumes that there are no duplicates.
    static int64_t EstimateNumBuckets(int64_t num_rows) {
        /// Assume max 66% fill factor and no duplicates.
        return BitUtil::next_power_of_two(3 * num_rows / 2);
    }
    static int64_t EstimateSize(int64_t num_rows) {
        int64_t num_buckets = EstimateNumBuckets(num_rows);
        return num_buckets * sizeof(Bucket);
    }

    /// Return the size of a hash table bucket in bytes.
    static int64_t BucketSize() { return sizeof(Bucket); }

    /// Returns the memory occupied by the hash table, takes into account the number of
    /// duplicates.
    int64_t CurrentMemSize() const;

    /// Returns the number of inserts that can be performed before resizing the table.
    int64_t NumInsertsBeforeResize() const;

    /// Calculates the fill factor if 'buckets_to_fill' additional buckets were to be
    /// filled and resizes the hash table so that the projected fill factor is below the
    /// max fill factor.
    /// If 'got_memory' is true, then it is guaranteed at least 'rows_to_add' rows can be
    /// inserted without need to resize. If there is not enough memory available to
    /// resize the hash table, Status::OK()() is returned and 'got_memory' is false. If a
    /// another error occurs, an error status may be returned.
    Status CheckAndResize(uint64_t buckets_to_fill, const PartitionedHashTableCtx* ht_ctx,
                          bool* got_memory);

    /// Returns the number of bytes allocated to the hash table from the block manager.
    int64_t ByteSize() const { return num_buckets_ * sizeof(Bucket) + total_data_page_size_; }

    /// Returns an iterator at the beginning of the hash table.  Advancing this iterator
    /// will traverse all elements.
    Iterator Begin(const PartitionedHashTableCtx* ht_ctx);

    /// Return an iterator pointing to the first element (Bucket or DuplicateNode, if the
    /// bucket has duplicates) in the hash table that does not have its matched flag set.
    /// Used in right joins and full-outer joins.
    Iterator FirstUnmatched(PartitionedHashTableCtx* ctx);

    /// Return true if there was a least one match.
    bool HasMatches() const { return has_matches_; }

    /// Return end marker.
    Iterator End() { return Iterator(); }

    /// Dump out the entire hash table to string.  If 'skip_empty', empty buckets are
    /// skipped.  If 'show_match', it also prints the matched flag of each node. If
    /// 'build_desc' is non-null, the build rows will be printed. Otherwise, only the
    /// the addresses of the build rows will be printed.
    std::string DebugString(bool skip_empty, bool show_match, const RowDescriptor* build_desc);

    /// Print the content of a bucket or node.
    void DebugStringTuple(std::stringstream& ss, HtData& htdata, const RowDescriptor* desc);

    /// Update and print some statistics that can be used for performance debugging.
    std::string PrintStats() const;

    /// Number of hash collisions so far in the lifetime of this object
    int64_t NumHashCollisions() const { return num_hash_collisions_; }

    /// stl-like iterator interface.
    class Iterator {
    private:
        /// Bucket index value when probe is not successful.
        static const int64_t BUCKET_NOT_FOUND = -1;

    public:
        Iterator()
                : table_(nullptr),
                  scratch_row_(nullptr),
                  bucket_idx_(BUCKET_NOT_FOUND),
                  node_(nullptr) {}

        /// Iterates to the next element. It should be called only if !AtEnd().
        void Next();

        /// Iterates to the next duplicate node. If the bucket does not have duplicates or
        /// when it reaches the last duplicate node, then it moves the Iterator to AtEnd().
        /// Used when we want to iterate over all the duplicate nodes bypassing the Next()
        /// interface (e.g. in semi/outer joins without other_join_conjuncts, in order to
        /// iterate over all nodes of an unmatched bucket).
        void NextDuplicate();

        /// Iterates to the next element that does not have its matched flag set. Used in
        /// right-outer and full-outer joins.
        void NextUnmatched();

        /// Return the current row or tuple. Callers must check the iterator is not AtEnd()
        /// before calling them.  The returned row is owned by the iterator and valid until
        /// the next call to GetRow(). It is safe to advance the iterator.
        TupleRow* GetRow() const;
        Tuple* GetTuple() const;

        /// Set the current tuple for an empty bucket. Designed to be used with the iterator
        /// returned from FindBuildRowBucket() in the case when the value is not found.  It is
        /// not valid to call this function if the bucket already has an entry.
        void SetTuple(Tuple* tuple, uint32_t hash);

        /// Sets as matched the Bucket or DuplicateNode currently pointed by the iterator,
        /// depending on whether the bucket has duplicates or not. The iterator cannot be
        /// AtEnd().
        void SetMatched();

        /// Returns the 'matched' flag of the current Bucket or DuplicateNode, depending on
        /// whether the bucket has duplicates or not. It should be called only if !AtEnd().
        bool IsMatched() const;

        /// Resets everything but the pointer to the hash table.
        void SetAtEnd();

        /// Returns true if this iterator is at the end, i.e. GetRow() cannot be called.
        bool ALWAYS_INLINE AtEnd() const { return bucket_idx_ == BUCKET_NOT_FOUND; }

        /// Prefetch the hash table bucket which the iterator is pointing to now.
        template <const bool READ>
        void PrefetchBucket();

    private:
        friend class PartitionedHashTable;

        ALWAYS_INLINE
        Iterator(PartitionedHashTable* table, TupleRow* row, int bucket_idx, DuplicateNode* node)
                : table_(table), scratch_row_(row), bucket_idx_(bucket_idx), node_(node) {}

        PartitionedHashTable* table_;

        /// Scratch buffer to hold generated rows. Not owned.
        TupleRow* scratch_row_;

        /// Current bucket idx.
        int64_t bucket_idx_;

        /// Pointer to the current duplicate node.
        DuplicateNode* node_;
    };

private:
    friend class Iterator;
    friend class HashTableTest;

    /// Hash table constructor. Private because Create() should be used, instead
    /// of calling this constructor directly.
    ///  - quadratic_probing: set to true when the probing algorithm is quadratic, as
    ///    opposed to linear.
    PartitionedHashTable(bool quadratic_probing, Suballocator* allocator, bool stores_duplicates,
                         int num_build_tuples, BufferedTupleStream3* tuple_stream,
                         int64_t max_num_buckets, int64_t initial_num_buckets);

    /// Performs the probing operation according to the probing algorithm (linear or
    /// quadratic. Returns one of the following:
    /// (a) the index of the bucket that contains the entry that matches with the last row
    ///     evaluated in 'ht_ctx'. If 'ht_ctx' is nullptr then it does not check for row
    ///     equality and returns the index of the first empty bucket.
    /// (b) the index of the first empty bucket according to the probing algorithm (linear
    ///     or quadratic), if the entry is not in the hash table or 'ht_ctx' is nullptr.
    /// (c) Iterator::BUCKET_NOT_FOUND if the probe was not successful, i.e. the maximum
    ///     distance was traveled without finding either an empty or a matching bucket.
    /// Using the returned index value, the caller can create an iterator that can be
    /// iterated until End() to find all the matching rows.
    ///
    /// EvalAndHashBuild() or EvalAndHashProbe() must have been called before calling
    /// this function. The values of the expression values cache in 'ht_ctx' will be
    /// used to probe the hash table.
    ///
    /// 'FORCE_NULL_EQUALITY' is true if NULLs should always be considered equal when
    /// comparing two rows.
    ///
    /// 'hash' is the hash computed by EvalAndHashBuild() or EvalAndHashProbe().
    /// 'found' indicates that a bucket that contains an equal row is found.
    ///
    /// There are wrappers of this function that perform the Find and Insert logic.
    template <bool FORCE_NULL_EQUALITY>
    int64_t Probe(Bucket* buckets, int64_t num_buckets, PartitionedHashTableCtx* ht_ctx,
                  uint32_t hash, bool* found);

    /// Performs the insert logic. Returns the HtData* of the bucket or duplicate node
    /// where the data should be inserted. Returns nullptr if the insert was not successful
    /// and either sets 'status' to OK if it failed because not enough reservation was
    /// available or the error if an error was encountered.
    HtData* InsertInternal(PartitionedHashTableCtx* ht_ctx, Status* status);

    /// Updates 'bucket_idx' to the index of the next non-empty bucket. If the bucket has
    /// duplicates, 'node' will be pointing to the head of the linked list of duplicates.
    /// Otherwise, 'node' should not be used. If there are no more buckets, sets
    /// 'bucket_idx' to BUCKET_NOT_FOUND.
    void NextFilledBucket(int64_t* bucket_idx, DuplicateNode** node);

    /// Resize the hash table to 'num_buckets'. 'got_memory' is false on OOM.
    Status ResizeBuckets(int64_t num_buckets, const PartitionedHashTableCtx* ht_ctx,
                         bool* got_memory);

    /// Appends the DuplicateNode pointed by next_node_ to 'bucket' and moves the next_node_
    /// pointer to the next DuplicateNode in the page, updating the remaining node counter.
    DuplicateNode* AppendNextNode(Bucket* bucket);

    /// Creates a new DuplicateNode for a entry and chains it to the bucket with index
    /// 'bucket_idx'. The duplicate nodes of a bucket are chained as a linked list.
    /// This places the new duplicate node at the beginning of the list. If this is the
    /// first duplicate entry inserted in this bucket, then the entry already contained by
    /// the bucket is converted to a DuplicateNode. That is, the contents of 'data' of the
    /// bucket are copied to a DuplicateNode and 'data' is updated to pointing to a
    /// DuplicateNode.
    /// Returns nullptr and sets 'status' to OK if the node array could not grow, i.e. there
    /// was not enough memory to allocate a new DuplicateNode. Returns nullptr and sets
    /// 'status' to an error if another error was encountered.
    DuplicateNode* InsertDuplicateNode(int64_t bucket_idx, Status* status);

    /// Resets the contents of the empty bucket with index 'bucket_idx', in preparation for
    /// an insert. Sets all the fields of the bucket other than 'data'.
    void PrepareBucketForInsert(int64_t bucket_idx, uint32_t hash);

    /// Return the TupleRow pointed by 'htdata'.
    TupleRow* GetRow(HtData& htdata, TupleRow* row) const;

    /// Returns the TupleRow of the pointed 'bucket'. In case of duplicates, it
    /// returns the content of the first chained duplicate node of the bucket.
    TupleRow* GetRow(Bucket* bucket, TupleRow* row) const;

    /// Grow the node array. Returns true and sets 'status' to OK on success. Returns false
    /// and set 'status' to OK if we can't get sufficient reservation to allocate the next
    /// data page. Returns false and sets 'status' if another error is encountered.
    bool GrowNodeArray(Status* status);

    /// Functions to be replaced by codegen to specialize the hash table.
    bool stores_tuples() const { return stores_tuples_; }
    bool stores_duplicates() const { return stores_duplicates_; }
    bool quadratic_probing() const { return quadratic_probing_; }

    /// Load factor that will trigger growing the hash table on insert.  This is
    /// defined as the number of non-empty buckets / total_buckets
    static constexpr double MAX_FILL_FACTOR = 0.75;

    /// The size in bytes of each page of duplicate nodes. Should be large enough to fit
    /// enough DuplicateNodes to amortise the overhead of allocating each page and low
    /// enough to not waste excessive memory to internal fragmentation.
    static constexpr int64_t DATA_PAGE_SIZE = 64L * 1024;

    RuntimeState* state_;

    /// Suballocator to allocate data pages and hash table buckets with.
    Suballocator* allocator_;

    /// Stream contains the rows referenced by the hash table. Can be nullptr if the
    /// row only contains a single tuple, in which case the TupleRow indirection
    /// is removed by the hash table.
    BufferedTupleStream3* tuple_stream_;

    /// Constants on how the hash table should behave.

    /// True if the HtData uses the Tuple* representation, or false if it uses FlatRowPtr.
    const bool stores_tuples_;

    /// True if duplicates may be inserted into hash table.
    const bool stores_duplicates_;

    /// Quadratic probing enabled (as opposed to linear).
    const bool quadratic_probing_;

    /// Data pages for all nodes. Allocated from suballocator to reduce memory
    /// consumption of small tables.
    std::vector<std::unique_ptr<Suballocation>> data_pages_;

    /// Byte size of all buffers in data_pages_.
    int64_t total_data_page_size_;

    /// Next duplicate node to insert. Valid when node_remaining_current_page_ > 0.
    DuplicateNode* next_node_;

    /// Number of nodes left in the current page.
    int node_remaining_current_page_;

    /// Number of duplicate nodes.
    int64_t num_duplicate_nodes_;

    const int64_t max_num_buckets_;

    /// Allocation containing all buckets.
    std::unique_ptr<Suballocation> bucket_allocation_;

    /// Pointer to the 'buckets_' array from 'bucket_allocation_'.
    Bucket* buckets_;

    /// Total number of buckets (filled and empty).
    int64_t num_buckets_;

    /// Number of non-empty buckets.  Used to determine when to resize.
    int64_t num_filled_buckets_;

    /// Number of (non-empty) buckets with duplicates. These buckets do not point to slots
    /// in the tuple stream, rather than to a linked list of Nodes.
    int64_t num_buckets_with_duplicates_;

    /// Number of build tuples, used for constructing temp row* for probes.
    const int num_build_tuples_;

    /// Flag used to check that we don't lose stored matches when spilling hash tables
    /// (IMPALA-1488).
    bool has_matches_;

    /// The stats below can be used for debugging perf.
    /// TODO: Should we make these statistics atomic?
    /// Number of FindProbeRow(), Insert(), or FindBuildRowBucket() calls that probe the
    /// hash table.
    int64_t num_probes_;

    /// Number of probes that failed and had to fall back to linear probing without cap.
    int64_t num_failed_probes_;

    /// Total distance traveled for each probe. That is the sum of the diff between the end
    /// position of a probe (find/insert) and its start position
    /// (hash & (num_buckets_ - 1)).
    int64_t travel_length_;

    /// The number of cases where we had to compare buckets with the same hash value, but
    /// the row equality failed.
    int64_t num_hash_collisions_;

    /// How many times this table has resized so far.
    int64_t num_resizes_;
};

} // namespace doris

#endif
