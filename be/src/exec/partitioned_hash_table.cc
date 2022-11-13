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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/partitioned-hash-table.cc
// and modified by Doris

#include <gutil/strings/substitute.h>

#include <functional>
#include <numeric>

#include "exec/exec_node.h"
#include "exec/partitioned_hash_table.inline.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

using namespace doris;
using namespace strings;

// DEFINE_bool(enable_quadratic_probing, true, "Enable quadratic probing hash table");

// Random primes to multiply the seed with.
static uint32_t SEED_PRIMES[] = {
        1, // First seed must be 1, level 0 is used by other operators in the fragment.
        1431655781, 1183186591, 622729787, 472882027, 338294347, 275604541, 41161739, 29999999,
        27475109,   611603,     16313357,  11380003,  21261403,  33393119,  101,      71043403};

// Put a non-zero constant in the result location for nullptr.
// We don't want(nullptr, 1) to hash to the same as (0, 1).
// This needs to be as big as the biggest primitive type since the bytes
// get copied directly.
// TODO find a better approach, since primitives like CHAR(N) can be up
// to 255 bytes
static int64_t NULL_VALUE[] = {
        HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
        HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
        HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
        HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
        HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
        HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
        HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED,
        HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED};

PartitionedHashTableCtx::PartitionedHashTableCtx(const std::vector<Expr*>& build_exprs,
                                                 const std::vector<Expr*>& probe_exprs,
                                                 bool stores_nulls,
                                                 const std::vector<bool>& finds_nulls,
                                                 int32_t initial_seed, int max_levels,
                                                 MemPool* mem_pool, MemPool* expr_results_pool)
        : build_exprs_(build_exprs),
          probe_exprs_(probe_exprs),
          stores_nulls_(stores_nulls),
          finds_nulls_(finds_nulls),
          finds_some_nulls_(std::accumulate(finds_nulls_.begin(), finds_nulls_.end(), false,
                                            std::logical_or<bool>())),
          level_(0),
          scratch_row_(nullptr),
          mem_pool_(mem_pool),
          expr_results_pool_(expr_results_pool) {
    DCHECK(!finds_some_nulls_ || stores_nulls_);
    // Compute the layout and buffer size to store the evaluated expr results
    DCHECK_EQ(build_exprs_.size(), probe_exprs_.size());
    DCHECK_EQ(build_exprs_.size(), finds_nulls_.size());
    DCHECK(!build_exprs_.empty());

    // Populate the seeds to use for all the levels. TODO: revisit how we generate these.
    DCHECK_GE(max_levels, 0);
    DCHECK_LT(max_levels, sizeof(SEED_PRIMES) / sizeof(SEED_PRIMES[0]));
    DCHECK_NE(initial_seed, 0);
    seeds_.resize(max_levels + 1);
    seeds_[0] = initial_seed;
    for (int i = 1; i <= max_levels; ++i) {
        seeds_[i] = seeds_[i - 1] * SEED_PRIMES[i];
    }
}

Status PartitionedHashTableCtx::Init(ObjectPool* pool, RuntimeState* state, int num_build_tuples,
                                     const RowDescriptor& row_desc,
                                     const RowDescriptor& row_desc_probe) {
    int scratch_row_size = sizeof(Tuple*) * num_build_tuples;
    scratch_row_ = reinterpret_cast<TupleRow*>(malloc(scratch_row_size));
    if (UNLIKELY(scratch_row_ == nullptr)) {
        return Status::InternalError(
                "Failed to allocate {} bytes for scratch row of "
                "PartitionedHashTableCtx.",
                scratch_row_size);
    }

    // TODO chenhao replace ExprContext with ScalarFnEvaluator
    for (int i = 0; i < build_exprs_.size(); i++) {
        ExprContext* context = pool->add(new ExprContext(build_exprs_[i]));
        RETURN_IF_ERROR(context->prepare(state, row_desc));
        build_expr_evals_.push_back(context);
    }
    DCHECK_EQ(build_exprs_.size(), build_expr_evals_.size());

    for (int i = 0; i < probe_exprs_.size(); i++) {
        ExprContext* context = pool->add(new ExprContext(probe_exprs_[i]));
        RETURN_IF_ERROR(context->prepare(state, row_desc_probe));
        probe_expr_evals_.push_back(context);
    }
    DCHECK_EQ(probe_exprs_.size(), probe_expr_evals_.size());
    return expr_values_cache_.Init(state, build_exprs_);
}

Status PartitionedHashTableCtx::Create(ObjectPool* pool, RuntimeState* state,
                                       const std::vector<Expr*>& build_exprs,
                                       const std::vector<Expr*>& probe_exprs, bool stores_nulls,
                                       const std::vector<bool>& finds_nulls, int32_t initial_seed,
                                       int max_levels, int num_build_tuples, MemPool* mem_pool,
                                       MemPool* expr_results_pool, const RowDescriptor& row_desc,
                                       const RowDescriptor& row_desc_probe,
                                       std::unique_ptr<PartitionedHashTableCtx>* ht_ctx) {
    ht_ctx->reset(new PartitionedHashTableCtx(build_exprs, probe_exprs, stores_nulls, finds_nulls,
                                              initial_seed, max_levels, mem_pool,
                                              expr_results_pool));
    return (*ht_ctx)->Init(pool, state, num_build_tuples, row_desc, row_desc_probe);
}

Status PartitionedHashTableCtx::Open(RuntimeState* state) {
    // TODO chenhao replace ExprContext with ScalarFnEvaluator
    for (int i = 0; i < build_expr_evals_.size(); i++) {
        RETURN_IF_ERROR(build_expr_evals_[i]->open(state));
    }
    for (int i = 0; i < probe_expr_evals_.size(); i++) {
        RETURN_IF_ERROR(probe_expr_evals_[i]->open(state));
    }
    return Status::OK();
}

void PartitionedHashTableCtx::Close(RuntimeState* state) {
    free(scratch_row_);
    scratch_row_ = nullptr;
    expr_values_cache_.Close();
    for (int i = 0; i < build_expr_evals_.size(); i++) {
        build_expr_evals_[i]->close(state);
    }

    for (int i = 0; i < probe_expr_evals_.size(); i++) {
        probe_expr_evals_[i]->close(state);
    }

    // TODO chenhao release new expr in Init, remove this after merging
    // ScalarFnEvaluator.
    build_expr_evals_.clear();
    probe_expr_evals_.clear();
}

void PartitionedHashTableCtx::FreeBuildLocalAllocations() {
    //ExprContext::FreeLocalAllocations(build_expr_evals_);
}

void PartitionedHashTableCtx::FreeProbeLocalAllocations() {
    //ExprContext::FreeLocalAllocations(probe_expr_evals_);
}

void PartitionedHashTableCtx::FreeLocalAllocations() {
    FreeBuildLocalAllocations();
    FreeProbeLocalAllocations();
}

uint32_t PartitionedHashTableCtx::Hash(const void* input, int len, uint32_t hash) const {
    /// Use CRC hash at first level for better performance. Switch to murmur hash at
    /// subsequent levels since CRC doesn't randomize well with different seed inputs.
    if (level_ == 0) return HashUtil::hash(input, len, hash);
    return HashUtil::murmur_hash2_64(input, len, hash);
}

uint32_t PartitionedHashTableCtx::HashRow(const uint8_t* expr_values,
                                          const uint8_t* expr_values_null) const noexcept {
    DCHECK_LT(level_, seeds_.size());
    if (expr_values_cache_.var_result_offset() == -1) {
        /// This handles NULLs implicitly since a constant seed value was put
        /// into results buffer for nulls.
        return Hash(expr_values, expr_values_cache_.expr_values_bytes_per_row(), seeds_[level_]);
    } else {
        return PartitionedHashTableCtx::HashVariableLenRow(expr_values, expr_values_null);
    }
}

bool PartitionedHashTableCtx::EvalRow(TupleRow* row, const vector<ExprContext*>& ctxs,
                                      uint8_t* expr_values, uint8_t* expr_values_null) noexcept {
    bool has_null = false;
    for (int i = 0; i < ctxs.size(); ++i) {
        void* loc = expr_values_cache_.ExprValuePtr(expr_values, i);
        void* val = ctxs[i]->get_value(row);
        if (val == nullptr) {
            // If the table doesn't store nulls, no reason to keep evaluating
            if (!stores_nulls_) return true;
            expr_values_null[i] = true;
            val = reinterpret_cast<void*>(&NULL_VALUE);
            has_null = true;
            DCHECK_LE(build_exprs_[i]->type().get_slot_size(), sizeof(NULL_VALUE));
            RawValue::write(val, loc, build_exprs_[i]->type(), nullptr);
        } else {
            expr_values_null[i] = false;
            DCHECK_LE(build_exprs_[i]->type().get_slot_size(), sizeof(NULL_VALUE));
            RawValue::write(val, loc, build_exprs_[i]->type(), expr_results_pool_);
        }
    }
    return has_null;
}

uint32_t PartitionedHashTableCtx::HashVariableLenRow(const uint8_t* expr_values,
                                                     const uint8_t* expr_values_null) const {
    uint32_t hash = seeds_[level_];
    int var_result_offset = expr_values_cache_.var_result_offset();
    // Hash the non-var length portions (if there are any)
    if (var_result_offset != 0) {
        hash = Hash(expr_values, var_result_offset, hash);
    }

    for (int i = 0; i < build_exprs_.size(); ++i) {
        // non-string and null slots are already part of 'expr_values'.
        // if (build_expr_ctxs_[i]->root()->type().type != TYPE_STRING
        PrimitiveType type = build_exprs_[i]->type().type;
        if (type != TYPE_CHAR && type != TYPE_VARCHAR && type != TYPE_STRING) {
            continue;
        }

        const void* loc = expr_values_cache_.ExprValuePtr(expr_values, i);
        if (expr_values_null[i]) {
            // Hash the null random seed values at 'loc'
            hash = Hash(loc, sizeof(StringValue), hash);
        } else {
            // Hash the string
            // TODO: when using CRC hash on empty string, this only swaps bytes.
            const StringValue* str = reinterpret_cast<const StringValue*>(loc);
            hash = Hash(str->ptr, str->len, hash);
        }
    }
    return hash;
}

template <bool FORCE_NULL_EQUALITY>
bool PartitionedHashTableCtx::Equals(TupleRow* build_row, const uint8_t* expr_values,
                                     const uint8_t* expr_values_null) const noexcept {
    for (int i = 0; i < build_expr_evals_.size(); ++i) {
        void* val = build_expr_evals_[i]->get_value(build_row);
        if (val == nullptr) {
            if (!(FORCE_NULL_EQUALITY || finds_nulls_[i])) return false;
            if (!expr_values_null[i]) return false;
            continue;
        } else {
            if (expr_values_null[i]) return false;
        }

        const void* loc = expr_values_cache_.ExprValuePtr(expr_values, i);
        if (!RawValue::eq(loc, val, build_exprs_[i]->type())) {
            return false;
        }
    }
    return true;
}

template bool PartitionedHashTableCtx::Equals<true>(TupleRow* build_row, const uint8_t* expr_values,
                                                    const uint8_t* expr_values_null) const;
template bool PartitionedHashTableCtx::Equals<false>(TupleRow* build_row,
                                                     const uint8_t* expr_values,
                                                     const uint8_t* expr_values_null) const;

PartitionedHashTableCtx::ExprValuesCache::ExprValuesCache()
        : capacity_(0),
          cur_expr_values_(nullptr),
          cur_expr_values_null_(nullptr),
          cur_expr_values_hash_(nullptr),
          cur_expr_values_hash_end_(nullptr),
          expr_values_array_(nullptr),
          expr_values_null_array_(nullptr),
          expr_values_hash_array_(nullptr),
          null_bitmap_(0) {}

Status PartitionedHashTableCtx::ExprValuesCache::Init(RuntimeState* state,
                                                      const std::vector<Expr*>& build_exprs) {
    // Initialize the number of expressions.
    num_exprs_ = build_exprs.size();
    // Compute the layout of evaluated values of a row.
    expr_values_bytes_per_row_ =
            Expr::compute_results_layout(build_exprs, &expr_values_offsets_, &var_result_offset_);
    if (expr_values_bytes_per_row_ == 0) {
        DCHECK_EQ(num_exprs_, 0);
        return Status::OK();
    }
    DCHECK_GT(expr_values_bytes_per_row_, 0);
    // Compute the maximum number of cached rows which can fit in the memory budget.
    // TODO: Find the optimal prefetch batch size. This may be something
    // processor dependent so we may need calibration at Impala startup time.
    capacity_ = std::max(1, std::min(state->batch_size(),
                                     MAX_EXPR_VALUES_ARRAY_SIZE / expr_values_bytes_per_row_));

    int mem_usage = MemUsage(capacity_, expr_values_bytes_per_row_, num_exprs_);
    if (UNLIKELY(!thread_context()->thread_mem_tracker()->check_limit(mem_usage))) {
        capacity_ = 0;
        string details = Substitute(
                "PartitionedHashTableCtx::ExprValuesCache failed to allocate $0 bytes", mem_usage);
        RETURN_LIMIT_EXCEEDED(state, details, mem_usage);
    }

    int expr_values_size = expr_values_bytes_per_row_ * capacity_;
    expr_values_array_.reset(new uint8_t[expr_values_size]);
    cur_expr_values_ = expr_values_array_.get();
    memset(cur_expr_values_, 0, expr_values_size);

    int expr_values_null_size = num_exprs_ * capacity_;
    expr_values_null_array_.reset(new uint8_t[expr_values_null_size]);
    cur_expr_values_null_ = expr_values_null_array_.get();
    memset(cur_expr_values_null_, 0, expr_values_null_size);

    expr_values_hash_array_.reset(new uint32_t[capacity_]);
    cur_expr_values_hash_ = expr_values_hash_array_.get();
    cur_expr_values_hash_end_ = cur_expr_values_hash_;
    memset(cur_expr_values_hash_, 0, sizeof(uint32) * capacity_);

    null_bitmap_.Reset(capacity_);
    return Status::OK();
}

void PartitionedHashTableCtx::ExprValuesCache::Close() {
    if (capacity_ == 0) return;
    cur_expr_values_ = nullptr;
    cur_expr_values_null_ = nullptr;
    cur_expr_values_hash_ = nullptr;
    cur_expr_values_hash_end_ = nullptr;
    expr_values_array_.reset();
    expr_values_null_array_.reset();
    expr_values_hash_array_.reset();
    null_bitmap_.Reset(0);
}

int PartitionedHashTableCtx::ExprValuesCache::MemUsage(int capacity, int expr_values_bytes_per_row,
                                                       int num_exprs) {
    return expr_values_bytes_per_row * capacity + // expr_values_array_
           num_exprs * capacity +                 // expr_values_null_array_
           sizeof(uint32) * capacity +            // expr_values_hash_array_
           Bitmap::MemUsage(capacity);            // null_bitmap_
}

void PartitionedHashTableCtx::ExprValuesCache::ResetIterators() {
    cur_expr_values_ = expr_values_array_.get();
    cur_expr_values_null_ = expr_values_null_array_.get();
    cur_expr_values_hash_ = expr_values_hash_array_.get();
}

void PartitionedHashTableCtx::ExprValuesCache::Reset() noexcept {
    ResetIterators();
    // Set the end pointer after resetting the other pointers so they point to
    // the same location.
    cur_expr_values_hash_end_ = cur_expr_values_hash_;
    null_bitmap_.SetAllBits(false);
}

void PartitionedHashTableCtx::ExprValuesCache::ResetForRead() {
    // Record the end of hash values iterator to be used in AtEnd().
    // Do it before resetting the pointers.
    cur_expr_values_hash_end_ = cur_expr_values_hash_;
    ResetIterators();
}

constexpr double PartitionedHashTable::MAX_FILL_FACTOR;
constexpr int64_t PartitionedHashTable::DATA_PAGE_SIZE;

PartitionedHashTable* PartitionedHashTable::Create(Suballocator* allocator, bool stores_duplicates,
                                                   int num_build_tuples,
                                                   BufferedTupleStream3* tuple_stream,
                                                   int64_t max_num_buckets,
                                                   int64_t initial_num_buckets) {
    return new PartitionedHashTable(config::enable_quadratic_probing, allocator, stores_duplicates,
                                    num_build_tuples, tuple_stream, max_num_buckets,
                                    initial_num_buckets);
}

PartitionedHashTable::PartitionedHashTable(bool quadratic_probing, Suballocator* allocator,
                                           bool stores_duplicates, int num_build_tuples,
                                           BufferedTupleStream3* stream, int64_t max_num_buckets,
                                           int64_t num_buckets)
        : allocator_(allocator),
          tuple_stream_(stream),
          stores_tuples_(num_build_tuples == 1),
          stores_duplicates_(stores_duplicates),
          quadratic_probing_(quadratic_probing),
          total_data_page_size_(0),
          next_node_(nullptr),
          node_remaining_current_page_(0),
          num_duplicate_nodes_(0),
          max_num_buckets_(max_num_buckets),
          buckets_(nullptr),
          num_buckets_(num_buckets),
          num_filled_buckets_(0),
          num_buckets_with_duplicates_(0),
          num_build_tuples_(num_build_tuples),
          has_matches_(false),
          num_probes_(0),
          num_failed_probes_(0),
          travel_length_(0),
          num_hash_collisions_(0),
          num_resizes_(0) {
    DCHECK_EQ((num_buckets & (num_buckets - 1)), 0) << "num_buckets must be a power of 2";
    DCHECK_GT(num_buckets, 0) << "num_buckets must be larger than 0";
    DCHECK(stores_tuples_ || stream != nullptr);
}

Status PartitionedHashTable::Init(bool* got_memory) {
    int64_t buckets_byte_size = num_buckets_ * sizeof(Bucket);
    RETURN_IF_ERROR(allocator_->Allocate(buckets_byte_size, &bucket_allocation_));
    if (bucket_allocation_ == nullptr) {
        num_buckets_ = 0;
        *got_memory = false;
        return Status::OK();
    }
    buckets_ = reinterpret_cast<Bucket*>(bucket_allocation_->data());
    memset(buckets_, 0, buckets_byte_size);
    *got_memory = true;
    return Status::OK();
}

void PartitionedHashTable::Close() {
    // Print statistics only for the large or heavily used hash tables.
    // TODO: Tweak these numbers/conditions, or print them always?
    const int64_t LARGE_HT = 128 * 1024;
    const int64_t HEAVILY_USED = 1024 * 1024;
    // TODO: These statistics should go to the runtime profile as well.
    if ((num_buckets_ > LARGE_HT) || (num_probes_ > HEAVILY_USED)) VLOG_CRITICAL << PrintStats();
    for (auto& data_page : data_pages_) allocator_->Free(std::move(data_page));
    data_pages_.clear();
    if (bucket_allocation_ != nullptr) allocator_->Free(std::move(bucket_allocation_));
}

Status PartitionedHashTable::CheckAndResize(uint64_t buckets_to_fill,
                                            const PartitionedHashTableCtx* ht_ctx,
                                            bool* got_memory) {
    uint64_t shift = 0;
    while (num_filled_buckets_ + buckets_to_fill > (num_buckets_ << shift) * MAX_FILL_FACTOR) {
        ++shift;
    }
    if (shift > 0) return ResizeBuckets(num_buckets_ << shift, ht_ctx, got_memory);
    *got_memory = true;
    return Status::OK();
}

Status PartitionedHashTable::ResizeBuckets(int64_t num_buckets,
                                           const PartitionedHashTableCtx* ht_ctx,
                                           bool* got_memory) {
    DCHECK_EQ((num_buckets & (num_buckets - 1)), 0)
            << "num_buckets=" << num_buckets << " must be a power of 2";
    DCHECK_GT(num_buckets, num_filled_buckets_)
            << "Cannot shrink the hash table to smaller number of buckets than the number of "
            << "filled buckets.";
    VLOG_CRITICAL << "Resizing hash table from " << num_buckets_ << " to " << num_buckets
                  << " buckets.";
    if (max_num_buckets_ != -1 && num_buckets > max_num_buckets_) {
        *got_memory = false;
        return Status::OK();
    }
    ++num_resizes_;

    // All memory that can grow proportional to the input should come from the block mgrs
    // mem tracker.
    // Note that while we copying over the contents of the old hash table, we need to have
    // allocated both the old and the new hash table. Once we finish, we return the memory
    // of the old hash table.
    // int64_t old_size = num_buckets_ * sizeof(Bucket);
    int64_t new_size = num_buckets * sizeof(Bucket);

    std::unique_ptr<Suballocation> new_allocation;
    RETURN_IF_ERROR(allocator_->Allocate(new_size, &new_allocation));
    if (new_allocation == nullptr) {
        *got_memory = false;
        return Status::OK();
    }
    Bucket* new_buckets = reinterpret_cast<Bucket*>(new_allocation->data());
    memset(new_buckets, 0, new_size);

    // Walk the old table and copy all the filled buckets to the new (resized) table.
    // We do not have to do anything with the duplicate nodes. This operation is expected
    // to succeed.
    for (PartitionedHashTable::Iterator iter = Begin(ht_ctx); !iter.AtEnd();
         NextFilledBucket(&iter.bucket_idx_, &iter.node_)) {
        Bucket* bucket_to_copy = &buckets_[iter.bucket_idx_];
        bool found = false;
        int64_t bucket_idx =
                Probe<true>(new_buckets, num_buckets, nullptr, bucket_to_copy->hash, &found);
        DCHECK(!found);
        DCHECK_NE(bucket_idx, Iterator::BUCKET_NOT_FOUND)
                << " Probe failed even though "
                   " there are free buckets. "
                << num_buckets << " " << num_filled_buckets_;
        Bucket* dst_bucket = &new_buckets[bucket_idx];
        *dst_bucket = *bucket_to_copy;
    }

    num_buckets_ = num_buckets;
    allocator_->Free(std::move(bucket_allocation_));
    bucket_allocation_ = std::move(new_allocation);
    buckets_ = reinterpret_cast<Bucket*>(bucket_allocation_->data());
    *got_memory = true;
    return Status::OK();
}

bool PartitionedHashTable::GrowNodeArray(Status* status) {
    std::unique_ptr<Suballocation> allocation;
    *status = allocator_->Allocate(DATA_PAGE_SIZE, &allocation);
    if (!status->ok() || allocation == nullptr) return false;
    next_node_ = reinterpret_cast<DuplicateNode*>(allocation->data());
    data_pages_.push_back(std::move(allocation));
    node_remaining_current_page_ = DATA_PAGE_SIZE / sizeof(DuplicateNode);
    total_data_page_size_ += DATA_PAGE_SIZE;
    return true;
}

void PartitionedHashTable::DebugStringTuple(std::stringstream& ss, HtData& htdata,
                                            const RowDescriptor* desc) {
    if (stores_tuples_) {
        ss << "(" << htdata.tuple << ")";
    } else {
        ss << "(" << htdata.flat_row << ")";
    }
    if (desc != nullptr) {
        Tuple* row[num_build_tuples_];
        ss << " " << GetRow(htdata, reinterpret_cast<TupleRow*>(row))->to_string(*desc);
    }
}

string PartitionedHashTable::DebugString(bool skip_empty, bool show_match,
                                         const RowDescriptor* desc) {
    std::stringstream ss;
    ss << std::endl;
    for (int i = 0; i < num_buckets_; ++i) {
        if (skip_empty && !buckets_[i].filled) continue;
        ss << i << ": ";
        if (show_match) {
            if (buckets_[i].matched) {
                ss << " [M]";
            } else {
                ss << " [U]";
            }
        }
        if (buckets_[i].hasDuplicates) {
            DuplicateNode* node = buckets_[i].bucketData.duplicates;
            bool first = true;
            ss << " [D] ";
            while (node != nullptr) {
                if (!first) ss << ",";
                DebugStringTuple(ss, node->htdata, desc);
                node = node->next;
                first = false;
            }
        } else {
            ss << " [B] ";
            if (buckets_[i].filled) {
                DebugStringTuple(ss, buckets_[i].bucketData.htdata, desc);
            } else {
                ss << " - ";
            }
        }
        ss << std::endl;
    }
    return ss.str();
}

string PartitionedHashTable::PrintStats() const {
    double curr_fill_factor = (double)num_filled_buckets_ / (double)num_buckets_;
    double avg_travel = (double)travel_length_ / (double)num_probes_;
    double avg_collisions = (double)num_hash_collisions_ / (double)num_filled_buckets_;
    std::stringstream ss;
    ss << "Buckets: " << num_buckets_ << " " << num_filled_buckets_ << " " << curr_fill_factor
       << std::endl;
    ss << "Duplicates: " << num_buckets_with_duplicates_ << " buckets " << num_duplicate_nodes_
       << " nodes" << std::endl;
    ss << "Probes: " << num_probes_ << std::endl;
    ss << "FailedProbes: " << num_failed_probes_ << std::endl;
    ss << "Travel: " << travel_length_ << " " << avg_travel << std::endl;
    ss << "HashCollisions: " << num_hash_collisions_ << " " << avg_collisions << std::endl;
    ss << "Resizes: " << num_resizes_ << std::endl;
    return ss.str();
}
