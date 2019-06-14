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

#include "exec/new_partitioned_hash_table.inline.h"

#include <functional>
#include <numeric>
#include <gutil/strings/substitute.h>

#include "codegen/codegen_anyval.h"
#include "codegen/llvm_codegen.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/slot_ref.h"
#include "runtime/bufferpool/reservation_tracker.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "util/doris_metrics.h"

#include "common/names.h"

using namespace doris;
// using namespace llvm;
using namespace strings;

// DEFINE_bool(enable_quadratic_probing, true, "Enable quadratic probing hash table");

const char* NewPartitionedHashTableCtx::LLVM_CLASS_NAME = "class.doris::NewPartitionedHashTableCtx";

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
  HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED, HashUtil::FNV_SEED
};

NewPartitionedHashTableCtx::NewPartitionedHashTableCtx(const std::vector<Expr*>& build_exprs,
    const std::vector<Expr*>& probe_exprs, bool stores_nulls,
    const std::vector<bool>& finds_nulls, int32_t initial_seed,
    int max_levels, MemPool* mem_pool, MemPool* expr_results_pool)
    : build_exprs_(build_exprs),
      probe_exprs_(probe_exprs),
      stores_nulls_(stores_nulls),
      finds_nulls_(finds_nulls),
      finds_some_nulls_(std::accumulate(
          finds_nulls_.begin(), finds_nulls_.end(), false, std::logical_or<bool>())),
      level_(0),
      scratch_row_(NULL),
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

Status NewPartitionedHashTableCtx::Init(ObjectPool* pool, RuntimeState* state, int num_build_tuples,
        MemTracker* tracker, const RowDescriptor& row_desc, const RowDescriptor& row_desc_probe) {

  int scratch_row_size = sizeof(Tuple*) * num_build_tuples;
  scratch_row_ = reinterpret_cast<TupleRow*>(malloc(scratch_row_size));
  if (UNLIKELY(scratch_row_ == NULL)) {
    return Status::InternalError(Substitute("Failed to allocate $0 bytes for scratch row of "
        "NewPartitionedHashTableCtx.", scratch_row_size));
  }

  // TODO chenhao replace ExprContext with ScalarFnEvaluator
  for (int i = 0; i < build_exprs_.size(); i++) {
      ExprContext* context = pool->add(new ExprContext(build_exprs_[i]));
      context->prepare(state, row_desc, tracker); 
      if (context == nullptr) {
          return Status::InternalError("Hashtable init error.");
      }
      build_expr_evals_.push_back(context);
  }
  DCHECK_EQ(build_exprs_.size(), build_expr_evals_.size());
  
  for (int i = 0; i < probe_exprs_.size(); i++) {
      ExprContext* context = pool->add(new ExprContext(probe_exprs_[i]));
      context->prepare(state, row_desc_probe, tracker);
      if (context == nullptr) {
          return Status::InternalError("Hashtable init error.");
      }
      probe_expr_evals_.push_back(context);
  }
  DCHECK_EQ(probe_exprs_.size(), probe_expr_evals_.size());
  return expr_values_cache_.Init(state, mem_pool_->mem_tracker(), build_exprs_);
}

Status NewPartitionedHashTableCtx::Create(ObjectPool* pool, RuntimeState* state,
    const std::vector<Expr*>& build_exprs,
    const std::vector<Expr*>& probe_exprs, bool stores_nulls,
    const std::vector<bool>& finds_nulls, int32_t initial_seed, int max_levels,
    int num_build_tuples, MemPool* mem_pool, MemPool* expr_results_pool, 
    MemTracker* tracker, const RowDescriptor& row_desc,
    const RowDescriptor& row_desc_probe,
    scoped_ptr<NewPartitionedHashTableCtx>* ht_ctx) {
  ht_ctx->reset(new NewPartitionedHashTableCtx(build_exprs, probe_exprs, stores_nulls,
      finds_nulls, initial_seed, max_levels, mem_pool, expr_results_pool));
  return (*ht_ctx)->Init(pool, state, num_build_tuples, tracker, row_desc, row_desc_probe);
}

Status NewPartitionedHashTableCtx::Open(RuntimeState* state) {
    // TODO chenhao replace ExprContext with ScalarFnEvaluator
    for (int i = 0; i < build_expr_evals_.size(); i++) {
        RETURN_IF_ERROR(build_expr_evals_[i]->open(state));
    }
    for (int i = 0; i < probe_expr_evals_.size(); i++) {
        RETURN_IF_ERROR(probe_expr_evals_[i]->open(state));
    }
    return Status::OK();
}

void NewPartitionedHashTableCtx::Close(RuntimeState* state) {
  free(scratch_row_);
  scratch_row_ = NULL;
  expr_values_cache_.Close(mem_pool_->mem_tracker());
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

void NewPartitionedHashTableCtx::FreeBuildLocalAllocations() {
  //ExprContext::FreeLocalAllocations(build_expr_evals_);
}

void NewPartitionedHashTableCtx::FreeProbeLocalAllocations() {
  //ExprContext::FreeLocalAllocations(probe_expr_evals_);
}

void NewPartitionedHashTableCtx::FreeLocalAllocations() {
  FreeBuildLocalAllocations();
  FreeProbeLocalAllocations();
}

uint32_t NewPartitionedHashTableCtx::Hash(const void* input, int len, uint32_t hash) const {
  /// Use CRC hash at first level for better performance. Switch to murmur hash at
  /// subsequent levels since CRC doesn't randomize well with different seed inputs.
  if (level_ == 0) return HashUtil::hash(input, len, hash);
  return HashUtil::murmur_hash2_64(input, len, hash);
}

uint32_t NewPartitionedHashTableCtx::HashRow(
    const uint8_t* expr_values, const uint8_t* expr_values_null) const noexcept {
  DCHECK_LT(level_, seeds_.size());
  if (expr_values_cache_.var_result_offset() == -1) {
    /// This handles NULLs implicitly since a constant seed value was put
    /// into results buffer for nulls.
    return Hash(
        expr_values, expr_values_cache_.expr_values_bytes_per_row(), seeds_[level_]);
  } else {
    return NewPartitionedHashTableCtx::HashVariableLenRow(expr_values, expr_values_null);
  }
}

bool NewPartitionedHashTableCtx::EvalRow(TupleRow* row, const vector<ExprContext*>& ctxs,
    uint8_t* expr_values, uint8_t* expr_values_null) noexcept {
  bool has_null = false;
  for (int i = 0; i < ctxs.size(); ++i) {
    void* loc = expr_values_cache_.ExprValuePtr(expr_values, i);
    void* val = ctxs[i]->get_value(row);
    if (val == NULL) {
      // If the table doesn't store nulls, no reason to keep evaluating
      if (!stores_nulls_) return true;
      expr_values_null[i] = true;
      val = reinterpret_cast<void*>(&NULL_VALUE);
      has_null = true;
      DCHECK_LE(build_exprs_[i]->type().get_slot_size(),
          sizeof(NULL_VALUE));
      RawValue::write(val, loc, build_exprs_[i]->type(), NULL);
    } else {
      expr_values_null[i] = false;
      DCHECK_LE(build_exprs_[i]->type().get_slot_size(),
          sizeof(NULL_VALUE));
      RawValue::write(val, loc, build_exprs_[i]->type(), expr_results_pool_);
    }
  }
  return has_null;
}

uint32_t NewPartitionedHashTableCtx::HashVariableLenRow(const uint8_t* expr_values,
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
    if (type != TYPE_CHAR && type != TYPE_VARCHAR) {
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
bool NewPartitionedHashTableCtx::Equals(TupleRow* build_row, const uint8_t* expr_values,
    const uint8_t* expr_values_null) const noexcept {
  for (int i = 0; i < build_expr_evals_.size(); ++i) {
    void* val = build_expr_evals_[i]->get_value(build_row);
    if (val == NULL) {
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

template bool NewPartitionedHashTableCtx::Equals<true>(TupleRow* build_row,
    const uint8_t* expr_values, const uint8_t* expr_values_null) const;
template bool NewPartitionedHashTableCtx::Equals<false>(TupleRow* build_row,
    const uint8_t* expr_values, const uint8_t* expr_values_null) const;

NewPartitionedHashTableCtx::ExprValuesCache::ExprValuesCache()
  : capacity_(0),
    cur_expr_values_(NULL),
    cur_expr_values_null_(NULL),
    cur_expr_values_hash_(NULL),
    cur_expr_values_hash_end_(NULL),
    expr_values_array_(NULL),
    expr_values_null_array_(NULL),
    expr_values_hash_array_(NULL),
    null_bitmap_(0) {}

Status NewPartitionedHashTableCtx::ExprValuesCache::Init(RuntimeState* state,
    MemTracker* tracker, const std::vector<Expr*>& build_exprs) {
  // Initialize the number of expressions.
  num_exprs_ = build_exprs.size();
  // Compute the layout of evaluated values of a row.
  expr_values_bytes_per_row_ = Expr::compute_results_layout(build_exprs,
      &expr_values_offsets_, &var_result_offset_);
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
  if (UNLIKELY(!tracker->try_consume(mem_usage))) {
    capacity_ = 0;
    string details = Substitute("NewPartitionedHashTableCtx::ExprValuesCache failed to allocate $0 bytes.",
        mem_usage);
    return tracker->MemLimitExceeded(state, details, mem_usage);
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

void NewPartitionedHashTableCtx::ExprValuesCache::Close(MemTracker* tracker) {
  if (capacity_ == 0) return;
  cur_expr_values_ = NULL;
  cur_expr_values_null_ = NULL;
  cur_expr_values_hash_ = NULL;
  cur_expr_values_hash_end_ = NULL;
  expr_values_array_.reset();
  expr_values_null_array_.reset();
  expr_values_hash_array_.reset();
  null_bitmap_.Reset(0);
  int mem_usage = MemUsage(capacity_, expr_values_bytes_per_row_, num_exprs_);
  tracker->release(mem_usage);
}

int NewPartitionedHashTableCtx::ExprValuesCache::MemUsage(int capacity,
    int expr_values_bytes_per_row, int num_exprs) {
  return expr_values_bytes_per_row * capacity + // expr_values_array_
      num_exprs * capacity +                    // expr_values_null_array_
      sizeof(uint32) * capacity +               // expr_values_hash_array_
      Bitmap::MemUsage(capacity);               // null_bitmap_
}

uint8_t* NewPartitionedHashTableCtx::ExprValuesCache::ExprValuePtr(
    uint8_t* expr_values, int expr_idx) const {
  return expr_values + expr_values_offsets_[expr_idx];
}

const uint8_t* NewPartitionedHashTableCtx::ExprValuesCache::ExprValuePtr(
    const uint8_t* expr_values, int expr_idx) const {
  return expr_values + expr_values_offsets_[expr_idx];
}

void NewPartitionedHashTableCtx::ExprValuesCache::ResetIterators() {
  cur_expr_values_ = expr_values_array_.get();
  cur_expr_values_null_ = expr_values_null_array_.get();
  cur_expr_values_hash_ = expr_values_hash_array_.get();
}

void NewPartitionedHashTableCtx::ExprValuesCache::Reset() noexcept {
  ResetIterators();
  // Set the end pointer after resetting the other pointers so they point to
  // the same location.
  cur_expr_values_hash_end_ = cur_expr_values_hash_;
  null_bitmap_.SetAllBits(false);
}

void NewPartitionedHashTableCtx::ExprValuesCache::ResetForRead() {
  // Record the end of hash values iterator to be used in AtEnd().
  // Do it before resetting the pointers.
  cur_expr_values_hash_end_ = cur_expr_values_hash_;
  ResetIterators();
}

constexpr double NewPartitionedHashTable::MAX_FILL_FACTOR;
constexpr int64_t NewPartitionedHashTable::DATA_PAGE_SIZE;

NewPartitionedHashTable* NewPartitionedHashTable::Create(Suballocator* allocator, bool stores_duplicates,
    int num_build_tuples, BufferedTupleStream3* tuple_stream, int64_t max_num_buckets,
    int64_t initial_num_buckets) {
  return new NewPartitionedHashTable(config::enable_quadratic_probing, allocator, stores_duplicates,
      num_build_tuples, tuple_stream, max_num_buckets, initial_num_buckets);
}

NewPartitionedHashTable::NewPartitionedHashTable(bool quadratic_probing, Suballocator* allocator,
    bool stores_duplicates, int num_build_tuples, BufferedTupleStream3* stream,
    int64_t max_num_buckets, int64_t num_buckets)
  : allocator_(allocator),
    tuple_stream_(stream),
    stores_tuples_(num_build_tuples == 1),
    stores_duplicates_(stores_duplicates),
    quadratic_probing_(quadratic_probing),
    total_data_page_size_(0),
    next_node_(NULL),
    node_remaining_current_page_(0),
    num_duplicate_nodes_(0),
    max_num_buckets_(max_num_buckets),
    buckets_(NULL),
    num_buckets_(num_buckets),
    num_filled_buckets_(0),
    num_buckets_with_duplicates_(0),
    num_build_tuples_(num_build_tuples),
    has_matches_(false),
    num_probes_(0), num_failed_probes_(0), travel_length_(0), num_hash_collisions_(0),
    num_resizes_(0) {
  DCHECK_EQ((num_buckets & (num_buckets - 1)), 0) << "num_buckets must be a power of 2";
  DCHECK_GT(num_buckets, 0) << "num_buckets must be larger than 0";
  DCHECK(stores_tuples_ || stream != NULL);
}

Status NewPartitionedHashTable::Init(bool* got_memory) {
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

void NewPartitionedHashTable::Close() {
  // Print statistics only for the large or heavily used hash tables.
  // TODO: Tweak these numbers/conditions, or print them always?
  const int64_t LARGE_HT = 128 * 1024;
  const int64_t HEAVILY_USED = 1024 * 1024;
  // TODO: These statistics should go to the runtime profile as well.
  if ((num_buckets_ > LARGE_HT) || (num_probes_ > HEAVILY_USED)) VLOG(2) << PrintStats();
  for (auto& data_page : data_pages_) allocator_->Free(move(data_page));
  data_pages_.clear();
  //if (DorisMetrics::hash_table_total_bytes() != NULL) {
  //  DorisMetrics::hash_table_total_bytes()->increment(-total_data_page_size_);
  //}
  if (bucket_allocation_ != nullptr) allocator_->Free(move(bucket_allocation_));
}

Status NewPartitionedHashTable::CheckAndResize(
    uint64_t buckets_to_fill, const NewPartitionedHashTableCtx* ht_ctx, bool* got_memory) {
  uint64_t shift = 0;
  while (num_filled_buckets_ + buckets_to_fill >
         (num_buckets_ << shift) * MAX_FILL_FACTOR) {
    ++shift;
  }
  if (shift > 0) return ResizeBuckets(num_buckets_ << shift, ht_ctx, got_memory);
  *got_memory = true;
  return Status::OK();
}

Status NewPartitionedHashTable::ResizeBuckets(
    int64_t num_buckets, const NewPartitionedHashTableCtx* ht_ctx, bool* got_memory) {
  DCHECK_EQ((num_buckets & (num_buckets - 1)), 0)
      << "num_buckets=" << num_buckets << " must be a power of 2";
  DCHECK_GT(num_buckets, num_filled_buckets_)
    << "Cannot shrink the hash table to smaller number of buckets than the number of "
    << "filled buckets.";
  VLOG(2) << "Resizing hash table from " << num_buckets_ << " to " << num_buckets
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
  if (new_allocation == NULL) {
    *got_memory = false;
    return Status::OK();
  }
  Bucket* new_buckets = reinterpret_cast<Bucket*>(new_allocation->data());
  memset(new_buckets, 0, new_size);

  // Walk the old table and copy all the filled buckets to the new (resized) table.
  // We do not have to do anything with the duplicate nodes. This operation is expected
  // to succeed.
  for (NewPartitionedHashTable::Iterator iter = Begin(ht_ctx); !iter.AtEnd();
       NextFilledBucket(&iter.bucket_idx_, &iter.node_)) {
    Bucket* bucket_to_copy = &buckets_[iter.bucket_idx_];
    bool found = false;
    int64_t bucket_idx =
        Probe<true>(new_buckets, num_buckets, NULL, bucket_to_copy->hash, &found);
    DCHECK(!found);
    DCHECK_NE(bucket_idx, Iterator::BUCKET_NOT_FOUND) << " Probe failed even though "
        " there are free buckets. " << num_buckets << " " << num_filled_buckets_;
    Bucket* dst_bucket = &new_buckets[bucket_idx];
    *dst_bucket = *bucket_to_copy;
  }

  num_buckets_ = num_buckets;
  allocator_->Free(move(bucket_allocation_));
  bucket_allocation_ = std::move(new_allocation);
  buckets_ = reinterpret_cast<Bucket*>(bucket_allocation_->data());
  *got_memory = true;
  return Status::OK();
}

bool NewPartitionedHashTable::GrowNodeArray(Status* status) {
  std::unique_ptr<Suballocation> allocation;
  *status = allocator_->Allocate(DATA_PAGE_SIZE, &allocation);
  if (!status->ok() || allocation == nullptr) return false;
  next_node_ = reinterpret_cast<DuplicateNode*>(allocation->data());
  data_pages_.push_back(std::move(allocation));
  //DorisMetrics::hash_table_total_bytes()->increment(DATA_PAGE_SIZE);
  node_remaining_current_page_ = DATA_PAGE_SIZE / sizeof(DuplicateNode);
  total_data_page_size_ += DATA_PAGE_SIZE;
  return true;
}

void NewPartitionedHashTable::DebugStringTuple(std::stringstream& ss, HtData& htdata,
    const RowDescriptor* desc) {
  if (stores_tuples_) {
    ss << "(" << htdata.tuple << ")";
  } else {
    ss << "(" << htdata.flat_row << ")";
  }
  if (desc != NULL) {
    Tuple* row[num_build_tuples_];
    ss << " " << GetRow(htdata, reinterpret_cast<TupleRow*>(row))->to_string(*desc);
  }
}

string NewPartitionedHashTable::DebugString(bool skip_empty, bool show_match,
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
      while (node != NULL) {
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

string NewPartitionedHashTable::PrintStats() const {
  double curr_fill_factor = (double)num_filled_buckets_/(double)num_buckets_;
  double avg_travel = (double)travel_length_/(double)num_probes_;
  double avg_collisions = (double)num_hash_collisions_/(double)num_filled_buckets_;
  std::stringstream ss;
  ss << "Buckets: " << num_buckets_ << " " << num_filled_buckets_ << " "
     << curr_fill_factor << std::endl;
  ss << "Duplicates: " << num_buckets_with_duplicates_ << " buckets "
     << num_duplicate_nodes_ << " nodes" << std::endl;
  ss << "Probes: " << num_probes_ << std::endl;
  ss << "FailedProbes: " << num_failed_probes_ << std::endl;
  ss << "Travel: " << travel_length_ << " " << avg_travel << std::endl;
  ss << "HashCollisions: " << num_hash_collisions_ << " " << avg_collisions << std::endl;
  ss << "Resizes: " << num_resizes_ << std::endl;
  return ss.str();
}

#if 0

// Helper function to store a value into the results buffer if the expr
// evaluated to NULL.  We don't want (NULL, 1) to hash to the same as (0,1) so
// we'll pick a more random value.
static void CodegenAssignNullValue(
    LlvmCodeGen* codegen, LlvmBuilder* builder, Value* dst, const ColumnType& type) {
  uint64_t fnv_seed = HashUtil::FNV_SEED;

  if (type.type == TYPE_STRING || type.type == TYPE_VARCHAR) {
    Value* dst_ptr = builder->CreateStructGEP(NULL, dst, 0, "string_ptr");
    Value* dst_len = builder->CreateStructGEP(NULL, dst, 1, "string_len");
    Value* null_len = codegen->GetIntConstant(TYPE_INT, fnv_seed);
    Value* null_ptr = builder->CreateIntToPtr(null_len, codegen->ptr_type());
    builder->CreateStore(null_ptr, dst_ptr);
    builder->CreateStore(null_len, dst_len);
  } else {
    Value* null_value = NULL;
    int byte_size = type.GetByteSize();
    // Get a type specific representation of fnv_seed
    switch (type.type) {
      case TYPE_BOOLEAN:
        // In results, booleans are stored as 1 byte
        dst = builder->CreateBitCast(dst, codegen->ptr_type());
        null_value = codegen->GetIntConstant(TYPE_TINYINT, fnv_seed);
        break;
      case TYPE_TIMESTAMP: {
        // Cast 'dst' to 'i128*'
        DCHECK_EQ(byte_size, 16);
        PointerType* fnv_seed_ptr_type =
            codegen->GetPtrType(Type::getIntNTy(codegen->context(), byte_size * 8));
        dst = builder->CreateBitCast(dst, fnv_seed_ptr_type);
        null_value = codegen->GetIntConstant(byte_size, fnv_seed, fnv_seed);
        break;
      }
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
      case TYPE_DECIMAL:
        null_value = codegen->GetIntConstant(byte_size, fnv_seed, fnv_seed);
        break;
      case TYPE_FLOAT: {
        // Don't care about the value, just the bit pattern
        float fnv_seed_float = *reinterpret_cast<float*>(&fnv_seed);
        null_value = ConstantFP::get(codegen->context(), APFloat(fnv_seed_float));
        break;
      }
      case TYPE_DOUBLE: {
        // Don't care about the value, just the bit pattern
        double fnv_seed_double = *reinterpret_cast<double*>(&fnv_seed);
        null_value = ConstantFP::get(codegen->context(), APFloat(fnv_seed_double));
        break;
      }
      default:
        DCHECK(false);
    }
    builder->CreateStore(null_value, dst);
  }
}

// Codegen for evaluating a tuple row over either build_expr_ctxs_ or probe_expr_ctxs_.
// For a group by with (big int, string) the IR looks like:
//
// define i1 @EvalProbeRow(%"class.impala::NewPartitionedHashTableCtx"* %this_ptr,
//    %"class.impala::TupleRow"* %row, i8* %expr_values, i8* %expr_values_null) #34 {
// entry:
//   %loc_addr = getelementptr i8, i8* %expr_values, i32 0
//   %loc = bitcast i8* %loc_addr to i64*
//   %result = call { i8, i64 } @GetSlotRef.2(%"class.impala::ExprContext"*
//        inttoptr (i64 197737664 to %"class.impala::ExprContext"*),
//        %"class.impala::TupleRow"* %row)
//   %0 = extractvalue { i8, i64 } %result, 0
//   %is_null = trunc i8 %0 to i1
//   %1 = zext i1 %is_null to i8
//   %null_byte_loc = getelementptr i8, i8* %expr_values_null, i32 0
//   store i8 %1, i8* %null_byte_loc
//   br i1 %is_null, label %null, label %not_null
//
// null:                                             ; preds = %entry
//   store i64 2166136261, i64* %loc
//   br label %continue
//
// not_null:                                         ; preds = %entry
//   %val = extractvalue { i8, i64 } %result, 1
//   store i64 %val, i64* %loc
//   br label %continue
//
// continue:                                         ; preds = %not_null, %null
//   %is_null_phi = phi i1 [ true, %null ], [ false, %not_null ]
//   %has_null = or i1 false, %is_null_phi
//   %loc_addr1 = getelementptr i8, i8* %expr_values, i32 8
//   %loc2 = bitcast i8* %loc_addr1 to %"struct.impala::StringValue"*
//   %result6 = call { i64, i8* } @GetSlotRef.3(%"class.impala::ExprContext"*
//      inttoptr (i64 197738048 to %"class.impala::ExprContext"*),
//      %"class.impala::TupleRow"* %row)
//   %2 = extractvalue { i64, i8* } %result6, 0
//   %is_null7 = trunc i64 %2 to i1
//   %3 = zext i1 %is_null7 to i8
//   %null_byte_loc8 = getelementptr i8, i8* %expr_values_null, i32 1
//   store i8 %3, i8* %null_byte_loc8
//   br i1 %is_null7, label %null3, label %not_null4
//
// null3:                                            ; preds = %continue
//   %string_ptr = getelementptr inbounds %"struct.impala::StringValue",
//        %"struct.impala::StringValue"* %loc2, i32 0, i32 0
//   %string_len = getelementptr inbounds %"struct.impala::StringValue",
//        %"struct.impala::StringValue"* %loc2, i32 0, i32 1
//   store i8* inttoptr (i32 -2128831035 to i8*), i8** %string_ptr
//   store i32 -2128831035, i32* %string_len
//   br label %continue5
//
// not_null4:                                        ; preds = %continue
//   %4 = extractvalue { i64, i8* } %result6, 0
//   %5 = ashr i64 %4, 32
//   %6 = trunc i64 %5 to i32
//   %7 = insertvalue %"struct.impala::StringValue" zeroinitializer, i32 %6, 1
//   %result9 = extractvalue { i64, i8* } %result6, 1
//   %8 = insertvalue %"struct.impala::StringValue" %7, i8* %result9, 0
//   store %"struct.impala::StringValue" %8, %"struct.impala::StringValue"* %loc2
//   br label %continue5
//
// continue5:                                        ; preds = %not_null4, %null3
//   %is_null_phi10 = phi i1 [ true, %null3 ], [ false, %not_null4 ]
//   %has_null11 = or i1 %has_null, %is_null_phi10
//   ret i1 %has_null11
// }
//
// For each expr, we create 3 code blocks.  The null, not null and continue blocks.
// Both the null and not null branch into the continue block.  The continue block
// becomes the start of the next block for codegen (either the next expr or just the
// end of the function).
Status NewPartitionedHashTableCtx::CodegenEvalRow(LlvmCodeGen* codegen, bool build, Function** fn) {
  const vector<ExprContext*>& ctxs = build ? build_expr_ctxs_ : probe_expr_ctxs_;
  for (int i = 0; i < ctxs.size(); ++i) {
    // Disable codegen for CHAR
    if (ctxs[i]->root()->type().type == TYPE_CHAR) {
      return Status::InternalError("NewPartitionedHashTableCtx::CodegenEvalRow(): CHAR NYI");
    }
  }

  // Get types to generate function prototype
  Type* this_type = codegen->GetType(NewPartitionedHashTableCtx::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = codegen->GetPtrType(this_type);
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  DCHECK(tuple_row_type != NULL);
  PointerType* tuple_row_ptr_type = codegen->GetPtrType(tuple_row_type);
  LlvmCodeGen::FnPrototype prototype(codegen, build ? "EvalBuildRow" : "EvalProbeRow",
      codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("expr_values", codegen->ptr_type()));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("expr_values_null", codegen->ptr_type()));

  LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  Value* args[4];
  *fn = prototype.GeneratePrototype(&builder, args);
  Value* this_ptr = args[0];
  Value* row = args[1];
  Value* expr_values = args[2];
  Value* expr_values_null = args[3];
  Value* has_null = codegen->false_value();

  // ctx_vector = &build_expr_ctxs_[0] / ctx_vector = &probe_expr_ctxs_[0]
  Value* ctx_vector = codegen->CodegenCallFunction(&builder, build ?
      IRFunction::HASH_TABLE_GET_BUILD_EXPR_CTX :
      IRFunction::HASH_TABLE_GET_PROBE_EXPR_CTX,
      this_ptr, "ctx_vector");

  for (int i = 0; i < ctxs.size(); ++i) {
    // TODO: refactor this to somewhere else?  This is not hash table specific except for
    // the null handling bit and would be used for anyone that needs to materialize a
    // vector of exprs
    // Convert result buffer to llvm ptr type
    int offset = expr_values_cache_.expr_values_offsets(i);
    Value* loc = builder.CreateInBoundsGEP(
        NULL, expr_values, codegen->GetIntConstant(TYPE_INT, offset), "loc_addr");
    Value* llvm_loc = builder.CreatePointerCast(
        loc, codegen->GetPtrType(ctxs[i]->root()->type()), "loc");

    BasicBlock* null_block = BasicBlock::Create(context, "null", *fn);
    BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", *fn);
    BasicBlock* continue_block = BasicBlock::Create(context, "continue", *fn);

    // Call expr
    Function* expr_fn;
    Status status = ctxs[i]->root()->GetCodegendComputeFn(codegen, &expr_fn);
    if (!status.ok()) {
      (*fn)->eraseFromParent(); // deletes function
      *fn = NULL;
      return Status::InternalError(Substitute(
          "Problem with NewPartitionedHashTableCtx::CodegenEvalRow(): $0", status.GetDetail()));
    }

    // Avoid bloating function by inlining too many exprs into it.
    if (i >= LlvmCodeGen::CODEGEN_INLINE_EXPRS_THRESHOLD) {
      codegen->SetNoInline(expr_fn);
    }

    Value* expr_ctx = codegen->CodegenArrayAt(&builder, ctx_vector, i, "expr_ctx");
    CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(
        codegen, &builder, ctxs[i]->root()->type(), expr_fn, {expr_ctx, row}, "result");
    Value* is_null = result.GetIsNull();

    // Set null-byte result
    Value* null_byte = builder.CreateZExt(is_null, codegen->GetType(TYPE_TINYINT));
    Value* llvm_null_byte_loc = builder.CreateInBoundsGEP(
        NULL, expr_values_null, codegen->GetIntConstant(TYPE_INT, i), "null_byte_loc");
    builder.CreateStore(null_byte, llvm_null_byte_loc);
    builder.CreateCondBr(is_null, null_block, not_null_block);

    // Null block
    builder.SetInsertPoint(null_block);
    if (!stores_nulls_) {
      // hash table doesn't store nulls, no reason to keep evaluating exprs
      builder.CreateRet(codegen->true_value());
    } else {
      CodegenAssignNullValue(codegen, &builder, llvm_loc, ctxs[i]->root()->type());
      builder.CreateBr(continue_block);
    }

    // Not null block
    builder.SetInsertPoint(not_null_block);
    result.ToNativePtr(llvm_loc);
    builder.CreateBr(continue_block);

    // Continue block
    builder.SetInsertPoint(continue_block);
    if (stores_nulls_) {
      // Update has_null
      PHINode* is_null_phi = builder.CreatePHI(codegen->boolean_type(), 2, "is_null_phi");
      is_null_phi->addIncoming(codegen->true_value(), null_block);
      is_null_phi->addIncoming(codegen->false_value(), not_null_block);
      has_null = builder.CreateOr(has_null, is_null_phi, "has_null");
    }
  }
  builder.CreateRet(has_null);

  // Avoid inlining a large EvalRow() function into caller.
  if (ctxs.size() > LlvmCodeGen::CODEGEN_INLINE_EXPR_BATCH_THRESHOLD) {
    codegen->SetNoInline(*fn);
  }

  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status::InternalError("Codegen'd NewPartitionedHashTableCtx::EvalRow() function failed verification, "
                  "see log");
  }
  return Status::OK();
}

// Codegen for hashing the current row.  In the case with both string and non-string data
// (group by int_col, string_col), the IR looks like:
//
// define i32 @HashRow(%"class.impala::NewPartitionedHashTableCtx"* %this_ptr, i8* %expr_values,
//    i8* %expr_values_null) #34 {
// entry:
//   %seed = call i32 @_ZNK6impala12NewPartitionedHashTableCtx11GetHashSeedEv(
//        %"class.impala::NewPartitionedHashTableCtx"* %this_ptr)
//   %hash = call i32 @CrcHash8(i8* %expr_values, i32 8, i32 %seed)
//   %loc_addr = getelementptr i8, i8* %expr_values, i32 8
//   %null_byte_loc = getelementptr i8, i8* %expr_values_null, i32 1
//   %null_byte = load i8, i8* %null_byte_loc
//   %is_null = icmp ne i8 %null_byte, 0
//   br i1 %is_null, label %null, label %not_null
//
// null:                                             ; preds = %entry
//   %str_null = call i32 @CrcHash16(i8* %loc_addr, i32 16, i32 %hash)
//   br label %continue
//
// not_null:                                         ; preds = %entry
//   %str_val = bitcast i8* %loc_addr to %"struct.impala::StringValue"*
//   %0 = getelementptr inbounds %"struct.impala::StringValue",
//        %"struct.impala::StringValue"* %str_val, i32 0, i32 0
//   %1 = getelementptr inbounds %"struct.impala::StringValue",
//        %"struct.impala::StringValue"* %str_val, i32 0, i32 1
//   %ptr = load i8*, i8** %0
//   %len = load i32, i32* %1
//   %string_hash = call i32 @IrCrcHash(i8* %ptr, i32 %len, i32 %hash)
//   br label %continue
//
// continue:                                         ; preds = %not_null, %null
//   %hash_phi = phi i32 [ %string_hash, %not_null ], [ %str_null, %null ]
//   ret i32 %hash_phi
// }
Status NewPartitionedHashTableCtx::CodegenHashRow(LlvmCodeGen* codegen, bool use_murmur, Function** fn) {
  for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
    // Disable codegen for CHAR
    if (build_expr_ctxs_[i]->root()->type().type == TYPE_CHAR) {
      return Status::InternalError("NewPartitionedHashTableCtx::CodegenHashRow(): CHAR NYI");
    }
  }

  // Get types to generate function prototype
  Type* this_type = codegen->GetType(NewPartitionedHashTableCtx::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = codegen->GetPtrType(this_type);

  LlvmCodeGen::FnPrototype prototype(
      codegen, (use_murmur ? "MurmurHashRow" : "HashRow"), codegen->GetType(TYPE_INT));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("expr_values", codegen->ptr_type()));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("expr_values_null", codegen->ptr_type()));

  LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  Value* args[3];
  *fn = prototype.GeneratePrototype(&builder, args);
  Value* this_arg = args[0];
  Value* expr_values = args[1];
  Value* expr_values_null = args[2];

  // Call GetHashSeed() to get seeds_[level_]
  Value* seed = codegen->CodegenCallFunction(&builder,
      IRFunction::HASH_TABLE_GET_HASH_SEED, this_arg, "seed");

  Value* hash_result = seed;
  const int var_result_offset = expr_values_cache_.var_result_offset();
  const int expr_values_bytes_per_row = expr_values_cache_.expr_values_bytes_per_row();
  if (var_result_offset == -1) {
    // No variable length slots, just hash what is in 'expr_expr_values_cache_'
    if (expr_values_bytes_per_row > 0) {
      Function* hash_fn = use_murmur ?
                          codegen->GetMurmurHashFunction(expr_values_bytes_per_row) :
                          codegen->GetHashFunction(expr_values_bytes_per_row);
      Value* len = codegen->GetIntConstant(TYPE_INT, expr_values_bytes_per_row);
      hash_result = builder.CreateCall(
          hash_fn, ArrayRef<Value*>({expr_values, len, hash_result}), "hash");
    }
  } else {
    if (var_result_offset > 0) {
      Function* hash_fn = use_murmur ?
                          codegen->GetMurmurHashFunction(var_result_offset) :
                          codegen->GetHashFunction(var_result_offset);
      Value* len = codegen->GetIntConstant(TYPE_INT, var_result_offset);
      hash_result = builder.CreateCall(
          hash_fn, ArrayRef<Value*>({expr_values, len, hash_result}), "hash");
    }

    // Hash string slots
    for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
      if (build_expr_ctxs_[i]->root()->type().type != TYPE_STRING
          && build_expr_ctxs_[i]->root()->type().type != TYPE_VARCHAR) continue;

      BasicBlock* null_block = NULL;
      BasicBlock* not_null_block = NULL;
      BasicBlock* continue_block = NULL;
      Value* str_null_result = NULL;

      int offset = expr_values_cache_.expr_values_offsets(i);
      Value* llvm_loc = builder.CreateInBoundsGEP(
          NULL, expr_values, codegen->GetIntConstant(TYPE_INT, offset), "loc_addr");

      // If the hash table stores nulls, we need to check if the stringval
      // evaluated to NULL
      if (stores_nulls_) {
        null_block = BasicBlock::Create(context, "null", *fn);
        not_null_block = BasicBlock::Create(context, "not_null", *fn);
        continue_block = BasicBlock::Create(context, "continue", *fn);

        Value* llvm_null_byte_loc = builder.CreateInBoundsGEP(NULL, expr_values_null,
            codegen->GetIntConstant(TYPE_INT, i), "null_byte_loc");
        Value* null_byte = builder.CreateLoad(llvm_null_byte_loc, "null_byte");
        Value* is_null = builder.CreateICmpNE(
            null_byte, codegen->GetIntConstant(TYPE_TINYINT, 0), "is_null");
        builder.CreateCondBr(is_null, null_block, not_null_block);

        // For null, we just want to call the hash function on the portion of
        // the data
        builder.SetInsertPoint(null_block);
        Function* null_hash_fn = use_murmur ?
                                 codegen->GetMurmurHashFunction(sizeof(StringValue)) :
                                 codegen->GetHashFunction(sizeof(StringValue));
        Value* len = codegen->GetIntConstant(TYPE_INT, sizeof(StringValue));
        str_null_result = builder.CreateCall(null_hash_fn,
            ArrayRef<Value*>({llvm_loc, len, hash_result}), "str_null");
        builder.CreateBr(continue_block);

        builder.SetInsertPoint(not_null_block);
      }

      // Convert expr_values_buffer_ loc to llvm value
      Value* str_val = builder.CreatePointerCast(llvm_loc,
          codegen->GetPtrType(TYPE_STRING), "str_val");

      Value* ptr = builder.CreateStructGEP(NULL, str_val, 0);
      Value* len = builder.CreateStructGEP(NULL, str_val, 1);
      ptr = builder.CreateLoad(ptr, "ptr");
      len = builder.CreateLoad(len, "len");

      // Call hash(ptr, len, hash_result);
      Function* general_hash_fn = use_murmur ? codegen->GetMurmurHashFunction() :
                                  codegen->GetHashFunction();
      Value* string_hash_result = builder.CreateCall(general_hash_fn,
          ArrayRef<Value*>({ptr, len, hash_result}), "string_hash");

      if (stores_nulls_) {
        builder.CreateBr(continue_block);
        builder.SetInsertPoint(continue_block);
        // Use phi node to reconcile that we could have come from the string-null
        // path and string not null paths.
        PHINode* phi_node = builder.CreatePHI(codegen->GetType(TYPE_INT), 2, "hash_phi");
        phi_node->addIncoming(string_hash_result, not_null_block);
        phi_node->addIncoming(str_null_result, null_block);
        hash_result = phi_node;
      } else {
        hash_result = string_hash_result;
      }
    }
  }

  builder.CreateRet(hash_result);

  // Avoid inlining into caller if there are many exprs.
  if (build_expr_ctxs_.size() > LlvmCodeGen::CODEGEN_INLINE_EXPR_BATCH_THRESHOLD) {
    codegen->SetNoInline(*fn);
  }
  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status::InternalError(
        "Codegen'd NewPartitionedHashTableCtx::HashRow() function failed verification, see log");
  }
  return Status::OK();
}

// Codegen for NewPartitionedHashTableCtx::Equals.  For a group by with (bigint, string),
// the IR looks like:
//
// define i1 @Equals(%"class.impala::NewPartitionedHashTableCtx"* %this_ptr, %"class.impala::TupleRow"*
// %row,
//      i8* %expr_values, i8* %expr_values_null) #34 {
// entry:
//   %0 = alloca { i64, i8* }
//   %result = call { i8, i64 } @GetSlotRef.2(%"class.impala::ExprContext"*
//        inttoptr (i64 139107136 to %"class.impala::ExprContext"*),
//        %"class.impala::TupleRow"* %row)
//   %1 = extractvalue { i8, i64 } %result, 0
//   %is_null = trunc i8 %1 to i1
//   %null_byte_loc = getelementptr i8, i8* %expr_values_null, i32 0
//   %2 = load i8, i8* %null_byte_loc
//   %3 = icmp ne i8 %2, 0
//   %loc = getelementptr i8, i8* %expr_values, i32 0
//   %row_val = bitcast i8* %loc to i64*
//   br i1 %is_null, label %null, label %not_null
//
// false_block:                                      ; preds = %cmp9, %not_null2, %null1,
//                                                             %cmp, %not_null, %null
//   ret i1 false
//
// null:                                             ; preds = %entry
//   br i1 %3, label %continue, label %false_block
//
// not_null:                                         ; preds = %entry
//   br i1 %3, label %false_block, label %cmp
//
// continue:                                         ; preds = %cmp, %null
//   %result4 = call { i64, i8* } @GetSlotRef.3(%"class.impala::ExprContext"*
//        inttoptr (i64 139107328 to %"class.impala::ExprContext"*),
//        %"class.impala::TupleRow"* %row)
//   %4 = extractvalue { i64, i8* } %result4, 0
//   %is_null5 = trunc i64 %4 to i1
//   %null_byte_loc6 = getelementptr i8, i8* %expr_values_null, i32 1
//   %5 = load i8, i8* %null_byte_loc6
//   %6 = icmp ne i8 %5, 0
//   %loc7 = getelementptr i8, i8* %expr_values, i32 8
//   %row_val8 = bitcast i8* %loc7 to %"struct.impala::StringValue"*
//   br i1 %is_null5, label %null1, label %not_null2
//
// cmp:                                              ; preds = %not_null
//   %7 = load i64, i64* %row_val
//   %val = extractvalue { i8, i64 } %result, 1
//   %cmp_raw = icmp eq i64 %val, %7
//   br i1 %cmp_raw, label %continue, label %false_block
//
// null1:                                            ; preds = %continue
//   br i1 %6, label %continue3, label %false_block
//
// not_null2:                                        ; preds = %continue
//   br i1 %6, label %false_block, label %cmp9
//
// continue3:                                        ; preds = %cmp9, %null1
//   ret i1 true
//
// cmp9:                                             ; preds = %not_null2
//   store { i64, i8* } %result4, { i64, i8* }* %0
//   %8 = bitcast { i64, i8* }* %0 to %"struct.impala_udf::StringVal"*
//   %cmp_raw10 = call i1
//        @_Z13StringValueEqRKN10impala_udf9StringValERKN6impala11StringValueE(
//        %"struct.impala_udf::StringVal"* %8, %"struct.impala::StringValue"* %row_val8)
//   br i1 %cmp_raw10, label %continue3, label %false_block
// }
Status NewPartitionedHashTableCtx::CodegenEquals(LlvmCodeGen* codegen, bool force_null_equality,
    Function** fn) {
  for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
    // Disable codegen for CHAR
    if (build_expr_ctxs_[i]->root()->type().type == TYPE_CHAR) {
      return Status::InternalError("NewPartitionedHashTableCtx::CodegenEquals(): CHAR NYI");
    }
  }

  // Get types to generate function prototype
  Type* this_type = codegen->GetType(NewPartitionedHashTableCtx::LLVM_CLASS_NAME);
  DCHECK(this_type != NULL);
  PointerType* this_ptr_type = codegen->GetPtrType(this_type);
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  DCHECK(tuple_row_type != NULL);
  PointerType* tuple_row_ptr_type = codegen->GetPtrType(tuple_row_type);

  LlvmCodeGen::FnPrototype prototype(codegen, "Equals", codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("expr_values", codegen->ptr_type()));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("expr_values_null", codegen->ptr_type()));

  LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  Value* args[4];
  *fn = prototype.GeneratePrototype(&builder, args);
  Value* this_ptr = args[0];
  Value* row = args[1];
  Value* expr_values = args[2];
  Value* expr_values_null = args[3];

  // ctx_vector = &build_expr_ctxs_[0]
  Value* ctx_vector = codegen->CodegenCallFunction(&builder,
      IRFunction::HASH_TABLE_GET_BUILD_EXPR_CTX, this_ptr, "ctx_vector");

  BasicBlock* false_block = BasicBlock::Create(context, "false_block", *fn);
  for (int i = 0; i < build_expr_ctxs_.size(); ++i) {
    BasicBlock* null_block = BasicBlock::Create(context, "null", *fn);
    BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", *fn);
    BasicBlock* continue_block = BasicBlock::Create(context, "continue", *fn);

    // call GetValue on build_exprs[i]
    Function* expr_fn;
    Status status = build_expr_ctxs_[i]->root()->GetCodegendComputeFn(codegen, &expr_fn);
    if (!status.ok()) {
      (*fn)->eraseFromParent(); // deletes function
      *fn = NULL;
      return Status::InternalError(
          Substitute("Problem with NewPartitionedHashTableCtx::CodegenEquals: $0", status.GetDetail()));
    }
    if (build_expr_ctxs_.size() > LlvmCodeGen::CODEGEN_INLINE_EXPRS_THRESHOLD) {
      // Avoid bloating function by inlining too many exprs into it.
      codegen->SetNoInline(expr_fn);
    }

    // Load ExprContext*: expr_ctx = ctx_vector[i];
    Value* expr_ctx = codegen->CodegenArrayAt(&builder, ctx_vector, i, "expr_ctx");

    // Evaluate the expression.
    CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
        build_expr_ctxs_[i]->root()->type(), expr_fn, {expr_ctx, row}, "result");
    Value* is_null = result.GetIsNull();

    // Determine if row is null (i.e. expr_values_null[i] == true). In
    // the case where the hash table does not store nulls, this is always false.
    Value* row_is_null = codegen->false_value();

    // We consider null values equal if we are comparing build rows or if the join
    // predicate is <=>
    if (force_null_equality || finds_nulls_[i]) {
      Value* llvm_null_byte_loc = builder.CreateInBoundsGEP(
          NULL, expr_values_null, codegen->GetIntConstant(TYPE_INT, i), "null_byte_loc");
      Value* null_byte = builder.CreateLoad(llvm_null_byte_loc);
      row_is_null =
          builder.CreateICmpNE(null_byte, codegen->GetIntConstant(TYPE_TINYINT, 0));
    }

    // Get llvm value for row_val from 'expr_values'
    int offset = expr_values_cache_.expr_values_offsets(i);
    Value* loc = builder.CreateInBoundsGEP(
        NULL, expr_values, codegen->GetIntConstant(TYPE_INT, offset), "loc");
    Value* row_val = builder.CreatePointerCast(
        loc, codegen->GetPtrType(build_expr_ctxs_[i]->root()->type()), "row_val");

    // Branch for GetValue() returning NULL
    builder.CreateCondBr(is_null, null_block, not_null_block);

    // Null block
    builder.SetInsertPoint(null_block);
    builder.CreateCondBr(row_is_null, continue_block, false_block);

    // Not-null block
    builder.SetInsertPoint(not_null_block);
    if (stores_nulls_) {
      BasicBlock* cmp_block = BasicBlock::Create(context, "cmp", *fn);
      // First need to compare that row expr[i] is not null
      builder.CreateCondBr(row_is_null, false_block, cmp_block);
      builder.SetInsertPoint(cmp_block);
    }
    // Check result == row_val
    Value* is_equal = result.EqToNativePtr(row_val);
    builder.CreateCondBr(is_equal, continue_block, false_block);

    builder.SetInsertPoint(continue_block);
  }
  builder.CreateRet(codegen->true_value());

  builder.SetInsertPoint(false_block);
  builder.CreateRet(codegen->false_value());

  // Avoid inlining into caller if it is large.
  if (build_expr_ctxs_.size() > LlvmCodeGen::CODEGEN_INLINE_EXPR_BATCH_THRESHOLD) {
    codegen->SetNoInline(*fn);
  }
  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status::InternalError("Codegen'd NewPartitionedHashTableCtx::Equals() function failed verification, "
                  "see log");
  }
  return Status::OK();
}

Status NewPartitionedHashTableCtx::ReplaceHashTableConstants(LlvmCodeGen* codegen,
    bool stores_duplicates, int num_build_tuples, Function* fn,
    HashTableReplacedConstants* replacement_counts) {

  replacement_counts->stores_nulls = codegen->ReplaceCallSitesWithBoolConst(
      fn, stores_nulls(), "stores_nulls");
  replacement_counts->finds_some_nulls = codegen->ReplaceCallSitesWithBoolConst(
      fn, finds_some_nulls(), "finds_some_nulls");
  replacement_counts->stores_tuples = codegen->ReplaceCallSitesWithBoolConst(
      fn, num_build_tuples == 1, "stores_tuples");
  replacement_counts->stores_duplicates = codegen->ReplaceCallSitesWithBoolConst(
      fn, stores_duplicates, "stores_duplicates");
  replacement_counts->quadratic_probing = codegen->ReplaceCallSitesWithBoolConst(
      fn, FLAGS_enable_quadratic_probing, "quadratic_probing");
  return Status::OK();
}

#endif

