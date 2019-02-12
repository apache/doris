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

#include "codegen/codegen_anyval.h"
#include "codegen/llvm_codegen.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/slot_ref.h"
#include "runtime/buffered_block_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.hpp"
#include "util/doris_metrics.h"

// using namespace llvm;

// DEFINE_bool(enable_quadratic_probing, true, "Enable quadratic probing hash table");

using std::string;
using std::stringstream;
using std::vector;
using std::endl;

using llvm::BasicBlock;
using llvm::Value;
using llvm::Function;
using llvm::Type;
using llvm::PointerType;
using llvm::LLVMContext;
using llvm::PHINode;
using llvm::ConstantFP;
using llvm::APFloat;

namespace doris {

const char* PartitionedHashTableCtx::_s_llvm_class_name = "class.doris::PartitionedHashTableCtx";

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

#if 0
// Helper function to store a value into the results buffer if the expr
// evaluated to NULL.  We don't want (NULL, 1) to hash to the same as (0,1) so
// we'll pick a more random value.
static void codegen_assign_null_value(LlvmCodeGen* codegen,
        // LlvmCodeGen::LlvmBuilder* builder, Value* dst, const ColumnType& type) {
        LlvmCodeGen::LlvmBuilder* builder, Value* dst, const TypeDescriptor& type) {
    int64_t fvn_seed = HashUtil::FNV_SEED;

    // if (type.type == TYPE_STRING || type.type == TYPE_VARCHAR) {
    if (type.type == TYPE_VARCHAR) {
        Value* dst_ptr = builder->CreateStructGEP(dst, 0, "string_ptr");
        Value* dst_len = builder->CreateStructGEP(dst, 1, "string_len");
        Value* null_len = codegen->get_int_constant(TYPE_INT, fvn_seed);
        Value* null_ptr = builder->CreateIntToPtr(null_len, codegen->ptr_type());
        builder->CreateStore(null_ptr, dst_ptr);
        builder->CreateStore(null_len, dst_len);
    } else {
        Value* null_value = NULL;
        // Get a type specific representation of fvn_seed
        switch (type.type) {
            case TYPE_BOOLEAN:
                // In results, booleans are stored as 1 byte
                dst = builder->CreateBitCast(dst, codegen->ptr_type());
                null_value = codegen->get_int_constant(TYPE_TINYINT, fvn_seed);
                break;
            case TYPE_TINYINT:
            case TYPE_SMALLINT:
            case TYPE_INT:
            case TYPE_BIGINT:
                null_value = codegen->get_int_constant(type.type, fvn_seed);
                break;
            case TYPE_FLOAT: {
                                 // Don't care about the value, just the bit pattern
                                 float fvn_seed_float = *reinterpret_cast<float*>(&fvn_seed);
                                 null_value = ConstantFP::get(codegen->context(), APFloat(fvn_seed_float));
                                 break;
                             }
            case TYPE_DOUBLE: {
                                  // Don't care about the value, just the bit pattern
                                  double fvn_seed_double = *reinterpret_cast<double*>(&fvn_seed);
                                  null_value = ConstantFP::get(codegen->context(), APFloat(fvn_seed_double));
                                  break;
                              }
            default:
                              DCHECK(false);
        }
        builder->CreateStore(null_value, dst);
    }
}

// Codegen for evaluating a tuple row over either _build_expr_ctxs or _probe_expr_ctxs.
// For the case where we are joining on a single int, the IR looks like
// define i1 @EvalBuildRow(%"class.impala::PartitionedHashTableCtx"* %this_ptr,
//                         %"class.impala::TupleRow"* %row) #20 {
// entry:
//   %result = call i64 @GetSlotRef1(%"class.impala::ExprContext"* inttoptr
//                                     (i64 67971664 to %"class.impala::ExprContext"*),
//                                   %"class.impala::TupleRow"* %row)
//   %is_null = trunc i64 %result to i1
//   %0 = zext i1 %is_null to i8
//   store i8 %0, i8* inttoptr (i64 95753144 to i8*)
//   br i1 %is_null, label %null, label %not_null
//
// null:                                             ; preds = %entry
//   store i32 -2128831035, i32* inttoptr (i64 95753128 to i32*)
//   br label %continue
//
// not_null:                                         ; preds = %entry
//   %1 = ashr i64 %result, 32
//   %2 = trunc i64 %1 to i32
//   store i32 %2, i32* inttoptr (i64 95753128 to i32*)
//   br label %continue
//
// continue:                                         ; preds = %not_null, %null
//   ret i1 true
// }
// For each expr, we create 3 code blocks.  The null, not null and continue blocks.
// Both the null and not null branch into the continue block.  The continue block
// becomes the start of the next block for codegen (either the next expr or just the
// end of the function).
Function* PartitionedHashTableCtx::codegen_eval_row(RuntimeState* state, bool build) {
    // TODO: codegen_assign_null_value() can't handle TYPE_TIMESTAMP or TYPE_DECIMAL yet
    const vector<ExprContext*>& ctxs = build ? _build_expr_ctxs : _probe_expr_ctxs;
    for (int i = 0; i < ctxs.size(); ++i) {
        PrimitiveType type = ctxs[i]->root()->type().type;
        // if (type == TYPE_TIMESTAMP || type == TYPE_DECIMAL || type == TYPE_CHAR) {
        if (type == TYPE_DATETIME || type == TYPE_DATE
                || type == TYPE_DECIMAL || type == TYPE_CHAR) {
            return NULL;
        }
    }

    LlvmCodeGen* codegen;
    if (!state->get_codegen(&codegen).ok()) {
        return NULL;
    }

    // Get types to generate function prototype
    Type* tuple_row_type = codegen->get_type(TupleRow::_s_llvm_class_name);
    DCHECK(tuple_row_type != NULL);
    PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

    Type* this_type = codegen->get_type(PartitionedHashTableCtx::_s_llvm_class_name);
    DCHECK(this_type != NULL);
    PointerType* this_ptr_type = PointerType::get(this_type, 0);

    LlvmCodeGen::FnPrototype prototype(codegen, build ? "EvalBuildRow" : "EvalProbeRow",
            codegen->get_type(TYPE_BOOLEAN));
    prototype.add_argument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
    prototype.add_argument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

    LLVMContext& context = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(context);
    Value* args[2];
    Function* fn = prototype.generate_prototype(&builder, args);

    Value* row = args[1];
    Value* has_null = codegen->false_value();

    for (int i = 0; i < ctxs.size(); ++i) {
        // TODO: refactor this to somewhere else?  This is not hash table specific except for
        // the null handling bit and would be used for anyone that needs to materialize a
        // vector of exprs
        // Convert result buffer to llvm ptr type
        void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];
        Value* llvm_loc = codegen->cast_ptr_to_llvm_ptr(
                codegen->get_ptr_type(ctxs[i]->root()->type()), loc);

        BasicBlock* null_block = BasicBlock::Create(context, "null", fn);
        BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", fn);
        BasicBlock* continue_block = BasicBlock::Create(context, "continue", fn);

        // Call expr
        Function* expr_fn;
        Status status = ctxs[i]->root()->get_codegend_compute_fn(state, &expr_fn);
        if (!status.ok()) {
            VLOG_QUERY << "Problem with codegen_eval_row: " << status.get_error_msg();
            fn->eraseFromParent(); // deletes function
            return NULL;
        }

        Value* ctx_arg = codegen->cast_ptr_to_llvm_ptr(
                codegen->get_ptr_type(ExprContext::_s_llvm_class_name), ctxs[i]);
        Value* expr_fn_args[] = { ctx_arg, row };
        CodegenAnyVal result = CodegenAnyVal::create_call_wrapped(
                codegen, &builder, ctxs[i]->root()->type(), expr_fn, expr_fn_args, "result");
        Value* is_null = result.get_is_null();

        // Set null-byte result
        Value* null_byte = builder.CreateZExt(is_null, codegen->get_type(TYPE_TINYINT));
        uint8_t* null_byte_loc = &_expr_value_null_bits[i];
        Value* llvm_null_byte_loc =
            codegen->cast_ptr_to_llvm_ptr(codegen->ptr_type(), null_byte_loc);
        builder.CreateStore(null_byte, llvm_null_byte_loc);

        builder.CreateCondBr(is_null, null_block, not_null_block);

        // Null block
        builder.SetInsertPoint(null_block);
        if (!_stores_nulls) {
            // hash table doesn't store nulls, no reason to keep evaluating exprs
            builder.CreateRet(codegen->true_value());
        } else {
            codegen_assign_null_value(codegen, &builder, llvm_loc, ctxs[i]->root()->type());
            builder.CreateBr(continue_block);
        }

        // Not null block
        builder.SetInsertPoint(not_null_block);
        result.ToNativePtr(llvm_loc);
        builder.CreateBr(continue_block);

        // Continue block
        builder.SetInsertPoint(continue_block);
        if (_stores_nulls) {
            // Update has_null
            PHINode* is_null_phi = builder.CreatePHI(codegen->boolean_type(), 2, "is_null_phi");
            is_null_phi->addIncoming(codegen->true_value(), null_block);
            is_null_phi->addIncoming(codegen->false_value(), not_null_block);
            has_null = builder.CreateOr(has_null, is_null_phi, "has_null");
        }
    }
    builder.CreateRet(has_null);

    return codegen->FinalizeFunction(fn);
}

// Codegen for hashing the current row.  In the case with both string and non-string data
// (group by int_col, string_col), the IR looks like:
// define i32 @HashCurrentRow(%"class.impala::PartitionedHashTableCtx"* %this_ptr) #20 {
// entry:
//   %seed = call i32 @get_hash_seed(%"class.impala::PartitionedHashTableCtx"* %this_ptr)
//   %0 = call i32 @CrcHash16(i8* inttoptr (i64 119151296 to i8*), i32 16, i32 %seed)
//   %1 = load i8* inttoptr (i64 119943721 to i8*)
//   %2 = icmp ne i8 %1, 0
//   br i1 %2, label %null, label %not_null
//
// null:                                             ; preds = %entry
//   %3 = call i32 @CrcHash161(i8* inttoptr (i64 119151312 to i8*), i32 16, i32 %0)
//   br label %continue
//
// not_null:                                         ; preds = %entry
//   %4 = load i8** getelementptr inbounds (%"struct.impala::StringValue"* inttoptr
//       (i64 119151312 to %"struct.impala::StringValue"*), i32 0, i32 0)
//   %5 = load i32* getelementptr inbounds (%"struct.impala::StringValue"* inttoptr
//       (i64 119151312 to %"struct.impala::StringValue"*), i32 0, i32 1)
//   %6 = call i32 @IrCrcHash(i8* %4, i32 %5, i32 %0)
//   br label %continue
//
// continue:                                         ; preds = %not_null, %null
//   %7 = phi i32 [ %6, %not_null ], [ %3, %null ]
//   call void @set_hash(%"class.impala::PartitionedHashTableCtx"* %this_ptr, i32 %7)
//   ret i32 %7
// }
Function* PartitionedHashTableCtx::codegen_hash_current_row(RuntimeState* state, bool use_murmur) {
    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        // Disable codegen for CHAR
        if (_build_expr_ctxs[i]->root()->type().type == TYPE_CHAR) return NULL;
    }

    LlvmCodeGen* codegen;
    if (!state->get_codegen(&codegen).ok()) return NULL;

    // Get types to generate function prototype
    Type* this_type = codegen->get_type(PartitionedHashTableCtx::_s_llvm_class_name);
    DCHECK(this_type != NULL);
    PointerType* this_ptr_type = PointerType::get(this_type, 0);

    LlvmCodeGen::FnPrototype prototype(codegen,
            (use_murmur ? "MurmurHashCurrentRow" : "HashCurrentRow"),
            codegen->get_type(TYPE_INT));
    prototype.add_argument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));

    LLVMContext& context = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(context);
    Value* this_arg;
    Function* fn = prototype.generate_prototype(&builder, &this_arg);

    // Call get_hash_seed() to get _seeds[_level]
    Function* get_hash_seed_fn = codegen->GetFunction(IRFunction::HASH_TABLE_GET_HASH_SEED);
    Value* seed = builder.CreateCall(get_hash_seed_fn, this_arg, "seed");

    Value* hash_result = seed;
    Value* data = codegen->cast_ptr_to_llvm_ptr(codegen->ptr_type(), _expr_values_buffer);
    if (_var_result_begin == -1) {
        // No variable length slots, just hash what is in '_expr_values_buffer'
        if (_results_buffer_size > 0) {
            Function* hash_fn = use_murmur ?
                codegen->GetMurmurHashFunction(_results_buffer_size) :
                codegen->GetHashFunction(_results_buffer_size);
            Value* len = codegen->get_int_constant(TYPE_INT, _results_buffer_size);
            hash_result = builder.CreateCall3(hash_fn, data, len, hash_result, "hash");
        }
    } else {
        if (_var_result_begin > 0) {
            Function* hash_fn = use_murmur ?
                codegen->GetMurmurHashFunction(_var_result_begin) :
                codegen->GetHashFunction(_var_result_begin);
            Value* len = codegen->get_int_constant(TYPE_INT, _var_result_begin);
            hash_result = builder.CreateCall3(hash_fn, data, len, hash_result, "hash");
        }

        // Hash string slots
        for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
            // if (_build_expr_ctxs[i]->root()->type().type != TYPE_STRING
            //     && _build_expr_ctxs[i]->root()->type().type != TYPE_VARCHAR) continue;
            if (_build_expr_ctxs[i]->root()->type().type != TYPE_VARCHAR) {
                continue;
            }

            BasicBlock* null_block = NULL;
            BasicBlock* not_null_block = NULL;
            BasicBlock* continue_block = NULL;
            Value* str_null_result = NULL;

            void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];

            // If the hash table stores nulls, we need to check if the stringval
            // evaluated to NULL
            if (_stores_nulls) {
                null_block = BasicBlock::Create(context, "null", fn);
                not_null_block = BasicBlock::Create(context, "not_null", fn);
                continue_block = BasicBlock::Create(context, "continue", fn);

                uint8_t* null_byte_loc = &_expr_value_null_bits[i];
                Value* llvm_null_byte_loc =
                    codegen->cast_ptr_to_llvm_ptr(codegen->ptr_type(), null_byte_loc);
                Value* null_byte = builder.CreateLoad(llvm_null_byte_loc, "null_byte");
                Value* is_null = builder.CreateICmpNE(null_byte,
                        codegen->get_int_constant(TYPE_TINYINT, 0), "is_null");
                builder.CreateCondBr(is_null, null_block, not_null_block);

                // For null, we just want to call the hash function on the portion of
                // the data
                builder.SetInsertPoint(null_block);
                Function* null_hash_fn = use_murmur ?
                    codegen->GetMurmurHashFunction(sizeof(StringValue)) :
                    codegen->GetHashFunction(sizeof(StringValue));
                Value* llvm_loc = codegen->cast_ptr_to_llvm_ptr(codegen->ptr_type(), loc);
                Value* len = codegen->get_int_constant(TYPE_INT, sizeof(StringValue));
                str_null_result =
                    builder.CreateCall3(null_hash_fn, llvm_loc, len, hash_result, "str_null");
                builder.CreateBr(continue_block);

                builder.SetInsertPoint(not_null_block);
            }

            // Convert _expr_values_buffer loc to llvm value
            // Value* str_val = codegen->cast_ptr_to_llvm_ptr(codegen->get_ptr_type(TYPE_STRING), loc);
            Value* str_val = codegen->cast_ptr_to_llvm_ptr(codegen->get_ptr_type(TYPE_VARCHAR), loc);

            Value* ptr = builder.CreateStructGEP(str_val, 0);
            Value* len = builder.CreateStructGEP(str_val, 1);
            ptr = builder.CreateLoad(ptr, "ptr");
            len = builder.CreateLoad(len, "len");

            // Call hash(ptr, len, hash_result);
            Function* general_hash_fn = use_murmur ? codegen->GetMurmurHashFunction() :
                codegen->GetHashFunction();
            Value* string_hash_result =
                builder.CreateCall3(general_hash_fn, ptr, len, hash_result, "string_hash");

            if (_stores_nulls) {
                builder.CreateBr(continue_block);
                builder.SetInsertPoint(continue_block);
                // Use phi node to reconcile that we could have come from the string-null
                // path and string not null paths.
                PHINode* phi_node = builder.CreatePHI(codegen->get_type(TYPE_INT), 2, "hash_phi");
                phi_node->addIncoming(string_hash_result, not_null_block);
                phi_node->addIncoming(str_null_result, null_block);
                hash_result = phi_node;
            } else {
                hash_result = string_hash_result;
            }
        }
    }

    builder.CreateRet(hash_result);
    return codegen->FinalizeFunction(fn);
}

// Codegen for PartitionedHashTableCtx::equals.  For a hash table with two exprs (string,int),
// the IR looks like:
//
// define i1 @equals(%"class.impala::PartitionedHashTableCtx"* %this_ptr,
//                   %"class.impala::TupleRow"* %row) {
// entry:
//   %result = call i64 @GetSlotRef(%"class.impala::ExprContext"* inttoptr
//                                  (i64 146381856 to %"class.impala::ExprContext"*),
//                                  %"class.impala::TupleRow"* %row)
//   %0 = trunc i64 %result to i1
//   br i1 %0, label %null, label %not_null
//
// false_block:                            ; preds = %not_null2, %null1, %not_null, %null
//   ret i1 false
//
// null:                                             ; preds = %entry
//   br i1 false, label %continue, label %false_block
//
// not_null:                                         ; preds = %entry
//   %1 = load i32* inttoptr (i64 104774368 to i32*)
//   %2 = ashr i64 %result, 32
//   %3 = trunc i64 %2 to i32
//   %cmp_raw = icmp eq i32 %3, %1
//   br i1 %cmp_raw, label %continue, label %false_block
//
// continue:                                         ; preds = %not_null, %null
//   %result4 = call { i64, i8* } @GetSlotRef1(
//       %"class.impala::ExprContext"* inttoptr
//       (i64 146381696 to %"class.impala::ExprContext"*),
//       %"class.impala::TupleRow"* %row)
//   %4 = extractvalue { i64, i8* } %result4, 0
//   %5 = trunc i64 %4 to i1
//   br i1 %5, label %null1, label %not_null2
//
// null1:                                            ; preds = %continue
//   br i1 false, label %continue3, label %false_block
//
// not_null2:                                        ; preds = %continue
//   %6 = extractvalue { i64, i8* } %result4, 0
//   %7 = ashr i64 %6, 32
//   %8 = trunc i64 %7 to i32
//   %result5 = extractvalue { i64, i8* } %result4, 1
//   %cmp_raw6 = call i1 @_Z11StringValEQPciPKN6impala11StringValueE(
//       i8* %result5, i32 %8, %"struct.impala::StringValue"* inttoptr
//       (i64 104774384 to %"struct.impala::StringValue"*))
//   br i1 %cmp_raw6, label %continue3, label %false_block
//
// continue3:                                        ; preds = %not_null2, %null1
//   ret i1 true
// }
Function* PartitionedHashTableCtx::codegen_equals(RuntimeState* state) {
    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        // Disable codegen for CHAR
        if (_build_expr_ctxs[i]->root()->type().type == TYPE_CHAR) return NULL;
    }

    LlvmCodeGen* codegen;
    if (!state->get_codegen(&codegen).ok()) return NULL;
    // Get types to generate function prototype
    Type* tuple_row_type = codegen->get_type(TupleRow::_s_llvm_class_name);
    DCHECK(tuple_row_type != NULL);
    PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

    Type* this_type = codegen->get_type(PartitionedHashTableCtx::_s_llvm_class_name);
    DCHECK(this_type != NULL);
    PointerType* this_ptr_type = PointerType::get(this_type, 0);

    LlvmCodeGen::FnPrototype prototype(codegen, "Equals", codegen->get_type(TYPE_BOOLEAN));
    prototype.add_argument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
    prototype.add_argument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

    LLVMContext& context = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(context);
    Value* args[2];
    Function* fn = prototype.generate_prototype(&builder, args);
    Value* row = args[1];

    BasicBlock* false_block = BasicBlock::Create(context, "false_block", fn);
    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        BasicBlock* null_block = BasicBlock::Create(context, "null", fn);
        BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", fn);
        BasicBlock* continue_block = BasicBlock::Create(context, "continue", fn);

        // call get_value on build_exprs[i]
        Function* expr_fn;
        Status status = _build_expr_ctxs[i]->root()->get_codegend_compute_fn(state, &expr_fn);
        if (!status.ok()) {
            // VLOG_QUERY << "Problem with codegen_equals: " << status.GetDetail();
            VLOG_QUERY << "Problem with codegen_equals: " << status.get_error_msg();
            fn->eraseFromParent(); // deletes function
            return NULL;
        }

        Value* ctx_arg = codegen->cast_ptr_to_llvm_ptr(
                codegen->get_ptr_type(ExprContext::_s_llvm_class_name), _build_expr_ctxs[i]);
        Value* expr_fn_args[] = { ctx_arg, row };
        CodegenAnyVal result = CodegenAnyVal::create_call_wrapped(codegen, &builder,
                _build_expr_ctxs[i]->root()->type(), expr_fn, expr_fn_args, "result");
        Value* is_null = result.get_is_null();

        // Determine if probe is null (i.e. _expr_value_null_bits[i] == true). In
        // the case where the hash table does not store nulls, this is always false.
        Value* probe_is_null = codegen->false_value();
        uint8_t* null_byte_loc = &_expr_value_null_bits[i];
        if (_stores_nulls) {
            Value* llvm_null_byte_loc =
                codegen->cast_ptr_to_llvm_ptr(codegen->ptr_type(), null_byte_loc);
            Value* null_byte = builder.CreateLoad(llvm_null_byte_loc);
            probe_is_null = builder.CreateICmpNE(null_byte,
                    codegen->get_int_constant(TYPE_TINYINT, 0));
        }

        // Get llvm value for probe_val from '_expr_values_buffer'
        void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];
        Value* probe_val = codegen->cast_ptr_to_llvm_ptr(
                codegen->get_ptr_type(_build_expr_ctxs[i]->root()->type()), loc);

        // Branch for get_value() returning NULL
        builder.CreateCondBr(is_null, null_block, not_null_block);

        // Null block
        builder.SetInsertPoint(null_block);
        builder.CreateCondBr(probe_is_null, continue_block, false_block);

        // Not-null block
        builder.SetInsertPoint(not_null_block);
        if (_stores_nulls) {
            BasicBlock* cmp_block = BasicBlock::Create(context, "cmp", fn);
            // First need to compare that probe expr[i] is not null
            builder.CreateCondBr(probe_is_null, false_block, cmp_block);
            builder.SetInsertPoint(cmp_block);
        }
        // Check result == probe_val
        Value* is_equal = result.EqToNativePtr(probe_val);
        builder.CreateCondBr(is_equal, continue_block, false_block);

        builder.SetInsertPoint(continue_block);
    }
    builder.CreateRet(codegen->true_value());

    builder.SetInsertPoint(false_block);
    builder.CreateRet(codegen->false_value());

    return codegen->FinalizeFunction(fn);
}
#endif

} // namespace doris

